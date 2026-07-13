/*********************************************************************************
 * Modifications Copyright 2026 eBay Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 *********************************************************************************/

#include "net/async_conn.hpp"

#include <cerrno>
#include <cstring>
#include <utility>

#include <sisl/logging/logging.h> // r/w + fault trace (base module; visible when a consumer inits logging)

extern "C" {
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h> // TCP_NODELAY
#include <sys/socket.h>
#include <unistd.h>
}

namespace craft::net {

namespace {
template < class T >
std::span< uint8_t const > as_bytes(T const& v) {
    return {reinterpret_cast< uint8_t const* >(&v), sizeof(T)};
}
constexpr std::size_t k_recv_chunk = 64 * 1024; // per-recv landing size; a message may span several recvs
} // namespace

craft_async_conn::craft_async_conn(std::string host, uint16_t port, uint32_t max_tx, ::io_uring* ring) :
        host_{std::move(host)}, port_{port}, max_tx_{max_tx}, ring_{ring} {}

craft_async_conn::~craft_async_conn() { shutdown(); }

::io_uring_sqe* craft_async_conn::acquire_sqe() noexcept {
    ::io_uring_sqe* sqe = ::io_uring_get_sqe(ring_);
    if (nullptr != sqe) return sqe;
    (void)::io_uring_submit(ring_); // SQ full: flush the queued legs to make room, then retry once
    return ::io_uring_get_sqe(ring_);
}

void craft_async_conn::drop_fd() noexcept {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    ready_ = false;
}

void craft_async_conn::release_send() noexcept {
    if (send_waiters_.empty()) {
        sending_ = false;
        return;
    }
    auto const h = send_waiters_.front();
    send_waiters_.erase(send_waiters_.begin()); // FIFO; sending_ STAYS true -- the slot is handed to h
    if (h) h.resume();
}

void craft_async_conn::fail_all() noexcept {
    // Move-then-resume: a resumed leg erases its own pending_ entry, which would invalidate an iterator we held.
    auto pend = std::move(pending_);
    pending_.clear();
    for (auto& [rid, slot] : pend) {
        slot->failed = true;
        slot->ready = true;
        if (slot->waiter) slot->waiter.resume();
    }
    // Ops parked for the send slot must unwind too: their reply_slots are already flagged failed above, so once
    // resumed they send on the (now dead) fd, fall through the failed reply, and return an error. Resume after the
    // reply-waiters so none observes a half-torn-down state.
    sending_ = false;
    auto sw = std::move(send_waiters_);
    send_waiters_.clear();
    for (auto const h : sw)
        if (h) h.resume();
}

void craft_async_conn::shutdown() noexcept {
    stopping_ = true;
    if (pump_) {
        // The caller must have quiesced the ring first (no recv CQE of ours left to reap), so destroying the
        // suspended pump frame here cannot be raced by a reap. Mirrors ublkpp's queue_service dtor.
        pump_.destroy();
        pump_ = {};
    }
    drop_fd();
    fail_all();
}

// ── on-ring primitives (each co_awaits a cqe_awaitable tied to its own SQE) ──

sisl::async::task< int > craft_async_conn::ring_connect() {
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ >= 0) { // see set_nodelay in conn.cpp: CRAFT is request/response, so Nagle only ever adds latency
        int const one = 1;
        ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    }
    if (fd_ < 0) co_return -errno;

    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port_);
    if (::inet_pton(AF_INET, host_.c_str(), &sa.sin_addr) != 1) co_return -EINVAL; // loopback / IP literal only (v1)

    sisl::async::cqe_awaitable ev;
    auto* sqe = acquire_sqe();
    if (nullptr == sqe) co_return -EAGAIN;
    ::io_uring_prep_connect(sqe, fd_, reinterpret_cast< sockaddr* >(&sa), sizeof(sa)); // sa is frame-local, stable
    ::io_uring_sqe_set_data64(sqe, sisl::async::encode_managed_user_data(&ev));
    int const res = co_await ev;
    co_return (res < 0) ? res : 0;
}

sisl::async::task< int > craft_async_conn::ring_send_all(std::span< uint8_t const > data) {
    std::size_t off = 0;
    while (off < data.size()) {
        sisl::async::cqe_awaitable ev;
        auto* sqe = acquire_sqe();
        if (nullptr == sqe) co_return -EAGAIN;
        ::io_uring_prep_send(sqe, fd_, data.data() + off, data.size() - off, MSG_NOSIGNAL);
        ::io_uring_sqe_set_data64(sqe, sisl::async::encode_managed_user_data(&ev));
        int const n = co_await ev;
        if (n <= 0) co_return (n == 0 ? -EPIPE : n); // 0 = peer closed; <0 = error
        off += static_cast< std::size_t >(n);
    }
    co_return 0;
}

// ── the recv pump: one persistent recv, demux every complete message by request_id ──

sisl::async::disk_task< int > craft_async_conn::run_pump() {
    for (;;) {
        recv_scratch_.resize(k_recv_chunk);
        sisl::async::cqe_awaitable ev;
        auto* sqe = acquire_sqe();
        if (nullptr == sqe) {
            // No SQE for the recv: without a live recv nothing can complete, so this connection is wedged. Fail
            // every waiter rather than hang; the next op reconnects.
            fail_all();
            co_return 0;
        }
        ::io_uring_prep_recv(sqe, fd_, recv_scratch_.data(), recv_scratch_.size(), 0);
        ::io_uring_sqe_set_data64(sqe, sisl::async::encode_managed_user_data(&ev));
        int const n = co_await ev;

        if (stopping_) co_return 0; // shutdown() already failed the waiters + will destroy this frame
        if (n <= 0) {               // 0 = EOF, <0 = recv error: the connection is gone
            LOGDEBUG("craft_async_conn[{}:{}] recv fault (n={}): peer closed / reset, failing {} pending op(s)", host_,
                     port_, n, pending_.size());
            ready_ = false;
            fail_all();
            co_return 0;
        }
        rx_.insert(rx_.end(), recv_scratch_.data(), recv_scratch_.data() + n);

        // Drain every complete message now buffered, demuxing each to the leg that awaits its request_id.
        for (;;) {
            auto parsed = wire::parse_message(rx_, max_tx_);
            if (!parsed) {
                if (parsed.error() == wire::parse_error::incomplete) break; // need more bytes; keep the pump going
                // Malformed framing: the stream is unusable. Fail everyone; the next op reconnects.
                LOGDEBUG("craft_async_conn[{}:{}] malformed reply framing: resetting connection", host_, port_);
                ready_ = false;
                fail_all();
                co_return 0;
            }
            uint16_t const rid = parsed->hdr.request_id;
            std::size_t const total = parsed->total;
            auto it = pending_.find(rid);
            if (it != pending_.end()) {
                auto* slot = it->second;
                pending_.erase(it); // erase BEFORE the resume (the leg's own defensive erase then no-ops)
                slot->msg.assign(rx_.begin(), rx_.begin() + static_cast< std::ptrdiff_t >(total));
                slot->ready = true;
                rx_.erase(rx_.begin(), rx_.begin() + static_cast< std::ptrdiff_t >(total));
                if (slot->waiter) slot->waiter.resume(); // resumes the leg (nested, on this reap thread)
            } else {
                rx_.erase(rx_.begin(), rx_.begin() + static_cast< std::ptrdiff_t >(total)); // stale/unknown rid
            }
        }
    }
}

// ── framed request/response over the pump ──

sisl::async::task< std::expected< std::vector< uint8_t >, net_error > >
craft_async_conn::round_trip(wire::op o, std::span< uint8_t const > op_hdr, std::span< uint8_t const > body) {
    uint16_t const rid = next_rid_++;
    if (next_rid_ == 0) next_rid_ = 1; // 0 is a fine correlator, but keep ids monotonic and non-zero

    std::vector< uint8_t > out;
    wire::frame_message(out, o, 0, rid, op_hdr, body); // out lives in this frame until ring_send_all completes

    reply_slot slot; // in this frame -> stable while the pump holds &slot
    pending_[rid] = &slot;

    // Hold the wire's send slot for the WHOLE of this message so its bytes are contiguous on the socket, then
    // release before waiting for the reply (the next op sends while we wait; replies demux by request_id).
    co_await send_awaitable{this};
    int const sr = co_await ring_send_all(out);
    release_send();
    if (sr < 0) {
        pending_.erase(rid);
        co_return std::unexpected(net_error::send);
    }

    co_await reply_awaitable{&slot}; // the pump fills slot + resumes us (or fail_all does, with failed=true)
    pending_.erase(rid);             // defensive: the pump already erased on delivery
    if (slot.failed) co_return std::unexpected(net_error::closed);
    co_return std::move(slot.msg);
}

// ── lazy connect + HELO + arm the pump ──

sisl::async::task< std::expected< void, net_error > >
craft_async_conn::ensure_ready(std::array< uint8_t, 16 > const& vol, uint64_t token, uint64_t term) {
    if (ready_) co_return std::expected< void, net_error >{};
    if (connecting_) {
        // Another op is establishing the connection (QD>1); park until it finishes, then re-check.
        co_await connect_awaitable{this};
        if (ready_) co_return std::expected< void, net_error >{};
        co_return std::unexpected(net_error::connect);
    }
    connecting_ = true;

    // Clears connecting_ and resumes every op that parked behind us, then yields `r` as our own result.
    auto finish = [this](std::expected< void, net_error > r) {
        connecting_ = false;
        auto waiters = std::move(connect_waiters_);
        connect_waiters_.clear();
        for (auto h : waiters)
            if (h) h.resume();
        return r;
    };

    // A prior fault parked the pump at final_suspend; reclaim it before reconnecting.
    if (pump_) {
        pump_.destroy();
        pump_ = {};
    }
    drop_fd();
    rx_.clear();
    stopping_ = false;

    LOGTRACE("craft_async_conn[{}:{}] connecting (on-ring data path)", host_, port_);
    if (int const cr = co_await ring_connect(); cr < 0) {
        LOGDEBUG("craft_async_conn[{}:{}] connect failed (res={})", host_, port_, cr);
        drop_fd();
        co_return finish(std::unexpected(net_error::connect));
    }

    // Arm the pump BEFORE HELO -- HELO's reply is demuxed by the pump like any op. Steal the frame out of the
    // disk_task (so its dtor does not free it), then resume once to stage the first recv on the ring.
    auto pt = run_pump();
    pump_ = std::exchange(pt._coro, {});
    pump_.resume();

    wire::helo_req req{vol, token, term};
    auto reply = co_await round_trip(wire::op::helo, as_bytes(req), {});
    if (!reply) co_return finish(std::unexpected(reply.error()));
    auto parsed = wire::parse_message(*reply, max_tx_);
    if (!parsed || parsed->hdr.op != static_cast< uint8_t >(wire::op::helo_rsp)) {
        drop_fd();
        co_return finish(std::unexpected(net_error::malformed));
    }
    if (static_cast< wire::status >(parsed->hdr.status) != wire::status::ok) {
        LOGDEBUG("craft_async_conn[{}:{}] HELO rejected (status={}) at term={}", host_, port_,
                 static_cast< int >(parsed->hdr.status), term);
        drop_fd(); // HELO rejected: treat as a connection-level fault so the client re-establishes the session
        co_return finish(std::unexpected(net_error::closed));
    }
    ready_ = true;
    LOGTRACE("craft_async_conn[{}:{}] ready (HELO'd at term={})", host_, port_, term);
    co_return finish(std::expected< void, net_error >{});
}

// ── IO ops ──

sisl::async::task< std::expected< lsn_reply, net_error > >
craft_async_conn::write(int64_t dlsn, uint64_t addr, uint64_t len, std::span< uint8_t const > data, int64_t commit_lsn,
                        int64_t all_committed_lsn) {
    wire::write_req req{{commit_lsn, all_committed_lsn}, dlsn, addr, len};
    auto reply = co_await round_trip(wire::op::write, as_bytes(req), data);
    if (!reply) co_return std::unexpected(reply.error());
    auto parsed = wire::parse_message(*reply, max_tx_);
    if (!parsed || parsed->hdr.op != static_cast< uint8_t >(wire::op::write_rsp))
        co_return std::unexpected(net_error::malformed);
    auto const wr = wire::decode< wire::write_rsp >(parsed->op_header);
    co_return lsn_reply{static_cast< wire::status >(parsed->hdr.status), wr.commit_lsn, wr.last_append_lsn};
}

sisl::async::task< std::expected< lsn_reply, net_error > > craft_async_conn::keep_alive(int64_t commit_lsn,
                                                                                        int64_t all_committed_lsn) {
    wire::keepalive_req req{{commit_lsn, all_committed_lsn}};
    auto reply = co_await round_trip(wire::op::keepalive, as_bytes(req), {});
    if (!reply) co_return std::unexpected(reply.error());
    auto parsed = wire::parse_message(*reply, max_tx_);
    if (!parsed || parsed->hdr.op != static_cast< uint8_t >(wire::op::keepalive_rsp))
        co_return std::unexpected(net_error::malformed);
    auto const ka = wire::decode< wire::keepalive_rsp >(parsed->op_header);
    co_return lsn_reply{static_cast< wire::status >(parsed->hdr.status), ka.commit_lsn, ka.last_append_lsn};
}

sisl::async::task< std::expected< read_reply, net_error > >
craft_async_conn::read(int64_t read_lsn, uint64_t addr, uint64_t len, std::span< uint8_t > dest, int64_t commit_lsn,
                       int64_t all_committed_lsn) {
    wire::read_req req{{commit_lsn, all_committed_lsn}, read_lsn, addr, len};
    auto reply = co_await round_trip(wire::op::read, as_bytes(req), {});
    if (!reply) co_return std::unexpected(reply.error());
    auto parsed = wire::parse_message(*reply, max_tx_);
    if (!parsed || parsed->hdr.op != static_cast< uint8_t >(wire::op::read_rsp))
        co_return std::unexpected(net_error::malformed);

    auto const rr = wire::decode< wire::read_rsp >(parsed->op_header);
    read_reply out{static_cast< wire::status >(parsed->hdr.status), rr.commit_lsn, rr.last_append_lsn, {}};
    if (out.status != wire::status::ok) co_return out; // no body on a non-ok reply

    // Scatter the sparse reply into `dest` in wire order, zero the holes (verbatim from the blocking client).
    auto extents = wire::decode_extents(parsed->body, rr.extent_count);
    if (!extents) co_return std::unexpected(net_error::malformed);
    auto const plan = wire::plan_scatter(*extents, addr, len);
    if (!plan.ok) co_return std::unexpected(net_error::malformed);
    std::span< uint8_t const > const packed = parsed->body.subspan(rr.extent_count * sizeof(wire::extent_desc));
    std::size_t src = 0;
    for (auto const& [dst_off, n] : plan.data) {
        if (src + n > packed.size()) co_return std::unexpected(net_error::malformed);
        std::memcpy(dest.data() + dst_off, packed.data() + src, n);
        src += n;
    }
    for (auto const& [dst_off, n] : plan.holes)
        std::memset(dest.data() + dst_off, 0, n);
    out.extents = std::move(*extents);
    co_return out;
}

} // namespace craft::net
