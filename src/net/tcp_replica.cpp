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

#include <craft/net/tcp_replica.hpp>

#include <cstring>
#include <span>
#include <utility>

#include <craft/status.hpp> // status_to_error (the shared wire <-> craft_error bridge)

#include "net/async_conn.hpp" // the on-ring data path (internal); its dtor is emitted here where it is complete

namespace craft {

namespace {
auto fail(craft_error e) { return std::unexpected(make_error_condition(e)); }

// Transport fault -> the domain error. The send/recv asymmetry matters for craft_client's sub-quorum
// accounting: a failed SEND provably never reached the peer (REPLICA_DOWN, a deterministic reject it may
// count), but a lost/garbled REPLY means the peer MAY have applied the write, so it must look like a timeout
// (never counted, never resolves the slot Empty).
std::error_condition net_to_error(net::net_error e) {
    switch (e) {
    case net::net_error::send:
    case net::net_error::setup:
    case net::net_error::connect:
        return make_error_condition(craft_error::REPLICA_DOWN); // never delivered
    case net::net_error::recv:
    case net::net_error::closed:
    case net::net_error::malformed:
    case net::net_error::timed_out:
        return std::make_error_condition(std::errc::timed_out); // delivered; reply lost -> may have applied
    case net::net_error::invalid_argument:
        return std::make_error_condition(std::errc::invalid_argument);
    }
    return make_error_condition(craft_error::REPLICA_DOWN);
}

std::vector< io_extent > to_io_extents(std::vector< wire::extent_desc > const& es) {
    std::vector< io_extent > out;
    out.reserve(es.size());
    for (auto const& e : es)
        out.push_back(io_extent{e.addr, e.len, e.hole != 0});
    return out;
}
} // namespace

CraftTcpReplica::CraftTcpReplica(std::string host, uint16_t port, peer_id_t id, std::array< uint8_t, 16 > vol,
                                 std::chrono::milliseconds op_timeout) :
        host_{std::move(host)}, port_{port}, id_{id}, vol_id_{vol}, op_timeout_{op_timeout} {
    worker_ = std::thread([this] { worker_loop(); });
}

void CraftTcpReplica::shutdown() {
    {
        std::lock_guard< std::mutex > g{mu_};
        stop_ = true;
    }
    cv_.notify_all();
    // Join only from a non-worker thread. The harness (TcpReplicaHandles) calls this from the main thread,
    // volumes still alive, before dropping any proxy -- draining in-flight ops while nothing can hit zero refs.
    if (worker_.joinable() && worker_.get_id() != std::this_thread::get_id()) worker_.join();
}

CraftTcpReplica::~CraftTcpReplica() {
    shutdown();
    // Last resort if ~ somehow ran on the worker (must not, under the harness contract): detach so a joinable
    // std::thread does not std::terminate. The worker holds no dangling reference to us past its current job.
    if (worker_.joinable()) worker_.detach();
}

std::shared_ptr< CraftTcpReplica::hop_event > CraftTcpReplica::hop() {
    auto ev = std::make_shared< hop_event >();
    {
        std::lock_guard< std::mutex > g{mu_};
        if (!stop_) {
            jobs_.push_back([ev] { ev->complete({}); }); // the worker resumes the coroutine there
            cv_.notify_one();
            return ev;
        }
    }
    ev->complete({}); // shutting down: complete inline so the coroutine is never stranded
    return ev;
}

void CraftTcpReplica::worker_loop() {
    std::unique_lock< std::mutex > lk{mu_};
    for (;;) {
        cv_.wait(lk, [this] { return stop_ || !jobs_.empty(); });
        if (jobs_.empty()) return; // stop_ with nothing left to drain
        auto job = std::move(jobs_.front());
        jobs_.pop_front();
        lk.unlock();
        job(); // resumes the coroutine INLINE: it runs its blocking socket op here and completes
        lk.lock();
    }
}

bool CraftTcpReplica::ensure_connected() {
    if (connected_) return true;
    auto c = net::craft_tcp_client::connect(host_, port_);
    if (!c) return false;
    conn_ = std::move(*c); // drops any prior (poisoned) client -> its ring/socket, with a stuck recv, is torn down
    conn_.set_op_timeout(op_timeout_);
    connected_ = true;
    return true;
}

std::error_condition CraftTcpReplica::on_net_fault(net::net_error e) {
    // A timed-out recv is still pending in the ring, and a closed/errored socket is dead -- either way this
    // connection cannot serve the next op. Poison it (drop connected_/bound_) so the next op reconnects and
    // re-HELOs. Only invalid_argument (a caller precondition, e.g. a short read dest) leaves the socket intact.
    //
    // Resetting the WHOLE connection on a per-op timeout is a SHIM artifact, not target semantics. This proxy
    // is strictly serial (one worker, one op in flight), so a timeout can only strand that single op's reply --
    // there are no other in-flight ops on this socket to lose -- and the still-pending recv SQE poisons the
    // one-op ring, so the socket has to go. The async transport that replaces this pipelines many ops keyed by
    // request_id: there a deadline abandons only that op's landing pad (cancel its SQE, drop a late reply by
    // id) and KEEPS the connection so every other in-flight op still lands. Full teardown is then reserved for
    // real connection faults (EOF / malformed / send). Either way a timeout is std::errc::timed_out, never a
    // deterministic reject: the reply was lost, the peer may well have applied the write.
    if (e != net::net_error::invalid_argument) {
        connected_ = false;
        bound_ = false;
    }
    return net_to_error(e);
}

std::optional< std::error_condition > CraftTcpReplica::ensure_bound(uint64_t term) {
    if (!ensure_connected()) return make_error_condition(craft_error::REPLICA_DOWN);
    // Re-HELO whenever the requested term differs from what this connection is bound at -- not just on the first
    // IO. A logout+relogin bumps the session term (leader run_login: ++term_), and the wire carries no per-op
    // term (it is a connection constant), so a connection still bound at the old term would have the server
    // stamp that stale term on every IO and fence it. Binding is idempotent server-side, so re-HELO rebinds
    // this same connection to the new term. (A post-timeout reset drops bound_, so that path re-HELOs too.)
    if (bound_ && term == bound_term_) return std::nullopt;
    auto h = conn_.helo(vol_id_, /*client_token=*/0, term); // token unused by the server's HELO (auth is P6)
    if (!h) return on_net_fault(h.error());
    if (*h != wire::status::ok) return status_to_error(*h);
    bound_ = true;
    bound_term_ = term;
    return std::nullopt;
}

// ── client-facing ──

async_result< LoginResult > CraftTcpReplica::login(uint64_t client_token) {
    auto ev = hop();
    co_await *ev; // now on the worker thread
    if (!ensure_connected()) co_return fail(craft_error::REPLICA_DOWN);
    auto r = conn_.login(client_token);
    if (!r) co_return std::unexpected(on_net_fault(r.error()));
    if (r->max_tx) max_tx_ = r->max_tx;  // the volume's max transfer PAYLOAD (login_rsp)
    if (r->lba_size) lba_ = r->lba_size; // block size; with max_tx_ it sizes the on-ring parse bound (framed_body_max)

    if (r->term > 0) { // a successful login binds this (leader) connection at its term; a redirect does not
        bound_ = true;
        bound_term_ = r->term;
    }
    LoginResult out;
    out.dLSN = r->dlsn;
    out.term = r->term;
    out.lba_size = r->lba_size;
    out.capacity = r->capacity; // wire login_rsp already carried it; surface it instead of dropping it
    out.max_tx = r->max_tx;     // ditto: the volume max transfer, conveyed once via login
    std::memcpy(&out.leader_hint, r->leader_hint.data(), 16);
    out.members.reserve(r->members.size());
    for (auto const& m : r->members) {
        replica_endpoint ep;
        std::memcpy(&ep.id, m.id.data(), 16);
        ep.addr = m.addr;
        out.members.push_back(std::move(ep));
    }
    co_return out;
}

async_status CraftTcpReplica::logout(client_hdr hdr) {
    auto ev = hop();
    co_await *ev;
    if (!ensure_connected()) co_return fail(craft_error::REPLICA_DOWN);
    (void)hdr;
    auto r = conn_.logout();
    if (!r) co_return std::unexpected(on_net_fault(r.error()));
    if (*r != wire::status::ok) co_return std::unexpected(status_to_error(*r));
    co_return ok();
}

async_status CraftTcpReplica::write(client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len, sisl::sg_list data) {
    // Serialize the payload NOW, on the CALLER's thread, before the first suspension. This write may be a
    // straggler that keeps running after craft_client acked at quorum and the caller recycled its buffer, so
    // nothing past here may reference `data` (the when_quorum payload contract). This owned copy is the mem
    // model's take_payload equivalent -- the transport's one write-path copy.
    std::vector< uint8_t > payload; // empty == a zero write
    if (data.size != 0) {
        payload.reserve(data.size);
        for (auto const& iov : data.iovs) {
            auto const* p = static_cast< uint8_t const* >(iov.iov_base);
            payload.insert(payload.end(), p, p + iov.iov_len);
        }
    }
    if (ring_) { // ── on-ring data path: lazily HELO the data fd at hdr.term, then send over the caller's ring ──
        if (!aconn_)
            aconn_ =
                std::make_unique< net::craft_async_conn >(host_, port_, wire::framed_body_max(max_tx_, lba_), ring_);
        if (auto e = co_await aconn_->ensure_ready(vol_id_, /*token=*/0, hdr.term); !e)
            co_return std::unexpected(net_to_error(e.error()));
        auto r = co_await aconn_->write(dlsn, addr, len, payload, hdr.commit_lsn, hdr.all_committed_lsn);
        if (!r) co_return std::unexpected(net_to_error(r.error()));
        if (r->status != wire::status::ok) co_return std::unexpected(status_to_error(r->status));
        co_return ok();
    }

    auto ev = hop();
    co_await *ev;
    if (auto e = ensure_bound(hdr.term)) co_return std::unexpected(*e);
    auto r = conn_.write(dlsn, addr, len, payload, hdr.commit_lsn, hdr.all_committed_lsn);
    if (!r) co_return std::unexpected(on_net_fault(r.error()));
    if (r->status != wire::status::ok) co_return std::unexpected(status_to_error(r->status));
    co_return ok();
}

async_result< std::vector< io_extent > > CraftTcpReplica::read(client_hdr hdr, int64_t read_lsn, uint64_t addr,
                                                               uint64_t len, sisl::sg_list dest) {
    // Single-iovec dest: fill it in place. Otherwise read into scratch and scatter into the dest iovecs. Pure
    // span math -- safe on the caller's thread before either path suspends.
    bool const inplace = dest.iovs.size() == 1 && dest.iovs[0].iov_len >= len;
    std::vector< uint8_t > scratch;
    std::span< uint8_t > d;
    if (inplace) {
        d = {static_cast< uint8_t* >(dest.iovs[0].iov_base), len};
    } else {
        scratch.resize(len);
        d = {scratch.data(), len};
    }

    net::read_reply reply;
    if (ring_) { // ── on-ring data path ──
        if (!aconn_)
            aconn_ =
                std::make_unique< net::craft_async_conn >(host_, port_, wire::framed_body_max(max_tx_, lba_), ring_);
        if (auto e = co_await aconn_->ensure_ready(vol_id_, /*token=*/0, hdr.term); !e)
            co_return std::unexpected(net_to_error(e.error()));
        auto r = co_await aconn_->read(read_lsn, addr, len, d, hdr.commit_lsn, hdr.all_committed_lsn);
        if (!r) co_return std::unexpected(net_to_error(r.error()));
        reply = std::move(*r);
    } else {
        auto ev = hop();
        co_await *ev;
        if (auto e = ensure_bound(hdr.term)) co_return std::unexpected(*e);
        auto r = conn_.read(read_lsn, addr, len, d, hdr.commit_lsn, hdr.all_committed_lsn);
        if (!r) co_return std::unexpected(on_net_fault(r.error()));
        reply = std::move(*r);
    }
    if (reply.status != wire::status::ok) co_return std::unexpected(status_to_error(reply.status));

    if (!inplace) { // scatter the contiguous result into the caller's iovecs
        std::size_t off = 0;
        for (auto const& iov : dest.iovs) {
            std::size_t const n = std::min< std::size_t >(iov.iov_len, len - off);
            std::memcpy(iov.iov_base, scratch.data() + off, n);
            off += n;
            if (off >= len) break;
        }
    }
    co_return to_io_extents(reply.extents);
}

async_result< lsn_pair > CraftTcpReplica::keep_alive(client_hdr hdr) {
    if (ring_) { // ── on-ring data path ──
        if (!aconn_)
            aconn_ =
                std::make_unique< net::craft_async_conn >(host_, port_, wire::framed_body_max(max_tx_, lba_), ring_);
        if (auto e = co_await aconn_->ensure_ready(vol_id_, /*token=*/0, hdr.term); !e)
            co_return std::unexpected(net_to_error(e.error()));
        auto r = co_await aconn_->keep_alive(hdr.commit_lsn, hdr.all_committed_lsn);
        if (!r) co_return std::unexpected(net_to_error(r.error()));
        if (r->status != wire::status::ok) co_return std::unexpected(status_to_error(r->status));
        co_return lsn_pair{r->commit_lsn, r->last_append_lsn};
    }

    auto ev = hop();
    co_await *ev;
    if (auto e = ensure_bound(hdr.term)) co_return std::unexpected(*e);
    auto r = conn_.keep_alive(hdr.commit_lsn, hdr.all_committed_lsn);
    if (!r) co_return std::unexpected(on_net_fault(r.error()));
    if (r->status != wire::status::ok) co_return std::unexpected(status_to_error(r->status));
    co_return lsn_pair{r->commit_lsn, r->last_append_lsn};
}

// ── peer-facing: a client never invokes these; stubbed so the vtable is complete ──

async_result< lsn_pair > CraftTcpReplica::get_lsns() { co_return fail(craft_error::NOT_LEADER); }
async_result< lsn_pair > CraftTcpReplica::get_rs_commit_lsn() { co_return fail(craft_error::NOT_LEADER); }
async_result< std::vector< JournalSlot > > CraftTcpReplica::fetch_data(std::vector< int64_t >) {
    co_return fail(craft_error::NOT_LEADER);
}
async_status CraftTcpReplica::truncate(int64_t) { co_return fail(craft_error::NOT_LEADER); }

} // namespace craft
