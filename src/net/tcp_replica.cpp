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

#include "net/tcp_replica.hpp"

#include <cassert>
#include <cstring>
#include <span>
#include <utility>
#include <fmt/ranges.h>
#include <chrono>
#include <boost/uuid/uuid_io.hpp>

#include <sisl/logging/logging.h> // the round-trip summary at shutdown (rt_stat)

#include <craft/status.hpp> // status_to_error (the shared wire <-> craft_error bridge)

#include "net/async_conn.hpp" // the on-ring data path (internal); its dtor is emitted here where it is complete

namespace craft {

namespace {
// Nanoseconds since `t0`, for the wire round-trip stats. steady_clock::now() is a vDSO read (no syscall).
inline uint64_t ns_since(std::chrono::steady_clock::time_point t0) {
    return static_cast< uint64_t >(
        std::chrono::duration_cast< std::chrono::nanoseconds >(std::chrono::steady_clock::now() - t0).count());
}
} // namespace

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
        host_{std::move(host)},
        port_{port},
        id_{id},
        vol_id_{vol},
        op_timeout_{op_timeout},
        mgr_{net::craft_session_mgr::get()} {}

net::craft_async_conn* CraftTcpReplica::conn_for(::io_uring* q) {
    if (nullptr == q) return nullptr;
    // Steady state: an acquire load of the published count, then a pointer scan. Slots are append-only and a
    // slot is fully built before n_slots_ advances over it, so everything below `n` is safe to read lock-free.
    std::size_t const n = n_slots_.load(std::memory_order_acquire);
    for (std::size_t i = 0; i < n; ++i) {
        if (slots_[i].ring == q) {
            assert(slots_[i].owner == std::this_thread::get_id() &&
                   "a ring must only be passed from its owner thread");
            return slots_[i].conn.get();
        }
    }
    // Table permanently full: the miss can never seat (append-only, monotone), so stay lock-free on the
    // degraded tier too -- and be LOUD in debug: past the cap this queue's verbs silently change resumption
    // thread (blocking tier), which a driver counting on ring affinity must find out about at test time.
    if (n == k_max_queues) {
        assert(!"conn_for: ring table full (k_max_queues) -- this queue degrades to the blocking path");
        return nullptr;
    }
    // First verb from this ring: append its slot. Rare (once per queue per session), so the lock is off the
    // steady-state path; the re-scan under it covers a racing append of the SAME ring (contract violation, but
    // double-constructing a conn would be worse than tolerating it).
    std::lock_guard< std::mutex > g{slots_mu_};
    std::size_t const m = n_slots_.load(std::memory_order_relaxed);
    for (std::size_t i = n; i < m; ++i) {
        if (slots_[i].ring == q) return slots_[i].conn.get();
    }
    if (m == k_max_queues) return nullptr; // table full: this queue degrades to the blocking path
    auto& s = slots_[m];
    s.ring = q;
    s.conn = std::make_unique< net::craft_async_conn >(host_, port_, wire::framed_body_max(max_tx_, lba_), q);
#ifndef NDEBUG
    s.owner = std::this_thread::get_id();
#endif
    n_slots_.store(m + 1, std::memory_order_release); // publish AFTER the slot is complete
    return s.conn.get();
}

void CraftTcpReplica::shutdown() {
    // WIRE + SERVER time, per op class -- everything craft_client does is ABOVE this proxy and excluded. If the
    // read average here tracks the driver's read latency, the client is WAITING and the cost is the wire or the
    // server; if it does not, the gap is being burned above us and belongs to a profiler.
#ifndef NDEBUG
    auto const dump = [this](char const* what, rt_stat const& s) {
        auto const n = s.count.load(std::memory_order_relaxed);
        if (n == 0) return;
        double const avg_us = static_cast< double >(s.total_ns.load(std::memory_order_relaxed)) / n / 1000.0;
        double const max_us = static_cast< double >(s.max_ns.load(std::memory_order_relaxed)) / 1000.0;
        LOGINFO("[peer_id: {}] craft_rt {}:{} {:<9} n={:<8} avg={:8.2f}us  max={:9.2f}us", boost::uuids::to_string(id_),
                host_, port_, what, n, avg_us, max_us);
    };
    dump("read", rt_read_);
    dump("write", rt_write_);
    dump("keepalive", rt_keepalive_);
    // Aggregate the pump stats across the whole per-ring conn grid. shutdown() runs post-quiesce (every ring
    // exited), so the plain per-conn counters are safe to read from here.
    uint64_t recvs = 0, replies = 0, recv_bytes = 0;
    for (std::size_t i = 0, n = n_slots_.load(std::memory_order_acquire); i < n; ++i) {
        auto const& c = slots_[i].conn;
        if (!c) continue;
        recvs += c->n_recv();
        replies += c->n_reply();
        recv_bytes += c->n_recv_bytes();
    }
    if (replies > 0 && recvs > 0) {
        LOGINFO(
            "[peer_id: {}] craft_rt {}:{} pump      recvs={} replies={} recvs/reply={:.2f} avg_recv={:.0f}B queues={}",
            boost::uuids::to_string(id_), host_, port_, recvs, replies,
            static_cast< double >(recvs) / static_cast< double >(replies),
            static_cast< double >(recv_bytes) / static_cast< double >(recvs), n_slots_.load(std::memory_order_relaxed));
    }
#endif
    {
        std::lock_guard< std::mutex > g{mu_};
        stop_ = true;
    }
    // Fence the shared session-mgr queue: any hop that saw !stop_ posted (under mu_) before the flip above, so
    // once the fence runs every previously hopped op has finished its round-trip. On the mgr thread itself the
    // fence is skipped -- the running job IS the drain point (the old self-join guard, with no thread to join).
    mgr_->drain();
}

CraftTcpReplica::~CraftTcpReplica() { shutdown(); }

std::shared_ptr< CraftTcpReplica::hop_event > CraftTcpReplica::hop() {
    auto ev = std::make_shared< hop_event >();
    {
        std::lock_guard< std::mutex > g{mu_};
        if (!stop_) {
            mgr_->post([ev] { ev->complete({}); }); // the mgr thread resumes the coroutine there
            return ev;
        }
    }
    ev->complete({}); // shutting down: complete inline so the coroutine is never stranded
    return ev;
}

bool CraftTcpReplica::ensure_connected() {
    if (connected_) return true;
    // The handshake deadline: op_timeout_ when set (it bounds a full round-trip, so certainly a SYN), else the
    // default -- never unbounded, since this runs on the shared session-mgr thread where a blackholed peer
    // would stall every proxy's admin plane, not just this one's.
    auto const cto = (op_timeout_ > std::chrono::milliseconds{0}) ? op_timeout_ : net::k_connect_timeout;
    auto c = net::wire_client::connect(host_, port_, cto);
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
    // Resetting the WHOLE connection on a per-op timeout is a SHIM artifact, not target semantics. This proxy's
    // blocking ops are strictly serial (one op in flight, on the session-mgr thread), so a timeout can only
    // strand that single op's reply --
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

std::optional< std::error_condition > CraftTcpReplica::ensure_bound(uint64_t term, uint64_t client_token) {
    if (!ensure_connected()) return make_error_condition(craft_error::REPLICA_DOWN);
    // Re-HELO whenever the requested term differs from what this connection is bound at -- not just on the first
    // IO. A logout+relogin bumps the session term (leader run_login: ++term_), and the wire carries no per-op
    // term (it is a connection constant), so a connection still bound at the old term would have the server
    // stamp that stale term on every IO and fence it. Binding is idempotent server-side, so re-HELO rebinds
    // this same connection to the new term. (A post-timeout reset drops bound_, so that path re-HELOs too.)
    if (bound_ && term == bound_term_) return std::nullopt;
    LOGDEBUG("[peer_id: {}] sending helo, vol id {}, client token {}, term {}", boost::uuids::to_string(id_),
             fmt::format("{:02x}", fmt::join(vol_id_, "")), client_token, term);
    auto h = conn_.helo(vol_id_, client_token, term);
    if (!h) return on_net_fault(h.error());
    if (*h != wire::status::ok) return status_to_error(*h);
    bound_ = true;
    bound_term_ = term;
    return std::nullopt;
}

// ── client-facing ──

async_result< LoginResult > CraftTcpReplica::login(uint64_t client_token) {
    auto ev = hop();
    co_await *ev; // now on the session-mgr thread
    if (!ensure_connected()) co_return fail(craft_error::REPLICA_DOWN);
    auto r = conn_.login(vol_id_, client_token); // LOGIN names the volume, exactly as HELO does
    if (!r) co_return std::unexpected(on_net_fault(r.error()));
    if (r->max_tx) max_tx_ = r->max_tx;  // the volume's max transfer PAYLOAD (login_rsp)
    if (r->lba_size) lba_ = r->lba_size; // block size; with max_tx_ it sizes the on-ring parse bound (framed_body_max)

    if (r->term > 0) { // a successful login binds this (leader) connection at its term; a redirect does not
        bound_ = true;
        bound_term_ = r->term;
    }
    LOGINFO("[peer_id: {}] received login response from the server, client_token {}, max_tx {}, lba_size {}, term {}",
            boost::uuids::to_string(id_), client_token, max_tx_, lba_, bound_term_);
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
    co_return sisl::ok();
}

async_result< lsn_pair > CraftTcpReplica::write(::io_uring* q, client_hdr hdr, int64_t dlsn, uint64_t addr,
                                                uint64_t len,
                                                sisl::sg_list data) {
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
    // ── on-ring data path: this queue's conn, lazily HELO'd at hdr.term, sends over the caller's ring ──
    if (auto* conn = conn_for(q)) {
        if (auto e = co_await conn->ensure_ready(vol_id_, hdr.client_token, hdr.term); !e)
            co_return std::unexpected(net_to_error(e.error()));
        auto const t0 = std::chrono::steady_clock::now();
        auto r = co_await conn->write(dlsn, addr, len, payload, hdr.commit_lsn, hdr.all_committed_lsn);
        rt_write_.add(ns_since(t0));
        if (!r) co_return std::unexpected(net_to_error(r.error()));
        if (r->status != wire::status::ok) co_return std::unexpected(status_to_error(r->status));
        co_return lsn_pair{r->commit_lsn, r->last_append_lsn}; // the reply's piggybacked watermarks
    }

    auto ev = hop();
    co_await *ev;
    if (auto e = ensure_bound(hdr.term, hdr.client_token)) co_return std::unexpected(*e);
    auto r = conn_.write(dlsn, addr, len, payload, hdr.commit_lsn, hdr.all_committed_lsn);
    if (!r) co_return std::unexpected(on_net_fault(r.error()));
    if (r->status != wire::status::ok) co_return std::unexpected(status_to_error(r->status));
    co_return lsn_pair{r->commit_lsn, r->last_append_lsn};
}

async_result< read_result > CraftTcpReplica::read(::io_uring* q, client_hdr hdr, int64_t read_lsn, uint64_t addr,
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
    if (auto* conn = conn_for(q)) { // ── on-ring data path ──
        if (auto e = co_await conn->ensure_ready(vol_id_, hdr.client_token, hdr.term); !e)
            co_return std::unexpected(net_to_error(e.error()));
        auto const t0 = std::chrono::steady_clock::now();
        auto r = co_await conn->read(read_lsn, addr, len, d, hdr.commit_lsn, hdr.all_committed_lsn);
        rt_read_.add(ns_since(t0));
        if (!r) co_return std::unexpected(net_to_error(r.error()));
        reply = std::move(*r);
    } else {
        auto ev = hop();
        co_await *ev;
        if (auto e = ensure_bound(hdr.term, hdr.client_token)) co_return std::unexpected(*e);
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
    co_return read_result{to_io_extents(reply.extents), lsn_pair{reply.commit_lsn, reply.last_append_lsn}};
}

async_result< lsn_pair > CraftTcpReplica::keep_alive(::io_uring* q, client_hdr hdr) {
    if (auto* conn = conn_for(q)) { // ── on-ring data path ──
        LOGDEBUG("[PEER_ID: {}] sending ensure ready, vol id {}, client token {}, term {}",
                 boost::uuids::to_string(id_), fmt::format("{:02x}", fmt::join(vol_id_, "")), hdr.client_token,
                 hdr.term);
        if (auto e = co_await conn->ensure_ready(vol_id_, hdr.client_token, hdr.term); !e)
            co_return std::unexpected(net_to_error(e.error()));
        auto const t0 = std::chrono::steady_clock::now();
        auto r = co_await conn->keep_alive(hdr.commit_lsn, hdr.all_committed_lsn);
        rt_keepalive_.add(ns_since(t0));
        if (!r) co_return std::unexpected(net_to_error(r.error()));
        if (r->status != wire::status::ok) co_return std::unexpected(status_to_error(r->status));
        co_return lsn_pair{r->commit_lsn, r->last_append_lsn};
    }

    auto ev = hop();
    co_await *ev;
    if (auto e = ensure_bound(hdr.term, hdr.client_token)) co_return std::unexpected(*e);
    auto r = conn_.keep_alive(hdr.commit_lsn, hdr.all_committed_lsn);
    if (!r) co_return std::unexpected(on_net_fault(r.error()));
    if (r->status != wire::status::ok) co_return std::unexpected(status_to_error(r->status));
    co_return lsn_pair{r->commit_lsn, r->last_append_lsn};
}

async_result< resolution_result > CraftTcpReplica::request_resolution(::io_uring* q, client_hdr hdr, int64_t upto) {
    // On-ring: this fires from the write path's failure branch, i.e. FROM a queue thread in async mode, so it
    // must not hop to a blocking round-trip. A round is slow leader work, but the pump demuxes replies by
    // request_id, so the parked leg costs the data ops in flight nothing.
    if (auto* conn = conn_for(q)) {
        if (auto e = co_await conn->ensure_ready(vol_id_, hdr.client_token, hdr.term); !e)
            co_return std::unexpected(net_to_error(e.error()));
        auto r = co_await conn->resolve(upto, hdr.commit_lsn, hdr.all_committed_lsn);
        if (!r) co_return std::unexpected(net_to_error(r.error()));
        if (r->status != wire::status::ok) co_return std::unexpected(status_to_error(r->status));
        co_return resolution_result{r->resolved_upto, std::move(r->empty_slots)};
    }

    // No-ring tier: the blocking session-mgr path, like login/logout.
    auto ev = hop();
    co_await *ev;
    if (auto e = ensure_bound(hdr.term, hdr.client_token)) co_return std::unexpected(*e);
    auto r = conn_.resolve(upto, hdr.commit_lsn, hdr.all_committed_lsn);
    if (!r) co_return std::unexpected(on_net_fault(r.error()));
    if (r->status != wire::status::ok) co_return std::unexpected(status_to_error(r->status));
    co_return resolution_result{r->resolved_upto, std::move(r->empty_slots)};
}

// ── peer-facing: a client never invokes these; stubbed so the vtable is complete ──

} // namespace craft
