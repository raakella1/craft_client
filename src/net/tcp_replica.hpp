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
#pragma once

// CraftTcpReplica: the client-side transport adapter -- a craft_replica implemented over the wire-only
// wire_client. It is the initiator: craft_client holds N of these (or N MemCraftReplicas for the no-wire
// tier) and never knows the difference, exactly like a RAID1/iSCSI split. Every wire reply is mapped here to
// the homeblocks domain result (lsn_pair / io_extent / craft_error); the client speaks only the interface.
//
// CONCURRENCY BRIDGE. wire_client is blocking, so each blocking-path op hops onto the process-wide CRAFT
// session-mgr thread (net::craft_session_mgr, via a shared_awaitable exactly as MemTransport::after does),
// runs the socket round-trip there, and resumes the awaiting coroutine on completion. ONE admin thread for
// every proxy in the process -- a 50-disk RAID0 at N=3 idles one thread, not 150. Once the ring is primed
// that thread's whole job is login/logout (they bracket the ring's lifetime); on the no-ring tier every leg
// of every proxy serializes on it, so ack-at-quorum degrades to FIFO completion order -- fine for the
// shim/test tier, irrelevant on-ring where the mid-session verbs fan out on the caller's io_uring.
//
// Homeblocks-coupled by construction (it implements craft_replica); it is the one client-side coupled adapter,
// the mirror of craft_tcp_server on the far end.

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <vector>

#include <sisl/async/shared_awaitable.hpp>

#include "craft_replica.hpp" // the craft_replica interface + async_result/async_status + domain types
#include "net/session_mgr.hpp" // the process-wide admin thread every proxy shares
#include "net/wire_client.hpp"

namespace craft {

namespace net {
class craft_async_conn; // the on-ring data path (src/net/async_conn.hpp); held behind a unique_ptr, opened lazily
}

class CraftTcpReplica final : public craft_replica {
public:
    // Address the replica at host:port; `id` is its endpoint id (routing), `volume_id` is what HELO presents
    // to bind a follower connection to the session. `op_timeout` (0 = none) bounds every reply wait -- past it
    // an op returns timed_out and the connection is reset (reconnect + re-HELO on the next op). Nothing
    // connects until the first op (on the session-mgr thread).
    CraftTcpReplica(std::string host, uint16_t port, peer_id_t id, std::array< uint8_t, 16 > volume_id,
                    std::chrono::milliseconds op_timeout = std::chrono::milliseconds{0});
    ~CraftTcpReplica() override;

    CraftTcpReplica(CraftTcpReplica const&) = delete;
    CraftTcpReplica& operator=(CraftTcpReplica const&) = delete;

    // Drain this proxy's in-flight blocking ops: flip stop_ (later hops complete inline) and fence the shared
    // session-mgr queue -- once the fence runs, every previously hopped op has finished its round-trip. The
    // harness calls it from the main thread, volumes still alive, before dropping any proxy. Safe from ANY
    // thread: on the mgr thread itself the fence is skipped (a detached keep_alive can hold the last
    // volume_handle and destroy this proxy ON the mgr thread -- nothing joins, so nothing deadlocks).
    // Idempotent; also called from ~.
    void shutdown();

    // ── craft_replica: client-facing ──
    async_result< LoginResult > login(uint64_t client_token) override;
    async_status logout(client_hdr hdr) override;
    async_result< lsn_pair > write(client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
                                   sisl::sg_list data) override;
    async_result< read_result > read(client_hdr hdr, int64_t read_lsn, uint64_t addr, uint64_t len,
                                     sisl::sg_list dest) override;
    async_result< lsn_pair > keep_alive(client_hdr hdr) override;
    // The client-requested resolution round. It fires MID-SESSION, from the write path's failure branch -- the
    // ring thread in async mode -- so once the ring is primed it rides the data connection like keep_alive
    // (replies demux by request_id; the server handles a connection's ops concurrently, so the slow round
    // blocks nothing). Unprimed, it takes the blocking session-mgr path like login/logout.
    async_result< resolution_result > request_resolution(client_hdr hdr, int64_t upto) override;

    // Prime the ON-RING data path: store the caller's ring. Just a pointer -- raw fds, no IOSQE_FIXED_FILE, so the
    // data connection (craft_async_conn) is opened lazily on it at the first mid-session op and reconnected at
    // will. login/logout stay on the blocking session-mgr path (they bracket the ring's lifetime: login ran before any
    // ring existed, to yield lba/capacity/term); everything mid-session (write/read/keep_alive/resolve) moves
    // onto the ring.
    void prepare_for_async(::io_uring* ring) noexcept override { ring_ = ring; }

    peer_id_t id() const override { return id_; }

    // ── wire round-trip accounting (DIAGNOSTIC) ──
    // Wraps exactly the on-ring request->reply await: submit the SQE, wait for the pump to demux the reply.
    // So it measures WIRE + SERVER and nothing else -- everything craft_client does (dLSN assignment, quorum,
    // plan_read, eligible) is above this proxy and excluded. Comparing read vs write here is what splits
    // "the client is computing" from "the client is waiting", which no fio number can do on its own.
    // Dumped once per proxy at shutdown(). Relaxed atomics: off the correctness path, never read by it.
    struct rt_stat {
        std::atomic< uint64_t > count{0};
        std::atomic< uint64_t > total_ns{0};
        std::atomic< uint64_t > max_ns{0};
        void add(uint64_t ns) {
            count.fetch_add(1, std::memory_order_relaxed);
            total_ns.fetch_add(ns, std::memory_order_relaxed);
            uint64_t m = max_ns.load(std::memory_order_relaxed);
            while (ns > m && !max_ns.compare_exchange_weak(m, ns, std::memory_order_relaxed)) {}
        }
    };

    // No peer-plane verbs here, by construction: this is a CLIENT-side proxy, and the peer plane's caller is a
    // replica applying a RAFT entry (see craft_peer.hpp). It used to carry four NOT_LEADER stubs it could never
    // honor -- a proxy has no journal to hand a peer.

private:
    using hop_event = sisl::async::shared_awaitable< std::monostate >;
    // Post a resume onto the session-mgr thread and return the event to co_await. After the await, the
    // coroutine is running on the mgr thread, so its blocking socket work is off the caller's thread.
    std::shared_ptr< hop_event > hop();

    // Session-mgr-thread-only state (this proxy's blocking ops all run serially there, so no lock guards these).
    bool ensure_connected(); // lazy connect on first use
    // Lazy HELO for a follower connection (login binds the leader's). nullopt = bound; else why it could not be.
    std::optional< std::error_condition > ensure_bound(uint64_t term);
    // Map a transport fault to the domain error AND, for any connection-level fault (timed out / closed / send
    // / recv), poison this connection so the next op reconnects + re-HELOs. A timed-out recv is still pending
    // in the ring, so the socket cannot be reused as-is; the straggler ack-at-quorum path lands right here.
    std::error_condition on_net_fault(net::net_error e);

    std::string host_;
    uint16_t port_;
    peer_id_t id_;
    std::array< uint8_t, 16 > vol_id_;
    std::chrono::milliseconds op_timeout_{0}; // forwarded onto conn_ at connect; 0 = block forever

    net::wire_client conn_; // touched ONLY on the session-mgr thread (login/logout: the blocking admin path)

    // The ON-RING data path (prepare_for_async): the caller's ring + a lazily-opened async connection over it.
    // Null ring_ => not primed => write/read/keep_alive take the blocking conn_ path above. Opened/torn down on
    // the caller's (queue) thread, so no lock; its dtor is defined in the .cpp where craft_async_conn is complete.
    ::io_uring* ring_{nullptr};
    std::unique_ptr< net::craft_async_conn > aconn_;
    uint32_t max_tx_{
        wire::k_default_max_tx}; // the volume max transfer PAYLOAD; a safe ceiling until login learns it from login_rsp
    uint32_t lba_{0};            // the volume block size (login_rsp); with max_tx_ it sizes the on-ring parse bound
                                 // (wire::framed_body_max: payload + a read reply's extent table)
    rt_stat rt_read_, rt_write_, rt_keepalive_; // wire round-trip, per op class (see rt_stat)

    bool connected_{false};
    bool bound_{false};
    uint64_t bound_term_{0}; // the session term this connection is bound at; a new term forces a re-HELO

    // The process-wide admin thread every proxy shares (replaces the old per-proxy worker; the thread retires
    // when the last proxy in the process drops its ref).
    std::shared_ptr< net::craft_session_mgr > mgr_;
    std::mutex mu_;    // orders hop() against shutdown(): a hop that saw !stop_ posted before the drain fence
    bool stop_{false}; // set by shutdown(): later hops complete inline instead of posting
};

} // namespace craft
