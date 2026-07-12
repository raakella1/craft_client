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
// craft_tcp_client. It is the initiator: craft_client holds N of these (or N MemCraftReplicas for the no-wire
// tier) and never knows the difference, exactly like a RAID1/iSCSI split. Every wire reply is mapped here to
// the homeblocks domain result (lsn_pair / io_extent / craft_error); the client speaks only the interface.
//
// CONCURRENCY BRIDGE. craft_client fans a write out to every replica and acks at QUORUM, leaving stragglers
// detached -- so the per-replica ops must run concurrently. craft_tcp_client is blocking, so each op hops onto
// this proxy's own worker thread (via a shared_awaitable, exactly as MemTransport::after does), runs the
// blocking socket round-trip there, and resumes the awaiting coroutine on completion. One worker per proxy =
// one connection driven serially, N proxies = N connections in flight. This is the shim model (never
// benchmarked); a single-threaded async io_uring transport drops in behind this same interface later.
//
// Homeblocks-coupled by construction (it implements craft_replica); it is the one client-side coupled adapter,
// the mirror of craft_tcp_server on the far end.

#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include <sisl/async/shared_awaitable.hpp>

#include <craft/replica.hpp> // the craft_replica interface + async_result/async_status + domain types
#include <craft/net/tcp_client.hpp>

namespace craft {

namespace net {
class craft_async_conn; // the on-ring data path (src/net/async_conn.hpp); held behind a unique_ptr, opened lazily
}

class CraftTcpReplica final : public craft_replica {
public:
    // Address the replica at host:port; `id` is its endpoint id (routing), `volume_id` is what HELO presents
    // to bind a follower connection to the session. `op_timeout` (0 = none) bounds every reply wait -- past it
    // an op returns timed_out and the connection is reset (reconnect + re-HELO on the next op). Nothing
    // connects until the first op (on the worker).
    CraftTcpReplica(std::string host, uint16_t port, peer_id_t id, std::array< uint8_t, 16 > volume_id,
                    std::chrono::milliseconds op_timeout = std::chrono::milliseconds{0});
    ~CraftTcpReplica() override;

    CraftTcpReplica(CraftTcpReplica const&) = delete;
    CraftTcpReplica& operator=(CraftTcpReplica const&) = delete;

    // Stop and JOIN the worker thread. MUST be called from a non-worker thread (the harness does, from the
    // main thread) BEFORE this proxy can be dropped: a detached keep_alive can hold the last volume_handle
    // and, when it completes on the worker, destroy this proxy ON the worker -- so ~ must never be the one to
    // join (it would join itself: "Resource deadlock avoided"). Idempotent; also called from ~ as a backstop.
    void shutdown();

    // ── craft_replica: client-facing ──
    async_result< LoginResult > login(uint64_t client_token) override;
    async_status logout(client_hdr hdr) override;
    async_status write(client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len, sisl::sg_list data) override;
    async_result< std::vector< io_extent > > read(client_hdr hdr, int64_t read_lsn, uint64_t addr, uint64_t len,
                                                  sisl::sg_list dest) override;
    async_result< lsn_pair > keep_alive(client_hdr hdr) override;

    // Prime the ON-RING data path: store the caller's ring. Just a pointer -- raw fds, no IOSQE_FIXED_FILE, so the
    // data connection (craft_async_conn) is opened lazily on it at the first write/read/keep_alive and reconnected
    // at will. login/logout stay on the blocking worker path (they ran before any ring existed, to yield
    // lba/capacity/term); only the data path moves onto the ring.
    void prepare_for_async(::io_uring* ring) noexcept override { ring_ = ring; }

    // ── craft_replica: peer-facing (server-to-server; a client never invokes these) -- stubbed NOT_LEADER ──
    async_result< lsn_pair > get_lsns() override;
    async_result< lsn_pair > get_rs_commit_lsn() override;
    async_result< std::vector< JournalSlot > > fetch_data(std::vector< int64_t > lsns) override;
    async_status truncate(int64_t lsn) override;
    peer_id_t id() const override { return id_; }

private:
    using hop_event = sisl::async::shared_awaitable< std::monostate >;
    // Post a resume onto the worker thread and return the event to co_await. After the await, the coroutine is
    // running on the worker, so its blocking socket work is off the caller's thread.
    std::shared_ptr< hop_event > hop();
    void worker_loop();

    // Worker-thread-only state (every op runs serially on the worker, so no lock guards these).
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

    net::craft_tcp_client conn_; // touched ONLY on the worker thread (login/logout: the blocking admin path)

    // The ON-RING data path (prepare_for_async): the caller's ring + a lazily-opened async connection over it.
    // Null ring_ => not primed => write/read/keep_alive take the blocking conn_ path above. Opened/torn down on
    // the caller's (queue) thread, so no lock; its dtor is defined in the .cpp where craft_async_conn is complete.
    ::io_uring* ring_{nullptr};
    std::unique_ptr< net::craft_async_conn > aconn_;
    uint32_t max_tx_{
        wire::k_default_max_tx}; // the volume max transfer PAYLOAD; a safe ceiling until login learns it from login_rsp
    uint32_t lba_{0};            // the volume block size (login_rsp); with max_tx_ it sizes the on-ring parse bound
                                 // (wire::framed_body_max: payload + a read reply's extent table)
    bool connected_{false};
    bool bound_{false};
    uint64_t bound_term_{0}; // the session term this connection is bound at; a new term forces a re-HELO

    // The per-proxy worker: a single thread draining a job queue (mirrors MemTransport's replica_service).
    std::thread worker_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::deque< std::function< void() > > jobs_;
    bool stop_{false};
};

} // namespace craft
