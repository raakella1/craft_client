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

// In-memory, HomeStore-free implementation of ONE CRAFT replica device -- the SERVER the reference client is
// built against. It owns an in-memory data journal (dLSN -> slot), an applied LBA index, and derives the
// journal-tail overlay on demand from the unapplied journal tail. All real work is synchronous under a
// per-replica mutex. NOT crash-resilient and NOT performant by design.
//
// The client-facing methods (write/read/keep_alive) are 1-line delegations across the wire, MemTransport
// (mem_craft_cluster.hpp), the in-process stand-in for the network. Everything a network owns lives there and
// dies with it: payload ownership (the one byte copy), deliverability (REPLICA_DOWN == "never delivered"),
// injected latency and the op deadline. Peer-to-peer concerns (leader election, login orchestration, resync,
// fault injection) live there too. Nothing in this file copies payload bytes.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <craft/client.hpp>  // result types
#include "craft_peer.hpp"    // the PEER plane: craft_peer + JournalSlot + lba_t (this model is its only implementer)
#include "craft_replica.hpp" // the CLIENT plane: the craft_replica interface

namespace craft {

class MemTransport; // in-process network + cold path
class RaftReplica;  // raft based replica server
class Watchdog;     // the keepalive and login watchdog

using sisl::ok;
template < typename T >
using result = sisl::result< T >;
using status = sisl::status;

// Per-partition CRAFT state, internal to a replica implementation. Authoritative in memory; a production replica
// recovers it from the journal + superblock on restart (this model does not). Not on either plane's interface --
// it is one implementation's state, which is why it lives here and HomeBlocks keeps its own copy.
struct CraftPartitionState {
    int64_t commit_lsn{-1};      // contiguous committed prefix (== Synced)
    int64_t last_append_lsn{-1}; // highest appended dLSN (may be uncommitted)
    uint64_t client_token{0};    // token from the last successful InternalLogin
    uint64_t term{0};            // current session term
};

// A read-only snapshot of one replica's CRAFT state, for observability (the craft_ublk REST endpoint and
// the model unit test). Pure data: no HomeStore, no JSON, nothing on the protocol path reads it.
//
// `missing_count` counts Missing slots: dLSNs in (commit_lsn, last_append_lsn] that this replica has no
// journal entry for. That is precisely the set apply_up_to() stalls on, so a non-zero count explains a
// pinned commit_lsn. NOTE it stays 0 when a replica misses a *suffix* of writes (last_append_lsn simply
// never advances), which is the common sub-quorum case; compare the LSNs against the client's watermarks
// to see that kind of lag. A Missing slot is NOT a hole (a hole is a resolved reads-as-zero range).
struct replica_stats {
    peer_id_t id{};
    std::string addr;
    uint32_t page_size{0};

    // CraftPartitionState
    int64_t commit_lsn{-1};
    int64_t last_append_lsn{-1};
    uint64_t term{0};
    uint64_t client_token{0};

    // data journal
    std::size_t journal_slots{0};
    int64_t journal_first_dlsn{-1};
    int64_t journal_last_dlsn{-1};
    std::size_t zero_write_slots{0}; // all_zeros (WRITE_ZEROES); allocates nothing
    std::size_t empty_slots{0};      // is_empty (the Empty verdict; skipped on apply)
    uint64_t journal_data_bytes{0};  // sum(len) * page_size over data slots

    std::size_t missing_count{0};
    std::vector< int64_t > missing_sample; // ascending, capped at k_missing_sample

    std::size_t mapped_blocks{0}; // index_.size(); coverage of the applied prefix
};

// One replica's fault/latency knobs, in an immutable snapshot the IO path reads with a single acquire load
// and NO lock (up / write_ok / delay are consulted several times per IO). This is the per-replica successor to
// the transport's old central fault_state: the knobs are a property of the REPLICA the test holds (matching how
// a real backend will do it -- there is no separate transport to reach for), so MemTransport merely consults
// the target replica. Mutation is copy-on-write, serialized by fault_mu_ and confined to control paths (tests,
// the CLI, the transport's forwarders). A superseded snapshot is RETIRED, never freed, so an op holding a
// pointer to it mid-flight can never dangle. `op_timeout` is NOT here: it is the client's deadline, set-wide on
// the transport, not a property of a replica.
struct replica_faults {
    bool up{true};                      // reachable; false => REPLICA_DOWN ("never delivered")
    bool write_ok{true};                // false => this replica drops writes (the sub-quorum knob)
    std::chrono::milliseconds delay{0}; // injected network latency to this replica
};

// enable_shared_from_this: a write the transport timed out is delivered late, from the transport's timer
// thread. That closure must hold a WEAK reference here (a strong one would cycle: replica -> net_ -> closure
// -> replica), so the replica must be reachable as a shared_ptr. It always is; make_mem_replica_group is the
// only constructor caller and it uses make_shared.
// Implements BOTH planes -- and is currently the only thing that implements the peer plane at all. That is not an
// accident of the model: a real replica is exactly the thing that can answer both "serve this client's read" and
// "hand a peer the journal slot it is Missing". A client-side transport proxy (CraftTcpReplica) implements only
// craft_replica, because a client never asks a peer question.
class MemCraftReplica : public craft_replica,
                        public craft_peer,
                        public std::enable_shared_from_this< MemCraftReplica > {
public:
    // How many Missing dLSNs stats() lists individually. The count is always exact.
    static constexpr std::size_t k_missing_sample = 16;

    MemCraftReplica(replica_endpoint ep, uint32_t page_size, std::shared_ptr< MemTransport > net);

    // Snapshot this replica's state. Takes mu_ and deliberately does NOT consult net_: do_write() locks
    // the transport before mu_, so reading net_ under mu_ here would invert that order. Callers that want
    // liveness (is_up / write_allowed) ask the transport themselves.
    replica_stats stats() const;

    // Test observability: reads this replica has served (see reads_served_).
    std::size_t reads_served() const { return reads_served_.load(std::memory_order_relaxed); }
    // Test observability: keep_alives this replica has answered (the client's timer-less liveness drive).
    std::size_t keepalives_served() const { return keepalives_served_.load(std::memory_order_relaxed); }

    // ── fault injection (model / test knobs; the production craft_replica has none) ──
    // Set on the replica the test holds, so no transport is needed to make a peer look down / slow / write-
    // dropping. Copy-on-write; a concurrent IO reading the old snapshot stays valid. The transport's
    // set_up/set_delay/force_subquorum forward here, so either surface works.
    void set_up(bool up);                        // reachability (false => REPLICA_DOWN)
    void set_delay(std::chrono::milliseconds d); // injected latency (0 removes it)
    void drop_writes(bool drop);                 // sub-quorum: true => refuse incoming writes
    void clear_faults();                         // back to up / accepting / no delay
    // Lock-free readers (the transport's forwarders + observability use these; the IO path reads one snapshot).
    bool is_up() const;
    bool write_allowed() const;
    std::chrono::milliseconds delay() const;

    // ── craft_replica: client-facing ──
    // A verb's leading `q` binds THAT op's delivery timer to the caller's ring (a SQE the ring owner's reap
    // loop completes -- many legs in flight at once, QD>1, on the caller's thread) instead of a MemTransport
    // pool hop. Null q keeps the pool path -- where a verb's awaiter RESUMES ON THE REPLICA'S POOL THREAD
    // (freestanding tasks resume inline at completion). The CALLER owns the reap loop: with a non-null q it
    // MUST drain the ring's CQEs (dispatch each managed one via sisl::async::complete_cqe_state) -- including
    // the detached straggler legs -- or the submitted SQEs never fire and the ops hang.
    async_result< LoginResult > login(uint64_t client_token) override;
    async_status logout(client_hdr hdr) override;
    async_result< lsn_pair > write(::io_uring* q, client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
                                   sisl::sg_list data) override;
    async_result< read_result > read(::io_uring* q, client_hdr hdr, int64_t read_lsn, uint64_t addr, uint64_t len,
                                     sisl::sg_list dest) override;
    async_result< lsn_pair > keep_alive(::io_uring* q, client_hdr hdr) override;
    // The client-requested resolution round: term-fenced, then delegated to the transport's cold path
    // (leader-only; the model's stand-in for the leader's SyncRSCommitLSN pre-resolution).
    async_result< resolution_result > request_resolution(::io_uring* q, client_hdr hdr, int64_t upto) override;

    // ── craft_peer: the PEER plane (a holder answering another replica, never a client) ──
    // NOTE these are not yet reached THROUGH craft_peer: MemTransport's cold path (run_login / run_resolution) is
    // a friend and drives the cold_* / peek_* helpers below directly. Routing it through this interface is step
    // one of making the peer plane real; step two is allocating its opcodes (wire::op stops at 14).
    async_result< lsn_pair > get_rs_commit_lsn(uint64_t term, bool is_login) override;
    async_result< std::vector< JournalSlot > > fetch_data(std::vector< int64_t > lsns) override;

    peer_id_t id() const override { return ep_.id; } // craft_replica

    // ── local-server surface: drive this replica directly, with an EXTERNAL transport (the TCP frontend,
    // craft_tcp_server, or any real network) as the wire. Each wraps a synchronous core WITHOUT a
    // MemTransport hop: deliverability, latency and payload ownership are the external transport's job now,
    // so these do none of it (srv_write adopts a payload the transport already owns; bytes==nullptr is a
    // zero write). A replica built with a null transport (net_ == nullptr) serves EXCLUSIVELY through
    // these -- its public craft_replica methods above require net_ and are unused. This is the seam the
    // roadmap's "reuse MemCraftReplica" rides on: the journal / index / apply / fencing, minus the model
    // network. ──
    result< lsn_pair > srv_write(client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
                                 std::shared_ptr< std::vector< uint8_t > > bytes) {
        return do_write(hdr, dlsn, addr, len, std::move(bytes));
    }
    result< read_result > srv_read(client_hdr hdr, int64_t read_lsn, uint64_t addr, uint64_t len, sisl::sg_list dest) {
        return do_read(hdr, read_lsn, addr, len, std::move(dest));
    }
    result< lsn_pair > srv_keep_alive(client_hdr hdr) { return do_keep_alive(hdr); }
    // The standalone (one-process = one-replica) resolution round: itself lacking a slot IS the quorum-lacks
    // evidence at N=1, so every hole <= upto is verdicted Empty and the frontier advances through it.
    result< resolution_result > srv_resolve(client_hdr hdr, int64_t upto) { return do_resolve_local(hdr, upto); }
    void srv_establish(uint64_t client_token, uint64_t term) { cold_apply_login(client_token, term); }
    void srv_end() { cold_apply_logout(); }
    lsn_pair srv_lsns() { return peek_lsns(); }

private:
    friend class MemTransport; // the cold path drives the cold_* / peek helpers below directly, and the IO
                               // path (send_*) reads fault_snapshot() to decide deliverability / latency
    friend class RaftReplica;  // tcp server backed replica that uses the MemCraftReplica as the foundation

    // The IO path's view of this replica's faults: one acquire load, no lock. Hold the returned pointer for the
    // whole of one op so its checks (up / write_ok / delay) see a consistent snapshot (a superseded one is
    // retired, never freed).
    replica_faults const* fault_snapshot() const { return faults_.load(std::memory_order_acquire); }
    // Copy-on-write: build the successor under fault_mu_, retire the old one so readers stay valid, publish with
    // a release store. Only the public knob setters call this.
    template < class Fn >
    void mutate_faults(Fn&& fn);

    struct MemJournalSlot {
        uint64_t term{0};
        lba_t lba{0};
        lba_count_t len{0};
        bool all_zeros{false};
        bool is_empty{false};                            // Empty verdict (resync seam)
        std::shared_ptr< std::vector< uint8_t > > bytes; // len*page_size bytes; null iff all_zeros/empty
    };
    struct IndexCell {
        int64_t dlsn{-1};
        std::shared_ptr< std::vector< uint8_t > > buf; // one page at buf->data()+off
        std::size_t off{0};
    };

    // Synchronous cores: the SERVER. Each takes mu_. Deliverability, latency and payload ownership are the
    // transport's job (MemTransport::send_*), which is why nothing below consults net_ or copies bytes.
    // do_write takes the payload already owned and adopts it; `bytes == nullptr` is a zero write.
    result< lsn_pair > do_write(client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
                                std::shared_ptr< std::vector< uint8_t > > bytes);
    result< read_result > do_read(client_hdr hdr, int64_t read_lsn, uint64_t addr, uint64_t len, sisl::sg_list dest);
    result< lsn_pair > do_keep_alive(client_hdr hdr);
    result< lsn_pair > do_lsns();
    status do_truncate(int64_t lsn);
    result< resolution_result > do_resolve_local(client_hdr hdr, int64_t upto); // N=1 resolution (srv seam)
protected:
    result< std::vector< JournalSlot > > do_fetch(std::vector< int64_t > const& lsns);

private:
    // ── on-ring transport (a verb's non-null `q`) ──
    // ring_delay suspends the calling leg on a timeout/nop SQE placed on `q`; the reap loop's
    // complete_cqe_state resumes it. The ring is a parameter, not a member: each leg rides the ring its verb
    // was called with, for the leg's whole life (a leg never leaves its queue). late_write is the detached
    // straggler leg: a write whose delay ran past the client deadline lands here, late, after its own ring
    // timer -- exactly the arrival that leaves a Missing slot behind at QD>1. `this` outlives it because the
    // driver drains every ring timer before teardown.
    async_status ring_delay(::io_uring* q, std::chrono::milliseconds d);
    async_status late_write(::io_uring* q, client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
                            std::shared_ptr< std::vector< uint8_t > > bytes, std::chrono::milliseconds deliver);

    // helpers (mu_ held by caller)

    void apply_up_to(int64_t target);
    void apply_slot(int64_t dlsn, MemJournalSlot const& s);
    // Fill `dest` for byte range [addr,addr+len) (data -> bytes, holes -> zeros) and return the layout.
    std::vector< io_extent > read_range(int64_t H, uint64_t addr, uint64_t len, sisl::sg_list const& dest);
    MemJournalSlot const* highest_slot_le(lba_t x, int64_t H) const; // journal-tail slot, honoring the horizon clamp

    // cold-path hooks used by MemTransport (each takes mu_)
    lsn_pair peek_lsns();
    void cold_apply_sync(int64_t rs_commit_lsn, uint64_t client_token);
    void cold_apply_logout();

    // resolution-round hooks used by MemTransport::run_resolution (each takes mu_). A fetched copy shares the
    // holder's bytes buffer (immutable once appended), so a fill copies no payload.
    std::optional< MemJournalSlot > peek_slot(int64_t dlsn); // copy of the slot, or nullopt if absent

    void cold_mark_empty(int64_t dlsn);                // Empty verdict tombstone; overwrites held
                                                       // data (reconciliation: Empty beats data)
    std::vector< int64_t > peek_empties(int64_t upto); // every is_empty dLSN <= upto

protected:
    void cold_apply_login(uint64_t client_token, uint64_t term);
    void cold_truncate_above(int64_t rs_commit_lsn);
    void cold_install_slot(int64_t dlsn, MemJournalSlot s); // fill a hole; never overwrites an entry

    // Test observability: how many reads this replica actually served. Lets a test witness read routing
    // (e.g. round-robin distribution across members). Not part of the CRAFT surface.
    std::atomic< std::size_t > reads_served_{0};
    std::atomic< std::size_t > keepalives_served_{0};

    // Per-replica fault/latency knobs (lock-free read on the IO path; COW mutation on control paths). `faults_`
    // is the published snapshot; `fault_retired_` owns every snapshot ever published so a reader can hold a raw
    // pointer across a suspension without a refcount. Separate from mu_ on purpose: the IO path checks faults
    // WITHOUT taking the journal mutex.
    std::atomic< replica_faults const* > faults_{nullptr};
    std::mutex fault_mu_;                                                  // serializes mutators only
    std::vector< std::unique_ptr< replica_faults const > > fault_retired_; // guarded by fault_mu_

protected:
    replica_endpoint ep_;
    uint32_t page_size_;
    std::shared_ptr< MemTransport > net_;

    CraftPartitionState state_;
    std::map< int64_t, MemJournalSlot > journal_; // dLSN -> slot (out-of-order arrival tolerated)
    std::map< lba_t, IndexCell > index_;          // applied prefix (<= commit_lsn); an absent LBA is a hole
    mutable std::mutex mu_;
    std::shared_ptr< Watchdog > watchdog_; // the keepalive and login watchdog
};

} // namespace craft