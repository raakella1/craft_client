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
#include <string>
#include <vector>

#include <craft/replica.hpp> // craft_replica interface + CRAFT data types

namespace craft {

class MemTransport; // in-process network + cold path

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
class MemCraftReplica final : public craft_replica, public std::enable_shared_from_this< MemCraftReplica > {
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
    async_result< LoginResult > login(uint64_t client_token) override;
    async_status logout(client_hdr hdr) override;
    async_status write(client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len, sisl::sg_list data) override;
    async_result< std::vector< io_extent > > read(client_hdr hdr, int64_t read_lsn, uint64_t addr, uint64_t len,
                                                  sisl::sg_list dest) override;
    async_result< lsn_pair > keep_alive(client_hdr hdr) override;

    // On-ring data path: bind write/read/keep_alive to `ring` so their delivery timer is a ring SQE the ring
    // owner's reap loop completes (many legs in flight at once, QD>1, on the caller's thread) instead of a
    // MemTransport pool hop. A null ring (the default) keeps the existing pool path. The CALLER owns the reap
    // loop: after this, it MUST drain the ring's CQEs (dispatch each managed one via
    // sisl::async::complete_cqe_state) -- including the detached straggler legs -- or the submitted SQEs never
    // fire and the ops hang. See craft_replica::prepare_for_async.
    void prepare_for_async(::io_uring* ring) noexcept override;

    // ── craft_replica: peer-facing (driven by MemTransport) ──
    async_result< lsn_pair > get_lsns() override;
    async_result< lsn_pair > get_rs_commit_lsn() override;
    async_result< std::vector< JournalSlot > > fetch_data(std::vector< int64_t > lsns) override;
    async_status truncate(int64_t lsn) override;
    peer_id_t id() const override { return ep_.id; }

    // ── local-server surface: drive this replica directly, with an EXTERNAL transport (the TCP frontend,
    // craft_tcp_server, or any real network) as the wire. Each wraps a synchronous core WITHOUT a
    // MemTransport hop: deliverability, latency and payload ownership are the external transport's job now,
    // so these do none of it (srv_write adopts a payload the transport already owns; bytes==nullptr is a
    // zero write). A replica built with a null transport (net_ == nullptr) serves EXCLUSIVELY through
    // these -- its public craft_replica methods above require net_ and are unused. This is the seam the
    // roadmap's "reuse MemCraftReplica" rides on: the journal / index / apply / fencing, minus the model
    // network. ──
    status srv_write(client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
                     std::shared_ptr< std::vector< uint8_t > > bytes) {
        return do_write(hdr, dlsn, addr, len, std::move(bytes));
    }
    result< std::vector< io_extent > > srv_read(client_hdr hdr, int64_t read_lsn, uint64_t addr, uint64_t len,
                                                sisl::sg_list dest) {
        return do_read(hdr, read_lsn, addr, len, std::move(dest));
    }
    result< lsn_pair > srv_keep_alive(client_hdr hdr) { return do_keep_alive(hdr); }
    void srv_establish(uint64_t client_token, uint64_t term) { cold_apply_login(client_token, term); }
    void srv_end() { cold_apply_logout(); }
    lsn_pair srv_lsns() { return peek_lsns(); }

private:
    friend class MemTransport; // the cold path drives the cold_* / peek helpers below directly, and the IO
                               // path (send_*) reads fault_snapshot() to decide deliverability / latency

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
    status do_write(client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
                    std::shared_ptr< std::vector< uint8_t > > bytes);
    result< std::vector< io_extent > > do_read(client_hdr hdr, int64_t read_lsn, uint64_t addr, uint64_t len,
                                               sisl::sg_list dest);
    result< lsn_pair > do_keep_alive(client_hdr hdr);
    result< lsn_pair > do_lsns();
    status do_truncate(int64_t lsn);
    result< std::vector< JournalSlot > > do_fetch(std::vector< int64_t > const& lsns);

    // ── on-ring transport (prepare_for_async) ──
    // ring_delay suspends the calling leg on a timeout/nop SQE placed on ring_; the reap loop's
    // complete_cqe_state resumes it. late_write is the detached straggler leg: a write whose delay ran past the
    // client deadline lands here, late, after its own ring timer -- exactly the arrival that leaves a Missing
    // slot behind at QD>1. `this` outlives it because the driver drains every ring timer before teardown.
    async_status ring_delay(std::chrono::milliseconds d);
    async_status late_write(client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
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
    void cold_apply_login(uint64_t client_token, uint64_t term);
    void cold_apply_logout();
    void cold_truncate_above(int64_t rs_commit_lsn);

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

    replica_endpoint ep_;
    uint32_t page_size_;
    std::shared_ptr< MemTransport > net_;
    ::io_uring* ring_{nullptr}; // prepare_for_async: the driver-owned ring the on-ring data path submits on

    CraftPartitionState state_;
    std::map< int64_t, MemJournalSlot > journal_; // dLSN -> slot (out-of-order arrival tolerated)
    std::map< lba_t, IndexCell > index_;          // applied prefix (<= commit_lsn); an absent LBA is a hole
    mutable std::mutex mu_;
};

} // namespace craft
