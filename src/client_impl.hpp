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

// INTERNAL. The concrete craft_client -- the CRAFT protocol logic (dLSN assignment, quorum broadcast, read
// routing, commit tracking). No consumer ever sees this class: a driver builds it via make_client
// ("replica.hpp") over the backends a transport builder handed it, then holds the opaque handle from
// <craft/client.hpp> and calls the free functions. Only make_client and those free functions (all defined in
// client.cpp) touch it. A storage backend is never on this side of the wire and never constructs one.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <system_error>
#include <vector>

#include <sisl/fds/buffer.hpp> // sisl::sg_list

#include "craft_replica.hpp" // the per-replica backend interface craft_client drives

#include "dlsn_tracker.hpp"   // internal: the dLSN state machine (slot_outcome, tracker_stats, read_plan)
#include "read_route_map.hpp" // internal: the per-member Missing / read-eligibility map

namespace craft {

class craft_client {
public:
    craft_client(std::vector< std::shared_ptr< craft_replica > > replicas, uint32_t leader = 0,
                 uint32_t max_inflight = 128) :
            replicas_(std::move(replicas)), leader_(leader), tracker_(std::make_shared< dlsn_tracker >(max_inflight)) {}

    // Mid-session verbs carry the caller's queue ring (`q`, null = the blocking tier) straight through to every
    // backend leg they fan out -- one IO's whole leg chain rides one ring, so its resumptions all land back on
    // that queue's reap thread. login/logout are ringless: they bracket every ring's lifetime.
    async_status login(uint64_t client_token);
    async_result< size_t > write(::io_uring* q, uint64_t addr, uint64_t len, sisl::sg_list data);
    async_result< size_t > read(::io_uring* q, uint64_t addr, uint64_t len, sisl::sg_list dest);
    async_status flush(::io_uring* q);
    async_status logout();
    void drive_keepalives(::io_uring* q, std::size_t exclude_idx);

    uint32_t lba_size() const { return lba_size_; }
    uint64_t capacity() const { return capacity_; }
    uint32_t max_tx() const { return max_tx_; }
    uint64_t term() const { return term_; }
    int64_t commit_lsn() const { return tracker_->frontier(); }
    int64_t read_horizon() const { return tracker_->read_horizon(); }
    uint64_t winner_scans() const { return tracker_->winner_scans(); }
    int64_t route_folded() const { return route_->folded(); }
    bool route_caught_up(std::size_t idx) const { return route_->caught_up(idx); }
    int64_t all_committed_lsn() const { return route_->all_committed(); }
    tracker_stats dlsn_stats(std::size_t sample_limit = 16) const { return tracker_->stats(sample_limit); }
    std::size_t replica_count() const { return replicas_.size(); }
    uint32_t leader_index() const { return leader_; }

private:
    client_hdr make_hdr() const;
    std::size_t quorum() const { return replicas_.size() / 2 + 1; }
    // Trim the router's overlay from the WRITE path, exactly as dlsn_tracker batch-truncates its own
    // StreamTracker on resolve (k_trunc_batch). Without this, read_route_map::fold_to() is reached ONLY from
    // read(), so a write-only stream grows the overlay without bound and the first read after a burst pays to
    // fold all of it at once (measured: 271us of eligible() scan after 64Ki unfolded writes, vs 0.13us folded).
    // Batched on purpose: folded() is one relaxed atomic load, so a write off the batch boundary costs nothing,
    // while fold_to() itself would take the overlay's shared_lock every time.
    void maybe_fold();
    std::optional< std::error_condition > precheck(uint64_t addr, uint64_t len) const;
    async_result< lsn_pair > issue_plan(::io_uring* q, std::shared_ptr< craft_replica > const& target, client_hdr hdr,
                                        read_plan const& plan, uint64_t addr, uint64_t len, sisl::sg_list& dest);
    // Fire the client-requested resolution round for failed slot `upto`: record the want, then BROADCAST a
    // detached request to every peer without one outstanding (per-peer single-flight, the keep_alive
    // collapse) -- the client cannot know who leads mid-session, so whichever member is the leader resolves.
    // Each leg captures the tracker's and router's shared_ptrs, never `this`; it rides the ring of the write
    // whose failure fired it.
    void request_resolution_round(::io_uring* q, int64_t upto);

    std::vector< std::shared_ptr< craft_replica > > replicas_;
    uint32_t leader_{0};
    uint64_t term_{0};
    uint32_t lba_size_{0};
    uint64_t capacity_{0};
    uint32_t max_tx_{0};

    // shared_ptr like route_: the detached resolution-round runner retires slots into the tracker and may
    // outlive the client, so it holds this map's/tracker's lifetime rather than referencing `this`.
    std::shared_ptr< dlsn_tracker > tracker_;
    // shared_ptr, not a plain member: a detached when_quorum straggler's completion leg records into this map
    // and may finish after the client is destroyed. The leg captures a copy, so a late completion writes into
    // a still-alive (orphaned) map rather than a freed one -- the same discipline the transport uses.
    std::shared_ptr< read_route_map > route_{std::make_shared< read_route_map >()};
};

} // namespace craft
