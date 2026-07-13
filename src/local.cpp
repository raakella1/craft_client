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

// local_cluster: the in-process reference model behind an opaque handle. The concrete class lives ENTIRELY here
// (never installed) -- the public surface is craft/local.hpp's free functions over local_cluster_handle. Wraps
// make_mem_replica_group so a consumer assembles the no-remote path without ever naming a craft/mem/* type; the
// group's dtor drains the reference pools while it still owns every replica.

#include <craft/local.hpp>

#include <algorithm>

#include "mem/cluster.hpp" // make_mem_replica_group, MemReplicaGroup (+ MemCraftReplica fault knobs)

namespace craft {

class local_cluster {
public:
    explicit local_cluster(MemReplicaGroup&& group) :
            group_{std::move(group)}, backends_(group_.replicas.begin(), group_.replicas.end()) {}
    // ~MemReplicaGroup (inside group_) drains the reference pools while it still owns every replica.

    std::vector< std::shared_ptr< craft_replica > > const& backends() const { return backends_; }

    void set_replica_up(std::size_t i, bool up) { group_.replicas[i]->set_up(up); }
    void set_replica_delay(std::size_t i, std::chrono::milliseconds d) { group_.replicas[i]->set_delay(d); }
    void force_subquorum(std::vector< std::size_t > const& keep) {
        for (std::size_t i = 0; i < group_.replicas.size(); ++i) {
            bool const kept = std::find(keep.begin(), keep.end(), i) != keep.end();
            group_.replicas[i]->drop_writes(!kept); // members not in `keep` drop writes -> sub-quorum
        }
    }
    void clear_faults() {
        for (auto& r : group_.replicas)
            r->clear_faults();
    }

private:
    MemReplicaGroup group_;
    std::vector< std::shared_ptr< craft_replica > > backends_; // upcast of group_.replicas; index 0 is the leader
};

// ── free functions: the public surface ──

local_cluster_handle make_local_cluster(volume_id_t vol_id, uint32_t n, uint32_t page_size, uint64_t capacity,
                                        uint32_t max_tx) {
    return std::make_shared< local_cluster >(
        make_mem_replica_group(vol_id, n, page_size, /*threads_per_replica=*/2, capacity, max_tx));
}

std::vector< std::shared_ptr< craft_replica > > const& backends(local_cluster_handle const& c) { return c->backends(); }

void set_replica_up(local_cluster_handle const& c, std::size_t i, bool up) { c->set_replica_up(i, up); }
void set_replica_delay(local_cluster_handle const& c, std::size_t i, std::chrono::milliseconds d) {
    c->set_replica_delay(i, d);
}
void force_subquorum(local_cluster_handle const& c, std::vector< std::size_t > keep) { c->force_subquorum(keep); }
void clear_faults(local_cluster_handle const& c) { c->clear_faults(); }

} // namespace craft
