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

// craft/local.hpp -- the no-remote / in-process backend builder. Assemble a CRAFT client that runs entirely in
// this process against the reference model (no TCP, no wire): make_local_cluster() stands up N reference
// replicas; backends() hands their craft_replica backends to make_client(). This is the reference transport (it
// links craft_reference); for a real remote client see craft/tcp.hpp. Neither the mem model nor make_client
// needs to know which builder produced the backends -- the client sees only the craft_replica interface.
//
// Same shape as the client and craft/tcp.hpp: an OPAQUE handle + free functions, never a concrete class on the
// surface. The handle OWNS the reference cluster (the replicas + the reference transport). Hold it for the
// client's lifetime and destroy the client FIRST: dropping the last handle drains the reference pools while it
// still owns every replica (the same teardown rule as the TCP builder). Declaring the cluster handle before the
// client handle gives exactly that order.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include <craft/replica.hpp> // craft_replica + make_client
#include <craft/types.hpp>   // volume_id_t
#include <craft/wire.hpp>    // wire::k_default_max_tx (the single-sourced volume max-transfer default)

namespace craft {

// Opaque: defined only in src/. Consumers name it solely through the handle below.
class local_cluster;
using local_cluster_handle = std::shared_ptr< local_cluster >;

// N reference replicas serving `vol_id` at `page_size` bytes/block (index 0 is the default leader). `capacity`
// is the volume size in bytes reported at login (the device geometry a driver sizes from); `max_tx` is the
// volume's max transfer, likewise conveyed at login (a driver caps its device IO to it).
local_cluster_handle make_local_cluster(volume_id_t vol_id, uint32_t n = 3, uint32_t page_size = 4096,
                                        uint64_t capacity = uint64_t{1} << 30,
                                        uint32_t max_tx = wire::k_default_max_tx);

// The backends to hand to make_client(); index 0 is the leader. Valid until the last handle is dropped.
std::vector< std::shared_ptr< craft_replica > > const& backends(local_cluster_handle const& c);

// ── fault injection (the reference transport's knobs, by replica index) ── each sets the knob on the underlying
// reference replica, so a test drives faults straight through the builder without reaching for a transport (the
// same shape the reference cluster server exposes).
void set_replica_up(local_cluster_handle const& c, std::size_t i, bool up);                        // down / up
void set_replica_delay(local_cluster_handle const& c, std::size_t i, std::chrono::milliseconds d); // 0 clears
void force_subquorum(local_cluster_handle const& c, std::vector< std::size_t > keep); // writes land ONLY on these
void clear_faults(local_cluster_handle const& c);                                     // every replica healthy

} // namespace craft
