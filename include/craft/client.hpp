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

// The CRAFT client's driver-facing API: an OPAQUE handle + free-function verbs. A driver (e.g. a ublk disk)
// includes only this + <craft/types.hpp>, holds a client_handle, and calls login / write / read / flush /
// logout. The handle carries a transport behind it; a driver never constructs one -- construction lives in a
// transport header (make_client in <craft/replica.hpp>, or the backend builders in <craft/tcp.hpp> /
// <craft/local.hpp>), and the craft_client type itself is defined only inside the library.

#include <cstddef>
#include <cstdint>
#include <memory>

#include <sisl/fds/buffer.hpp> // sisl::sg_list

#include <craft/types.hpp> // the CRAFT vocabulary + the result / async_result aliases

namespace craft {

class craft_client; // opaque -- defined only in the library's source
using client_handle = std::shared_ptr< craft_client >;
struct tracker_stats; // returned by dlsn_stats(); a driver never calls it (its definition is internal)

// ── the driver surface (verbs over the handle) ──

// Establish the session, following NOT_LEADER redirects.
async_status login(client_handle const& c, uint64_t client_token);

// Broadcast a write at a fresh dLSN; commit advances once quorum acks. Empty `data` is a zero write.
async_result< size_t > write(client_handle const& c, uint64_t addr, uint64_t len, sisl::sg_list data);

// Fill `dest` in place (data bytes; holes -> zeros) and resolve to the byte count. Splits into parallel
// sub-reads when blocks in the range need different horizons; routes around behind/down members.
async_result< size_t > read(client_handle const& c, uint64_t addr, uint64_t len, sisl::sg_list dest);

// The dedicated commit carrier: broadcast keep_alive to every replica, advancing the set-wide frontier + floor.
async_status flush(client_handle const& c);

// Explicit, term-fenced session teardown. Best-effort; a fresh login re-establishes a session.
async_status logout(client_handle const& c);

// Fire a keep_alive at every leg except `exclude_idx` that has none outstanding -- the timer-less liveness
// drive. read() does this for the legs it did not serve; a ublk adapter's idle probe calls it too.
inline constexpr std::size_t k_no_leg = ~std::size_t{0};
void drive_keepalives(client_handle const& c, std::size_t exclude_idx = k_no_leg);

uint32_t lba_size(client_handle const& c); // volume block size in bytes (alignment unit for addr/len)
uint64_t capacity(client_handle const& c); // volume size in bytes (a block-device driver's device geometry)
uint64_t term(client_handle const& c);

// ── observability (safe to call while IO is in flight) ── dlsn_stats returns tracker_stats, whose definition
// is an internal header, so a plain driver never calls it.
int64_t commit_lsn(client_handle const& c);
int64_t read_horizon(client_handle const& c);
uint64_t winner_scans(client_handle const& c);
int64_t route_folded(client_handle const& c);
bool route_caught_up(client_handle const& c, std::size_t idx);
int64_t all_committed_lsn(client_handle const& c);
tracker_stats dlsn_stats(client_handle const& c, std::size_t sample_limit = 16);
std::size_t replica_count(client_handle const& c);
uint32_t leader_index(client_handle const& c);

} // namespace craft
