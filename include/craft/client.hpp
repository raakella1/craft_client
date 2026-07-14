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
// includes only this + <craft/types.hpp> + one backend builder (<craft/tcp.hpp> or <craft/local.hpp>): it picks a
// builder, hands the builder's backends to make_client (below), and drives the returned handle with the verbs.
// Neither craft_client nor craft_replica is ever named -- both are opaque here and defined only inside the library.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include <sisl/async/light_task.hpp> // sisl::async::light_result / ::light_status (the co_await-able result carrier)
#include <sisl/fds/buffer.hpp>       // sisl::sg_list
#include <sisl/result.hpp>           // sisl::result / ::status / ::ok

#include <craft/types.hpp> // the CRAFT vocabulary + the result / async_result aliases

struct io_uring; // liburing (fwd-decl only: prepare_for_async takes a pointer to a host ring)

namespace craft {

class craft_client; // opaque -- defined only in the library's source
using client_handle = std::shared_ptr< craft_client >;
struct tracker_stats; // returned by dlsn_stats(); a driver never calls it (its definition is internal)

// The per-replica backend the client drives. OPAQUE to a driver: a builder hands back a vector of these and the
// driver passes it straight to make_client, never naming or dereferencing one (a shared_ptr type-erases its
// deleter at construction, inside the builder, so an incomplete type here is fine). The interface itself is
// internal -- it is the CLIENT's view of a member, implemented by a transport proxy or the reference model, and a
// storage backend neither implements it nor appears on this side of the wire.
class craft_replica;

// The FREESTANDING task (sisl::async::light_task): a plain awaitable co_await-able from any coroutine --
// another light_task, a ublk driver's disk_task, an exec::task -- with no scheduler anywhere. THE THREADING
// CONTRACT IS THE COMPLETION'S: the awaiting coroutine resumes on whatever thread completes the op -- the
// ring owner's reap thread once prepare_for_async has bound a ring, else a transport-internal thread (the
// reference model's replica pool, the TCP proxy's session-mgr thread). A coroutine-native consumer must
// bind a ring or tolerate foreign-thread resumption; a blocking consumer (sisl::async::sync_get) is safe
// either way. (The previous exec::task currency could hop an async consumer back to its own scheduler;
// nothing in this stack used that, and the on-ring data path is built on NOT doing it.)
template < typename T >
using async_result = sisl::async::light_result< T >;
using async_status = sisl::async::light_status;

// ── construction: the ONE seam ──
//
// Build a client over a transport: one backend per member, in membership order (`leader` is where login is tried
// first; `max_inflight` sizes the tracker's winner-scan tripwire). Get the backends from a builder --
// make_tcp_cluster (<craft/tcp.hpp>) or make_local_cluster (<craft/local.hpp>) -- and keep the builder's handle
// alive for at least as long as the client: it OWNS the backends.
client_handle make_client(std::vector< std::shared_ptr< craft_replica > > replicas, uint32_t leader = 0,
                          uint32_t max_inflight = 128);

// ── the driver surface (verbs over the handle) ──
//
// THREADING: every verb returns a freestanding task (see craft/types.hpp) -- co_await it from any coroutine,
// and the awaiting coroutine RESUMES ON THE THREAD THAT COMPLETES THE OP. With a ring bound (prepare_for_async,
// below) that is the ring owner's reap thread -- the single-threaded on-ring model a ublk queue wants. WITHOUT
// a ring it is a transport-internal thread (the reference model's per-replica pool, the TCP proxy's shared
// session-mgr thread): a coroutine-native caller off-ring must tolerate resuming there, or block instead via
// sisl::async::sync_get, which is safe from any non-completing thread. There is no scheduler anywhere in this
// stack to hop a consumer back to its own thread -- by design; do not assume one.

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

// Bind the client's whole backend set to a host io_uring `ring` for on-ring async completion, so writes/reads
// go in flight at once (QD>1) and complete on the ring owner's reap thread. A driver that owns a ring (a ublk
// queue, or a test harness) calls this once after login, off the IO path; it fans out to each backend's
// prepare_for_async, and transports that don't submit on a caller ring ignore it. UNCHANGED verbs: write/read
// keep their signatures -- the client never learns the completion source moved onto the ring.
//
// For a coroutine-native consumer this is effectively PART OF THE DATA-PATH CONTRACT, not an optimization:
// binding the ring is what makes the verbs' resumptions land on YOUR thread (see the threading note above).
// Login runs before any ring exists and is unaffected -- drive it with sync_get on a setup thread.
void prepare_for_async(client_handle const& c, ::io_uring* ring);

uint32_t lba_size(client_handle const& c); // volume block size in bytes (alignment unit for addr/len)
uint64_t capacity(client_handle const& c); // volume size in bytes (a block-device driver's device geometry)
uint32_t max_tx(client_handle const& c);   // volume max transfer in bytes (login-conveyed; a driver caps IO to it)
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
