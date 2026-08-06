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

#include <sisl/fds/buffer.hpp> // sisl::sg_list

#include <craft/types.hpp> // the CRAFT vocabulary + the result / async_result aliases

struct io_uring; // liburing (fwd-decl only: the async verb overloads take a pointer to the caller's queue ring)

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
// reap thread of the ring a verb was passed (the async overloads, below), else a transport-internal thread
// (the reference model's replica pool, the TCP proxy's session-mgr thread). A coroutine-native consumer must
// pass its ring or tolerate foreign-thread resumption; a blocking consumer (sisl::async::sync_get) is safe
// either way. (The previous exec::task currency could hop an async consumer back to its own scheduler;
// nothing in this stack used that, and the on-ring data path is built on NOT doing it.)

// ── construction: the ONE seam ──
//
// Build a client over a transport: one backend per member, in membership order (`leader` is where login is tried
// first; `max_inflight` sizes the tracker's winner-scan tripwire and is the AGGREGATE in-flight bound across
// every queue -- a blk-mq driver passes nr_hw_queues x queue_depth). Get the backends from a builder --
// make_tcp_cluster (<craft/tcp.hpp>) or make_local_cluster (<craft/local.hpp>) -- and keep the builder's handle
// alive for at least as long as the client: it OWNS the backends.
client_handle make_client(std::vector< std::shared_ptr< craft_replica > > replicas, uint32_t leader = 0,
                          uint32_t max_inflight = 128);

// ── the driver surface (verbs over the handle) ──
//
// EVERY MID-SESSION VERB COMES IN TWO FORMS. The ASYNC form takes the caller's queue ring (`q`, right after
// the handle -- the ublk parameter order): the verb submits its SQEs on `q`, many ops go in flight at once
// (QD>1), and the awaiting coroutine RESUMES ON q's REAP THREAD -- the single-threaded on-ring model a ublk
// queue wants. The SYNC form (no ring) takes the blocking tier -- a transport-internal completion thread (the
// reference model's per-replica pool, the TCP proxy's shared session-mgr thread): a coroutine-native caller
// must tolerate resuming there, or block instead via sisl::async::sync_get, which is safe from any
// non-completing thread. There is no scheduler anywhere in this stack to hop a consumer back to its own
// thread -- by design; do not assume one.
//
// BLK-MQ: a driver with nr_hw_queues rings calls the async verbs concurrently, each queue thread passing its
// own ring -- the transport keeps one data connection per (ring, replica), the nr_hw_queues x N grid, opened
// lazily at each ring's first verb. AFFINITY IS YOURS, exactly as with a raw io_uring: pass a ring only from
// the thread that owns and reaps it. One op's whole leg chain (its broadcast legs, failovers, and the
// keep_alives/resolutions it spawns) rides the ring it was called with. A ring is remembered BY ADDRESS for
// the client's whole life (there is no unbind): every ring passed must outlive the client, and a destroyed
// ring's address must not reappear as a new ring mid-life. Ordering contract: login (sync_get, a setup
// thread) happens-before any queue's first verb; all queues quiesce and exit their rings before logout /
// client teardown -- where "quiesce" includes reaping the DETACHED legs the queue's verbs spawned (straggler
// writes, keep_alives, resolution rounds), not just its own awaited ops.

// Establish the session, following NOT_LEADER redirects. Always blocking-tier: it brackets every ring's
// lifetime (it learns lba/capacity/term before any queue IO exists) -- drive it with sync_get on a setup thread.
async_status login(client_handle const& c, uint64_t client_token);

// Broadcast a write at a fresh dLSN; commit advances once quorum acks. Empty `data` is a zero write.
async_result< size_t > write(client_handle const& c, uint64_t addr, uint64_t len, sisl::sg_list data);
async_result< size_t > write(client_handle const& c, ::io_uring* q, uint64_t addr, uint64_t len, sisl::sg_list data);

// Fill `dest` in place (data bytes; holes -> zeros) and resolve to the byte count. Splits into parallel
// sub-reads when blocks in the range need different horizons; routes around behind/down members.
async_result< size_t > read(client_handle const& c, uint64_t addr, uint64_t len, sisl::sg_list dest);
async_result< size_t > read(client_handle const& c, ::io_uring* q, uint64_t addr, uint64_t len, sisl::sg_list dest);

// The dedicated commit carrier: broadcast keep_alive to every replica, advancing the set-wide frontier + floor.
// (q is trailing here, so one defaulted declaration covers both tiers.)
async_status flush(client_handle const& c, ::io_uring* q = nullptr);

// Explicit, term-fenced session teardown. Best-effort; a fresh login re-establishes a session. Blocking-tier,
// like login: every queue has quiesced and exited its ring by the time this runs.
async_status logout(client_handle const& c);

// Fire a keep_alive at every leg except `exclude_idx` that has none outstanding -- the timer-less liveness
// drive. read() does this for the legs it did not serve; a ublk adapter's idle probe calls it too. The
// one-outstanding-per-leg collapse is client-wide, not per queue (liveness needs ONE keep_alive per member);
// whichever queue wins a leg fires it on its own ring.
inline constexpr std::size_t k_no_leg = ~std::size_t{0};
void drive_keepalives(client_handle const& c, std::size_t exclude_idx = k_no_leg);
void drive_keepalives(client_handle const& c, ::io_uring* q, std::size_t exclude_idx = k_no_leg);

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
