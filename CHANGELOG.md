# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.3.0

### Changed
- **BREAKING -- multi-queue (blk-mq) rings: the ring travels with the verb; `prepare_for_async` is gone.**
  Every mid-session verb now has an async overload taking the caller's io_uring right after the handle (the
  ublk parameter order): `write(c, q, ...)`, `read(c, q, ...)`, `flush(c, q)`, `drive_keepalives(c, q, ...)`.
  The ringless forms remain and are the blocking tier (transport-internal completion: reference-model pool /
  session-mgr thread). A driver with `nr_hw_queues` rings calls the async verbs concurrently, each queue
  thread passing its own ring: the TCP proxy keeps one lazily-opened `craft_async_conn` per (ring, replica) --
  the transport.md `nr_hw_queues x N` connection grid, each data socket HELO-bound at the shared term, each
  with its own `request_id` space -- selected by a lock-free ring-pointer scan (append-only slot table; the
  registration mutex is off the steady-state path). AFFINITY IS THE CALLER'S CONTRACT, as with a raw io_uring:
  a ring is passed only from the thread that owns and reaps it, so each connection stays thread-confined by
  construction and `craft_async_conn` keeps its no-locks design. One op's whole leg chain (broadcast legs,
  read failovers, the keep_alives/resolutions it spawns) rides the ring it was called with. Internally
  `craft_replica`'s mid-session verbs take a leading `::io_uring* q` and the `prepare_for_async` virtual is
  removed; the mem model takes the ring per call (no stored ring). Ordering contract (unchanged in spirit,
  now per queue): login happens-before any queue's first verb; all queues quiesce and exit their rings before
  logout / teardown.
- `max_inflight` (make_client) documented as the AGGREGATE in-flight bound across all queues -- a blk-mq
  driver passes `nr_hw_queues x queue_depth`.
- Migration note: `drive_keepalives(c, 0)` with a LITERAL zero exclude index no longer compiles (0 converts
  equally to `std::size_t` and a null `::io_uring*`). Write `std::size_t{0}` or pass a ring. Rings are
  remembered by address for the client's whole life: every ring passed must outlive the client.
- Keep_alive / resolution single-flight stays per-member CLIENT-WIDE, not per queue (liveness needs one
  keep_alive per member); whichever queue wins a leg fires it on its own ring.

### Added
- `craft_cluster_server::connections_accepted()` (test observability): witnesses the connection grid --
  `nr_hw_queues x N` data sockets + the 1 admin LOGIN socket.
- Multi-queue tests: `CraftAsyncMem.MultiQueueWriteReadRoundTrip`, `CraftAsyncMem.BlockingTierCoexistsWithRings`,
  `CraftAsyncTcp.MultiQueueGridOverTcp` -- Q threads x Q rings driving one shared client, data verified per
  queue, dLSN density and the grid's socket count asserted.

## 0.2.0

### Changed
- **Task currency: `exec::task` -> `sisl::async::light_task`** (`async_result`/`async_status` aliases flipped in
  `craft/types.hpp`). The verbs are now co_await-able from ANY coroutine -- a ublk driver's `disk_task` awaits
  them directly, no shim/detach/rendezvous. THREADING CONTRACT (now explicit on the verbs and
  `prepare_for_async`): the awaiting coroutine resumes on the thread that completes the op -- the bound ring's
  reap thread after `prepare_for_async`, else a transport-internal thread (reference-model pool / session-mgr).
  The old scheduler hop-back that `exec::task` could theoretically provide is gone; nothing used it (every site
  suppressed it with `inline_scheduler`). Blocking callers keep `sisl::async::sync_get`. Fire-and-forget legs
  (`fire_keepalive`/`fire_resolution`/`late_write`) launch via the explicit `.detach()` member.

## 0.1.1

### Added
- `craft_async_conn::resolve`: the client-requested resolution round rides the on-ring data connection once
  `prepare_for_async` primes the ring (it fires from the write path's failure branch -- the ring thread -- so
  the blocking hop would stall the reactor). Replies demux by `request_id` like every data op, so the slow
  leader round costs the in-flight IOs nothing. The blocking path remains only for the no-ring tier.
- `craft_session_mgr`: ONE process-wide admin thread shared by every `CraftTcpReplica`, replacing the
  per-proxy worker (a 50-disk RAID0 at N=3 would otherwise idle 150 threads). Refcounted, not a leaky
  singleton: the thread retires when the last proxy in the process drops, and a later attach mints a fresh
  one. Proxy `shutdown()` drains via a FIFO fence instead of a join; the detached-leg self-destruction edge
  (the old UB-adjacent detach backstop) is now memory-safe -- the thread co-owns its queue state.
- Connect deadline: `craft_conn::connect` is ALWAYS bounded (non-blocking connect + poll; default
  `k_connect_timeout` 2s, or the proxy's `op_timeout` when set). A peer silently dropping SYNs fails at the
  deadline instead of parking the shared session-mgr thread for the kernel's ~2min retry window -- which
  would stall every disk's admin plane, not one proxy's.

### Changed
- On the no-ring tier every proxy's blocking legs serialize FIFO on the shared session-mgr thread, so
  ack-at-quorum degrades to FIFO completion order there -- acceptable for the shim/test tier, irrelevant
  on-ring where write/read/keep_alive/resolve all fan out on the caller's io_uring.

## 0.1.x
- Initial commit

### Added
- Client-requested resolution round (the design's client-request `SyncRSCommitLSN` trigger):
  `craft_replica::request_resolution(hdr, upto)` + the wire `RESOLVE`/`RESOLVE_RSP` ops (13/14). A failed
  (sub-quorum) write BROADCASTS the request to every member (the client cannot know who leads mid-session),
  at most one outstanding per peer -- the keep_alive collapse -- with failed dLSNs CAS-maxed into one want
  watermark, so a burst of failures never sends one request each. The current leader fills each unresolved
  slot <= `upto` from a holder or verdicts it Empty (quorum-lacks evidence); non-leaders answer NOT_LEADER
  (a real replica may forward instead) and leave the want for the leader's leg. The client retires the
  covered slots off the verdicts (`dlsn_tracker::retire_upto` + `read_route_map::note_filled`), releasing
  the commit frontier without waiting for re-login. Replicas REJECT a late write into an Empty-verdicted
  slot (reconciliation: Empty beats data), so a voided write fails deterministically on its own ack path.
- On-ring TCP transport (`craft_async_conn`): the async data path submits socket ops on the caller's io_uring
  (QD>1), with replies demuxed by `request_id` through a single persistent recv pump; bound via
  `craft::prepare_for_async`. Sends are serialized per connection to keep the framed stream intact.
- `craft_reference_tcp_srv`: a standalone per-replica server (one process = one replica) for a real
  multi-process TCP cluster, with read/write trace logging (base module; `-v trace`).
- `wire::framed_body_max(payload, lba)`: the single-sourced transport body bound.

### Changed
- `craft_replica::write` returns the replica's `lsn_pair` and `read` returns `read_result{extents, lsns}`:
  every IO response piggybacks `{commit_lsn, last_append_lsn}` (as the wire always did), and the client feeds
  them to its router -- the reclaim floor / Missing-map recovery now advance off ordinary writes and reads,
  not only keep_alives. (`advance_synced` gained a lock-free monotonic pre-check to keep ack paths cheap.)
- Wire `login_req` now carries `volume_id[16]` ahead of `client_token` (the design's
  `login(client_token, vol_id)`), mirroring `helo_req`; `craft_tcp_client::login(volume_id, token)`. The
  cluster reference server verifies the id; the standalone single-volume server accepts any.
- `max_tx` is the volume's IO **payload** (data only), advertised to drivers. The transport bounds a message
  body at `framed_body_max` (payload + a read reply's extent table), so a full-payload read still parses.
