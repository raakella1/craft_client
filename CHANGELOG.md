# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
