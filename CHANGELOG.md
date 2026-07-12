# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
