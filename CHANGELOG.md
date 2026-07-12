# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.1.x
- Initial commit

### Added
- On-ring TCP transport (`craft_async_conn`): the async data path submits socket ops on the caller's io_uring
  (QD>1), with replies demuxed by `request_id` through a single persistent recv pump; bound via
  `craft::prepare_for_async`. Sends are serialized per connection to keep the framed stream intact.
- `craft_reference_tcp_srv`: a standalone per-replica server (one process = one replica) for a real
  multi-process TCP cluster, with read/write trace logging (base module; `-v trace`).
- `wire::framed_body_max(payload, lba)`: the single-sourced transport body bound.

### Changed
- `max_tx` is the volume's IO **payload** (data only), advertised to drivers. The transport bounds a message
  body at `framed_body_max` (payload + a read reply's extent table), so a full-payload read still parses.
