# craft_client -- the CRAFT reference client, wire protocol, and reference model

A **standalone, transport-agnostic, storage-engine-free** package for the client half of **CRAFT**
(Client Assisted RAFT), the replication protocol for HomeBlocks block volumes. Extracted from HomeBlocks so
it can be embedded anywhere: HomeBlocks depends *back* on it, and ublkpp's `craft_disk` driver drives it to
expose a `/dev/ublkbN`.

Its whole dependency footprint is **sisl + boost + liburing** (+ gtest for tests). No HomeBlocks, no HomeStore.

## CRAFT in one paragraph

CRAFT separates the **data path** from the **consensus path**: a client broadcasts writes directly to all
replicas at *client-assigned* data-LSNs (dLSNs), and acknowledges at a **quorum without waiting for the
slowest replica** -- RAFT is used only for leader election, login synchronization, and recovery bookkeeping.
Write data never flows through the RAFT log. Reads are unicast to a single eligible replica at a read horizon
`H`, routed around replicas that are missing the range. Writes are thin (a payload-free "zero write" names just
a byte range) and reads are sparse (data extents + holes; zeros never cross the wire).

> **Canonical design** lives in two wiki pages -- the store-agnostic protocol,
> [CRAFT Design](https://github.com/eBay/HomeBlocks/wiki/CRAFT-Design), and its HomeStore binding,
> [CRAFT on HomeBlocks](https://github.com/eBay/HomeBlocks/wiki/CRAFT-on-HomeBlocks). They are the source of truth.

## Components (strictly one-way dependencies)

| Component | What it is | Depends on |
|---|---|---|
| **`craft_wire`** | The on-wire codec -- packed little-endian message framing, CRC32C digests, extent scatter. A **std-only leaf** (the future standalone dependency). | -- (std) |
| **`craft_types`** | The domain vocabulary (`LSNPair`, `io_extent`, `client_hdr`, `LoginResult`, `craft_error`, `peer_id_t`) + the `async_result` aliases + the `craft_replica` backend interface + `make_client`. Header-only. | sisl, boost |
| **`craft_client`** | The client behind the opaque handle: dLSN assignment, quorum broadcast, read routing + failover, login/redirect -- driven by free functions, over a pluggable `craft_replica` transport. Plus the io_uring TCP transport. | craft_wire, craft_types, sisl, liburing |
| **`craft_reference`** | An in-memory reference `craft_replica` + a loopback cluster server, so the client can be driven end-to-end with **no storage engine**. Also hosts the public in-process builder (`craft/local.hpp`). Test/dev-support. | craft_client |

The result vocabulary itself (`result<T>` / `async_result<T>`) is owned by **sisl** (`sisl::result`,
`sisl::async::result`) -- the same type HomeStore and nuraft_mesg use -- so a domain error (`craft_error`) rides
the type-erased `std::error_condition` and no layer forks the vocabulary.

## Public API surface

The client is an **opaque handle + free-function verbs** -- a driver never sees the `craft_client` type, and the
construction seam is separate so the *transport* (not the client) is what varies. The surface is intentionally
tiny, split by who needs what:

| Header | Audience | Contents |
|---|---|---|
| `craft/types.hpp` | everyone | the vocabulary + `async_result` aliases |
| `craft/client.hpp` | **drivers** (e.g. a ublk disk) | `client_handle` + `login`/`write`/`read`/`flush`/`logout`/`drive_keepalives` + observers |
| `craft/replica.hpp` | **transport authors** (e.g. HomeStore) | the `craft_replica` interface + `make_client(backends)` |
| `craft/tcp.hpp`, `craft/local.hpp` | assembling a binary | backend builders `make_tcp_cluster` / `make_local_cluster` → an **opaque handle**; `backends(handle)` feeds `make_client` |
| `craft/wire.hpp`, `craft/status.hpp` | **server / protocol** authors (the future CraftConnector) | the codec + the wire↔`craft_error` bridge |

A consumer never names the client type -- it picks a backend builder, hands its backends to `make_client`, and drives
the handle with the verbs. The builder is an RAII **owner** of the transport; keep it alive for (and destroy it
after) the client:

```cpp
auto cluster = craft::make_tcp_cluster(endpoints, vol_id); // <craft/tcp.hpp> -- or make_local_cluster(...)
auto c = craft::make_client(craft::backends(cluster));     // <craft/replica.hpp> -- the one construction seam
co_await craft::login(c, token);                           // <craft/client.hpp> -- verbs over the handle
co_await craft::write(c, addr, len, buf);
```

Every builder follows the client's shape: an **opaque handle + free functions**, never a concrete class on the
surface (`make_local_cluster` also exposes `force_subquorum(handle, ...)` / `set_replica_up(handle, ...)` fault
verbs the same way).

The **only variable is the transport** behind the handle: a transport author implements `craft_replica`
(TCP sockets, the in-process reference, or a HomeStore-API adapter) and hands the backends to `make_client`.
The `craft_client` class itself, `dlsn_tracker`, `read_route_map`, and the io_uring transport all live in
`src/` -- never installed, never on the surface.

## The client model

The hard part of the client is one question -- **which version of a block may a read see, and how does that
follow from writes still in flight?** -- answered by `dlsn_tracker`. The second thread is what a peer's answer
lets the client *conclude*: an ack, a **deterministic reject**, and a **timeout** are three different epistemic
states, and conflating the last two is divergence (a timed-out write *may* have applied; it is never counted).

| Piece (internal) | Role |
|---|---|
| `dlsn_tracker` | The state machine: assigns dLSNs, tracks each slot's fate, derives the commit frontier and the read horizon. |
| `sisl::async::when_quorum` | Fan-out that resumes at the quorum'th ack and leaves the stragglers **detached** -- the marquee data-path win. A generic k-of-n combinator (sibling of `when_all`); lives in sisl. |
| `read_route_map` | Per-member Missing map: routes each read to an eligible holder, fails over on a miss/down. |
| `craft_client` | Broadcast, quorum tally, login/redirect. Owns a `dlsn_tracker`; the opaque type behind `client_handle`. |
| `net/` | The io_uring TCP transport: `craft_conn`, the wire-only `craft_tcp_client`, `CraftTcpReplica` (the `craft_replica` proxy). |

The generic pieces this leans on -- `sisl::result`/`async::result`, `sisl::async::when_quorum`, and the
`sisl::async::sync_get`/`detach` coroutine bridges -- were hoisted **into sisl**, so nothing here forks them.

## Building

Requires a conan 2.x profile with C++23. sisl / liburing resolve from your remotes (or editable checkouts).

```sh
conan install . -of=build --build=missing -s build_type=Debug -s compiler.cppstd=23
cmake --preset conan-debug
cmake --build --preset conan-debug
ctest --preset conan-debug            # 11 suites: wire codec, types, public API (in-process + TCP), mem model, client, transport
```

Sanitizers: add `-o sanitize=address` (or `thread`) to the `conan install`.

## Relationship to HomeBlocks and ublkpp

There are two integration seams, and HomeBlocks can sit behind either:

- **Transport seam (`craft_replica` + `make_client`).** A transport author implements `craft_replica` and hands
  the backends to `make_client` -- the client never changes. Two builders ship: `make_tcp_cluster` (real
  remote servers over io_uring TCP) and `make_local_cluster` (the in-process reference, no server/no wire);
  HomeStore adds its own `make_homeblocks_client()` that builds in-process, homeblocks-API-backed replicas.
  HomeBlocks also depends back on `craft_wire` + `craft_types` for its public CRAFT vocabulary, and on
  `craft_reference` for its volume-level e2e test.
- **Server seam (`craft_wire` + `craft/status.hpp`).** An ordinary TCP client speaks the wire to *any*
  wire-speaking server. A HomeBlocks CRAFT server (the future `CraftConnector`) decodes the wire request and
  translates it to a HomeBlocks API call -- structurally identical to the reference `cluster_server`, just backed
  by HomeBlocks. The client has no idea what's on the other end.

- **ublkpp**'s `craft_disk` is written once against the driver surface (`client_handle` + the verbs) and is
  **agnostic to what's behind the handle** -- standalone it drives the reference over TCP/in-process; on the
  HomeBlocks side the handle carries a HomeBlocks-backed transport. `craft_client` knows nothing about ublk.

## Documentation

| Doc | Contents |
|---|---|
| [docs/wire.md](docs/wire.md) | On-wire byte encoding -- message framing, op headers, digests, extent tables. |
| [docs/transport.md](docs/transport.md) | TCP transport binding: connection lifecycle, admission/auth, deadlines, reconnect. |
| [docs/client-internals.md](docs/client-internals.md) | The client's hard part -- dLSN tracking, the read horizon, split reads, and what a peer's answer lets the client conclude. |

## Key design properties

- **Ack at quorum, never at the slowest.** A write returns as soon as a majority acks; the straggler keeps
  running detached and still lands the write late (delivered, not lost).
- **Term-fenced single writer.** Every IO carries the session `term`; a deposed client's IOs (even keep_alive)
  are rejected `STALE_TERM`, so it cannot keep the session alive.
- **Client drives commit (piggybacked, no standalone verb).** The client stamps `commit_lsn` on every
  write / read / keep_alive; replicas apply strictly in dLSN order at the contiguous frontier.
- **Byte-based, one buffer type.** `addr`/`len` are byte offsets (block-aligned to `lba_size`); a single
  caller-owned `sisl::sg_list` is used both ways -- an **empty** write buffer is a zero write.
- **Thin + sparse.** Writes may be payload-free (`WRITE_ZEROES`); reads return a sparse `io_extent` layout
  (data vs holes) and collapse all-zero regions to holes. A **hole** (reads-as-zero) is not **Missing**
  (known-but-not-yet-received).
- **Client-routed reads.** Reads are unicast to one eligible member by LBA-overlap against the per-replica
  Missing map (plus `Synced ≥ L`, the login dLSN). The read path never fetches from a peer; fetch is resync-only.

## Glossary

| Term | Definition |
|---|---|
| **CRAFT** | Client Assisted RAFT -- the replication protocol. |
| **dLSN** | Data LSN -- a dense, per-partition sequence number in the data journal; the only LSN CRAFT itself uses. |
| **term** | CRAFT session term, incremented on every client login; replicas reject stale-term IOs. |
| **commit_lsn (≡ Synced)** | The contiguous applied prefix; every dLSN ≤ it is applied to the LBA index in dLSN order. |
| **last_append_lsn** | Highest dLSN whose data is in the journal (possibly uncommitted). |
| **Missing** | A dLSN a replica knows about but has not received data for -- the read-eligibility signal. |
| **Empty** | A dLSN proven never quorum-durable; a permanent no-op the commit skips. |
| **hole** | A read sub-range with no data (never/zero-written); returned as a marker, read as zeros. Not `Missing`. |
| **io_extent** | One sub-range of a read's sparse layout, in bytes: `{addr, len, hole}` -- carries no bytes. |
| **client_hdr** | Session + watermark fields stamped on every IO: `{term, commit_lsn, all_committed_lsn}`. |
