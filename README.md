# craft_client -- the CRAFT reference client, wire protocol, and reference model

[![Conan Build](https://github.com/szmyd/craft_client/actions/workflows/conan_build.yml/badge.svg?branch=dev/v0.x)](https://github.com/szmyd/craft_client/actions/workflows/conan_build.yml)
[![codecov](https://codecov.io/github/szmyd/craft_client/graph/badge.svg?token=JW7LC97YM2)](https://codecov.io/github/szmyd/craft_client)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

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
| **`craft_types`** | The domain vocabulary (`lsn_pair`, `io_extent`, `client_hdr`, `LoginResult`, `craft_error`, `peer_id_t`) + the `async_result` aliases. Header-only, and the **only** thing a storage backend needs. | sisl, boost |
| **`craft_client`** | The client behind the opaque handle: dLSN assignment, quorum broadcast, read routing + failover, login/redirect -- driven by free functions, over pluggable `craft_replica` backends. Plus the io_uring TCP transport. | craft_wire, craft_types, sisl, liburing |
| **`craft_reference`** | An in-memory reference `craft_replica` + a loopback cluster server + a TCP server, so the client can be driven end-to-end with **no storage engine**. Reached publicly only through the in-process builder (`craft/local.hpp`). Test/dev-support. | craft_client |

The result vocabulary itself (`result<T>` / `async_result<T>`) is owned by **sisl** (`sisl::result`,
`sisl::async::light_result` -- the freestanding, stdexec-free task) so a domain error (`craft_error`) rides the
type-erased `std::error_condition` and no layer forks the vocabulary. The verbs are co_await-able from **any**
coroutine (a ublk driver's `disk_task`, an `exec::task`, another `light_task`); the awaiting coroutine resumes
on the thread that completes the op -- the reap thread of the ring passed to the verb's async overload, else a
transport-internal thread. Blocking callers use `sisl::async::sync_get`.

## Public API surface

The client is an **opaque handle + free-function verbs** -- a driver never sees the `craft_client` type, and the
construction seam is separate so the *backend* (not the client) is what varies. The surface is intentionally
tiny, split by **three disjoint audiences**:

**`include/craft/` is the whole of it -- eight headers.** Everything else (the `craft_replica` interface, the
reference model, the TCP proxy/client/server) lives under `src/` and is not shipped in the package at all:

| Header | Audience | Contents |
|---|---|---|
| `craft/types.hpp` | everyone (and a **storage backend**'s only dependency) | the vocabulary + `async_result` aliases |
| `craft/client.hpp` | **drivers** -- a ublk disk, a test, an app | `client_handle` + `make_client` + `login`/`write`/`read`/`flush`/`logout`/`drive_keepalives` + observers |
| `craft/tcp.hpp`, `craft/local.hpp` | **drivers**, assembling a binary | backend builders `make_tcp_cluster` / `make_local_cluster` → an **opaque handle**; `backends(handle)` feeds `make_client` |
| `craft/wire.hpp`, `craft/status.hpp` | **server authors** (the future `CraftConnector`) | the codec + the wire↔`craft_error` bridge |
| `craft/net/conn.hpp` | **server authors** | the socket + message framing (`recv_message` / `send_all`) a wire server terminates on |
| `craft/peer.hpp` | **storage backend** | the peer communication interface + methods for serialize/deserialize peer API objects |

`craft_replica` is **opaque even to a driver**: `craft/client.hpp` only forward-declares it, and a driver passes the
builder's `std::vector<std::shared_ptr<craft_replica>>` straight to `make_client` without ever naming or
dereferencing one (a `shared_ptr` type-erases its deleter at construction, inside the builder). The `test_api`
suite compiles against `include/` alone and exists to fail loudly if the public surface ever stops sufficing.

A consumer never names the client type -- it picks a backend builder, hands its backends to `make_client`, and drives
the handle with the verbs. The builder is an RAII **owner** of the backends; keep it alive for (and destroy it
after) the client:

```cpp
auto cluster = craft::make_tcp_cluster(endpoints, vol_id); // <craft/tcp.hpp> -- or make_local_cluster(...)
auto c = craft::make_client(craft::backends(cluster));     // <craft/client.hpp> -- the one construction seam
co_await craft::login(c, token);                           // <craft/client.hpp> -- verbs over the handle
co_await craft::write(c, addr, len, buf);
```

Every builder follows the client's shape: an **opaque handle + free functions**, never a concrete class on the
surface (`make_local_cluster` also exposes `force_subquorum(handle, ...)` / `set_replica_up(handle, ...)` fault
verbs the same way).

### `craft_replica` is the CLIENT'S view of a member -- not a storage contract

This is the seam's most-mistaken point, so it is worth stating flatly: **a production storage backend never
implements `craft_replica`, never calls `make_client`, and never links this package's client at all.** Everything
above is the *initiator* side of the wire.

`craft_replica` is "one member, as the client addresses it", and exactly two kinds of thing implement it:

- a **transport proxy** -- holds no state, marshals each verb onto a wire and unmarshals the reply
  (`CraftTcpReplica`). This is the **near half** of a transport, and it is the only implementation that ships in a
  production binary.
- the **in-process reference** (`MemCraftReplica`) -- a stand-in that lets the client be driven end-to-end with no
  wire and no storage engine at all (`make_local_cluster`). Test/dev support.

The real storage sits on the **far side of a wire, always**, and is reached only through a server:

| Half | In-tree example | Needs |
|---|---|---|
| **near** -- the `craft_replica` proxy the client holds | `CraftTcpReplica` + the `make_tcp_cluster` builder | the interface (`src/craft_replica.hpp`) + `craft/wire.hpp` (encode) + `craft/status.hpp` (reply status byte → `craft_error`) |
| **far** -- a server that terminates the wire and calls the backend's own API | `craft_tcp_server` (over the reference model) | `craft/wire.hpp` (decode) + `craft/status.hpp` (`craft_error` → status byte) + `craft/net/conn.hpp` (framing) + **the backend's surface, whatever it is** |

Note the asymmetry: the **far** half needs nothing but the public codec, so `CraftConnector` links `craft_wire` and
terminates on `craft_conn` without ever seeing this package's client. The **near** half is internal, because every
transport that will exist lives in this repo -- adding one (RDMA, Homa) means adding a proxy under `src/net/` and a
builder header, not implementing an exported interface.

Both halves belong to the **transport author**. The **backend author** (HomeBlocks) writes neither: it exposes its
own per-replica API and a wire server adapts to it. HomeBlocks depends on this package only for `craft_wire` +
`craft_types` -- the vocabulary and the codec -- and *never* for the client.

> **Each backend fronts its own server; they share the codec, not an interface.** `craft_tcp_server` hardwires
> `MemCraftReplica` and calls its concrete `srv_*` methods. HomeBlocks' `CraftConnector` will adapt this server to
> call the HomeBlocks per-replica API instead (`get_replica(volume_id)` → an opaque handle → the free-function
> verbs), keeping HomeBlocks' own API -- not a C++ interface from this package -- as the boundary. The shared part
> is `craft/wire.hpp` + `craft/status.hpp`; the per-backend part is the handful of `on_*` handlers. So **the wire is
> what forces the two servers to agree**, and nothing else does.
>
> Note the term does **not** originate in the server. A real backend answers LOGIN from its RAFT leader and returns
> the session term, so the connector *forwards* LOGIN and then stamps the returned term on subsequent IO. The
> reference server mints `next_term_++` itself only because `MemCraftReplica` has no leader to ask.

> **The far half has no declared interface.** `craft_tcp_server` calls `MemCraftReplica`'s concrete `srv_*` methods;
> a HomeBlocks CRAFT server would call HomeBlocks' own per-replica API. The verb set is the same shape --
> `establish(token, term)` / `end()` / `write` / `read` / `keep_alive` / `resolve` / `lsns` -- but it is **not**
> `craft_replica` verbatim: session establishment is the *server's* job (it assigns the term on LOGIN/HELO and
> pushes it down), and the peer-facing verbs (`fetch_data` / `truncate` / `get_rs_commit_lsn`) never cross the
> client wire. Nothing in this package forces the two servers to agree on that surface; **only the wire does.**
> (The reference needs a separate `srv_*` seam only because its `craft_replica` methods route through
> `MemTransport`, the model's stand-in network -- which a real wire replaces. A real backend has no such detour.)

The `craft_client` class itself, `dlsn_tracker`, `read_route_map`, and the io_uring TCP internals all live in
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
| `net/` | The TCP transport: `craft_conn`, the wire-only `wire_client`, `CraftTcpReplica` (the `craft_replica` proxy), `craft_async_conn` (the on-ring data path -- every mid-session verb on the caller's io_uring), `craft_session_mgr` (the ONE process-wide admin thread; retires with the last proxy). |

The generic pieces this leans on -- `sisl::result`, the freestanding `sisl::async::light_task` (with its
`.detach()` fire-and-forget mode and `sync_get` blocking bridge), and the `light_task` forms of
`sisl::async::when_all`/`when_quorum` -- were hoisted **into sisl**, so nothing here forks them.

## Building

Requires a conan 2.x profile with C++23. sisl / liburing resolve from your remotes (or editable checkouts).

```sh
conan install . -of=build --build=missing -s build_type=Debug -s compiler.cppstd=23
cmake --preset conan-debug
cmake --build --preset conan-debug
ctest --preset conan-debug            # 13 suites: wire codec, types, public API (in-process + TCP), mem model, client, transport
```

Sanitizers: add `-o sanitize=address` (or `thread`) to the `conan install`.

## Relationship to HomeBlocks and ublkpp

The three roles map cleanly onto three repos, and **HomeBlocks is only ever the backend**:

- **HomeBlocks is the replica -- the far side of the wire, never a client.** It exposes its own per-replica CRAFT
  API (free functions over a `volume_handle`, backed by `CraftReplDev`) and a wire-speaking server (the planned
  `CraftConnector`) adapts wire requests onto it -- structurally what `craft_tcp_server` does over the reference
  model. **`make_client` never appears in the HomeBlocks repo**, and no HomeBlocks class implements `craft_replica`.
  Its dependency here is `craft_wire` + `craft_types` only: the codec and the vocabulary.
- **The transport is the middle, and it is written once.** `CraftTcpReplica` + `make_tcp_cluster` (the near half)
  are already generic -- they do not care what is behind the socket. So HomeBlocks integration is *only* the far
  half: a server that decodes the wire and calls the HomeBlocks API. The client will not know the difference.
- **ublkpp's `craft_disk` is the driver.** Written once against the driver surface (`client_handle` + the verbs), it
  is agnostic to everything below: standalone it drives the reference model in-process or over TCP; against
  HomeBlocks the same handle carries the same TCP proxy, pointed at a HomeBlocks server. `craft_client` knows
  nothing about ublk.

## Documentation

| Doc | Contents |
|---|---|
| [docs/wire.md](docs/wire.md) | On-wire byte encoding -- message framing, op headers, digests, extent tables. |
| [docs/transport.md](docs/transport.md) | TCP transport binding: connection lifecycle, admission/auth, deadlines, reconnect. |
| [docs/client-internals.md](docs/client-internals.md) | The client's hard part -- dLSN tracking, the read horizon, split reads, and what a peer's answer lets the client conclude. |
| [docs/peer-plane.md](docs/peer-plane.md) | CRAFT's **other** plane (replica ↔ replica, driven by a RAFT commit): why it is a separate interface, and why it is deferred at the *wire* -- no opcode is allocated. |

## Key design properties

- **Ack at quorum, never at the slowest.** A write returns as soon as a majority acks; the straggler keeps
  running detached and still lands the write late (delivered, not lost).
- **One admin thread per process; no client threads on the data path.** Every mid-session verb -- write, read,
  keep_alive, resolve -- has an async overload that takes the caller's io_uring and runs on it. Multi-queue
  (blk-mq) falls out of that: each of a driver's `nr_hw_queues` threads passes its own ring, and the transport
  keeps one data connection per (queue, replica) -- the `nr_hw_queues x N` grid -- so no queue ever crosses
  another's thread. Blocking admin work (login/logout, which bracket every ring's lifetime) serializes on a
  single process-wide session-mgr thread shared by every proxy: a 50-disk RAID0 at N=3 idles one thread, not
  150, and it retires when the last disk detaches. Connects are always deadline-bounded, so a blackholed
  replica cannot park that thread.
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
