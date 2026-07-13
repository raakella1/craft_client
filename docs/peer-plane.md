# The peer plane (deferred — and deferred at the *wire*)

CRAFT has **two planes**. The client plane is built, specified and tested. The peer plane is not, and this page
records exactly how far the deferral goes, because the honest answer is further than "we haven't written the
transport yet."

|  | **Client plane** | **Peer plane** |
|---|---|---|
| Interface | `craft_replica` (`src/craft_replica.hpp`) | `craft_peer` (`src/craft_peer.hpp`) |
| Caller | `craft_client` — the IO client | **a replica**, applying a RAFT entry |
| Callee | a member | another member (a *holder*) |
| Trigger | user IO | committing `SyncRSCommitLSN` / `InternalLogin` |
| Verbs | `login` `logout` `write` `read` `keep_alive` `request_resolution` | `get_lsns` `get_rs_commit_lsn` `fetch_data` `truncate` |
| Wire opcodes | `wire::op` **1–14**, complete | **none allocated** |
| Near half | `CraftTcpReplica` | — |
| Far half | `craft_tcp_server` (reference) / `CraftConnector` (planned) | — |

They share nothing but the codec.

## Why they are separate interfaces

They were one interface, and it cost four pure virtuals that nobody could honor:

- `craft_client` **never calls a peer verb** — not one, anywhere.
- `CraftTcpReplica` had to stub all four `NOT_LEADER`. It could not do otherwise: a client-side proxy has no
  journal with which to answer a peer.
- Even the reference model routed *around* them. `MemTransport`'s cold path (`run_login`, `run_resolution`) is a
  `friend` of `MemCraftReplica` and drives its concrete `cold_*` / `peek_*` helpers directly.

So the one component that conceptually needed the peer verbs never used them, and the two that were forced to
declare them could not implement them. `craft_replica` is now exactly the wire's client ops — six verbs, six
opcodes, 1:1 — and `craft_peer` names the other plane so the deferral is explicit rather than accidental.

## Who initiates the peer plane

**The storage backend does.** A replica applying a `SyncRSCommitLSN` entry discovers dLSNs it is *Missing* and
pulls them from a holder. That makes `CraftReplDev` an **initiator** on this plane: it will hold N−1 peer proxies
and call `fetch_data` on RAFT commit.

This does not contradict "there is no CRAFT client in HomeBlocks." `make_client` builds the **IO client** — the
thing that assigns dLSNs, broadcasts writes and acks at quorum. A peer proxy is a different thing entirely.
The precise statement:

> HomeBlocks is **both ends** of the peer plane, and **only the far end** of the client plane.

## What has to happen to make it real

1. **Allocate the opcodes.** `wire::op` stops at `resolve_rsp = 14`. Peer ops start at **15** and must keep the
   request-odd / response-even convention. `wire::op::k_max_op` bounds `is_response()`; a peer op added without
   bumping it is silently misclassified as "not a response" rather than failing loudly. That constant exists to
   close that trap — do not re-introduce a literal.
2. **Route `MemTransport` through `craft_peer`** instead of the `friend` + `cold_*` shortcut. Until that happens,
   `craft_peer` has exactly one implementer (`MemCraftReplica`) and zero callers, and the model's leader
   orchestration is not actually exercising the interface it is supposed to model.
3. **Write the two halves** — a peer proxy and a peer server — on whatever channel is chosen. Note this is a
   *separate* choice from the client plane's transport: the client plane is TCP/io_uring, but the peer plane runs
   between HomeBlocks nodes, which already have an inter-node RPC channel it may prefer to ride.

Until (1) and (2), any claim that the peer transport is "just pluggable" is wrong: there is nothing to plug into.
