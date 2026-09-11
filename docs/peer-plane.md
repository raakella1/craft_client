# The peer plane (deferred -- and deferred at the *wire*)

CRAFT has **two planes**. The client plane is built, specified and tested. The peer plane is not, and this page
records exactly how far the deferral goes, because the honest answer is further than "we haven't written the
transport yet."

|  | **Client plane** | **Peer plane** |
|---|---|---|
| Interface | `craft_replica` (`src/craft_replica.hpp`) | `craft_peer` (`include/craft/peer.hpp`) |
| Caller | `craft_client` -- the IO client | **a replica**, applying a RAFT entry |
| Callee | a member | another member (a *holder*) |
| Trigger | user IO | committing `SyncRSCommitLSN` / `InternalLogin` |
| Verbs | `login` `logout` `write` `read` `keep_alive` `request_resolution` | `get_lsns` `get_rs_commit_lsn` `fetch_data` `truncate` |
| Wire opcodes | `wire::op` **1-14**, complete | **none allocated** |
| Near half | `CraftTcpReplica` | -- |
| Far half | `craft_tcp_server` (reference) / `CraftConnector` (planned) | -- |

They share nothing but the codec.

## Why they are separate interfaces

They were one interface, and it cost four pure virtuals that nobody could honor:

- `craft_client` **never calls a peer verb** -- not one, anywhere.
- `CraftTcpReplica` had to stub all four `NOT_LEADER`. It could not do otherwise: a client-side proxy has no
  journal with which to answer a peer.
- Even the reference model routed *around* them. `MemTransport`'s cold path (`run_login`, `run_resolution`) is a
  `friend` of `MemCraftReplica` and drives its concrete `cold_*` / `peek_*` helpers directly.

So the one component that conceptually needed the peer verbs never used them, and the two that were forced to
declare them could not implement them. `craft_replica` is now exactly the wire's client ops -- six verbs, six
opcodes, 1:1 -- and `craft_peer` names the other plane so the deferral is explicit rather than accidental.

## Who initiates the peer plane

**The storage backend does.** A replica applying a `SyncRSCommitLSN` entry discovers dLSNs it is *Missing* and
pulls them from a holder. That makes `CraftReplDev` an **initiator** on this plane: it will hold N-1 peer proxies
and call `fetch_data` on RAFT commit.

This does not contradict "there is no CRAFT client in HomeBlocks." `make_client` builds the **IO client** -- the
thing that assigns dLSNs, broadcasts writes and acks at quorum. A peer proxy is a different thing entirely.
The precise statement:

> HomeBlocks is **both ends** of the peer plane, and **only the far end** of the client plane.

## What has to happen to make it real

1. **Allocate the opcodes.** `wire::op` stops at `resolve_rsp = 14`. Peer ops start at **15** and must keep the
   request-odd / response-even convention. `wire::op::k_max_op` bounds `is_response()`; a peer op added without
   bumping it is silently misclassified as "not a response" rather than failing loudly. That constant exists to
   close that trap -- do not re-introduce a literal.
2. **Route `MemTransport` through `craft_peer`** instead of the `friend` + `cold_*` shortcut. Until that happens,
   `craft_peer` has exactly one implementer (`MemCraftReplica`) and zero callers, and the model's leader
   orchestration is not actually exercising the interface it is supposed to model.
3. **Write the two halves** -- a peer proxy and a peer server -- on whatever channel is chosen. Note this is a
   *separate* choice from the client plane's transport: the client plane is TCP/io_uring, but the peer plane runs
   between HomeBlocks nodes, which already have an inter-node RPC channel it may prefer to ride.

Until (1) and (2), any claim that the peer transport is "just pluggable" is wrong: there is nothing to plug into.

---

# Variant: server-side fan-out (parked, not built)

A recurring question: *can the replication fan-out move off the client and onto a fast server-side fabric?* It can,
and the client model survives almost intact. This section records how, and what it costs, so the idea can be picked
up (or answered) without re-deriving it.

**The shape.** The client keeps sessions with all N members and keeps assigning dLSNs, but it **unicasts** a write to
a single **ingress** member. The ingress appends it, acks, and forwards it to the other members over a fast
server-side fabric. The client learns that the other members have it from the watermark on their **keep_alive**
replies, and acks the write to the application once a quorum of members hold it. Reads are untouched: they are
already unicast to one eligible member and routed by the same map.

**The win.** The client sends the payload **once instead of N times**, so a 3-way set costs 1x client write
bandwidth instead of 3x. Reads already cost 1x, so the client link stops paying the replication tax entirely and the
fan-out moves to a backend fabric where bandwidth is cheap. On a bandwidth-bound client NIC this is roughly a 3x
write-throughput increase, and it is the whole reason to consider the variant.

## What does not change

- **dLSN assignment stays client-side.** The ingress orders nothing; it forwards a slot the client already numbered.
  CRAFT's core invariant is untouched.
- **Reads.** Unicast to one eligible member, routed by the two-tier map. Zero change.
- **The commit frontier, the piggybacked commit, and the reclaim floor** (`all_committed_lsn = min` across members).
- **The keep_alive channel already exists.** `drive_keepalives(exclude_idx)` already fires a keep_alive at every
  member that did NOT get the current IO. In this variant that is precisely the set whose watermark the client is
  waiting on. The polling machinery is already built.
- **`read_route_map`'s holder bitmask.** Already a bitmask; it just gets filled from a watermark rather than from N
  per-leg acks.

## The trap: last_append_lsn is NOT an ack

The tempting move is to let the client count member B as a holder of dLSN `d` once B's keep_alive reports
`last_append_lsn >= d`. **Do not.** `last_append_lsn` is the HIGHEST dLSN the replica has appended, not a promise
that it has everything below it. The model says so explicitly: `missing_count` counts Missing slots in
`(commit_lsn, last_append_lsn]`, so **holes live below last_append_lsn by construction**.

With a single ingress and an order-preserving forward channel, B's appends happen to be dense and the shortcut looks
correct. It breaks at **ingress failover**, which is the case the replica set exists for:

> A is the ingress. It has forwarded `d..d+4` to B, but dies before C receives `d+1..d+4`. The client fails over to
> B as ingress; B forwards from `d+5`. C appends `d+5`, so C's `last_append_lsn` jumps to `d+5` **while it is
> Missing `d+1..d+4`**. C's next keep_alive reports `d+5`, the client counts C as a holder of `d+2`, acks it at
> quorum, and sets C's holder bit. A later read of `d+2` is then routed to C, which serves the older block.

That is silent wrong data, from a watermark that was never a contiguity proof.

## The fix: a dense receive watermark

Report a third watermark: the highest dLSN with **no Missing slot at or below it** (first-Missing minus one). Call it
`appended_upto`.

- **It is free.** The replica already tracks its Missing set; that set is exactly what `apply_up_to()` stalls on.
- **It is not `commit_lsn`.** `commit_lsn` is the contiguous *applied* prefix, and it only advances when the client
  stamps a commit, which the client can only do once it has quorum. Circular: `commit_lsn` cannot be the ack,
  because the ack is what produces it. `appended_upto` is the contiguous *received* prefix and depends on nothing
  the client does.
- **It is self-certifying.** Correct however the writes arrived: across failover, reconnect, and retransmit gaps.
  It answers exactly the question the client is asking ("is it safe to read `d` from here?") unconditionally.
- **It is cumulative.** One keep_alive reply carrying `appended_upto = 5000` acks writes `1..5000` at that member in
  a single message. Today every write costs an ack from every member. This is a real amortization win, not just a
  workaround.

The write then completes like this, with the durability contract (ack at quorum) preserved:

```
client -> A       write(d)                   A appends, acks           holder #1 (direct)
A -> B, C         forward(client_hdr, d)     over the fabric
client -> B, C    keep_alive                 reply: appended_upto >= d  holder #2 (cumulative)
                                             -> quorum -> ack to the application
```

## What it costs

1. **The peer plane stops being a cold path and becomes THE data path.** Today it is resync-only. Here every write
   crosses it, so it must be as fast as the client wire, and the forward **must carry the client's `client_hdr`** --
   the `term` above all, because B and C still have to fence a deposed client's writes. A forward that skips
   term-fencing breaks the single-writer guarantee. This is the real cost of the variant, and it is why it is parked
   behind the rest of this document.
2. **The "provably Empty" fast path is lost.** Today the client sees each member's own answer and distinguishes ack
   / deterministic reject / timeout; when every member deterministically rejects, the slot is *provably Empty*, a
   resolved no-op. With only the ingress answering directly, the client cannot observe that, so a failed write must
   go through a resolution round instead. RESOLVE becomes more important in this variant, not less.
3. **Write latency grows by a keep_alive round** (ingress ack, then the keep_alive that carries `appended_upto` past
   `d`). At depth this is a latency tax, not a throughput one, since the evidence for `d` lands while `d+1..d+k` are
   in flight. If that tax is unacceptable, the alternative is for the **ingress to wait for the fabric quorum and
   return a holder bitmask in its reply**: one client round-trip, contract preserved, and the client's
   `record_completion(dlsn, idx, acked)` simply becomes `record_holders(dlsn, mask)` with the route map unchanged.
