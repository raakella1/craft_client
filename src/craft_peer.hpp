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

// THE PEER PLANE -- a replica's view of another replica. This is the OTHER of CRAFT's two planes, and it shares
// nothing with the first but the codec:
//
//                    │ caller            │ callee   │ trigger              │ wire
//   ── client plane ─┼───────────────────┼──────────┼──────────────────────┼──────────────────────
//   craft_replica    │ craft_client      │ a member │ user IO              │ wire::op 1..14  (spec'd)
//   craft_peer       │ a REPLICA         │ a holder │ applying a RAFT entry│ NOT ALLOCATED   (deferred)
//                    │ (CraftReplDev, as │          │ (SyncRSCommitLSN /   │
//                    │  a side effect of │          │  InternalLogin)      │
//                    │  a RAFT commit)   │          │                      │
//
// So the peer plane's INITIATOR is the storage backend itself: on applying a SyncRSCommitLSN entry a replica
// discovers slots it is Missing and pulls them from a holder. That is why these verbs must not hang off
// craft_replica -- the CRAFT client never calls one of them, and a client-side transport proxy (CraftTcpReplica)
// cannot answer one. Conflating them made four pure virtuals that every implementer had to stub.
//
// It also means HomeBlocks is BOTH ends of this plane (it initiates on RAFT commit and it serves peers), while it
// is only the FAR end of the client plane. "No CRAFT client in HomeBlocks" still holds -- make_client builds the
// IO client, which is a different thing from a peer proxy.
//
// STATUS: DEFERRED, and deferred at the WIRE, not just the transport. craft::wire::op stops at 14 (resolve_rsp);
// no opcode is allocated for any verb below, and wire::is_response() hardcodes that bound. Peer ops take 15+.
// Today the ONLY implementer is MemCraftReplica, and the model's stand-in for the peer network is MemTransport's
// cold path (run_login / run_resolution), which reaches the model's cold_* / peek_* helpers directly as a friend
// rather than through this interface. Wiring MemTransport through craft_peer is the first step to making the
// plane real; the second is allocating the opcodes. See docs/peer-plane.md.

#include <cstdint>
#include <vector>

#include <sisl/fds/buffer.hpp> // sisl::sg_list

#include <craft/types.hpp> // the CRAFT vocabulary + the result / async_result aliases
#include <craft/client.hpp> // result types

namespace craft {

// Block-addressing units for the replica-side journal/index representation. The CRAFT client API and wire are
// byte-based (raw uint64_t addr/len); only JournalSlot (below) and a replica's own per-block index speak these
// block units -- which is exactly why they live on the PEER plane and not in the client vocab (craft/types.hpp).
using lba_t = uint64_t;
using lba_count_t = uint32_t;

// One journal slot returned by fetch_data(). Server-to-server only: this never crosses the CLIENT wire. Four-way:
// data (is_empty=false, all_zeros=false), zero write (all_zeros=true, no data), Empty (is_empty=true),
// or omitted from the response (not-present-here).
struct JournalSlot {
    int64_t lsn{-1};
    bool is_empty{false};
    bool all_zeros{false};
    lba_t lba{0};
    lba_count_t len{0};
    sisl::sg_list data{};
};

// The peer-facing surface of one replica: what a PEER (not a client) may ask of it. Driven by the leader during
// login's GetRSCommitLSN / SyncRSCommitLSN rounds and by any replica applying a RAFT entry that leaves it Missing
// slots. A CRAFT client never invokes one of these.
class craft_peer {
public:
    virtual ~craft_peer() = default;

    // Snapshot {commit_lsn, last_append_lsn} for this replica -- the leader's GetRSCommitLSN poll. Identical to
    // what the client plane's keep_alive returns, but asked by a peer, not a client.
    virtual async_result< lsn_pair > get_lsns() = 0;
    virtual async_result< lsn_pair > get_rs_commit_lsn(uint64_t term, bool is_login) = 0;

    // Pull raw journal data for the requested dLSNs -- the resync fetch. A slot this replica has verdicted Empty
    // comes back as JournalSlot{.is_empty = true} rather than an error; a slot it simply does not hold is omitted.
    virtual async_result< std::vector< JournalSlot > > fetch_data(std::vector< int64_t > lsns) = 0;

    // Drop every journal entry above `lsn` -- the leader trimming a stale tail during login recovery.
    virtual async_status truncate(int64_t lsn) = 0;
};

} // namespace craft
