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

// The per-replica CLIENT-SIDE surface -- one member of a partition, as the CLIENT addresses it. The client holds
// N of these and issues its ops 1:1, never knowing whether a given one is a TCP proxy to a remote server
// (CraftTcpReplica -- the production case, the near half of a transport) or the in-memory reference model
// (MemCraftReplica -- test/dev support, no wire and no storage engine).
//
// This is NOT a storage contract, and a production backend (e.g. HomeBlocks) never CONSTRUCTS a client -- no
// make_client on that side, ever. The backend lives on the FAR side of a wire: it exposes its own per-replica API
// and a wire server (craft_tcp_server here; a CraftConnector there) decodes the wire onto that API. Whether a
// backend also reuses this interface as its internal volume->engine vtable is its own business; the client never
// sees it.
//
// Engine-free: only craft/types.hpp + sisl (result / async::result carrier + sg_list). No storage engine.

#include <cstdint>
#include <memory>
#include <vector>

#include <sisl/fds/buffer.hpp> // sisl::sg_list

#include <craft/client.hpp> // client_handle (the opaque handle make_client returns)
#include <craft/types.hpp>  // the CRAFT vocabulary + the result / async_result aliases

struct io_uring; // liburing (fwd-decl only: the on-ring data-path seam is a pointer, see prepare_for_async)

namespace craft {

// ── internal / peer-only data types (deliberately NOT on the public client surface) ──

// Per-partition CRAFT state (internal to a replica implementation). Authoritative in memory; the
// production impl recovers it from the journal + superblock on restart (the reference model does not).
struct CraftPartitionState {
    int64_t commit_lsn{-1};      // contiguous committed prefix (== Synced)
    int64_t last_append_lsn{-1}; // highest appended dLSN (may be uncommitted)
    uint64_t client_token{0};    // token from the last successful InternalLogin
    uint64_t term{0};            // current session term
};

// Block-addressing units for the replica-side journal/index representation. The CRAFT client API and wire are
// byte-based (raw uint64_t addr/len); only JournalSlot (below) and the reference model's per-block index speak
// these block units, so they live here with the peer/journal types, not in the client vocab (craft/types.hpp).
using lba_t = uint64_t;
using lba_count_t = uint32_t;

// One journal slot returned by fetch_data() (server-to-server resync; not client-facing). Four-way:
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

class craft_replica {
public:
    virtual ~craft_replica() = default;

    // ── client-facing (what a CRAFT client issues against this one replica) ──

    // Login: run the session-establishment sequence. A follower returns LoginResult{term=0, leader_hint}
    // (not an error) so the client can redirect; craft_error::NO_QUORUM / REPLICA_DOWN are errors.
    virtual async_result< LoginResult > login(uint64_t client_token) = 0;

    // Explicit logout. Term-fenced; leader propagates InternalLogout to all live replicas so subsequent
    // IOs with the old term fail with STALE_TERM. Returns craft_error::NOT_LEADER on a follower.
    virtual async_status logout(client_hdr hdr) = 0;

    // Append one client-assigned write at slot `dlsn`. `addr`/`len` are BYTE offset/length, aligned to
    // the volume's lba_size (else std::errc::invalid_argument). `data` is a caller-owned (iomgr) buffer:
    // empty (size 0) => a zero write (WRITE_ZEROES / unmap; `len` is the range); non-empty => a data
    // write of exactly `len` bytes (scatter-gather, any iovec count). Does NOT apply to the index; the
    // frontier is advanced by hdr.commit_lsn (piggybacked commit). craft_error::STALE_TERM if
    // hdr.term != the session term. Returns the replica's {commit_lsn, last_append_lsn} after the append --
    // every IO response piggybacks the watermarks, so any round-trip refreshes the client's view.
    virtual async_result< lsn_pair > write(client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
                                           sisl::sg_list data) = 0;

    // Latest version <= read_lsn (horizon H) for [addr, addr+len) (BYTE offset/length, aligned). Fills the
    // caller-owned `dest` buffer in place (scatter-gather; data sub-ranges get bytes, holes get zeros) and
    // returns the sparse layout (data vs holes) plus the piggybacked {commit_lsn, last_append_lsn}.
    // Advances the frontier to hdr.commit_lsn. STALE_TERM on term mismatch.
    virtual async_result< read_result > read(client_hdr hdr, int64_t read_lsn, uint64_t addr, uint64_t len,
                                             sisl::sg_list dest) = 0;

    // Advance the frontier toward hdr.commit_lsn + reset the client-liveness watchdog -- which is WHY
    // it is term-fenced: a stale client must not be able to keep the session alive. Returns the
    // achieved {commit_lsn, last_append_lsn}. No standalone commit verb; keep_alive is its carrier.
    virtual async_result< lsn_pair > keep_alive(client_hdr hdr) = 0;

    // The client-requested resolution round (the design's client-request SyncRSCommitLSN trigger): resolve
    // every unresolved slot <= `upto` NOW -- fetch each from a holder, or, on quorum-lacks evidence, declare
    // it Empty -- instead of waiting for the watchdog / periodic cadence. The round itself is LEADER work,
    // but the client cannot know who leads mid-session (it learned the leader at login; leadership may have
    // moved), so it BROADCASTS this to every member, at most one outstanding per peer: whichever member is
    // the current leader runs the round, and a non-leader answers craft_error::NOT_LEADER (a real replica may
    // instead forward to its leader -- either is a valid implementation of this verb). The client fires it
    // after a failed (sub-quorum) write and retires the covered slots off the returned verdicts. NOTE the
    // watermark semantics: a write <= `upto` that had not landed ANYWHERE when the leader resolved is
    // verdicted Empty, and a replica rejects a late arrival into an Empty slot -- so that write's own ack
    // path sees deterministic rejects and fails, the undefined-outcome contract for un-acked IO. Term-fenced
    // (STALE_TERM).
    virtual async_result< resolution_result > request_resolution(client_hdr hdr, int64_t upto) = 0;

    // Bind this backend's data path to a host io_uring `ring` (a ublk queue's, or a test's) for on-ring async
    // completion. Called once per ring, OFF the IO path (never concurrently with an in-flight op). Default
    // no-op: transports that don't submit on a caller-provided ring (the worker-thread TCP client, the
    // in-process reference over its own pool) ignore it and keep their existing completion source. After this,
    // write/read/keep_alive submit their SQEs on `ring` and complete via sisl::async::cqe_state, which the ring
    // owner's reap loop dispatches -- so many ops go in flight at once (QD>1) on the caller's thread.
    virtual void prepare_for_async(::io_uring* /*ring*/) noexcept {}

    // ── peer-facing (server-to-server; driven by the cold path / resync, never by a client) ──

    // Snapshot {commit_lsn, last_append_lsn} for this replica -- used by the leader during
    // GetRSCommitLSN polls and SyncRSCommitLSN rounds; identical to what keep_alive returns.
    virtual async_result< lsn_pair > get_lsns() = 0;
    virtual async_result< lsn_pair > get_rs_commit_lsn() = 0;
    virtual async_result< std::vector< JournalSlot > > fetch_data(std::vector< int64_t > lsns) = 0;
    virtual async_status truncate(int64_t lsn) = 0;

    // This replica's endpoint id (for routing / membership).
    virtual peer_id_t id() const = 0;
};

} // namespace craft
