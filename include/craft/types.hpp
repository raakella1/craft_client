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

// CRAFT client-facing data types -- the pure-data structs the client API takes and returns. Deliberately
// engine-free and self-contained (only boost + sisl's ENUM + std): the reference model, the transport, and
// any consumer (a real HomeStore backend, the ublk driver) share one definition without pulling a storage
// engine. Internal / peer-only types (CraftPartitionState, JournalSlot) live in craft/replica.hpp.
//
// peer_id_t is a plain boost uuid here; a consumer that has its own uuid id-type (e.g. homeblocks' node
// identity) interoperates freely because it is the SAME underlying type.

#include <cstdint>
#include <string>
#include <system_error>
#include <vector>

#include <boost/uuid/uuid.hpp>   // boost::uuids::uuid (== peer_id_t)
#include <sisl/async/result.hpp> // sisl::async::result / ::status (the co_await-able result carrier)
#include <sisl/result.hpp>       // sisl::result / ::status / ::ok
#include <sisl/utility/enum.hpp> // ENUM

namespace craft {

// The operation-result vocabulary the client API speaks: a flat local spelling of sisl's canonical result
// types, so a consumer includes just this header for both the data types and the `async_result<T>` /
// `async_status` its method signatures return. sisl owns the underlying type (result<T> == sisl::result<T> ==
// std::expected<T, std::error_condition>); the domain code (craft_error, below) rides the type-erased
// std::error_condition, so no layer forks the vocabulary.
template < typename T >
using result = sisl::result< T >;
using status = sisl::status;
using sisl::ok;

template < typename T >
using async_result = sisl::async::result< T >;
using async_status = sisl::async::status;

// A replica's endpoint id (routing / membership). A 16-byte uuid; identical to any consumer's own uuid alias.
using peer_id_t = boost::uuids::uuid;

// The id of the volume/partition a replica set serves. Same 16-byte uuid type; the reference model derives
// each replica's peer_id_t deterministically from it (mem_replica_id).
using volume_id_t = boost::uuids::uuid;

// Fundamental block-addressing aliases. The byte-based CRAFT API uses raw uint64_t, but the model's per-block
// index speaks lba_t / lba_count_t.
using lba_t = uint64_t;
using lba_count_t = uint32_t;

// Network address of a replica, as returned in login()'s member list.
struct replica_endpoint {
    peer_id_t id;
    std::string addr; // "host:port"
};

// {commit_lsn, last_append_lsn} snapshot -- returned by get_lsns() / keep_alive().
struct LSNPair {
    int64_t commit_lsn{-1};
    int64_t last_append_lsn{-1};
};

// The session + watermark fields the client stamps on EVERY CRAFT IO (write / read / keep_alive).
// Addressing (dLSN / read horizon / LBA / len) stays op-specific; keep_alive carries only this header.
//  - term:              fences a stale writer (the protocol's ETERM). Every IO, incl. keep_alive, is
//                       rejected with craft_error::STALE_TERM if it != the replica's session term --
//                       so a deposed client cannot even reset the liveness watchdog.
//  - commit_lsn:        advance the contiguous frontier toward this, best-effort, in dLSN order.
//                       -1 = do not advance. This is CRAFT's commit: it piggybacks on the IO the
//                       client is already sending; there is no standalone commit verb.
//  - all_committed_lsn: the client-computed set-wide min commit_lsn; floors journal reclaim. -1 = unknown.
struct client_hdr {
    uint64_t term{0};
    int64_t commit_lsn{-1};
    int64_t all_committed_lsn{-1};
};

// Returned by login(): the replica set, the starting dLSN for new I/O, the session term, and the volume
// geometry. `lba_size` is the volume's block size in bytes -- the client aligns every addr/len to it.
// `capacity` is the volume's size in bytes -- the geometry a block-device driver (a ublk disk) sizes its
// device from, so it self-configures from login rather than being told the size out of band. Both are the
// server's authoritative geometry, carried on the login response; they are 0 on a redirect.
//
// NOT_LEADER redirect: when login() is sent to a follower the call still succeeds (not an error), but
// `term == 0` and `leader_hint` is the id of the current leader; all other fields are unset. The client
// finds the matching handle by id and retries login there. `term > 0` always means a successful login.
struct LoginResult {
    std::vector< replica_endpoint > members;
    int64_t dLSN{-1};        // starting (per-partition) LSN for new I/O
    uint64_t term{0};        // 0 == NOT_LEADER redirect; >0 == session term
    uint32_t lba_size{0};    // block size in bytes (alignment unit for addr/len)
    uint64_t capacity{0};    // volume size in bytes (the driver's device geometry)
    peer_id_t leader_hint{}; // non-nil iff this is a redirect (term==0); retry login there
};

// One sub-range of a contiguous IO's sparse layout, in BYTES: a data extent (hole=false) or a hole
// (hole=true, reads as zeros -- unmapped / WRITE_ZEROES). Carries NO bytes -- the bytes live in the
// caller-owned (iomgr) sg_list buffer that write reads from and read fills. A read returns
// std::vector<io_extent> (ascending by addr) describing which byte sub-ranges of its dest buffer are
// data vs holes. A hole is NOT the same as Missing (a hole is a resolved "reads as zero").
struct io_extent {
    uint64_t addr{0}; // byte offset into the volume
    uint64_t len{0};  // byte length
    bool hole{false};
};

// CRAFT-specific failures, registered as a std::error_condition enum so they ride result<T> while staying
// branchable: if (r.error() == craft_error::STALE_TERM) { ... }. Anything with a standard equivalent is
// still returned via std::make_error_condition(std::errc::*).
ENUM(craft_error, uint16_t,
     STALE_TERM = 1, // IO term != session term (the protocol's ETERM)
     NOT_LEADER,     // leader-only op (logout) sent to a follower; login returns a redirect instead
     NO_QUORUM,      // could not reach a quorum of live replicas
     WRONG_TOKEN,    // client_token is not the current owner
     NOT_ELIGIBLE,   // replica cannot serve this read (Missing overlap / below login-dLSN L)
     REPLICA_DOWN);  // addressed replica is down (fault injection / unreachable)

class craft_error_category : public std::error_category {
public:
    const char* name() const noexcept override { return "craft"; }
    std::string message(int ev) const override { return std::string{enum_name(static_cast< craft_error >(ev))}; }
};
inline std::error_category const& craft_error_category_inst() noexcept {
    static craft_error_category inst;
    return inst;
}
inline std::error_condition make_error_condition(craft_error e) noexcept {
    return std::error_condition{static_cast< int >(e), craft_error_category_inst()};
}

} // namespace craft

template <>
struct std::is_error_condition_enum< craft::craft_error > : std::true_type {};
