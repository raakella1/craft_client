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

// THE PEER PLANE -- a replica's view of another replica.
//
// The implementor is the storage backend (it is BOTH initiator and responder). craft_peer is a
// pure-virtual interface the backend provides; CRAFT invokes it. A craft_client never calls any
// of these -- they are replica-to-replica only.
//
// craft::peer::encode_* / decode_* convert peer verbs to/from flat byte buffers suitable for any
// point-to-point transport. Use the k_* constants as verb identifiers on the wire.
//
// Buffer contract:
//   encode_* returns a vector<uint8_t> the caller owns and must keep alive until the send
//   completes. encode_fetch_data_rsp additionally references JournalSlot sg_list data in-place
//   (zero-copy); that data must also outlive the send.
//   decode_fetch_data_rsp returns slots whose sg_list iovecs borrow from the input blob; the
//   blob's backing buffer must remain valid while the slots are in use.

#include <cstdint>
#include <expected>
#include <span>
#include <string_view>
#include <vector>

#include <sisl/fds/buffer.hpp> // sisl::sg_list, sisl::io_blob, io_blob_list_t

#include <craft/types.hpp> // async_result, async_status, lsn_pair

namespace craft {

// Block-addressing units for the replica-side journal/index. The client API is byte-based;
// only JournalSlot and a replica's own per-block index speak these block units.
using lba_t = uint64_t;
using lba_count_t = uint32_t;

// One journal slot returned by fetch_data(). Replica-to-replica only; never crosses the client wire.
// Four states: data (is_empty=false, all_zeros=false), zero-fill (all_zeros=true),
// empty (is_empty=true -- verdicted permanently empty), absent (omitted from response).
struct JournalSlot {
    int64_t lsn{-1};
    bool is_empty{false};
    bool all_zeros{false};
    lba_t lba{0};
    lba_count_t len{0};
    sisl::sg_list data{}; // borrowed when returned by decode_fetch_data_rsp; see buffer contract above
};

// The peer-facing surface of one replica: what a peer may ask of it.
class craft_peer {
public:
    virtual ~craft_peer() = default;

    // {commit_lsn, last_append_lsn} snapshot -- the leader's commit-lsn poll.
    // is_login flag is set during the login workflow and the replicas apply
    // the quesce barrier to fence any further writes in the current term.
    virtual async_result< lsn_pair > get_rs_commit_lsn(uint64_t term, bool is_login) = 0;

    // Pull raw journal data for the requested dLSNs. A slot verdicted Empty comes back as
    // JournalSlot{.is_empty=true}; a slot not held by this replica is omitted from the result.
    virtual async_result< std::vector< JournalSlot > > fetch_data(std::vector< int64_t > lsns) = 0;
};

// ── Codec: flat byte buffers for peer verb transport ─────────────────────────────────────────────
namespace peer {

// Verb identifier strings for point-to-point transport registration.
inline constexpr std::string_view k_get_rs_commit_lsn = "craft.get_rs_commit_lsn";
inline constexpr std::string_view k_fetch_data = "craft.fetch_data";

enum class decode_error {
    truncated, // buffer too short for the declared content
    malformed, // counts / offsets are internally inconsistent
};

struct get_rs_commit_lsn_req {
    uint64_t term;
    bool is_login;
};
std::vector< uint8_t > encode_get_rs_commit_lsn_req(uint64_t term, bool is_login);
std::expected< get_rs_commit_lsn_req, decode_error > decode_get_rs_commit_lsn_req(std::span< uint8_t const > bytes);
std::vector< uint8_t > encode_get_rs_commit_lsn_rsp(lsn_pair lsns);
std::expected< lsn_pair, decode_error > decode_get_rs_commit_lsn_rsp(std::span< uint8_t const > bytes);

// fetch_data request: 4-byte count + N x int64_t lsns.
std::vector< uint8_t > encode_fetch_data_req(std::vector< int64_t > const& lsns);
std::expected< std::vector< int64_t >, decode_error > decode_fetch_data_req(std::span< uint8_t const > bytes);

// fetch_data response encode: blobs references slot sg_list data in-place (zero-copy for payload).
// use blobs() to send the response over the wire; slot data must outlive the send.
struct fetch_data_rsp_encoded {
    std::vector< uint8_t > header;
    sisl::io_blob_list_t data_blobs; // per-slot iovs only, no header entry

    // Returns a blob list valid as long as *this is alive and unmodified.
    sisl::io_blob_list_t blobs() const {
        sisl::io_blob_list_t out;
        out.reserve(1 + data_blobs.size());
        out.emplace_back(const_cast< uint8_t* >(header.data()), static_cast< uint32_t >(header.size()), false);
        out.insert(out.end(), data_blobs.begin(), data_blobs.end());
        return out;
    }
};
fetch_data_rsp_encoded encode_fetch_data_rsp(std::vector< JournalSlot > const& slots);

// fetch_data response decode: returned slot sg_list iovecs borrow from blob.
std::expected< std::vector< JournalSlot >, decode_error > decode_fetch_data_rsp(sisl::io_blob const& blob);

} // namespace peer
} // namespace craft
