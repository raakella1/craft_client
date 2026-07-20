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

// craft_wire: the CRAFT on-wire codec. Transport-agnostic and I/O-free -- fixed little-endian packed structs
// that overlay the wire buffer in place, plus framing (message boundaries) and validation. Shared by the TCP
// client and server transports. See docs/craft/wire.md for the byte-level spec this implements.
//
// DEPENDENCY LEAF (load-bearing invariant): this header depends on NOTHING but the standard library -- no
// homeblocks/homestore/sisl/boost header may ever be included here. That is what lets craft_wire lift out into
// a standalone dependency later; everything depends on IT, one way, never the reverse. The wire<->domain
// bridging (e.g. wire::status <-> craft_error) lives on the homeblocks side (net/craft_status.hpp), which
// depends on this. Guarded by a compile check: craft_wire.hpp must compile with only src/include on the path.

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace craft::wire {

// The default volume max transfer in bytes: the largest single CRAFT IO PAYLOAD (data only, like iSCSI's 512 KiB
// payload with the header excluded) -- the number the reference model configures, that login conveys, and that a
// driver caps its device IO to. A real volume's max_tx (login_rsp / LoginResult) overrides it; this is the ONE
// definition so no layer re-picks it. The transport frames MORE than this (see framed_body_max) but the payload
// stays the clean advertised number.
constexpr uint32_t k_default_max_tx = 512 * 1024;

// The structs below are laid out to match the little-endian wire, so a receiver reinterprets the buffer as a
// struct and reads fields directly. That is only correct on a little-endian host (every target is x86-64 /
// ARM64-LE); a big-endian build would need explicit byte swaps and is refused here rather than silently
// corrupting.
static_assert(std::endian::native == std::endian::little, "craft_wire assumes a little-endian host");

// Op codes: request and response are distinct values (there is no response flag bit), so a server only ever
// receives request ops and a client only ever receives _rsp ops -- the value alone is a direction check.
enum class op : uint8_t {
    login = 1,
    login_rsp = 2,
    helo = 3,
    helo_rsp = 4,
    write = 5,
    write_rsp = 6,
    read = 7,
    read_rsp = 8,
    keepalive = 9,
    keepalive_rsp = 10,
    logout = 11,
    logout_rsp = 12,
    resolve = 13, // client-requested resolution round (leader-only)
    resolve_rsp = 14,

    // 1..14 is the CLIENT plane, and it is complete: these are exactly the verbs of craft_replica.
    //
    // 15.. is RESERVED for the PEER plane (a replica asking another replica: get_lsns / get_rs_commit_lsn /
    // fetch_data / truncate -- see craft_peer.hpp). Nothing is allocated yet: the peer plane is deferred at the
    // WIRE, not merely at the transport. Two rules bind whoever allocates them:
    //   1. keep the request=odd / response=even convention, and
    //   2. bump k_max_op below -- is_response() is a range check, so a peer op added without it is silently
    //      misclassified as "not a response". That is the trap this constant exists to close.
    create_volume = 15, // client-requested volume creation (leader-only)
    create_volume_rsp = 16,
    get_rs_commit_lsn = 17,
    get_rs_commit_lsn_rsp = 18,
    fetch_data = 19,
    fetch_data_rsp = 20,
    k_max_op = 20, // highest allocated opcode; raise when the peer plane lands
};

// Response `status` byte; 1-6 mirror craft_error (craft_types.hpp).
enum class status : uint8_t {
    ok = 0,
    stale_term = 1,
    not_leader = 2,
    no_quorum = 3,
    wrong_token = 4,
    not_eligible = 5,
    replica_down = 6,
    invalid_argument = 7,
    internal = 255,
};

#pragma pack(push, 1)

// 8-byte message header, on every request and response. `op` implies the operation-header size (no
// op_hdr_len on the wire); `body_len` is the only length carried.
struct msg_hdr {
    uint8_t op;          // wire::op
    uint8_t status;      // wire::status on response ops; 0 on requests
    uint16_t request_id; // per-connection correlator; the response echoes it
    uint32_t body_len;   // payload bytes after the operation header
};

// 16-byte common request header, leading WRITE/READ/KEEPALIVE/LOGOUT requests (the client watermarks).
struct req_hdr {
    int64_t commit_lsn;
    int64_t all_committed_lsn;
};

// LOGIN names the volume: a multi-volume server routes the session-establishment by it (the design's
// login(client_token, vol_id)). Same 16-byte id HELO presents to bind follow-on connections.
struct login_req {
    std::array< uint8_t, 16 > volume_id;
    uint64_t client_token;
};
struct login_rsp {
    uint64_t term;
    // THE LOGIN WATERMARK: the LAST dLSN already durable -- NOT the next one to use. On a fresh replica this is
    // -1. The client reads it as a watermark (dlsn_tracker::reset_at: frontier_ = dlsn, next_dlsn_ = dlsn + 1;
    // read_route_map::reset: everything <= dlsn is universally held), so a server that sends last_append_lsn + 1
    // here makes the client (a) start writing one slot too high, leaving that slot permanently Missing on EVERY
    // replica, and (b) believe that slot is already durable. Each replica's apply_up_to() then stalls on the hole
    // FOREVER -- nothing fills a Missing slot until resync exists (the peer plane) -- so commit_lsn pins at -1,
    // no journal ever reclaims, and every read degrades to a backward walk of the whole journal tail. It is not a
    // correctness bug (reads still serve off the tail overlay), which is exactly why it hides. Send last_append_lsn.
    int64_t dlsn;
    uint64_t capacity;
    uint32_t lba_size;
    uint32_t max_tx;
    uint32_t member_count;
    uint32_t reserved;
    std::array< uint8_t, 16 > leader_hint;
};
// login_rsp body: member_count x { uint8_t id[16]; uint16_t addr_len; char addr[addr_len]; }.

struct helo_req {
    std::array< uint8_t, 16 > volume_id;
    uint64_t client_token;
    uint64_t term;
};
// helo_rsp: status only (no operation header, no body).

struct write_req {
    req_hdr hdr;
    int64_t dlsn;
    uint64_t addr;
    uint64_t len;
};
// write_req body: len bytes of data (empty => zero write).
struct write_rsp {
    int64_t commit_lsn;
    int64_t last_append_lsn;
};

struct read_req {
    req_hdr hdr;
    int64_t read_lsn; // the read horizon H
    uint64_t addr;
    uint64_t len;
};
struct read_rsp {
    int64_t commit_lsn;
    int64_t last_append_lsn;
    uint32_t extent_count;
    uint32_t reserved;
};
// read_rsp body: extent_count x extent_desc, then the data bytes for the non-hole extents.

struct extent_desc {
    uint64_t addr;
    uint64_t len;
    uint8_t hole; // 1 = hole (reads as zero, no bytes in the body); 0 = data
    uint8_t reserved[7];
};

// The largest message BODY the transport must frame/parse for a `payload`-byte IO at block size `lba`. A write
// request body is just the payload; a read REPLY body is the extent table -- worst case one extent_desc per block
// (fully fragmented, all data) -- laid down BEFORE the data. So a payload-sized read reads back as payload plus
// that table, exceeding the payload itself. The transport adds this headroom to its parse bound / recv buffers so
// a full-payload read still fits, while max_tx (the payload) stays the clean number advertised to drivers. Both
// peers derive it from the payload agreed at login, so there is nothing extra to negotiate. lba==0 -> no headroom.
constexpr uint32_t framed_body_max(uint32_t payload, uint32_t lba) {
    uint32_t const blocks = (lba == 0) ? 0u : (payload + lba - 1u) / lba;
    return payload + blocks * static_cast< uint32_t >(sizeof(extent_desc));
}

struct keepalive_req {
    req_hdr hdr;
};
struct keepalive_rsp {
    int64_t commit_lsn;
    int64_t last_append_lsn;
};

struct logout_req {
    req_hdr hdr;
};
// logout_rsp: status only.

// Client-requested resolution round: resolve every unresolved slot <= `upto` (leader-only; the design's
// client-request SyncRSCommitLSN trigger, fired after a failed write).
struct resolve_req {
    req_hdr hdr;
    int64_t upto;
};
// resolve_rsp body: empty_count x int64_t -- the slots <= resolved_upto verdicted Empty. Everything else
// <= resolved_upto that was unresolved is now durable (filled from a holder).
struct resolve_rsp {
    int64_t resolved_upto;
    uint32_t empty_count;
    uint32_t reserved;
};

// CREATE_VOLUME: leader-only, requests a new volume with the given data-member set.
// volume_create_req body: member_count x { uint8_t id[16]; uint16_t addr_len; char addr[addr_len]; }
struct volume_create_req {
    std::array< uint8_t, 16 > volume_id;
    uint64_t capacity;
    uint32_t lba_size;
    uint32_t member_count; // how many members follow in the body
};
// volume_create_rsp: status only (no operation header, no body).

// GetRSCommitLSN: non-RAFT peer query of a replica's {commit_lsn, last_append_lsn}. is_login triggers the
// quiesce barrier on the responder (see CRAFT Design's Login section). my_commit/my_append are the LEADER's
// own watermarks, riding the request per "the poll set includes the leader itself".
struct get_rs_commit_lsn_req {
    uint64_t term;
    uint8_t is_login; // bool, but keep POD-packed layout consistent with the rest of this file
    uint8_t reserved[7];
};
struct get_rs_commit_lsn_rsp {
    int64_t commit_lsn;
    int64_t last_append_lsn;
};

// used during login and recovery
struct fetch_data_req {
    uint32_t lsn_count;
    uint32_t reserved;
}; // body: lsn_count x int64_t

struct fetch_slot_desc {
    int64_t lsn;         // which dLSN this is
    uint64_t lba;        // where it writes to
    uint32_t len;        // how many blocks
    uint8_t is_empty;    // Empty verdict? (no data follows)
    uint8_t all_zeros;   // zero write? (no data follows)
    uint8_t reserved[2]; // padding
};
struct fetch_data_rsp {
    uint32_t slot_count; // how many fetch_slot_desc entries are in the body
    uint32_t reserved;
};
// body: slot_count x fetch_slot_desc, THEN the raw data bytes for slots that have real data

#pragma pack(pop)

static_assert(sizeof(msg_hdr) == 8);
static_assert(sizeof(req_hdr) == 16);
static_assert(sizeof(login_req) == 24);
static_assert(sizeof(login_rsp) == 56);
static_assert(sizeof(helo_req) == 32);
static_assert(sizeof(write_req) == 40);
static_assert(sizeof(write_rsp) == 16);
static_assert(sizeof(read_req) == 40);
static_assert(sizeof(read_rsp) == 24);
static_assert(sizeof(extent_desc) == 24);
static_assert(sizeof(keepalive_req) == 16);
static_assert(sizeof(keepalive_rsp) == 16);
static_assert(sizeof(logout_req) == 16);
static_assert(sizeof(resolve_req) == 24);
static_assert(sizeof(resolve_rsp) == 16);
static_assert(sizeof(volume_create_req) == 32);
static_assert(sizeof(get_rs_commit_lsn_req) == 16);
static_assert(sizeof(get_rs_commit_lsn_rsp) == 16);
static_assert(sizeof(fetch_data_req) == 8);
static_assert(sizeof(fetch_slot_desc) == 24);
static_assert(sizeof(fetch_data_rsp) == 8);
// The fixed operation-header size for an op code (0 for a status-only response). nullopt = unknown op, which
// is unframeable -- the caller resets the connection.
std::optional< std::size_t > op_hdr_size(uint8_t op_code) noexcept;

// A response op (2/4/6/8/10/12): a client receives only these, a server only requests. The direction check.
bool is_response(uint8_t op_code) noexcept;

// Ops that carry a payload: write (data), login_rsp (members), read_rsp (extents + data). All others must
// have body_len == 0.
bool op_allows_body(uint8_t op_code) noexcept;

// CRC32C (Castagnoli) of a byte span -- the digest primitive (the polynomial iSCSI and NVMe/TCP use).
uint32_t crc32c(std::span< uint8_t const > data) noexcept;

// Which per-connection digests the framing carries (transport-negotiated). `header` covers [msg_hdr]
// [op_header] and sits before the body; `data` covers the body and sits after it. Both are u32 CRC32C.
// Default is none -- the transport passes the negotiated config.
struct digest_cfg {
    bool header = false;
    bool data = false;
};

enum class parse_error {
    incomplete, // not enough bytes yet -- not an error; keep reading
    unknown_op, // the op code has no known layout -- malformed; reset
    bad_length, // body_len over max_tx, or a body on a bodyless op -- malformed; reset
    bad_digest, // a header or data CRC32C did not match -- corruption; reset
};

// A parsed, validated message. `op_header` and `body` are spans INTO the caller's buffer (no payload copy);
// `hdr` is copied out (8 bytes, always safe against an unaligned buffer). Valid only while the buffer lives.
struct message {
    msg_hdr hdr;
    std::span< uint8_t const > op_header;
    std::span< uint8_t const > body;
    std::size_t total; // bytes this message occupies at the front of `buf`
};

// Parse ONE message from the front of `buf`. `max_tx` bounds `body_len` (a DoS guard); `dg` says which
// digests the framing carries (they are verified). On success the spans point into `buf`; on failure see
// parse_error.
std::expected< message, parse_error > parse_message(std::span< uint8_t const > buf, uint32_t max_tx,
                                                    digest_cfg dg = {}) noexcept;

// Copy a POD wire struct out of a byte span (safe against an unaligned buffer). Caller ensures s.size() >=
// sizeof(T) -- e.g. s is a message::op_header for the op's header type.
template < class T >
T decode(std::span< uint8_t const > s) noexcept {
    T t{};
    std::memcpy(&t, s.data(), sizeof(T));
    return t;
}

// Append a POD wire struct's raw bytes to a buffer (the encoding primitive).
template < class T >
void put(std::vector< uint8_t >& out, T const& v) {
    auto const* p = reinterpret_cast< uint8_t const* >(&v);
    out.insert(out.end(), p, p + sizeof(T));
}

// Append a complete framed message -- [msg_hdr][op_header][HDGST?][body][DDGST?] -- filling body_len and the
// CRC32C digests per `dg`. For tests and small (non-bulk) messages; the transport gather-sends a bulk body
// zero-copy rather than concatenating it here.
void frame_message(std::vector< uint8_t >& out, op o, uint8_t status_code, uint16_t request_id,
                   std::span< uint8_t const > op_header, std::span< uint8_t const > body, digest_cfg dg = {});

// A replica endpoint in login_rsp's variable-length member list.
struct member {
    std::array< uint8_t, 16 > id;
    std::string addr; // "host:port"
};

// Append one member to a login_rsp body: id[16] + addr_len(u16) + addr bytes.
void put_member(std::vector< uint8_t >& out, member const& m);

// Decode `count` members from a login_rsp body. nullopt if the body is truncated or malformed.
std::optional< std::vector< member > > decode_members(std::span< uint8_t const > body, uint32_t count);

// Decode `count` fixed-size extent descriptors from the front of a read_rsp body (the remaining bytes are the
// concatenated data for the non-hole extents). nullopt if the body is too short for `count` descriptors.
std::optional< std::vector< extent_desc > > decode_extents(std::span< uint8_t const > body, uint32_t count);

// Decode `count` packed int64 dLSNs from a body (resolve_rsp's Empty-verdict list). nullopt if truncated.
std::optional< std::vector< int64_t > > decode_lsns(std::span< uint8_t const > body, uint32_t count);

// A plan for placing a read_rsp's packed body data into the caller's dest buffer. `data` is one {dest_offset,
// len} per non-hole extent in wire order -- the recv scatters the contiguous body into these; `holes` are the
// {dest_offset, len} ranges to zero-fill. `req_addr`/`req_len` are the read request's range (dest covers it);
// `ok` is false if any extent falls outside that range (a malformed reply -- reset).
struct scatter_plan {
    std::vector< std::pair< std::size_t, std::size_t > > data;
    std::vector< std::pair< std::size_t, std::size_t > > holes;
    bool ok = false;
};
scatter_plan plan_scatter(std::span< extent_desc const > extents, uint64_t req_addr, uint64_t req_len) noexcept;

} // namespace craft::wire
