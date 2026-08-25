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
#include <craft/peer.hpp>

#include <cstddef>
#include <cstring>

// Internal wire layout -- not part of the TCP framing in wire.hpp.
// Little-endian (same host assumption as wire.hpp).
namespace {
#pragma pack(push, 1)

struct get_rs_commit_lsn_req_wire {
    uint64_t term;
    uint8_t  is_login;
    uint8_t  pad[7];
};

struct lsn_pair_wire {
    int64_t commit_lsn;
    int64_t last_append_lsn;
};

struct fetch_req_hdr {
    uint32_t lsn_count;
    uint32_t reserved;
};

struct fetch_rsp_hdr {
    uint32_t slot_count;
    uint32_t reserved;
};

// One slot descriptor in a fetch_data response body.
// Layout: [fetch_rsp_hdr][N x slot_desc][data for slot_0][data for slot_1]...
struct slot_desc {
    int64_t  lsn;
    uint64_t lba;
    uint32_t len;      // lba count
    uint64_t byte_len; // raw payload bytes; 0 for empty / all_zeros slots
    uint8_t  is_empty;
    uint8_t  all_zeros;
    uint8_t  pad[2];
};

#pragma pack(pop)

static_assert(sizeof(get_rs_commit_lsn_req_wire) == 16);
static_assert(sizeof(lsn_pair_wire) == 16);
static_assert(sizeof(fetch_req_hdr) == 8);
static_assert(sizeof(fetch_rsp_hdr) == 8);
static_assert(sizeof(slot_desc) == 32);
} // namespace

namespace craft::peer {

// ── get_rs_commit_lsn ────────────────────────────────────────────────────────────────────────────

std::vector< uint8_t > encode_get_rs_commit_lsn_req(uint64_t term, bool is_login) {
    std::vector< uint8_t > out(sizeof(get_rs_commit_lsn_req_wire));
    get_rs_commit_lsn_req_wire w{term, static_cast< uint8_t >(is_login ? 1 : 0), {}};
    std::memcpy(out.data(), &w, sizeof(w));
    return out;
}

std::expected< get_rs_commit_lsn_req, decode_error > decode_get_rs_commit_lsn_req(std::span< uint8_t const > bytes) {
    if (bytes.size() < sizeof(get_rs_commit_lsn_req_wire)) return std::unexpected(decode_error::truncated);
    get_rs_commit_lsn_req_wire w{};
    std::memcpy(&w, bytes.data(), sizeof(w));
    return get_rs_commit_lsn_req{w.term, w.is_login != 0};
}

std::vector< uint8_t > encode_get_rs_commit_lsn_rsp(lsn_pair lsns) {
    std::vector< uint8_t > out(sizeof(lsn_pair_wire));
    lsn_pair_wire w{lsns.commit_lsn, lsns.last_append_lsn};
    std::memcpy(out.data(), &w, sizeof(w));
    return out;
}

std::expected< lsn_pair, decode_error > decode_get_rs_commit_lsn_rsp(std::span< uint8_t const > bytes) {
    if (bytes.size() < sizeof(lsn_pair_wire)) return std::unexpected(decode_error::truncated);
    lsn_pair_wire w{};
    std::memcpy(&w, bytes.data(), sizeof(w));
    return lsn_pair{w.commit_lsn, w.last_append_lsn};
}

// ── fetch_data request ───────────────────────────────────────────────────────────────────────────

std::vector< uint8_t > encode_fetch_data_req(std::vector< int64_t > const& lsns) {
    std::vector< uint8_t > out(sizeof(fetch_req_hdr) + lsns.size() * sizeof(int64_t));
    fetch_req_hdr h{static_cast< uint32_t >(lsns.size()), 0};
    std::memcpy(out.data(), &h, sizeof(h));
    std::memcpy(out.data() + sizeof(h), lsns.data(), lsns.size() * sizeof(int64_t));
    return out;
}

std::expected< std::vector< int64_t >, decode_error > decode_fetch_data_req(std::span< uint8_t const > bytes) {
    if (bytes.size() < sizeof(fetch_req_hdr)) return std::unexpected(decode_error::truncated);
    fetch_req_hdr h{};
    std::memcpy(&h, bytes.data(), sizeof(h));
    auto const data_sz = static_cast< std::size_t >(h.lsn_count) * sizeof(int64_t);
    if (bytes.size() < sizeof(h) + data_sz) return std::unexpected(decode_error::truncated);
    std::vector< int64_t > lsns(h.lsn_count);
    std::memcpy(lsns.data(), bytes.data() + sizeof(h), data_sz);
    return lsns;
}

// ── fetch_data response ──────────────────────────────────────────────────────────────────────────

fetch_data_rsp_encoded encode_fetch_data_rsp(std::vector< JournalSlot > const& slots) {
    // Build the descriptor section: rsp header + one slot_desc per slot.
    auto const hdr_bytes = sizeof(fetch_rsp_hdr) + slots.size() * sizeof(slot_desc);
    std::vector< uint8_t > header(hdr_bytes);

    fetch_rsp_hdr rh{static_cast< uint32_t >(slots.size()), 0};
    std::memcpy(header.data(), &rh, sizeof(rh));

    auto* dp = reinterpret_cast< slot_desc* >(header.data() + sizeof(rh));
    for (auto const& s : slots) {
        uint64_t data_size = 0;
        if (!s.is_empty && !s.all_zeros) {
            for (auto const& iov : s.data.iovs) { data_size += iov.iov_len; }
        }
        slot_desc d{};
        d.lsn       = s.lsn;
        d.lba       = s.lba;
        d.len       = s.len;
        d.byte_len = data_size;
        d.is_empty  = s.is_empty  ? 1 : 0;
        d.all_zeros = s.all_zeros ? 1 : 0;
        std::memcpy(dp++, &d, sizeof(d));
    }

    // Blob list: header blob first, then one blob per iov (zero-copy; caller owns lifetime).
    sisl::io_blob_list_t blobs;
    blobs.emplace_back(header.data(), static_cast< uint32_t >(header.size()), false);
    for (auto const& s : slots) {
        if (!s.is_empty && !s.all_zeros) {
            for (auto const& iov : s.data.iovs) {
                blobs.emplace_back(static_cast< uint8_t* >(iov.iov_base),
                                   static_cast< uint32_t >(iov.iov_len), false);
            }
        }
    }

    return {std::move(header), std::move(blobs)};
}

std::expected< std::vector< JournalSlot >, decode_error > decode_fetch_data_rsp(sisl::io_blob const& blob) {
    auto const* base  = blob.cbytes();
    auto const  total = static_cast< std::size_t >(blob.size());

    if (total < sizeof(fetch_rsp_hdr)) return std::unexpected(decode_error::truncated);

    fetch_rsp_hdr rh{};
    std::memcpy(&rh, base, sizeof(rh));

    auto const desc_offset = sizeof(rh);
    auto const desc_bytes  = static_cast< std::size_t >(rh.slot_count) * sizeof(slot_desc);
    if (total < desc_offset + desc_bytes) return std::unexpected(decode_error::truncated);

    std::vector< JournalSlot > slots;
    slots.reserve(rh.slot_count);

    std::size_t data_offset = desc_offset + desc_bytes;
    for (uint32_t i = 0; i < rh.slot_count; ++i) {
        slot_desc d{};
        std::memcpy(&d, base + desc_offset + i * sizeof(slot_desc), sizeof(d));

        JournalSlot s;
        s.lsn       = d.lsn;
        s.lba       = d.lba;
        s.len       = d.len;
        s.is_empty  = d.is_empty  != 0;
        s.all_zeros = d.all_zeros != 0;

        if (d.byte_len > 0) {
            if (data_offset + d.byte_len > total) return std::unexpected(decode_error::malformed);
            // Borrow from blob -- caller must keep the backing buffer alive.
            iovec iov{};
            iov.iov_base = const_cast< uint8_t* >(base + data_offset);
            iov.iov_len  = d.byte_len;
            s.data.iovs.push_back(iov);
            s.data.size = d.byte_len;
            data_offset += d.byte_len;
        }

        slots.push_back(std::move(s));
    }

    return slots;
}

} // namespace craft::peer
