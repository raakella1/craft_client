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

#include <craft/wire.hpp>

namespace craft::wire {

std::optional< std::size_t > op_hdr_size(uint8_t op_code) noexcept {
    // Indexed by op code 1..12. 0 for a status-only response (helo_rsp, logout_rsp); index 0 is unused.
    static constexpr std::size_t k[] = {
        0,                     // 0  unused
        sizeof(login_req),     // 1  login
        sizeof(login_rsp),     // 2  login_rsp
        sizeof(helo_req),      // 3  helo
        0,                     // 4  helo_rsp (status only)
        sizeof(write_req),     // 5  write
        sizeof(write_rsp),     // 6  write_rsp
        sizeof(read_req),      // 7  read
        sizeof(read_rsp),      // 8  read_rsp
        sizeof(keepalive_req), // 9  keepalive
        sizeof(keepalive_rsp), // 10 keepalive_rsp
        sizeof(logout_req),    // 11 logout
        0,                     // 12 logout_rsp (status only)
    };
    if (op_code < 1 || op_code > 12) return std::nullopt;
    return k[op_code];
}

bool is_response(uint8_t op_code) noexcept { return op_code >= 1 && op_code <= 12 && (op_code % 2 == 0); }

bool op_allows_body(uint8_t op_code) noexcept {
    return op_code == static_cast< uint8_t >(op::write) ||  // data
        op_code == static_cast< uint8_t >(op::login_rsp) || // member list
        op_code == static_cast< uint8_t >(op::read_rsp);    // extents + data
}

namespace {
// CRC32C (Castagnoli) lookup table, reflected form (polynomial 0x82F63B78), computed at compile time.
constexpr std::array< uint32_t, 256 > make_crc32c_table() {
    std::array< uint32_t, 256 > t{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int k = 0; k < 8; ++k)
            c = (c & 1u) ? (0x82F63B78u ^ (c >> 1)) : (c >> 1);
        t[i] = c;
    }
    return t;
}
constexpr std::array< uint32_t, 256 > k_crc32c_table = make_crc32c_table();
} // namespace

uint32_t crc32c(std::span< uint8_t const > data) noexcept {
    uint32_t crc = ~0u;
    for (uint8_t b : data)
        crc = k_crc32c_table[(crc ^ b) & 0xFFu] ^ (crc >> 8);
    return ~crc;
}

std::expected< message, parse_error > parse_message(std::span< uint8_t const > buf, uint32_t max_tx,
                                                    digest_cfg dg) noexcept {
    if (buf.size() < sizeof(msg_hdr)) return std::unexpected(parse_error::incomplete);

    message m{};
    std::memcpy(&m.hdr, buf.data(), sizeof(msg_hdr)); // 8 bytes, safe even if buf is unaligned

    auto const ohs = op_hdr_size(m.hdr.op);
    if (!ohs) return std::unexpected(parse_error::unknown_op);

    if (m.hdr.body_len > max_tx) return std::unexpected(parse_error::bad_length);
    if (m.hdr.body_len > 0 && !op_allows_body(m.hdr.op)) return std::unexpected(parse_error::bad_length);

    std::size_t const hdr_end = sizeof(msg_hdr) + *ohs; // end of [msg_hdr][op_header]
    std::size_t const hd = dg.header ? sizeof(uint32_t) : 0;
    std::size_t const dd = dg.data ? sizeof(uint32_t) : 0;
    std::size_t const body_start = hdr_end + hd;
    std::size_t const total = body_start + m.hdr.body_len + dd;
    if (buf.size() < total) return std::unexpected(parse_error::incomplete);

    if (dg.header) {
        uint32_t stored;
        std::memcpy(&stored, buf.data() + hdr_end, sizeof(stored));
        if (stored != crc32c(buf.subspan(0, hdr_end))) return std::unexpected(parse_error::bad_digest);
    }
    if (dg.data) {
        uint32_t stored;
        std::memcpy(&stored, buf.data() + body_start + m.hdr.body_len, sizeof(stored));
        if (stored != crc32c(buf.subspan(body_start, m.hdr.body_len))) return std::unexpected(parse_error::bad_digest);
    }

    m.op_header = buf.subspan(sizeof(msg_hdr), *ohs);
    m.body = buf.subspan(body_start, m.hdr.body_len);
    m.total = total;
    return m;
}

void frame_message(std::vector< uint8_t >& out, op o, uint8_t status_code, uint16_t request_id,
                   std::span< uint8_t const > op_header, std::span< uint8_t const > body, digest_cfg dg) {
    std::size_t const start = out.size();
    msg_hdr h{static_cast< uint8_t >(o), status_code, request_id, static_cast< uint32_t >(body.size())};
    put(out, h);
    out.insert(out.end(), op_header.begin(), op_header.end());
    if (dg.header) put(out, crc32c(std::span< uint8_t const >{out.data() + start, out.size() - start}));
    out.insert(out.end(), body.begin(), body.end());
    if (dg.data) put(out, crc32c(body));
}

void put_member(std::vector< uint8_t >& out, member const& m) {
    out.insert(out.end(), m.id.begin(), m.id.end());
    put(out, static_cast< uint16_t >(m.addr.size()));
    out.insert(out.end(), m.addr.begin(), m.addr.end());
}

std::optional< std::vector< member > > decode_members(std::span< uint8_t const > body, uint32_t count) {
    // No reserve(count): count is untrusted (from a peer), so a huge value must not trigger a giant
    // allocation. The vector grows as members are decoded, bounded by body.size() / the minimum member size.
    std::vector< member > out;
    std::size_t off = 0;
    for (uint32_t i = 0; i < count; ++i) {
        if (off + 16 + sizeof(uint16_t) > body.size()) return std::nullopt;
        member m{};
        std::memcpy(m.id.data(), body.data() + off, 16);
        off += 16;
        uint16_t alen;
        std::memcpy(&alen, body.data() + off, sizeof(alen));
        off += sizeof(alen);
        if (off + alen > body.size()) return std::nullopt;
        m.addr.assign(reinterpret_cast< char const* >(body.data() + off), alen);
        off += alen;
        out.push_back(std::move(m));
    }
    return out;
}

std::optional< std::vector< extent_desc > > decode_extents(std::span< uint8_t const > body, uint32_t count) {
    std::size_t const need = static_cast< std::size_t >(count) * sizeof(extent_desc);
    if (need > body.size()) return std::nullopt;
    std::vector< extent_desc > out(count);
    if (count) std::memcpy(out.data(), body.data(), need);
    return out;
}

scatter_plan plan_scatter(std::span< extent_desc const > extents, uint64_t req_addr, uint64_t req_len) noexcept {
    scatter_plan p;
    uint64_t const req_end = req_addr + req_len;
    for (auto const& e : extents) {
        uint64_t const e_end = e.addr + e.len;
        if (e_end < e.addr || e.addr < req_addr || e_end > req_end) return p; // ok stays false: out of range
        std::size_t const off = static_cast< std::size_t >(e.addr - req_addr);
        auto& dst = e.hole ? p.holes : p.data;
        dst.emplace_back(off, static_cast< std::size_t >(e.len));
    }
    p.ok = true;
    return p;
}

} // namespace craft::wire
