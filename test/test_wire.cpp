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

// P0 codec unit test: the wire structs, the op-header size table, framing (message boundaries + validation),
// and a fuzz pass. No I/O -- proves the encoding independent of any transport.

#include <cstdint>
#include <cstring>
#include <random>
#include <span>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include <craft/wire.hpp>

using namespace craft::wire;

namespace {
constexpr uint32_t kMaxTx = 512 * 1024;

template < class T >
std::span< uint8_t const > bytes(T const& v) {
    return {reinterpret_cast< uint8_t const* >(&v), sizeof(T)};
}

// An extent descriptor with its reserved bytes zeroed (a designated init would trip -Wmissing-field-init).
extent_desc ext(uint64_t addr, uint64_t len, uint8_t hole) {
    extent_desc e{};
    e.addr = addr;
    e.len = len;
    e.hole = hole;
    return e;
}

// Build a framed message: msg_hdr (body_len from `body`) + the op header bytes + the body bytes.
std::vector< uint8_t > frame(op o, uint8_t st, uint16_t rid, std::span< uint8_t const > ophdr,
                             std::span< uint8_t const > body) {
    std::vector< uint8_t > buf;
    msg_hdr h{static_cast< uint8_t >(o), st, rid, static_cast< uint32_t >(body.size())};
    put(buf, h);
    buf.insert(buf.end(), ophdr.begin(), ophdr.end());
    buf.insert(buf.end(), body.begin(), body.end());
    return buf;
}
} // namespace

// The op-header size table matches the spec, and unknown ops are rejected.
TEST(CraftWire, OpHeaderSizes) {
    EXPECT_EQ(op_hdr_size(static_cast< uint8_t >(op::login)), 24u); // volume_id[16] + client_token
    EXPECT_EQ(op_hdr_size(static_cast< uint8_t >(op::login_rsp)), 56u);
    EXPECT_EQ(op_hdr_size(static_cast< uint8_t >(op::helo)), 32u);
    EXPECT_EQ(op_hdr_size(static_cast< uint8_t >(op::helo_rsp)), 0u);
    EXPECT_EQ(op_hdr_size(static_cast< uint8_t >(op::write)), 40u);
    EXPECT_EQ(op_hdr_size(static_cast< uint8_t >(op::write_rsp)), 16u);
    EXPECT_EQ(op_hdr_size(static_cast< uint8_t >(op::read)), 40u);
    EXPECT_EQ(op_hdr_size(static_cast< uint8_t >(op::read_rsp)), 24u);
    EXPECT_EQ(op_hdr_size(static_cast< uint8_t >(op::keepalive)), 16u);
    EXPECT_EQ(op_hdr_size(static_cast< uint8_t >(op::logout)), 16u);
    EXPECT_EQ(op_hdr_size(static_cast< uint8_t >(op::resolve)), 24u);
    EXPECT_EQ(op_hdr_size(static_cast< uint8_t >(op::resolve_rsp)), 16u);
    EXPECT_EQ(op_hdr_size(static_cast< uint8_t >(op::create_volume)), 24u);
    EXPECT_EQ(op_hdr_size(static_cast< uint8_t >(op::create_volume_rsp)), 0u);
    EXPECT_FALSE(op_hdr_size(0).has_value());
    EXPECT_FALSE(op_hdr_size(17).has_value());
    EXPECT_FALSE(op_hdr_size(99).has_value());

    EXPECT_TRUE(is_response(static_cast< uint8_t >(op::write_rsp)));
    EXPECT_TRUE(is_response(static_cast< uint8_t >(op::resolve_rsp)));
    EXPECT_FALSE(is_response(static_cast< uint8_t >(op::write)));
    EXPECT_FALSE(is_response(static_cast< uint8_t >(op::resolve)));
    EXPECT_FALSE(is_response(0));
}

// A RESOLVE round-trips: the watermark request and the Empty-verdict list survive frame -> parse -> decode.
TEST(CraftWire, RoundTripResolve) {
    resolve_req rq{{.commit_lsn = 5, .all_committed_lsn = 3}, /*upto*/ 9};
    auto req_buf = frame(op::resolve, 0, /*rid*/ 3, bytes(rq), {});
    auto req = parse_message(req_buf, kMaxTx);
    ASSERT_TRUE(req.has_value());
    auto const rq2 = decode< resolve_req >(req->op_header);
    EXPECT_EQ(rq2.upto, 9);
    EXPECT_EQ(rq2.hdr.commit_lsn, 5);

    std::vector< int64_t > const empties{6, 8};
    std::vector< uint8_t > body;
    for (auto const d : empties)
        put(body, d);
    resolve_rsp rs{/*resolved_upto*/ 9, /*empty_count*/ 2, 0};
    auto rsp_buf = frame(op::resolve_rsp, 0, /*rid*/ 3, bytes(rs), body);
    auto rsp = parse_message(rsp_buf, kMaxTx);
    ASSERT_TRUE(rsp.has_value());
    auto const rs2 = decode< resolve_rsp >(rsp->op_header);
    EXPECT_EQ(rs2.resolved_upto, 9);
    auto const got = decode_lsns(rsp->body, rs2.empty_count);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, empties);
    EXPECT_FALSE(decode_lsns(rsp->body, 100).has_value()); // truncated list is rejected
}

// A WRITE request round-trips: header fields and the payload survive frame -> parse -> decode.
TEST(CraftWire, RoundTripWriteRequest) {
    write_req wr{{.commit_lsn = 100, .all_committed_lsn = 90}, /*dlsn*/ 42, /*addr*/ 4096, /*len*/ 8192};
    std::vector< uint8_t > data(8192, 0xAB);

    auto buf = frame(op::write, 0, /*rid*/ 7, bytes(wr), data);
    auto r = parse_message(buf, kMaxTx);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->hdr.op, static_cast< uint8_t >(op::write));
    EXPECT_EQ(r->hdr.request_id, 7);
    EXPECT_EQ(r->hdr.body_len, 8192u);
    EXPECT_EQ(r->total, 8u + 40u + 8192u);

    auto got = decode< write_req >(r->op_header);
    EXPECT_EQ(got.dlsn, 42);
    EXPECT_EQ(got.addr, 4096u);
    EXPECT_EQ(got.len, 8192u);
    EXPECT_EQ(got.hdr.commit_lsn, 100);
    EXPECT_EQ(got.hdr.all_committed_lsn, 90);
    ASSERT_EQ(r->body.size(), 8192u);
    EXPECT_EQ(r->body.front(), 0xAB);
    EXPECT_EQ(r->body.back(), 0xAB);
}

// A KEEPALIVE response round-trips (no body); direction check holds.
TEST(CraftWire, RoundTripKeepAliveResponse) {
    keepalive_rsp ka{/*commit_lsn*/ 55, /*last_append_lsn*/ 60};
    auto buf = frame(op::keepalive_rsp, static_cast< uint8_t >(status::ok), 3, bytes(ka), {});

    auto r = parse_message(buf, kMaxTx);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->hdr.op, static_cast< uint8_t >(op::keepalive_rsp));
    EXPECT_TRUE(is_response(r->hdr.op));
    EXPECT_EQ(r->body.size(), 0u);
    auto got = decode< keepalive_rsp >(r->op_header);
    EXPECT_EQ(got.commit_lsn, 55);
    EXPECT_EQ(got.last_append_lsn, 60);
}

// A buffer shorter than a full message is `incomplete`, not an error -- the framer waits for more bytes.
TEST(CraftWire, IncompleteBuffer) {
    write_req wr{{10, 5}, 1, 0, 4096};
    std::vector< uint8_t > data(4096, 0);
    auto buf = frame(op::write, 0, 1, bytes(wr), data);

    for (std::size_t n : {std::size_t{0}, std::size_t{4}, std::size_t{8}, std::size_t{40}, buf.size() - 1}) {
        auto r = parse_message(std::span< uint8_t const >{buf.data(), n}, kMaxTx);
        ASSERT_FALSE(r.has_value()) << "n=" << n;
        EXPECT_EQ(r.error(), parse_error::incomplete) << "n=" << n;
    }
    EXPECT_TRUE(parse_message(buf, kMaxTx).has_value());
}

// Malformed inputs are rejected (not treated as incomplete): unknown op, oversized body, body on a bodyless op.
TEST(CraftWire, RejectsMalformed) {
    {
        msg_hdr h{99, 0, 0, 0};
        std::vector< uint8_t > b;
        put(b, h);
        auto r = parse_message(b, kMaxTx);
        ASSERT_FALSE(r.has_value());
        EXPECT_EQ(r.error(), parse_error::unknown_op);
    }
    {
        msg_hdr h{static_cast< uint8_t >(op::write), 0, 0, kMaxTx + 1};
        std::vector< uint8_t > b;
        put(b, h);
        auto r = parse_message(b, kMaxTx);
        ASSERT_FALSE(r.has_value());
        EXPECT_EQ(r.error(), parse_error::bad_length);
    }
    {
        msg_hdr h{static_cast< uint8_t >(op::keepalive), 0, 0, 8}; // keepalive carries no body
        std::vector< uint8_t > b;
        put(b, h);
        auto r = parse_message(b, kMaxTx);
        ASSERT_FALSE(r.has_value());
        EXPECT_EQ(r.error(), parse_error::bad_length);
    }
}

// CRC32C matches the standard check value, so it interoperates with any peer (iSCSI/NVMe use the same).
TEST(CraftWire, Crc32cKnownVector) {
    std::string_view const s = "123456789";
    EXPECT_EQ(crc32c({reinterpret_cast< uint8_t const* >(s.data()), s.size()}), 0xE3069283u);
    EXPECT_EQ(crc32c({}), 0u); // empty input
}

// A LOGIN response with a variable member list round-trips: fixed op header + the member descriptors.
TEST(CraftWire, RoundTripLoginResponseWithMembers) {
    login_rsp lr{};
    lr.term = 7;
    lr.dlsn = 100;
    lr.capacity = uint64_t{1} << 30;
    lr.lba_size = 4096;
    lr.max_tx = kMaxTx;
    lr.member_count = 2;

    member a{};
    a.id[0] = 0xA1;
    a.addr = "10.0.0.1:5000";
    member b{};
    b.id[0] = 0xB2;
    b.addr = "10.0.0.2:5000";
    std::vector< uint8_t > body;
    put_member(body, a);
    put_member(body, b);

    std::vector< uint8_t > buf;
    frame_message(buf, op::login_rsp, static_cast< uint8_t >(status::ok), 1, bytes(lr), body);
    auto r = parse_message(buf, kMaxTx);
    ASSERT_TRUE(r.has_value());
    auto const got = decode< login_rsp >(r->op_header);
    EXPECT_EQ(got.term, 7u);
    EXPECT_EQ(got.capacity, uint64_t{1} << 30);
    EXPECT_EQ(got.member_count, 2u);

    auto members = decode_members(r->body, got.member_count);
    ASSERT_TRUE(members.has_value());
    ASSERT_EQ(members->size(), 2u);
    EXPECT_EQ((*members)[0].addr, "10.0.0.1:5000");
    EXPECT_EQ((*members)[0].id[0], 0xA1);
    EXPECT_EQ((*members)[1].addr, "10.0.0.2:5000");
    EXPECT_EQ((*members)[1].id[0], 0xB2);
}

// A READ response round-trips and the scatter plan reconstructs dest: data extents get their bytes, the hole
// reads as zero -- and the hole's zeros never cross the wire.
TEST(CraftWire, RoundTripReadResponseWithExtentsAndScatter) {
    constexpr uint64_t base = 1000, rlen = 30;
    std::vector< extent_desc > exts(3);
    exts[0] = ext(base + 0, 10, 0);  // data
    exts[1] = ext(base + 10, 10, 1); // hole
    exts[2] = ext(base + 20, 10, 0); // data
    std::vector< uint8_t > data(10, 'A');
    data.insert(data.end(), 10, 'B'); // the two data extents, packed (no bytes for the hole)

    read_rsp rr{};
    rr.commit_lsn = 50;
    rr.last_append_lsn = 55;
    rr.extent_count = 3;
    std::vector< uint8_t > body;
    for (auto const& e : exts)
        put(body, e);
    body.insert(body.end(), data.begin(), data.end());

    std::vector< uint8_t > buf;
    frame_message(buf, op::read_rsp, static_cast< uint8_t >(status::ok), 9, bytes(rr), body);
    auto r = parse_message(buf, kMaxTx);
    ASSERT_TRUE(r.has_value());
    auto const got = decode< read_rsp >(r->op_header);
    EXPECT_EQ(got.extent_count, 3u);

    auto ex = decode_extents(r->body, got.extent_count);
    ASSERT_TRUE(ex.has_value());
    auto const wire_data = r->body.subspan(got.extent_count * sizeof(extent_desc));
    ASSERT_EQ(wire_data.size(), 20u);

    auto const plan = plan_scatter(*ex, base, rlen);
    ASSERT_TRUE(plan.ok);
    ASSERT_EQ(plan.data.size(), 2u);
    ASSERT_EQ(plan.holes.size(), 1u);
    EXPECT_EQ(plan.data[0], (std::pair< std::size_t, std::size_t >{0, 10}));
    EXPECT_EQ(plan.holes[0], (std::pair< std::size_t, std::size_t >{10, 10}));
    EXPECT_EQ(plan.data[1], (std::pair< std::size_t, std::size_t >{20, 10}));

    std::vector< uint8_t > dest(rlen, 0xEE);
    std::size_t src = 0;
    for (auto const& [off, len] : plan.data) {
        std::memcpy(dest.data() + off, wire_data.data() + src, len);
        src += len;
    }
    for (auto const& [off, len] : plan.holes)
        std::memset(dest.data() + off, 0, len);
    EXPECT_EQ(src, 20u);
    for (std::size_t i = 0; i < 10; ++i)
        EXPECT_EQ(dest[i], 'A');
    for (std::size_t i = 10; i < 20; ++i)
        EXPECT_EQ(dest[i], 0);
    for (std::size_t i = 20; i < 30; ++i)
        EXPECT_EQ(dest[i], 'B');
}

// Header digest: framed and verified when enabled; a flipped byte in the header is caught as bad_digest.
TEST(CraftWire, HeaderDigest) {
    digest_cfg const dg{.header = true, .data = false};
    keepalive_rsp ka{/*commit_lsn*/ 55, /*last_append_lsn*/ 60};
    std::vector< uint8_t > buf;
    frame_message(buf, op::keepalive_rsp, static_cast< uint8_t >(status::ok), 3, bytes(ka), {}, dg);
    EXPECT_EQ(buf.size(), 8u + 16u + 4u); // msg_hdr + op header + HDGST

    ASSERT_TRUE(parse_message(buf, kMaxTx, dg).has_value());
    auto bad = buf;
    bad[10] ^= 0xFF; // a byte inside the op header
    auto rc = parse_message(bad, kMaxTx, dg);
    ASSERT_FALSE(rc.has_value());
    EXPECT_EQ(rc.error(), parse_error::bad_digest);
}

// Data digest: framed and verified when enabled; a flipped payload byte is caught.
TEST(CraftWire, DataDigest) {
    digest_cfg const dg{.header = true, .data = true};
    write_req wr{{10, 5}, 1, 4096, 16};
    std::vector< uint8_t > data(16, 0xCD);
    std::vector< uint8_t > buf;
    frame_message(buf, op::write, 0, 2, bytes(wr), data, dg);
    EXPECT_EQ(buf.size(), 8u + 40u + 4u + 16u + 4u); // + HDGST + body + DDGST

    ASSERT_TRUE(parse_message(buf, kMaxTx, dg).has_value());
    auto bad = buf;
    bad[8 + 40 + 4] ^= 0xFF; // the first body byte
    auto rc = parse_message(bad, kMaxTx, dg);
    ASSERT_FALSE(rc.has_value());
    EXPECT_EQ(rc.error(), parse_error::bad_digest);
}

// The variable-length decoders reject truncated/out-of-range input instead of reading past the buffer.
TEST(CraftWire, DecodeRejectsMalformed) {
    EXPECT_FALSE(decode_members(std::vector< uint8_t >(10, 0), 1).has_value()); // < id + addr_len
    std::vector< uint8_t > short_addr(16, 0);
    put(short_addr, static_cast< uint16_t >(100)); // addr_len 100 but no addr bytes
    EXPECT_FALSE(decode_members(short_addr, 1).has_value());
    EXPECT_FALSE(decode_extents(std::vector< uint8_t >(30, 0), 2).has_value()); // < 2 * 24
    std::vector< extent_desc > oor(1);
    oor[0] = ext(2000, 10, 0); // outside [1000, 1030)
    EXPECT_FALSE(plan_scatter(oor, 1000, 30).ok);
}

// Fuzz: arbitrary bytes (with random digest config) must never crash the parser or the variable-length
// decoders (ASAN/UBSAN catch OOB/UB); any success must be internally consistent.
TEST(CraftWire, FuzzNeverCrashes) {
    std::mt19937 rng{12345};
    std::uniform_int_distribution< int > byte{0, 255};
    std::uniform_int_distribution< std::size_t > len{0, 200};
    std::uniform_int_distribution< int > bit{0, 1};
    for (int i = 0; i < 100000; ++i) {
        std::vector< uint8_t > b(len(rng));
        for (auto& x : b)
            x = static_cast< uint8_t >(byte(rng));
        digest_cfg const dg{.header = bit(rng) != 0, .data = bit(rng) != 0};
        auto r = parse_message(b, kMaxTx, dg);
        if (!r.has_value()) continue;
        EXPECT_LE(r->total, b.size());
        if (r->hdr.op == static_cast< uint8_t >(op::login_rsp)) {
            (void)decode_members(r->body, decode< login_rsp >(r->op_header).member_count);
        } else if (r->hdr.op == static_cast< uint8_t >(op::read_rsp)) {
            if (auto ex = decode_extents(r->body, decode< read_rsp >(r->op_header).extent_count))
                (void)plan_scatter(*ex, 0, ~uint64_t{0});
        }
    }
}
