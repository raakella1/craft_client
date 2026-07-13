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

// LOGIN / LOGOUT and the IO ops (WRITE / READ / KEEPALIVE) over a real (loopback) io_uring TCP connection --
// the wire codec driven end to end through craft_conn, with the server backing its state with a real
// MemCraftReplica (via its srv_* local-server seam). Client and server in one process. This TU is itself
// wire-only: the client and server headers pull no homeblocks header, so the test speaks wire::status /
// wire::extent_desc, and a fenced op is a VALID reply carrying STALE_TERM, not an error.

#include <algorithm>
#include <cstdint>
#include <span>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include <craft/net/conn.hpp>
#include "net/wire_client.hpp"
#include "net/tcp_server.hpp"

using namespace craft::net;
namespace wire = craft::wire;

namespace {

constexpr uint32_t k_lba = 4096;

server_geometry make_geo() {
    server_geometry geo{};
    geo.capacity = uint64_t{1} << 30;
    geo.lba_size = k_lba;
    geo.max_tx = 512 * 1024;
    wire::member self{};
    self.id[0] = 0x01;
    self.addr = "127.0.0.1:0";
    geo.members.push_back(self);
    return geo;
}

// A per-byte-nonzero pattern of `n` bytes -- nonzero so no 4 KiB page collapses to a hole on the read path
// (read_range reads an all-zero data page back thin).
std::vector< uint8_t > pattern(std::size_t n, uint8_t seed = 0) {
    std::vector< uint8_t > v(n);
    for (std::size_t i = 0; i < n; ++i)
        v[i] = static_cast< uint8_t >(1 + ((i + seed) % 251));
    return v;
}

// Stand up a fresh single-replica server on an ephemeral loopback port, connect a client, log it in, and run
// `body(client, login_result)`. The client closes at inner-scope exit so serve() sees EOF and returns before
// the jthread joins.
template < class F >
void with_session(F&& body) {
    auto lst = craft_listener::bind_listen(0);
    ASSERT_TRUE(lst.has_value());
    uint16_t const port = lst->port();

    craft_tcp_server server{make_geo()};
    std::jthread srv([&] {
        auto conn = lst->accept();
        if (conn) server.serve(std::move(*conn));
    });

    {
        auto cli = wire_client::connect("127.0.0.1", port);
        ASSERT_TRUE(cli.has_value());
        auto lr = cli->login(/*volume_id=*/{}, 0x1234); // the standalone server fronts one volume: any id binds
        ASSERT_TRUE(lr.has_value());
        body(*cli, *lr);
    }
}

} // namespace

// login returns the geometry the server advertises; logout tears the session down; a second logout is fenced.
TEST(CraftTcp, LoginLogoutRoundTrip) {
    auto lst = craft_listener::bind_listen(0);
    ASSERT_TRUE(lst.has_value());
    uint16_t const port = lst->port();

    craft_tcp_server server{make_geo()};
    std::jthread srv([&] {
        auto conn = lst->accept();
        if (conn) server.serve(std::move(*conn));
    });

    {
        auto cli = wire_client::connect("127.0.0.1", port);
        ASSERT_TRUE(cli.has_value());

        auto lr = cli->login(/*volume_id=*/{}, 0xABCD);
        ASSERT_TRUE(lr.has_value());
        EXPECT_EQ(lr->term, 1u);
        EXPECT_EQ(lr->capacity, uint64_t{1} << 30);
        EXPECT_EQ(lr->lba_size, k_lba);
        EXPECT_EQ(lr->max_tx, 512u * 1024);
        EXPECT_EQ(lr->dlsn, 0); // fresh replica -> the first dLSN for new IO is 0
        ASSERT_EQ(lr->members.size(), 1u);
        EXPECT_EQ(lr->members[0].addr, "127.0.0.1:0");
        EXPECT_EQ(lr->members[0].id[0], 0x01);

        auto lo = cli->logout();
        ASSERT_TRUE(lo.has_value());
        EXPECT_EQ(*lo, wire::status::ok);

        auto lo2 = cli->logout(); // the session is gone -> fenced
        ASSERT_TRUE(lo2.has_value());
        EXPECT_EQ(*lo2, wire::status::stale_term);
    }
}

// Write two blocks, read them back at the horizon, and verify the bytes round-trip.
TEST(CraftTcp, WriteReadBack) {
    with_session([](wire_client& cli, login_result const&) {
        auto const data = pattern(2 * k_lba);
        auto w = cli.write(/*dlsn=*/0, /*addr=*/0, /*len=*/2 * k_lba, {data.data(), data.size()}, /*commit=*/0);
        ASSERT_TRUE(w.has_value());
        EXPECT_EQ(w->status, wire::status::ok);
        EXPECT_EQ(w->commit_lsn, 0);
        EXPECT_EQ(w->last_append_lsn, 0);

        std::vector< uint8_t > dest(2 * k_lba, 0xEE);
        auto r = cli.read(/*read_lsn=*/0, /*addr=*/0, /*len=*/2 * k_lba, {dest.data(), dest.size()});
        ASSERT_TRUE(r.has_value());
        EXPECT_EQ(r->status, wire::status::ok);
        ASSERT_EQ(r->extents.size(), 1u);
        EXPECT_FALSE(r->extents[0].hole);
        EXPECT_EQ(r->extents[0].addr, 0u);
        EXPECT_EQ(r->extents[0].len, 2u * k_lba);
        EXPECT_EQ(dest, data);
    });
}

// Write blocks 0 and 2 but not 1; a read across all three reconstructs the middle as a zero-filled hole.
TEST(CraftTcp, SparseReadReconstructsHoles) {
    with_session([](wire_client& cli, login_result const&) {
        auto const p0 = pattern(k_lba, 0);
        auto const p2 = pattern(k_lba, 100);
        ASSERT_TRUE(cli.write(0, 0 * k_lba, k_lba, {p0.data(), p0.size()}, /*commit=*/-1).has_value());
        auto w2 = cli.write(1, 2 * k_lba, k_lba, {p2.data(), p2.size()}, /*commit=*/1); // commits dLSN 0 and 1
        ASSERT_TRUE(w2.has_value());
        EXPECT_EQ(w2->status, wire::status::ok);
        EXPECT_EQ(w2->commit_lsn, 1);

        std::vector< uint8_t > dest(3 * k_lba, 0xEE);
        auto r = cli.read(/*read_lsn=*/1, 0, 3 * k_lba, {dest.data(), dest.size()});
        ASSERT_TRUE(r.has_value());
        EXPECT_EQ(r->status, wire::status::ok);
        ASSERT_EQ(r->extents.size(), 3u);
        EXPECT_FALSE(r->extents[0].hole);
        EXPECT_EQ(r->extents[0].addr, 0u * k_lba);
        EXPECT_TRUE(r->extents[1].hole);
        EXPECT_EQ(r->extents[1].addr, 1u * k_lba);
        EXPECT_EQ(r->extents[1].len, k_lba);
        EXPECT_FALSE(r->extents[2].hole);
        EXPECT_EQ(r->extents[2].addr, 2u * k_lba);

        EXPECT_TRUE(std::equal(p0.begin(), p0.end(), dest.begin()));
        EXPECT_TRUE(std::all_of(dest.begin() + k_lba, dest.begin() + 2 * k_lba, [](uint8_t b) { return b == 0; }));
        EXPECT_TRUE(std::equal(p2.begin(), p2.end(), dest.begin() + 2 * k_lba));
    });
}

// A zero write (empty payload) unmaps its range; a later read reads it back as a zero-filled hole.
TEST(CraftTcp, ZeroWriteReadsAsHole) {
    with_session([](wire_client& cli, login_result const&) {
        auto const p0 = pattern(k_lba);
        ASSERT_TRUE(cli.write(0, 0, k_lba, {p0.data(), p0.size()}, /*commit=*/0).has_value());

        std::vector< uint8_t > d1(k_lba, 0xEE);
        auto r1 = cli.read(0, 0, k_lba, {d1.data(), d1.size()});
        ASSERT_TRUE(r1.has_value());
        ASSERT_EQ(r1->extents.size(), 1u);
        EXPECT_FALSE(r1->extents[0].hole);
        EXPECT_EQ(d1, p0);

        auto wz = cli.write(1, 0, k_lba, std::span< uint8_t const >{}, /*commit=*/1); // empty payload => zero write
        ASSERT_TRUE(wz.has_value());
        EXPECT_EQ(wz->status, wire::status::ok);
        EXPECT_EQ(wz->commit_lsn, 1);

        std::vector< uint8_t > d2(k_lba, 0xEE);
        auto r2 = cli.read(1, 0, k_lba, {d2.data(), d2.size()});
        ASSERT_TRUE(r2.has_value());
        ASSERT_EQ(r2->extents.size(), 1u);
        EXPECT_TRUE(r2->extents[0].hole);
        EXPECT_TRUE(std::all_of(d2.begin(), d2.end(), [](uint8_t b) { return b == 0; }));
    });
}

// keep_alive advances the frontier and returns the replica's {commit_lsn, last_append_lsn}.
TEST(CraftTcp, KeepAliveReturnsLsns) {
    with_session([](wire_client& cli, login_result const&) {
        auto const p = pattern(k_lba);
        ASSERT_TRUE(cli.write(0, 0, k_lba, {p.data(), p.size()}, /*commit=*/0).has_value());

        auto k = cli.keep_alive(/*commit=*/0);
        ASSERT_TRUE(k.has_value());
        EXPECT_EQ(k->status, wire::status::ok);
        EXPECT_EQ(k->commit_lsn, 0);
        EXPECT_EQ(k->last_append_lsn, 0);
    });
}

// After LOGOUT, an IO on the same connection is fenced: a valid reply carrying STALE_TERM (not a transport
// error), so the client surfaces the status in the reply, not as a net_error.
TEST(CraftTcp, PostLogoutWriteFenced) {
    with_session([](wire_client& cli, login_result const&) {
        auto lo = cli.logout();
        ASSERT_TRUE(lo.has_value());
        EXPECT_EQ(*lo, wire::status::ok);

        auto const p = pattern(k_lba);
        auto w = cli.write(0, 0, k_lba, {p.data(), p.size()}, /*commit=*/0);
        ASSERT_TRUE(w.has_value());
        EXPECT_EQ(w->status, wire::status::stale_term);
    });
}
