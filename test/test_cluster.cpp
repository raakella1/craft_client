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

// P3 increment: the cluster server. One process fronts a 3-replica set over TCP, N ports; LOGIN on the leader
// establishes the set-wide term (faked-RAFT via MemTransport::run_login), a follower LOGIN redirects, HELO
// joins on the other connections, and IO flows to each replica across the grid. Wire-only test TU (the
// cluster server header is a pimpl).

#include <algorithm>
#include <array>
#include <cstdint>
#include <span>
#include <vector>

#include <gtest/gtest.h>

#include "net/cluster_server.hpp"
#include <craft/net/conn.hpp>
#include "net/tcp_client.hpp"

using namespace craft::net;
namespace wire = craft::wire;

namespace {

constexpr uint32_t k_lba = 4096;
constexpr uint64_t k_token = 0xABCD;

std::vector< uint8_t > pattern(std::size_t n, uint8_t seed = 0) {
    std::vector< uint8_t > v(n);
    for (std::size_t i = 0; i < n; ++i)
        v[i] = static_cast< uint8_t >(1 + ((i + seed) % 251));
    return v;
}

craft_cluster_server make_server(uint32_t n = 3) {
    craft_cluster_server s{n, k_lba, uint64_t{1} << 30, 512 * 1024};
    s.start();
    return s;
}

craft_tcp_client connect_to(craft_cluster_server const& s, std::size_t idx) {
    auto c = craft_tcp_client::connect("127.0.0.1", s.port(idx));
    EXPECT_TRUE(c.has_value());
    return std::move(*c);
}

} // namespace

// LOGIN on the leader establishes the session (term 1, the reconciled dLSN, the member list); LOGIN on a
// follower is a NOT_LEADER redirect pointing at the leader.
TEST(CraftCluster, LoginAndRedirect) {
    auto server = make_server(3);
    auto const mem = server.members();
    auto const vol = server.volume_id();
    ASSERT_EQ(mem.size(), 3u);
    std::size_t const leader = server.leader_index();

    {
        auto ldr = connect_to(server, leader);
        auto lr = ldr.login(vol, k_token);
        ASSERT_TRUE(lr.has_value());
        EXPECT_EQ(lr->term, 1u);
        EXPECT_EQ(lr->dlsn, -1); // fresh cluster: reconciled tail is -1, so the first new dLSN is 0
        EXPECT_EQ(lr->lba_size, k_lba);
        ASSERT_EQ(lr->members.size(), 3u);
        EXPECT_EQ(lr->members[leader].addr, mem[leader].addr);

        std::size_t const follower = (leader + 1) % 3;
        auto flw = connect_to(server, follower);
        auto rr = flw.login(vol, k_token);
        ASSERT_TRUE(rr.has_value());
        EXPECT_EQ(rr->term, 0u);                    // redirect
        EXPECT_EQ(rr->leader_hint, mem[leader].id); // ...pointing at the leader

        // LOGIN names the volume: a mismatched id is refused before any session-establishment runs.
        std::array< uint8_t, 16 > wrong = vol;
        wrong[0] ^= 0xFF;
        auto bad = ldr.login(wrong, k_token);
        ASSERT_TRUE(bad.has_value());
        EXPECT_EQ(bad->term, 0u); // no session
    }
    server.stop();
}

// The full grid: LOGIN the leader, HELO the followers into the same session, then write and read back on
// every replica's connection.
TEST(CraftCluster, GridWriteReadAcrossReplicas) {
    auto server = make_server(3);
    auto const vol = server.volume_id();
    std::size_t const leader = server.leader_index();

    {
        std::vector< craft_tcp_client > cli;
        for (std::size_t i = 0; i < 3; ++i)
            cli.push_back(connect_to(server, i));

        auto lr = cli[leader].login(vol, k_token);
        ASSERT_TRUE(lr.has_value());
        uint64_t const term = lr->term;
        ASSERT_EQ(term, 1u);

        for (std::size_t i = 0; i < 3; ++i) {
            if (i == leader) continue;
            auto h = cli[i].helo(vol, k_token, term);
            ASSERT_TRUE(h.has_value());
            EXPECT_EQ(*h, wire::status::ok);
        }

        // Broadcast the same write to every replica (dLSN 0, committed), as the client will at quorum.
        auto const data = pattern(k_lba);
        for (std::size_t i = 0; i < 3; ++i) {
            auto w = cli[i].write(/*dlsn=*/0, /*addr=*/0, k_lba, {data.data(), data.size()}, /*commit=*/0);
            ASSERT_TRUE(w.has_value());
            EXPECT_EQ(w->status, wire::status::ok);
            EXPECT_EQ(w->commit_lsn, 0);
        }

        // Read it back from every replica.
        for (std::size_t i = 0; i < 3; ++i) {
            std::vector< uint8_t > dest(k_lba, 0xEE);
            auto r = cli[i].read(/*read_lsn=*/0, /*addr=*/0, k_lba, {dest.data(), dest.size()});
            ASSERT_TRUE(r.has_value());
            EXPECT_EQ(r->status, wire::status::ok);
            ASSERT_EQ(r->extents.size(), 1u);
            EXPECT_FALSE(r->extents[0].hole);
            EXPECT_EQ(dest, data);
        }
    }
    server.stop();
}

// A HELO carrying the wrong term does not bind: the connection stays fenced.
TEST(CraftCluster, HeloWrongTermRejected) {
    auto server = make_server(3);
    auto const vol = server.volume_id();
    std::size_t const leader = server.leader_index();

    {
        auto ldr = connect_to(server, leader);
        auto lr = ldr.login(vol, k_token);
        ASSERT_TRUE(lr.has_value());

        std::size_t const follower = (leader + 1) % 3;
        auto flw = connect_to(server, follower);
        auto h = flw.helo(vol, k_token, lr->term + 99); // wrong term
        ASSERT_TRUE(h.has_value());
        EXPECT_EQ(*h, wire::status::stale_term);

        // Still unbound -> IO is fenced.
        auto const data = pattern(k_lba);
        auto w = flw.write(0, 0, k_lba, {data.data(), data.size()}, 0);
        ASSERT_TRUE(w.has_value());
        EXPECT_EQ(w->status, wire::status::stale_term);
    }
    server.stop();
}

// LOGOUT on the leader tears the session down SET-WIDE: a follower connection that was bound now fences,
// because run_logout cleared the term on every replica.
TEST(CraftCluster, LogoutFencesSetWide) {
    auto server = make_server(3);
    auto const vol = server.volume_id();
    std::size_t const leader = server.leader_index();

    {
        auto ldr = connect_to(server, leader);
        auto lr = ldr.login(vol, k_token);
        ASSERT_TRUE(lr.has_value());
        uint64_t const term = lr->term;

        std::size_t const follower = (leader + 1) % 3;
        auto flw = connect_to(server, follower);
        ASSERT_TRUE(flw.helo(vol, k_token, term).has_value());

        auto lo = ldr.logout();
        ASSERT_TRUE(lo.has_value());
        EXPECT_EQ(*lo, wire::status::ok);

        // The follower's connection is still "bound" locally, but the replica's term was cleared set-wide, so
        // its write fences on the replica.
        auto const data = pattern(k_lba);
        auto w = flw.write(0, 0, k_lba, {data.data(), data.size()}, 0);
        ASSERT_TRUE(w.has_value());
        EXPECT_EQ(w->status, wire::status::stale_term);
    }
    server.stop();
}
