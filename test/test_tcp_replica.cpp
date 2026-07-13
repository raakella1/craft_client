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

// P3 increment 3: the client-side transport adapter. CraftTcpReplica implements the craft_replica interface
// over the wire-only craft_tcp_client, driven through its worker-thread concurrency bridge. Here we drive it
// standalone (via detail::sync_get, off-reactor) against a cluster server -- login/write/read/keepalive/logout
// and the follower lazy-HELO + redirect -- proving the async bridge and the wire<->domain mapping before
// craft_client rides on top (increment 4).

#include <array>
#include <cstdint>
#include <cstring>
#include <vector>

#include <gtest/gtest.h>

#include "craft_test_util.hpp" // rg (sync_get), one_iov, page_of, chdr, PAGE
#include "net/cluster_server.hpp"
#include "net/tcp_replica.hpp"

using namespace craft;
using namespace craft;
using namespace craft::test;
namespace net = craft::net;

namespace {

peer_id_t to_uuid(std::array< uint8_t, 16 > const& b) {
    peer_id_t id;
    std::memcpy(&id, b.data(), 16);
    return id;
}

net::craft_cluster_server make_server() {
    net::craft_cluster_server s{3, PAGE, uint64_t{1} << 30, 512 * 1024};
    s.start();
    return s;
}

} // namespace

// The full client-facing surface through the proxy: login on the leader, write + read back, keep_alive, logout.
TEST(CraftTcpReplica, LoginWriteReadLogout) {
    auto server = make_server();
    std::size_t const leader = server.leader_index();
    auto const vol = server.volume_id();
    auto const lid = to_uuid(server.members()[leader].id);

    {
        CraftTcpReplica proxy{"127.0.0.1", server.port(leader), lid, vol};

        auto lr = rg(proxy.login(0x1234));
        ASSERT_TRUE(lr.has_value());
        EXPECT_EQ(lr->term, 1u);
        EXPECT_EQ(lr->lba_size, PAGE);
        ASSERT_EQ(lr->members.size(), 3u);
        uint64_t const term = lr->term;

        auto data = page_of(0xAB);
        ASSERT_TRUE(rg(proxy.write(chdr(term, /*commit=*/0), /*dlsn=*/0, /*addr=*/0, PAGE, one_iov(data))).has_value());

        std::vector< uint8_t > dst(PAGE, 0);
        auto r = rg(proxy.read(chdr(term), /*read_lsn=*/0, /*addr=*/0, PAGE, one_iov(dst)));
        ASSERT_TRUE(r.has_value());
        ASSERT_EQ(r->extents.size(), 1u);
        EXPECT_FALSE(r->extents[0].hole);
        EXPECT_EQ(r->lsns.commit_lsn, 0); // the reply piggybacks the replica's watermarks
        EXPECT_EQ(dst, data);

        auto ka = rg(proxy.keep_alive(chdr(term, /*commit=*/0)));
        ASSERT_TRUE(ka.has_value());
        EXPECT_EQ(ka->commit_lsn, 0);

        EXPECT_TRUE(rg(proxy.logout(chdr(term))).has_value());
    }
    server.stop();
}

// A follower proxy is never login()'d (the client logs into the leader); its first IO lazily HELOs into the
// established session, then applies.
TEST(CraftTcpReplica, FollowerLazyHelo) {
    auto server = make_server();
    std::size_t const leader = server.leader_index();
    auto const vol = server.volume_id();

    {
        CraftTcpReplica ldr{"127.0.0.1", server.port(leader), to_uuid(server.members()[leader].id), vol};
        auto lr = rg(ldr.login(0x1234));
        ASSERT_TRUE(lr.has_value());
        uint64_t const term = lr->term;

        std::size_t const f = (leader + 1) % 3;
        CraftTcpReplica flw{"127.0.0.1", server.port(f), to_uuid(server.members()[f].id), vol};

        auto data = page_of(0xCD);
        ASSERT_TRUE(rg(flw.write(chdr(term, 0), 0, 0, PAGE, one_iov(data))).has_value()); // HELOs, then writes

        std::vector< uint8_t > dst(PAGE, 0);
        auto r = rg(flw.read(chdr(term), 0, 0, PAGE, one_iov(dst)));
        ASSERT_TRUE(r.has_value());
        EXPECT_EQ(dst, data);
        EXPECT_GE(r->lsns.last_append_lsn, 0); // the follower's watermarks ride the read reply
    }
    server.stop();
}

// login() on a follower proxy surfaces the NOT_LEADER redirect (term 0 + leader_hint) as a value, not an error.
TEST(CraftTcpReplica, LoginRedirectOnFollower) {
    auto server = make_server();
    std::size_t const leader = server.leader_index();
    auto const vol = server.volume_id();

    {
        std::size_t const f = (leader + 1) % 3;
        CraftTcpReplica flw{"127.0.0.1", server.port(f), to_uuid(server.members()[f].id), vol};
        auto lr = rg(flw.login(0x1234));
        ASSERT_TRUE(lr.has_value());
        EXPECT_EQ(lr->term, 0u); // redirect
        EXPECT_EQ(lr->leader_hint, to_uuid(server.members()[leader].id));
    }
    server.stop();
}
