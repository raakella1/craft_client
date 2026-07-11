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

// The PUBLIC remote assembly path: craft/tcp.hpp's make_tcp_cluster() -> make_client, driven over a real
// TCP loopback. A consumer names only endpoints and the two public headers (tcp.hpp for the builder, client.hpp
// for the verbs) -- never a src/net proxy type. The server here is the reference cluster standing in for real
// replica servers; a HomeBlocks CRAFT server would be wire-compatible in its place.

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <craft/client.hpp>             // the opaque handle + the free-function verbs
#include <craft/tcp.hpp>                // make_tcp_cluster -- the PUBLIC builder under test
#include <craft/net/cluster_server.hpp> // the reference server to connect to (test-support)

#include "craft_test_util.hpp" // PAGE, blk, page_of, one_iov, rg

using namespace craft;
using namespace craft::test;

TEST(CraftTcpBuilder, ConnectAndDriveOverTcp) {
    // A reference cluster server, standing in for real replica servers listening on TCP.
    auto server = std::make_shared< net::craft_cluster_server >(3, PAGE, /*capacity=*/uint64_t{1} << 30,
                                                                /*max_tx=*/512 * 1024);
    server->start();

    // Discover endpoints the way a consumer would (from a seed / the login member list): {peer id, "host:port"}.
    auto const members = server->members();
    std::vector< replica_endpoint > eps;
    eps.reserve(members.size());
    for (std::size_t i = 0; i < members.size(); ++i) {
        peer_id_t id;
        std::memcpy(&id, members[i].id.data(), 16);
        eps.push_back({id, "127.0.0.1:" + std::to_string(server->port(i))});
    }
    volume_id_t vid;
    std::memcpy(&vid, server->volume_id().data(), 16);

    {
        // The PUBLIC assembly: make_tcp_cluster -> backends -> make_client. No internal proxy type in sight.
        auto cluster = make_tcp_cluster(eps, vid);
        client_handle const c = make_client(backends(cluster));

        ASSERT_TRUE(rg(login(c, 0x1234)).has_value());
        EXPECT_EQ(lba_size(c), PAGE);
        EXPECT_EQ(capacity(c), uint64_t{1} << 30); // surfaced from the server's login response over the wire
        EXPECT_GT(term(c), 0u);

        auto buf = page_of(0x5A);
        ASSERT_TRUE(rg(write(c, blk(3), buf.size(), one_iov(buf))).has_value());
        EXPECT_GE(commit_lsn(c), 0);

        std::vector< uint8_t > dest(PAGE, 0x00);
        ASSERT_TRUE(rg(read(c, blk(3), PAGE, one_iov(dest))).has_value());
        EXPECT_EQ(dest, buf);

        EXPECT_TRUE(rg(flush(c)).has_value());
        EXPECT_TRUE(rg(logout(c)).has_value());
    } // c destroyed first (releases its proxy refs), then cluster drains each worker -- all BEFORE stop() below

    server->stop();
}
