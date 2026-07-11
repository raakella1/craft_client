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

// The PUBLIC driver surface: assemble a client the way a consumer does -- a public backend builder
// (craft/local.hpp) + make_client -- then drive it ENTIRELY through the free-function verbs. No craft_client
// type and no internal (mem/, net/) header in sight: exactly the headers a driver consumer includes.

#include <gtest/gtest.h>

#include <boost/uuid/uuid_generators.hpp>

#include <craft/client.hpp> // the opaque handle + the free-function verbs
#include <craft/local.hpp>  // local_cluster: the in-process backend builder (make_client seam behind it)

#include "craft_test_util.hpp" // PAGE, blk, page_of, one_iov, rg

using namespace craft;
using namespace craft::test;

TEST(CraftApi, FreeFunctionsOverTheOpaqueHandle) {
    volume_id_t const vid = boost::uuids::random_generator()();
    auto cluster = make_local_cluster(vid, 3, PAGE); // owns the reference cluster; outlives the client (below)

    client_handle const c = make_client(backends(cluster)); // the opaque handle -- no craft_client visible

    ASSERT_TRUE(rg(login(c, 0xABCDEF)).has_value());
    EXPECT_EQ(lba_size(c), PAGE);
    EXPECT_EQ(capacity(c), uint64_t{1} << 30); // login surfaces the volume geometry (local_cluster default)
    EXPECT_GT(term(c), 0u);

    auto buf = page_of(0xA5);
    ASSERT_TRUE(rg(write(c, blk(0), buf.size(), one_iov(buf))).has_value());
    EXPECT_GE(commit_lsn(c), 0);

    std::vector< uint8_t > dest(PAGE, 0xEE);
    ASSERT_TRUE(rg(read(c, blk(0), PAGE, one_iov(dest))).has_value());
    EXPECT_EQ(dest, buf);

    EXPECT_TRUE(rg(flush(c)).has_value());
    EXPECT_GE(all_committed_lsn(c), 0);
    EXPECT_TRUE(rg(logout(c)).has_value());
}

// The builder's fault knobs are free functions over the handle too (no concrete cluster type), and their effect
// is observable through the ordinary client verbs: a sub-quorum drop makes the next write fail to commit.
TEST(CraftApi, BuilderFaultsAreFreeFunctionsOverTheHandle) {
    volume_id_t const vid = boost::uuids::random_generator()();
    auto cluster = make_local_cluster(vid, 3, PAGE);
    client_handle const c = make_client(backends(cluster));
    ASSERT_TRUE(rg(login(c, 0x1)).has_value());

    auto buf = page_of(0x11);
    ASSERT_TRUE(rg(write(c, blk(0), buf.size(), one_iov(buf))).has_value()); // healthy: reaches quorum

    // Only replica 0 accepts writes -> the next broadcast cannot reach a quorum of acks.
    force_subquorum(cluster, {0});
    EXPECT_FALSE(rg(write(c, blk(1), buf.size(), one_iov(buf))).has_value());

    // Lift it (fresh block, no per-block dLSN interaction): writes reach quorum again.
    clear_faults(cluster);
    EXPECT_TRUE(rg(write(c, blk(2), buf.size(), one_iov(buf))).has_value());

    EXPECT_TRUE(rg(logout(c)).has_value());
}
