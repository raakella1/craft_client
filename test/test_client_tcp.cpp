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

// P3 north star: the UNCHANGED craft_client, driven over REAL TCP. Same sequencing logic test_craft_client
// exercises against the in-process mem model, but here each replica sits behind a CraftTcpReplica ->
// craft_tcp_client -> socket -> cluster server -> mem model. This proves the client works over the wire:
// the HEALTHY path (quorum write/read, commit advance, the keep_alive drive), P4.1 fault injection (down
// replicas, forced sub-quorum), and P4.2 the straggler path -- ack at quorum without waiting for the slowest
// leg, the slow write landing late intact, and reconnect + re-HELO after a per-op timeout resets a connection.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <craft/client.hpp>
#include <client_impl.hpp> // white-box: the concrete craft_client
#include "craft_test_util.hpp"
#include <craft/net/tcp_set.hpp> // make_tcp_replica_set (server + proxies, no volumes)

SISL_LOGGING_DEF(homeblocks)
SISL_LOGGING_INIT(homeblocks)
SISL_OPTIONS_ENABLE(logging)

using namespace craft;
using namespace craft::test;

namespace {
constexpr uint64_t TOKEN = 0xABCDEFULL;

// A cluster server + a craft_client driving it over TCP. `client` is declared AFTER `set` so it destructs
// FIRST (releasing its proxy refs), letting ~TcpReplicaSet drain + close the proxies before stopping the server.
struct TcpCluster {
    craft::TcpReplicaSet set;
    std::shared_ptr< craft::craft_client > client;
};

TcpCluster make_tcp_cluster(uint32_t n, std::chrono::milliseconds op_timeout = std::chrono::milliseconds{0}) {
    auto set = craft::make_tcp_replica_set(n, PAGE, /*capacity=*/uint64_t{1} << 30, /*max_tx=*/512 * 1024, op_timeout);
    // The client holds the TCP proxy backends directly (craft_replica) -- no volume_handle, so this TU is
    // HomeStore-free and links without libhomeblocks.
    std::vector< std::shared_ptr< craft::craft_replica > > backends(set.replicas.begin(), set.replicas.end());
    auto client = std::make_shared< craft::craft_client >(std::move(backends), /*leader=*/0);
    EXPECT_TRUE(rg(client->login(TOKEN)).has_value());
    EXPECT_EQ(client->lba_size(), PAGE);
    return TcpCluster{std::move(set), std::move(client)};
}

bool wr(craft::craft_client& c, uint64_t off_blk, std::vector< uint8_t >& buf) {
    return rg(c.write(blk(off_blk), buf.size(), one_iov(buf))).has_value();
}
std::vector< uint8_t > rd(craft::craft_client& c, uint64_t off_blk, uint64_t nblk = 1) {
    std::vector< uint8_t > dest(nblk * PAGE, 0xEE);
    auto r = rg(c.read(blk(off_blk), nblk * PAGE, one_iov(dest)));
    EXPECT_TRUE(r.has_value());
    return dest;
}
} // namespace

// A quorum write to a 3-replica set, then read the bytes back -- the whole stack over the socket.
TEST(CraftClientTcp, N3_QuorumWriteRead) {
    auto tc = make_tcp_cluster(3);
    auto& c = *tc.client;
    auto buf = page_of(0xA5);
    EXPECT_TRUE(wr(c, /*blk=*/0, buf));
    EXPECT_EQ(rd(c, 0), buf);
}

// n=1 round-trip, and an unwritten block reads back as zeros.
TEST(CraftClientTcp, N1_WriteReadAndZeros) {
    auto tc = make_tcp_cluster(1);
    auto& c = *tc.client;
    auto buf = page_of(0x3C);
    EXPECT_TRUE(wr(c, 2, buf));
    EXPECT_EQ(rd(c, 2), buf);
    auto z = rd(c, 5);
    EXPECT_TRUE(std::all_of(z.begin(), z.end(), [](uint8_t b) { return b == 0; }));
}

// A single read spanning several written blocks reconstructs them all.
TEST(CraftClientTcp, N3_MultiBlockWriteRead) {
    auto tc = make_tcp_cluster(3);
    auto& c = *tc.client;
    auto b0 = page_of(0x11);
    auto b34 = page_of(0x22, 2);
    EXPECT_TRUE(wr(c, 0, b0));
    EXPECT_TRUE(wr(c, 3, b34));
    EXPECT_EQ(rd(c, 0), b0);
    EXPECT_EQ(rd(c, 3, 2), b34);
}

// A quorum-acked write advances the client's commit frontier.
TEST(CraftClientTcp, N3_CommitAdvances) {
    auto tc = make_tcp_cluster(3);
    auto& c = *tc.client;
    auto buf = page_of(0x77);
    EXPECT_TRUE(wr(c, 0, buf));
    EXPECT_GE(c.commit_lsn(), 0);
}

// flush() broadcasts keep_alive to every replica, advancing the set-wide reclaim floor over the wire.
TEST(CraftClientTcp, N3_KeepAliveAdvancesReclaimFloor) {
    auto tc = make_tcp_cluster(3);
    auto& c = *tc.client;
    auto buf = page_of(0x5A);
    EXPECT_TRUE(wr(c, 0, buf));
    EXPECT_TRUE(rg(c.flush()).has_value());
    EXPECT_GE(c.all_committed_lsn(), 0);
}

// ── P4: transport-level fault injection over TCP (the cluster server consults the group's fault_state) ──

// force_subquorum excludes replica 2: its server replies REPLICA_DOWN over the wire, but 2 of 3 still ack,
// meeting quorum -- so the write commits and reads back.
TEST(CraftClientTcp, N3_SubquorumStillCommits) {
    auto tc = make_tcp_cluster(3);
    tc.set.server->force_subquorum({0, 1}); // replica 2 excluded
    auto buf = page_of(0x33);
    EXPECT_TRUE(wr(*tc.client, 4, buf));
    EXPECT_EQ(rd(*tc.client, 4), buf); // served by a holder (0 or 1)
}

// Only the leader accepts: 1 of 3 acks, below quorum(3)=2 -> the write fails.
TEST(CraftClientTcp, N3_BelowQuorumFails) {
    auto tc = make_tcp_cluster(3);
    tc.set.server->force_subquorum({0});
    auto buf = page_of(0x44);
    EXPECT_FALSE(wr(*tc.client, 6, buf));
}

// A failed write fires the client-requested resolution round over the wire (RESOLVE to the leader): the
// leader fills the slot from its own copy set-wide, the client retires it off the reply, the frontier
// releases, and the failed write completes late as the durable version.
TEST(CraftClientTcp, N3_FailedWriteResolutionRoundHealsOverTcp) {
    auto tc = make_tcp_cluster(3);
    auto v0 = page_of(0x60);
    auto v1 = page_of(0x61);
    ASSERT_TRUE(wr(*tc.client, 4, v0)); // dLSN 0: durable everywhere
    tc.set.server->force_subquorum({0});
    EXPECT_FALSE(wr(*tc.client, 4, v1)); // dLSN 1 fails; the detached RESOLVE round fires at the leader
    tc.set.server->clear_faults();

    // The round is asynchronous over TCP (it rides the leader proxy's worker): wait for the retire.
    auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (tc.client->commit_lsn() < 1 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds{2});
    EXPECT_EQ(tc.client->commit_lsn(), 1) << "the round filled and retired the failed slot";
    EXPECT_EQ(rd(*tc.client, 4), v1) << "the failed write completed late: it is the durable version";
    EXPECT_EQ(tc.set.server->journal_slots(1), 2u) << "the fill landed set-wide";
}

// Two replicas taken fully down fail their reads with REPLICA_DOWN over the wire; the client routes the read
// around them to the one holder still up.
TEST(CraftClientTcp, N3_DownReplicasRoutedAroundOnRead) {
    auto tc = make_tcp_cluster(3);
    auto buf = page_of(0x55);
    ASSERT_TRUE(wr(*tc.client, 0, buf)); // all 3 hold it
    tc.set.server->set_replica_up(1, false);
    tc.set.server->set_replica_up(2, false); // only the leader (0) remains up
    EXPECT_EQ(rd(*tc.client, 0), buf);       // failover lands on the leader
}

// ── P4.2: the straggler path -- ack at quorum WITHOUT waiting for the slowest leg (the marquee CRAFT win) ──

// Replica 2's serve loop is delayed past the client's per-op deadline. The other two ack, quorum(3)=2 is met,
// and write() returns while replica 2 is still parked in recv on its proxy worker. Without when_quorum + the
// op deadline this would stall for the full delay on every write. Mirrors the mem model's same-named test.
TEST(CraftClientTcp, N3_WriteAcksAtQuorumWithoutWaitingForAStraggler) {
    // op_timeout well past the delay: replica 2's write completes late but succeeds -- the point here is purely
    // that write() returns at quorum, not that the straggler is dropped (the next two tests cover the timeout).
    auto tc = make_tcp_cluster(3, /*op_timeout=*/std::chrono::milliseconds{2000});
    tc.set.server->set_delay(2, std::chrono::milliseconds{400});

    auto buf = page_of(0x7C);
    auto const t0 = std::chrono::steady_clock::now();
    ASSERT_TRUE(wr(*tc.client, 4, buf)) << "quorum(3)=2 met by the two healthy replicas";
    auto const elapsed = std::chrono::steady_clock::now() - t0;

    EXPECT_LT(elapsed, std::chrono::milliseconds{200}) << "returned at quorum, not at the straggler's 400ms delay";
    EXPECT_GE(tc.client->commit_lsn(), 0) << "the slot acked; the frontier advanced";
}

// The straggler's write must LAND -- delivered late, never lost -- and with the bytes as of ISSUE time. The
// client acks at quorum, the op to replica 2 times out (poisoning its connection), and the caller recycles its
// buffer; the in-flight write already copied the payload before hopping, so replica 2 applies 0x5E, not the
// recycled 0xFF, once its delay elapses. Proven straight off replica 2, server-side. Mirrors the mem model.
TEST(CraftClientTcp, N3_StragglerWriteLandsIntactAfterTheClientReturned) {
    auto tc = make_tcp_cluster(3, /*op_timeout=*/std::chrono::milliseconds{20});
    tc.set.server->set_delay(2, std::chrono::milliseconds{200});

    auto buf = page_of(0x5E);
    ASSERT_TRUE(wr(*tc.client, 6, buf)) << "acks at quorum while replica 2 is still in flight";
    EXPECT_EQ(tc.set.server->journal_slots(2), 0u) << "not delivered yet";

    std::fill(buf.begin(), buf.end(), 0xFF); // the caller recycles its buffer the instant write() acked
    tc.set.server->set_delay(2, std::chrono::milliseconds{0}); // lift so the verifying path is not itself delayed

    auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (tc.set.server->journal_slots(2) == 0u && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds{2});
    ASSERT_EQ(tc.set.server->journal_slots(2), 1u) << "replica 2 applied the late write after the deadline";

    std::vector< uint8_t > dest;
    ASSERT_TRUE(tc.set.server->read_replica(2, /*H=*/0, blk(6), PAGE, dest));
    EXPECT_EQ(dest, page_of(0x5E)) << "payload copied at issue (before the hop), not after the sleep";
}

// After a straggler times out and poisons replica 2's connection, the NEXT op to replica 2 must reconnect and
// re-HELO onto the same session. With the delay lifted, a second write lands on all three -- so replica 2's
// journal reaches 2, proving the reset->reconnect->re-HELO path over the wire.
TEST(CraftClientTcp, N3_ReconnectsAndReHelosAfterAStragglerTimeout) {
    auto tc = make_tcp_cluster(3, /*op_timeout=*/std::chrono::milliseconds{20});
    tc.set.server->set_delay(2, std::chrono::milliseconds{200});

    auto b0 = page_of(0x10);
    ASSERT_TRUE(wr(*tc.client, 0, b0)); // acks at quorum; replica 2's op times out -> its proxy is poisoned

    auto const d0 = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (tc.set.server->journal_slots(2) == 0u && std::chrono::steady_clock::now() < d0)
        std::this_thread::sleep_for(std::chrono::milliseconds{2});
    ASSERT_EQ(tc.set.server->journal_slots(2), 1u) << "the first (late) write landed on replica 2";

    tc.set.server->set_delay(2, std::chrono::milliseconds{0}); // replica 2 is healthy again
    auto b1 = page_of(0x20);
    ASSERT_TRUE(wr(*tc.client, 1, b1)); // replica 2's poisoned proxy must reconnect + re-HELO to serve this

    auto const d1 = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (tc.set.server->journal_slots(2) < 2u && std::chrono::steady_clock::now() < d1)
        std::this_thread::sleep_for(std::chrono::milliseconds{2});
    EXPECT_EQ(tc.set.server->journal_slots(2), 2u) << "replica 2 reconnected, re-HELO'd, and applied the 2nd write";
}

// ── P4.3: logout tears the session down, relogin bumps the term, followers must re-HELO at the new term ──

// The marquee of P4.3: a logout+relogin bumps the session term set-wide, but the follower CONNECTIONS were
// bound at the old term and the wire carries no per-op term -- so unless they re-HELO, the server stamps the
// stale term on their IO and fences it, collapsing quorum. Watch the term clear to 0 on logout, come back
// bumped on relogin, and the next write reach quorum only because the followers rebound. Mirrors the mem model.
TEST(CraftClientTcp, LogoutThenReloginReHelosFollowersAtTheNewTerm) {
    auto tc = make_tcp_cluster(3);
    auto v = page_of(0x5A);
    ASSERT_TRUE(wr(*tc.client, 7, v)) << "session active: write succeeds";
    uint64_t const t0 = tc.client->term();
    ASSERT_NE(t0, 0u);
    for (uint32_t i = 0; i < 3; ++i)
        EXPECT_EQ(tc.set.server->replica_term(i), t0) << "session term held on every replica";

    ASSERT_TRUE(rg(tc.client->logout()).has_value());
    for (uint32_t i = 0; i < 3; ++i)
        EXPECT_EQ(tc.set.server->replica_term(i), 0u) << "logout tore the session down on every replica";

    ASSERT_TRUE(rg(tc.client->login(TOKEN)).has_value());
    uint64_t const t1 = tc.client->term();
    EXPECT_NE(t1, 0u) << "fresh session";
    EXPECT_NE(t1, t0) << "relogin bumped the term";
    for (uint32_t i = 0; i < 3; ++i)
        EXPECT_EQ(tc.set.server->replica_term(i), t1) << "the new term reached every replica";

    // The write lands on quorum ONLY if the follower connections re-HELO'd at t1: bound at t0 they would fence
    // stale_term, leaving just the (re-logged-in) leader, which is below quorum(3)=2.
    auto v3 = page_of(0x5C);
    EXPECT_TRUE(wr(*tc.client, 7, v3)) << "followers re-HELO'd at the new term -> quorum met";
    EXPECT_EQ(rd(*tc.client, 7), v3);
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    sisl::logging::SetLogPattern("[%D %T%z] [%^%L%$] [%t] %v");
    return RUN_ALL_TESTS();
}
