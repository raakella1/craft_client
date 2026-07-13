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

// test_async_tcp -- the ON-RING TCP transport (craft_async_conn: async connect + HELO + the recv-demux pump)
// driven over REAL loopback sockets at depth. This is the TCP analog of test_async_mem: a real 3-replica cluster
// server (make_tcp_replica_set) + a client whose data path is bound to a test-owned io_uring, with many writes in
// flight at once (QD>1) so replies come back and demux by request_id. Proves the pump works against a real
// server before the standalone-exec + ublk full stack; the transport is the same either way.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

#include <liburing.h>

#include <gtest/gtest.h>

#include <sisl/async/cqe_state.hpp> // is_managed / decode_managed_user_data + complete_cqe_state (the reap contract)
#include <sisl/async/coro.hpp>      // sisl::async::detach

#include <craft/client.hpp>  // client_handle + verbs + prepare_for_async
#include "net/tcp_set.hpp"   // make_tcp_replica_set (a real cluster server + CraftTcpReplica proxies)
#include "craft_replica.hpp" // make_client

#include "craft_test_util.hpp" // rg (sync_get, for the blocking login), PAGE, blk, one_iov, page_of
#include "dlsn_tracker.hpp"    // white-box: tracker_stats fields (issued / unresolved_count) for the depth proof

using namespace craft;
using namespace craft::test;

namespace {
constexpr uint64_t TOKEN = 0xC0FFEEULL;

// A driver-owned io_uring + the reap loop a ublk queue runs: submit queued SQEs, wait for a CQE, drain every
// ready one, completing each managed sisl::async::cqe_state so its suspended leg resumes. Same as test_async_mem;
// here the SQEs are socket connect/send/recv rather than timers, but the reap contract is identical.
struct ring_driver {
    ::io_uring ring{};

    explicit ring_driver(unsigned entries) {
        if (::io_uring_queue_init(entries, &ring, 0) < 0) throw std::runtime_error("io_uring_queue_init failed");
    }
    ~ring_driver() { ::io_uring_queue_exit(&ring); }
    ring_driver(ring_driver const&) = delete;
    ring_driver& operator=(ring_driver const&) = delete;

    void pump(std::chrono::milliseconds slice) {
        __kernel_timespec ts{.tv_sec = slice.count() / 1000, .tv_nsec = (slice.count() % 1000) * 1'000'000LL};
        ::io_uring_cqe* cqe = nullptr;
        int const r = ::io_uring_submit_and_wait_timeout(&ring, &cqe, 1, &ts, nullptr);
        if (r < 0 || nullptr == cqe) return;
        do {
            uint64_t const ud = cqe->user_data;
            int const res = cqe->res;
            ::io_uring_cqe_seen(&ring, cqe);
            if (sisl::async::is_managed_user_data(ud)) {
                if (auto* s = static_cast< sisl::async::cqe_state* >(sisl::async::decode_managed_user_data(ud))) {
                    sisl::async::complete_cqe_state(*s, res);
                }
            }
        } while (0 == ::io_uring_peek_cqe(&ring, &cqe) && nullptr != cqe);
    }

    template < class Pred >
    bool drive_until(Pred pred, int max_slices = 8000) {
        for (int i = 0; i < max_slices; ++i) {
            if (pred()) return true;
            pump(std::chrono::milliseconds{2});
        }
        return pred();
    }

    void drain_for(std::chrono::milliseconds wall) {
        auto const deadline = std::chrono::steady_clock::now() + wall;
        while (std::chrono::steady_clock::now() < deadline) {
            pump(std::chrono::milliseconds{2});
        }
    }
};

// Named wrappers (NOT capturing-lambda coroutines: their frames would dangle after the first suspend). Params by
// value; the buffer each sg_list points at is the test's, alive across the whole drive.
async_status issue_write(client_handle c, uint64_t addr, uint64_t len, sisl::sg_list buf,
                         std::shared_ptr< std::atomic< int > > done, std::shared_ptr< std::atomic< int > > ok) {
    auto const r = co_await craft::write(c, addr, len, buf);
    if (r.has_value()) ok->fetch_add(1, std::memory_order_relaxed);
    done->fetch_add(1, std::memory_order_relaxed);
    co_return craft::ok();
}
async_status issue_read(client_handle c, uint64_t addr, uint64_t len, sisl::sg_list dest,
                        std::shared_ptr< std::atomic< int > > done, std::shared_ptr< std::atomic< int > > ok) {
    auto const r = co_await craft::read(c, addr, len, dest);
    if (r.has_value()) ok->fetch_add(1, std::memory_order_relaxed);
    done->fetch_add(1, std::memory_order_relaxed);
    co_return craft::ok();
}
} // namespace

// N writes then N reads, all in flight over the on-ring TCP transport against a real 3-server cluster. Teardown
// order is load-bearing: the ring_driver is declared LAST so it exits its io_uring FIRST -- before the client and
// the server set destruct -- so each connection's pump frame (suspended on a recv SQE) is destroyed only after
// the ring is gone (no reap can race it), exactly the craft_async_conn::shutdown contract.
TEST(CraftAsyncTcp, DepthWriteReadRoundTripOverTcp) {
    constexpr int N = 8;
    constexpr uint64_t k_capacity = uint64_t{64} << 20;

    auto set = craft::make_tcp_replica_set(/*n=*/3, PAGE, k_capacity, wire::k_default_max_tx);
    std::vector< std::shared_ptr< craft_replica > > backends(set.replicas.begin(), set.replicas.end());
    auto client = craft::make_client(backends);
    ASSERT_TRUE(rg(craft::login(client, TOKEN)).has_value()); // blocking admin login (leader), off the ring
    ASSERT_EQ(craft::lba_size(client), PAGE);

    ring_driver driver{512};
    craft::prepare_for_async(client, &driver.ring); // bind the data path to the test ring; conns open lazily on it

    // ── writes: distinct pattern per block, all N started BEFORE polling ──
    std::vector< std::vector< uint8_t > > wbuf;
    wbuf.reserve(N);
    for (int i = 0; i < N; ++i) {
        wbuf.push_back(page_of(static_cast< uint8_t >(0x21 + i)));
    }

    auto wdone = std::make_shared< std::atomic< int > >(0);
    auto wok = std::make_shared< std::atomic< int > >(0);
    for (int i = 0; i < N; ++i) {
        sisl::async::detach(issue_write(client, blk(i), PAGE, one_iov(wbuf[i]), wdone, wok));
    }

    // DEPTH PROOF (before any reap): every write is suspended lazily connecting/HELO'ing/sending on the ring, so N
    // dLSNs are assigned and none is resolved -- the depth QD=1 can't reach (its first op would resolve first).
    {
        auto const st = craft::dlsn_stats(client);
        EXPECT_EQ(craft::commit_lsn(client), int64_t{-1}) << "nothing has completed yet";
        EXPECT_EQ(st.issued, int64_t{N - 1}) << "all N dLSNs assigned concurrently";
        EXPECT_EQ(st.unresolved_count, std::size_t{N}) << "all N writes in flight at once over TCP";
    }

    ASSERT_TRUE(driver.drive_until([&] { return wdone->load() == N; })) << "every write must resolve over the ring";
    EXPECT_EQ(wok->load(), N) << "all N writes succeeded (quorum over the 3 servers)";
    EXPECT_EQ(craft::commit_lsn(client), int64_t{N - 1}) << "contiguous commit frontier reached the last dLSN";

    // ── reads: zero each buffer first, then read back and verify the bytes came over the wire ──
    std::vector< std::vector< uint8_t > > rbuf(N, std::vector< uint8_t >(PAGE, 0));
    auto rdone = std::make_shared< std::atomic< int > >(0);
    auto rok = std::make_shared< std::atomic< int > >(0);
    for (int i = 0; i < N; ++i) {
        sisl::async::detach(issue_read(client, blk(i), PAGE, one_iov(rbuf[i]), rdone, rok));
    }
    ASSERT_TRUE(driver.drive_until([&] { return rdone->load() == N; })) << "every read must resolve over the ring";
    EXPECT_EQ(rok->load(), N) << "all N reads succeeded";
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(rbuf[i], page_of(static_cast< uint8_t >(0x21 + i))) << "block " << i << " reads back its bytes";
    }

    // Drain any detached straggler legs (a read's keep_alive to the replicas it did not serve) before the ring is
    // exited by ~ring_driver, so no pending socket SQE outlives its coroutine frame.
    driver.drain_for(std::chrono::milliseconds{100});
}
