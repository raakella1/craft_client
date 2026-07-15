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
#include <barrier>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

#include <liburing.h>

#include <gtest/gtest.h>

#include <sisl/async/cqe_state.hpp>  // is_managed / decode_managed_user_data + complete_cqe_state (the reap contract)
#include <sisl/async/light_task.hpp> // .detach() on the issue_* legs

#include <craft/client.hpp>  // client_handle + verbs (the ring-taking async overloads)
#include "net/tcp_set.hpp"   // make_tcp_replica_set (a real cluster server + CraftTcpReplica proxies)
#include "craft_replica.hpp" // make_client

#include "craft_test_util.hpp" // rg (sync_get, for the blocking login), PAGE, blk, one_iov, page_of
#include "dlsn_tracker.hpp"    // white-box: tracker_stats fields (issued / unresolved_count) for the depth proof

using namespace craft;
using namespace craft::test;
using sisl::ok;
template < typename T >
using result = sisl::result< T >;
using status = sisl::status;

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
// value; the buffer each sg_list points at is the test's, alive across the whole drive. Each takes the queue
// ring `q` right after the handle (the ublk parameter order) and passes it to the verb.
async_status issue_write(client_handle c, ::io_uring* q, uint64_t addr, uint64_t len, sisl::sg_list buf,
                         std::shared_ptr< std::atomic< int > > done, std::shared_ptr< std::atomic< int > > ok) {
    auto const r = co_await craft::write(c, q, addr, len, buf);
    if (r.has_value()) ok->fetch_add(1, std::memory_order_relaxed);
    done->fetch_add(1, std::memory_order_relaxed);
    co_return sisl::ok();
}
async_status issue_read(client_handle c, ::io_uring* q, uint64_t addr, uint64_t len, sisl::sg_list dest,
                        std::shared_ptr< std::atomic< int > > done, std::shared_ptr< std::atomic< int > > ok) {
    auto const r = co_await craft::read(c, q, addr, len, dest);
    if (r.has_value()) ok->fetch_add(1, std::memory_order_relaxed);
    done->fetch_add(1, std::memory_order_relaxed);
    co_return sisl::ok();
}
async_status issue_flush(client_handle c, ::io_uring* q, std::shared_ptr< std::atomic< bool > > flushed) {
    (void)co_await craft::flush(c, q);
    flushed->store(true, std::memory_order_relaxed);
    co_return sisl::ok();
}

// Proxy-direct wrappers for ResolveOverRing: drive one CraftTcpReplica below the client (the test assigns
// dLSNs by hand to shape the journal). Same no-capturing-lambda rule as above; the proxy outlives the drive.
async_status issue_proxy_write(CraftTcpReplica* p, ::io_uring* q, client_hdr hdr, int64_t dlsn, uint64_t addr,
                               uint64_t len, sisl::sg_list buf, std::shared_ptr< std::atomic< int > > done,
                               std::shared_ptr< std::atomic< int > > ok) {
    auto const r = co_await p->write(q, hdr, dlsn, addr, len, std::move(buf));
    if (r.has_value()) ok->fetch_add(1, std::memory_order_relaxed);
    done->fetch_add(1, std::memory_order_relaxed);
    co_return sisl::ok();
}
async_status issue_resolution(CraftTcpReplica* p, ::io_uring* q, client_hdr hdr, int64_t upto,
                              std::shared_ptr< std::optional< result< resolution_result > > > out,
                              std::shared_ptr< std::atomic< int > > done) {
    auto r = co_await p->request_resolution(q, hdr, upto);
    *out = std::move(r);
    done->fetch_add(1, std::memory_order_relaxed);
    co_return sisl::ok();
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

    ring_driver driver{512}; // every async verb below carries this ring; conns open lazily on it

    // ── writes: distinct pattern per block, all N started BEFORE polling ──
    std::vector< std::vector< uint8_t > > wbuf;
    wbuf.reserve(N);
    for (int i = 0; i < N; ++i) {
        wbuf.push_back(page_of(static_cast< uint8_t >(0x21 + i)));
    }

    auto wdone = std::make_shared< std::atomic< int > >(0);
    auto wok = std::make_shared< std::atomic< int > >(0);
    for (int i = 0; i < N; ++i) {
        issue_write(client, &driver.ring, blk(i), PAGE, one_iov(wbuf[i]), wdone, wok).detach();
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
        issue_read(client, &driver.ring, blk(i), PAGE, one_iov(rbuf[i]), rdone, rok).detach();
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

// RESOLVE rides the data connection once the ring is primed. It fires from the write path's failure branch --
// the ring thread in async mode -- so the blocking worker hop would stall the reactor; instead it parks a
// reply slot on the pump like any data op. Drive the leader proxy directly: land dLSN 0 and 2 over the ring,
// leaving the hole a failed sub-quorum write would leave at 1, then resolve upto=2 and expect the hole
// verdicted Empty. The negative check is the on-ring proof: a detached resolve leg makes NO progress until the
// ring is pumped (its send SQE is not even submitted) -- the worker path would complete on its own thread well
// within the sleep.
TEST(CraftAsyncTcp, ResolveOverRing) {
    constexpr uint64_t k_capacity = uint64_t{64} << 20;
    auto set = craft::make_tcp_replica_set(/*n=*/3, PAGE, k_capacity, wire::k_default_max_tx);
    auto& leader = *set.replicas[0]; // index 0 is the leader

    auto lr = rg(leader.login(TOKEN)); // blocking, pre-ring: the production ordering (login -> geometry -> ring)
    ASSERT_TRUE(lr.has_value());
    uint64_t const term = lr->term;

    ring_driver driver{256};

    auto d0 = page_of(0xA0);
    auto d2 = page_of(0xA2);
    auto wdone = std::make_shared< std::atomic< int > >(0);
    auto wok = std::make_shared< std::atomic< int > >(0);
    issue_proxy_write(&leader, &driver.ring, chdr(term), /*dlsn=*/0, blk(0), PAGE, one_iov(d0), wdone, wok).detach();
    issue_proxy_write(&leader, &driver.ring, chdr(term), /*dlsn=*/2, blk(2), PAGE, one_iov(d2), wdone, wok).detach();
    ASSERT_TRUE(driver.drive_until([&] { return wdone->load() == 2; })) << "both writes must land over the ring";
    ASSERT_EQ(wok->load(), 2);

    auto out = std::make_shared< std::optional< result< resolution_result > > >();
    auto rdone = std::make_shared< std::atomic< int > >(0);
    issue_resolution(&leader, &driver.ring, chdr(term), /*upto=*/2, out, rdone).detach();
    std::this_thread::sleep_for(std::chrono::milliseconds{25});
    EXPECT_EQ(rdone->load(), 0) << "resolve must park on the ring, not complete on the worker";

    ASSERT_TRUE(driver.drive_until([&] { return rdone->load() == 1; })) << "resolve must complete once pumped";
    ASSERT_TRUE(out->has_value() && (*out)->has_value()) << "the leader ran the round";
    EXPECT_EQ((**out)->resolved_upto, 2) << "everything <= upto is resolved set-wide";
    ASSERT_EQ((**out)->empty_slots.size(), 1u) << "the hole at dLSN 1 is the round's one Empty verdict";
    EXPECT_EQ((**out)->empty_slots[0], 1);

    driver.drain_for(std::chrono::milliseconds{25}); // nothing detached should remain; drain before ~ring_driver
}

namespace {
// One blk-mq queue over real TCP: its own ring + reap loop on its own thread, a disjoint block range on the
// SHARED client. The ring is a stack local, so it exits at thread end -- after drain_for -- and strictly before
// the client/server teardown on the main thread (threads are joined first): the plural ring-before-owner rule.
void run_tcp_queue(client_handle client, int qi, int writes, std::barrier<>& start, std::atomic< int >& failures) {
    ring_driver driver{512};
    std::vector< std::vector< uint8_t > > wbuf;
    wbuf.reserve(writes);
    for (int i = 0; i < writes; ++i) {
        wbuf.push_back(page_of(static_cast< uint8_t >(0x40 + qi * writes + i)));
    }
    start.arrive_and_wait();

    // Every exit path must drain before ~ring_driver: a failure return that skipped the drain would destroy
    // the ring under this queue's still-parked detached legs (leaking frames AND stranding the router's
    // single-flight keepalive flags, cascading into the OTHER queues' failures and masking the real one).
    auto const fail_and_drain = [&] {
        failures.fetch_add(1, std::memory_order_relaxed);
        driver.drain_for(std::chrono::milliseconds{100});
    };

    auto wdone = std::make_shared< std::atomic< int > >(0);
    auto wok = std::make_shared< std::atomic< int > >(0);
    for (int i = 0; i < writes; ++i) {
        issue_write(client, &driver.ring, blk(qi * writes + i), PAGE, one_iov(wbuf[i]), wdone, wok).detach();
    }
    if (!driver.drive_until([&] { return wdone->load() == writes; }) || wok->load() != writes) {
        fail_and_drain();
        return;
    }

    auto flushed = std::make_shared< std::atomic< bool > >(false);
    issue_flush(client, &driver.ring, flushed).detach();
    if (!driver.drive_until([&] { return flushed->load(); })) {
        fail_and_drain();
        return;
    }

    std::vector< std::vector< uint8_t > > rbuf(writes, std::vector< uint8_t >(PAGE, 0));
    auto rdone = std::make_shared< std::atomic< int > >(0);
    auto rok = std::make_shared< std::atomic< int > >(0);
    for (int i = 0; i < writes; ++i) {
        issue_read(client, &driver.ring, blk(qi * writes + i), PAGE, one_iov(rbuf[i]), rdone, rok).detach();
    }
    if (!driver.drive_until([&] { return rdone->load() == writes; }) || rok->load() != writes) {
        fail_and_drain();
        return;
    }
    for (int i = 0; i < writes; ++i) {
        if (rbuf[i] != wbuf[i]) { // compare against what we actually wrote, not a re-derived pattern
            failures.fetch_add(1, std::memory_order_relaxed);
        }
    }
    driver.drain_for(std::chrono::milliseconds{100}); // retire this queue's detached keep_alive legs
}
} // namespace

// The nr_hw_queues x N connection GRID over real sockets: Q queue threads, each with its own ring, drive one
// shared client against a 3-server cluster. Beyond the data integrity the mem twin proves, the server-side
// accept count witnesses it: one data connection per (queue, replica) -- Q x 3 -- plus one admin/session socket
// per replica. Admission logs in on one member and HELO-probes all three (each returns its commit_lsn for the
// read-eligibility gate), so all three session sockets stand up before any queue IO -- hence Q x 3 + 3.
TEST(CraftAsyncTcp, MultiQueueGridOverTcp) {
    constexpr int Q = 3; // queues (threads x rings)
    constexpr int N = 6; // writes per queue
    constexpr uint64_t k_capacity = uint64_t{64} << 20;

    auto set = craft::make_tcp_replica_set(/*n=*/3, PAGE, k_capacity, wire::k_default_max_tx);
    std::vector< std::shared_ptr< craft_replica > > backends(set.replicas.begin(), set.replicas.end());
    auto client = craft::make_client(backends);
    ASSERT_TRUE(rg(craft::login(client, TOKEN)).has_value()); // 3 session sockets (login + HELO-probe all), pre-IO

    std::barrier<> start{Q};
    std::atomic< int > failures{0};
    std::vector< std::thread > queues;
    queues.reserve(Q);
    for (int qi = 0; qi < Q; ++qi) {
        queues.emplace_back(run_tcp_queue, client, qi, N, std::ref(start), std::ref(failures));
    }
    for (auto& t : queues)
        t.join(); // every queue drained + exited its ring; only now may the client/server teardown proceed

    EXPECT_EQ(failures.load(), 0) << "every queue must complete all its writes and verified reads";
    EXPECT_EQ(craft::commit_lsn(client), int64_t{Q * N - 1}) << "dLSNs stay dense across concurrently-reserving queues";
    // Exact on purpose -- the point is ONE data socket per (queue, replica), no more. Exactness assumes no
    // transient reconnect (on_net_fault poisons a conn; the next op redials) and a leader at index 0 (no
    // NOT_LEADER redirect opening a second admin socket); both hold on a healthy loopback set. If this ever
    // flakes in CI, a reconnect happened -- investigate before loosening.
    EXPECT_EQ(set.server->connections_accepted(), std::size_t{Q * 3 + 3})
        << "one data socket per (queue, replica), plus one admin/session socket per replica (admission logs in "
           "on one and HELO-probes all three)";
}
