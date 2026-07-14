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

// test_async_mem -- the reference model driven over the ON-RING async transport (the ring-taking verb
// overloads), NOT the MemTransport pool. This is the one configuration the pool path structurally cannot
// reach: many replica legs suspended at once on a driver-owned io_uring, completing out of order as their
// timers fire on the reap thread.
// That is the depth CRAFT's correctness is emergent from -- with a single in-flight op the dlsn_tracker never
// leaves its degenerate straight-line mode (frontier == last dLSN, nothing unresolved, no straggler). Here we
// stand up exactly what a ublk queue does: a real ring, a reap loop that dispatches managed sisl::async::cqe_state
// (mirroring ublkpp's MockUblksrv::poll / io_uring_scheduler::poll_once), and prove: N writes go in flight at
// once; the fast quorum acks WITHOUT the straggler; the straggler's detached leg still lands its write late over
// the ring; and every block reads back its bytes -- the whole write+read path, on the ring.

#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#include <liburing.h>

#include <boost/uuid/uuid_generators.hpp> // random volume id
#include <gtest/gtest.h>

#include <sisl/async/cqe_state.hpp>  // is_managed / decode_managed_user_data + complete_cqe_state (the reap contract)
#include <sisl/async/light_task.hpp> // .detach() on the issue_* legs

#include <craft/client.hpp>  // client_handle + verbs (the ring-taking async overloads)
#include "craft_replica.hpp" // make_client
#include "mem/cluster.hpp"   // make_mem_replica_group (the reference set + its replicas)
#include "mem/replica.hpp"   // MemCraftReplica::set_delay / stats()

#include "craft_test_util.hpp" // rg (sync_get, for the pool-path login), PAGE, blk, one_iov, page_of
#include "dlsn_tracker.hpp"    // white-box: tracker_stats fields (issued / unresolved_count) for the depth proof

using namespace craft;
using namespace craft::test;

namespace {
constexpr uint64_t TOKEN = 0xC0FFEEULL;

// A driver-owned io_uring + the reap loop a ublk queue runs in production: submit the SQEs the transport queued,
// wait a slice for a CQE, then drain every ready one -- completing each managed sisl::async::cqe_state so its
// suspended leg resumes. This IS the "driver drives the reaping" contract the async verb overloads document;
// the transport only ever queues SQEs. Mirrors ublkpp::MockUblksrv::poll and io_uring_scheduler::poll_once.
struct ring_driver {
    ::io_uring ring{};

    explicit ring_driver(unsigned entries) {
        if (::io_uring_queue_init(entries, &ring, 0) < 0) throw std::runtime_error("io_uring_queue_init failed");
    }
    ~ring_driver() { ::io_uring_queue_exit(&ring); }
    ring_driver(ring_driver const&) = delete;
    ring_driver& operator=(ring_driver const&) = delete;

    // One reap slice: flush queued SQEs + wait up to `slice` for a CQE (one syscall), then drain all that are
    // ready. Consume each CQE BEFORE resuming its leg, so a resumed leg's own peek sees the next one, never this.
    void pump(std::chrono::milliseconds slice) {
        __kernel_timespec ts{.tv_sec = slice.count() / 1000, .tv_nsec = (slice.count() % 1000) * 1'000'000LL};
        ::io_uring_cqe* cqe = nullptr;
        int const r = ::io_uring_submit_and_wait_timeout(&ring, &cqe, 1, &ts, nullptr);
        if (r < 0 || nullptr == cqe) return; // -ETIME / -EINTR: no completion this slice
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

    // Pump until `pred()` holds, bounded so a hung leg fails the assertion instead of spinning forever.
    template < class Pred >
    bool drive_until(Pred pred, int max_slices = 4000) {
        for (int i = 0; i < max_slices; ++i) {
            if (pred()) return true;
            pump(std::chrono::milliseconds{2});
        }
        return pred();
    }

    // Keep reaping for `wall`, to drain any still-detached straggler legs (e.g. a slow replica's keep_alive a
    // read left running) so no ring SQE points at a live coroutine frame when the cluster is torn down.
    void drain_for(std::chrono::milliseconds wall) {
        auto const deadline = std::chrono::steady_clock::now() + wall;
        while (std::chrono::steady_clock::now() < deadline) {
            pump(std::chrono::milliseconds{2});
        }
    }
};

// Named wrappers, NOT capturing-lambda coroutines: a lambda coroutine's frame dangles its captures after the
// first suspend. Params by value; the buffer each sg_list points at is the test's, alive across the whole drive.
// Each takes the queue ring `q` right after the handle (the ublk parameter order) and passes it to the verb.
async_status issue_write(client_handle c, ::io_uring* q, uint64_t addr, uint64_t len, sisl::sg_list buf,
                         std::shared_ptr< std::atomic< int > > done, std::shared_ptr< std::atomic< int > > ok) {
    auto const r = co_await craft::write(c, q, addr, len, buf);
    if (r.has_value()) ok->fetch_add(1, std::memory_order_relaxed);
    done->fetch_add(1, std::memory_order_relaxed);
    co_return craft::ok();
}
async_status issue_read(client_handle c, ::io_uring* q, uint64_t addr, uint64_t len, sisl::sg_list dest,
                        std::shared_ptr< std::atomic< int > > done, std::shared_ptr< std::atomic< int > > ok) {
    auto const r = co_await craft::read(c, q, addr, len, dest);
    if (r.has_value()) ok->fetch_add(1, std::memory_order_relaxed);
    done->fetch_add(1, std::memory_order_relaxed);
    co_return craft::ok();
}
async_status issue_flush(client_handle c, ::io_uring* q, std::shared_ptr< std::atomic< bool > > flushed) {
    (void)co_await craft::flush(c, q);
    flushed->store(true, std::memory_order_relaxed);
    co_return craft::ok();
}

std::size_t journal_slots(std::shared_ptr< MemCraftReplica > const& r) { return r->stats().journal_slots; }
} // namespace

// Everything from the client's write through the reference replica's apply runs over a driver-owned ring at
// depth N: the depth QD=1 can never produce, and the reason CRAFT correctness needs this path to be testable.
TEST(CraftAsyncMem, DepthAtOnceQuorumWithoutStragglerThenStragglerDrainsOnRing) {
    constexpr int N = 8;
    volume_id_t const vid = boost::uuids::random_generator()();

    // 3 reference replicas + a client over their backends. Log in over the POOL path first (login is cold-path,
    // pool-completed -- rg/sync_get is fine and there is no ring bound yet).
    auto set = craft::make_mem_replica_group(vid, 3, PAGE);
    std::vector< std::shared_ptr< craft_replica > > backends(set.replicas.begin(), set.replicas.end());
    auto client = craft::make_client(backends);
    ASSERT_TRUE(rg(craft::login(client, TOKEN)).has_value());
    ASSERT_EQ(craft::lba_size(client), PAGE);

    // One straggler (replica 2, 15ms) vs a fast pair (1ms): every write's quorum forms from the fast pair while
    // the straggler's leg is still on its timer -- the quorum-without-the-slowest that leaves a Missing slot.
    set.replicas[0]->set_delay(std::chrono::milliseconds{1});
    set.replicas[1]->set_delay(std::chrono::milliseconds{1});
    set.replicas[2]->set_delay(std::chrono::milliseconds{15});

    // The driver's ring: every async verb below carries it, so the data path only ever QUEUES SQEs on it; the
    // reap loop is what completes them. Ring sized well past N*3 in-flight legs + the later reads/keep_alives.
    ring_driver driver{256};

    // ── issue N writes at distinct blocks, each a distinct byte pattern; do NOT drive yet ──
    std::vector< std::vector< uint8_t > > wbuf;
    wbuf.reserve(N);
    for (int i = 0; i < N; ++i) {
        wbuf.push_back(page_of(static_cast< uint8_t >(0x10 + i)));
    }

    auto wdone = std::make_shared< std::atomic< int > >(0);
    auto wok = std::make_shared< std::atomic< int > >(0);
    for (int i = 0; i < N; ++i) {
        issue_write(client, &driver.ring, blk(i), PAGE, one_iov(wbuf[i]), wdone, wok).detach();
    }

    // DEPTH PROOF (before any reap): every leg is suspended on its ring timer, so N dLSNs are handed out and NONE
    // is resolved. At QD=1 the first write would have to resolve before the second could even be issued, so
    // unresolved_count could never reach N. This is the tracker OUT of its degenerate straight-line mode.
    {
        auto const st = craft::dlsn_stats(client);
        EXPECT_EQ(craft::commit_lsn(client), int64_t{-1}) << "nothing has completed yet";
        EXPECT_EQ(st.issued, int64_t{N - 1}) << "all N dLSNs assigned concurrently";
        EXPECT_EQ(st.unresolved_count, std::size_t{N}) << "all N writes in flight at once -- depth QD=1 can't reach";
    }

    // ── drive: the fast pair's 1ms timers fire, each write acks at quorum WITHOUT the 15ms straggler ──
    ASSERT_TRUE(driver.drive_until([&] { return wdone->load() == N; })) << "every write must resolve at quorum";
    EXPECT_EQ(wok->load(), N) << "all N writes succeeded on the fast quorum";
    EXPECT_EQ(craft::commit_lsn(client), int64_t{N - 1}) << "contiguous commit frontier reached the last dLSN";
    // The fast pair holds every write; the straggler is (still) behind -- its writes are detached, mid-timer.
    EXPECT_EQ(journal_slots(set.replicas[0]), std::size_t{N});
    EXPECT_EQ(journal_slots(set.replicas[1]), std::size_t{N});

    // ── STRAGGLER DRAINS: when_quorum left the slow leg running, so its own 15ms ring timer still lands the
    //    write, late, over the ring. Drive until replica 2 has caught up to the full set. ──
    ASSERT_TRUE(driver.drive_until([&] { return journal_slots(set.replicas[2]) == std::size_t{N}; }))
        << "the detached straggler legs must land replica 2's writes late, via their own ring timers";

    // ── settle the read horizon (flush = keep_alive broadcast, also on the ring), then read every block back and
    //    verify its bytes: the READ path is on the ring too, and the data is what we wrote. ──
    auto flushed = std::make_shared< std::atomic< bool > >(false);
    issue_flush(client, &driver.ring, flushed).detach();
    ASSERT_TRUE(driver.drive_until([&] { return flushed->load(); }));
    EXPECT_GE(craft::read_horizon(client), int64_t{N - 1}) << "the horizon covers every committed write";

    std::vector< std::vector< uint8_t > > rbuf(N, std::vector< uint8_t >(PAGE, 0));
    auto rdone = std::make_shared< std::atomic< int > >(0);
    auto rok = std::make_shared< std::atomic< int > >(0);
    for (int i = 0; i < N; ++i) {
        issue_read(client, &driver.ring, blk(i), PAGE, one_iov(rbuf[i]), rdone, rok).detach();
    }
    ASSERT_TRUE(driver.drive_until([&] { return rdone->load() == N; })) << "every read must resolve over the ring";
    EXPECT_EQ(rok->load(), N) << "all N reads succeeded";
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(rbuf[i], page_of(static_cast< uint8_t >(0x10 + i))) << "block " << i << " reads back its bytes";
    }

    // Drain any still-detached legs (e.g. a slow-replica keep_alive a read left running) before teardown, so no
    // ring SQE outlives its coroutine frame. All timers are <= 15ms; this is well past the longest.
    driver.drain_for(std::chrono::milliseconds{60});
}

namespace {
// One blk-mq queue: its own ring + reap loop on its own thread, driving the SHARED client over a disjoint block
// range. Everything a ublk hw-queue thread does, minus the kernel. The ring_driver is a stack local, so this
// queue's ring exits when the thread ends -- after drain_for has retired every detached leg it fired -- and
// strictly before the client/cluster teardown on the main thread (threads are joined first).
void run_queue(client_handle client, int qi, int writes, std::barrier<>& start, std::atomic< int >& failures) {
    ring_driver driver{256};
    std::vector< std::vector< uint8_t > > wbuf;
    wbuf.reserve(writes);
    for (int i = 0; i < writes; ++i) {
        wbuf.push_back(page_of(static_cast< uint8_t >(0x30 + qi * writes + i)));
    }
    start.arrive_and_wait(); // all queues issue together: cross-queue dLSN interleave is the point

    // Every exit path must drain before ~ring_driver: a failure return that skipped the drain would destroy
    // the ring under this queue's still-parked detached legs (leaking frames AND stranding the router's
    // single-flight keepalive flags, cascading into the OTHER queues' failures and masking the real one).
    auto const fail_and_drain = [&] {
        failures.fetch_add(1, std::memory_order_relaxed);
        driver.drain_for(std::chrono::milliseconds{60});
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

    // Settle this queue's read horizon (flush = keep_alive broadcast, on THIS ring), then read our blocks back.
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
    // Drain the detached legs THIS queue fired (straggler writes, read-path keep_alives) before its ring exits.
    driver.drain_for(std::chrono::milliseconds{60});
}
} // namespace

// The blk-mq shape: Q queue threads, each with its OWN io_uring, drive ONE shared client concurrently -- each
// verb carries its queue's ring, so each queue's legs submit and resume only on that queue's reap thread while
// the protocol state (dLSN assignment, quorum, routing) is shared. Every write must land, the commit frontier
// must reach the full cross-queue count (dLSNs stay dense under concurrent reserve), and every block must read
// back its bytes on the queue that wrote it.
TEST(CraftAsyncMem, MultiQueueWriteReadRoundTrip) {
    constexpr int Q = 3; // queues (threads x rings)
    constexpr int N = 8; // writes per queue
    volume_id_t const vid = boost::uuids::random_generator()();

    auto set = craft::make_mem_replica_group(vid, 3, PAGE);
    std::vector< std::shared_ptr< craft_replica > > backends(set.replicas.begin(), set.replicas.end());
    auto client = craft::make_client(backends);
    ASSERT_TRUE(rg(craft::login(client, TOKEN)).has_value()); // pool path, BEFORE any queue IO (the ordering contract)

    // A fast pair + one straggler, so quorum forms without the slow leg on every queue at once.
    set.replicas[0]->set_delay(std::chrono::milliseconds{1});
    set.replicas[1]->set_delay(std::chrono::milliseconds{1});
    set.replicas[2]->set_delay(std::chrono::milliseconds{5});

    std::barrier<> start{Q};
    std::atomic< int > failures{0};
    std::vector< std::thread > queues;
    queues.reserve(Q);
    for (int qi = 0; qi < Q; ++qi) {
        queues.emplace_back(run_queue, client, qi, N, std::ref(start), std::ref(failures));
    }
    for (auto& t : queues)
        t.join(); // every queue drained + exited its ring; only now may the client/cluster go

    EXPECT_EQ(failures.load(), 0) << "every queue must complete all its writes and verified reads";
    EXPECT_EQ(craft::commit_lsn(client), int64_t{Q * N - 1}) << "dLSNs stay dense across concurrently-reserving queues";
    // The stragglers were drained per queue, so every replica holds every write.
    for (auto const& r : set.replicas) {
        EXPECT_EQ(journal_slots(r), std::size_t{Q * N});
    }
}

// The two tiers coexist within one session: while a queue thread holds a live ring (its conns open, its writes
// landed), a plain BLOCKING verb from an un-ringed thread still completes on the transport's own path -- the
// degradation contract for setup/teardown/idle-probe threads.
TEST(CraftAsyncMem, BlockingTierCoexistsWithRings) {
    volume_id_t const vid = boost::uuids::random_generator()();
    auto set = craft::make_mem_replica_group(vid, 3, PAGE);
    std::vector< std::shared_ptr< craft_replica > > backends(set.replicas.begin(), set.replicas.end());
    auto client = craft::make_client(backends);
    ASSERT_TRUE(rg(craft::login(client, TOKEN)).has_value());

    std::atomic< bool > ring_done{false}, blocking_done{false};
    std::thread queue{[&] {
        ring_driver driver{128};
        auto buf = page_of(0x5A);
        auto done = std::make_shared< std::atomic< int > >(0);
        auto ok = std::make_shared< std::atomic< int > >(0);
        issue_write(client, &driver.ring, blk(0), PAGE, one_iov(buf), done, ok).detach();
        EXPECT_TRUE(driver.drive_until([&] { return done->load() == 1; }));
        EXPECT_EQ(ok->load(), 1);
        ring_done.store(true, std::memory_order_release);
        // Park with the ring alive (still pumping) until the blocking write on the main thread completes.
        EXPECT_TRUE(driver.drive_until([&] { return blocking_done.load(std::memory_order_acquire); }, 8000));
        driver.drain_for(std::chrono::milliseconds{40});
    }};

    while (!ring_done.load(std::memory_order_acquire))
        std::this_thread::yield();
    // Main thread never touched a ring: the null-q verbs ride the MemTransport pool and sync_get blocks safely.
    auto wbuf = page_of(0xA5);
    EXPECT_TRUE(rg(craft::write(client, blk(1), PAGE, one_iov(wbuf))).has_value());
    EXPECT_TRUE(rg(craft::flush(client)).has_value());
    std::vector< uint8_t > dest(PAGE, 0);
    EXPECT_TRUE(rg(craft::read(client, blk(1), PAGE, one_iov(dest))).has_value());
    EXPECT_EQ(dest, wbuf) << "the blocking tier round-trips while a queue's ring is live";
    blocking_done.store(true, std::memory_order_release);
    queue.join();
}
