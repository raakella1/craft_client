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

#include "mem/replica.hpp"
#include "mem/cluster.hpp"       // the full MemTransport type
#include "raft/raft_service.hpp" // for raft channel
#include "replica_mgr.hpp"
#include "raft/raft_state_machine.hpp" // for raft message payload types
#include "helper.hpp"
#include "craft/types.hpp"

#include <algorithm>
#include <cstring>
#include <iterator>
#include <system_error>
#include <chrono>
#include <boost/uuid/string_generator.hpp>
#include <queue>

#include <liburing.h>               // the on-ring data path: SQE prep / user_data
#include <sisl/async/cqe_state.hpp> // sisl::async::cqe_awaitable + the managed-user_data contract the reap loop shares
#include <sisl/logging/logging.h>

namespace craft {

namespace {
bool all_zero(uint8_t const* p, std::size_t n) {
    for (std::size_t i = 0; i < n; ++i) {
        if (p[i] != 0) return false;
    }
    return true;
}
auto fail(craft_error e) { return std::unexpected(make_error_condition(e)); }

// Serialize an sg_list into an owned buffer AT ISSUE -- the transport contract that lets the client recycle its
// buffer once every replica leg has started (mirrors MemTransport::take_payload; a zero write carries none).
std::shared_ptr< std::vector< uint8_t > > take_payload(sisl::sg_list const& s) {
    auto b = std::make_shared< std::vector< uint8_t > >();
    b->reserve(s.size);
    for (auto const& io : s.iovs) {
        auto const* p = static_cast< uint8_t const* >(io.iov_base);
        b->insert(b->end(), p, p + io.iov_len);
    }
    return b;
}

constexpr auto LoginWaitTime = std::chrono::seconds(2);

} // namespace

// background worker for raft commit to run replica's business logic
class MemCraftReplica::RaftCommitWorker {
    std::queue< std::move_only_function< void() > > queue_;
    std::mutex mtx_;
    std::condition_variable_any cv_;
    std::jthread worker_;

public:
    RaftCommitWorker() : worker_([this](std::stop_token st) { run(st); }) {}

    void push_task(std::move_only_function< void() > work) {
        {
            std::lock_guard lk(mtx_);
            queue_.push(std::move(work));
        }
        cv_.notify_one();
    }

private:
    void run(std::stop_token st) {
        while (!st.stop_requested()) {
            std::unique_lock lk(mtx_);
            cv_.wait(lk, st, [this] { return !queue_.empty(); }); // wakes on stop too
            if (st.stop_requested() && queue_.empty()) return;

            auto task = std::move(queue_.front());
            queue_.pop();
            lk.unlock();
            if (task) { task(); }
        }
    }
};

void MemCraftReplica::init() {
    // Publish the initial (healthy) fault snapshot before any IO can read it.
    auto initial = std::make_unique< replica_faults const >();
    faults_.store(initial.get(), std::memory_order_release);
    fault_retired_.push_back(std::move(initial));

    // register raft callbacks
    // do not block commit thread, offload the business logic to the commit_worker
    auto raft_inst = raft_service::instance();
    if (!raft_inst->is_raft_enabled()) { return; }
    auto commit_cb = [this](uint64_t log_idx, nlohmann::json const& j, std::string const& vol_uuid_str) {
        auto const vol_uuid = boost::uuids::string_generator()(vol_uuid_str);
        auto const op_val = j.at("op").get< int >();
        switch (static_cast< Operation >(op_val)) {
        case Operation::SyncRSCommitLSN: {
            SyncRSCommitLSNMsg m;
            try {
                m = j.get< SyncRSCommitLSNMsg >();
            } catch (nlohmann::json::exception const& e) {
                LOGERROR("commit[{}]: malformed SyncRSCommitLSN: {}", log_idx, e.what());
                return;
            }
            LOGDEBUG("commit[{}][vol={}]: applying SyncRSCommitLSN rs_commit_lsn={} empty_slots={}", log_idx,
                     boost::uuids::to_string(vol_uuid), m.rs_commit_lsn, m.empty_slots.size());
            commit_worker_->push_task(
                [this, vol_uuid, m = std::move(m)]() mutable { apply_sync(vol_uuid, std::move(m)); });
            break;
        }
        case Operation::InternalLogin: {
            InternalLoginMsg m;
            try {
                m = j.get< InternalLoginMsg >();
            } catch (nlohmann::json::exception const& e) {
                LOGERROR("commit[{}]: malformed InternalLogin: {}", log_idx, e.what());
                return;
            }
            LOGDEBUG("commit[{}][vol={}]: applying InternalLogin term={} client_token={}", log_idx,
                     boost::uuids::to_string(vol_uuid), m.term, m.client_token);
            commit_worker_->push_task([this, vol_uuid, m = std::move(m)]() mutable { internal_login(m); });
            break;
        }
        default:
            LOGERROR("commit[{}]: unknown op={}", log_idx, op_val);
            break;
        }
    };

    auto group_create_cb = [this](boost::uuids::uuid const& group_id) {

    };
    raft_inst->add_commit_cb(std::move(commit_cb));

    // start background commit offload worker
    commit_worker_ = std::make_unique< MemCraftReplica::RaftCommitWorker >();
}

MemCraftReplica::MemCraftReplica(replica_endpoint ep, uint32_t page_size, std::shared_ptr< MemTransport > net) :
        geo_{.lba_size = page_size, .ep = std::move(ep)},
        net_{std::move(net)} {
    init();
    LOGDEBUG("MemCraftReplica constructed [id={}] page_size={}", boost::uuids::to_string(geo_.ep.id), page_size);
}

MemCraftReplica::MemCraftReplica(server_geometry geo) : geo_{std::move(geo)} {
    init();
    LOGDEBUG("MemCraftReplica constructed [id={}] lba_size={} capacity={}", boost::uuids::to_string(geo_.ep.id),
             geo_.lba_size, geo_.capacity);
}

MemCraftReplica::~MemCraftReplica() = default;

// ── fault injection (COW; readers never block, and a reader holding the old snapshot stays valid) ──

template < class Fn >
void MemCraftReplica::mutate_faults(Fn&& fn) {
    std::lock_guard< std::mutex > g{fault_mu_};
    auto next = std::make_unique< replica_faults >(*faults_.load(std::memory_order_relaxed)); // only we mutate
    fn(*next);
    auto const* raw = next.get();
    fault_retired_.push_back(std::move(next));     // keeps every published snapshot alive for life
    faults_.store(raw, std::memory_order_release); // fully built before it is visible
}

void MemCraftReplica::set_up(bool up) {
    LOGINFO("fault injection: set_up({}) [id={}]", up, boost::uuids::to_string(geo_.ep.id));
    mutate_faults([&](replica_faults& s) { s.up = up; });
}
void MemCraftReplica::set_delay(std::chrono::milliseconds d) {
    LOGINFO("fault injection: set_delay({}ms) [id={}]", d.count(), boost::uuids::to_string(geo_.ep.id));
    mutate_faults([&](replica_faults& s) { s.delay = (d.count() > 0) ? d : std::chrono::milliseconds{0}; });
}
void MemCraftReplica::drop_writes(bool drop) {
    LOGINFO("fault injection: drop_writes({}) [id={}]", drop, boost::uuids::to_string(geo_.ep.id));
    mutate_faults([&](replica_faults& s) { s.write_ok = !drop; });
}
void MemCraftReplica::clear_faults() {
    LOGINFO("fault injection: clear_faults [id={}]", boost::uuids::to_string(geo_.ep.id));
    mutate_faults([](replica_faults& s) { s = replica_faults{}; });
}
bool MemCraftReplica::is_up() const { return fault_snapshot()->up; }
bool MemCraftReplica::write_allowed() const { return fault_snapshot()->write_ok; }
std::chrono::milliseconds MemCraftReplica::delay() const { return fault_snapshot()->delay; }

// ── async wrappers: all real work is synchronous, so these complete inline (no reactor) ──

async_result< LoginResult > MemCraftReplica::login(uint64_t client_token) {
    if (!net_) co_return fail(craft_error::NO_QUORUM);
    if (!is_up()) co_return fail(craft_error::REPLICA_DOWN); // our own knob (was net_->is_up(id))
    co_return net_->run_login(this, client_token);
}
async_status MemCraftReplica::logout(client_hdr hdr) {
    if (net_ && !is_up()) co_return fail(craft_error::REPLICA_DOWN);
    {
        std::lock_guard< std::mutex > g{mu_};
        if (hdr.term != state_.term) co_return fail(craft_error::STALE_TERM);
    }
    LOGINFO("logout [id={}] term={}", boost::uuids::to_string(geo_.ep.id), hdr.term);
    co_return net_ ? net_->run_logout(this, hdr.term) : ok();
}
// Two data paths behind one interface, chosen by the verb's leading `q`. Null q: every op crosses the wire
// (MemTransport), which owns the payload, decides deliverability, and imposes latency -- the code below is
// just the server (journal + index) and copies nothing. On-ring (non-null q): the SAME delivery model, but
// this file owns the payload copy + delivery timer and submits it as an SQE on the caller's ring, so N legs
// run in flight at once and complete on the caller's reap thread. The ring rides the leg as a parameter for
// its whole life -- a leg never leaves the queue it was issued on.
async_result< lsn_pair > MemCraftReplica::write(::io_uring* q, client_hdr hdr, int64_t dlsn, uint64_t addr,
                                                uint64_t len, sisl::sg_list data) {
    if (!net_) co_return fail(craft_error::NO_QUORUM); // no wire, nothing to deliver over
    if (!q) co_return co_await net_->send_write(shared_from_this(), hdr, dlsn, addr, len, std::move(data));

    // ── on-ring path: the SAME delivery model as MemTransport::send_write, but the timer is a ring SQE the
    //    driver's reap loop completes -- so N replica legs sit in flight at once (QD>1) on the caller's thread. ──
    auto const* rf = fault_snapshot();
    if (!rf->up || !rf->write_ok) co_return fail(craft_error::REPLICA_DOWN); // never delivered (deterministic reject)
    auto bytes = (data.size == 0) ? nullptr : take_payload(data);            // serialize at issue (transport contract)

    // plan_delivery inlined: a delay past the client deadline abandons THIS leg at op_timeout but still lands the
    // write late (detached) at `delay` -- the arrival that leaves a Missing slot behind at QD>1.
    auto const op_to = net_->op_timeout();
    auto const delay = rf->delay;
    if (delay.count() > 0 && op_to.count() > 0 && delay >= op_to) {
        late_write(q, hdr, dlsn, addr, len, std::move(bytes), delay).detach();
        co_await ring_delay(q, op_to);
        co_return std::unexpected(std::make_error_condition(std::errc::timed_out));
    }
    co_await ring_delay(q, delay); // always suspends (0 delay => one reap cycle), as the wire's reply must
    auto const* rf2 = fault_snapshot();
    if (!rf2->up || !rf2->write_ok) co_return fail(craft_error::REPLICA_DOWN); // went unreachable in flight
    co_return do_write(hdr, dlsn, addr, len, std::move(bytes));
}
async_result< read_result > MemCraftReplica::read(::io_uring* q, client_hdr hdr, int64_t read_lsn, uint64_t addr,
                                                  uint64_t len, sisl::sg_list dest) {
    if (!net_) co_return fail(craft_error::NO_QUORUM);
    if (!q) co_return co_await net_->send_read(shared_from_this(), hdr, read_lsn, addr, len, std::move(dest));

    // On-ring read: no late delivery (a result nobody awaits is worthless); the deadline just caps the wait.
    auto const* rf = fault_snapshot();
    if (!rf->up) co_return fail(craft_error::REPLICA_DOWN);
    auto const op_to = net_->op_timeout();
    auto const delay = rf->delay;
    bool const timed_out = (delay.count() > 0 && op_to.count() > 0 && delay >= op_to);
    co_await ring_delay(q, timed_out ? op_to : delay);
    if (timed_out) co_return std::unexpected(std::make_error_condition(std::errc::timed_out));
    if (!fault_snapshot()->up) co_return fail(craft_error::REPLICA_DOWN);
    co_return do_read(hdr, read_lsn, addr, len, std::move(dest));
}
async_result< lsn_pair > MemCraftReplica::keep_alive(::io_uring* q, client_hdr hdr) {
    if (!net_) co_return fail(craft_error::NO_QUORUM);
    if (!q) co_return co_await net_->send_keep_alive(shared_from_this(), hdr);

    // On-ring keep_alive: like read -- deadline caps the wait, no late delivery.
    auto const* rf = fault_snapshot();
    if (!rf->up) co_return fail(craft_error::REPLICA_DOWN);
    auto const op_to = net_->op_timeout();
    auto const delay = rf->delay;
    bool const timed_out = (delay.count() > 0 && op_to.count() > 0 && delay >= op_to);
    co_await ring_delay(q, timed_out ? op_to : delay);
    if (timed_out) co_return std::unexpected(std::make_error_condition(std::errc::timed_out));
    if (!fault_snapshot()->up) co_return fail(craft_error::REPLICA_DOWN);
    co_return do_keep_alive(hdr);
}

// ── on-ring transport (a verb's non-null `q`) ──

// Suspend the calling leg on a single ring timer. Submission is DEFERRED to the driver's reap loop (which
// batches the burst of legs one QD>1 op fans out into); that loop reaps the CQE and calls complete_cqe_state,
// resuming us. The awaitable is frame-local and non-movable -- its address is the SQE's user_data, so it must
// stay put across the suspend, which a coroutine frame guarantees.
async_status MemCraftReplica::ring_delay(::io_uring* q, std::chrono::milliseconds d) {
    sisl::async::cqe_awaitable ev;
    ::io_uring_sqe* sqe = ::io_uring_get_sqe(q);
    if (nullptr == sqe) { // SQ full: flush the queued legs to make room, then retry once
        (void)::io_uring_submit(q);
        sqe = ::io_uring_get_sqe(q);
    }
    if (nullptr == sqe) co_return ok(); // still none (SQ undersized): degrade to no-delay, never hang
    if (d.count() > 0) {
        __kernel_timespec ts{};
        ts.tv_sec = static_cast< __kernel_time64_t >(d.count() / 1000);
        ts.tv_nsec = static_cast< long long >((d.count() % 1000) * 1'000'000);
        ::io_uring_prep_timeout(sqe, &ts, 0, 0); // relative pure timer; CQE res == -ETIME on fire (we ignore it)
    } else {
        ::io_uring_prep_nop(sqe); // 0 delay still crosses the ring: completes on the next reap pass
    }
    ::io_uring_sqe_set_data64(sqe, sisl::async::encode_managed_user_data(&ev));
    co_await ev;
    co_return ok();
}

// The straggler's detached leg: a write whose delay ran past the client deadline (its caller already gave up
// with timed_out) still lands here, late, after its OWN ring timer. Weak-ref semantics are unnecessary because
// each queue drains every timer on ITS ring -- this one included -- before that ring exits, and the cluster
// outlives the rings, so `this` is live.
async_status MemCraftReplica::late_write(::io_uring* q, client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
                                         std::shared_ptr< std::vector< uint8_t > > bytes,
                                         std::chrono::milliseconds deliver) {
    co_await ring_delay(q, deliver);
    auto const* lf = fault_snapshot();
    if (lf->up && lf->write_ok) (void)do_write(hdr, dlsn, addr, len, std::move(bytes)); // dropped iff down in flight
    co_return ok();
}
async_result< resolution_result > MemCraftReplica::request_resolution(::io_uring* /*q*/, client_hdr hdr, int64_t upto) {
    if (!net_) co_return fail(craft_error::NO_QUORUM); // srv-seam replicas resolve via srv_resolve instead
    if (!is_up()) co_return fail(craft_error::REPLICA_DOWN);
    // Term-fenced like logout: a deposed client must not be able to void the successor's in-flight slots.
    {
        std::lock_guard< std::mutex > g{mu_};
        if (hdr.term != state_.term) co_return fail(craft_error::STALE_TERM);
    }
    LOGDEBUG("request_resolution [id={}] term={} upto={}", boost::uuids::to_string(geo_.ep.id), hdr.term, upto);
    co_return net_->run_resolution(this, hdr.term, upto);
}

async_result< lsn_pair > MemCraftReplica::get_rs_commit_lsn(uint64_t, bool) { co_return do_lsns(); }
async_result< std::vector< JournalSlot > > MemCraftReplica::fetch_data(std::vector< int64_t > lsns) {
    co_return do_fetch(lsns);
}

// ── synchronous cores ──

// Takes the payload ALREADY owned: write() copied it exactly once, at issue. The journal slot adopts that
// buffer, so nothing here copies bytes -- this is the replica persisting what the transport handed it.
// `bytes == nullptr` is a zero write (WRITE_ZEROES), which allocates nothing.
result< lsn_pair > MemCraftReplica::do_write(client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
                                             std::shared_ptr< std::vector< uint8_t > > bytes) {
    // Deliverability is the transport's verdict, not ours: by the time we are called, the request arrived.
    // byte-based API: addr/len must be block-aligned (the model works in page_size blocks internally).
    if (addr % geo_.lba_size != 0 || len % geo_.lba_size != 0 || len == 0) {
        return std::unexpected(std::make_error_condition(std::errc::invalid_argument));
    }
    if (bytes && (bytes->size() != len)) {
        return std::unexpected(std::make_error_condition(std::errc::invalid_argument));
    }
    std::lock_guard< std::mutex > g{mu_};
    if (hdr.term != state_.term) return fail(craft_error::STALE_TERM);
    if (auto it = journal_.find(dlsn); it != journal_.end() && it->second.is_empty) {
        // An Empty verdict is permanent (reconciliation: Empty beats data). A late arrival into the slot is
        // REJECTED -- deterministically -- so that write's own ack path concludes the slot is void, matching
        // the verdict instead of phantom-acking a write every replica discarded.
        return std::unexpected(std::make_error_condition(std::errc::invalid_argument));
    }

    MemJournalSlot slot;
    slot.term = hdr.term;
    slot.lba = addr / geo_.lba_size;                            // byte offset -> block index
    slot.len = static_cast< lba_count_t >(len / geo_.lba_size); // byte length -> block count
    slot.all_zeros = !bytes;                                    // no payload => zero write; no all_zeros flag
    slot.bytes = std::move(bytes);                              // adopt the buffer; do not copy it again
    journal_[dlsn] = std::move(slot);
    state_.last_append_lsn = std::max(state_.last_append_lsn, dlsn);
    apply_up_to(hdr.commit_lsn); // piggybacked commit: advance the frontier best-effort, in dLSN order
    // Piggyback the watermarks on the ack (the wire's write_rsp), so any round-trip refreshes the client.
    return lsn_pair{state_.commit_lsn, state_.last_append_lsn};
}

result< read_result > MemCraftReplica::do_read(client_hdr hdr, int64_t read_lsn, uint64_t addr, uint64_t len,
                                               sisl::sg_list dest) {
    // byte-based API: addr/len block-aligned; dest is a single contiguous buffer covering [addr,addr+len)
    if (addr % geo_.lba_size != 0 || len % geo_.lba_size != 0 || len == 0) {
        return std::unexpected(std::make_error_condition(std::errc::invalid_argument));
    }
    if (dest.size < len) { return std::unexpected(std::make_error_condition(std::errc::invalid_argument)); }
    std::lock_guard< std::mutex > g{mu_};
    if (hdr.term != state_.term) return fail(craft_error::STALE_TERM);
    apply_up_to(hdr.commit_lsn);                           // piggybacked commit: advance the frontier opportunistically
    reads_served_.fetch_add(1, std::memory_order_relaxed); // test observability: witness read routing
    return read_result{read_range(read_lsn, addr, len, dest), lsn_pair{state_.commit_lsn, state_.last_append_lsn}};
}

result< lsn_pair > MemCraftReplica::do_keep_alive(client_hdr hdr) {
    std::lock_guard< std::mutex > g{mu_};
    // Term-fenced: a stale client must NOT reset the liveness watchdog (that would block failover).
    if (hdr.term != state_.term) return fail(craft_error::STALE_TERM);
    keepalives_served_.fetch_add(1, std::memory_order_relaxed); // test observability: the client's liveness drive
    apply_up_to(hdr.commit_lsn);
    // watchdog reset + journal reclaim below min(hdr.all_committed_lsn, commit_lsn) are deferred seams.
    return lsn_pair{state_.commit_lsn, state_.last_append_lsn};
}

result< lsn_pair > MemCraftReplica::do_lsns() {
    if (net_ && !net_->is_up(geo_.ep.id)) return fail(craft_error::REPLICA_DOWN);
    std::lock_guard< std::mutex > g{mu_};
    return lsn_pair{state_.commit_lsn, state_.last_append_lsn};
}

result< lsn_pair > MemCraftReplica::do_get_rs_commit_lsn(uint64_t term, bool is_login) {
    // TODO implement quiesce barrier
    if (net_ && !net_->is_up(geo_.ep.id)) return fail(craft_error::REPLICA_DOWN);
    std::lock_guard< std::mutex > g{mu_};
    return lsn_pair{state_.commit_lsn, state_.last_append_lsn};
}

status MemCraftReplica::do_truncate(int64_t lsn) {
    std::lock_guard< std::mutex > g{mu_};
    LOGDEBUG("do_truncate [id={}] above lsn={} (last_append_lsn was {})", boost::uuids::to_string(geo_.ep.id), lsn,
             state_.last_append_lsn);
    journal_.erase(journal_.upper_bound(lsn), journal_.end());
    state_.last_append_lsn = std::min(state_.last_append_lsn, lsn);
    return ok();
}

result< std::vector< JournalSlot > > MemCraftReplica::do_fetch(std::vector< int64_t > const& lsns) {
    std::lock_guard< std::mutex > g{mu_};
    std::vector< JournalSlot > out;
    for (auto lsn : lsns) {
        auto it = journal_.find(lsn);
        if (it == journal_.end()) continue; // not-present-here => omit
        auto const& s = it->second;
        JournalSlot js;
        js.lsn = lsn;
        js.is_empty = s.is_empty;
        js.all_zeros = s.all_zeros;
        js.lba = s.lba;
        js.len = s.len;
        if (!s.all_zeros && !s.is_empty && s.bytes) {
            // seam: the sg_list points into the slot's owned buffer (valid while the slot lives).
            js.data.size = s.bytes->size();
            js.data.iovs.push_back(iovec{s.bytes->data(), s.bytes->size()});
        }
        out.push_back(std::move(js));
    }
    LOGDEBUG("do_fetch [id={}] requested={} returned={}", boost::uuids::to_string(geo_.ep.id), lsns.size(), out.size());
    return out;
}

// The N=1 resolution round (the srv seam / standalone TCP server): this replica alone is the whole live set,
// so a hole in its own journal IS the quorum-lacks evidence -- every missing slot <= upto is verdicted Empty
// (tombstoned, so a late arrival is rejected) and the frontier advances through them.
result< resolution_result > MemCraftReplica::do_resolve_local(client_hdr hdr, int64_t upto) {
    std::lock_guard< std::mutex > g{mu_};
    if (hdr.term != state_.term) return fail(craft_error::STALE_TERM);
    resolution_result out{upto, {}};
    for (int64_t d = state_.commit_lsn + 1; d <= upto; ++d) {
        auto it = journal_.find(d);
        if (it == journal_.end()) {
            MemJournalSlot s;
            s.term = state_.term;
            s.is_empty = true;
            journal_[d] = std::move(s);
            out.empty_slots.push_back(d);
        } else if (it->second.is_empty) {
            out.empty_slots.push_back(d); // a prior verdict; re-report it so the client can retire the slot
        }
    }
    state_.last_append_lsn = std::max(state_.last_append_lsn, upto);
    apply_up_to(upto);
    LOGDEBUG("do_resolve_local [id={}] upto={} empty_slots={} commit_lsn now {}", boost::uuids::to_string(geo_.ep.id),
             upto, out.empty_slots.size(), state_.commit_lsn);
    return out;
}

// ── apply / read helpers (mu_ held) ──

void MemCraftReplica::apply_slot(int64_t dlsn, MemJournalSlot const& s) {
    if (s.all_zeros) {
        for (lba_count_t i = 0; i < s.len; ++i) {
            index_.erase(s.lba + i); // unmap => hole
        }
    } else {
        for (lba_count_t i = 0; i < s.len; ++i) {
            index_[s.lba + i] = IndexCell{dlsn, s.bytes, static_cast< std::size_t >(i) * geo_.lba_size};
        }
    }
}

void MemCraftReplica::apply_up_to(int64_t target) {
    int64_t next = state_.commit_lsn + 1;
    while (next <= target) {
        auto it = journal_.find(next);
        if (it == journal_.end()) break; // Missing hole -> stall (best-effort)
        if (!it->second.is_empty) apply_slot(next, it->second);
        state_.commit_lsn = next; // Empty slots are skipped on apply but still advance the frontier
        ++next;
    }
}

// Highest-dLSN journal-tail slot with commit_lsn < dLSN <= H that covers `x` (the journal-tail overlay,
// materialized on demand). Slots above H are never examined -- that is the horizon clamp.
MemCraftReplica::MemJournalSlot const* MemCraftReplica::highest_slot_le(lba_t x, int64_t H) const {
    for (auto it = journal_.upper_bound(H); it != journal_.begin();) {
        --it;
        if (it->first <= state_.commit_lsn) break; // reached the applied prefix (served from index_)
        auto const& s = it->second;
        if (s.is_empty) continue;
        if (s.lba <= x && x < s.lba + s.len) return &s;
    }
    return nullptr;
}

std::vector< io_extent > MemCraftReplica::read_range(int64_t H, uint64_t addr, uint64_t len,
                                                     sisl::sg_list const& dest) {
    lba_t const lba0 = addr / geo_.lba_size;
    lba_count_t const nblk = static_cast< lba_count_t >(len / geo_.lba_size);
    std::vector< io_extent > layout;

    // Scatter writer: advances through dest's iovecs sequentially, one geo_.lba_size chunk at a time.
    std::size_t iov_idx{0}, iov_off{0};
    auto sg_write = [&](uint8_t const* src, std::size_t n) {
        while (n > 0 && iov_idx < dest.iovs.size()) {
            auto const& iov = dest.iovs[iov_idx];
            std::size_t avail = iov.iov_len - iov_off;
            std::size_t to_write = std::min(n, avail);
            auto* dst = static_cast< uint8_t* >(iov.iov_base) + iov_off;
            if (src) {
                std::memcpy(dst, src, to_write);
                src += to_write;
            } else {
                std::memset(dst, 0, to_write);
            }
            n -= to_write;
            iov_off += to_write;
            if (iov_off == iov.iov_len) {
                ++iov_idx;
                iov_off = 0;
            }
        }
    };

    for (lba_count_t i = 0; i < nblk; ++i) {
        lba_t const x = lba0 + i;
        // Winner = highest-dLSN version <= H covering block x, between the applied index cell and the
        // journal tail. A tail slot's dLSN is always > commit_lsn >= any applied cell's, so the tail wins.
        uint8_t const* page = nullptr;
        bool hole = true;
        if (auto* s = highest_slot_le(x, H)) {
            if (!s->all_zeros) {
                page = s->bytes->data() + static_cast< std::size_t >(x - s->lba) * geo_.lba_size;
                hole = false;
            } // else: zero write => hole
        } else if (auto it = index_.find(x); it != index_.end()) {
            page = it->second.buf->data() + it->second.off;
            hole = false;
        }
        // read-time scan: an all-zero data page reads back thin (as a hole).
        if (!hole && all_zero(page, geo_.lba_size)) hole = true;

        // fill the caller's scatter-gather buffer: data pages get bytes, holes get zeros.
        sg_write(hole ? nullptr : page, geo_.lba_size);

        // coalesce the returned layout, in BYTES, with the previous extent if contiguous.
        uint64_t const x_addr = static_cast< uint64_t >(x) * geo_.lba_size;
        if (!layout.empty() && layout.back().hole == hole && layout.back().addr + layout.back().len == x_addr) {
            layout.back().len += geo_.lba_size;
        } else {
            layout.push_back(io_extent{x_addr, geo_.lba_size, hole});
        }
    }
    return layout;
}

// ── observability ──

replica_stats MemCraftReplica::stats() const {
    std::lock_guard< std::mutex > g{mu_};

    replica_stats s;
    s.id = geo_.ep.id;
    s.addr = geo_.ep.addr;
    s.page_size = geo_.lba_size;
    s.commit_lsn = state_.commit_lsn;
    s.last_append_lsn = state_.last_append_lsn;
    s.term = state_.term;
    s.client_token = state_.client_token;
    s.mapped_blocks = index_.size();

    s.journal_slots = journal_.size();
    if (!journal_.empty()) {
        s.journal_first_dlsn = journal_.begin()->first;
        s.journal_last_dlsn = journal_.rbegin()->first;
    }
    for (auto const& [dlsn, slot] : journal_) {
        if (slot.is_empty) {
            ++s.empty_slots;
        } else if (slot.all_zeros) {
            ++s.zero_write_slots;
        } else {
            s.journal_data_bytes += static_cast< uint64_t >(slot.len) * geo_.lba_size;
        }
    }

    // Missing = the dLSNs in (commit_lsn, last_append_lsn] with no journal entry. Count them by
    // subtraction rather than by walking the range: the uncommitted tail can be arbitrarily wide, while
    // the slots actually present in it are bounded by the client's in-flight window.
    int64_t const lo = state_.commit_lsn + 1;
    int64_t const hi = state_.last_append_lsn;
    if (hi >= lo) {
        auto const first = journal_.lower_bound(lo);
        auto const last = journal_.upper_bound(hi);
        auto const present = static_cast< std::size_t >(std::distance(first, last));
        s.missing_count = static_cast< std::size_t >(hi - lo + 1) - present;

        // Sample the gaps in one pass over the same range, bounded so a missing tail cannot run long.
        int64_t expect = lo;
        for (auto it = first; it != last && s.missing_sample.size() < k_missing_sample; ++it) {
            for (; expect < it->first && s.missing_sample.size() < k_missing_sample; ++expect) {
                s.missing_sample.push_back(expect);
            }
            expect = it->first + 1;
        }
        for (; expect <= hi && s.missing_sample.size() < k_missing_sample; ++expect) {
            s.missing_sample.push_back(expect);
        }
    }
    return s;
}

// ── cold-path hooks (driven by MemTransport, which does NOT hold its own lock while calling these) ──

lsn_pair MemCraftReplica::peek_lsns() {
    std::lock_guard< std::mutex > g{mu_};
    return lsn_pair{state_.commit_lsn, state_.last_append_lsn};
}
void MemCraftReplica::cold_apply_sync(int64_t rs_commit_lsn, uint64_t /*client_token*/) {
    std::lock_guard< std::mutex > g{mu_};
    // SyncRSCommitLSN: (seam) fetch any missing non-Empty slots <= rs from peers, then advance commit.
    apply_up_to(rs_commit_lsn);
}
void MemCraftReplica::cold_apply_login(uint64_t client_token, uint64_t term) {
    std::lock_guard< std::mutex > g{mu_};
    LOGDEBUG("cold_apply_login [id={}] client_token={} term={} (was term={})", boost::uuids::to_string(geo_.ep.id),
             client_token, term, state_.term);
    state_.client_token = client_token;
    state_.term = term;
}
void MemCraftReplica::cold_apply_logout() {
    std::lock_guard< std::mutex > g{mu_};
    LOGINFO("cold_apply_logout [id={}] clearing term={} client_token={}", boost::uuids::to_string(geo_.ep.id),
            state_.term, state_.client_token);
    state_.client_token = 0;
    state_.term = 0; // no active session; subsequent IOs with old term fail STALE_TERM
}
void MemCraftReplica::cold_truncate_above(int64_t rs_commit_lsn) {
    std::lock_guard< std::mutex > g{mu_};
    LOGDEBUG("cold_truncate_above [id={}] rs_commit_lsn={} (last_append_lsn was {})",
             boost::uuids::to_string(geo_.ep.id), rs_commit_lsn, state_.last_append_lsn);
    journal_.erase(journal_.upper_bound(rs_commit_lsn), journal_.end());
    state_.last_append_lsn = std::min(state_.last_append_lsn, rs_commit_lsn);
}

// ── peer comm hooks (driven by raft) ──

MemCraftReplica::MemJournalSlot MemCraftReplica::to_mem_journal_slot(JournalSlot const& j, uint64_t term) {
    std::shared_ptr< std::vector< uint8_t > > bytes;
    if (j.owned_data) {
        bytes = j.owned_data;
    } else if (j.data.size > 0) {
        bytes = take_payload(j.data);
    }
    return MemJournalSlot{
        .term = term,
        .lba = j.lba,
        .len = j.len,
        .all_zeros = j.all_zeros,
        .is_empty = j.is_empty,
        .bytes = std::move(bytes),
    };
}

std::vector< int64_t > MemCraftReplica::get_missing_slots(int64_t watermark) {
    std::lock_guard< std::mutex > g{mu_};
    std::vector< int64_t > missing;
    int64_t expect = state_.commit_lsn + 1;
    auto it = journal_.lower_bound(expect); // first present entry >= expect
    for (; it != journal_.end() && it->first <= watermark; ++it) {
        for (; expect < it->first; ++expect)
            missing.push_back(expect); // gap before this entry
        expect = it->first + 1;
    }
    for (; expect <= watermark; ++expect)
        missing.push_back(expect); // trailing gap after the last present entry
    LOGDEBUG("get_missing_slots [id={}] watermark={} missing_count={}", boost::uuids::to_string(geo_.ep.id), watermark,
             missing.size());
    return missing;
}

std::pair< std::vector< int64_t >, int64_t > MemCraftReplica::resolve_and_apply(boost::uuids::uuid const& vol_uuid,
                                                                                int64_t watermark,
                                                                                uint64_t client_token, uint64_t term) {
    auto const peers = replica_manager::instance()->get_volume(vol_uuid);
    auto const missing_lsns = get_missing_slots(watermark);
    LOGDEBUG("resolve_and_apply[vol={}] watermark={} missing={} peers={}", boost::uuids::to_string(vol_uuid), watermark,
             missing_lsns.size(), peers.size());
    if (missing_lsns.empty()) { return {{}, -1}; }

    // Brute force, no optimizations for now
    // Step 1: ask every peer for the full missing list, collect ALL responses first.
    std::vector< std::vector< JournalSlot > > all_responses;
    for (auto const& peer : peers) {
        if (peer.id == geo_.ep.id) { continue; }
        if (auto r = sisl::async::sync_get(peer.peer_client->fetch_data(missing_lsns)); r) {
            all_responses.emplace_back(std::move(r.value()));
        } else {
            LOGWARN("resolve_and_apply[vol={}]: fetch_data to peer {} failed/unreachable, error: {}",
                    boost::uuids::to_string(vol_uuid), boost::uuids::to_string(peer.id), r.error().message());
        }
    }

    // Step 2: for each requested lsn, look across every response and decide its fate.
    int64_t stalled_lsn{-1};
    std::vector< int64_t > empty_slots;
    for (auto lsn : missing_lsns) {
        uint32_t lacks_count = 1;
        bool is_data{false};

        for (auto const& resp : all_responses) {
            if (is_data) break;
            auto const it = std::ranges::find_if(resp, [&](auto const& s) { return s.lsn == lsn; });
            if (it == resp.end()) {
                // Omitted from a responding peer's list == "not-present-here": positive
                // lacks-evidence per the FetchData contract.
                ++lacks_count;
            } else if (it->is_empty) {
                // Peer already holds a prior positive Empty verdict: also lacks-evidence.
                ++lacks_count;
            } else {
                is_data = true;
                cold_install_slot(lsn, to_mem_journal_slot(*it, term));
            }
        }

        if (lacks_count >= peers.size() / 2 + 1) {
            empty_slots.push_back(lsn);
        } else if (!is_data) {
            // unresolved lsn
            stalled_lsn = (stalled_lsn == -1) ? lsn : std::min(lsn, stalled_lsn);
        }
    }
    if (stalled_lsn != -1) {
        LOGWARN("resolve_and_apply[vol={}]: could not resolve past lsn={} (quorum-lacks evidence insufficient)",
                boost::uuids::to_string(vol_uuid), stalled_lsn);
    } else {
        LOGDEBUG("resolve_and_apply[vol={}]: fully resolved up to watermark={}, empty_slots={}",
                 boost::uuids::to_string(vol_uuid), watermark, empty_slots.size());
    }
    return {empty_slots, stalled_lsn};
}

result< void > MemCraftReplica::sync_rs_commit_lsn(boost::uuids::uuid const& vol_uuid, int64_t rs_commit_lsn,
                                                   uint64_t client_token, uint64_t term) {
    auto const [empty_slots, stalled_lsn] = resolve_and_apply(vol_uuid, rs_commit_lsn, client_token, term);
    if (stalled_lsn != -1) {
        // leader could not resolve all the missing lsns
        LOGERROR("sync_rs_commit_lsn[vol={}]: leader could not resolve all missing lsns, stalled at {}",
                 boost::uuids::to_string(vol_uuid), stalled_lsn);
        return std::unexpected(make_error_condition(craft_error::INTERNAL));
    }
    if (auto const r = raft_service::instance()->propose(vol_uuid,
                                                         SyncRSCommitLSNMsg{.rs_commit_lsn = rs_commit_lsn,
                                                                            .client_token = client_token,
                                                                            .empty_slots = std::move(empty_slots)});
        !r) {
        // TODO: any cleanup required?
        LOGERROR("sync_rs_commit_lsn[vol={}]: propose(SyncRSCommitLSN={}) failed: {}",
                 boost::uuids::to_string(vol_uuid), rs_commit_lsn, r.error().message());
        return std::unexpected(r.error());
    }
    LOGINFO("sync_rs_commit_lsn[vol={}]: proposed rs_commit_lsn={} OK", boost::uuids::to_string(vol_uuid),
            rs_commit_lsn);
    return {};
}

result< LoginResult > MemCraftReplica::apply_login(std::array< uint8_t, 16 > const& volume_id, uint64_t client_token,
                                                   uint64_t term) {
    auto raft_service_inst = raft_service::instance();
    // A note on dlsn: The login WATERMARK: the last dLSN already durable (-1 on a fresh replica), NOT the next one
    // to use -- the client derives next_dlsn_ = dlsn + 1 itself. This used to send last_append_lsn + 1,
    // which skipped slot 0 on a fresh cluster: every replica was then permanently Missing dLSN 0,
    // apply_up_to() stalled there forever, and commit_lsn pinned at -1 -- so no journal reclaimed and every read
    // walked the whole tail. See wire.hpp.

    if (!raft_service_inst->is_raft_enabled()) {
        // return cold path if raft service has not started
        std::lock_guard< std::mutex > g{mu_};
        state_.term = term;
        state_.client_token = client_token;
        LOGINFO("apply_login [id={}]: raft disabled, cold-path login OK, term={} token={}",
                boost::uuids::to_string(geo_.ep.id), term, client_token);
        return LoginResult{.members = {geo_.ep},
                           .dLSN = state_.last_append_lsn,
                           .term = state_.term,
                           .lba_size = geo_.lba_size,
                           .capacity = geo_.capacity,
                           .max_tx = geo_.max_tx};
    }
    // Phase 1: collect replica LSN state (non-RAFT broadcast)
    // 1.1: accepted by leader only.
    // TODO: what happens if the leader changes before the login is complete?
    auto vol_uuid = craft::to_uuid(volume_id);
    LOGINFO("Login request, vol id {}, token {}, new session {}", boost::uuids::to_string(vol_uuid), client_token,
            term);
    if (!raft_service_inst->is_leader(vol_uuid)) {
        LOGERROR("current replica not a raft leader");
        return LoginResult{{}, -1, 0, 0, 0, raft_service_inst->leader_id(vol_uuid)};
    }

    // 1.2 collect replica LSN state (non-RAFT broadcast)
    std::vector< lsn_pair > peer_resp;
    uint64_t current_term;
    {
        std::lock_guard< std::mutex > g{mu_};
        current_term = state_.term;
        peer_resp.emplace_back(lsn_pair{state_.commit_lsn, state_.last_append_lsn});
    }

    auto const members = replica_manager::instance()->get_volume(vol_uuid);
    LOGDEBUG("apply_login[vol={}]: polling {} member(s) for GetRSCommitLSN", boost::uuids::to_string(vol_uuid),
             members.size());
    for (auto const& m : members) {
        if (m.id == geo_.ep.id) { continue; }
        if (auto r = sisl::async::sync_get(m.peer_client->get_rs_commit_lsn(term, true /* is_login */)); r) {
            LOGDEBUG("apply_login[vol={}]: peer {} reported commit_lsn={} last_append_lsn={}",
                     boost::uuids::to_string(vol_uuid), boost::uuids::to_string(m.id), r->commit_lsn,
                     r->last_append_lsn);
            peer_resp.emplace_back(r.value());
        } else {
            LOGWARN("apply_login[vol={}]: peer {} did not respond to GetRSCommitLSN", boost::uuids::to_string(vol_uuid),
                    boost::uuids::to_string(m.id));
        }
    }
    // compute watermark as max(quorum.last_append)
    if (peer_resp.size() <= members.size() / 2) {
        LOGERROR("apply_login[vol={}]: quorum not reached ({} of {} responded)", boost::uuids::to_string(vol_uuid),
                 peer_resp.size(), members.size());
        return std::unexpected(make_error_condition(craft_error::NO_QUORUM));
    }
    auto const rs_commit_lsn = std::ranges::max_element(peer_resp, {}, &lsn_pair::last_append_lsn)->last_append_lsn;
    LOGINFO("apply_login[vol={}]: computed rs_commit_lsn={} from {} responder(s)", boost::uuids::to_string(vol_uuid),
            rs_commit_lsn, peer_resp.size());

    // Phase 1b: Leader behind - resolve all the missing lsns and
    // Phase 2: SyncRSCommitLSN() via RAFT (data NOT in log)
    if (auto const r = sync_rs_commit_lsn(vol_uuid, rs_commit_lsn, client_token, current_term); !r) {
        LOGERROR("apply_login[vol={}]: sync_rs_commit_lsn failed: {}", boost::uuids::to_string(vol_uuid),
                 r.error().message());
        return std::unexpected(r.error());
    }

    // Phase 3: InternalLogin(token, term) via RAFT
    {
        std::lock_guard< std::mutex > lk(login_mu_);
        login_done_ = false;
    }
    if (auto const r = raft_service_inst->propose(
            vol_uuid, InternalLoginMsg{.client_token = client_token, .term = term, .rs_commit_lsn = rs_commit_lsn});
        !r) {
        // TODO: any cleanup required?
        LOGERROR("apply_login[vol={}]: propose(InternalLogin term={}) failed: {}", boost::uuids::to_string(vol_uuid),
                 term, r.error().message());
        return std::unexpected(r.error());
    }
    LOGDEBUG("apply_login[vol={}]: InternalLogin(term={}) proposed, waiting for commit",
             boost::uuids::to_string(vol_uuid), term);

    // Phase 4: truncate above rs_commit_lsn
    // This happens in the internal login commit. Wait until that happens.
    {
        std::unique_lock< std::mutex > lk(login_mu_);
        login_cv_.wait_for(lk, LoginWaitTime, [&] { return login_done_; });
        if (!login_done_) {
            LOGERROR("apply_login[vol={}]: timed out waiting for InternalLogin(term={}) commit callback",
                     boost::uuids::to_string(vol_uuid), term);
            return std::unexpected(make_error_condition(craft_error::INTERNAL));
        }
    }

    std::vector< replica_endpoint > replicas;
    for (auto const& m : members) {
        replicas.emplace_back(replica_endpoint{.id = m.id, .addr = fmt::format("{}:{}", m.host, m.tcp_port)});
    }
    LOGINFO("apply_login[vol={}]: LOGIN SUCCESS term={} dLSN={} members={}", boost::uuids::to_string(vol_uuid), term,
            rs_commit_lsn, replicas.size());
    return LoginResult{.members = replicas,
                       .dLSN = rs_commit_lsn,
                       .term = term,
                       .lba_size = geo_.lba_size,
                       .capacity = geo_.capacity,
                       .max_tx = geo_.max_tx};
}

// ── resolution-round hooks (driven by MemTransport::run_resolution) ──

std::optional< MemCraftReplica::MemJournalSlot > MemCraftReplica::peek_slot(int64_t dlsn) {
    std::lock_guard< std::mutex > g{mu_};
    auto const it = journal_.find(dlsn);
    if (it == journal_.end()) return std::nullopt;
    return it->second; // copies the slot; `bytes` is shared (immutable once appended), so no payload copy
}

void MemCraftReplica::cold_install_slot(int64_t dlsn, MemJournalSlot s) {
    std::lock_guard< std::mutex > g{mu_};
    if (journal_.contains(dlsn)) return; // already holds it (or a verdict); a fetch never overwrites
    journal_[dlsn] = std::move(s);
    state_.last_append_lsn = std::max(state_.last_append_lsn, dlsn);
}

void MemCraftReplica::cold_mark_empty(int64_t dlsn) {
    std::lock_guard< std::mutex > g{mu_};
    // Overwrites held data on purpose: the verdict says the slot was never quorum-durable, so a sub-quorum
    // copy here was never acked and is discarded (the design's reconciliation: Empty beats held data).
    MemJournalSlot s;
    s.term = state_.term;
    s.is_empty = true;
    journal_[dlsn] = std::move(s);
    state_.last_append_lsn = std::max(state_.last_append_lsn, dlsn);
}

std::vector< int64_t > MemCraftReplica::peek_empties(int64_t upto) {
    std::lock_guard< std::mutex > g{mu_};
    std::vector< int64_t > out;
    for (auto const& [d, s] : journal_) {
        if (d > upto) break;
        if (s.is_empty) out.push_back(d); // ascending: journal_ is an ordered map
    }
    return out;
}

// create peer raft group and add members to it.
result< void > MemCraftReplica::srv_create_volume(std::array< uint8_t, 16 > const& volume_id,
                                                  std::vector< replica_endpoint > const& members) {
    auto const vol_uuid = craft::to_uuid(volume_id);
    auto const& repl_mgr = replica_manager::instance();
    // return success if the volume exists
    if (auto const vol = repl_mgr->get_volume(vol_uuid); !vol.empty()) {
        LOGINFO("Volume {} exists! Returning ok", boost::uuids::to_string(vol_uuid));
        return {};
    }
    LOGINFO("srv_create_volume[vol={}]: creating with {} member(s)", boost::uuids::to_string(vol_uuid), members.size());

    auto const r = raft_service::instance()->srv_create_volume(vol_uuid, members);
    if (r) {
        repl_mgr->register_volume(vol_uuid, members);
        LOGINFO("srv_create_volume[vol={}]: SUCCESS", boost::uuids::to_string(vol_uuid));
    } else {
        LOGERROR("srv_create_volume[vol={}]: FAILED: {}", boost::uuids::to_string(vol_uuid), r.error().message());
    }
    return r;
}

// Follower-side catch-up on SyncRSCommitLSN apply. Verdicts are already decided by the leader (empty_slots)
// -- this never decides Empty itself, only obeys the verdict list or fetches real data.
void MemCraftReplica::apply_sync(boost::uuids::uuid const& vol_uuid, SyncRSCommitLSNMsg m) {
    // Verdicts first -- permanent no-ops, no fetch needed.
    for (auto lsn : m.empty_slots)
        cold_mark_empty(lsn);

    uint64_t term;
    {
        std::lock_guard< std::mutex > g{mu_};
        term = state_.term;
    }

    auto missing = get_missing_slots(m.rs_commit_lsn);
    auto const peers = replica_manager::instance()->get_volume(vol_uuid);
    for (auto const& peer : peers) {
        if (missing.empty()) break;
        if (peer.id == geo_.ep.id) continue; // don't ask self

        auto r = sisl::async::sync_get(peer.peer_client->fetch_data(missing));
        if (!r) continue; // unreachable, try next peer

        std::erase_if(missing, [&](int64_t lsn) {
            auto const it = std::ranges::find_if(*r, [&](auto const& s) { return s.lsn == lsn; });
            if (it == r->end()) return false; // this peer doesn't have it either
            if (it->is_empty) {
                cold_mark_empty(lsn); // a prior verdict this peer already knows about
            } else {
                cold_install_slot(lsn, to_mem_journal_slot(*it, term));
            }
            return true;
        });
    }

    if (!missing.empty()) {
        LOGERROR("apply_sync[vol={}]: still missing {} slot(s) <= {} after asking all peers; commit_lsn will "
                 "stall until the next SyncRSCommitLSN round -- first missing={}",
                 boost::uuids::to_string(vol_uuid), missing.size(), m.rs_commit_lsn, missing.front());
    }

    std::lock_guard< std::mutex > g{mu_};
    apply_up_to(m.rs_commit_lsn);
    rs_commit_lsn_.store(m.rs_commit_lsn, std::memory_order_relaxed);
    LOGDEBUG("apply_sync[vol={}]: done, commit_lsn now {} (target rs_commit_lsn={})", boost::uuids::to_string(vol_uuid),
             state_.commit_lsn, m.rs_commit_lsn);
}

session_info MemCraftReplica::srv_session_info(std::array< uint8_t, 16 > const&) const {
    std::lock_guard< std::mutex > g{mu_};
    return {state_.term, state_.client_token};
}

void MemCraftReplica::internal_login(InternalLoginMsg m) {
    cold_apply_login(m.client_token, m.term);
    if (m.rs_commit_lsn >= 0) { cold_truncate_above(m.rs_commit_lsn); }
    {
        std::lock_guard< std::mutex > lk(login_mu_);
        login_done_ = true;
    }
    login_cv_.notify_one();
    LOGINFO("internal_login [id={}]: InternalLogin COMMITTED term={} client_token={} rs_commit_lsn {}",
            boost::uuids::to_string(geo_.ep.id), m.term, m.client_token, m.rs_commit_lsn);
}

} // namespace craft