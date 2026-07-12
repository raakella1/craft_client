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

#include <craft/mem/replica.hpp>
#include <craft/mem/cluster.hpp> // the full MemTransport type

#include <algorithm>
#include <cstring>
#include <iterator>
#include <system_error>

#include <liburing.h>               // the on-ring data path: SQE prep / user_data
#include <sisl/async/cqe_state.hpp> // sisl::async::cqe_awaitable + the managed-user_data contract the reap loop shares
#include <sisl/async/coro.hpp>      // sisl::async::detach (the straggler's late-delivery leg)

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
} // namespace

MemCraftReplica::MemCraftReplica(replica_endpoint ep, uint32_t page_size, std::shared_ptr< MemTransport > net) :
        ep_{std::move(ep)}, page_size_{page_size}, net_{std::move(net)} {
    // Publish the initial (healthy) fault snapshot before any IO can read it.
    auto initial = std::make_unique< replica_faults const >();
    faults_.store(initial.get(), std::memory_order_release);
    fault_retired_.push_back(std::move(initial));
}

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
    mutate_faults([&](replica_faults& s) { s.up = up; });
}
void MemCraftReplica::set_delay(std::chrono::milliseconds d) {
    mutate_faults([&](replica_faults& s) { s.delay = (d.count() > 0) ? d : std::chrono::milliseconds{0}; });
}
void MemCraftReplica::drop_writes(bool drop) {
    mutate_faults([&](replica_faults& s) { s.write_ok = !drop; });
}
void MemCraftReplica::clear_faults() {
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
    co_return net_ ? net_->run_logout(this, hdr.term) : ok();
}
// Two data paths behind one interface, chosen by whether a ring is bound (prepare_for_async). Legacy (ring_
// null): every op crosses the wire (MemTransport), which owns the payload, decides deliverability, and imposes
// latency -- the code below is just the server (journal + index) and copies nothing. On-ring: the SAME delivery
// model, but this file owns the payload copy + delivery timer and submits it as an SQE on the driver's ring, so
// N legs run in flight at once and complete on the driver's reap thread.
async_status MemCraftReplica::write(client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len, sisl::sg_list data) {
    if (!net_) co_return fail(craft_error::NO_QUORUM); // no wire, nothing to deliver over
    if (!ring_) co_return co_await net_->send_write(shared_from_this(), hdr, dlsn, addr, len, std::move(data));

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
        sisl::async::detach(late_write(hdr, dlsn, addr, len, std::move(bytes), delay));
        co_await ring_delay(op_to);
        co_return std::unexpected(std::make_error_condition(std::errc::timed_out));
    }
    co_await ring_delay(delay); // always suspends (0 delay => one reap cycle), as the wire's reply must
    auto const* rf2 = fault_snapshot();
    if (!rf2->up || !rf2->write_ok) co_return fail(craft_error::REPLICA_DOWN); // went unreachable in flight
    co_return do_write(hdr, dlsn, addr, len, std::move(bytes));
}
async_result< std::vector< io_extent > > MemCraftReplica::read(client_hdr hdr, int64_t read_lsn, uint64_t addr,
                                                               uint64_t len, sisl::sg_list dest) {
    if (!net_) co_return fail(craft_error::NO_QUORUM);
    if (!ring_) co_return co_await net_->send_read(shared_from_this(), hdr, read_lsn, addr, len, std::move(dest));

    // On-ring read: no late delivery (a result nobody awaits is worthless); the deadline just caps the wait.
    auto const* rf = fault_snapshot();
    if (!rf->up) co_return fail(craft_error::REPLICA_DOWN);
    auto const op_to = net_->op_timeout();
    auto const delay = rf->delay;
    bool const timed_out = (delay.count() > 0 && op_to.count() > 0 && delay >= op_to);
    co_await ring_delay(timed_out ? op_to : delay);
    if (timed_out) co_return std::unexpected(std::make_error_condition(std::errc::timed_out));
    if (!fault_snapshot()->up) co_return fail(craft_error::REPLICA_DOWN);
    co_return do_read(hdr, read_lsn, addr, len, std::move(dest));
}
async_result< lsn_pair > MemCraftReplica::keep_alive(client_hdr hdr) {
    if (!net_) co_return fail(craft_error::NO_QUORUM);
    if (!ring_) co_return co_await net_->send_keep_alive(shared_from_this(), hdr);

    // On-ring keep_alive: like read -- deadline caps the wait, no late delivery.
    auto const* rf = fault_snapshot();
    if (!rf->up) co_return fail(craft_error::REPLICA_DOWN);
    auto const op_to = net_->op_timeout();
    auto const delay = rf->delay;
    bool const timed_out = (delay.count() > 0 && op_to.count() > 0 && delay >= op_to);
    co_await ring_delay(timed_out ? op_to : delay);
    if (timed_out) co_return std::unexpected(std::make_error_condition(std::errc::timed_out));
    if (!fault_snapshot()->up) co_return fail(craft_error::REPLICA_DOWN);
    co_return do_keep_alive(hdr);
}

// ── on-ring transport (prepare_for_async) ──

void MemCraftReplica::prepare_for_async(::io_uring* ring) noexcept { ring_ = ring; }

// Suspend the calling leg on a single ring timer. Submission is DEFERRED to the driver's reap loop (which
// batches the burst of legs one QD>1 op fans out into); that loop reaps the CQE and calls complete_cqe_state,
// resuming us. The awaitable is frame-local and non-movable -- its address is the SQE's user_data, so it must
// stay put across the suspend, which a coroutine frame guarantees.
async_status MemCraftReplica::ring_delay(std::chrono::milliseconds d) {
    sisl::async::cqe_awaitable ev;
    ::io_uring_sqe* sqe = ::io_uring_get_sqe(ring_);
    if (nullptr == sqe) { // SQ full: flush the queued legs to make room, then retry once
        (void)::io_uring_submit(ring_);
        sqe = ::io_uring_get_sqe(ring_);
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
// the driver drains every ring timer -- this one included -- before the cluster is torn down, so `this` is live.
async_status MemCraftReplica::late_write(client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
                                         std::shared_ptr< std::vector< uint8_t > > bytes,
                                         std::chrono::milliseconds deliver) {
    co_await ring_delay(deliver);
    auto const* lf = fault_snapshot();
    if (lf->up && lf->write_ok) (void)do_write(hdr, dlsn, addr, len, std::move(bytes)); // dropped iff down in flight
    co_return ok();
}
async_result< lsn_pair > MemCraftReplica::get_lsns() { co_return do_lsns(); }
async_result< lsn_pair > MemCraftReplica::get_rs_commit_lsn() { co_return do_lsns(); }
async_result< std::vector< JournalSlot > > MemCraftReplica::fetch_data(std::vector< int64_t > lsns) {
    co_return do_fetch(lsns);
}
async_status MemCraftReplica::truncate(int64_t lsn) { co_return do_truncate(lsn); }

// ── synchronous cores ──

// Takes the payload ALREADY owned: write() copied it exactly once, at issue. The journal slot adopts that
// buffer, so nothing here copies bytes -- this is the replica persisting what the transport handed it.
// `bytes == nullptr` is a zero write (WRITE_ZEROES), which allocates nothing.
status MemCraftReplica::do_write(client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
                                 std::shared_ptr< std::vector< uint8_t > > bytes) {
    // Deliverability is the transport's verdict, not ours: by the time we are called, the request arrived.
    // byte-based API: addr/len must be block-aligned (the model works in page_size blocks internally).
    if (addr % page_size_ != 0 || len % page_size_ != 0 || len == 0) {
        return std::unexpected(std::make_error_condition(std::errc::invalid_argument));
    }
    if (bytes && (bytes->size() != len)) {
        return std::unexpected(std::make_error_condition(std::errc::invalid_argument));
    }
    std::lock_guard< std::mutex > g{mu_};
    if (hdr.term != state_.term) return fail(craft_error::STALE_TERM);

    MemJournalSlot slot;
    slot.term = hdr.term;
    slot.lba = addr / page_size_;                            // byte offset -> block index
    slot.len = static_cast< lba_count_t >(len / page_size_); // byte length -> block count
    slot.all_zeros = !bytes;                                 // no payload => zero write; no all_zeros flag
    slot.bytes = std::move(bytes);                           // adopt the buffer; do not copy it again
    journal_[dlsn] = std::move(slot);
    state_.last_append_lsn = std::max(state_.last_append_lsn, dlsn);
    apply_up_to(hdr.commit_lsn); // piggybacked commit: advance the frontier best-effort, in dLSN order
    return ok();
}

result< std::vector< io_extent > > MemCraftReplica::do_read(client_hdr hdr, int64_t read_lsn, uint64_t addr,
                                                            uint64_t len, sisl::sg_list dest) {
    // byte-based API: addr/len block-aligned; dest is a single contiguous buffer covering [addr,addr+len)
    if (addr % page_size_ != 0 || len % page_size_ != 0 || len == 0) {
        return std::unexpected(std::make_error_condition(std::errc::invalid_argument));
    }
    if (dest.size < len) { return std::unexpected(std::make_error_condition(std::errc::invalid_argument)); }
    std::lock_guard< std::mutex > g{mu_};
    if (hdr.term != state_.term) return fail(craft_error::STALE_TERM);
    apply_up_to(hdr.commit_lsn);                           // piggybacked commit: advance the frontier opportunistically
    reads_served_.fetch_add(1, std::memory_order_relaxed); // test observability: witness read routing
    return read_range(read_lsn, addr, len, dest);
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
    if (net_ && !net_->is_up(ep_.id)) return fail(craft_error::REPLICA_DOWN);
    std::lock_guard< std::mutex > g{mu_};
    return lsn_pair{state_.commit_lsn, state_.last_append_lsn};
}

status MemCraftReplica::do_truncate(int64_t lsn) {
    std::lock_guard< std::mutex > g{mu_};
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
            index_[s.lba + i] = IndexCell{dlsn, s.bytes, static_cast< std::size_t >(i) * page_size_};
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
    lba_t const lba0 = addr / page_size_;
    lba_count_t const nblk = static_cast< lba_count_t >(len / page_size_);
    std::vector< io_extent > layout;

    // Scatter writer: advances through dest's iovecs sequentially, one page_size_ chunk at a time.
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
                page = s->bytes->data() + static_cast< std::size_t >(x - s->lba) * page_size_;
                hole = false;
            } // else: zero write => hole
        } else if (auto it = index_.find(x); it != index_.end()) {
            page = it->second.buf->data() + it->second.off;
            hole = false;
        }
        // read-time scan: an all-zero data page reads back thin (as a hole).
        if (!hole && all_zero(page, page_size_)) hole = true;

        // fill the caller's scatter-gather buffer: data pages get bytes, holes get zeros.
        sg_write(hole ? nullptr : page, page_size_);

        // coalesce the returned layout, in BYTES, with the previous extent if contiguous.
        uint64_t const x_addr = static_cast< uint64_t >(x) * page_size_;
        if (!layout.empty() && layout.back().hole == hole && layout.back().addr + layout.back().len == x_addr) {
            layout.back().len += page_size_;
        } else {
            layout.push_back(io_extent{x_addr, page_size_, hole});
        }
    }
    return layout;
}

// ── observability ──

replica_stats MemCraftReplica::stats() const {
    std::lock_guard< std::mutex > g{mu_};

    replica_stats s;
    s.id = ep_.id;
    s.addr = ep_.addr;
    s.page_size = page_size_;
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
            s.journal_data_bytes += static_cast< uint64_t >(slot.len) * page_size_;
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
    state_.client_token = client_token;
    state_.term = term;
}
void MemCraftReplica::cold_apply_logout() {
    std::lock_guard< std::mutex > g{mu_};
    state_.client_token = 0;
    state_.term = 0; // no active session; subsequent IOs with old term fail STALE_TERM
}
void MemCraftReplica::cold_truncate_above(int64_t rs_commit_lsn) {
    std::lock_guard< std::mutex > g{mu_};
    journal_.erase(journal_.upper_bound(rs_commit_lsn), journal_.end());
    state_.last_append_lsn = std::min(state_.last_append_lsn, rs_commit_lsn);
}

} // namespace craft
