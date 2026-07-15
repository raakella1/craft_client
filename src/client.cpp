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
#include <craft/client.hpp>  // the opaque handle + the free-function declarations
#include "craft_replica.hpp" // make_client

#include <chrono>
#include <utility>
#include <vector>

#include <sisl/async/when_all.hpp>
#include <sisl/async/when_quorum.hpp>
#include <sisl/fds/buffer.hpp> // sisl::sg_iterator

#include "client_impl.hpp" // the concrete craft_client (internal)

namespace craft {

using sisl::ok;
template < typename T >
using result = sisl::result< T >;

// How far the router's overlay may lag the frontier before a write trims it. Mirrors dlsn_tracker's
// k_trunc_batch: both structures are per-dLSN StreamTrackers, so both must be trimmed by whatever advances the
// frontier -- which is the WRITE path. read() still folds exactly (fold_to(F), unbatched), so a read never sees
// a stale horizon; this only bounds what a write leaves behind for it.
constexpr int64_t k_fold_batch = 512;

// Admission deadline. After the session is established the client HELOs every member (a ringless keep_alive,
// which stands the session leg up and returns that member's commit_lsn) and admits for I/O only once a quorum
// has acked AND at least one member holds data through the login frontier L. A member below L may resync into
// range, so the client re-probes until this deadline, then fails admission -- it never opens I/O it cannot serve.
constexpr auto k_admit_timeout = std::chrono::seconds{5};

void craft_client::maybe_fold() {
    int64_t const F = tracker_->frontier();
    if (F - route_->folded() >= k_fold_batch) route_->fold_to(F);
}

client_hdr craft_client::make_hdr() const {
    // Every IO piggybacks the commit frontier (CRAFT has no standalone commit verb) and the set-wide reclaim
    // floor -- min commit_lsn across members, which the broadcast keep_alive maintains (the login baseline
    // until the first sweep). A replica reclaims journal below min(all_committed_lsn, its own apply frontier).
    return client_hdr{term_, tracker_->frontier(), route_->all_committed()};
}

// Fail fast rather than burn a dLSN on an IO the replicas will reject anyway. They enforce alignment too.
std::optional< std::error_condition > craft_client::precheck(uint64_t addr, uint64_t len) const {
    if (term_ == 0) return make_error_condition(std::errc::not_connected);
    bool const aligned = (lba_size_ != 0) && (len != 0) && ((addr % lba_size_) == 0) && ((len % lba_size_) == 0);
    if (!aligned) return make_error_condition(std::errc::invalid_argument);
    return std::nullopt;
}

async_status craft_client::login(uint64_t client_token) {
    std::size_t const n = replicas_.size();
    std::error_condition last_err{};

    // A follower does not fail the call: it returns term == 0 plus leader_hint. We have no id -> handle map
    // (LoginResult::members is empty on a redirect), so walk the backends. A hop that errors is skipped, not
    // fatal -- a down replica must not block login.
    for (std::size_t hop = 0; hop < n; ++hop) {
        auto const target = static_cast< uint32_t >((leader_ + hop) % n);
        auto lr = co_await replicas_[target]->login(client_token);
        if (!lr.has_value()) {
            last_err = lr.error();
            continue;
        }
        if (lr->term == 0) continue; // NOT_LEADER redirect

        leader_ = target;
        term_ = lr->term;
        lba_size_ = lr->lba_size;
        capacity_ = lr->capacity;
        // max_tx is the volume's max DATA transfer (like iSCSI's 512 KiB payload, header excluded). A driver caps
        // its device IO to it directly -- a write of max_tx is pure data (no framing). The read-reply framing
        // (extent table on top of the data) lives in the PARSE bound instead (parse_message allows body_len a
        // margin over max_tx), so the payload stays the clean number.
        max_tx_ = lr->max_tx;
        tracker_->reset_at(lr->dLSN, lr->lba_size);
        // Install a FRESH router for this session (do NOT reset in place): a detached straggler from a prior
        // session still holds a shared_ptr to the old map (the when_quorum hook captured a copy) and may run
        // record_completion after we return -- reconstructing the live map under it would race. The old map is
        // orphaned and freed once its last straggler finishes.
        route_ = std::make_shared< read_route_map >();
        route_->reset(replicas_.size(), lr->dLSN);
        // Establish + probe every member and admit for I/O only once the cluster can actually serve it. There is
        // NO data-path leader: login may have been brokered by any member (a follower forwards to the RAFT leader
        // and answers), and the RAFT leader may be a skinny consensus node holding no data -- so no member is
        // trusted on faith. A ringless keep_alive HELOs the member's session leg and returns its own commit_lsn;
        // we seed that member's read-eligibility baseline (Synced) from it. Admit once a QUORUM has acked AND at
        // least one member holds data through the login frontier L (commit_lsn >= L), so every committed read is
        // servable. A member below L may resync into range, so re-probe until the admit deadline, then fail --
        // never open I/O we cannot serve. reset() seeded the floor; a member proves itself here or stays out.
        int64_t const L = lr->dLSN;
        auto const admit_deadline = std::chrono::steady_clock::now() + k_admit_timeout;
        for (;;) {
            client_hdr const probe_hdr = make_hdr();
            std::vector< async_result< lsn_pair > > probes;
            probes.reserve(replicas_.size());
            for (auto& h : replicas_)
                probes.push_back(h->keep_alive(nullptr, probe_hdr));
            auto const acks = co_await sisl::async::when_all(std::move(probes));
            std::size_t acked = 0;
            bool holder_at_L = false;
            for (std::size_t i = 0; i < replicas_.size(); ++i) {
                if (!acks[i].has_value()) continue;
                ++acked;
                route_->advance_synced(i, acks[i]->commit_lsn);
                if (acks[i]->commit_lsn >= L) holder_at_L = true;
            }
            if (acked >= quorum() && holder_at_L) co_return ok(); // quorum established + L is servable -> admit
            if (std::chrono::steady_clock::now() >= admit_deadline)
                co_return std::unexpected(make_error_condition(craft_error::NO_QUORUM));
            // else: sub-quorum, or nobody holds L yet. Re-probe -- the round-trip paces the retry, and a member
            // below L may resync into range before the deadline.
        }
    }
    co_return std::unexpected(last_err ? last_err : make_error_condition(craft_error::NOT_LEADER));
}

// One broadcast leg: run the write on `h`, then feed the completion -- and, on an ack, the reply's
// piggybacked commit_lsn -- to the router BEFORE the quorum latch can fire (a leg's body runs before
// when_quorum counts it, the same ordering the completion hook used to give record_completion). Feeding
// EVERY leg matters: the fold needs to know when all legs finished before it decides a member missed the
// write. Exceptions are swallowed as a non-ack so a throwing leg still reports its completion.
static async_result< lsn_pair > write_leg(::io_uring* q, std::shared_ptr< craft_replica > h,
                                          std::shared_ptr< read_route_map > route, client_hdr hdr, int64_t dlsn,
                                          uint64_t addr, uint64_t len, sisl::sg_list data, std::size_t idx) {
    result< lsn_pair > r = std::unexpected(make_error_condition(craft_error::REPLICA_DOWN));
    try {
        r = co_await h->write(q, hdr, dlsn, addr, len, std::move(data));
    } catch (...) {}
    route->record_completion(dlsn, idx, r.has_value());
    if (r.has_value()) route->advance_synced(idx, r->commit_lsn); // any round-trip refreshes the watermark
    co_return r;
}

async_result< size_t > craft_client::write(::io_uring* q, uint64_t addr, uint64_t len, sisl::sg_list data) {
    if (auto const e = precheck(addr, len)) co_return std::unexpected(*e);

    // Reserve the dLSN and record its range BEFORE the broadcast: any replica that can hold this slot implies a
    // concurrent read's scan can already see it, which is what makes the horizon safe. The router slot is
    // created here too, single-writer, so every completion below only updates it.
    int64_t const dlsn = tracker_->reserve(addr, len);
    route_->create(dlsn, addr, len);
    client_hdr const hdr = make_hdr();

    // Ack at quorum, not at the slowest replica: a straggler must not cost every write the transport's timeout.
    // Stragglers keep running detached, so each replica op must have consumed `data` by its first suspension
    // point (see when_quorum) -- the caller may recycle the buffer the moment we return.
    std::size_t acks = 0;
    std::shared_ptr< std::vector< result< lsn_pair > > > results;

    if (replicas_.size() == 1) {
        auto r = co_await write_leg(q, replicas_[0], route_, hdr, dlsn, addr, len, std::move(data), 0);
        acks = r.has_value() ? 1 : 0;
        results = std::make_shared< std::vector< result< lsn_pair > > >(1, std::move(r));
    } else {
        // Each leg gets its own sg_list descriptor copy; all point at the same caller-owned source buffer.
        std::vector< async_result< lsn_pair > > futs;
        futs.reserve(replicas_.size());
        for (std::size_t i = 0; i < replicas_.size(); ++i) {
            futs.push_back(write_leg(q, replicas_[i], route_, hdr, dlsn, addr, len, data, i));
        }
        // `qr`, not `q`: the ring parameter stays nameable in this whole function (the failure branch below
        // forwards it into the resolution round).
        auto qr = co_await sisl::async::when_quorum(std::move(futs), quorum());
        acks = qr.acks;
        results = std::move(qr.results);
    }

    if (acks >= quorum()) {
        // Quorum-durable. Do NOT read `results`: children we stopped waiting for may still be writing it.
        tracker_->resolve(dlsn, slot_outcome::acked);
        maybe_fold(); // trim the overlay here, not only on the read path
        co_return len;
    }

    // Sub-quorum. The latch cannot have fired on the quorum trigger, so every child has finished and the
    // per-replica errors are safe to read. A DETERMINISTIC rejection means the peer decided the request and
    // provably did not journal it (invalid_argument: refused; REPLICA_DOWN: never delivered). If EVERY replica
    // rejected deterministically, nobody holds the slot: Empty, a resolved no-op. A subset proves nothing, so
    // the slot stays unresolved and commit correctly stalls. A timeout must NEVER count (the peer may have
    // appended while the reply outran the deadline); STALE_TERM must not either (a successor may hold the slot).
    static constexpr auto deterministic_reject = [](std::error_condition const& e) {
        return (e == std::make_error_condition(std::errc::invalid_argument)) ||
            (e == make_error_condition(craft_error::REPLICA_DOWN));
    };

    std::size_t refused = 0;
    std::error_condition last_err{};
    for (auto const& r : *results) {
        if (r.has_value()) continue;
        last_err = r.error();
        if (deterministic_reject(r.error())) ++refused;
    }

    bool const provably_empty = (refused == replicas_.size());
    tracker_->resolve(dlsn, provably_empty ? slot_outcome::empty : slot_outcome::failed);
    maybe_fold();
    // A failed (sub-quorum, not provably-absent) slot pins the frontier until the leader fills or Empties it:
    // request the resolution round NOW (the design's client-request SyncRSCommitLSN trigger) instead of
    // waiting for a watchdog / periodic cadence that the client cannot see.
    if (!provably_empty) request_resolution_round(q, dlsn);
    co_return std::unexpected(last_err ? last_err : make_error_condition(craft_error::NO_QUORUM));
}

// One detached resolution leg to member `idx` -- fire-and-forget, like fire_keepalive: it captures the
// tracker/route shared_ptrs and the backend by value, never the client. It drains the want watermark: a burst
// of failed writes collapses into this one outstanding request per peer, and a want that lands while the
// leader is resolving is picked up by the next loop pass. A member that is NOT the leader answers NOT_LEADER
// and the leg simply ends, LEAVING the want set -- only the leader's leg (whichever member that currently is)
// resolves and consumes it. On success the covered slots are retired off the verdicts: Empty ones as Empty,
// filled ones as acked with the router told first (note_filled) so reads route only to certain holders until
// the commit_lsn certificates catch up. A leg that errors (member down, deposed term) also just ends: the
// slot stays pinned and the next failed write re-fires the legs.
static async_status fire_resolution(::io_uring* q, std::shared_ptr< craft_replica > h, std::size_t idx, uint64_t term,
                                    std::shared_ptr< dlsn_tracker > tracker, std::shared_ptr< read_route_map > route) {
    for (;;) {
        while (auto const want = tracker->resolution_want()) {
            client_hdr const hdr{term, tracker->frontier(), route->all_committed()};
            auto r = co_await h->request_resolution(q, hdr, *want);
            if (!r.has_value()) { // NOT_LEADER / down / stale: this peer cannot resolve; leave the want alone
                route->end_resolution(idx);
                co_return ok();
            }
            tracker->retire_upto(r->resolved_upto, r->empty_slots, [&](int64_t d) { route->note_filled(d, idx); });
            tracker->clear_resolution_want(r->resolved_upto);
        }
        route->end_resolution(idx);
        // Close the note-then-begin race: a want recorded between our last peek and the flag release would
        // strand until the next failure. Reclaim this peer's flag only if a want is actually pending.
        if (!tracker->resolution_want() || !route->try_begin_resolution(idx)) co_return ok();
    }
}

void craft_client::request_resolution_round(::io_uring* q, int64_t upto) {
    tracker_->note_resolution_want(upto);
    // BROADCAST, not a leader walk: the client learned the leader at login and leadership may have moved
    // since -- mid-session it cannot know who leads. Every member gets the request, at most one outstanding
    // per peer (the keep_alive collapse); whichever member IS the leader runs the round, the rest answer
    // NOT_LEADER (a real replica may instead forward to its leader -- either way the client need not know).
    // Each leg rides the failing write's ring, so its resumptions stay on that queue's reap thread.
    for (std::size_t m = 0; m < replicas_.size(); ++m) {
        if (route_->try_begin_resolution(m)) fire_resolution(q, replicas_[m], m, term_, tracker_, route_).detach();
    }
}

// Issue a whole read plan to one target, filling `dest` in place; resolves to the target's piggybacked
// watermarks (the highest across a split's segments -- same member, monotonic). Factored out of read() so the
// router can retry it against the next eligible member on a transport failure. `dest` is copied per attempt
// (descriptors only; both point at the caller's buffer), so a failover re-fills the same buffer.
async_result< lsn_pair > craft_client::issue_plan(::io_uring* q, std::shared_ptr< craft_replica > const& target,
                                                  client_hdr hdr, read_plan const& plan, uint64_t addr, uint64_t len,
                                                  sisl::sg_list& dest) {
    if (plan.size() == 1) {
        sisl::sg_list whole = dest;
        auto r = co_await target->read(q, hdr, plan.front().H, addr, len, std::move(whole));
        if (!r.has_value()) co_return std::unexpected(r.error());
        co_return r->lsns;
    }

    // Split read: sg_iterator walks `dest` once, in order, carving one descriptor per segment. Each is passed
    // by value into the callee's coroutine frame, so no descriptor of ours outlives this loop.
    sisl::sg_iterator slicer{dest.iovs};
    std::vector< async_result< read_result > > futs;
    futs.reserve(plan.size());
    for (auto const& seg : plan) {
        sisl::sg_list sub;
        sub.size = seg.len;
        sub.iovs = slicer.next_iovs(static_cast< uint32_t >(seg.len));
        futs.push_back(target->read(q, hdr, seg.H, seg.addr, seg.len, std::move(sub)));
    }
    lsn_pair lsns{-1, -1};
    // co_await hoisted out of the range-for initializer: GCC 16.1 ICEs (get_callee_fndecl) on the combined form.
    auto const results = co_await sisl::async::when_all(std::move(futs));
    for (auto const& r : results) {
        if (!r.has_value()) co_return std::unexpected(r.error());
        lsns.commit_lsn = std::max(lsns.commit_lsn, r->lsns.commit_lsn);
        lsns.last_append_lsn = std::max(lsns.last_append_lsn, r->lsns.last_append_lsn);
    }
    co_return lsns;
}

async_result< size_t > craft_client::read(::io_uring* q, uint64_t addr, uint64_t len, sisl::sg_list dest) {
    if (auto const e = precheck(addr, len)) co_return std::unexpected(*e);

    auto const plan = co_await tracker_->plan_read(addr, len);
    if (!plan.has_value()) co_return std::unexpected(plan.error());

    client_hdr const hdr = make_hdr();

    // Route to a member that actually holds the winner for this range, not blindly to the leader. Fold the
    // router up to the current frontier, then take the highest horizon across segments: a member eligible at
    // Hmax over the whole range is eligible for every segment at its own (<=) horizon.
    int64_t const F = tracker_->frontier();
    route_->fold_to(F);
    int64_t Hmax = F;
    for (auto const& seg : *plan)
        Hmax = std::max(Hmax, seg.H);

    // Search eligible members from a ROTATING start (not always the leader): a CRAFT read has no leader
    // affinity, so starting every read at the leader would pile all read load on it. The rotor advances to the
    // peer after the first eligible one we pick, so successive reads round-robin. thread_local (like ublkpp's
    // RAID1): each ublk queue thread rotates its own reads independently. On a transport-level failure (holder
    // down or timed out) we fail over to the next eligible one; any other error is the client's to surface.
    // `dest` is filled in place, so re-issuing re-fills the same buffer.
    std::size_t const n = replicas_.size();
    static thread_local std::size_t read_rotor = 0;
    std::size_t const start = read_rotor % n;
    bool any_eligible = false;
    std::error_condition last_err{};
    for (std::size_t hop = 0; hop < n; ++hop) {
        std::size_t const idx = (start + hop) % n;
        if (!route_->eligible(idx, addr, len, Hmax)) continue;
        if (!any_eligible) read_rotor = idx + 1; // next read on this queue thread starts at the following peer
        any_eligible = true;

        auto r = co_await issue_plan(q, replicas_[idx], hdr, *plan, addr, len, dest);
        if (r.has_value()) {
            // The reply piggybacked the serving member's watermarks: feed them to the router (this is what
            // makes "the leg we served is fresh" literally true), then top up every leg this read did NOT
            // touch with a keep_alive (one outstanding per leg) so their sessions and watermarks refresh too.
            route_->advance_synced(idx, r->commit_lsn);
            drive_keepalives(q, idx);
            co_return len;
        }

        last_err = r.error();
        bool const transport_failure = (last_err == make_error_condition(craft_error::REPLICA_DOWN)) ||
            (last_err == std::make_error_condition(std::errc::timed_out));
        if (!transport_failure) co_return std::unexpected(last_err);
        // else: this holder is unreachable right now; try the next eligible member.
    }

    // No eligible member served it. NOT_ELIGIBLE if none was eligible at all (the winner's holders are all
    // behind/excluded); NO_QUORUM if holders existed but every one failed the transport.
    co_return std::unexpected(any_eligible ? make_error_condition(craft_error::NO_QUORUM)
                                           : make_error_condition(craft_error::NOT_ELIGIBLE));
}

// A detached keep_alive to one leg: fetch its commit/append and reset its session watchdog, feed the reply to
// the router (advances synced_ -> the reclaim floor + recovery), then release the leg's one-outstanding flag.
// Fire-and-forget, so it captures the map by shared_ptr (it may outlive the client) and the backend by value.
static async_status fire_keepalive(::io_uring* q, std::shared_ptr< craft_replica > h, client_hdr hdr,
                                   std::shared_ptr< read_route_map > route, std::size_t idx) {
    auto r = co_await h->keep_alive(q, hdr);
    if (r.has_value()) route->advance_synced(idx, r->commit_lsn);
    route->end_keepalive(idx);
    co_return ok();
}

void craft_client::drive_keepalives(::io_uring* q, std::size_t exclude_idx) {
    if (term_ == 0) return; // no session yet: nothing to keep alive
    client_hdr const hdr = make_hdr();
    for (std::size_t m = 0; m < replicas_.size(); ++m) {
        if (m == exclude_idx) continue;
        // One outstanding keep_alive per leg is the whole collapse: a busy read stream tops a leg up again only
        // once its previous keep_alive has completed, never one-per-read. The per-leg flag is process-wide, not
        // per queue -- liveness needs ONE keep_alive per member -- so under blk-mq whichever queue thread wins
        // the flag fires the leg, which then rides (and resumes on) THAT queue's ring; everything the detached
        // leg touches (advance_synced, the flag itself) is atomic/lock-guarded in the router.
        if (route_->try_begin_keepalive(m)) fire_keepalive(q, replicas_[m], hdr, route_, m).detach();
    }
}

async_status craft_client::flush(::io_uring* q) {
    // keep_alive is CRAFT's commit carrier AND how the client learns each member's achieved commit_lsn.
    // Broadcast it to EVERY replica: feed each reply to the router, which advances that member's synced_ -> the
    // set-wide all_committed reclaim floor (min across members) and clears Missing entries the member applied. A
    // down/errored member is skipped: its synced_ stays put, correctly pinning the floor. Succeeds if any answered.
    std::size_t const n = replicas_.size();
    client_hdr const hdr = make_hdr();
    std::vector< async_result< lsn_pair > > futs;
    futs.reserve(n);
    for (auto& h : replicas_)
        futs.push_back(h->keep_alive(q, hdr));

    auto const results = co_await sisl::async::when_all(std::move(futs));
    bool any = false;
    std::error_condition last_err{};
    for (std::size_t i = 0; i < n; ++i) {
        if (results[i].has_value()) {
            any = true;
            route_->advance_synced(i, results[i]->commit_lsn);
        } else {
            last_err = results[i].error();
        }
    }
    if (!any) co_return std::unexpected(last_err ? last_err : make_error_condition(craft_error::NO_QUORUM));
    co_return ok();
}

async_status craft_client::logout() {
    // Explicit session teardown: the leader commits an InternalLogout that clears client_token/term on all
    // replicas, so any later IO from this (now stale) client is fenced with STALE_TERM. Term-fenced and
    // best-effort; follow NOT_LEADER redirects like login. On success -- or on STALE_TERM, meaning we are
    // already deposed -- clear local session state so a subsequent IO trips the not-connected precheck.
    if (term_ == 0) co_return ok(); // no active session to tear down
    std::size_t const n = replicas_.size();
    std::error_condition last_err{};
    for (std::size_t hop = 0; hop < n; ++hop) {
        auto const target = static_cast< uint32_t >((leader_ + hop) % n);
        auto r = co_await replicas_[target]->logout(make_hdr());
        if (r.has_value()) {
            leader_ = target;
            term_ = 0;
            lba_size_ = 0;
            capacity_ = 0;
            max_tx_ = 0;
            co_return ok();
        }
        last_err = r.error();
        if (r.error() == make_error_condition(craft_error::STALE_TERM)) {
            term_ = 0; // already fenced elsewhere; the session is gone
            lba_size_ = 0;
            capacity_ = 0;
            max_tx_ = 0;
            co_return ok();
        }
        // NOT_LEADER (redirect) or REPLICA_DOWN: try the next replica.
    }
    co_return std::unexpected(last_err ? last_err : make_error_condition(craft_error::NOT_LEADER));
}

// ── the public construction seam + the driver-facing free functions ──

client_handle make_client(std::vector< std::shared_ptr< craft_replica > > replicas, uint32_t leader,
                          uint32_t max_inflight) {
    return std::make_shared< craft_client >(std::move(replicas), leader, max_inflight);
}

async_status login(client_handle const& c, uint64_t client_token) { return c->login(client_token); }
async_result< size_t > write(client_handle const& c, uint64_t addr, uint64_t len, sisl::sg_list data) {
    return c->write(nullptr, addr, len, std::move(data));
}
async_result< size_t > write(client_handle const& c, ::io_uring* q, uint64_t addr, uint64_t len, sisl::sg_list data) {
    return c->write(q, addr, len, std::move(data));
}
async_result< size_t > read(client_handle const& c, uint64_t addr, uint64_t len, sisl::sg_list dest) {
    return c->read(nullptr, addr, len, std::move(dest));
}
async_result< size_t > read(client_handle const& c, ::io_uring* q, uint64_t addr, uint64_t len, sisl::sg_list dest) {
    return c->read(q, addr, len, std::move(dest));
}
async_status flush(client_handle const& c, ::io_uring* q) { return c->flush(q); }
async_status logout(client_handle const& c) { return c->logout(); }
void drive_keepalives(client_handle const& c, std::size_t exclude_idx) { c->drive_keepalives(nullptr, exclude_idx); }
void drive_keepalives(client_handle const& c, ::io_uring* q, std::size_t exclude_idx) {
    c->drive_keepalives(q, exclude_idx);
}

uint32_t lba_size(client_handle const& c) { return c->lba_size(); }
uint64_t capacity(client_handle const& c) { return c->capacity(); }
uint32_t max_tx(client_handle const& c) { return c->max_tx(); }
uint64_t term(client_handle const& c) { return c->term(); }
int64_t commit_lsn(client_handle const& c) { return c->commit_lsn(); }
int64_t read_horizon(client_handle const& c) { return c->read_horizon(); }
uint64_t winner_scans(client_handle const& c) { return c->winner_scans(); }
int64_t route_folded(client_handle const& c) { return c->route_folded(); }
bool route_caught_up(client_handle const& c, std::size_t idx) { return c->route_caught_up(idx); }
int64_t all_committed_lsn(client_handle const& c) { return c->all_committed_lsn(); }
tracker_stats dlsn_stats(client_handle const& c, std::size_t sample_limit) { return c->dlsn_stats(sample_limit); }
std::size_t replica_count(client_handle const& c) { return c->replica_count(); }
uint32_t leader_index(client_handle const& c) { return c->leader_index(); }

} // namespace craft
