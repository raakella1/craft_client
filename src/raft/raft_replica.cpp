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

#include "raft/raft_replica.hpp"
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

#include <sisl/logging/logging.h>

namespace craft {

namespace {

auto fail(craft_error e) { return std::unexpected(make_error_condition(e)); }

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
class RaftReplica::RaftCommitWorker {
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

RaftReplica::RaftReplica(replica_endpoint ep, uint32_t page_size, uint32_t max_tx) :
        MemCraftReplica{std::move(ep), page_size, nullptr}, max_tx_{max_tx} {
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
    commit_worker_ = std::make_unique< RaftReplica::RaftCommitWorker >();
    LOGDEBUG("RaftReplica constructed [id={}] lba_size={}", boost::uuids::to_string(ep_.id), page_size_);
}

RaftReplica::~RaftReplica() = default;

result< lsn_pair > RaftReplica::do_get_rs_commit_lsn(uint64_t term, bool is_login) {
    // TODO implement quiesce barrier
    std::lock_guard< std::mutex > g{mu_};
    return lsn_pair{state_.commit_lsn, state_.last_append_lsn};
}

result< std::vector< JournalSlot > > RaftReplica::srv_fetch_data(std::vector< int64_t > const& lsns) {
    return do_fetch(lsns);
}

// ── peer comm hooks (driven by raft) ──

MemCraftReplica::MemJournalSlot RaftReplica::to_mem_journal_slot(JournalSlot const& j, uint64_t term) {
    std::shared_ptr< std::vector< uint8_t > > bytes;
    if (j.owned_data) {
        bytes = j.owned_data;
    } else if (j.data.size > 0) {
        bytes = take_payload(j.data);
    }
    return MemCraftReplica::MemJournalSlot{
        .term = term,
        .lba = j.lba,
        .len = j.len,
        .all_zeros = j.all_zeros,
        .is_empty = j.is_empty,
        .bytes = std::move(bytes),
    };
}

std::vector< int64_t > RaftReplica::get_missing_slots(int64_t watermark) {
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
    LOGDEBUG("get_missing_slots [id={}] watermark={} missing_count={}", boost::uuids::to_string(ep_.id), watermark,
             missing.size());
    return missing;
}

std::pair< std::vector< int64_t >, int64_t > RaftReplica::resolve_and_apply(boost::uuids::uuid const& vol_uuid,
                                                                            int64_t watermark, uint64_t client_token,
                                                                            uint64_t term) {
    auto const peers = replica_manager::instance()->get_volume(vol_uuid);
    auto const missing_lsns = get_missing_slots(watermark);
    LOGDEBUG("resolve_and_apply[vol={}] watermark={} missing={} peers={}", boost::uuids::to_string(vol_uuid), watermark,
             missing_lsns.size(), peers.size());
    if (missing_lsns.empty()) { return {{}, -1}; }

    // Brute force, no optimizations for now
    // Step 1: ask every peer for the full missing list, collect ALL responses first.
    std::vector< std::vector< JournalSlot > > all_responses;
    for (auto const& peer : peers) {
        if (peer.id == ep_.id) { continue; }
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

result< void > RaftReplica::sync_rs_commit_lsn(boost::uuids::uuid const& vol_uuid, int64_t rs_commit_lsn,
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

result< LoginResult > RaftReplica::apply_login(std::array< uint8_t, 16 > const& volume_id, uint64_t client_token,
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
                boost::uuids::to_string(ep_.id), term, client_token);
        return LoginResult{.members = {ep_}, .dLSN = state_.last_append_lsn};
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
        if (m.id == ep_.id) { continue; }
        if (auto r = sisl::async::sync_get(m.peer_client->get_rs_commit_lsn(current_term, true /* is_login */)); r) {
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
    return LoginResult{.members = replicas, .dLSN = rs_commit_lsn};
}

// create peer raft group and add members to it.
result< void > RaftReplica::srv_create_volume(std::array< uint8_t, 16 > const& volume_id,
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
void RaftReplica::apply_sync(boost::uuids::uuid const& vol_uuid, SyncRSCommitLSNMsg m) {
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
        if (peer.id == ep_.id) continue; // don't ask self

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

session_info RaftReplica::srv_session_info(std::array< uint8_t, 16 > const&) const {
    std::lock_guard< std::mutex > g{mu_};
    return {state_.term, state_.client_token};
}

void RaftReplica::internal_login(InternalLoginMsg m) {
    cold_apply_login(m.client_token, m.term);
    if (m.rs_commit_lsn >= 0) { cold_truncate_above(m.rs_commit_lsn); }
    {
        std::lock_guard< std::mutex > lk(login_mu_);
        login_done_ = true;
    }
    login_cv_.notify_one();
    LOGINFO("internal_login [id={}]: InternalLogin COMMITTED term={} client_token={} rs_commit_lsn {}",
            boost::uuids::to_string(ep_.id), m.term, m.client_token, m.rs_commit_lsn);
}

} // namespace craft