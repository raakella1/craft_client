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
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "mem/replica.hpp"

namespace craft {

// for srv helo validation
struct session_info {
    uint64_t term;
    uint64_t client_token;
};

struct SyncRSCommitLSNMsg;
struct InternalLoginMsg;

class RaftReplica final : public MemCraftReplica {
public:
    RaftReplica(replica_endpoint ep, uint32_t page_size, uint32_t max_tx, std::string const& replica_config_path);

    ~RaftReplica();

    result< LoginResult > srv_establish(std::array< uint8_t, 16 > const& partition_id, uint64_t client_token,
                                        uint64_t term) {
        return apply_login(partition_id, client_token, term);
    }

    result< void > srv_create_partition(std::array< uint8_t, 16 > const& partition_id,
                                        std::vector< replica_endpoint > const& members);
    result< lsn_pair > srv_get_rs_commit_lsn(uint64_t term, bool is_login) {
        return do_get_rs_commit_lsn(term, is_login);
    }
    result< std::vector< JournalSlot > > srv_fetch_data(std::vector< int64_t > const& lsns);
    session_info srv_session_info(std::array< uint8_t, 16 > const& partition_id) const;

private:
    result< lsn_pair > do_get_rs_commit_lsn(uint64_t term, bool is_login);
    void apply_sync(boost::uuids::uuid const& vol_uuid, SyncRSCommitLSNMsg m);
    result< LoginResult > apply_login(std::array< uint8_t, 16 > const& partition_id, uint64_t client_token,
                                      uint64_t term);
    MemCraftReplica::MemJournalSlot to_mem_journal_slot(JournalSlot const& j, uint64_t term);
    std::vector< int64_t > get_missing_slots(int64_t watermark);
    std::pair< std::vector< int64_t >, int64_t >
    resolve_and_apply(boost::uuids::uuid const& vol_uuid, int64_t watermark, uint64_t client_token, uint64_t term);
    result< void > sync_rs_commit_lsn(boost::uuids::uuid const& vol_uuid, int64_t rs_commit_lsn, uint64_t client_token,
                                      uint64_t term);
    void internal_login(InternalLoginMsg m);
    void raft_init();
    void replica_init(std::string const& replica_config_path);

    uint32_t max_tx_;
    std::atomic< int64_t > rs_commit_lsn_{-1};
    std::mutex login_mu_;
    std::condition_variable login_cv_;
    bool login_done_{false};
    
    class RaftCommitWorker;
    std::unique_ptr< RaftCommitWorker > commit_worker_;
};

} // namespace craft
