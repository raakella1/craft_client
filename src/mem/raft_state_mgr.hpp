#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include <libnuraft/state_machine.hxx>
#include <nuraft_mesg/mesg_state_mgr.hpp>

namespace craft {

class raft_state_machine : public nuraft::state_machine {
public:
    raft_state_machine() = default;

    nuraft::ptr< nuraft::buffer > commit(const ulong, nuraft::buffer&) override { return nullptr; }
    bool apply_snapshot(nuraft::snapshot&) override { return true; }
    nuraft::ptr< nuraft::snapshot > last_snapshot() override { return nullptr; }
    ulong last_commit_index() override { return 0; }
    void create_snapshot(nuraft::snapshot&, nuraft::async_result< bool >::handler_type&) override {}
};

class raft_state_mgr : public nuraft_mesg::mesg_state_mgr {
public:
    raft_state_mgr(int32_t server_id, nuraft_mesg::group_id_t group_id, std::weak_ptr< nuraft_mesg::manager > weak_manager) : server_id_{server_id}, group_id_{std::move(group_id)}, weak_manager_{std::move(weak_manager)} {}

    nuraft::ptr< nuraft::cluster_config > load_config() override { return nullptr; }
    void save_config(const nuraft::cluster_config&) override {}
    void save_state(const nuraft::srv_state&) override {}
    nuraft::ptr< nuraft::srv_state > read_state() override { return nullptr; }
    nuraft::ptr< nuraft::log_store > load_log_store() override {
        return nullptr;
    }
    int32_t server_id() override { return server_id_; }
    void system_exit(const int) override {}

    uint32_t get_logstore_id() const override { return 0; }
    std::shared_ptr< nuraft::state_machine > get_state_machine() override {
        return std::make_shared< raft_state_machine >();
    }
    void permanent_destroy() override {}
    void leave() override {}
    
    bool bind_non_raft_service() {
        auto mgr = weak_manager_.lock();
        if (!mgr) {
            return false;
        }
        
        if (!mgr->bind_data_service_request("GetRSCommitLSN", group_id_, [this](boost::intrusive_ptr< sisl::GenericRpcData >& rpc_data) {
                LOGINFO("Received non raft service request for group_id={}", group_id_);
            })) {
            LOGERROR("Failed to bind non raft service request GetRSCommitLSN for group_id={}", group_id_);
            return false;
        }

        if (!mgr->bind_data_service_request("SyncRSCommitLSN", group_id_, [this](boost::intrusive_ptr< sisl::GenericRpcData >& rpc_data) {
                LOGINFO("Received non raft service request for group_id={}", group_id_);
            })) {
            LOGERROR("Failed to bind non raft service request SyncRSCommitLSN for group_id={}", group_id_);
            return false;
        }
        return true;
    }

private:
    int32_t server_id_;
    nuraft_mesg::group_id_t group_id_;
    std::weak_ptr< nuraft_mesg::manager > weak_manager_;
};

} // namespace craft::net
