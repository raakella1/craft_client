#pragma once

#include <nuraft_mesg/mesg_state_mgr.hpp>
#include <sisl/logging/logging.h>
#include "raft_state_machine.hpp"

namespace craft {

class raft_service;
class registry_manager;

class raft_state_mgr : public nuraft_mesg::mesg_state_mgr {
public:
    raft_state_mgr(int32_t srv_id, nuraft_mesg::peer_id_t const& srv_addr, nuraft_mesg::group_id_t const& group_id,
                   raft_commit_cb_t cb, std::shared_ptr< registry_manager > registry_mgr);

    nuraft::ptr< nuraft::cluster_config > load_config() override;
    void save_config(const nuraft::cluster_config& config) override;
    void save_state(const nuraft::srv_state& state) override;
    nuraft::ptr< nuraft::srv_state > read_state() override;
    nuraft::ptr< nuraft::log_store > load_log_store() override;
    int32_t server_id() override { return _srv_id; }

    void system_exit(const int exit_code) override { LOGINFO("System exiting with code [{}]", exit_code); }

    uint32_t get_logstore_id() const override;
    std::shared_ptr< nuraft::state_machine > get_state_machine() override;
    void leave() override;
    void permanent_destroy() override;

private:
    int32_t const _srv_id;
    std::string const _srv_addr;
    std::string const _group_id;
    raft_commit_cb_t _commit_cb;
    std::shared_ptr< registry_manager > _registry_mgr;
};

} // namespace craft
