#pragma once

#include <memory>
#include <mutex>
#include <boost/uuid/uuid.hpp>
#include <stdexec/execution.hpp>
#include <tuple>
#include <nuraft_mesg/nuraft_mesg.hpp>
#include <craft/client.hpp> // result types
#include "raft_state_machine.hpp"

namespace nuraft_mesg {
class manager;
}

using consensus_handle = std::shared_ptr< nuraft_mesg::manager >;
using raft_peer_t = std::pair< std::string, uint16_t >; // <host, raft_port>

namespace craft {

class raft_state_mgr;
class registry_manager;

// Process-wide bridge for the peer-to-peer consensus engine used by the TCP server and replica-side code.
// The concrete nuraft_mesg::manager instance is installed once and then shared by anyone that needs to create
// groups, add members, or issue consensus operations.
class raft_service : public nuraft_mesg::messaging_application, public std::enable_shared_from_this< raft_service > {
public:
    inline static const std::string default_group_type_{"raft_service_raft"};
    inline static const std::string peer_id_key(boost::uuids::uuid const& peer_id) {
        return fmt::format("raft_peer_{}", boost::uuids::to_string(peer_id));
    }
    raft_service(boost::uuids::uuid const& server_uuid, std::shared_ptr< registry_manager > registry_mgr);
    virtual ~raft_service();
    result< void > srv_create_partition(boost::uuids::uuid const& group_id,
                                        std::vector< replica_endpoint > const& members);
    void add_commit_cb(raft_commit_cb_t cb);
    bool is_leader(nuraft_mesg::group_id_t const& group_id);
    nuraft_mesg::peer_id_t leader_id(nuraft_mesg::group_id_t const& group_id);

    // raft append entries
    template < typename MsgT >
    result< void > propose(boost::uuids::uuid const& group_id, MsgT const& payload);

    // messaging_application overrides
    std::string lookup_peer(nuraft_mesg::peer_id_t const&) override;
    std::shared_ptr< nuraft_mesg::mesg_state_mgr > create_state_mgr(int32_t const srv_id,
                                                                    nuraft_mesg::group_id_t const& group_id) override;

private:
    raft_service() = default;
    consensus_handle consensus_;
    nuraft_mesg::peer_id_t server_uuid_;
    std::once_flag raft_started_;
    nlohmann::json server_config_;
    std::shared_mutex mu_;
    std::map< nuraft_mesg::group_id_t, std::shared_ptr< raft_state_mgr > > state_mgrs_;
    raft_commit_cb_t commit_cb_;
    std::shared_ptr< registry_manager > registry_mgr_;

    result< std::shared_ptr< raft_state_mgr > > get_state_mgr(nuraft_mesg::group_id_t const& group_id);
    void add_state_mgr(nuraft_mesg::group_id_t const& group_id, std::shared_ptr< raft_state_mgr > mgr);
};

} // namespace craft
