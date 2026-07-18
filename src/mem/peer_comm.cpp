#include <sisl/logging/logging.h>
#include "peer_comm.hpp"
#include "raft_state_mgr.hpp"

#include <boost/uuid/random_generator.hpp>
#include <libnuraft/raft_params.hxx>
#include <utility>

namespace craft {

std::shared_ptr< peer_comm > peer_comm::instance() {
    static std::shared_ptr< peer_comm > instance{new peer_comm()};
    return instance;
}

consensus_handle peer_comm::get_consensus() { return consensus_; }

void peer_comm::init_raft_server(boost::uuids::uuid const& server_uuid, uint16_t port) {
    auto params = nuraft_mesg::manager::params{
        .server_uuid_ = server_uuid,
        .mesg_port_ = port,
        .default_group_type_ = default_group_type_,
    };
    consensus_ = nuraft_mesg::init_messaging(params, weak_from_this(), true);
    auto raft_params = nuraft::raft_params{};
    consensus_->register_mgr_type(default_group_type_, raft_params);
    LOGINFO("Initialized peer_comm for {} with raft consensus manager, port {}", params.server_uuid_, params.mesg_port_);
}

std::string peer_comm::lookup_peer(nuraft_mesg::peer_id_t const& peer_id) {
    auto it = peer_lookup_map_.find(peer_id);
    if (it == peer_lookup_map_.end()) {
        LOGWARN("Peer {} not found in lookup map", boost::uuids::to_string(peer_id));
        return {};
    }
    return it->second;
}

std::shared_ptr< nuraft_mesg::mesg_state_mgr > peer_comm::create_state_mgr(int32_t const srv_id,
                                                                            nuraft_mesg::group_id_t const& group_id) {
    LOGINFO("Creating raft state manager for server_id={} group_id={}", srv_id, boost::uuids::to_string(group_id));
    return std::make_shared< raft_state_mgr >(srv_id, group_id, consensus_);
}

} // namespace craft
