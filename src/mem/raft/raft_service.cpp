#include <sisl/logging/logging.h>
#include "raft_service.hpp"
#include "raft_state_manager.hpp"
#include "mem/helper.hpp"
#include "replica_mgr.hpp"

#include <libnuraft/raft_params.hxx>
#include <libnuraft/srv_config.hxx>
#include <boost/uuid/string_generator.hpp>
#include <utility>

namespace craft {

namespace {
auto fail(craft_error e) { return std::unexpected(make_error_condition(e)); }
} // namespace

std::shared_ptr< raft_service > raft_service::instance() {
    static std::shared_ptr< raft_service > instance{new raft_service()};
    return instance;
}

consensus_handle raft_service::get_consensus() { return consensus_; }

void raft_service::start_raft_service(boost::uuids::uuid const& server_uuid) {
    std::call_once(raft_started_, [&] {
        auto const my_port = replica_manager::instance()->get(server_uuid)->raft_port;
        auto params = nuraft_mesg::manager::params{
            .server_uuid_ = server_uuid,
            .mesg_port_ = my_port,
            .default_group_type_ = default_group_type_,
        };
        consensus_ = nuraft_mesg::init_messaging(params, weak_from_this(), false /*with_data_svc*/);
        auto raft_params = nuraft::raft_params{};
        consensus_->register_mgr_type(default_group_type_, raft_params);
        server_uuid_ = server_uuid;
        LOGINFO("Initialized raft_service for {} with raft consensus manager, port {}", params.server_uuid_,
                params.mesg_port_);
    });
}

result< void > raft_service::srv_create_volume(std::array< uint8_t, 16 > const& volume_id,
                                               std::vector< wire::member > const& members) {
    auto const group_id = craft::to_uuid(volume_id);
    auto consensus = raft_service::instance()->get_consensus();

    // Seat THIS replica as leader by creating the group.
    if (auto const status = craft::sync_get(consensus->create_group(group_id, raft_service::default_group_type_));
        !status) {
        return fail(craft_error::INTERNAL);
    }

    // Add every OTHER member as a follower.
    for (auto const& m : members) {
        auto const member_id = craft::to_uuid(m.id);
        if (lookup_peer(member_id).empty()) {
            return fail(craft_error::INTERNAL); // sanity: member missing in the server config json
        }
        if (member_id == server_uuid_) continue;

        auto srv_cfg =
            nuraft::srv_config(nuraft_mesg::to_server_id(member_id), 0, boost::uuids::to_string(member_id), "", false);
        if (auto const result = craft::sync_get(consensus->add_member(group_id, srv_cfg)); !result) {
            return fail(craft_error::INTERNAL);
        }
    }
    return {};
}

std::string raft_service::lookup_peer(nuraft_mesg::peer_id_t const& peer_id) {

    auto peer_addr = replica_manager::instance()->lookup_peer(peer_id);
    if (peer_addr.empty()) { LOGWARN("Peer {} not found in lookup map", boost::uuids::to_string(peer_id)); }
    return peer_addr;
}

std::shared_ptr< nuraft_mesg::mesg_state_mgr > raft_service::create_state_mgr(int32_t const srv_id,
                                                                              nuraft_mesg::group_id_t const& group_id) {
    LOGINFO("Creating raft state manager for server_id={} group_id={}, server_uuid={}", srv_id,
            boost::uuids::to_string(group_id), boost::uuids::to_string(server_uuid_));
    return std::make_shared< raft_state_mgr >(srv_id, server_uuid_, group_id);
}

} // namespace craft
