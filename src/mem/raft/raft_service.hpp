#pragma once

#include <memory>
#include <mutex>
#include <boost/uuid/uuid.hpp>
#include <stdexec/execution.hpp>
#include <tuple>
#include <nuraft_mesg/nuraft_mesg.hpp>
#include <craft/wire.hpp>
#include <craft/client.hpp> // result types

namespace nuraft_mesg {
class manager;
}

using consensus_handle = std::shared_ptr< nuraft_mesg::manager >;

namespace craft {

// Process-wide bridge for the peer-to-peer consensus engine used by the TCP server and replica-side code.
// The concrete nuraft_mesg::manager instance is installed once and then shared by anyone that needs to create
// groups, add members, or issue consensus operations.
class raft_service : public nuraft_mesg::messaging_application, public std::enable_shared_from_this< raft_service > {
public:
    inline static const std::string default_group_type_{"raft_service_raft"};

    virtual ~raft_service() = default;
    static std::shared_ptr< raft_service > instance();
    consensus_handle get_consensus();
    void start_raft_service(boost::uuids::uuid const& server_uuid);
    result< void > srv_create_volume(std::array< uint8_t, 16 > const& volume_id,
                                     std::vector< wire::member > const& members);

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
};

} // namespace craft
