#pragma once

#include <memory>
#include <mutex>
#include <boost/uuid/uuid.hpp>
#include <stdexec/execution.hpp>
#include <tuple>
#include <nuraft_mesg/nuraft_mesg.hpp>

namespace nuraft_mesg {
class manager;
}

using consensus_handle = std::shared_ptr< nuraft_mesg::manager >;

namespace craft {

// Process-wide bridge for the peer-to-peer consensus engine used by the TCP server and replica-side code.
// The concrete nuraft_mesg::manager instance is installed once and then shared by anyone that needs to create
// groups, add members, or issue consensus operations.
class peer_comm : public nuraft_mesg::messaging_application,
                  public std::enable_shared_from_this< peer_comm > {
public:
    inline static const std::string default_group_type_{"peer_comm_raft"};

    virtual ~peer_comm() = default;
    static std::shared_ptr< peer_comm > instance();
    consensus_handle get_consensus();
    void init_raft_server(boost::uuids::uuid const& server_uuid, uint16_t port);

    // messaging_application overrides
    std::string lookup_peer(nuraft_mesg::peer_id_t const&) override;
    std::shared_ptr< nuraft_mesg::mesg_state_mgr > create_state_mgr(int32_t const srv_id,
                                                                    nuraft_mesg::group_id_t const& group_id) override;

private:
    peer_comm() = default;
    consensus_handle consensus_;
    std::map< nuraft_mesg::peer_id_t, std::string > peer_lookup_map_;
};

// helper methods

inline static boost::uuids::uuid to_uuid(std::array< uint8_t, 16 > const& arr) {
    boost::uuids::uuid u{};
    std::copy(arr.begin(), arr.end(), u.begin());
    return u;
}

// make sync coro calls, taken from homestore
template < typename Task >
inline auto sync_get(Task&& task) {
    auto result = stdexec::sync_wait(std::forward< Task >(task)).value();
    if constexpr (std::tuple_size_v< decltype(result) > == 0) {
        return;
    } else {
        return std::get< 0 >(std::move(result));
    }
}

} // namespace craft
