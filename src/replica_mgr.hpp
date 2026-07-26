#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include <boost/uuid/uuid.hpp>

#include <craft/types.hpp> // peer_id_t
#include "net/tcp_peer.hpp"

namespace craft {

// One member's full identity + reachability, everything replica_manager needs to answer both raft's
// lookup_peer and the wire-plane peer client from a single source of truth.
struct replica_info {
    boost::uuids::uuid id{};
    std::string host;
    uint16_t raft_port{0};
    uint16_t tcp_port{0};
    std::shared_ptr< net::CraftTcpPeer > peer_client{nullptr};
};

// Process-wide registry: peer identity -> reachability, and (lazily) the open peer-plane connection to it.
// Replaces peer_comm::peer_lookup_map_ and net::peer_client_service's addrs_/peers_ split -- one map for
// static info (loaded once, rarely mutated), one for live connections (grown lazily, per actual use).
class replica_manager {
public:
    static std::shared_ptr< replica_manager > instance();

    void start_replica_service(std::string const& path);

    // raft's messaging_application::lookup_peer bridge: peer_id -> "host:raft_port".
    std::string lookup_peer(boost::uuids::uuid const& id) const;
    // wire-plane peer client: lazily connect-and-cache a CraftTcpPeer for this id.
    std::shared_ptr< net::CraftTcpPeer > get_peer_client(boost::uuids::uuid const& id);
    std::optional< replica_info > get(boost::uuids::uuid const& id) const;
    void register_volume(boost::uuids::uuid const& vol_uuid, std::vector< replica_endpoint > const& members);
    std::vector< replica_info > get_volume(boost::uuids::uuid const& volume_id);

private:
    replica_manager() = default;

    mutable std::shared_mutex mu_;
    std::map< boost::uuids::uuid, replica_info > replicas_;                  // static, loaded once
    std::map< boost::uuids::uuid, std::vector< replica_info > > volumes_;
};

} // namespace craft