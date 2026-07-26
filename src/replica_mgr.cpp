#include "replica_mgr.hpp"
#include "helper.hpp"

#include <fstream>

#include <boost/uuid/string_generator.hpp>
#include <nlohmann/json.hpp>
#include <sisl/logging/logging.h>

namespace craft {

std::shared_ptr< replica_manager > replica_manager::instance() {
    static std::shared_ptr< replica_manager > inst{new replica_manager()};
    return inst;
}

void replica_manager::start_replica_service(std::string const& path) {
    std::ifstream istrm(path, std::ios::binary);
    if (!istrm.is_open()) {
        LOGERROR("replica_manager: could not open {}", path);
        return;
    }

    nlohmann::json j;
    try {
        istrm >> j;
    } catch (nlohmann::json::parse_error const& e) {
        LOGERROR("replica_manager: parse error in {}: {}", path, e.what());
        return;
    }

    replicas_.clear();
    for (auto const& m : j.at("members")) {
        replica_info info;
        info.id = boost::uuids::string_generator()(m.at("uuid").get< std::string >());
        info.host = m.at("host").get< std::string >();
        info.raft_port = m.at("raft_port").get< uint16_t >();
        info.tcp_port = m.at("tcp_port").get< uint16_t >();
        replicas_[info.id] = std::move(info);
    }
}

std::string replica_manager::lookup_peer(boost::uuids::uuid const& id) const {
    std::shared_lock< std::shared_mutex > g(mu_);
    auto const it = replicas_.find(id);
    if (it == replicas_.end()) return {};
    return it->second.host + ":" + std::to_string(it->second.raft_port);
}

std::shared_ptr< net::CraftTcpPeer > replica_manager::get_peer_client(boost::uuids::uuid const& id) {
    std::lock_guard< std::shared_mutex > g{mu_};

    auto const rit = replicas_.find(id);
    if (rit == replicas_.end()) return nullptr;
    if (!rit->second.peer_client) {
        rit->second.peer_client = std::make_shared< net::CraftTcpPeer >(rit->second.host, rit->second.tcp_port, id);
    }
    return rit->second.peer_client;
}

std::optional< replica_info > replica_manager::get(boost::uuids::uuid const& id) const {
    std::shared_lock< std::shared_mutex > g(mu_);
    auto const it = replicas_.find(id);
    if (it == replicas_.end()) return std::nullopt;
    return it->second;
}

void replica_manager::register_volume(boost::uuids::uuid const& vol_uuid,
                                      std::vector< replica_endpoint > const& members) {
    std::lock_guard< std::shared_mutex > g{mu_};
    std::vector< replica_info > rinfos;
    for (auto const& m : members) {
        rinfos.emplace_back(replicas_[m.id]);
    }
    volumes_[vol_uuid] = rinfos;
}

std::vector< replica_info > replica_manager::get_volume(boost::uuids::uuid const& volume_id) {
    std::shared_lock< std::shared_mutex > g(mu_);
    auto const it = volumes_.find(volume_id);
    if (it == volumes_.end()) return {};
    return it->second;
}

} // namespace craft