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

void replica_manager::start_replica_service(std::string const& path, boost::uuids::uuid const& my_uuid) {
    id_ = my_uuid;
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
        auto const id = boost::uuids::string_generator()(m.at("uuid").get< std::string >());
        replicas_.emplace(id,
                          replica_info{
                              .id = id,
                              .host = m.at("host").get< std::string >(),
                              .raft_port = m.at("raft_port").get< uint16_t >(),
                              .tcp_port = m.at("tcp_port").get< uint16_t >(),
                              .peer_client = (id == my_uuid)
                                  ? nullptr
                                  : std::make_shared< net::CraftTcpPeer >(m.at("host").get< std::string >(),
                                                                          m.at("tcp_port").get< uint16_t >(), id),
                          });
    }
}

std::string replica_manager::lookup_peer(boost::uuids::uuid const& id) const {
    std::shared_lock< std::shared_mutex > g(mu_);
    auto const it = replicas_.find(id);
    if (it == replicas_.end()) return {};
    return it->second.host + ":" + std::to_string(it->second.raft_port);
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