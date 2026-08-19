#include "raft_state_manager.hpp"

#include <fstream>
#include <system_error>

#include <nlohmann/json.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include "in_memory_log_store.hpp"
#include "helper.hpp"

namespace {
std::string const log_store_key_prefix{"raft_log_store"};
}

namespace craft {

std::error_condition loadConfigFile(json& config_map, std::string const& _group_id, int32_t const _srv_id, std::shared_ptr< registry_manager >& _registry_mgr) {
    auto const registry_key = fmt::format(FMT_STRING("{}_s{}_config"), _group_id, _srv_id);
    auto json_ptr = _registry_mgr->get< json >(registry_key);
    if (!json_ptr) {
        LOGWARN("Could not load config for group_id={} server_id={}", _group_id, _srv_id);
        return std::make_error_condition(std::errc::no_such_file_or_directory);
    }
    config_map = *json_ptr;
    return {};
}

std::error_condition loadStateFile(json& state_map, std::string const& _group_id, int32_t const _srv_id, std::shared_ptr< registry_manager >& _registry_mgr) {
    auto const registry_key = fmt::format(FMT_STRING("{}_s{}_state"), _group_id, _srv_id);
    auto json_ptr = _registry_mgr->get< json >(registry_key);
    if (!json_ptr) {
        LOGWARN("Could not load state for group_id={} server_id={}", _group_id, _srv_id);
        return std::make_error_condition(std::errc::no_such_file_or_directory);
    }
    state_map = *json_ptr;
    return {};
}

nuraft::ptr< nuraft::srv_config > fromServer(json const& server) {
    auto const id = static_cast< int32_t >(server["id"]);
    auto const dc_id = static_cast< int32_t >(server["dc_id"]);
    auto const endpoint = server["endpoint"];
    auto const aux = server["aux"];
    auto const learner = server["learner"];
    auto const prior = static_cast< int32_t >(server["priority"]);
    return nuraft::cs_new< nuraft::srv_config >(id, dc_id, endpoint, aux, learner, prior);
}

void fromServers(json const& servers, std::list< nuraft::ptr< nuraft::srv_config > >& server_list) {
    for (auto const& server_conf : servers) {
        server_list.push_back(fromServer(server_conf));
    }
}

json toServers(std::list< nuraft::ptr< nuraft::srv_config > > const& server_list) {
    auto servers = json::array();
    for (auto const& server_conf : server_list) {
        servers.push_back(json{{"id", server_conf->get_id()},
                               {"dc_id", server_conf->get_dc_id()},
                               {"endpoint", server_conf->get_endpoint()},
                               {"aux", server_conf->get_aux()},
                               {"learner", server_conf->is_learner()},
                               {"priority", server_conf->get_priority()}});
    }
    return servers;
}

nuraft::ptr< nuraft::cluster_config > fromClusterConfig(json const& cluster_config) {
    auto const& log_idx = cluster_config["log_idx"];
    auto const& prev_log_idx = cluster_config["prev_log_idx"];
    auto const& eventual = cluster_config["eventual_consistency"];

    auto raft_config = nuraft::cs_new< nuraft::cluster_config >(log_idx, prev_log_idx, eventual);
    fromServers(cluster_config["servers"], raft_config->get_servers());
    return raft_config;
}

raft_state_mgr::raft_state_mgr(int32_t srv_id, nuraft_mesg::peer_id_t const& srv_addr,
                               nuraft_mesg::group_id_t const& group_id, raft_commit_cb_t cb,
                               std::shared_ptr< registry_manager > registry_mgr) :
        nuraft_mesg::mesg_state_mgr(),
        _srv_id(srv_id),
        _srv_addr(to_string(srv_addr)),
        _group_id(to_string(group_id)),
        _commit_cb(std::move(cb)),
        _registry_mgr(std::move(registry_mgr)) {}

nuraft::ptr< nuraft::cluster_config > raft_state_mgr::load_config() {
    LOGDEBUG("Loading config for [{}]", _group_id);
    json config_map;
    if (auto err = loadConfigFile(config_map, _group_id, _srv_id, _registry_mgr); !err) { return fromClusterConfig(config_map); }
    auto conf = nuraft::cs_new< nuraft::cluster_config >();
    conf->get_servers().push_back(nuraft::cs_new< nuraft::srv_config >(_srv_id, _srv_addr));
    return conf;
}

nuraft::ptr< nuraft::log_store > raft_state_mgr::load_log_store() {
    auto log_store = _registry_mgr->get< nuraft::inmem_log_store >(
        registry_key(log_store_key_prefix, boost::uuids::string_generator()(_group_id)));
    if (log_store) {
        LOGDEBUG("RAFT log store for group_id={} already exists, returning existing instance", _group_id);
        return log_store;
    }
    LOGDEBUG("Creating RAFT log store for group_id={}", _group_id);
    log_store = std::make_shared< nuraft::inmem_log_store >();
    _registry_mgr->put< nuraft::inmem_log_store >(
        registry_key(log_store_key_prefix, boost::uuids::string_generator()(_group_id)), log_store);
    return log_store;
}

nuraft::ptr< nuraft::srv_state > raft_state_mgr::read_state() {
    LOGDEBUG("Loading state for server: {}", _srv_id);
    json state_map;
    auto state = nuraft::cs_new< nuraft::srv_state >();
    if (auto err = loadStateFile(state_map, _group_id, _srv_id, _registry_mgr); !err) {
        try {
            state->set_term(static_cast< uint64_t >(state_map["term"]));
            state->set_voted_for(static_cast< int >(state_map["voted_for"]));
        } catch (std::out_of_range& e) { LOGWARN("State file was not in the expected format!"); }
    }
    return state;
}

void raft_state_mgr::save_config(const nuraft::cluster_config& config) {
    auto const config_file = fmt::format(FMT_STRING("{}_s{}/config.json"), _group_id, _srv_id);
    auto json_obj = json{{"log_idx", config.get_log_idx()},
                         {"prev_log_idx", config.get_prev_log_idx()},
                         {"eventual_consistency", config.is_async_replication()},
                         {"user_ctx", config.get_user_ctx()},
                         {"servers", toServers(const_cast< nuraft::cluster_config& >(config).get_servers())}};
    _registry_mgr->put< json >(fmt::format(FMT_STRING("{}_s{}_config"), _group_id, _srv_id),
                               std::make_shared< json >(std::move(json_obj)));
}

void raft_state_mgr::save_state(const nuraft::srv_state& state) {
    auto const state_file = fmt::format(FMT_STRING("{}_s{}/state.json"), _group_id, _srv_id);
    auto json_obj = json{{"term", state.get_term()}, {"voted_for", state.get_voted_for()}};

    _registry_mgr->put< json >(fmt::format(FMT_STRING("{}_s{}_state"), _group_id, _srv_id),
                               std::make_shared< json >(std::move(json_obj)));
}

uint32_t raft_state_mgr::get_logstore_id() const { return 0; }

std::shared_ptr< nuraft::state_machine > raft_state_mgr::get_state_machine() {
    return std::make_shared< echo_state_machine >(_commit_cb, _group_id, _registry_mgr);
}

void raft_state_mgr::permanent_destroy() {}

void raft_state_mgr::leave() {}

} // namespace craft
