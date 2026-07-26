#include <sisl/logging/logging.h>
#include "raft_service.hpp"
#include "raft_state_manager.hpp"
#include "helper.hpp"
#include "replica_mgr.hpp"

#include <libnuraft/raft_params.hxx>
#include <libnuraft/srv_config.hxx>
#include <boost/uuid/string_generator.hpp>
#include <utility>

namespace craft {

namespace {
auto fail(craft_error e) { return std::unexpected(make_error_condition(e)); }
} // namespace

static nuraft::ptr< nuraft::buffer > create_message(nlohmann::json const& j_obj) {
    auto v_msgpack = nlohmann::json::to_msgpack(j_obj);
    auto buf = nuraft::buffer::alloc(v_msgpack.size() + sizeof(int32_t));
    buf->put(&v_msgpack[0], v_msgpack.size());
    buf->pos(0);
    return buf;
}

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

result< void > raft_service::srv_create_volume(boost::uuids::uuid const& group_id,
                                               std::vector< replica_endpoint > const& members, raft_commit_cb_t cb) {
    auto consensus = raft_service::instance()->get_consensus();

    // Seat THIS replica as leader by creating the group.
    if (auto const status = craft::sync_get(consensus->create_group(group_id, raft_service::default_group_type_));
        !status) {
        return fail(craft_error::INTERNAL);
    }

    // Add every OTHER member as a follower.
    for (auto const& m : members) {
        if (lookup_peer(m.id).empty()) {
            return fail(craft_error::INTERNAL); // sanity: member missing in the server config json
        }
        if (m.id == server_uuid_) continue;

        auto srv_cfg =
            nuraft::srv_config(nuraft_mesg::to_server_id(m.id), 0, boost::uuids::to_string(m.id), "", false);
        if (auto const result = craft::sync_get(consensus->add_member(group_id, srv_cfg)); !result) {
            return fail(craft_error::INTERNAL);
        }
    }
    add_commit_cb(group_id, cb);
    return {};
}

std::string raft_service::lookup_peer(nuraft_mesg::peer_id_t const& peer_id) {

    auto peer_addr = replica_manager::instance()->lookup_peer(peer_id);
    if (peer_addr.empty()) { LOGWARN("Peer {} not found in lookup map", boost::uuids::to_string(peer_id)); }
    return peer_addr;
}

std::shared_ptr< nuraft_mesg::mesg_state_mgr > raft_service::create_state_mgr(int32_t const srv_id,
                                                                              nuraft_mesg::group_id_t const& group_id) {
    auto result = get_state_mgr(group_id);
    if (result) {
        LOGINFO("RAFT state manager for group_id={} already exists, returning existing instance",
                boost::uuids::to_string(group_id));
        return result.value();
    }
    LOGINFO("Creating RAFT state manager for server_id={} group_id={}", srv_id, boost::uuids::to_string(group_id));
    auto const r = get_commit_cb(group_id);
    auto mgr = std::make_shared< raft_state_mgr >(srv_id, server_uuid_, group_id, (r ? r.value() : nullptr));
    add_state_mgr(group_id, mgr);
    return mgr;
}

result< std::shared_ptr< raft_state_mgr > > raft_service::get_state_mgr(nuraft_mesg::group_id_t const& group_id) {
    std::shared_lock< std::shared_mutex > g{mu_};
    auto const it = state_mgrs_.find(group_id);
    if (it == state_mgrs_.end()) return fail(craft_error::INTERNAL);
    return it->second;
}

void raft_service::add_state_mgr(nuraft_mesg::group_id_t const& group_id, std::shared_ptr< raft_state_mgr > mgr) {
    std::lock_guard< std::shared_mutex > g{mu_};
    state_mgrs_[group_id] = std::move(mgr);
}

result< raft_commit_cb_t > raft_service::get_commit_cb(nuraft_mesg::group_id_t const& group_id) {
    std::shared_lock< std::shared_mutex > g{mu_};
    auto const it = commit_cbs_.find(group_id);
    if (it == commit_cbs_.end()) return fail(craft_error::INTERNAL);
    return it->second;
}

void raft_service::add_commit_cb(nuraft_mesg::group_id_t const& group_id, raft_commit_cb_t cb) {
    std::lock_guard< std::shared_mutex > g{mu_};
    commit_cbs_.emplace(group_id, std::move(cb));
}

bool raft_service::is_leader(nuraft_mesg::group_id_t const& group_id) {
    auto const state_mgr = get_state_mgr(group_id);
    if (!state_mgr) {
        LOGWARN("RAFT state manager for group_id={} not found", boost::uuids::to_string(group_id));
        return false;
    }
    auto* raft_ctx = state_mgr.value()->repl_ctx();
    return raft_ctx && raft_ctx->is_raft_leader();
}

nuraft_mesg::peer_id_t raft_service::leader_id(nuraft_mesg::group_id_t const& group_id) {
    auto const state_mgr = get_state_mgr(group_id);
    if (!state_mgr) {
        LOGWARN("RAFT state manager for group_id={} not found", boost::uuids::to_string(group_id));
        return {};
    }
    auto* raft_ctx = state_mgr.value()->repl_ctx();
    if (!raft_ctx) {
        LOGWARN("No leader for the raft group {}", group_id);
        return {};
    }
    return boost::uuids::string_generator()(raft_ctx->raft_leader_id());
}

template < typename MsgT >
result< void > raft_service::propose(boost::uuids::uuid const& group_id, MsgT const& payload) {
    auto const state_mgr = get_state_mgr(group_id);
    if (!state_mgr) {
        LOGWARN("RAFT state manager for group_id={} not found", boost::uuids::to_string(group_id));
        return std::unexpected(make_error_condition(craft_error::INTERNAL));
    }
    auto* raft_ctx = state_mgr.value()->repl_ctx();
    if (!raft_ctx) {
        LOGWARN("RAFT state manager context for group_id={} not found", boost::uuids::to_string(group_id));
        return std::unexpected(make_error_condition(craft_error::INTERNAL));
    }

    auto const append_status = raft_ctx->raft_server()->append_entries({create_message(nlohmann::json(payload))});
    if (append_status && !append_status->get_accepted()) {
        return std::unexpected(nuraft_mesg::to_condition(append_status->get_result_code()));
    }
    return {};
}

template result< void > raft_service::propose< SyncRSCommitLSNMsg >(boost::uuids::uuid const&,
                                                                    SyncRSCommitLSNMsg const&);
template result< void > raft_service::propose< InternalLoginMsg >(boost::uuids::uuid const&, InternalLoginMsg const&);

} // namespace craft
