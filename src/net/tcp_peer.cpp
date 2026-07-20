#include "net/tcp_peer.hpp"

#include <cstring>

namespace craft::net {

namespace {
auto fail(craft_error e) { return std::unexpected(make_error_condition(e)); }

std::error_condition net_to_error(net_error e) {
    // Same mapping as tcp_replica.cpp's net_to_error -- kept local for now since this file has no other
    // reason to depend on tcp_replica.cpp. Worth factoring into a shared helper once both exist stably.
    switch (e) {
    case net_error::send:
    case net_error::setup:
    case net_error::connect:
        return make_error_condition(craft_error::REPLICA_DOWN);
    case net_error::recv:
    case net_error::closed:
    case net_error::malformed:
    case net_error::timed_out:
        return std::make_error_condition(std::errc::timed_out);
    case net_error::invalid_argument:
        return std::make_error_condition(std::errc::invalid_argument);
    }
    return make_error_condition(craft_error::REPLICA_DOWN);
}
} // namespace

CraftTcpPeer::CraftTcpPeer(std::string host, uint16_t port, peer_id_t id, std::chrono::milliseconds op_timeout) :
        host_{std::move(host)}, port_{port}, id_{id}, op_timeout_{op_timeout} {}

CraftTcpPeer::~CraftTcpPeer() = default;

bool CraftTcpPeer::ensure_connected() {
    if (connected_) return true;
    auto c = craft_conn::connect(host_, port_, op_timeout_ > std::chrono::milliseconds{0} ? op_timeout_
                                                                                          : k_connect_timeout);
    if (!c) return false;
    conn_ = std::move(*c);
    connected_ = true;
    return true;
}

async_result< lsn_pair > CraftTcpPeer::get_rs_commit_lsn(uint64_t term, bool is_login) {
    if (!ensure_connected()) co_return fail(craft_error::REPLICA_DOWN);

    wire::get_rs_commit_lsn_req req{};
    req.term = term;
    req.is_login = is_login ? 1 : 0;

    std::vector< uint8_t > out;
    wire::frame_message(out, wire::op::get_rs_commit_lsn, 0, next_request_id(),
                        {reinterpret_cast< uint8_t const* >(&req), sizeof(req)}, {});
    if (!conn_.send_all(out)) {
        connected_ = false;
        co_return fail(craft_error::REPLICA_DOWN);
    }

    auto msg = conn_.recv_message(wire::k_default_max_tx);
    if (!msg) {
        connected_ = false;
        co_return std::unexpected(net_to_error(msg.error()));
    }
    auto parsed = wire::parse_message(*msg, wire::k_default_max_tx);
    if (!parsed) {
        connected_ = false;
        co_return fail(craft_error::INTERNAL); // malformed reply
    }
    auto const rsp = wire::decode< wire::get_rs_commit_lsn_rsp >(parsed->op_header);
    if (static_cast< wire::status >(parsed->hdr.status) != wire::status::ok) {
        co_return fail(craft_error::INTERNAL); // TODO: proper status_to_error mapping, mirrors tcp_replica.cpp
    }
    co_return lsn_pair{rsp.commit_lsn, rsp.last_append_lsn};
}

// ── existing craft_peer methods: not yet implemented over the wire (no opcodes allocated for these yet) ──
async_result< lsn_pair > CraftTcpPeer::get_lsns() { co_return fail(craft_error::INTERNAL); }
async_result< std::vector< JournalSlot > > CraftTcpPeer::fetch_data(std::vector< int64_t > lsns) {
    co_return fail(craft_error::INTERNAL);
}
async_status CraftTcpPeer::truncate(int64_t lsn) { co_return fail(craft_error::INTERNAL); }

} // namespace craft::net