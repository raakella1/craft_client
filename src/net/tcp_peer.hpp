#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include <craft/net/conn.hpp> // craft_conn -- raw transport, NOT wire_client (that's client-plane only)
#include <craft/wire.hpp>

#include "craft_peer.hpp"

namespace craft::net {

class CraftTcpPeer final : public craft_peer {
public:
    CraftTcpPeer(std::string host, uint16_t port, peer_id_t id,
                 std::chrono::milliseconds op_timeout = std::chrono::milliseconds{0});
    ~CraftTcpPeer() override;

    CraftTcpPeer(CraftTcpPeer const&) = delete;
    CraftTcpPeer& operator=(CraftTcpPeer const&) = delete;

    // ── craft_peer: existing (unchanged) ──
    virtual async_result< lsn_pair > get_rs_commit_lsn(uint64_t term, bool is_login) override;
    async_result< std::vector< JournalSlot > > fetch_data(std::vector< int64_t > lsns) override;

    // ── NEW, not yet on craft_peer.hpp: Login's Phase 1 poll specifically -- carries the caller's own
    // watermarks + the quiesce flag. Add to craft_peer.hpp as a pure virtual once this compiles standalone,
    // at which point MemCraftReplica also needs a stub implementation.
    async_result< lsn_pair > login_poll(uint64_t term, bool is_login, int64_t my_commit, int64_t my_append);

private:
    bool ensure_connected();
    uint16_t next_request_id() { return next_rid_++; }

    std::string host_;
    uint16_t port_;
    peer_id_t id_;
    std::chrono::milliseconds op_timeout_{0};

    craft_conn conn_;
    bool connected_{false};
    uint16_t next_rid_{1};
};

} // namespace craft::net
