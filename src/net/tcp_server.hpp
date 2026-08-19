/*********************************************************************************
 * Modifications Copyright 2026 eBay Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 *********************************************************************************/
#pragma once

// The CRAFT TCP server: decodes wire requests and drives a real MemCraftReplica through its srv_* local-server
// seam (the roadmap's "reuse the model" lever), so the tested journal / index / apply / read-with-holes logic
// serves over real TCP with no second modeled network underneath it. Unlike the client, the server is coupled
// to the reference model -- but only in the .cpp: the replica is a pimpl (forward-declared here, included in
// craft_tcp_server.cpp), so THIS header stays homeblocks-free (wire + connection only).
//
// Blocking submit-and-wait. HELO is handled as a FAKE cold path (until peer-to-peer replica comms land): a
// standalone replica this client never logged into adopts the presented session term and establishes locally, so
// a fresh (empty) cluster of independent craft_reference_tcp_srv processes serves term-fenced IO. No auth (P6).

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include <craft/net/conn.hpp>
#include <craft/wire.hpp>

namespace craft {
struct replica_endpoint;
class RaftReplica; // the server's state backing (pimpl; included only in craft_tcp_server.cpp)
class registry_manager;
} // namespace craft

namespace craft::net {

// The server's per-volume geometry -- what LOGIN advertises; the replica's journal/index is built from it.
struct server_geometry {
    uint64_t capacity = 0;
    uint32_t lba_size = 0;
    uint32_t max_tx = 0;
    wire::member member;
};

class craft_tcp_server {
public:
    explicit craft_tcp_server(server_geometry geo, std::string const& server_config_file = {},
                              std::shared_ptr< registry_manager > registry_mgr = nullptr,
                              bool init_raft_service = false);
    ~craft_tcp_server();
    craft_tcp_server(craft_tcp_server&&) = default;
    craft_tcp_server& operator=(craft_tcp_server&&) = default;

    // Handle one connection until it closes (blocking). Run on a thread for the test.
    void serve(craft_conn conn);

    // DIAGNOSTIC: log this replica's CRAFT state -- commit frontier, journal depth, and any MISSING dLSNs. A
    // Missing slot (a write this replica never received, because the client acked at quorum without it)
    // permanently PINS commit_lsn below it: nothing fills the hole, because filling it is resync == the peer
    // plane, which does not exist yet (see docs/peer-plane.md). A pinned commit_lsn also means the journal never
    // reclaims and every read walks the unapplied tail. Defined in the .cpp, where MemCraftReplica is complete.
    void log_stats() const;

private:
    server_geometry geo_;
    std::shared_ptr< RaftReplica > replica_;     // the real state; driven via its srv_* local-server seam
    uint64_t next_term_ = 0;                     // monotonic term source; a fresh LOGIN takes ++next_term_
    uint64_t session_term_ = 0;                  // the current session's term, stamped on every IO
    bool session_active_ = false;                // false before LOGIN / after LOGOUT -> IO is fenced
    std::shared_ptr< registry_manager > registry_mgr_ = nullptr; // the registry manager instance
    bool raft_enabled_ = false;

    void on_login(craft_conn&, wire::message const&);
    void on_helo(craft_conn&, wire::message const&);
    void on_logout(craft_conn&, wire::message const&);
    void on_write(craft_conn&, wire::message const&);
    void on_read(craft_conn&, wire::message const&);
    void on_keep_alive(craft_conn&, wire::message const&);
    void on_resolve(craft_conn&, wire::message const&);
    void on_create_volume(craft_conn&, wire::message const&);
    void on_get_rs_commit_lsn(craft_conn& conn, wire::message const& req);
    void on_fetch_data(craft_conn& conn, wire::message const& req);
};

} // namespace craft::net
