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

// make_tcp_replica_set: a cluster server + one CraftTcpReplica per replica (index 0 is the leader) -- the TCP
// drop-in for make_mem_replica_group. The client holds these craft_replica proxies DIRECTLY; there is no
// volume_handle here, so nothing pulls volume.hpp and a consumer links WITHOUT libhomeblocks.
//
// Teardown order is load-bearing: drain each proxy's in-flight ops off the shared session-mgr thread (from
// the caller's thread, while the proxies are still alive) BEFORE dropping any of them, then stop the server --
// see CraftTcpReplica::shutdown. The client, which co-owns the proxies, must be destroyed first.

#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include "net/cluster_server.hpp"
#include "net/tcp_replica.hpp"

namespace craft {

struct TcpReplicaSet {
    std::shared_ptr< net::craft_cluster_server > server;
    std::vector< std::shared_ptr< CraftTcpReplica > > replicas; // index 0 is the leader

    TcpReplicaSet() = default;
    TcpReplicaSet(TcpReplicaSet&&) = default;
    TcpReplicaSet& operator=(TcpReplicaSet&&) = default;
    TcpReplicaSet(TcpReplicaSet const&) = delete;
    TcpReplicaSet& operator=(TcpReplicaSet const&) = delete;
    ~TcpReplicaSet() {
        for (auto& p : replicas)
            if (p) p->shutdown();   // drain each proxy from HERE (main thread) before dropping any of them
        replicas.clear();           // ~CraftTcpReplica closes each connection
        if (server) server->stop(); // serve loops hit EOF -> the acceptor/serve threads join
    }
};

// `op_timeout` (0 = block forever) is set on every proxy: past it an op returns timed_out and the proxy resets
// its connection. Straggler tests set it below the injected delay so the client acks at quorum without the slow
// leg; the healthy tests leave it 0.
inline TcpReplicaSet make_tcp_replica_set(uint32_t n, uint32_t page_size, uint64_t capacity, uint32_t max_tx,
                                          std::chrono::milliseconds op_timeout = std::chrono::milliseconds{0}) {
    auto server = std::make_shared< net::craft_cluster_server >(n, page_size, capacity, max_tx);
    server->start();
    auto const members = server->members();
    auto const vol = server->volume_id();

    TcpReplicaSet out;
    out.server = server;
    out.replicas.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        peer_id_t id;
        std::memcpy(&id, members[i].id.data(), 16);
        out.replicas.push_back(std::make_shared< CraftTcpReplica >("127.0.0.1", server->port(i), id, vol, op_timeout));
    }
    return out;
}

} // namespace craft
