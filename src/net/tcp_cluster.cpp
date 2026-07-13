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

// tcp_cluster: the io_uring TCP transport behind an opaque handle. The concrete class lives ENTIRELY here (never
// installed) -- the public surface is craft/tcp.hpp's free functions over tcp_cluster_handle. It builds one
// CraftTcpReplica proxy per endpoint and keeps that proxy type (and every src/net header) off the surface.
// Teardown drains each proxy's in-flight ops off the shared session-mgr thread before any proxy drops (see
// tcp_set.hpp for the same rule the reference server-side set uses).

#include <cstring>
#include <string>

#include <craft/tcp.hpp>

#include "net/tcp_replica.hpp" // CraftTcpReplica (internal)

namespace craft {

class tcp_cluster {
public:
    tcp_cluster(std::vector< std::shared_ptr< CraftTcpReplica > > proxies,
                std::vector< std::shared_ptr< craft_replica > > backends) :
            proxies_{std::move(proxies)}, backends_{std::move(backends)} {}
    ~tcp_cluster() {
        for (auto& p : proxies_)
            if (p) p->shutdown(); // drain each proxy from HERE (the destroying thread) before any of them drops
    }

    std::vector< std::shared_ptr< craft_replica > > const& backends() const { return backends_; }

private:
    std::vector< std::shared_ptr< CraftTcpReplica > > proxies_; // concrete: needed to call shutdown()
    std::vector< std::shared_ptr< craft_replica > > backends_;  // upcast aliases handed to make_client()
};

// ── free functions: the public surface ──

tcp_cluster_handle make_tcp_cluster(std::vector< replica_endpoint > const& members, volume_id_t vol_id,
                                    std::chrono::milliseconds op_timeout) {
    std::array< uint8_t, 16 > vol{};
    std::memcpy(vol.data(), &vol_id, 16); // uuid is 16 contiguous bytes -- what HELO presents

    std::vector< std::shared_ptr< CraftTcpReplica > > proxies;
    std::vector< std::shared_ptr< craft_replica > > backends;
    proxies.reserve(members.size());
    backends.reserve(members.size());
    for (auto const& m : members) {
        auto const colon = m.addr.rfind(':'); // rfind so an IPv6-ish host with colons keeps only the last field
        auto const host = m.addr.substr(0, colon);
        auto const port = static_cast< uint16_t >(std::stoul(m.addr.substr(colon + 1)));
        auto proxy = std::make_shared< CraftTcpReplica >(host, port, m.id, vol, op_timeout);
        backends.push_back(proxy);
        proxies.push_back(std::move(proxy));
    }
    return std::make_shared< tcp_cluster >(std::move(proxies), std::move(backends));
}

std::vector< std::shared_ptr< craft_replica > > const& backends(tcp_cluster_handle const& c) { return c->backends(); }

} // namespace craft
