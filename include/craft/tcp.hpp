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

// craft/tcp.hpp -- the remote backend builder. Connect a CRAFT client to a set of replica servers over TCP:
// make_tcp_cluster() builds one io_uring proxy per replica; backends() hands their craft_replica backends to
// make_client(). This is the real transport (it links craft_client); for an in-process client (no server, no
// wire) see craft/local.hpp. The io_uring proxy (CraftTcpReplica) stays internal -- a consumer names only
// endpoints and this builder, never a src/net header.
//
// Same shape as the client and craft/local.hpp: an OPAQUE handle + free functions, never a concrete class on the
// surface. The handle OWNS the proxies. Hold it for the client's lifetime and destroy the client FIRST: dropping
// the last handle drains each proxy's worker from the caller's thread while it still owns them (the detached-
// keep_alive self-join rule -- a completing keep_alive must never be the thread that joins its own worker).
// Declaring the cluster handle before the client handle gives exactly that order.

#include <chrono>
#include <memory>
#include <vector>

#include <craft/replica.hpp> // craft_replica + make_client
#include <craft/types.hpp>   // volume_id_t, replica_endpoint

namespace craft {

// Opaque: defined only in src/. Consumers name it solely through the handle below.
class tcp_cluster;
using tcp_cluster_handle = std::shared_ptr< tcp_cluster >;

// Connect to `members` (index 0 is the leader you first log in to; a NOT_LEADER redirect selects another). Each
// endpoint is {peer id, "host:port"}. `vol_id` is what HELO presents to bind the session. `op_timeout` (0 =
// block forever) bounds every reply: past it an op returns timed_out and the proxy resets + reconnects (re-HELO
// on the next op). Nothing actually connects until the first op is issued on a proxy's worker.
tcp_cluster_handle make_tcp_cluster(std::vector< replica_endpoint > const& members, volume_id_t vol_id,
                                    std::chrono::milliseconds op_timeout = std::chrono::milliseconds{0});

// The backends to hand to make_client(); same order as `members`. Valid until the last handle is dropped.
std::vector< std::shared_ptr< craft_replica > > const& backends(tcp_cluster_handle const& c);

} // namespace craft
