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

// craft_reference_tcp_srv -- a STANDALONE reference CRAFT replica server, one process = one replica. Run several
// on different ports to form a cluster of independent processes, then point a client at them (e.g. `ublkpp_disk
// --craft-tcp 127.0.0.1:p0,127.0.0.1:p1,127.0.0.1:p2`). It wraps craft_tcp_server (which drives a real
// MemCraftReplica over the wire, via its srv_* seam). Login/HELO are the FAKE local cold path (no peer-to-peer
// replica comms yet): a fresh cluster starts empty on every replica, so each independently mints/adopts the
// session term and serves term-fenced IO. Geometry (capacity / block size / max transfer) must match across the
// processes -- pass the SAME --capacity/--lba_size/--max_tx to all of them; the client reads it from the leader's
// login. Ctrl-C to stop.

#include <algorithm>
#include <atomic>
#include <csignal>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <craft/net/conn.hpp>
#include <craft/net/tcp_server.hpp>
#include <craft/wire.hpp>

// A 0 default means "unset" -> resolved in code (capacity to 1 GiB, max_tx to the single-sourced wire default), so
// no size magic number is duplicated in a CLI string.
SISL_OPTION_GROUP(craft_srv,
                  (port, "", "port", "Listen port (required)", ::cxxopts::value< uint16_t >()->default_value("0"),
                   "<port>"),
                  (capacity, "", "capacity", "Volume capacity in bytes (default 1 GiB)",
                   ::cxxopts::value< uint64_t >()->default_value("0"), "<bytes>"),
                  (lba_size, "", "lba_size", "Block size in bytes",
                   ::cxxopts::value< uint32_t >()->default_value("4096"), "<bytes>"),
                  (max_tx, "", "max_tx", "Volume max transfer in bytes (default wire::k_default_max_tx)",
                   ::cxxopts::value< uint32_t >()->default_value("0"), "<bytes>"))

#define SRV_OPTIONS logging, craft_srv
SISL_OPTIONS_ENABLE(SRV_OPTIONS)
SISL_LOGGING_DEF(craft)  // DEFINE the module (INIT alone only references it -> "undefined symbol module_level_craft")
SISL_LOGGING_INIT(craft) // register it for level control

namespace {
std::atomic< bool > g_stop{false};
void on_signal(int) { g_stop.store(true); }
} // namespace

int main(int argc, char** argv) {
    SISL_OPTIONS_LOAD(argc, argv, SRV_OPTIONS);
    sisl::logging::SetLogger("craft_reference_tcp_srv");

    auto const port = SISL_OPTIONS["port"].as< uint16_t >();
    if (port == 0) {
        std::cerr << "usage: craft_reference_tcp_srv --port P [--capacity BYTES] [--lba_size BYTES] [--max_tx BYTES]\n";
        return 2;
    }
    uint64_t capacity = SISL_OPTIONS["capacity"].as< uint64_t >();
    if (capacity == 0) capacity = uint64_t{1} << 30;
    auto const lba_size = SISL_OPTIONS["lba_size"].as< uint32_t >();
    uint32_t max_tx = SISL_OPTIONS["max_tx"].as< uint32_t >();
    if (max_tx == 0) max_tx = craft::wire::k_default_max_tx;

    // Advertise this one replica in login_rsp. The id is cosmetic here (the client routes by index, and HELO
    // fences by term, not id) -- a fresh random id is fine; the client's --craft-tcp supplies its own members.
    craft::net::server_geometry geo;
    geo.capacity = capacity;
    geo.lba_size = lba_size;
    geo.max_tx = max_tx;
    craft::wire::member self{};
    auto const id = boost::uuids::random_generator()();
    std::copy(id.begin(), id.end(), self.id.begin());
    self.addr = "127.0.0.1:" + std::to_string(port);
    geo.members.push_back(self);

    auto lst = craft::net::craft_listener::bind_listen(port);
    if (!lst) {
        std::cerr << "craft_reference_tcp_srv: bind/listen failed on 127.0.0.1:" << port << "\n";
        return 1;
    }
    // The server (replica + session state) is shared across connections: the client keeps a login connection AND
    // an on-ring data connection open at once, and they must see the same session. Session state races are benign
    // -- login and the data HELO both set the same term -- so a plain shared instance is fine for the reference.
    craft::net::craft_tcp_server server{std::move(geo)};

    // sigaction WITHOUT SA_RESTART: glibc's signal() sets SA_RESTART, which auto-restarts the blocking accept()
    // after the handler runs, so the loop would never re-check g_stop and Ctrl-C could not stop the server. With
    // the flag cleared, accept() returns EINTR on the signal and the loop breaks.
    struct sigaction sa{};
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    ::sigaction(SIGINT, &sa, nullptr);
    ::sigaction(SIGTERM, &sa, nullptr);
    std::cout << "craft_reference_tcp_srv: 127.0.0.1:" << port << "  capacity=" << capacity << " lba_size=" << lba_size
              << " max_tx=" << max_tx << "  (Ctrl-C to stop)" << std::endl;

    std::vector< std::thread > workers;
    while (!g_stop.load()) {
        auto conn = lst->accept(); // blocking; a signal interrupts it -> error -> re-check g_stop
        if (!conn) {
            if (g_stop.load()) break;
            continue;
        }
        workers.emplace_back([&server, c = std::move(*conn)]() mutable { server.serve(std::move(c)); });
    }
    for (auto& w : workers)
        w.detach(); // connections close as clients disconnect; the process is exiting anyway
    std::cout << "craft_reference_tcp_srv: stopped." << std::endl;
    return 0;
}
