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

// craft_conn: a blocking io_uring TCP connection -- one socket with its own ring, submit-and-wait per op.
// This is the P1 stepping stone: it proves the wire codec over a real socket and the io_uring send/recv API.
// The async model (concurrent IOs, coroutine resume, the request_id landing pad) is P2; the send_all /
// recv_message interface is what stays, so only the internals change.

#include <chrono>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include <liburing.h>

#include <craft/wire.hpp>

namespace craft::net {

enum class net_error {
    setup,            // socket / ring / bind setup failed
    connect,          // connect() or accept() failed
    send,             // a send op errored
    recv,             // a recv op errored
    closed,           // peer closed the connection mid-message
    malformed,        // the framing was invalid (unknown op / bad length / bad digest)
    invalid_argument, // a client call precondition failed (e.g. a read dest smaller than the read length)
    timed_out,        // no reply within the op deadline; the connection is left needing a reset
};

// One connection. Movable, non-copyable; owns the socket fd and its io_uring.
class craft_conn {
public:
    craft_conn() = default;
    ~craft_conn();
    craft_conn(craft_conn&&) noexcept;
    craft_conn& operator=(craft_conn&&) noexcept;
    craft_conn(craft_conn const&) = delete;
    craft_conn& operator=(craft_conn const&) = delete;

    // Connect to host:port (blocking connect) and set up the ring.
    static std::expected< craft_conn, net_error > connect(std::string const& host, uint16_t port);
    // Wrap an already-connected/accepted fd and set up a ring (server side).
    static std::expected< craft_conn, net_error > adopt(int fd);

    // Send all of `data`, looping over partial sends. False on error.
    bool send_all(std::span< uint8_t const > data);

    // Receive exactly one framed CRAFT message: recv into an internal buffer until parse_message succeeds,
    // returning that message's bytes (any bytes read past it are kept for the next call). `dg` says which
    // digests the framing carries. `timeout` (0 = block forever) bounds the wait; on timed_out the recv is
    // left pending, so the caller MUST reset this connection before using it again.
    std::expected< std::vector< uint8_t >, net_error >
    recv_message(uint32_t max_tx, std::chrono::milliseconds timeout = std::chrono::milliseconds{0},
                 wire::digest_cfg dg = {});

    bool valid() const { return fd_ >= 0; }

private:
    int fd_ = -1;
    io_uring ring_{};
    bool ring_ready_ = false;
    std::vector< uint8_t > rx_; // bytes recv'd past one message boundary, carried to the next recv_message

    void close_all() noexcept;
    int io_send(std::span< uint8_t const >); // CQE res: bytes, or <0 on error
    // CQE res: bytes; 0 = EOF; -1 error; -2 timed out (recv SQE left pending -> caller must reset the conn).
    int io_recv(std::span< uint8_t >, std::optional< std::chrono::steady_clock::time_point > deadline = std::nullopt);
};

// A loopback TCP listener: bind + listen; accept() yields one craft_conn per connection.
class craft_listener {
public:
    craft_listener() = default;
    ~craft_listener();
    craft_listener(craft_listener&&) noexcept;
    craft_listener& operator=(craft_listener&&) noexcept;
    craft_listener(craft_listener const&) = delete;
    craft_listener& operator=(craft_listener const&) = delete;

    // Bind to 127.0.0.1:port (port 0 = an ephemeral port, readable via port()) and listen.
    static std::expected< craft_listener, net_error > bind_listen(uint16_t port);

    std::expected< craft_conn, net_error > accept(); // blocking
    uint16_t port() const { return port_; }

private:
    int fd_ = -1;
    uint16_t port_ = 0;
};

} // namespace craft::net
