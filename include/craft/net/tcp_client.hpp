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

// The CRAFT TCP reference client: LOGIN / LOGOUT and the IO ops (WRITE / READ / KEEPALIVE) over a blocking
// io_uring connection (craft_conn).
//
// Deliberately WIRE-ONLY. It includes the wire codec and the connection and NO homeblocks header: it speaks
// wire::status / wire::extent_desc and the connection's net_error, nothing from the storage engine. That is
// what keeps it an embeddable, self-contained reference client (buildable against just the wire spec +
// liburing). A transport fault is a net_error; a server's protocol status (STALE_TERM, ...) is NOT an error
// here -- it rides IN the reply value, so the caller inspects it. The homeblocks mapping (wire::status ->
// craft_error, replies -> LSNPair / io_extent) belongs to the P3 craft_replica adapter, not here.
//
// Still blocking submit-and-wait and one connection == one session; the send_all / recv_message interface is
// what stays as the async model swaps craft_conn's internals.

#include <array>
#include <chrono>
#include <cstdint>
#include <expected>
#include <span>
#include <string>
#include <vector>

#include <craft/net/conn.hpp>
#include <craft/wire.hpp>

namespace craft::net {

// What the client learns from a successful LOGIN.
struct login_result {
    uint64_t term = 0;
    int64_t dlsn = -1;
    uint64_t capacity = 0;
    uint32_t lba_size = 0;
    uint32_t max_tx = 0;
    std::array< uint8_t, 16 > leader_hint{};
    std::vector< wire::member > members;
};

// A WRITE / KEEPALIVE reply: the protocol status plus the replica's {commit_lsn, last_append_lsn}. A non-ok
// status is a VALID reply the caller inspects (the LSNs are then not meaningful); only a transport fault is a
// net_error.
struct lsn_reply {
    wire::status status = wire::status::ok;
    int64_t commit_lsn = -1;
    int64_t last_append_lsn = -1;
};

// A READ reply: status + LSNs + the sparse layout (ascending by addr). On an ok status the caller's `dest`
// buffer was filled IN PLACE -- data sub-ranges scattered from the reply, holes zero-filled.
struct read_reply {
    wire::status status = wire::status::ok;
    int64_t commit_lsn = -1;
    int64_t last_append_lsn = -1;
    std::vector< wire::extent_desc > extents;
};

class craft_tcp_client {
public:
    craft_tcp_client() = default;
    craft_tcp_client(craft_tcp_client&&) = default;
    craft_tcp_client& operator=(craft_tcp_client&&) = default;

    static std::expected< craft_tcp_client, net_error > connect(std::string const& host, uint16_t port);

    // ── session ──
    std::expected< login_result, net_error > login(uint64_t client_token);
    // Bind THIS connection to an already-established session (established by LOGIN on the leader). Carries the
    // volume id + client token + the session term the client learned from login. Status only.
    std::expected< wire::status, net_error > helo(std::array< uint8_t, 16 > const& volume_id, uint64_t client_token,
                                                  uint64_t term);
    std::expected< wire::status, net_error > logout();

    // ── IO (blocking; one request/response per call over the single connection) ──
    //
    // `commit_lsn` / `all_committed_lsn` are the piggybacked client watermarks (req_hdr): commit_lsn advances
    // the replica's frontier best-effort in dLSN order (-1 = do not advance); all_committed_lsn floors journal
    // reclaim (-1 = unknown). The term fence is a connection constant (established at LOGIN), not re-sent.

    // Append one client-assigned write at `dlsn`. `data` empty => a zero write (WRITE_ZEROES over [addr,
    // addr+len)); non-empty => exactly `len` bytes.
    std::expected< lsn_reply, net_error > write(int64_t dlsn, uint64_t addr, uint64_t len,
                                                std::span< uint8_t const > data, int64_t commit_lsn = -1,
                                                int64_t all_committed_lsn = -1);

    // Read the latest version <= `read_lsn` (horizon H) of [addr, addr+len) into `dest` (filled in place on
    // ok). `dest` must cover `len` bytes (else net_error::invalid_argument).
    std::expected< read_reply, net_error > read(int64_t read_lsn, uint64_t addr, uint64_t len,
                                                std::span< uint8_t > dest, int64_t commit_lsn = -1,
                                                int64_t all_committed_lsn = -1);

    // Advance the frontier toward `commit_lsn` and keep the session alive; returns {commit_lsn,
    // last_append_lsn}. The timer-less liveness drive's carrier, and CRAFT's only commit verb.
    std::expected< lsn_reply, net_error > keep_alive(int64_t commit_lsn = -1, int64_t all_committed_lsn = -1);

    // Bound the wait on every reply. 0 (default) blocks forever. On a timeout an op returns
    // net_error::timed_out, and the connection is left needing a reset (the proxy above reconnects).
    void set_op_timeout(std::chrono::milliseconds t) { op_timeout_ = t; }

private:
    craft_conn conn_;
    uint64_t term_ = 0;
    uint16_t next_rid_ = 1;
    std::chrono::milliseconds op_timeout_{0};
};

} // namespace craft::net
