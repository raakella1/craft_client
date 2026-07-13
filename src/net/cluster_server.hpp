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

// The CRAFT TCP cluster server: ONE process fronting a whole replica SET over TCP -- the P3 loopback stand-in
// for N separate replica processes (which arrive at P5). It wraps a MemReplicaGroup (N MemCraftReplicas + the
// MemTransport that runs the faked-RAFT login orchestration) and listens on N loopback ports, one per member,
// so the member list it hands out has real distinct addrs and the whole thing splits into N one-port
// processes later with no wire change.
//
// The session is SERVER-WIDE, not per-connection: LOGIN on the leader's port establishes the set-wide term
// once (MemTransport::run_login; a follower LOGIN returns the NOT_LEADER redirect); every other connection
// sends HELO to JOIN that same session. IO (write/read/keepalive) routes to the connection's replica and runs
// on the srv_* seam over real TCP.
//
// Homeblocks-coupled -- but only in the .cpp: the group is a pimpl, so this header pulls only wire + the
// connection, and a consumer (the test) needs no engine header.

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include <craft/net/conn.hpp>
#include <craft/wire.hpp>

namespace craft::net {

class craft_cluster_server {
public:
    // Build an n-replica set (deterministic ids from an internal volume id), page/geometry as given. Nothing
    // is bound until start().
    craft_cluster_server(uint32_t n, uint32_t page_size, uint64_t capacity, uint32_t max_tx);
    ~craft_cluster_server();
    craft_cluster_server(craft_cluster_server&&) noexcept;
    craft_cluster_server& operator=(craft_cluster_server&&) noexcept;

    // Bind one ephemeral loopback listener per member, resolve the member list, and start accepting.
    void start();
    // Stop accepting and join every thread. Idempotent; also run from the destructor. Callers must have closed
    // their client connections first (each serve loop returns on EOF).
    void stop();

    // The resolved member list: {id, "127.0.0.1:<port>"} per replica, in index order. Valid after start().
    std::vector< wire::member > members() const;
    uint16_t port(std::size_t idx) const;
    std::size_t leader_index() const;
    std::array< uint8_t, 16 > volume_id() const; // what a client presents in HELO

    // ── fault injection (test only) ── Forwarded to the wrapped MemTransport AND consulted on the IO path, so
    // they reach the srv_* data plane over TCP (the transport-level analog of the mem model's knobs). The
    // fault_state is COW / lock-free-read, so injecting from the test thread races nothing on the serve threads.
    void set_replica_up(std::size_t idx, bool up);             // full down: reads/writes/keepalives -> REPLICA_DOWN
    void force_subquorum(std::vector< std::size_t > keep_idx); // subsequent WRITES land only on `keep_idx`
    void clear_faults();

    // The straggler knob: replica `idx`'s serve loop sleeps `d` before applying+replying to EACH op. Paired
    // with the proxy's op_timeout it reproduces a real sub-quorum on the data path -- the client acks at
    // quorum without this slow leg, yet the leg STILL applies the write once the delay elapses (delivered
    // late, never lost). 0 removes the delay. This is the marquee CRAFT win, exercised end to end over TCP.
    void set_delay(std::size_t idx, std::chrono::milliseconds d);

    // ── test observability (read straight off replica `idx`, server-side, no wire) ──
    std::size_t journal_slots(std::size_t idx) const; // applied+journaled data slots (proves a write landed)
    uint64_t replica_term(std::size_t idx) const;     // the replica's session term (0 = no live session)
    // Read [addr, addr+len) bytes at horizon `read_lsn` off replica `idx` into `dest` (sized to `len`, holes
    // zero-filled). Mirrors the mem test's direct-off-the-straggler read; false if the read errored.
    bool read_replica(std::size_t idx, int64_t read_lsn, uint64_t addr, uint64_t len,
                      std::vector< uint8_t >& dest) const;

private:
    struct impl;
    std::unique_ptr< impl > p_;
};

} // namespace craft::net
