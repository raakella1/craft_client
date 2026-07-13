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

// craft_async_conn -- the P2 ON-RING TCP connection (the async model conn.hpp/tcp_client.hpp promised). INTERNAL:
// lives under src/, never installed. tcp_replica.hpp forward-declares it behind a unique_ptr; only tcp_replica.cpp
// (and a white-box test) include this. It submits socket SQEs on a CALLER-OWNED ::io_uring (a ublk queue's, or a
// test's) instead of blocking on its own ring, so MANY requests sit in flight at once (QD>1) and complete on the
// ring owner's reap thread -- exactly the scattered concurrency CRAFT correctness is emergent from. A socket op
// is NOT 1:1 SQE:CQE: a send CQE means "request left", the reply arrives later via a recv, out of order. So ONE
// persistent recv SQE (the pump) demuxes every reply by wire request_id back to the leg that awaits it.
//
// Raw fd, NOT IOSQE_FIXED_FILE: nothing is registered with the ring, so a socket is opened lazily, dropped on a
// fault, and reconnected at will -- reconnect is just the lazy path again. Management (connect + HELO) is off the
// caller's admin path: login already ran (blocking) at factory time to yield lba/capacity/term BEFORE the disk
// existed, so this only owns the DATA path (write/read/keep_alive), HELO'ing the data fd with the session term
// the op carries. The wire codec (frame_message / parse_message) is shared verbatim with the blocking client.

#include <array>
#include <coroutine>
#include <cstdint>
#include <expected>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include <liburing.h>

#include <sisl/async/cqe_state.hpp> // cqe_awaitable (send/connect/recv completions) + the managed-user_data contract
#include <sisl/async/disk_task.hpp> // disk_task<T>: the stand-alone service-loop handle (steal _coro / destroy), as ublkpp's queue_service
#include <sisl/async/task.hpp> // sisl::async::task<T> == exec::task<T>

#include <craft/net/conn.hpp> // net_error
#include "net/tcp_client.hpp" // lsn_reply / read_reply
#include <craft/wire.hpp>     // op / frame_message / parse_message

namespace craft::net {

class craft_async_conn {
public:
    // `ring` is the caller's io_uring; every SQE this connection submits goes on it, and the caller's reap loop
    // (ublk's run_queue_loop or a test) drives completion via sisl::async::complete_cqe_state. `max_tx` is the max
    // framed BODY the transport may parse -- NOT the payload, but wire::framed_body_max(payload, lba): the payload
    // plus a read reply's extent table, so a full-payload read still fits. Nothing connects until first ensure_ready.
    craft_async_conn(std::string host, uint16_t port, uint32_t max_tx, ::io_uring* ring);
    ~craft_async_conn();
    craft_async_conn(craft_async_conn const&) = delete;
    craft_async_conn& operator=(craft_async_conn const&) = delete;

    // Lazy connect + HELO(vol, token, term) + arm the recv pump, on the ring. Idempotent while ready; after a
    // fault the fd was dropped, so this reconnects. A follower/leader distinction is irrelevant here -- HELO
    // binds this data fd to the already-established session by term.
    sisl::async::task< std::expected< void, net_error > > ensure_ready(std::array< uint8_t, 16 > const& vol,
                                                                       uint64_t token, uint64_t term);

    // ── IO (async, on the ring; many may be in flight, demuxed by request_id) ──
    sisl::async::task< std::expected< lsn_reply, net_error > > write(int64_t dlsn, uint64_t addr, uint64_t len,
                                                                     std::span< uint8_t const > data,
                                                                     int64_t commit_lsn, int64_t all_committed_lsn);
    sisl::async::task< std::expected< read_reply, net_error > > read(int64_t read_lsn, uint64_t addr, uint64_t len,
                                                                     std::span< uint8_t > dest, int64_t commit_lsn,
                                                                     int64_t all_committed_lsn);
    sisl::async::task< std::expected< lsn_reply, net_error > > keep_alive(int64_t commit_lsn,
                                                                          int64_t all_committed_lsn);

    bool ready() const noexcept { return ready_; }

    // Stop the pump, close the fd, and fail every pending leg. The CALLER must have quiesced the ring (no SQE of
    // ours left un-reaped) before this destroys the pump frame -- same rule as ublkpp's queue_service dtor: the
    // ublk queue tears its io_uring down (kernel-cancelling our recv) before the disk that owns us is dropped.
    void shutdown() noexcept;

private:
    // One in-flight request's landing pad. The pump moves the reply's bytes in (or flags a fault) and resumes
    // `waiter`; the leg then parses `msg`. Lives in the awaiting leg's coroutine frame (stable across suspension).
    struct reply_slot {
        std::coroutine_handle<> waiter{};
        std::vector< uint8_t > msg;
        bool ready{false};  // reply arrived (msg valid) OR the connection faulted (see failed)
        bool failed{false}; // the connection dropped before the reply -- msg is empty
    };
    // A plain awaitable (NOT tied to an SQE): the pump, not a CQE, resumes it. Same single-slot shape as the mem
    // model's event, keyed by the pending_ map instead of a 1:1 SQE.
    struct reply_awaitable {
        reply_slot* s;
        bool await_ready() const noexcept { return s->ready; }
        void await_suspend(std::coroutine_handle<> h) noexcept { s->waiter = h; }
        void await_resume() const noexcept {}
    };
    // Connect is a once-per-fault operation but QD>1 means many ops reach ensure_ready before it finishes. The
    // FIRST sets connecting_ and does the connect+HELO; the rest park here until it clears, then re-check ready_.
    struct connect_awaitable {
        craft_async_conn* c;
        bool await_ready() const noexcept { return !c->connecting_; }
        void await_suspend(std::coroutine_handle<> h) noexcept { c->connect_waiters_.push_back(h); }
        void await_resume() const noexcept {}
    };
    // Serialize the SEND half: only ONE op's framed message may be on the wire at a time. Concurrent ops share
    // this socket, and a send that goes partial (socket buffer full at high throughput) would otherwise let the
    // next op's bytes land BETWEEN the chunks -- corrupting the stream so the peer resets the connection. This
    // gates only sending; reply-waits still overlap freely (they demux by request_id). Acquire the slot in
    // await_ready when free, else park; release_send() hands it to the next waiter.
    struct send_awaitable {
        craft_async_conn* c;
        bool await_ready() const noexcept {
            if (!c->sending_) {
                c->sending_ = true;
                return true;
            }
            return false;
        }
        void await_suspend(std::coroutine_handle<> h) noexcept { c->send_waiters_.push_back(h); }
        void await_resume() const noexcept {}
    };
    void release_send() noexcept; // hand the send slot to the next parked op, or free it

    // Frame `op_hdr` + `body` as `o` at a fresh request_id, send it on the ring, and await the reply via the
    // pump. Returns the raw reply message bytes (the caller decodes the typed rsp), or a net_error.
    sisl::async::task< std::expected< std::vector< uint8_t >, net_error > >
    round_trip(wire::op o, std::span< uint8_t const > op_hdr, std::span< uint8_t const > body);

    // socket() + async IORING_OP_CONNECT on the ring; sets fd_. Returns 0 on success, <0 (-errno) on failure.
    sisl::async::task< int > ring_connect();
    // Send all of `data`, looping over partial sends via IORING_OP_SEND on the ring. 0 ok, <0 on error.
    sisl::async::task< int > ring_send_all(std::span< uint8_t const > data);
    // The persistent recv pump: one IORING_OP_RECV at a time; parse every complete message out of rx_ and demux
    // it by request_id to the waiting reply_slot. Re-arms until shutdown / a recv fault fails all pending. A
    // stand-alone disk_task (like ublkpp's queue_service): we steal its _coro, resume once, and destroy at
    // shutdown -- it has no continuation and never completes on its own.
    sisl::async::disk_task< int > run_pump();

    ::io_uring_sqe* acquire_sqe() noexcept; // get_sqe, flushing the SQ once if full; null only if truly exhausted
    void drop_fd() noexcept;                // close fd_, mark not-ready
    void fail_all() noexcept;               // resume every pending leg with failed=true

    std::string host_;
    uint16_t port_;
    uint32_t max_tx_;
    ::io_uring* ring_{nullptr};
    int fd_{-1};
    bool ready_{false};
    bool stopping_{false};
    bool connecting_{false};
    std::vector< std::coroutine_handle<> > connect_waiters_; // ops parked while the first connects
    bool sending_{false};
    std::vector< std::coroutine_handle<> > send_waiters_; // ops parked while another holds the send slot

    std::vector< uint8_t > rx_;                           // recv accumulation across message boundaries
    std::vector< uint8_t > recv_scratch_;                 // the pump's per-recv landing buffer
    std::unordered_map< uint16_t, reply_slot* > pending_; // request_id -> the leg awaiting that reply
    uint16_t next_rid_{1};
    std::coroutine_handle<> pump_{}; // the persistent pump frame; destroyed by shutdown
};

} // namespace craft::net
