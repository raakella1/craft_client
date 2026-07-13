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

// Wire-only: this TU includes only its own header (-> craft_conn + craft_wire) and the standard library. No
// homeblocks/homestore/sisl symbol enters, so the reference client links standalone.

#include "net/wire_client.hpp"

#include <cstring>
#include <utility>

namespace craft::net {

namespace {
// Bound the recv at the framed BODY max for the default payload at the smallest block size (512 B) -- the worst
// case for the extent-descriptor table a read reply lays down on top of the data -- so a full-payload read parses
// regardless of the volume's lba. max_tx itself (the payload) is the clean 512 KiB; the transport carries more.
// This reference client assumes the default payload (a real volume conveys its own via login).
constexpr uint32_t k_max_tx = wire::framed_body_max(wire::k_default_max_tx, 512);

template < class T >
std::span< uint8_t const > as_bytes(T const& v) {
    return {reinterpret_cast< uint8_t const* >(&v), sizeof(T)};
}
} // namespace

std::expected< wire_client, net_error > wire_client::connect(std::string const& host, uint16_t port) {
    auto c = craft_conn::connect(host, port);
    if (!c) return std::unexpected(c.error());
    wire_client cli;
    cli.conn_ = std::move(*c);
    return cli;
}

std::expected< login_result, net_error > wire_client::login(std::array< uint8_t, 16 > const& volume_id,
                                                            uint64_t client_token) {
    wire::login_req req{volume_id, client_token};
    std::vector< uint8_t > out;
    wire::frame_message(out, wire::op::login, 0, next_rid_++, as_bytes(req), {});
    if (!conn_.send_all(out)) return std::unexpected(net_error::send);

    auto msg = conn_.recv_message(k_max_tx, op_timeout_);
    if (!msg) return std::unexpected(msg.error());
    auto parsed = wire::parse_message(*msg, k_max_tx);
    if (!parsed || parsed->hdr.op != static_cast< uint8_t >(wire::op::login_rsp))
        return std::unexpected(net_error::malformed);

    auto const lr = wire::decode< wire::login_rsp >(parsed->op_header);
    auto members = wire::decode_members(parsed->body, lr.member_count);
    if (!members) return std::unexpected(net_error::malformed);

    login_result res{};
    res.term = lr.term;
    res.dlsn = lr.dlsn;
    res.capacity = lr.capacity;
    res.lba_size = lr.lba_size;
    res.max_tx = lr.max_tx;
    res.leader_hint = lr.leader_hint;
    res.members = std::move(*members);
    term_ = lr.term;
    return res;
}

std::expected< wire::status, net_error > wire_client::helo(std::array< uint8_t, 16 > const& volume_id,
                                                           uint64_t client_token, uint64_t term) {
    wire::helo_req req{volume_id, client_token, term};
    std::vector< uint8_t > out;
    wire::frame_message(out, wire::op::helo, 0, next_rid_++, as_bytes(req), {});
    if (!conn_.send_all(out)) return std::unexpected(net_error::send);

    auto msg = conn_.recv_message(k_max_tx, op_timeout_);
    if (!msg) return std::unexpected(msg.error());
    auto parsed = wire::parse_message(*msg, k_max_tx);
    if (!parsed || parsed->hdr.op != static_cast< uint8_t >(wire::op::helo_rsp))
        return std::unexpected(net_error::malformed);
    auto const st = static_cast< wire::status >(parsed->hdr.status);
    if (st == wire::status::ok) term_ = term; // this connection is now bound to the session
    return st;
}

std::expected< wire::status, net_error > wire_client::logout() {
    wire::logout_req req{{.commit_lsn = -1, .all_committed_lsn = -1}};
    std::vector< uint8_t > out;
    wire::frame_message(out, wire::op::logout, 0, next_rid_++, as_bytes(req), {});
    if (!conn_.send_all(out)) return std::unexpected(net_error::send);

    auto msg = conn_.recv_message(k_max_tx, op_timeout_);
    if (!msg) return std::unexpected(msg.error());
    auto parsed = wire::parse_message(*msg, k_max_tx);
    if (!parsed || parsed->hdr.op != static_cast< uint8_t >(wire::op::logout_rsp))
        return std::unexpected(net_error::malformed);
    return static_cast< wire::status >(parsed->hdr.status);
}

std::expected< lsn_reply, net_error > wire_client::write(int64_t dlsn, uint64_t addr, uint64_t len,
                                                         std::span< uint8_t const > data, int64_t commit_lsn,
                                                         int64_t all_committed_lsn) {
    wire::write_req req{{commit_lsn, all_committed_lsn}, dlsn, addr, len};
    std::vector< uint8_t > out;
    wire::frame_message(out, wire::op::write, 0, next_rid_++, as_bytes(req), data); // data is the body
    if (!conn_.send_all(out)) return std::unexpected(net_error::send);

    auto msg = conn_.recv_message(k_max_tx, op_timeout_);
    if (!msg) return std::unexpected(msg.error());
    auto parsed = wire::parse_message(*msg, k_max_tx);
    if (!parsed || parsed->hdr.op != static_cast< uint8_t >(wire::op::write_rsp))
        return std::unexpected(net_error::malformed);

    auto const wr = wire::decode< wire::write_rsp >(parsed->op_header);
    return lsn_reply{static_cast< wire::status >(parsed->hdr.status), wr.commit_lsn, wr.last_append_lsn};
}

std::expected< read_reply, net_error > wire_client::read(int64_t read_lsn, uint64_t addr, uint64_t len,
                                                         std::span< uint8_t > dest, int64_t commit_lsn,
                                                         int64_t all_committed_lsn) {
    if (dest.size() < len) return std::unexpected(net_error::invalid_argument);

    wire::read_req req{{commit_lsn, all_committed_lsn}, read_lsn, addr, len};
    std::vector< uint8_t > out;
    wire::frame_message(out, wire::op::read, 0, next_rid_++, as_bytes(req), {});
    if (!conn_.send_all(out)) return std::unexpected(net_error::send);

    auto msg = conn_.recv_message(k_max_tx, op_timeout_);
    if (!msg) return std::unexpected(msg.error());
    auto parsed = wire::parse_message(*msg, k_max_tx);
    if (!parsed || parsed->hdr.op != static_cast< uint8_t >(wire::op::read_rsp))
        return std::unexpected(net_error::malformed);

    auto const rr = wire::decode< wire::read_rsp >(parsed->op_header);
    read_reply reply{static_cast< wire::status >(parsed->hdr.status), rr.commit_lsn, rr.last_append_lsn, {}};
    if (reply.status != wire::status::ok) return reply; // no body on a non-ok reply

    auto extents = wire::decode_extents(parsed->body, rr.extent_count);
    if (!extents) return std::unexpected(net_error::malformed); // truncated extent table
    auto const plan = wire::plan_scatter(*extents, addr, len);
    if (!plan.ok) return std::unexpected(net_error::malformed); // an extent fell outside the request range

    // The packed non-hole data follows the extent table; scatter it into dest in wire order, zero the holes.
    std::span< uint8_t const > const packed = parsed->body.subspan(rr.extent_count * sizeof(wire::extent_desc));
    std::size_t src = 0;
    for (auto const& [dst_off, n] : plan.data) {
        if (src + n > packed.size()) return std::unexpected(net_error::malformed); // body shorter than claimed
        std::memcpy(dest.data() + dst_off, packed.data() + src, n);
        src += n;
    }
    for (auto const& [dst_off, n] : plan.holes)
        std::memset(dest.data() + dst_off, 0, n);

    reply.extents = std::move(*extents);
    return reply;
}

std::expected< lsn_reply, net_error > wire_client::keep_alive(int64_t commit_lsn, int64_t all_committed_lsn) {
    wire::keepalive_req req{{commit_lsn, all_committed_lsn}};
    std::vector< uint8_t > out;
    wire::frame_message(out, wire::op::keepalive, 0, next_rid_++, as_bytes(req), {});
    if (!conn_.send_all(out)) return std::unexpected(net_error::send);

    auto msg = conn_.recv_message(k_max_tx, op_timeout_);
    if (!msg) return std::unexpected(msg.error());
    auto parsed = wire::parse_message(*msg, k_max_tx);
    if (!parsed || parsed->hdr.op != static_cast< uint8_t >(wire::op::keepalive_rsp))
        return std::unexpected(net_error::malformed);

    auto const ka = wire::decode< wire::keepalive_rsp >(parsed->op_header);
    return lsn_reply{static_cast< wire::status >(parsed->hdr.status), ka.commit_lsn, ka.last_append_lsn};
}

std::expected< resolve_reply, net_error > wire_client::resolve(int64_t upto, int64_t commit_lsn,
                                                               int64_t all_committed_lsn) {
    wire::resolve_req req{{commit_lsn, all_committed_lsn}, upto};
    std::vector< uint8_t > out;
    wire::frame_message(out, wire::op::resolve, 0, next_rid_++, as_bytes(req), {});
    if (!conn_.send_all(out)) return std::unexpected(net_error::send);

    auto msg = conn_.recv_message(k_max_tx, op_timeout_);
    if (!msg) return std::unexpected(msg.error());
    auto parsed = wire::parse_message(*msg, k_max_tx);
    if (!parsed || parsed->hdr.op != static_cast< uint8_t >(wire::op::resolve_rsp))
        return std::unexpected(net_error::malformed);

    auto const rr = wire::decode< wire::resolve_rsp >(parsed->op_header);
    resolve_reply reply{static_cast< wire::status >(parsed->hdr.status), rr.resolved_upto, {}};
    if (reply.status != wire::status::ok) return reply; // no body on a non-ok reply
    auto empties = wire::decode_lsns(parsed->body, rr.empty_count);
    if (!empties) return std::unexpected(net_error::malformed); // truncated verdict list
    reply.empty_slots = std::move(*empties);
    return reply;
}

} // namespace craft::net
