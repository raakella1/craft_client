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

// This is the reference TCP server: it backs the server with the reference model
// (MemCraftReplica) and speaks its domain types (client_hdr, lsn_pair, io_extent, craft_error). It references
// no homestore SYMBOL, so it still links without the engine (see test_craft_tcp).

#include "net/tcp_server.hpp"

#include <algorithm>
#include <utility>

#include <sisl/logging/logging.h> // server-side r/w trace (base module; visible with -v trace / when a consumer inits logging)

#include "mem/replica.hpp"  // the full MemCraftReplica (+ sisl::sg_list via sisl/fds/buffer.hpp)
#include <craft/status.hpp> // to_wire_status (the shared wire <-> craft_error bridge)

namespace craft::net {

namespace {
template < class T >
std::span< uint8_t const > as_bytes(T const& v) {
    return {reinterpret_cast< uint8_t const* >(&v), sizeof(T)};
}
} // namespace

craft_tcp_server::craft_tcp_server(server_geometry geo) : geo_{std::move(geo)} {
    replica_endpoint ep;
    if (!geo_.members.empty()) {
        std::copy(geo_.members[0].id.begin(), geo_.members[0].id.end(), ep.id.begin()); // wire id[16] -> uuid
        ep.addr = geo_.members[0].addr;
    }
    // net == nullptr: this replica serves exclusively through its srv_* seam (the TCP frontend IS the wire).
    replica_ = std::make_shared< MemCraftReplica >(std::move(ep), geo_.lba_size, nullptr);
}

craft_tcp_server::~craft_tcp_server() = default;

void craft_tcp_server::serve(craft_conn conn) {
    for (;;) {
        auto msg = conn.recv_message(geo_.max_tx);
        if (!msg) return; // peer closed, or a framing error -- done with this connection
        auto parsed = wire::parse_message(*msg, geo_.max_tx);
        if (!parsed) return;
        switch (static_cast< wire::op >(parsed->hdr.op)) {
        case wire::op::login:
            on_login(conn, *parsed);
            break;
        case wire::op::helo:
            on_helo(conn, *parsed);
            break;
        case wire::op::write:
            on_write(conn, *parsed);
            break;
        case wire::op::read:
            on_read(conn, *parsed);
            break;
        case wire::op::keepalive:
            on_keep_alive(conn, *parsed);
            break;
        case wire::op::resolve:
            on_resolve(conn, *parsed);
            break;
        case wire::op::logout:
            on_logout(conn, *parsed);
            break;
        default:
            return; // a client sends only request ops we serve; anything else resets the connection
        }
    }
}

void craft_tcp_server::on_login(craft_conn& conn, wire::message const& req) {
    // login_req names the volume; this standalone reference server fronts exactly one, so any presented id is
    // accepted (like its fake HELO cold path). A multi-volume server routes the session-establishment by it.
    auto const lr = wire::decode< wire::login_req >(req.op_header);
    session_term_ = ++next_term_; // a fresh session term, established (and fenced) on this connection
    session_active_ = true;
    replica_->srv_establish(lr.client_token, session_term_);

    auto const lsns = replica_->srv_lsns();
    wire::login_rsp rsp{};
    rsp.term = session_term_;
    rsp.dlsn = lsns.last_append_lsn + 1; // the next dLSN for new IO
    rsp.capacity = geo_.capacity;
    rsp.lba_size = geo_.lba_size;
    rsp.max_tx = geo_.max_tx;
    rsp.member_count = static_cast< uint32_t >(geo_.members.size());

    std::vector< uint8_t > body;
    for (auto const& m : geo_.members)
        wire::put_member(body, m);
    std::vector< uint8_t > out;
    wire::frame_message(out, wire::op::login_rsp, static_cast< uint8_t >(wire::status::ok), req.hdr.request_id,
                        as_bytes(rsp), body);
    conn.send_all(out);
}

void craft_tcp_server::on_helo(craft_conn& conn, wire::message const& req) {
    auto const hr = wire::decode< wire::helo_req >(req.op_header);
    // FAKE cold path (until peer-to-peer replica comms): a follower this client never logged into ADOPTS the
    // presented (leader's) session term and establishes locally. A fresh cluster starts empty (dLSN -1 on every
    // replica), so no cross-replica RS-commit-lsn sync is needed yet; term-fencing is what HELO must restore so
    // subsequent IO at this term is accepted. Re-HELO after a term bump just re-establishes at the new term.
    session_term_ = hr.term;
    session_active_ = true;
    replica_->srv_establish(hr.client_token, hr.term);
    std::vector< uint8_t > out;
    wire::frame_message(out, wire::op::helo_rsp, static_cast< uint8_t >(wire::status::ok), req.hdr.request_id, {}, {});
    conn.send_all(out);
}

void craft_tcp_server::on_logout(craft_conn& conn, wire::message const& req) {
    auto st = wire::status::ok;
    if (!session_active_)
        st = wire::status::stale_term; // no active session to tear down
    else {
        session_active_ = false;
        replica_->srv_end(); // clear the replica's term; later IO with the old term now fences STALE_TERM
    }
    std::vector< uint8_t > out;
    wire::frame_message(out, wire::op::logout_rsp, static_cast< uint8_t >(st), req.hdr.request_id, {}, {});
    conn.send_all(out);
}

void craft_tcp_server::on_write(craft_conn& conn, wire::message const& req) {
    auto const wr = wire::decode< wire::write_req >(req.op_header);
    wire::status code = wire::status::ok;
    lsn_pair lsns{};
    if (!session_active_) {
        code = wire::status::stale_term; // pre-LOGIN / post-LOGOUT IO is fenced
        lsns = replica_->srv_lsns();
    } else {
        // The body IS the payload: empty => zero write; else the transport now owns these bytes, so copy
        // them into a buffer the journal slot adopts (the server-side equivalent of the model's one copy).
        std::shared_ptr< std::vector< uint8_t > > bytes;
        if (!req.body.empty()) bytes = std::make_shared< std::vector< uint8_t > >(req.body.begin(), req.body.end());
        client_hdr const hdr{session_term_, wr.hdr.commit_lsn, wr.hdr.all_committed_lsn};
        auto const r = replica_->srv_write(hdr, wr.dlsn, wr.addr, wr.len, std::move(bytes));
        if (!r) {
            code = to_wire_status(r.error());
            lsns = replica_->srv_lsns();
        } else {
            lsns = *r; // the ack's piggybacked watermarks, snapshotted atomically with the append
        }
    }
    LOGTRACE("craft_srv WR [rid:{}] dlsn={} addr={} len={} status={} commit_lsn={}", req.hdr.request_id, wr.dlsn,
             wr.addr, wr.len, static_cast< int >(code), lsns.commit_lsn);
    wire::write_rsp rsp{lsns.commit_lsn, lsns.last_append_lsn};
    std::vector< uint8_t > out;
    wire::frame_message(out, wire::op::write_rsp, static_cast< uint8_t >(code), req.hdr.request_id, as_bytes(rsp), {});
    conn.send_all(out);
}

void craft_tcp_server::on_read(craft_conn& conn, wire::message const& req) {
    auto const rr = wire::decode< wire::read_req >(req.op_header);
    client_hdr const hdr{session_term_, rr.hdr.commit_lsn, rr.hdr.all_committed_lsn};

    std::vector< uint8_t > body;
    wire::read_rsp rsp{};
    wire::status code = wire::status::ok;

    if (!session_active_) {
        code = wire::status::stale_term;
    } else {
        // do_read fills dest (data bytes + zero-filled holes) and returns the sparse layout; we send only the
        // extent table + the non-hole bytes (the client zero-fills the holes locally from the layout).
        std::vector< uint8_t > dest_buf(rr.len);
        sisl::sg_list dest;
        dest.size = rr.len;
        dest.iovs.push_back(iovec{dest_buf.data(), static_cast< std::size_t >(rr.len)});
        auto const r = replica_->srv_read(hdr, rr.read_lsn, rr.addr, rr.len, dest);
        if (!r) {
            code = to_wire_status(r.error());
            auto const lsns = replica_->srv_lsns();
            rsp.commit_lsn = lsns.commit_lsn;
            rsp.last_append_lsn = lsns.last_append_lsn;
        } else {
            rsp.commit_lsn = r->lsns.commit_lsn; // piggybacked, snapshotted atomically with the read
            rsp.last_append_lsn = r->lsns.last_append_lsn;
            auto const& layout = r->extents;
            rsp.extent_count = static_cast< uint32_t >(layout.size());
            for (auto const& e : layout) {
                wire::extent_desc ed{};
                ed.addr = e.addr;
                ed.len = e.len;
                ed.hole = e.hole ? 1 : 0;
                wire::put(body, ed);
            }
            for (auto const& e : layout) {
                if (e.hole) continue;
                auto const off = static_cast< std::size_t >(e.addr - rr.addr);
                body.insert(body.end(), dest_buf.data() + off, dest_buf.data() + off + e.len);
            }
        }
    }
    LOGTRACE("craft_srv RD [rid:{}] read_lsn={} addr={} len={} status={} extents={} body={}B", req.hdr.request_id,
             rr.read_lsn, rr.addr, rr.len, static_cast< int >(code), rsp.extent_count, body.size());
    std::vector< uint8_t > out;
    wire::frame_message(out, wire::op::read_rsp, static_cast< uint8_t >(code), req.hdr.request_id, as_bytes(rsp), body);
    conn.send_all(out);
}

void craft_tcp_server::on_resolve(craft_conn& conn, wire::message const& req) {
    auto const rr = wire::decode< wire::resolve_req >(req.op_header);
    wire::resolve_rsp rsp{};
    std::vector< uint8_t > body;
    wire::status code = wire::status::ok;

    if (!session_active_) {
        code = wire::status::stale_term;
    } else {
        client_hdr const hdr{session_term_, rr.hdr.commit_lsn, rr.hdr.all_committed_lsn};
        auto const r = replica_->srv_resolve(hdr, rr.upto); // N=1 semantics: every hole <= upto is Empty
        if (!r) {
            code = to_wire_status(r.error());
        } else {
            rsp.resolved_upto = r->resolved_upto;
            rsp.empty_count = static_cast< uint32_t >(r->empty_slots.size());
            for (auto const d : r->empty_slots)
                wire::put(body, d);
        }
    }
    LOGTRACE("craft_srv RS [rid:{}] upto={} status={} empties={}", req.hdr.request_id, rr.upto,
             static_cast< int >(code), rsp.empty_count);
    std::vector< uint8_t > out;
    wire::frame_message(out, wire::op::resolve_rsp, static_cast< uint8_t >(code), req.hdr.request_id, as_bytes(rsp),
                        body);
    conn.send_all(out);
}

void craft_tcp_server::on_keep_alive(craft_conn& conn, wire::message const& req) {
    auto const ka = wire::decode< wire::keepalive_req >(req.op_header);
    wire::keepalive_rsp rsp{-1, -1};
    wire::status code = wire::status::ok;

    if (!session_active_) {
        code = wire::status::stale_term;
    } else {
        client_hdr const hdr{session_term_, ka.hdr.commit_lsn, ka.hdr.all_committed_lsn};
        auto const r = replica_->srv_keep_alive(hdr);
        if (!r) {
            code = to_wire_status(r.error());
        } else {
            rsp.commit_lsn = r->commit_lsn;
            rsp.last_append_lsn = r->last_append_lsn;
        }
    }
    std::vector< uint8_t > out;
    wire::frame_message(out, wire::op::keepalive_rsp, static_cast< uint8_t >(code), req.hdr.request_id, as_bytes(rsp),
                        {});
    conn.send_all(out);
}

} // namespace craft::net
