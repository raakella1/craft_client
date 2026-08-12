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
// (RaftReplica) and speaks its domain types (client_hdr, lsn_pair, io_extent, craft_error). It references
// no homestore SYMBOL, so it still links without the engine (see test_craft_tcp).

#include "net/tcp_server.hpp"

#include <algorithm>
#include <utility>

#include <sisl/logging/logging.h> // server-side r/w trace (base module; visible with -v trace / when a consumer inits logging)

#include "raft/raft_replica.hpp" // the full RaftReplica (+ sisl::sg_list via sisl/fds/buffer.hpp)
#include <craft/status.hpp> // to_wire_status (the shared wire <-> craft_error bridge)
#include "raft/raft_service.hpp"
#include "replica_mgr.hpp"
#include "helper.hpp"

namespace craft::net {

namespace {
template < class T >
std::span< uint8_t const > as_bytes(T const& v) {
    return {reinterpret_cast< uint8_t const* >(&v), sizeof(T)};
}
} // namespace

craft_tcp_server::craft_tcp_server(server_geometry geo, std::string const& server_config_file) : geo_{std::move(geo)} {
    auto ep = replica_endpoint{.id = to_uuid(geo_.member.id), .addr = geo_.member.addr};
    LOGINFO("craft_tcp_server: starting [id={}] config_file='{}'", boost::uuids::to_string(ep.id), server_config_file);
    // net == nullptr: this replica serves exclusively through its srv_* seam (the TCP frontend IS the wire).
    // start replica service and raft service if server_config_file is provided
    if (!server_config_file.empty()) {
        replica_manager::instance()->start_replica_service(server_config_file, ep.id);
        raft_service::instance()->start_raft_service(ep.id);
        LOGINFO("craft_tcp_server: replica_manager + raft_service started [id={}]", boost::uuids::to_string(ep.id));
    } else {
        LOGINFO("craft_tcp_server: no server_config_file given -- running in standalone/cold-path mode [id={}]",
                boost::uuids::to_string(ep.id));
    }
    replica_ = std::make_shared< RaftReplica >(std::move(ep), geo_.lba_size, geo_.max_tx);
}

craft_tcp_server::~craft_tcp_server() = default;

void craft_tcp_server::log_stats() const {
    auto const s = replica_->stats();
    LOGINFO("craft_srv STATS commit_lsn={} last_append_lsn={} journal_slots={} missing={} mapped_blocks={}",
            s.commit_lsn, s.last_append_lsn, s.journal_slots, s.missing_count, s.mapped_blocks);
    if (s.missing_count != 0) {
        std::string sample;
        for (auto const d : s.missing_sample)
            sample += std::to_string(d) + " ";
        LOGWARN("craft_srv MISSING {} slot(s) [{}] -- commit_lsn is PINNED at {} (< last_append {}). Nothing fills "
                "a Missing slot: that is resync, i.e. the PEER PLANE, which does not exist yet. Every read now "
                "walks the journal tail backward from its horizon.",
                s.missing_count, sample, s.commit_lsn, s.last_append_lsn);
    }
}

void craft_tcp_server::serve(craft_conn conn) {
    LOGDEBUG("craft_srv: connection accepted, serving");
    for (;;) {
        auto msg = conn.recv_message(geo_.max_tx);
        if (!msg) {
            LOGDEBUG("craft_srv: connection closed (peer closed, or framing error)");
            return; // peer closed, or a framing error -- done with this connection
        }
        auto parsed = wire::parse_message(*msg, geo_.max_tx);
        if (!parsed) {
            LOGWARN("craft_srv: malformed message, resetting connection");
            return;
        }
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
        case wire::op::create_volume:
            on_create_volume(conn, *parsed);
            break;
        case wire::op::get_rs_commit_lsn:
            on_get_rs_commit_lsn(conn, *parsed);
            break;
        case wire::op::fetch_data:
            on_fetch_data(conn, *parsed);
            break;
        default:
            LOGWARN("craft_srv: unknown op={}, resetting connection", static_cast< int >(parsed->hdr.op));
            return; // a client sends only request ops we serve; anything else resets the connection
        }
    }
}

void craft_tcp_server::on_login(craft_conn& conn, wire::message const& req) {
    // session_active_ is still maintained here, change it once we support multi volume
    std::vector< uint8_t > out;
    session_term_ = ++next_term_; // a fresh session term, established (and fenced) on this connection
    auto const lr = wire::decode< wire::login_req >(req.op_header);
    LOGINFO("craft_srv LOGIN [rid:{}]: client_token={} new_term={}", req.hdr.request_id, lr.client_token,
            session_term_);
    auto result = replica_->srv_establish(lr.volume_id, lr.client_token, session_term_);
    if (!result) {
        LOGERROR("craft_srv LOGIN [rid:{}]: srv_establish failed: {}", req.hdr.request_id, result.error().message());
        session_active_ = false; // establish never happened -- don't hold the slot open
        wire::frame_message(out, wire::op::login_rsp, static_cast< uint8_t >(to_wire_status(result.error())),
                            req.hdr.request_id, {}, {});
        conn.send_all(out);
        return;
    }
    auto const srv_rsp = result.value();
    if (!srv_rsp.leader_hint.is_nil()) {
        LOGINFO("craft_srv LOGIN [rid:{}]: NOT_LEADER, redirecting to {}", req.hdr.request_id,
                boost::uuids::to_string(srv_rsp.leader_hint));
        session_active_ = false;
        wire::frame_message(out, wire::op::login_rsp, static_cast< uint8_t >(wire::status::not_leader),
                            req.hdr.request_id, {}, {});
        conn.send_all(out);
        return;
    }
    wire::login_rsp rsp{};
    rsp.term = session_term_;
    rsp.dlsn = srv_rsp.dLSN;
    rsp.capacity = geo_.capacity;
    rsp.lba_size = geo_.lba_size;
    rsp.max_tx = geo_.max_tx;
    rsp.member_count = static_cast< uint32_t >(srv_rsp.members.size());

    std::vector< uint8_t > body;
    for (auto const& m : srv_rsp.members) {
        wire::member wm{};
        std::memcpy(wm.id.data(), &m.id, 16);
        wm.addr = m.addr;
        wire::put_member(body, wm);
    }

    LOGINFO("craft_srv LOGIN [rid:{}]: SUCCESS term={} dlsn={} members={}", req.hdr.request_id, rsp.term, rsp.dlsn,
            rsp.member_count);
    session_active_ = true;
    wire::frame_message(out, wire::op::login_rsp, static_cast< uint8_t >(wire::status::ok), req.hdr.request_id,
                        as_bytes(rsp), body);
    conn.send_all(out);
}

void craft_tcp_server::on_helo(craft_conn& conn, wire::message const& req) {
    auto const hr = wire::decode< wire::helo_req >(req.op_header);
    wire::status code = wire::status::ok;

    if (bool is_raft_enabled = raft_service::instance()->is_raft_enabled(); !is_raft_enabled) {
        // no raft, follow fake cold path
        auto result = replica_->srv_establish(hr.volume_id, hr.client_token, session_term_);
        if (!result) {
            LOGERROR("craft_srv HELO [rid:{}]: token: {}, term: {}, srv_establish failed: {}", req.hdr.request_id,
                     hr.client_token, hr.term, result.error().message());
            code = to_wire_status(result.error());
        }
    } else if (auto const current = replica_->srv_session_info(hr.volume_id);
               hr.term != current.term || hr.client_token != current.client_token) {
        // Raft is enabled, Fence: HELO must present the term + token of the session the replica already knows about.
        // Only binds this connection if it matches.
        LOGWARN("craft_srv HELO [rid:{}]: FENCED -- presented term={} token={}, current term={} token={}",
                req.hdr.request_id, hr.term, hr.client_token, current.term, current.client_token);
        code = wire::status::stale_term;
    }

    if (code == wire::status::ok) {
        session_term_ = hr.term;
        session_active_ = true;
        LOGDEBUG("craft_srv HELO [rid:{}]: bound connection at term={}", req.hdr.request_id, hr.term);
    }

    std::vector< uint8_t > out;
    wire::frame_message(out, wire::op::helo_rsp, static_cast< uint8_t >(code), req.hdr.request_id, {}, {});
    conn.send_all(out);
}

void craft_tcp_server::on_logout(craft_conn& conn, wire::message const& req) {
    auto st = wire::status::ok;
    if (!session_active_) {
        LOGWARN("craft_srv LOGOUT [rid:{}]: no active session (already fenced)", req.hdr.request_id);
        st = wire::status::stale_term; // no active session to tear down
    } else {
        LOGINFO("craft_srv LOGOUT [rid:{}]: term={}", req.hdr.request_id, session_term_);
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
    LOGDEBUG("craft_srv RS [rid:{}] upto={} status={} empties={}", req.hdr.request_id, rr.upto,
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

void craft_tcp_server::on_create_volume(craft_conn& conn, wire::message const& req) {
    auto const cr = wire::decode< wire::volume_create_req >(req.op_header);
    auto const members = wire::decode_members(req.body, cr.member_count);
    LOGINFO("craft_srv CREATE_VOLUME [rid:{}] member_count={}", req.hdr.request_id, cr.member_count);
    wire::status code = wire::status::ok;
    if (!members) {
        LOGERROR("craft_srv CREATE_VOLUME [rid:{}]: malformed body (member list truncated)", req.hdr.request_id);
        code = wire::status::invalid_argument; // body shorter than member_count implies -- malformed request
    } else {
        std::vector< replica_endpoint > replica_members;
        for (auto const& m : *members) {
            replica_members.emplace_back(replica_endpoint{.id = craft::to_uuid(m.id), .addr = m.addr});
        }

        if (auto const r = replica_->srv_create_volume(cr.volume_id, replica_members); !r) {
            LOGERROR("craft_srv CREATE_VOLUME [rid:{}]: srv_create_volume failed: {}", req.hdr.request_id,
                     r.error().message());
            code = to_wire_status(r.error());
        }
    }

    LOGINFO("craft_srv CREATE_VOLUME [rid:{}] members={} status={}", req.hdr.request_id, cr.member_count,
            static_cast< int >(code));

    std::vector< uint8_t > out;
    wire::frame_message(out, wire::op::create_volume_rsp, static_cast< uint8_t >(code), req.hdr.request_id, {}, {});
    conn.send_all(out);
}

void craft_tcp_server::on_get_rs_commit_lsn(craft_conn& conn, wire::message const& req) {
    auto const gr = wire::decode< wire::get_rs_commit_lsn_req >(req.op_header);
    wire::get_rs_commit_lsn_rsp rsp{-1, -1};
    wire::status code = wire::status::ok;

    auto const r = replica_->srv_get_rs_commit_lsn(gr.term, gr.is_login != 0);
    if (!r) {
        LOGWARN("craft_srv GET_RS_COMMIT_LSN [rid:{}]: failed: {}", req.hdr.request_id, r.error().message());
        code = to_wire_status(r.error());
    } else {
        rsp.commit_lsn = r->commit_lsn;
        rsp.last_append_lsn = r->last_append_lsn;
    }

    LOGINFO("craft_srv GET_RS_COMMIT_LSN [rid:{}] term={} is_login={} status={} commit_lsn={} last_append_lsn={}",
            req.hdr.request_id, gr.term, gr.is_login, static_cast< int >(code), rsp.commit_lsn, rsp.last_append_lsn);

    std::vector< uint8_t > out;
    wire::frame_message(out, wire::op::get_rs_commit_lsn_rsp, static_cast< uint8_t >(code), req.hdr.request_id,
                        as_bytes(rsp), {});
    conn.send_all(out);
}

void craft_tcp_server::on_fetch_data(craft_conn& conn, wire::message const& req) {
    // TODO: Apply max_tx cap and chunking for very large fetch requests.
    auto const fr = wire::decode< wire::fetch_data_req >(req.op_header);
    auto const lsns = wire::decode_lsns(req.body, fr.lsn_count);

    wire::fetch_data_rsp rsp{};
    std::vector< uint8_t > body;
    wire::status code = wire::status::ok;

    if (!lsns) {
        LOGERROR("craft_srv FETCH_DATA [rid:{}]: malformed body (lsn list truncated)", req.hdr.request_id);
        code = wire::status::invalid_argument; // body shorter than lsn_count implies -- malformed request
    } else {
        auto const r = replica_->srv_fetch_data(*lsns);
        if (!r) {
            LOGWARN("craft_srv FETCH_DATA [rid:{}]: failed: {}", req.hdr.request_id, r.error().message());
            code = to_wire_status(r.error());
        } else {
            rsp.slot_count = static_cast< uint32_t >(r->size());
            for (auto const& slot : *r) {
                wire::fetch_slot_desc sd{};
                sd.lsn = slot.lsn;
                sd.lba = slot.lba;
                sd.len = slot.len;
                sd.is_empty = slot.is_empty ? 1 : 0;
                sd.all_zeros = slot.all_zeros ? 1 : 0;
                sd.byte_len = 0;
                if (!slot.is_empty && !slot.all_zeros) { sd.byte_len += slot.data.size; }
                wire::put(body, sd);
            }
            for (auto const& slot : *r) {
                if (slot.is_empty || slot.all_zeros) continue;
                for (auto const& iov : slot.data.iovs) {
                    auto const* p = static_cast< uint8_t const* >(iov.iov_base);
                    body.insert(body.end(), p, p + iov.iov_len);
                }
            }
        }
    }

    LOGDEBUG("craft_srv FETCH_DATA [rid:{}] requested={} status={} returned={}", req.hdr.request_id, fr.lsn_count,
             static_cast< int >(code), rsp.slot_count);

    std::vector< uint8_t > out;
    wire::frame_message(out, wire::op::fetch_data_rsp, static_cast< uint8_t >(code), req.hdr.request_id,
                        {reinterpret_cast< uint8_t const* >(&rsp), sizeof(rsp)}, body);
    conn.send_all(out);
}

} // namespace craft::net