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

// Homeblocks-coupled TU: wraps the reference model (MemReplicaGroup) and speaks its domain types. Uses
// MemTransport's cold path (run_login / run_logout) for the faked-RAFT session orchestration, and the srv_*
// seam for the IO data plane. References no homestore SYMBOL, so it still links without the engine.

#include <craft/net/cluster_server.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include <craft/mem/cluster.hpp> // MemReplicaGroup, MemTransport::run_login/run_logout
#include <craft/mem/replica.hpp> // MemCraftReplica srv_* seam (+ sisl::sg_list via sisl/fds/buffer.hpp)
#include <craft/status.hpp>      // to_wire_status (the shared wire <-> craft_error bridge)

namespace craft::net {

namespace {
template < class T >
std::span< uint8_t const > as_bytes(T const& v) {
    return {reinterpret_cast< uint8_t const* >(&v), sizeof(T)};
}

// Open and immediately close a loopback connection to `port`, to unblock a thread parked in accept() at stop.
void poke_port(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return;
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(port);
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    (void)::connect(fd, reinterpret_cast< sockaddr* >(&a), sizeof(a));
    ::close(fd);
}
} // namespace

struct craft_cluster_server::impl {
    uint32_t page_size;
    uint64_t capacity;
    uint32_t max_tx;
    volume_id_t vol_id{};
    MemReplicaGroup group;
    std::size_t leader_idx{0};

    std::vector< wire::member > members; // resolved {id, "127.0.0.1:port"}
    std::vector< uint16_t > ports;
    std::vector< craft_listener > listeners;
    std::vector< std::thread > acceptors;
    std::vector< std::thread > conns;
    std::mutex conns_mu;
    std::atomic< bool > stopping{false};

    // The one server-wide session (all connections share it). Guarded by sess_mu; the IO path does not touch
    // it -- each connection captures the term at bind (LOGIN/HELO) into a stack local and stamps that, so a
    // later term bump fences the stale connection without a per-IO lock.
    std::mutex sess_mu;
    bool sess_active{false};
    uint64_t sess_term{0};

    impl(uint32_t n, uint32_t ps, uint64_t cap, uint32_t mtx) : page_size{ps}, capacity{cap}, max_tx{mtx} {
        auto* p = reinterpret_cast< uint8_t* >(&vol_id); // a deterministic, recognizable volume id
        for (int i = 0; i < 16; ++i)
            p[i] = static_cast< uint8_t >(0xC0 + i);
        group = make_mem_replica_group(vol_id, n, page_size);
        // Default leader is index 0 (MemReplicaGroup contract); find its index by id for robustness.
        auto const leader = group.net->leader();
        for (std::size_t i = 0; i < group.replicas.size(); ++i) {
            if (group.replicas[i]->id() == leader) leader_idx = i;
        }
    }
    ~impl() { stop(); }

    void start() {
        auto const n = group.replicas.size();
        listeners.reserve(n);
        ports.reserve(n);
        members.reserve(n);
        for (std::size_t i = 0; i < n; ++i) {
            auto lst = craft_listener::bind_listen(0); // ephemeral loopback port
            uint16_t const port = lst->port();
            listeners.push_back(std::move(*lst));
            ports.push_back(port);
            wire::member m{};
            auto const pid = group.replicas[i]->id();
            std::memcpy(m.id.data(), &pid, 16);
            m.addr = "127.0.0.1:" + std::to_string(port);
            members.push_back(std::move(m));
        }
        for (std::size_t i = 0; i < n; ++i)
            acceptors.emplace_back([this, i] { accept_loop(i); });
    }

    void stop() {
        if (stopping.exchange(true)) return;
        for (auto port : ports)
            poke_port(port); // unblock each acceptor's accept()
        for (auto& t : acceptors)
            if (t.joinable()) t.join();
        for (auto& t : conns)
            if (t.joinable()) t.join();
        listeners.clear(); // close listen fds
    }

    // ── fault injection (test): forward to the group's MemTransport; the IO handlers consult its fault_state ──
    void set_replica_up(std::size_t idx, bool up) { group.net->set_up(group.replicas[idx]->id(), up); }
    void force_subquorum(std::vector< std::size_t > keep) {
        std::vector< peer_id_t > ids;
        ids.reserve(keep.size());
        for (auto i : keep)
            ids.push_back(group.replicas[i]->id());
        group.net->force_subquorum(std::move(ids));
    }
    void clear_faults() { group.net->clear_faults(); }
    void set_delay(std::size_t idx, std::chrono::milliseconds d) { group.net->set_delay(group.replicas[idx]->id(), d); }

    // ── test observability: read straight off the mem replica, server-side (no wire, no fault/delay path) ──
    std::size_t journal_slots(std::size_t idx) const { return group.replicas[idx]->stats().journal_slots; }
    uint64_t replica_term(std::size_t idx) const { return group.replicas[idx]->stats().term; }
    bool read_replica(std::size_t idx, int64_t read_lsn, uint64_t addr, uint64_t len, std::vector< uint8_t >& dest) {
        dest.assign(len, 0);
        sisl::sg_list d;
        d.size = len;
        d.iovs.push_back(iovec{dest.data(), static_cast< std::size_t >(len)});
        client_hdr const hdr{sess_term, /*commit*/ -1, /*all_committed*/ -1};
        return group.replicas[idx]->srv_read(hdr, read_lsn, addr, len, d).has_value();
    }

    void accept_loop(std::size_t idx) {
        for (;;) {
            auto c = listeners[idx].accept();
            if (stopping.load(std::memory_order_acquire)) return; // the poke, or a real stop -- drop it
            if (!c) continue;                                     // transient accept error while running
            std::lock_guard< std::mutex > g{conns_mu};
            conns.emplace_back([this, cc = std::move(*c), idx]() mutable { serve(std::move(cc), idx); });
        }
    }

    // One connection, until it closes. `idx` is the replica this connection's port belongs to.
    void serve(craft_conn conn, std::size_t idx) {
        auto* replica = group.replicas[idx].get();
        uint64_t conn_term = 0; // captured at bind (LOGIN/HELO); stamped on every IO
        bool bound = false;

        for (;;) {
            auto msg = conn.recv_message(max_tx);
            if (!msg) return;
            auto parsed = wire::parse_message(*msg, max_tx);
            if (!parsed) return;
            uint16_t const rid = parsed->hdr.request_id;
            std::vector< uint8_t > out;

            switch (static_cast< wire::op >(parsed->hdr.op)) {
            case wire::op::login:
                on_login(*parsed, idx, replica, rid, conn_term, bound, out);
                break;
            case wire::op::helo:
                on_helo(*parsed, rid, conn_term, bound, out);
                break;
            case wire::op::write:
                on_write(*parsed, replica, rid, conn_term, bound, out);
                break;
            case wire::op::read:
                on_read(*parsed, replica, rid, conn_term, bound, out);
                break;
            case wire::op::keepalive:
                on_keep_alive(*parsed, replica, rid, conn_term, bound, out);
                break;
            case wire::op::resolve:
                on_resolve(*parsed, replica, rid, conn_term, bound, out);
                break;
            case wire::op::logout:
                on_logout(replica, rid, conn_term, bound, out);
                break;
            default:
                return; // a client sends only request ops we serve; anything else resets the connection
            }
            if (!out.empty() && !conn.send_all(out)) return;
        }
    }

    // LOGIN: run the faked-RAFT orchestration on this connection's replica. If it is the leader, the term is
    // established set-wide; a follower returns the NOT_LEADER redirect (term 0 + leader_hint).
    void on_login(wire::message const& req, std::size_t idx, MemCraftReplica* replica, uint16_t rid,
                  uint64_t& conn_term, bool& bound, std::vector< uint8_t >& out) {
        (void)idx;
        auto const lreq = wire::decode< wire::login_req >(req.op_header);
        // LOGIN names the volume; this server fronts exactly one, so a mismatched id is a caller error
        // (a multi-volume server would route by it instead).
        if (std::memcmp(lreq.volume_id.data(), &vol_id, 16) != 0) {
            wire::login_rsp rsp{};
            wire::frame_message(out, wire::op::login_rsp, static_cast< uint8_t >(wire::status::invalid_argument), rid,
                                as_bytes(rsp), {});
            return;
        }
        std::lock_guard< std::mutex > g{sess_mu};
        auto const lr = group.net->run_login(replica, lreq.client_token);
        if (!lr) {
            wire::login_rsp rsp{};
            wire::frame_message(out, wire::op::login_rsp, static_cast< uint8_t >(to_wire_status(lr.error())), rid,
                                as_bytes(rsp), {});
            return;
        }
        wire::login_rsp rsp{};
        rsp.term = lr->term;
        rsp.dlsn = lr->dLSN;
        rsp.capacity = capacity;
        rsp.lba_size = page_size;
        rsp.max_tx = max_tx;
        if (lr->term == 0) {
            std::memcpy(rsp.leader_hint.data(), &lr->leader_hint, 16); // redirect: point at the leader
            wire::frame_message(out, wire::op::login_rsp, static_cast< uint8_t >(wire::status::ok), rid, as_bytes(rsp),
                                {});
            return;
        }
        sess_active = true;
        sess_term = lr->term;
        conn_term = lr->term;
        bound = true;
        rsp.member_count = static_cast< uint32_t >(members.size());
        std::vector< uint8_t > body;
        for (auto const& m : members)
            wire::put_member(body, m);
        wire::frame_message(out, wire::op::login_rsp, static_cast< uint8_t >(wire::status::ok), rid, as_bytes(rsp),
                            body);
    }

    // HELO: join the already-established session. Binds iff the presented term matches the live session.
    void on_helo(wire::message const& req, uint16_t rid, uint64_t& conn_term, bool& bound,
                 std::vector< uint8_t >& out) {
        auto const hreq = wire::decode< wire::helo_req >(req.op_header);
        auto st = wire::status::stale_term;
        {
            std::lock_guard< std::mutex > g{sess_mu};
            if (sess_active && hreq.term == sess_term) {
                conn_term = sess_term;
                bound = true;
                st = wire::status::ok;
            }
        }
        wire::frame_message(out, wire::op::helo_rsp, static_cast< uint8_t >(st), rid, {}, {});
    }

    void on_write(wire::message const& req, MemCraftReplica* replica, uint16_t rid, uint64_t conn_term, bool bound,
                  std::vector< uint8_t >& out) {
        auto const wr = wire::decode< wire::write_req >(req.op_header);
        wire::status code = wire::status::ok;
        if (!bound) {
            code = wire::status::stale_term;
        } else if (!group.net->is_up(replica->id()) || !group.net->write_allowed(replica->id())) {
            code = wire::status::replica_down; // injected fault: down / excluded from the forced sub-quorum
        } else {
            // Straggler: this leg is up but slow. Sleep BEFORE applying so the write is delivered+applied late
            // (mirrors the mem model's delayed delivery) -- the client times out and acks at quorum without us,
            // yet the write still lands here once the delay elapses. This serve thread blocks; the peers do not.
            if (auto const d = group.net->delay_for(replica->id()); d.count() > 0) std::this_thread::sleep_for(d);
            std::shared_ptr< std::vector< uint8_t > > bytes;
            if (!req.body.empty()) bytes = std::make_shared< std::vector< uint8_t > >(req.body.begin(), req.body.end());
            client_hdr const hdr{conn_term, wr.hdr.commit_lsn, wr.hdr.all_committed_lsn};
            auto const r = replica->srv_write(hdr, wr.dlsn, wr.addr, wr.len, std::move(bytes));
            if (!r) {
                code = to_wire_status(r.error());
            } else {
                wire::write_rsp rsp{r->commit_lsn, r->last_append_lsn}; // piggybacked with the append
                wire::frame_message(out, wire::op::write_rsp, static_cast< uint8_t >(code), rid, as_bytes(rsp), {});
                return;
            }
        }
        auto const lsns = replica->srv_lsns();
        wire::write_rsp rsp{lsns.commit_lsn, lsns.last_append_lsn};
        wire::frame_message(out, wire::op::write_rsp, static_cast< uint8_t >(code), rid, as_bytes(rsp), {});
    }

    void on_read(wire::message const& req, MemCraftReplica* replica, uint16_t rid, uint64_t conn_term, bool bound,
                 std::vector< uint8_t >& out) {
        auto const rr = wire::decode< wire::read_req >(req.op_header);
        std::vector< uint8_t > body;
        wire::read_rsp rsp{};
        wire::status code = wire::status::ok;

        if (!bound) {
            code = wire::status::stale_term;
        } else if (!group.net->is_up(replica->id())) {
            code = wire::status::replica_down; // injected fault: this replica is down -> the client fails over
        } else {
            if (auto const d = group.net->delay_for(replica->id()); d.count() > 0) std::this_thread::sleep_for(d);
            std::vector< uint8_t > dest_buf(rr.len);
            sisl::sg_list dest;
            dest.size = rr.len;
            dest.iovs.push_back(iovec{dest_buf.data(), static_cast< std::size_t >(rr.len)});
            client_hdr const hdr{conn_term, rr.hdr.commit_lsn, rr.hdr.all_committed_lsn};
            auto const r = replica->srv_read(hdr, rr.read_lsn, rr.addr, rr.len, dest);
            if (!r) {
                code = to_wire_status(r.error());
                auto const lsns = replica->srv_lsns();
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
        wire::frame_message(out, wire::op::read_rsp, static_cast< uint8_t >(code), rid, as_bytes(rsp), body);
    }

    void on_keep_alive(wire::message const& req, MemCraftReplica* replica, uint16_t rid, uint64_t conn_term, bool bound,
                       std::vector< uint8_t >& out) {
        auto const ka = wire::decode< wire::keepalive_req >(req.op_header);
        wire::keepalive_rsp rsp{-1, -1};
        wire::status code = wire::status::ok;
        if (!bound) {
            code = wire::status::stale_term;
        } else if (!group.net->is_up(replica->id())) {
            code = wire::status::replica_down; // injected fault: down -> its synced_ stays put, pinning the floor
        } else {
            if (auto const d = group.net->delay_for(replica->id()); d.count() > 0) std::this_thread::sleep_for(d);
            client_hdr const hdr{conn_term, ka.hdr.commit_lsn, ka.hdr.all_committed_lsn};
            auto const r = replica->srv_keep_alive(hdr);
            if (!r)
                code = to_wire_status(r.error());
            else {
                rsp.commit_lsn = r->commit_lsn;
                rsp.last_append_lsn = r->last_append_lsn;
            }
        }
        wire::frame_message(out, wire::op::keepalive_rsp, static_cast< uint8_t >(code), rid, as_bytes(rsp), {});
    }

    // RESOLVE: the client-requested resolution round, run set-wide through the group's cold path (leader-only;
    // NOT_LEADER on a follower's port, exactly like LOGOUT).
    void on_resolve(wire::message const& req, MemCraftReplica* replica, uint16_t rid, uint64_t conn_term, bool bound,
                    std::vector< uint8_t >& out) {
        auto const rq = wire::decode< wire::resolve_req >(req.op_header);
        wire::resolve_rsp rsp{};
        std::vector< uint8_t > body;
        wire::status code = wire::status::ok;
        if (!bound) {
            code = wire::status::stale_term;
        } else if (!group.net->is_up(replica->id())) {
            code = wire::status::replica_down;
        } else {
            auto const r = group.net->run_resolution(replica, conn_term, rq.upto);
            if (!r) {
                code = to_wire_status(r.error());
            } else {
                rsp.resolved_upto = r->resolved_upto;
                rsp.empty_count = static_cast< uint32_t >(r->empty_slots.size());
                for (auto const d : r->empty_slots)
                    wire::put(body, d);
            }
        }
        wire::frame_message(out, wire::op::resolve_rsp, static_cast< uint8_t >(code), rid, as_bytes(rsp), body);
    }

    // LOGOUT: leader-only teardown of the whole session (run_logout returns NOT_LEADER on a follower).
    void on_logout(MemCraftReplica* replica, uint16_t rid, uint64_t conn_term, bool& bound,
                   std::vector< uint8_t >& out) {
        auto st = wire::status::ok;
        if (!bound) {
            st = wire::status::stale_term;
        } else {
            std::lock_guard< std::mutex > g{sess_mu};
            auto const s = group.net->run_logout(replica, conn_term);
            if (!s)
                st = to_wire_status(s.error());
            else {
                sess_active = false;
                sess_term = 0;
                bound = false;
            }
        }
        wire::frame_message(out, wire::op::logout_rsp, static_cast< uint8_t >(st), rid, {}, {});
    }
};

craft_cluster_server::craft_cluster_server(uint32_t n, uint32_t page_size, uint64_t capacity, uint32_t max_tx) :
        p_{std::make_unique< impl >(n, page_size, capacity, max_tx)} {}
craft_cluster_server::~craft_cluster_server() = default;
craft_cluster_server::craft_cluster_server(craft_cluster_server&&) noexcept = default;
craft_cluster_server& craft_cluster_server::operator=(craft_cluster_server&&) noexcept = default;

void craft_cluster_server::start() { p_->start(); }
void craft_cluster_server::stop() { p_->stop(); }
std::vector< wire::member > craft_cluster_server::members() const { return p_->members; }
uint16_t craft_cluster_server::port(std::size_t idx) const { return p_->ports[idx]; }
std::size_t craft_cluster_server::leader_index() const { return p_->leader_idx; }
std::array< uint8_t, 16 > craft_cluster_server::volume_id() const {
    std::array< uint8_t, 16 > v{};
    std::memcpy(v.data(), &p_->vol_id, 16);
    return v;
}
void craft_cluster_server::set_replica_up(std::size_t idx, bool up) { p_->set_replica_up(idx, up); }
void craft_cluster_server::force_subquorum(std::vector< std::size_t > keep) { p_->force_subquorum(std::move(keep)); }
void craft_cluster_server::clear_faults() { p_->clear_faults(); }
void craft_cluster_server::set_delay(std::size_t idx, std::chrono::milliseconds d) { p_->set_delay(idx, d); }
std::size_t craft_cluster_server::journal_slots(std::size_t idx) const { return p_->journal_slots(idx); }
uint64_t craft_cluster_server::replica_term(std::size_t idx) const { return p_->replica_term(idx); }
bool craft_cluster_server::read_replica(std::size_t idx, int64_t read_lsn, uint64_t addr, uint64_t len,
                                        std::vector< uint8_t >& dest) const {
    return p_->read_replica(idx, read_lsn, addr, len, dest);
}

} // namespace craft::net
