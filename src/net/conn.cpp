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

#include <craft/net/conn.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <optional>
#include <utility>

namespace craft::net {

namespace {
constexpr unsigned k_ring_entries = 8; // one op in flight at a time (blocking submit-and-wait)
constexpr std::size_t k_recv_chunk = 16 * 1024;
} // namespace

// ── craft_conn ──

craft_conn::~craft_conn() { close_all(); }

craft_conn::craft_conn(craft_conn&& o) noexcept :
        fd_(o.fd_), ring_(o.ring_), ring_ready_(o.ring_ready_), rx_(std::move(o.rx_)) {
    // io_uring's pointers reference external mmap'd memory, so copying the struct and disarming the source
    // (so only we exit the ring) is a valid move.
    o.fd_ = -1;
    o.ring_ready_ = false;
}

craft_conn& craft_conn::operator=(craft_conn&& o) noexcept {
    if (this != &o) {
        close_all();
        fd_ = o.fd_;
        ring_ = o.ring_;
        ring_ready_ = o.ring_ready_;
        rx_ = std::move(o.rx_);
        o.fd_ = -1;
        o.ring_ready_ = false;
    }
    return *this;
}

void craft_conn::close_all() noexcept {
    if (ring_ready_) {
        io_uring_queue_exit(&ring_);
        ring_ready_ = false;
    }
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

std::expected< craft_conn, net_error > craft_conn::adopt(int fd) {
    craft_conn c;
    c.fd_ = fd;
    if (io_uring_queue_init(k_ring_entries, &c.ring_, 0) < 0) {
        ::close(fd);
        return std::unexpected(net_error::setup);
    }
    c.ring_ready_ = true;
    return c;
}

std::expected< craft_conn, net_error > craft_conn::connect(std::string const& host, uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return std::unexpected(net_error::setup);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        ::close(fd);
        return std::unexpected(net_error::setup);
    }
    if (::connect(fd, reinterpret_cast< sockaddr* >(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return std::unexpected(net_error::connect);
    }
    return adopt(fd);
}

int craft_conn::io_send(std::span< uint8_t const > d) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    io_uring_prep_send(sqe, fd_, d.data(), d.size(), MSG_NOSIGNAL);
    if (io_uring_submit(&ring_) < 0) return -1;
    io_uring_cqe* cqe = nullptr;
    if (io_uring_wait_cqe(&ring_, &cqe) < 0) return -1;
    int const res = cqe->res;
    io_uring_cqe_seen(&ring_, cqe);
    return res;
}

int craft_conn::io_recv(std::span< uint8_t > d, std::optional< std::chrono::steady_clock::time_point > deadline) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    io_uring_prep_recv(sqe, fd_, d.data(), d.size(), 0);
    if (io_uring_submit(&ring_) < 0) return -1;
    io_uring_cqe* cqe = nullptr;
    if (deadline) {
        auto const now = std::chrono::steady_clock::now();
        if (*deadline <= now) return -2; // already past; recv SQE left pending
        auto const rem = *deadline - now;
        auto const secs = std::chrono::duration_cast< std::chrono::seconds >(rem);
        __kernel_timespec ts{};
        ts.tv_sec = secs.count();
        ts.tv_nsec = std::chrono::duration_cast< std::chrono::nanoseconds >(rem - secs).count();
        int const w = io_uring_wait_cqe_timeout(&ring_, &cqe, &ts);
        if (w == -ETIME) return -2; // timed out; the recv SQE is still in flight -> the caller resets us
        if (w < 0) return -1;
    } else {
        if (io_uring_wait_cqe(&ring_, &cqe) < 0) return -1;
    }
    int const res = cqe->res;
    io_uring_cqe_seen(&ring_, cqe);
    return res;
}

bool craft_conn::send_all(std::span< uint8_t const > data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        int const n = io_send(data.subspan(sent));
        if (n <= 0) return false;
        sent += static_cast< std::size_t >(n);
    }
    return true;
}

std::expected< std::vector< uint8_t >, net_error >
craft_conn::recv_message(uint32_t max_tx, std::chrono::milliseconds timeout, wire::digest_cfg dg) {
    std::optional< std::chrono::steady_clock::time_point > deadline;
    if (timeout > std::chrono::milliseconds{0}) deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        auto r = wire::parse_message(rx_, max_tx, dg);
        if (r.has_value()) {
            std::size_t const total = r->total;
            std::vector< uint8_t > msg(rx_.begin(), rx_.begin() + total);
            rx_.erase(rx_.begin(), rx_.begin() + total); // retain any bytes past this message
            return msg;
        }
        if (r.error() != wire::parse_error::incomplete) return std::unexpected(net_error::malformed);

        std::size_t const old = rx_.size();
        rx_.resize(old + k_recv_chunk);
        int const n = io_recv({rx_.data() + old, k_recv_chunk}, deadline);
        if (n == -2) {
            rx_.resize(old);
            return std::unexpected(net_error::timed_out);
        }
        if (n == 0) return std::unexpected(net_error::closed);
        if (n < 0) {
            rx_.resize(old);
            return std::unexpected(net_error::recv);
        }
        rx_.resize(old + static_cast< std::size_t >(n));
    }
}

// ── craft_listener ──

craft_listener::~craft_listener() {
    if (fd_ >= 0) ::close(fd_);
}

craft_listener::craft_listener(craft_listener&& o) noexcept : fd_(o.fd_), port_(o.port_) { o.fd_ = -1; }

craft_listener& craft_listener::operator=(craft_listener&& o) noexcept {
    if (this != &o) {
        if (fd_ >= 0) ::close(fd_);
        fd_ = o.fd_;
        port_ = o.port_;
        o.fd_ = -1;
    }
    return *this;
}

std::expected< craft_listener, net_error > craft_listener::bind_listen(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return std::unexpected(net_error::setup);
    int const opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    if (::bind(fd, reinterpret_cast< sockaddr* >(&addr), sizeof(addr)) < 0 || ::listen(fd, 16) < 0) {
        ::close(fd);
        return std::unexpected(net_error::setup);
    }
    socklen_t len = sizeof(addr);
    ::getsockname(fd, reinterpret_cast< sockaddr* >(&addr), &len); // resolve an ephemeral (port 0) bind

    craft_listener l;
    l.fd_ = fd;
    l.port_ = ntohs(addr.sin_port);
    return l;
}

std::expected< craft_conn, net_error > craft_listener::accept() {
    int const cfd = ::accept(fd_, nullptr, nullptr);
    if (cfd < 0) return std::unexpected(net_error::connect);
    return craft_conn::adopt(cfd);
}

} // namespace craft::net
