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

// craft_session_mgr: ONE process-wide admin thread shared by every CraftTcpReplica, replacing the per-proxy
// worker (a 50-disk RAID0 at N=3 would otherwise idle 150 threads). With resolve on the ring, the blocking
// verbs either bracket the ring's lifetime (login/logout) or belong to the no-ring tier, so they serialize on
// one thread without cost. Jobs stay FIFO on the single thread, so every "worker-thread-only, no lock"
// invariant in the proxies carries over verbatim -- the serializing thread is process-wide now, not per-proxy.
//
// Lifetime is refcounted, not a leaky singleton: each proxy holds a shared_ptr (get()), and the thread retires
// when the last proxy in the process drops -- zero CRAFT threads while no session exists.

#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

namespace craft::net {

class craft_session_mgr {
public:
    // The process-wide instance: created by the first taker, retired (thread joined) when the last drops it.
    static std::shared_ptr< craft_session_mgr > get();
    ~craft_session_mgr();
    craft_session_mgr(craft_session_mgr const&) = delete;
    craft_session_mgr& operator=(craft_session_mgr const&) = delete;

    void post(std::function< void() > job);

    // A FIFO fence: returns once every job posted before this call has run to completion. On the mgr thread
    // itself it returns immediately -- the running job IS the drain point, and a fence would wait on itself.
    void drain();

    bool on_mgr_thread() const noexcept { return std::this_thread::get_id() == tid_; }

private:
    craft_session_mgr();

    // The thread co-owns the queue state: the last proxy ref can drop ON the mgr thread (a detached leg held
    // the last volume_handle), where ~craft_session_mgr can only detach -- the detached thread then finishes
    // draining on its own shared copy, dangling nothing.
    struct state {
        std::mutex mu;
        std::condition_variable cv;
        std::deque< std::function< void() > > jobs;
        bool stop{false};
    };
    std::shared_ptr< state > st_;
    std::thread thr_;
    std::thread::id tid_;
};

} // namespace craft::net
