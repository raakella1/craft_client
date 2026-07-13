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

#include "net/session_mgr.hpp"

namespace craft::net {

namespace {
std::mutex g_mu;                             // guards g_mgr against concurrent first-takers
std::weak_ptr< craft_session_mgr > g_mgr;    // the process-wide instance, alive while any proxy holds it
} // namespace

std::shared_ptr< craft_session_mgr > craft_session_mgr::get() {
    std::lock_guard< std::mutex > g{g_mu};
    if (auto m = g_mgr.lock()) return m;
    auto m = std::shared_ptr< craft_session_mgr >(new craft_session_mgr());
    g_mgr = m;
    return m;
}

craft_session_mgr::craft_session_mgr() : st_{std::make_shared< state >()} {
    thr_ = std::thread([st = st_] { // the thread's own ref: st_ stays valid even through the detach path
        std::unique_lock< std::mutex > lk{st->mu};
        for (;;) {
            st->cv.wait(lk, [&] { return st->stop || !st->jobs.empty(); });
            if (st->jobs.empty()) return; // stop with nothing left to drain
            auto job = std::move(st->jobs.front());
            st->jobs.pop_front();
            lk.unlock();
            job(); // resumes a hopped coroutine INLINE: it runs its blocking socket op here and completes
            lk.lock();
        }
    });
    tid_ = thr_.get_id();
}

craft_session_mgr::~craft_session_mgr() {
    {
        std::lock_guard< std::mutex > g{st_->mu};
        st_->stop = true;
    }
    st_->cv.notify_all();
    // The last ref can drop ON the mgr thread (a detached leg held the last volume_handle, and dropping the
    // proxy dropped us); a thread cannot join itself, so detach -- it drains what remains on its own shared
    // copy of the state and exits.
    if (on_mgr_thread()) thr_.detach();
    else thr_.join();
}

void craft_session_mgr::post(std::function< void() > job) {
    {
        std::lock_guard< std::mutex > g{st_->mu};
        st_->jobs.push_back(std::move(job));
    }
    st_->cv.notify_one();
}

void craft_session_mgr::drain() {
    if (on_mgr_thread()) return; // the running job is the drain point; a fence would wait on itself
    std::mutex mu;
    std::condition_variable cv;
    bool hit = false;
    post([&] {
        std::lock_guard< std::mutex > g{mu};
        hit = true;
        cv.notify_one();
    });
    std::unique_lock< std::mutex > lk{mu};
    cv.wait(lk, [&] { return hit; });
}

} // namespace craft::net
