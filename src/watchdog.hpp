#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <stop_token>
#include <thread>
#include <unordered_map>
#include <vector>

// Lightweight, single-thread, heap-based timer service.
//
//   - add_oneshot(delay, cb)     -> fires once, e.g. login quiesce-barrier timeout
//   - add_recurring(period, cb)  -> fires every `period` unless reset(), e.g. a
//                                   per-session keepalive lease watchdog
//   - reset(id)                  -> pushes the deadline out by the timer's
//                                   original delay/period, measured from now
//                                   (this is what a keep_alive RPC calls)
//   - cancel(id)                 -> removes the timer; safe to call from
//                                   inside a callback (including the timer's
//                                   own callback) or concurrently with a fire
//
// Not a general-purpose scheduler: no priority levels, no thread pool for
// callbacks (they run serially on the watchdog's own thread -- keep them
// short; if a callback needs to do real work, have it hand off to another
// thread/executor rather than block here).

namespace craft {
class Watchdog {
public:
    using Clock = std::chrono::steady_clock;
    using TimerId = std::uint64_t;
    using Callback = std::move_only_function< void() >;

    Watchdog();
    ~Watchdog();

    Watchdog(const Watchdog&) = delete;
    Watchdog& operator=(const Watchdog&) = delete;

    TimerId add_oneshot(Clock::duration delay, Callback cb);
    TimerId add_recurring(Clock::duration period, Callback cb);
    bool reset(TimerId id);
    bool cancel(TimerId id);

private:
    struct TimerState {
        Clock::time_point deadline;
        Clock::duration period{}; // reset() increment; = original delay for one-shots
        bool recurring = false;
        std::uint64_t generation = 0;
        Callback cb;
    };

    struct HeapEntry {
        Clock::time_point deadline;
        TimerId id;
        std::uint64_t generation;
        // std::greater<> as the queue comparator makes this a min-heap on
        // deadline: earliest deadline pops first.
        bool operator>(const HeapEntry& o) const { return deadline > o.deadline; }
    };

    TimerId add_timer(Clock::duration delay, std::optional< Clock::duration > period, Callback cb);
    void run(std::stop_token stoken);

    mutable std::mutex mutex_;
    std::condition_variable_any cv_;
    std::priority_queue< HeapEntry, std::vector< HeapEntry >, std::greater<> > heap_;
    std::unordered_map< TimerId, std::shared_ptr< TimerState > > timers_;
    std::atomic< TimerId > next_id_{1};

    // Declared LAST so it's the FIRST destroyed: the thread must stop and
    // join before mutex_/cv_/heap_/timers_ are torn down.
    std::jthread worker_;
};
} // namespace craft