#include "watchdog.hpp"

namespace craft {
Watchdog::Watchdog() : worker_([this](std::stop_token st) { run(std::move(st)); }) {}

Watchdog::~Watchdog() = default;

Watchdog::TimerId Watchdog::add_oneshot(Clock::duration delay, Callback cb) {
    return add_timer(delay, /*period=*/std::nullopt, std::move(cb));
}

Watchdog::TimerId Watchdog::add_recurring(Clock::duration period, Callback cb) {
    return add_timer(period, period, std::move(cb));
}

bool Watchdog::reset(TimerId id) {
    std::unique_lock lk(mutex_);
    auto it = timers_.find(id);
    if (it == timers_.end()) return false;
    auto& state = *it->second;
    state.deadline = Clock::now() + state.period;
    ++state.generation; // invalidates any heap entry already popped-pending
    heap_.push(HeapEntry{state.deadline, id, state.generation});
    lk.unlock();
    cv_.notify_all();
    return true;
}

bool Watchdog::cancel(TimerId id) {
    std::unique_lock lk(mutex_);
    auto erased = timers_.erase(id) != 0;
    lk.unlock();
    if (erased) cv_.notify_all();
    return erased;
}

Watchdog::TimerId Watchdog::add_timer(Clock::duration delay, std::optional< Clock::duration > period, Callback cb) {
    std::unique_lock lk(mutex_);
    const TimerId id = next_id_.fetch_add(1, std::memory_order_relaxed);
    auto state = std::make_shared< TimerState >();
    state->deadline = Clock::now() + delay;
    state->recurring = period.has_value();
    // For a one-shot, `period` stores the *original delay* so that
    // reset() -- if ever called on a one-shot -- extends it by the same
    // amount rather than collapsing the deadline to "now". `recurring`
    // (not `period`) is what actually gates self-rescheduling on fire.
    state->period = period.value_or(delay);
    state->cb = std::move(cb);
    const auto gen = state->generation;
    heap_.push(HeapEntry{state->deadline, id, gen});
    timers_.emplace(id, std::move(state));
    lk.unlock();
    cv_.notify_all();
    return id;
}

void Watchdog::run(std::stop_token stoken) {
    // Wake the condvar as soon as a stop is requested (jthread dtor).
    std::stop_callback wake(stoken, [this] { cv_.notify_all(); });

    std::unique_lock lk(mutex_);
    while (!stoken.stop_requested()) {
        if (heap_.empty()) {
            cv_.wait(lk, [&] { return !heap_.empty() || stoken.stop_requested(); });
            continue;
        }

        const auto deadline = heap_.top().deadline;
        cv_.wait_until(lk, deadline); // wakes early on notify too
        if (stoken.stop_requested()) break;
        if (heap_.empty()) continue;
        if (Clock::now() < heap_.top().deadline) {
            continue; // spurious/early wake (new timer added or reset); re-check top
        }

        const HeapEntry entry = heap_.top();
        heap_.pop();

        const auto it = timers_.find(entry.id);
        if (it == timers_.end()) continue;                        // cancelled
        if (it->second->generation != entry.generation) continue; // stale (was reset)

        std::shared_ptr< TimerState > state = it->second; // keeps it alive across unlock
        const bool recurring = state->recurring;
        const auto period = state->period;
        const auto fired_generation = state->generation;

        lk.unlock();
        state->cb();
        lk.lock();

        if (recurring) {
            const auto it2 = timers_.find(entry.id);
            // Only reschedule if it's still registered and wasn't
            // reset()/cancel()'d (by the callback itself, or concurrently
            // from another thread) while we were unlocked running it.
            if (it2 != timers_.end() && it2->second == state && state->generation == fired_generation) {
                state->deadline = Clock::now() + period;
                heap_.push(HeapEntry{state->deadline, entry.id, state->generation});
            }
        } else {
            timers_.erase(entry.id);
        }
    }
}
} // namespace craft