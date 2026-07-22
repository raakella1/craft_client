#pragma once

#include <libnuraft/nuraft.hxx>
#include <sisl/logging/logging.h>

namespace craft {

class echo_state_machine : public nuraft::state_machine {
public:
    echo_state_machine() : lock_(), last_commit_idx_(0) {}

public:
    virtual nuraft::ptr< nuraft::buffer > commit(const nuraft::ulong log_idx, nuraft::buffer& data) {
        auto_lock(lock_);

        LOGINFO("Commit message [{}] : {}", log_idx, reinterpret_cast< const char* >(data.data()));
        last_commit_idx_ = log_idx;
        return nullptr;
    }

    virtual nuraft::ptr< nuraft::buffer > pre_commit(const nuraft::ulong log_idx, nuraft::buffer& data) {
        auto_lock(lock_);
        LOGINFO("Pre-Commit message [{}] : {}", log_idx, reinterpret_cast< const char* >(data.data()));
        return nullptr;
    }

    virtual void rollback(const nuraft::ulong log_idx, nuraft::buffer& data) {
        auto_lock(lock_);
        LOGINFO("Rollback[{}] : {}", log_idx, reinterpret_cast< const char* >(data.data()));
    }

    virtual void save_snapshot_data(nuraft::snapshot& s, const nuraft::ulong offset, nuraft::buffer& data) {}
    virtual bool apply_snapshot(nuraft::snapshot& s) { return true; }

    virtual int read_snapshot_data(nuraft::snapshot& s, const nuraft::ulong offset, nuraft::buffer& data) { return 0; }

    virtual nuraft::ptr< nuraft::snapshot > last_snapshot() { return nuraft::ptr< nuraft::snapshot >(); }

    virtual void create_snapshot(nuraft::snapshot& s, nuraft::async_result< bool >::handler_type& when_done) {}

    virtual nuraft::ulong last_commit_index() { return last_commit_idx_; }

private:
    std::mutex lock_;
    nuraft::ulong last_commit_idx_;
};
}