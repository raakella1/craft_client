#pragma once

#include <libnuraft/nuraft.hxx>
#include <sisl/logging/logging.h>

namespace craft {

inline auto unwrap_buffer(nuraft::buffer& data) {
    size_t buffer_len{0};
    auto const buffer_begin = data.get_bytes(buffer_len);
    auto const buffer_end = buffer_begin + buffer_len;
    auto input = std::vector< uint8_t >(buffer_begin, buffer_end);
    return nlohmann::json::from_msgpack(std::move(input));
}

enum class Operation { SyncRSCommitLSN = 0, InternalLogin };

// ── SyncRSCommitLSN payload ──
struct SyncRSCommitLSNMsg {
    int64_t rs_commit_lsn{-1};
    uint64_t client_token{0};
    std::vector< int64_t > empty_slots;
};

inline void to_json(nlohmann::json& j, SyncRSCommitLSNMsg const& m) {
    j = nlohmann::json{
        {"op", static_cast< int >(Operation::SyncRSCommitLSN)},
        {"rs_commit_lsn", m.rs_commit_lsn},
        {"client_token", m.client_token},
        {"empty_slots", m.empty_slots},
    };
}
inline void from_json(nlohmann::json const& j, SyncRSCommitLSNMsg& m) {
    j.at("rs_commit_lsn").get_to(m.rs_commit_lsn);
    j.at("client_token").get_to(m.client_token);
    j.at("empty_slots").get_to(m.empty_slots);
}

// ── InternalLogin payload ──
struct InternalLoginMsg {
    uint64_t client_token{0};
    uint64_t term{0};
    int64_t rs_commit_lsn{-1};
};

inline void to_json(nlohmann::json& j, InternalLoginMsg const& m) {
    j = nlohmann::json{
        {"op", static_cast< int >(Operation::InternalLogin)},
        {"client_token", m.client_token},
        {"term", m.term},
    };
}
inline void from_json(nlohmann::json const& j, InternalLoginMsg& m) {
    j.at("client_token").get_to(m.client_token);
    j.at("term").get_to(m.term);
}

using raft_commit_cb_t = std::function< void(uint64_t log_idx, nlohmann::json const& j, std::string const& group_id) >;

class echo_state_machine : public nuraft::state_machine {
public:
    echo_state_machine(raft_commit_cb_t const& cb, std::string const& group_id) :
            commit_cb_{cb},
            group_id_{group_id},
            last_commit_idx_(0) {}

    virtual nuraft::ptr< nuraft::buffer > commit(nuraft::ulong log_idx, nuraft::buffer& data) override {
        nlohmann::json j;
        try {
            j = unwrap_buffer(data);
        } catch (nlohmann::json::parse_error const& e) {
            LOGERROR("commit[{}]: msgpack decode failed: {}", log_idx, e.what());
            return nullptr;
        }
        LOGDEBUG("commit[{}]: decoded json={}, cb_set={}", log_idx, j.dump(), commit_cb_ ? "yes" : "no");
        if (commit_cb_) { commit_cb_(log_idx, j, group_id_); }
        last_commit_idx_ = log_idx;
        return nullptr;
    }

    virtual nuraft::ptr< nuraft::buffer > pre_commit(const nuraft::ulong log_idx, nuraft::buffer& data) override {
        return nullptr;
    }

    virtual void rollback(const nuraft::ulong log_idx, nuraft::buffer& data) override {}

    virtual void save_snapshot_data(nuraft::snapshot& s, const nuraft::ulong offset, nuraft::buffer& data) override {}
    virtual bool apply_snapshot(nuraft::snapshot& s) override { return true; }

    virtual int read_snapshot_data(nuraft::snapshot& s, const nuraft::ulong offset, nuraft::buffer& data) override {
        return 0;
    }

    virtual nuraft::ptr< nuraft::snapshot > last_snapshot() override { return nuraft::ptr< nuraft::snapshot >(); }

    virtual void create_snapshot(nuraft::snapshot& s, nuraft::async_result< bool >::handler_type& when_done) override {
        auto null_except = std::shared_ptr< std::exception >();
        auto ret_val{true};
        if (when_done) { when_done(ret_val, null_except); }
    }

    virtual nuraft::ulong last_commit_index() override { return last_commit_idx_; }

private:
    raft_commit_cb_t commit_cb_;
    std::string group_id_;
    nuraft::ulong last_commit_idx_;
};
} // namespace craft
