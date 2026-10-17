#pragma once
#include <array>
#include <cstdint>
#include <memory>
#include <string>

namespace lattice {
class lattice_db;
class database;
struct owned_catalog_state;

/// A provisional frame has no source/write/ACK authority. The callback must not
/// retain the pointer, reenter this owner, or publish the unfinished reduction.
using owned_catalog_callback = bool (*)(void*, int32_t, const uint8_t*, int64_t);

class catalog_stream_result {
    friend struct owned_catalog_state;
    bool succeeded_ = false, cleanup_failed_ = false;
    int32_t finalize_ = -1, transaction_ = -1, rollback_ = -1, stop_ = 0;
    std::array<char, 2049> message_{};
public:
    bool success() const noexcept { return succeeded_; }
    bool cleanup_failed() const noexcept { return cleanup_failed_; }
    int32_t finalize_code() const noexcept { return finalize_; }
    int32_t transaction_code() const noexcept { return transaction_; }
    int32_t rollback_code() const noexcept { return rollback_; }
    int32_t stop_code() const noexcept { return stop_; }
    std::string error_message() const { return std::string(message_.data()); }
};

/// Creation retains the actual parent plus a never-renewed <=30s end, but
/// admits no reader, SQLite statement or physical-store ticket before capture.
/// Exactly one capture is allowed. Cancellation never targets a successor.
class catalog_stream_operation {
    std::shared_ptr<owned_catalog_state> state_;
    explicit catalog_stream_operation(std::shared_ptr<owned_catalog_state> state) : state_(std::move(state)) {}
    friend catalog_stream_operation make_catalog_stream_operation(
        std::shared_ptr<lattice_db>, const std::string&, int64_t);
public:
    catalog_stream_operation() = default;
    bool valid() const noexcept;
    void cancel() const noexcept;
    catalog_stream_result capture(void*, owned_catalog_callback) const noexcept;
};

catalog_stream_operation make_catalog_stream_operation(
    std::shared_ptr<lattice_db> owner, const std::string& tables_json, int64_t timeout_nanoseconds);

namespace detail {
// Denial only. Called by actual parent/database entry points; no origin grant.
void require_catalog_database_callback_absent(const database*);
}
}
