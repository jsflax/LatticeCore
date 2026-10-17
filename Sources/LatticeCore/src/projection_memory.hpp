#pragma once

// Private implementation. This header is not part of the Swift module surface.
#include "lattice/projection.hpp"
#include "lattice/db.hpp"
#include <mutex>
#include <set>
#include <unordered_set>
#include <limits>

namespace lattice {
struct projection_capture_failure : std::runtime_error {
    projection_status status;
    projection_capture_failure(projection_status status, const char* message)
        : std::runtime_error(message), status(status) {}
};

struct projection_capture_budget;
/// Retains counters only, never the parent/service/connection.
struct projection_capture_account : std::enable_shared_from_this<projection_capture_account> {
    static constexpr size_t ceiling = 64 * 1024 * 1024;
    std::shared_ptr<projection_capture_budget> reserve(size_t limit);
private:
    friend struct projection_capture_budget;
    std::mutex mutex;
    size_t reserved = 0;
};

struct projection_capture_budget {
    ~projection_capture_budget();
    void charge(size_t bytes);
    void release(size_t bytes) noexcept;
    /// Return unused reservation. Already-returned native batch allocations
    /// remain charged until their last owner releases them.
    void finish() noexcept;
private:
    friend struct projection_capture_account;
    projection_capture_budget(std::shared_ptr<projection_capture_account> account, size_t limit)
        : account(std::move(account)), limit(limit) {}
    std::shared_ptr<projection_capture_account> account;
    std::mutex mutex;
    size_t limit, used = 0;
    bool active = false; // Published only after the parent reservation succeeds.
};

/// Private shared allocator: allocation payload/capacity is charged before
/// allocation, including transient old+new buffers. Malloc overhead is excluded.
template<class T> struct capture_allocator {
    using value_type = T;
    std::shared_ptr<projection_capture_budget> budget;
    explicit capture_allocator(std::shared_ptr<projection_capture_budget> budget) : budget(std::move(budget)) {}
    template<class U> capture_allocator(const capture_allocator<U>& other) : budget(other.budget) {}
    T* allocate(size_t count) {
        if (count > std::numeric_limits<size_t>::max() / sizeof(T))
            throw projection_capture_failure(projection_status::capture_budget_exceeded, "capture allocation size overflow");
        const size_t bytes = count * sizeof(T);
        budget->charge(bytes);
        try { return std::allocator<T>{}.allocate(count); }
        catch (...) { budget->release(bytes); throw; }
    }
    void deallocate(T* pointer, size_t count) noexcept {
        std::allocator<T>{}.deallocate(pointer, count);
        budget->release(count * sizeof(T));
    }
    template<class U> bool operator==(const capture_allocator<U>& other) const noexcept { return budget == other.budget; }
    template<class U> bool operator!=(const capture_allocator<U>& other) const noexcept { return !(*this == other); }
};

class projection_capture_policy;

struct projection_capture_batch {
    struct storage;
    std::shared_ptr<const storage> data;
    int64_t row_count() const noexcept;
    int64_t copied_bytes() const noexcept;
    column_value_t value(int64_t row, int64_t column) const;
};

/// All result backing uses the charged allocator, including container capacity
/// and allocate_shared control blocks. Fixed operation/account bookkeeping is
/// bounded separately by operation admission; SQLite/malloc overhead is not RSS.
class projection_capture_storage {
public:
    projection_capture_storage(const std::shared_ptr<projection_capture_budget>&, int64_t columns);
    ~projection_capture_storage();
    void append(sqlite3_stmt* statement);
    bool empty() const noexcept;
    std::shared_ptr<const projection_capture_batch> take(int64_t max_rows);
private:
    struct storage;
    std::shared_ptr<storage> data_;
};

/// A stack-only, nonescaping borrower. It never publishes an interrupt target,
/// begins a transaction, drains hooks, or calls caller code. Every statement
/// handed through `statement` is finalized before policies/mutex are released.
struct database_projection_capture {
    database_projection_capture(database&, const std::shared_ptr<database_read_control>&,
                                sqlite3_stmt*& statement);
    ~database_projection_capture() noexcept;
    database_projection_capture(const database_projection_capture&) = delete;
    database_projection_capture& operator=(const database_projection_capture&) = delete;
    sqlite3* handle() const noexcept { return handle_; }
    /// First-stage fail-closed borrower: only one readonly SELECT and no virtual
    /// table callbacks. RTree/JSON-module admission is a separate review gate;
    /// this private scope does not yet enable public memory projection requests.
    void prepare_read(const std::string& sql);
    bool denied() const noexcept { return denied_; }
private:
    friend class projection_capture_policy;
    projection_capture_policy* policy_ = nullptr;
    static int authorize(void*, int, const char*, const char*, const char*, const char*) noexcept;
    static int progress(void*) noexcept;
    static int busy(void*, int) noexcept;
    void check() const;
    void restore() noexcept;
    void inventory_functions();
    database& database_;
    std::shared_ptr<database_read_control> control_;
    sqlite3_stmt*& statement_;
    sqlite3* handle_ = nullptr;
    sqlite3_mutex* mutex_ = nullptr;
    int prior_busy_timeout_ = 0;
    bool locked_ = false, installed_ = false, denied_ = false;
    std::unordered_set<std::string> builtin_functions_;
    database_projection_capture* previous_ = nullptr;
    static thread_local database_projection_capture* current_;
};
} // namespace lattice
