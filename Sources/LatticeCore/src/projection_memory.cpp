#include "projection_memory.hpp"
#include "projection_capture_policy.hpp"
#include <algorithm>
#include <cctype>
#include <cstring>
#include <deque>
#include <limits>
#include <thread>

namespace lattice {
namespace {
[[noreturn]] void capture_fail(projection_status status, const char* message) {
    throw projection_capture_failure(status, message);
}
struct capture_cell {
    int type = SQLITE_NULL;
    size_t offset = 0, length = 0;
    union { int64_t integer; double real; } scalar{};
};
struct capture_chunk {
    using cells_type = std::vector<capture_cell, capture_allocator<capture_cell>>;
    using bytes_type = std::vector<uint8_t, capture_allocator<uint8_t>>;
    cells_type cells;
    bytes_type bytes;
    int64_t rows = 0;
    explicit capture_chunk(const std::shared_ptr<projection_capture_budget>& budget)
        : cells(capture_allocator<capture_cell>(budget)), bytes(capture_allocator<uint8_t>(budget)) {}
};
struct capture_range {
    std::shared_ptr<const capture_chunk> chunk;
    int64_t first, rows;
};
int64_t cell_bytes(const capture_cell& cell) noexcept {
    if (cell.type == SQLITE_NULL) return 0;
    if (cell.type == SQLITE_INTEGER || cell.type == SQLITE_FLOAT) return 8;
    return static_cast<int64_t>(cell.length);
}
} // namespace

std::shared_ptr<projection_capture_budget> projection_capture_account::reserve(size_t limit) {
    if (limit == 0 || limit > ceiling)
        capture_fail(projection_status::invalid_request, "capture limit must be positive and at most 64 MiB");
    auto result = std::shared_ptr<projection_capture_budget>(new projection_capture_budget(shared_from_this(), limit));
    std::lock_guard<std::mutex> lock(mutex);
    if (limit > ceiling - reserved)
        capture_fail(projection_status::admission_rejected, "memory projection reservation is busy");
    reserved += limit;
    result->active = true;
    return result;
}
projection_capture_budget::~projection_capture_budget() { finish(); }
void projection_capture_budget::charge(size_t bytes) {
    std::lock_guard<std::mutex> lock(mutex);
    if (!active || bytes > limit - used)
        capture_fail(projection_status::capture_budget_exceeded, "memory projection backing budget exceeded");
    used += bytes;
}
void projection_capture_budget::release(size_t bytes) noexcept {
    std::lock_guard<std::mutex> lock(mutex);
    used -= bytes;
    if (!active) {
        std::lock_guard<std::mutex> parent_lock(account->mutex);
        account->reserved -= bytes;
    }
}
void projection_capture_budget::finish() noexcept {
    std::lock_guard<std::mutex> lock(mutex);
    if (!active) return;
    std::lock_guard<std::mutex> parent_lock(account->mutex);
    account->reserved -= limit - used;
    active = false;
}

struct projection_capture_batch::storage {
    int64_t columns, rows = 0, bytes = 0;
    std::vector<capture_range, capture_allocator<capture_range>> ranges;
    storage(const std::shared_ptr<projection_capture_budget>& budget, int64_t columns)
        : columns(columns), ranges(capture_allocator<capture_range>(budget)) {}
};
struct projection_capture_storage::storage {
    using chunk_pointer = std::shared_ptr<capture_chunk>;
    std::shared_ptr<projection_capture_budget> budget;
    int64_t columns, consumed = 0;
    std::deque<chunk_pointer, capture_allocator<chunk_pointer>> chunks;
    storage(const std::shared_ptr<projection_capture_budget>& budget, int64_t columns)
        : budget(budget), columns(columns), chunks(capture_allocator<chunk_pointer>(budget)) {}
};
projection_capture_storage::projection_capture_storage(const std::shared_ptr<projection_capture_budget>& budget, int64_t columns) {
    if (columns <= 0 || columns > 64)
        capture_fail(projection_status::invalid_request, "invalid memory capture column count");
    data_ = std::allocate_shared<storage>(capture_allocator<storage>(budget), budget, columns);
}
projection_capture_storage::~projection_capture_storage() = default;
bool projection_capture_storage::empty() const noexcept { return data_->chunks.empty(); }
void projection_capture_storage::append(sqlite3_stmt* statement) {
    // Caller has already validated the complete row's encoded bytes and row
    // count before any value is copied. Charged allocations additionally bound
    // metadata/capacity, including simultaneous old+new reallocation buffers.
    auto& data = *data_;
    if (!statement || sqlite3_column_count(statement) != data.columns)
        capture_fail(projection_status::invalid_request, "capture row has the wrong column count");
    if (data.chunks.empty() || data.chunks.back()->rows == 512) {
        data.chunks.push_back(std::allocate_shared<capture_chunk>(capture_allocator<capture_chunk>(data.budget), data.budget));
    }
    auto& chunk = *data.chunks.back();
    for (int column = 0; column < data.columns; ++column) {
        capture_cell cell;
        cell.type = sqlite3_column_type(statement, column);
        switch (cell.type) {
            case SQLITE_NULL: break;
            case SQLITE_INTEGER: cell.scalar.integer = sqlite3_column_int64(statement, column); break;
            case SQLITE_FLOAT: cell.scalar.real = sqlite3_column_double(statement, column); break;
            case SQLITE_TEXT:
            case SQLITE_BLOB: {
                const auto* bytes = static_cast<const uint8_t*>(cell.type == SQLITE_TEXT
                    ? static_cast<const void*>(sqlite3_column_text(statement, column)) : sqlite3_column_blob(statement, column));
                const int length = sqlite3_column_bytes(statement, column);
                if (length < 0 || (length && !bytes) || (cell.type == SQLITE_TEXT && !bytes))
                    capture_fail(projection_status::database_failure, "memory projection SQLite conversion failed");
                cell.offset = chunk.bytes.size(); cell.length = static_cast<size_t>(length);
                if (cell.length > std::numeric_limits<size_t>::max() - cell.offset)
                    capture_fail(projection_status::capture_budget_exceeded, "capture payload offset overflow");
                if (length) chunk.bytes.insert(chunk.bytes.end(), bytes, bytes + length);
                break;
            }
            default: capture_fail(projection_status::database_failure, "unknown memory projection cell type");
        }
        chunk.cells.push_back(cell);
    }
    ++chunk.rows;
}
std::shared_ptr<const projection_capture_batch> projection_capture_storage::take(int64_t max_rows) {
    auto& source = *data_;
    if (max_rows <= 0)
        capture_fail(projection_status::invalid_request, "capture batch size must be positive");
    auto data = std::allocate_shared<projection_capture_batch::storage>(
        capture_allocator<projection_capture_batch::storage>(source.budget), source.budget, source.columns);
    while (max_rows > 0 && !source.chunks.empty()) {
        const auto& chunk = source.chunks.front();
        const int64_t count = std::min(max_rows, chunk->rows - source.consumed);
        data->ranges.push_back({chunk, source.consumed, count});
        for (int64_t row = source.consumed; row < source.consumed + count; ++row)
            for (int64_t column = 0; column < source.columns; ++column)
                data->bytes += cell_bytes(chunk->cells[static_cast<size_t>(row * source.columns + column)]);
        data->rows += count; max_rows -= count; source.consumed += count;
        if (source.consumed == chunk->rows) { source.chunks.pop_front(); source.consumed = 0; }
    }
    auto batch = std::allocate_shared<projection_capture_batch>(capture_allocator<projection_capture_batch>(source.budget));
    batch->data = std::move(data);
    return batch;
}
int64_t projection_capture_batch::row_count() const noexcept { return data ? data->rows : 0; }
int64_t projection_capture_batch::copied_bytes() const noexcept { return data ? data->bytes : 0; }
column_value_t projection_capture_batch::value(int64_t row, int64_t column) const {
    if (!data || row < 0 || row >= data->rows || column < 0 || column >= data->columns)
        throw std::out_of_range("memory projection cell index");
    for (const auto& range : data->ranges) {
        if (row >= range.rows) { row -= range.rows; continue; }
        const auto& cell = range.chunk->cells[static_cast<size_t>((range.first + row) * data->columns + column)];
        switch (cell.type) {
            case SQLITE_NULL: return nullptr;
            case SQLITE_INTEGER: return cell.scalar.integer;
            case SQLITE_FLOAT: return cell.scalar.real;
            case SQLITE_TEXT:
                return cell.length ? std::string(reinterpret_cast<const char*>(range.chunk->bytes.data() + cell.offset), cell.length) : std::string{};
            case SQLITE_BLOB:
                return cell.length ? std::vector<uint8_t>(range.chunk->bytes.data() + cell.offset,
                    range.chunk->bytes.data() + cell.offset + cell.length) : std::vector<uint8_t>{};
            default: throw db_error("invalid captured cell type");
        }
    }
    throw std::out_of_range("missing memory projection range");
}


thread_local database_projection_capture* database_projection_capture::current_ = nullptr;
namespace {
// ASCII case folding matches SQLite's identifier/function matching, without
// locale-sensitive transformation of UTF-8 bytes.
std::string capture_lower(const char* text) {
    std::string result = text ? text : "";
    for (char& c : result) if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
    return result;
}
bool capture_stopped(database_read_control& control) noexcept {
    if (control.stop_code.load(std::memory_order_acquire) != 0) return true;
    if (std::chrono::steady_clock::now() < control.deadline) return false;
    int32_t expected = 0;
    control.stop_code.compare_exchange_strong(expected,
        static_cast<int32_t>(projection_status::deadline_exceeded), std::memory_order_acq_rel);
    return true;
}
}

void database_projection_capture::check() const {
    if (capture_stopped(*control_))
        capture_fail(static_cast<projection_status>(control_->stop_code.load(std::memory_order_acquire)),
                     "memory capture stopped");
    if (database_.is_closed())
        capture_fail(projection_status::snapshot_expired, "memory capture connection closed");
}
int database_projection_capture::progress(void* context) noexcept {
    return capture_stopped(*static_cast<database_projection_capture*>(context)->control_) ? 1 : 0;
}
int database_projection_capture::busy(void* context, int) noexcept {
    auto& capture = *static_cast<database_projection_capture*>(context);
    // Never sleep while holding the borrowed writer for a foreign connection's
    // lock. The caller can retry a new request, with a new explicit deadline.
    (void)capture_stopped(*capture.control_);
    return 0;
}

void database_projection_capture::inventory_functions() {
    // Required by typed scalar/string/JSON predicates. No callback-based Core
    // extension or arbitrary UDF is admitted. A nonbuiltin registration with
    // the same name invalidates that name even if another builtin arity exists
    // (for example PRAGMA case_sensitive_like installs a replacement).
    static const std::unordered_set<std::string> permitted{
        "abs", "coalesce", "count", "glob", "hex", "ifnull", "json_array_length",
        "json_extract", "length", "like", "lower", "nullif", "round",
        "substr", "substring", "typeof", "unicode", "upper"
    };
    sqlite3_stmt* inventory = nullptr;
    struct finish { sqlite3_stmt*& value; ~finish() { if (value) sqlite3_finalize(value); } } cleanup{inventory};
    database::record_statement();
    if (sqlite3_prepare_v3(handle_, "PRAGMA function_list", -1, SQLITE_PREPARE_NO_VTAB,
                           &inventory, nullptr) != SQLITE_OK)
        capture_fail(projection_status::unsupported, "memory capture function inventory is unavailable");
    std::unordered_set<std::string> overridden;
    size_t inspected = 0;
    int rc;
    while ((rc = sqlite3_step(inventory)) == SQLITE_ROW) {
        check();
        if (++inspected > 4096)
            capture_fail(projection_status::unsupported, "memory capture function inventory exceeds bound");
        const auto* raw = reinterpret_cast<const char*>(sqlite3_column_text(inventory, 0));
        const int length = sqlite3_column_bytes(inventory, 0);
        if (!raw || length < 0 || length > 128)
            capture_fail(projection_status::unsupported, "memory capture function name is unsupported");
        auto name = capture_lower(raw);
        if (!permitted.count(name)) continue;
        if (sqlite3_column_int(inventory, 1) == 1) builtin_functions_.insert(std::move(name));
        else overridden.insert(std::move(name));
    }
    if (rc != SQLITE_DONE) { check(); throw db_error("memory capture function inventory failed"); }
    for (const auto& name : overridden) builtin_functions_.erase(name);
}

int database_projection_capture::authorize(void* context, int action, const char* first,
                                            const char* second, const char* schema, const char* trigger) noexcept {
    auto& capture = *static_cast<database_projection_capture*>(context);
    if (capture.policy_) return capture.policy_->authorize(action, first, second, schema, trigger);
    try {
        if (action == SQLITE_SELECT || action == SQLITE_READ || action == SQLITE_RECURSIVE)
            return SQLITE_OK;
        if (action == SQLITE_FUNCTION && second) {
            for (const auto& name : capture.builtin_functions_)
                if (sqlite3_stricmp(name.c_str(), second) == 0) return SQLITE_OK;
        }
    } catch (...) {
        // No exception crosses SQLite's C callback boundary. Matching itself
        // is allocation-free and invokes no application/native extension code.
    }
    capture.denied_ = true;
    return SQLITE_DENY;
}

database_projection_capture::database_projection_capture(
    database& database, const std::shared_ptr<database_read_control>& control, sqlite3_stmt*& statement)
    : database_(database), control_(control), statement_(statement) {
    if (!control_ || statement_)
        capture_fail(projection_status::invalid_request, "capture requires a control and empty statement slot");
    handle_ = database_.internal_handle();
    if (!handle_ || database_.read_control_)
        capture_fail(projection_status::unsupported, "capture requires the owning writer connection");
    for (auto* scope = current_; scope; scope = scope->previous_)
        if (scope->handle_ == handle_)
            capture_fail(projection_status::admission_rejected, "recursive memory capture is unsupported");
    if (database::update_hook_scope::active_for(handle_))
        capture_fail(projection_status::admission_rejected, "memory capture from update hook is unsupported");
    // The control must never be shared with a private reader or publish this
    // writer as its sqlite3_interrupt target. Check before holding SQLite.
    {
        std::lock_guard<std::mutex> lock(control_->target_mutex);
        if (control_->target)
            capture_fail(projection_status::invalid_request, "borrowed capture control has an interrupt target");
    }
    mutex_ = sqlite3_db_mutex(handle_);
    if (!mutex_) capture_fail(projection_status::unsupported, "capture requires serialized SQLite");
    try {
        while (sqlite3_mutex_try(mutex_) != SQLITE_OK) {
            check();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        locked_ = true;
        check();
        if (database_.raw_handle_escaped_.load(std::memory_order_acquire))
            capture_fail(projection_status::unsupported, "raw SQLite handle escaped; capture policies are unowned");
        if (database_.canonical_custody_bootstrap_ || database_.canonical_callback_custody_ ||
            std::atomic_load(&database_.local_producer_callback_custody_))
            capture_fail(projection_status::unsupported, "recovery writer owns connection policies; borrowed capture refused");
        if (!sqlite3_get_autocommit(handle_))
            capture_fail(projection_status::admission_rejected, "memory capture requires committed state");
        for (auto* existing = sqlite3_next_stmt(handle_, nullptr); existing; existing = sqlite3_next_stmt(handle_, existing))
            if (sqlite3_stmt_busy(existing))
                capture_fail(projection_status::admission_rejected, "memory writer has an active statement");

        // No raw escape and no read_control means the current Core writer has
        // no progress handler or authorizer. Its only busy policy is SQLite's
        // numeric timeout; read the actual PRAGMA, including user SQL changes.
        sqlite3_stmt* timeout = nullptr;
        struct finish { sqlite3_stmt*& value; ~finish() { if (value) sqlite3_finalize(value); } } cleanup{timeout};
        database::record_statement();
        if (sqlite3_prepare_v3(handle_, "PRAGMA busy_timeout", -1, SQLITE_PREPARE_NO_VTAB,
                               &timeout, nullptr) != SQLITE_OK || sqlite3_step(timeout) != SQLITE_ROW)
            throw db_error("memory capture busy policy is unavailable");
        prior_busy_timeout_ = sqlite3_column_int(timeout, 0);
        if (prior_busy_timeout_ < 0) throw db_error("memory capture busy policy is invalid");
        sqlite3_finalize(timeout); timeout = nullptr;

        // Mark installed before the first policy mutation so constructor throws
        // restore all known policies. There is no BEGIN/ROLLBACK or settled drain.
        installed_ = true;
        sqlite3_progress_handler(handle_, 1000, progress, this);
        if (sqlite3_busy_handler(handle_, busy, this) != SQLITE_OK)
            throw db_error("memory capture busy policy installation failed");
        inventory_functions();
        if (sqlite3_set_authorizer(handle_, authorize, this) != SQLITE_OK)
            throw db_error("memory capture authorizer installation failed");
        previous_ = current_;
        current_ = this;
    } catch (...) { restore(); throw; }
}

void database_projection_capture::prepare_read(const std::string& sql) {
    check();
    if (statement_) capture_fail(projection_status::invalid_request, "capture already owns a statement");
    if (sql.size() > 1024 * 1024 || sql.find('\0') != std::string::npos)
        capture_fail(projection_status::invalid_request, "capture SQL exceeds admission bounds");
    const char* tail = nullptr;
    database::record_statement();
    const int rc = sqlite3_prepare_v3(handle_, sql.c_str(), static_cast<int>(sql.size() + 1),
        SQLITE_PREPARE_NO_VTAB, &statement_, &tail);
    check();
    if (rc != SQLITE_OK) {
        if (denied_) capture_fail(projection_status::unsupported, "memory capture SQL callback or operation is unsupported");
        throw db_error("memory capture prepare failed: " + std::string(sqlite3_errmsg(handle_)));
    }
    while (tail && *tail && std::isspace(static_cast<unsigned char>(*tail))) ++tail;
    if (!statement_ || (tail && *tail) || !sqlite3_stmt_readonly(statement_))
        capture_fail(projection_status::invalid_request, "capture requires one readonly SELECT");
}

void database_projection_capture::restore() noexcept {
    // Finalization and policy restoration happen under the same connection
    // ownership. No user callbacks are delivered, and no interrupt is issued.
    if (statement_) { sqlite3_finalize(statement_); statement_ = nullptr; }
    if (installed_) {
        sqlite3_set_authorizer(handle_, nullptr, nullptr);
        sqlite3_progress_handler(handle_, 0, nullptr, nullptr);
        sqlite3_busy_timeout(handle_, prior_busy_timeout_);
        installed_ = false;
    }
    if (current_ == this) current_ = previous_;
    if (locked_) { sqlite3_mutex_leave(mutex_); locked_ = false; }
}
database_projection_capture::~database_projection_capture() noexcept { restore(); }
} // namespace lattice
