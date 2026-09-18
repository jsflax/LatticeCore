#include "lattice/projection.hpp"
#include "lattice/lattice.hpp"
#include "lattice/spatial_query.hpp"
#include "projection_memory.hpp"
#include "projection_capture_policy.hpp"
#include <condition_variable>
#include <cctype>
#include <cmath>
#include <cstring>
#include <limits>
#include <set>
#include <thread>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <pthread.h>
#endif

namespace lattice {
namespace {
using clock_type = std::chrono::steady_clock;
constexpr size_t kMaxColumns = 64;
constexpr size_t kMaxBatchRows = 512;
constexpr size_t kMaxOperations = 64;
// Source integration gate: lift only after the policy and memory service slice.
constexpr bool kMemoryCaptureEnabled = true;
constexpr size_t kMaxLeases = 2; // Explicit resource choice; not a measured optimum.
constexpr int64_t kMaxRequestBytes = 1024 * 1024;
std::atomic<uint64_t> next_operation_id{1};

std::string quote_sql_identifier(const std::string& name) {
    std::string value = "\"";
    for (char c : name) { value += c; if (c == '"') value += '"'; }
    return value + '"';
}
std::string readonly_uri(const std::string& path) {
    // ATTACH must be explicitly read-only, independently of the main flags.
    if (path.rfind("file:", 0) == 0) {
        if (path.find("mode=ro") != std::string::npos) return path;
        if (path.find("mode=") != std::string::npos)
            throw db_error("projection: attached URI must permit explicit read-only mode");
        return path + (path.find('?') == std::string::npos ? "?mode=ro" : "&mode=ro");
    }
    static constexpr char hex[] = "0123456789ABCDEF";
    std::string uri = "file:";
    for (unsigned char c : path) {
        if (c == '%' || c == '?' || c == '#' || c == ' ') {
            uri += '%'; uri += hex[c >> 4]; uri += hex[c & 15];
        } else uri += static_cast<char>(c);
    }
    return uri + "?mode=ro";
}
struct projection_failure : std::runtime_error {
    projection_status status;
    projection_failure(projection_status status, const std::string& text)
        : std::runtime_error(text), status(status) {}
};
[[noreturn]] void fail(projection_status status, const char* text) { throw projection_failure(status, text); }

const char* status_message(projection_status status) {
    switch (status) {
        case projection_status::cancelled: return "projection cancelled";
        case projection_status::deadline_exceeded: return "projection deadline exceeded";
        case projection_status::snapshot_expired: return "projection snapshot retired or topology changed";
        case projection_status::closed: return "projection closed";
        case projection_status::concurrent_next: return "concurrent projection nextBatch calls";
        case projection_status::admission_rejected: return "projection resource admission rejected";
        default: return "projection failed";
    }
}
void bind_checked(sqlite3_stmt* statement, int index, const column_value_t& value) {
    const int rc = std::visit([&](const auto& cell) -> int {
        using T = std::decay_t<decltype(cell)>;
        if constexpr (std::is_same_v<T, std::nullptr_t>) return sqlite3_bind_null(statement, index);
        else if constexpr (std::is_same_v<T, int64_t>) return sqlite3_bind_int64(statement, index, cell);
        else if constexpr (std::is_same_v<T, double>) return sqlite3_bind_double(statement, index, cell);
        else if constexpr (std::is_same_v<T, std::string>)
            return sqlite3_bind_text(statement, index, cell.data(), static_cast<int>(cell.size()), SQLITE_TRANSIENT);
        else return cell.empty() ? sqlite3_bind_zeroblob(statement, index, 0) :
            sqlite3_bind_blob(statement, index, cell.data(), static_cast<int>(cell.size()), SQLITE_TRANSIENT);
    }, value);
    if (rc != SQLITE_OK) throw db_error("projection parameter binding failed");
}
} // namespace

class projection_service : public std::enable_shared_from_this<projection_service> {
public:
    explicit projection_service(lattice_db* owner) : owner_(owner) {}
    projection_read_operation start(const projection_query& query);
    bool claim();
    void unclaim();
    bool acquire_lease();
    void release_lease();
    std::shared_ptr<projection_capture_budget> reserve_capture(size_t limit);
    void erase(uint64_t id);
    void stop_all(projection_status reason, bool closing);
    size_t resources() const;
    void shutdown();
    struct topology {
        std::string path;
        bool memory = false;
        std::vector<std::pair<std::string, std::string>> attachments;
        std::vector<std::shared_ptr<const physical_store_identity>> identities;
        std::map<std::string, std::shared_ptr<const physical_store_identity>> files_by_schema;
        std::vector<std::string> views;
        int64_t idle_ttl_ms = 30000, max_age_ms = 300000;
    };
    topology capture(const std::shared_ptr<database_read_control>& control);
    void capture_memory(projection_operation_state&, const topology&);
private:
    lattice_db* owner_; // Only accessed while a counted claim prevents teardown.
    mutable std::mutex mutex_;
    std::condition_variable settled_;
    bool closed_ = false;
    size_t calls_ = 0, leases_ = 0;
    std::map<uint64_t, std::shared_ptr<projection_operation_state>> operations_;
};

struct projection_operation_state : std::enable_shared_from_this<projection_operation_state> {
    uint64_t id = next_operation_id.fetch_add(1, std::memory_order_relaxed);
    std::shared_ptr<projection_service> service;
    projection_query query;
    std::shared_ptr<database_read_control> control = std::make_shared<database_read_control>();
    std::mutex execution_mutex;
    std::atomic<bool> executing{false}, resources{false}, done{false};
    std::unique_ptr<database> connection;
    sqlite3_stmt* statement = nullptr;
    bool leased = false, initialized = false;
    std::shared_ptr<projection_capture_budget> capture_budget;
    std::unique_ptr<projection_capture_storage> captured_rows;
    projection_store_ticket stores;
    int64_t column_count = 0, rows = 0, bytes = 0;
    std::atomic<int64_t> last_access_ms{0}, lease_start_ms{0};
    int64_t idle_ttl_ms = 30000, max_age_ms = 300000;
    std::string error;
    std::mutex callback_mutex;
    bool callback_registered = false, callback_delivered = false;
    void* callback_context = nullptr;
    void (*released_callback)(void*) = nullptr;

    projection_operation_state(std::shared_ptr<projection_service> service, const projection_query& query,
                               clock_type::time_point deadline)
        : service(std::move(service)), query(query), column_count(static_cast<int64_t>(query.columns.size())) {
        control->deadline = deadline;
    }
    static int64_t now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(clock_type::now().time_since_epoch()).count();
    }
    void check() {
        if (control->stopped()) {
            const auto status = static_cast<projection_status>(control->stop_code.load());
            fail(status, status_message(status));
        }
    }
    void signal(projection_status reason) noexcept;
    void notify_released() noexcept;
    void cleanup_locked() noexcept;
    void sweep() noexcept;
    void initialize();
    void prepare_query(sqlite3*, const projection_service::topology&, projection_capture_policy* = nullptr);
    void capture_all(sqlite3*);
    projection_read_batch next(int64_t max_rows);
    projection_read_batch result(projection_status status, std::shared_ptr<const std::vector<column_value_t>> cells = {}) {
        projection_read_batch batch;
        batch.status_ = status;
        batch.error_ = error;
        batch.columns_ = column_count;
        batch.cumulative_rows_ = rows;
        batch.cumulative_bytes_ = bytes;
        batch.cells_ = std::move(cells);
        return batch;
    }
};

namespace {
/// One joinable process-wide janitor. Weak registrations never keep a parent
/// lattice alive. 10ms resolution is a cleanup/watchdog cadence, not a hard
/// real-time promise for an OS/VFS call that SQLite cannot interrupt.
class projection_watchdog {
public:
    static projection_watchdog& instance() {
        // Process-lifetime service, like the instance registry: late global
        // lattice destructors may still signal it. Its one joinable thread is
        // never detached and never owns a parent lattice.
        static auto* instance = [] {
            auto* created = new projection_watchdog;
            available_.store(created, std::memory_order_release);
            return created;
        }();
        return *instance;
    }
    static void wake_if_started() noexcept {
        if (auto* existing = available_.load(std::memory_order_acquire)) existing->wake();
    }
    void add(const std::shared_ptr<projection_operation_state>& state) {
        std::lock_guard<std::mutex> lock(mutex_);
        states_.push_back(state);
        changed_.notify_one();
    }
    void wake() noexcept { changed_.notify_one(); }
private:
    projection_watchdog() {
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
        pthread_attr_t attributes;
        if (pthread_attr_init(&attributes) != 0) throw std::runtime_error("projection watchdog attributes failed");
        const int stack_rc = pthread_attr_setstacksize(&attributes, 8 * 1024 * 1024);
        const int create_rc = stack_rc == 0 ? pthread_create(&thread_, &attributes, [](void* context) -> void* {
            static_cast<projection_watchdog*>(context)->run(); return nullptr;
        }, this) : stack_rc;
        pthread_attr_destroy(&attributes);
        if (create_rc != 0) throw std::runtime_error("projection watchdog creation failed");
#elif !defined(__EMSCRIPTEN__)
        thread_ = std::thread([this] { run(); });
#endif
    }
    ~projection_watchdog() {
        { std::lock_guard<std::mutex> lock(mutex_); stopped_ = true; }
        changed_.notify_all();
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
        pthread_join(thread_, nullptr);
#elif !defined(__EMSCRIPTEN__)
        if (thread_.joinable()) thread_.join();
#endif
    }
    void run() {
        std::unique_lock<std::mutex> lock(mutex_);
        while (!stopped_) {
            changed_.wait_for(lock, std::chrono::milliseconds(10));
            std::vector<std::shared_ptr<projection_operation_state>> live;
            for (auto it = states_.begin(); it != states_.end();) {
                if (auto state = it->lock()) {
                    live.push_back(state);
                    if ((state->done.load() || state->control->stop_code.load() != 0) &&
                        !state->resources.load() && !state->executing.load()) it = states_.erase(it);
                    else ++it;
                } else it = states_.erase(it);
            }
            lock.unlock();
            for (const auto& state : live) state->sweep();
            lock.lock();
        }
    }
    static inline std::atomic<projection_watchdog*> available_{nullptr};
    std::mutex mutex_;
    std::condition_variable changed_;
    std::vector<std::weak_ptr<projection_operation_state>> states_;
    bool stopped_ = false;
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    pthread_t thread_{};
#elif !defined(__EMSCRIPTEN__)
    std::thread thread_;
#endif
};
} // namespace

namespace {
using physical_key = std::pair<uint64_t, uint64_t>;
physical_key key_for(const physical_store_identity& identity) { return {identity.device, identity.inode}; }
struct projection_store_epoch {
    uint64_t epoch = 0;
    std::map<uint64_t, std::weak_ptr<database_read_control>> operations;
    std::vector<std::weak_ptr<projection_pressure_source>> pressure;
};
struct projection_store_registry {
    std::mutex mutex;
    std::map<physical_key, std::weak_ptr<projection_store_epoch>> stores;
    static projection_store_registry& instance() {
        static auto* registry = new projection_store_registry;
        return *registry;
    }
    void prune() {
        for (auto it = stores.begin(); it != stores.end();) {
            if (it->second.expired()) it = stores.erase(it); else ++it;
        }
    }
    std::shared_ptr<projection_store_epoch> get(const physical_store_identity& identity) {
        auto& entry = stores[key_for(identity)];
        auto node = entry.lock();
        if (!node) { node = std::make_shared<projection_store_epoch>(); entry = node; }
        return node;
    }
    static bool pressured(projection_store_epoch& node) {
        bool pending = false;
        for (auto it = node.pressure.begin(); it != node.pressure.end();) {
            if (auto source = it->lock()) { pending = pending || source->pending(); ++it; }
            else it = node.pressure.erase(it);
        }
        return pending;
    }
};
} // namespace

struct projection_store_ticket_state {
    std::vector<std::pair<std::shared_ptr<projection_store_epoch>, uint64_t>> epochs;
    uint64_t operation_id = 0;
    ~projection_store_ticket_state() {
        if (!operation_id) return;
        auto& registry = projection_store_registry::instance();
        std::lock_guard<std::mutex> lock(registry.mutex);
        for (const auto& [node, _] : epochs) node->operations.erase(operation_id);
    }
};
projection_store_ticket capture_projection_stores(const std::vector<std::shared_ptr<const physical_store_identity>>& identities) {
    projection_store_ticket ticket;
    auto state = std::make_shared<projection_store_ticket_state>();
    std::set<physical_key> seen;
    auto& registry = projection_store_registry::instance();
    std::lock_guard<std::mutex> lock(registry.mutex);
    registry.prune();
    for (const auto& identity : identities) {
        if (!identity) fail(projection_status::unsupported, "projection requires a supported stable local file identity");
        if (!seen.insert(key_for(*identity)).second) continue;
        auto node = registry.get(*identity);
        state->epochs.emplace_back(node, node->epoch);
    }
    ticket.state_ = std::move(state);
    return ticket;
}
projection_status projection_store_ticket::publish(uint64_t operation_id, const std::shared_ptr<database_read_control>& control) const {
    if (!state_ || !operation_id || !control) return projection_status::invalid_request;
    auto& registry = projection_store_registry::instance();
    std::lock_guard<std::mutex> lock(registry.mutex);
    if (state_->operation_id) return projection_status::invalid_request;
    if (control->stopped()) return static_cast<projection_status>(control->stop_code.load());
    for (const auto& [node, epoch] : state_->epochs) {
        if (node->epoch != epoch || epoch == std::numeric_limits<uint64_t>::max()) return projection_status::snapshot_expired;
        if (projection_store_registry::pressured(*node)) return projection_status::admission_rejected;
    }
    // Allocate all entries before setting operation_id; unwind partial insertion
    // explicitly so failed admission cannot leave a phantom outstanding lease.
    try {
        for (const auto& [node, _] : state_->epochs) node->operations.emplace(operation_id, control);
    } catch (...) {
        for (const auto& [node, _] : state_->epochs) node->operations.erase(operation_id);
        throw;
    }
    state_->operation_id = operation_id;
    return projection_status::batch;
}
void projection_store_ticket::release() noexcept { state_.reset(); }
std::shared_ptr<projection_pressure_source> make_projection_pressure_source(const std::shared_ptr<const physical_store_identity>& identity) {
    if (!identity) return {};
    auto source = std::make_shared<projection_pressure_source>();
    source->identity = identity;
    auto& registry = projection_store_registry::instance();
    std::lock_guard<std::mutex> lock(registry.mutex);
    registry.prune();
    auto node = registry.get(*identity);
    projection_store_registry::pressured(*node); // Prune expired weak sources.
    node->pressure.push_back(source);
    source->registry_anchor = std::move(node);
    return source;
}
void retire_projection_store(const std::shared_ptr<const physical_store_identity>& identity, projection_status reason) {
    if (!identity) return;
    std::vector<std::shared_ptr<database_read_control>> controls;
    auto& registry = projection_store_registry::instance();
    {
        std::lock_guard<std::mutex> lock(registry.mutex);
        auto it = registry.stores.find(key_for(*identity));
        if (it == registry.stores.end()) return;
        auto node = it->second.lock();
        if (!node) { registry.stores.erase(it); return; }
        if (node->epoch != std::numeric_limits<uint64_t>::max()) ++node->epoch;
        for (auto op = node->operations.begin(); op != node->operations.end();) {
            if (auto control = op->second.lock()) { controls.push_back(std::move(control)); ++op; }
            else op = node->operations.erase(op);
        }
    }
    // Never acquire a private target lock or invoke SQLite while holding the
    // registry lock. Tokens own no parent/service and cannot prolong lifetimes.
    for (const auto& control : controls) control->stop(static_cast<int32_t>(reason));
#ifndef __EMSCRIPTEN__
    projection_watchdog::wake_if_started();
#endif
}
size_t projection_store_readers(const std::shared_ptr<const physical_store_identity>& identity) {
    if (!identity) return 0;
    auto& registry = projection_store_registry::instance();
    std::lock_guard<std::mutex> lock(registry.mutex);
    auto it = registry.stores.find(key_for(*identity));
    if (it == registry.stores.end()) return 0;
    auto node = it->second.lock();
    if (!node) { registry.stores.erase(it); return 0; }
    size_t count = 0;
    for (auto op = node->operations.begin(); op != node->operations.end();) {
        if (op->second.expired()) op = node->operations.erase(op);
        else { ++count; ++op; }
    }
    return count;
}

struct projection_handle_lifetime {
    explicit projection_handle_lifetime(std::shared_ptr<projection_operation_state> state) : state(std::move(state)) {}
    ~projection_handle_lifetime() { state->signal(projection_status::closed); }
    std::shared_ptr<projection_operation_state> state;
};

void projection_operation_state::signal(projection_status reason) noexcept {
    if (!done.load(std::memory_order_acquire)) control->stop(static_cast<int32_t>(reason));
#ifndef __EMSCRIPTEN__
    // Never allocate or start a thread from cancel/close/destruction, including
    // a terminal handle returned after watchdog initialization failure.
    projection_watchdog::wake_if_started();
#endif
}
void projection_operation_state::notify_released() noexcept {
    void (*callback)(void*) = nullptr;
    void* context = nullptr;
    {
        std::lock_guard<std::mutex> lock(callback_mutex);
        if (callback_registered && !callback_delivered && !resources.load() && !executing.load() &&
            (done.load() || control->stop_code.load() != 0)) {
            callback_delivered = true;
            callback = released_callback;
            context = callback_context;
        }
    }
    if (callback) { try { callback(context); } catch (...) {} }
}
void projection_operation_state::cleanup_locked() noexcept {
    if (connection) {
        auto* handle = connection->internal_handle();
        control->unpublish(handle);
        sqlite3_progress_handler(handle, 0, nullptr, nullptr);
        sqlite3_busy_timeout(handle, 0);
        if (statement) { sqlite3_finalize(statement); statement = nullptr; }
        if (sqlite3_get_autocommit(handle) == 0) sqlite3_exec(handle, "ROLLBACK", nullptr, nullptr, nullptr);
        connection.reset();
    }
    // Borrowed SQL scopes must already have finalized/unlocked before stored
    // rows reach this operation. Teardown owns only immutable backing here.
    captured_rows.reset();
    if (capture_budget) { capture_budget->finish(); capture_budget.reset(); }
    if (leased) { leased = false; service->release_lease(); }
    stores.release(); // Connection/transaction and service lock are gone first.
    resources.store(false, std::memory_order_release);
    query.parameters.clear();
    service->erase(id);
}
void projection_operation_state::sweep() noexcept {
    if (!done.load()) {
        control->stopped();
        const int64_t start = lease_start_ms.load(), last = last_access_ms.load();
        if (start != 0 && ((max_age_ms > 0 && now_ms() - start >= max_age_ms) ||
            (idle_ttl_ms > 0 && !executing.load() && now_ms() - last >= idle_ttl_ms)))
            control->stop(static_cast<int32_t>(projection_status::snapshot_expired));
    }
    if (control->stop_code.load() != 0 || done.load()) {
        // Interrupt an active private connection even if progress is blocked
        // in a busy wait. The target cannot belong to a successor operation.
        if (!done.load()) control->stop(control->stop_code.load());
        std::unique_lock<std::mutex> lock(execution_mutex, std::try_to_lock);
        if (lock.owns_lock() && !executing.load()) cleanup_locked();
    }
    notify_released();
}

bool projection_service::claim() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_) return false;
    ++calls_; return true;
}
void projection_service::unclaim() {
    std::lock_guard<std::mutex> lock(mutex_); --calls_; settled_.notify_all();
}
bool projection_service::acquire_lease() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_ || leases_ >= kMaxLeases) return false;
    ++leases_; return true;
}
void projection_service::release_lease() {
    std::lock_guard<std::mutex> lock(mutex_); --leases_; settled_.notify_all();
}
std::shared_ptr<projection_capture_budget> projection_service::reserve_capture(size_t limit) {
    // A counted claim protects owner_ through this operation. Do not hold the
    // service mutex, SQL mutex or topology lock when acquiring the parent leaf.
    std::shared_ptr<projection_capture_account> account;
    {
        std::lock_guard<std::mutex> lock(owner_->projection_service_mutex_);
        if (!owner_->projection_capture_account_)
            owner_->projection_capture_account_ = std::make_shared<projection_capture_account>();
        account = owner_->projection_capture_account_;
    }
    return account->reserve(limit); // Account owns counters, never the parent.
}
void projection_service::erase(uint64_t id) {
    std::lock_guard<std::mutex> lock(mutex_); operations_.erase(id); settled_.notify_all();
}
size_t projection_service::resources() const {
    std::lock_guard<std::mutex> lock(mutex_); return leases_;
}
void projection_service::stop_all(projection_status reason, bool closing) {
    std::vector<std::shared_ptr<projection_operation_state>> states;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closing) closed_ = true;
        for (const auto& [_, state] : operations_) states.push_back(state);
    }
    for (const auto& state : states) state->signal(reason);
}
void projection_service::shutdown() {
    stop_all(projection_status::snapshot_expired, true);
    std::vector<std::shared_ptr<projection_operation_state>> states;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        settled_.wait(lock, [&] { return calls_ == 0; });
        for (const auto& [_, state] : operations_) states.push_back(state);
    }
    // Also reap idle leases on the closing thread. A release callback may call
    // parent.close() from the janitor; waiting for that same janitor to reap a
    // second idle lease would deadlock. No callbacks run inside these locks.
    for (const auto& state : states) {
        std::unique_lock<std::mutex> lock(state->execution_mutex);
        state->cleanup_locked();
    }
    for (const auto& state : states) state->notify_released();
    std::unique_lock<std::mutex> lock(mutex_);
    settled_.wait(lock, [&] { return leases_ == 0; });
    owner_ = nullptr;
}

projection_service::topology projection_service::capture(const std::shared_ptr<database_read_control>& control) {
    std::unique_lock<std::mutex> lock(owner_->attach_mutex_, std::defer_lock);
    while (!lock.try_lock()) {
        if (control->stopped()) fail(static_cast<projection_status>(control->stop_code.load()), "projection cancelled waiting for topology");
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (owner_->closed_.load()) fail(projection_status::closed, "lattice is closed");
    if (!owner_->db_) fail(projection_status::snapshot_expired, "writer is unavailable during exclusive maintenance");
    if (!owner_->attachment_topology_valid_) fail(projection_status::snapshot_expired, "attachment topology is incomplete");
    topology result;
    if (owner_->config_.path.empty())
        fail(projection_status::unsupported, "temporary empty-filename projection storage is unsupported");
    result.memory = owner_->config_.is_in_memory();
    result.attachments = owner_->attached_dbs_;
    for (const auto& [_, path] : result.attachments) {
        if (path.empty()) fail(projection_status::unsupported, "temporary projection attachment storage is unsupported");
        result.memory = result.memory || configuration::path_is_memory(path);
    }
    if (result.memory && !kMemoryCaptureEnabled)
        fail(projection_status::unsupported, "memory capture policy and service qualification is pending");
    if (!owner_->config_.is_in_memory()) {
        auto main_identity = owner_->db_->physical_identity("main", control, true);
        if (control->stopped()) fail(static_cast<projection_status>(control->stop_code.load()), "projection stopped waiting for store metadata");
        if (!main_identity) fail(projection_status::unsupported, "projection requires a supported stable local file identity");
        result.path = main_identity->filename;
        result.identities.push_back(main_identity);
        result.files_by_schema.emplace("main", main_identity);
    }
    for (auto& [alias, path] : result.attachments) {
        if (!owner_->attached_route_tokens_.count(alias)) fail(projection_status::snapshot_expired, "attachment token is invalid");
        if (configuration::path_is_memory(path)) continue;
        const auto expected = owner_->attached_projection_identities_.find(alias);
        if (expected == owner_->attached_projection_identities_.end() || !expected->second)
            fail(projection_status::unsupported, "attachment has no supported stable local file identity");
        auto actual = owner_->db_->physical_identity(alias, control, true);
        if (control->stopped()) fail(static_cast<projection_status>(control->stop_code.load()), "projection stopped waiting for attachment metadata");
        if (!actual || !(*actual == *expected->second))
            fail(projection_status::snapshot_expired, "attachment physical file changed");
        result.identities.push_back(expected->second);
        result.files_by_schema.emplace(alias, expected->second);
        path = expected->second->filename;
    }
    for (const auto& [_, sql] : owner_->attached_view_sql_) result.views.push_back(sql);
    result.idle_ttl_ms = owner_->read_generation_ttl_ms_.load();
    result.max_age_ms = owner_->read_generation_max_age_ms_.load();
    return result;
}


void projection_service::capture_memory(projection_operation_state& operation, const topology& topology) {
    // The caller holds a counted service claim, never the service mutex. That
    // claim protects owner_/db_ through this stack-only borrower and its locks.
    auto gate = owner_->store_write_gate();
    std::unique_lock<std::recursive_timed_mutex> gate_lock;
    if (gate) {
        gate_lock = std::unique_lock<std::recursive_timed_mutex>(*gate, std::try_to_lock);
        if (!gate_lock.owns_lock())
            fail(projection_status::admission_rejected, "memory projection writer gate is busy");
    }
    std::unique_lock<std::mutex> topology_lock(owner_->attach_mutex_, std::defer_lock);
    while (!topology_lock.try_lock()) {
        operation.check();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    operation.check();
    if (owner_->closed_.load() || !owner_->db_ || !owner_->attachment_topology_valid_)
        fail(projection_status::snapshot_expired, "memory projection topology is unavailable");
    std::vector<std::string> schemas{"main"};
    for (const auto& [alias, _] : topology.attachments) schemas.push_back(alias);
    {
        database_projection_capture capture(*owner_->db_, operation.control, operation.statement);
        // Raw ATTACH/DETACH is outside the owned memory-topology contract.
        // Still reject a detectable same-name file replacement before any
        // catalog/module work, while this connection can no longer rebind it.
        for (const auto& [schema, expected] : topology.files_by_schema) {
            auto actual = owner_->db_->physical_identity_locked(schema, operation.control);
            operation.check();
            if (!actual || !(*actual == *expected))
                fail(projection_status::snapshot_expired, "memory projection physical file changed before capture");
        }
        {
            projection_capture_policy policy(capture, operation.capture_budget, operation.query);
            if (!policy.matches_schemas(schemas))
                fail(projection_status::snapshot_expired, "untracked memory projection attachment topology");
            operation.prepare_query(capture.handle(), topology, &policy);
            operation.capture_all(capture.handle());
            operation.check();
        } // Finalize projected/catalog statements and detach authorizer delegate.
    } // Restore actual writer policy and release SQLite before other locks.
}

void projection_operation_state::capture_all(sqlite3* handle) {
    int64_t captured_count = 0, captured_bytes = 0;
    for (;;) {
        check();
        const int rc = sqlite3_step(statement);
        if (rc == SQLITE_DONE) { check(); return; }
        if (rc != SQLITE_ROW) {
            check();
            if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED ||
                (rc & 0xff) == SQLITE_BUSY || (rc & 0xff) == SQLITE_LOCKED)
                fail(projection_status::admission_rejected, "memory projection source is busy");
            throw db_error(sqlite3_errmsg(handle));
        }
        check();
        if (captured_count >= query.max_rows)
            fail(projection_status::row_budget_exceeded, "projection row budget exceeded");
        int64_t row_bytes = 0;
        for (int column = 0; column < column_count; ++column) {
            const int type = sqlite3_column_type(statement, column);
            if (type == SQLITE_TEXT && !sqlite3_column_text(statement, column))
                throw db_error("projection text conversion failed inside SQLite");
            const int64_t size = type == SQLITE_NULL ? 0 :
                (type == SQLITE_INTEGER || type == SQLITE_FLOAT ? 8 : sqlite3_column_bytes(statement, column));
            if (size < 0 || size > query.max_copied_bytes - captured_bytes - row_bytes)
                fail(projection_status::byte_budget_exceeded, "projection copied-byte budget exceeded");
            row_bytes += size;
        }
        // All encoded sizes are checked before copying. A partial append error
        // terminalizes the operation; the caller never retries this row/chunk.
        captured_rows->append(statement);
        captured_bytes += row_bytes;
        ++captured_count;
    }
}

projection_read_operation projection_service::start(const projection_query& query) {
    const auto entered_at = clock_type::now();
    const auto available_ms = std::chrono::duration_cast<std::chrono::milliseconds>(clock_type::time_point::max() - entered_at).count();
    const bool valid_timeout = query.timeout_ms > 0 && query.timeout_ms <= available_ms;
    const auto deadline = valid_timeout ? entered_at + std::chrono::milliseconds(query.timeout_ms) : entered_at;
#ifndef __EMSCRIPTEN__
    // No SQL/lease can open until this succeeds. Failed startup creates only
    // a terminal resource-free handle, never a service/state retain cycle.
    try { (void)projection_watchdog::instance(); }
    catch (const std::exception& failure) {
        auto state = std::make_shared<projection_operation_state>(shared_from_this(), projection_query{}, deadline);
        state->error = failure.what();
        state->control->stop_code.store(static_cast<int32_t>(projection_status::database_failure));
        return projection_read_operation(state);
    }
#endif
    projection_status rejection = projection_status::batch;
    const char* reason = "";
#ifdef __EMSCRIPTEN__
    rejection = projection_status::unsupported;
    reason = "WASM projection leases are not implemented";
#else
    // Validate bounded metadata BEFORE cloning caller-owned strings/bindings.
    // These are admission bounds, independent of copied result payload bytes.
    size_t request_bytes = 0;
    auto add_size = [&](size_t size) {
        if (size > static_cast<size_t>(kMaxRequestBytes) - request_bytes) return false;
        request_bytes += size; return true;
    };
    bool valid = !query.table.empty() && query.table.find('\0') == std::string::npos &&
        !query.columns.empty() && query.columns.size() <= kMaxColumns && query.order_columns.size() <= kMaxColumns &&
        query.parameters.size() <= 1024 &&
        query.limit >= -1 && query.offset >= 0 && query.max_rows >= 0 && query.max_copied_bytes >= 0 &&
        query.max_capture_bytes > 0 && query.max_capture_bytes <= static_cast<int64_t>(projection_capture_account::ceiling) &&
        valid_timeout;
    for (const auto* text : {&query.table, &query.where_clause, &query.order_by, &query.group_by, &query.distinct_by})
        valid = valid && text->find('\0') == std::string::npos && add_size(text->size());
    if (valid) for (const auto& column : query.columns)
        valid = valid && !column.empty() && column.find('\0') == std::string::npos && add_size(column.size());
    if (valid) for (const auto& column : query.order_columns)
        valid = valid && !column.empty() && column.find('\0') == std::string::npos && add_size(column.size());
    if (query.bounds) {
        const auto& bounds = *query.bounds;
        valid = valid && !bounds.column.empty() && bounds.column.find('\0') == std::string::npos &&
            add_size(bounds.column.size()) && std::isfinite(bounds.min_lat) && std::isfinite(bounds.max_lat) &&
            std::isfinite(bounds.min_lon) && std::isfinite(bounds.max_lon) &&
            bounds.min_lat <= bounds.max_lat && bounds.min_lon <= bounds.max_lon;
    } else if (query.has_bounds) valid = false;
    valid = valid && request_bytes <= 65536;
    if (valid) for (const auto& parameter : query.parameters) {
        const size_t size = std::visit([](const auto& cell) -> size_t {
            using T = std::decay_t<decltype(cell)>;
            if constexpr (std::is_same_v<T, std::string> || std::is_same_v<T, std::vector<uint8_t>>) return cell.size();
            else return 8;
        }, parameter);
        if (!add_size(size)) { valid = false; break; }
    }
    if (!valid) { rejection = projection_status::invalid_request; reason = "invalid projection request, admission bounds, or unrepresentable deadline"; }
#endif
    std::shared_ptr<projection_operation_state> state;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closed_) { rejection = projection_status::snapshot_expired; reason = "lattice is closed"; }
        else if (operations_.size() >= kMaxOperations) { rejection = projection_status::admission_rejected; reason = "projection pending operation bound exceeded"; }
        state = std::make_shared<projection_operation_state>(shared_from_this(),
            rejection == projection_status::batch ? query : projection_query{}, deadline);
        if (rejection != projection_status::batch) {
            state->error = reason;
            state->control->stop_code.store(static_cast<int32_t>(rejection));
        } else operations_.emplace(state->id, state);
    }
    projection_read_operation operation;
    try { operation = projection_read_operation(state); }
    catch (...) { erase(state->id); throw; }
#ifndef __EMSCRIPTEN__
    if (rejection == projection_status::batch) {
        try { projection_watchdog::instance().add(state); }
        catch (...) {
            state->control->stop_code.store(static_cast<int32_t>(projection_status::database_failure));
            erase(state->id); // No registration means no janitor-owned cleanup.
            try { state->error = "projection watchdog registration failed"; } catch (...) {}
        }
    }
#endif
    return operation;
}

void projection_operation_state::initialize() {
#ifdef __EMSCRIPTEN__
    fail(projection_status::unsupported, "WASM projection leases are not implemented");
#endif
    check();
    if (query.table.empty() || query.table.find('\0') != std::string::npos || query.columns.empty() ||
        query.columns.size() > kMaxColumns || query.limit < -1 || query.offset < 0 || query.max_rows < 0 ||
        query.max_copied_bytes < 0 || query.max_capture_bytes <= 0 ||
        query.max_capture_bytes > static_cast<int64_t>(projection_capture_account::ceiling) || query.timeout_ms <= 0)
        fail(projection_status::invalid_request, "invalid projection request or budget");
    if (!query.group_by.empty() && !query.distinct_by.empty() && !query.order_by.empty() && query.order_columns.empty())
        fail(projection_status::invalid_request, "nested projection grouping requires explicit ORDER BY column dependencies");
    size_t request_bytes = query.table.size() + query.where_clause.size() + query.order_by.size() +
        query.group_by.size() + query.distinct_by.size() + (query.bounds ? query.bounds->column.size() : 0);
    for (const auto& column : query.columns) {
        if (column.empty() || column.find('\0') != std::string::npos || column == "_source" || column == "_lattice_attach_token")
            fail(projection_status::invalid_request, "invalid projected stored column");
        request_bytes += column.size();
    }
    for (const auto& column : query.order_columns) request_bytes += column.size();
    for (const auto* column : {&query.group_by, &query.distinct_by})
        if (*column == "_source" || *column == "_lattice_attach_token")
            fail(projection_status::invalid_request, "routing metadata is not a projection grouping field");
    for (const auto& column : query.order_columns)
        if (column == "_source" || column == "_lattice_attach_token")
            fail(projection_status::invalid_request, "routing metadata is not a projection order field");
    if (request_bytes > 65536 || query.parameters.size() > 1024)
        fail(projection_status::invalid_request, "projection request metadata exceeds limit");
    for (const auto& parameter : query.parameters) {
        request_bytes += std::visit([](const auto& cell) -> size_t {
            using T = std::decay_t<decltype(cell)>;
            if constexpr (std::is_same_v<T, std::string> || std::is_same_v<T, std::vector<uint8_t>>) return cell.size();
            else return 8;
        }, parameter);
        if (request_bytes > kMaxRequestBytes) fail(projection_status::invalid_request, "projection bindings exceed limit");
    }
    if (static_cast<uint64_t>(query.max_rows) > std::numeric_limits<size_t>::max() / query.columns.size())
        fail(projection_status::invalid_request, "projection row/column budget overflows metadata size");
    auto topology = service->capture(control);
    // Epoch capture is outside topology/SQLite locks and precedes every private
    // open. Publication below rechecks epochs and live WAL-pressure sources.
    stores = capture_projection_stores(topology.identities);
    check();
    if (!service->acquire_lease()) {
        check();
        fail(projection_status::admission_rejected, "two owned projection leases are already active");
    }
    leased = true;
    resources.store(true, std::memory_order_release);
    const auto admission = stores.publish(id, control);
    if (admission != projection_status::batch) fail(admission, status_message(admission));
    idle_ttl_ms = topology.idle_ttl_ms; max_age_ms = topology.max_age_ms;
    lease_start_ms.store(now_ms()); last_access_ms.store(now_ms());
    if (topology.memory) {
        capture_budget = service->reserve_capture(static_cast<size_t>(query.max_capture_bytes));
        captured_rows = std::make_unique<projection_capture_storage>(capture_budget, column_count);
        service->capture_memory(*this, topology);
        check();
        initialized = true;
        return;
    }
    connection = std::make_unique<database>(readonly_uri(topology.path), database::open_mode::read_only, 0, control);
    auto private_identity = connection->physical_identity("main", control, true);
    check();
    if (!private_identity || !(*private_identity == *topology.identities.front()))
        fail(projection_status::snapshot_expired, "projection main file changed before private open");
    size_t attachment_index = 1;
    for (const auto& [alias, path] : topology.attachments) {
        check();
        connection->execute("ATTACH DATABASE ? AS " + quote_sql_identifier(alias), {readonly_uri(path)});
        if (sqlite3_db_readonly(connection->internal_handle(), alias.c_str()) != 1)
            fail(projection_status::database_failure, "projection attachment was not opened read-only");
        private_identity = connection->physical_identity(alias, control, true);
        check();
        if (!private_identity || !(*private_identity == *topology.identities[attachment_index++]))
            fail(projection_status::snapshot_expired, "projection attached file changed before private open");
    }
    for (const auto& view : topology.views) { check(); connection->execute(view); }
    connection->execute("PRAGMA query_only = ON");
    connection->execute("BEGIN");
    check();

    prepare_query(connection->internal_handle(), topology);
    initialized = true;
}

void projection_operation_state::prepare_query(sqlite3* handle,
    const projection_service::topology& topology, projection_capture_policy* policy) {
    // File metadata stays on its private connection; memory metadata comes
    // only from the preflighted policy while its schema guards remain active.
    sqlite3_stmt* metadata = nullptr;
    struct finalizer { sqlite3_stmt*& p; ~finalizer() { if (p) sqlite3_finalize(p); } } metadata_cleanup{metadata};
    bool has_source = false;
    if (policy) policy->validate_columns(query, has_source);
    else {
    const auto metadata_sql = "PRAGMA table_info(" + quote_sql_identifier(query.table) + ')';
    database::record_statement();
    int rc = sqlite3_prepare_v2(handle, metadata_sql.c_str(), -1, &metadata, nullptr);
    if (rc != SQLITE_OK) throw db_error(sqlite3_errmsg(handle));
    std::set<std::string> missing(query.columns.begin(), query.columns.end());
    missing.insert(query.order_columns.begin(), query.order_columns.end());
    if (!query.group_by.empty()) missing.insert(query.group_by);
    if (!query.distinct_by.empty()) missing.insert(query.distinct_by);
    size_t inspected_columns = 0;
    while ((rc = sqlite3_step(metadata)) == SQLITE_ROW) {
        check();
        if (++inspected_columns > 4096) fail(projection_status::schema_changed, "projection schema exceeds column bound");
        const auto* name = reinterpret_cast<const char*>(sqlite3_column_text(metadata, 1));
        const int length = sqlite3_column_bytes(metadata, 1);
        if (!name) throw db_error("projection schema name is unavailable");
        if (length == 7 && std::memcmp(name, "_source", 7) == 0) has_source = true;
        for (auto it = missing.begin(); it != missing.end();) {
            if (it->size() == static_cast<size_t>(length) && std::memcmp(it->data(), name, length) == 0) it = missing.erase(it);
            else ++it;
        }
    }
    if (rc != SQLITE_DONE) throw db_error(sqlite3_errmsg(handle));
    sqlite3_finalize(metadata); metadata = nullptr;
    if (!missing.empty()) fail(projection_status::schema_changed, "projected or shape-dependent stored column is missing");
    }

    std::string predicate = query.where_clause;
    if (query.bounds) {
        const auto& bounds = *query.bounds;
        // Resolve spatial storage on this same private SELECT snapshot. Bounds
        // properties expand to scalar columns or a separate list table, so the
        // property name itself need not appear in table_info(model).
        auto table_exists = [&](const std::string& schema, const std::string& table) {
            check();
            if (policy) return policy->table_exists(schema, table);
            const auto sql = "SELECT 1 FROM " + quote_sql_identifier(schema) +
                ".sqlite_master WHERE type='table' AND name=? LIMIT 1";
            database::record_statement();
            int result = sqlite3_prepare_v2(handle, sql.c_str(), -1, &metadata, nullptr);
            if (result != SQLITE_OK) throw db_error(sqlite3_errmsg(handle));
            bind_checked(metadata, 1, table);
            result = sqlite3_step(metadata);
            check();
            if (result != SQLITE_ROW && result != SQLITE_DONE) throw db_error(sqlite3_errmsg(handle));
            const bool exists = result == SQLITE_ROW;
            sqlite3_finalize(metadata); metadata = nullptr;
            return exists;
        };
        std::vector<spatial_query_arm> arms;
        auto add_arm = [&](const std::string& schema) {
            if (!table_exists(schema, query.table)) return;
            const auto list_table = "_" + query.table + "_" + bounds.column;
            if (!table_exists(schema, list_table + "_rtree"))
                fail(projection_status::schema_changed, "projection bounds index is missing from a physical store");
            arms.push_back({schema, table_exists(schema, list_table)});
        };
        add_arm("main");
        for (const auto& [alias, _] : topology.attachments) add_arm(alias);
        if (arms.empty()) fail(projection_status::schema_changed, "projection bounds table is missing");
        std::array<std::string, 4> coordinates;
        for (size_t i = 0; i != coordinates.size(); ++i)
            coordinates[i] = "?" + std::to_string(query.parameters.size() + i + 1);
        const auto membership = build_spatial_membership_predicate(query.table, bounds.column, arms,
            has_source && !topology.attachments.empty(), coordinates);
        predicate = predicate.empty() ? membership : "(" + predicate + ") AND (" + membership + ')';
    }

    std::string select;
    for (const auto& column : query.columns) { if (!select.empty()) select += ','; select += quote_sql_identifier(column); }
    // Keep only requested values and subsequent grouping/sort dependencies in
    // the intermediate row. Large unselected payloads must not enter a GROUP
    // BY temporary row. Representative values remain SQLite's unspecified
    // group member, just as in the existing model query API.
    std::string from = quote_sql_identifier(query.table);
    auto group = query.group_by.empty() ? std::nullopt : std::optional<std::string>(quote_sql_identifier(query.group_by));
    auto distinct = query.distinct_by.empty() ? std::nullopt : std::optional<std::string>(quote_sql_identifier(query.distinct_by));
    if (group && distinct) {
        std::set<std::string> included;
        std::string inner;
        auto include = [&](const std::string& column) {
            if (!included.insert(column).second) return;
            if (!inner.empty()) inner += ',';
            inner += quote_sql_identifier(column);
        };
        for (const auto& column : query.columns) include(column);
        include(query.group_by); include(query.distinct_by);
        for (const auto& column : query.order_columns) include(column);
        from = "(SELECT " + inner + " FROM " + from +
            (predicate.empty() ? "" : " WHERE " + predicate) + " GROUP BY " + *distinct + ") AS " + quote_sql_identifier(query.table);
        predicate.clear(); distinct.reset();
    }
    auto sql = lattice_db::build_query_rows_sql(from,
        predicate.empty() ? std::nullopt : std::optional<std::string>(predicate),
        query.order_by.empty() ? std::nullopt : std::optional<std::string>(query.order_by),
        (query.limit >= 0 || query.offset > 0) ? std::optional<int64_t>(query.limit) : std::nullopt,
        query.offset > 0 ? std::optional<int64_t>(query.offset) : std::nullopt,
        group, distinct, select);
    const char* tail = nullptr;
    if (policy) policy->prepare_read(sql);
    else {
        database::record_statement();
        const int rc = sqlite3_prepare_v2(handle, sql.c_str(), -1, &statement, &tail);
        if (rc != SQLITE_OK) throw db_error(sqlite3_errmsg(handle));
    }
    while (tail && *tail && std::isspace(static_cast<unsigned char>(*tail))) ++tail;
    if (!statement || (tail && *tail) || !sqlite3_stmt_readonly(statement) ||
        sqlite3_column_count(statement) != column_count ||
        sqlite3_bind_parameter_count(statement) != static_cast<int>(query.parameters.size() + (query.bounds ? 4 : 0)))
        fail(projection_status::invalid_request, "projection must be one read-only SELECT with matching bindings");
    for (size_t i = 0; i < query.parameters.size(); ++i) bind_checked(statement, static_cast<int>(i + 1), query.parameters[i]);
    if (query.bounds) {
        const auto& bounds = *query.bounds;
        const std::array<double, 4> values{bounds.min_lat, bounds.max_lat, bounds.min_lon, bounds.max_lon};
        for (size_t i = 0; i != values.size(); ++i)
            bind_checked(statement, static_cast<int>(query.parameters.size() + i + 1), values[i]);
    }
}

projection_read_batch projection_operation_state::next(int64_t max_rows) {
    bool expected = false;
    if (!executing.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        signal(projection_status::concurrent_next);
        projection_read_batch batch;
        batch.status_ = projection_status::concurrent_next;
        batch.error_ = "concurrent projection nextBatch calls";
        return batch; // The original caller/watchdog owns the cleanup acknowledgement.
    }
    struct finish {
        projection_operation_state* state;
        bool claimed = false;
        ~finish() {
            if (claimed) state->service->unclaim();
            state->executing.store(false, std::memory_order_release);
            state->notify_released();
        }
    } finish_guard{this};
    std::unique_lock<std::mutex> lock(execution_mutex);
    if (done.load()) return result(projection_status::done);
    if (!service->claim()) {
        control->stop(static_cast<int32_t>(projection_status::closed));
    } else finish_guard.claimed = true;
    try {
        check();
        if (max_rows <= 0) fail(projection_status::invalid_request, "batch size must be positive");
        if (!initialized) initialize();
        const size_t row_capacity = static_cast<size_t>(std::min<int64_t>(max_rows, kMaxBatchRows));
        if (column_count <= 0 || row_capacity > std::numeric_limits<size_t>::max() / static_cast<size_t>(column_count))
            fail(projection_status::invalid_request, "batch metadata size overflow");
        if (captured_rows) {
            auto captured = captured_rows->take(static_cast<int64_t>(row_capacity));
            check();
            rows += captured->row_count(); bytes += captured->copied_bytes();
            const bool complete = captured->row_count() < static_cast<int64_t>(row_capacity);
            if (complete) { done.store(true); cleanup_locked(); }
            else last_access_ms.store(now_ms());
            auto batch = result(complete ? projection_status::done : projection_status::batch);
            batch.captured_ = std::move(captured);
            return batch;
        }
        auto cells = std::make_shared<std::vector<column_value_t>>();
        const size_t capacity = row_capacity * static_cast<size_t>(column_count);
        if (capacity > cells->max_size()) fail(projection_status::invalid_request, "batch metadata exceeds container limit");
        cells->reserve(capacity); // Fixed metadata bounded independently of payload bytes.
        size_t batch_rows = 0;
        while (batch_rows < row_capacity) {
            check();
            const int rc = sqlite3_step(statement);
            if (rc == SQLITE_DONE) {
                check(); done.store(true); cleanup_locked();
                return result(projection_status::done, std::move(cells));
            }
            if (rc != SQLITE_ROW) { check(); throw db_error(sqlite3_errmsg(connection->internal_handle())); }
            check();
            if (rows >= query.max_rows) fail(projection_status::row_budget_exceeded, "projection row budget exceeded");
            int64_t row_bytes = 0;
            // Inspect the complete row before allocating ANY text/blob copies.
            for (int column = 0; column < column_count; ++column) {
                const int type = sqlite3_column_type(statement, column);
                // Force SQLite's UTF-8 representation before measuring it;
                // any conversion workspace belongs to SQLite, not copied cells.
                if (type == SQLITE_TEXT && !sqlite3_column_text(statement, column))
                    throw db_error("projection text conversion failed inside SQLite");
                const int64_t size = type == SQLITE_NULL ? 0 :
                    (type == SQLITE_INTEGER || type == SQLITE_FLOAT ? 8 : sqlite3_column_bytes(statement, column));
                if (size < 0 || size > query.max_copied_bytes - bytes - row_bytes)
                    fail(projection_status::byte_budget_exceeded, "projection copied-byte budget exceeded");
                row_bytes += size;
            }
            for (int column = 0; column < column_count; ++column) {
                switch (sqlite3_column_type(statement, column)) {
                    case SQLITE_NULL: cells->emplace_back(nullptr); break;
                    case SQLITE_INTEGER: cells->emplace_back(static_cast<int64_t>(sqlite3_column_int64(statement, column))); break;
                    case SQLITE_FLOAT: cells->emplace_back(sqlite3_column_double(statement, column)); break;
                    case SQLITE_TEXT: {
                        const auto* data = reinterpret_cast<const char*>(sqlite3_column_text(statement, column));
                        const int size = sqlite3_column_bytes(statement, column);
                        if (!data) throw db_error("projection text allocation failed inside SQLite");
                        cells->emplace_back(std::string(data, static_cast<size_t>(size))); break;
                    }
                    case SQLITE_BLOB: {
                        const auto* data = static_cast<const uint8_t*>(sqlite3_column_blob(statement, column));
                        const int size = sqlite3_column_bytes(statement, column);
                        if (size && !data) throw db_error("projection blob allocation failed inside SQLite");
                        cells->emplace_back(size ? std::vector<uint8_t>(data, data + size) : std::vector<uint8_t>{}); break;
                    }
                    default: fail(projection_status::database_failure, "unknown SQLite cell type");
                }
            }
            bytes += row_bytes; ++rows; ++batch_rows;
        }
        check(); last_access_ms.store(now_ms());
        return result(projection_status::batch, std::move(cells));
    } catch (const projection_capture_failure& failure) {
        control->stop(static_cast<int32_t>(failure.status));
        try { error = failure.what(); } catch (...) {}
    } catch (const projection_failure& failure) {
        control->stop(static_cast<int32_t>(failure.status));
        try { error = failure.what(); } catch (...) {}
    } catch (const std::exception& failure) {
        if (control->stop_code.load() == 0) control->stop(static_cast<int32_t>(projection_status::database_failure));
        try { error = failure.what(); } catch (...) {}
    } catch (...) {
        control->stop(static_cast<int32_t>(projection_status::database_failure));
    }
    cleanup_locked();
    return result(static_cast<projection_status>(control->stop_code.load()));
}

projection_read_operation::projection_read_operation(std::shared_ptr<projection_operation_state> state)
    : handle_(std::make_shared<projection_handle_lifetime>(std::move(state))) {}
uint64_t projection_read_operation::operation_id() const noexcept { return handle_ ? handle_->state->id : 0; }
projection_read_batch projection_read_operation::next_batch(int64_t max_rows) const {
    if (handle_) return handle_->state->next(max_rows);
    projection_read_batch batch;
    batch.status_ = projection_status::invalid_request; batch.error_ = "empty projection handle"; return batch;
}
void projection_read_operation::cancel() const noexcept { if (handle_) handle_->state->signal(projection_status::cancelled); }
void projection_read_operation::close() const noexcept { if (handle_) handle_->state->signal(projection_status::closed); }
bool projection_read_operation::is_terminal() const noexcept {
    return !handle_ || handle_->state->done.load() || handle_->state->control->stop_code.load() != 0;
}
bool projection_read_operation::has_resources() const noexcept { return handle_ && handle_->state->resources.load(); }
bool projection_read_operation::when_released(void* context, void (*callback)(void*)) const noexcept {
    if (!callback) return false;
    if (!handle_) { try { callback(context); } catch (...) {} return true; }
    auto state = handle_->state;
    {
        std::lock_guard<std::mutex> lock(state->callback_mutex);
        if (state->callback_registered) return false;
        state->callback_registered = true; state->callback_context = context; state->released_callback = callback;
    }
    state->notify_released(); return true;
}
int64_t projection_read_batch::row_count() const noexcept {
    if (captured_) return captured_->row_count();
    return cells_ && columns_ > 0 ? static_cast<int64_t>(cells_->size() / columns_) : 0;
}
column_value_t projection_read_batch::value(int64_t row, int64_t column) const {
    if (captured_) return captured_->value(row, column);
    if (!cells_ || row < 0 || column < 0 || row >= row_count() || column >= columns_)
        throw std::out_of_range("projection cell index out of range");
    return (*cells_)[static_cast<size_t>(row) * static_cast<size_t>(columns_) + static_cast<size_t>(column)];
}

projection_read_operation lattice_db::start_projection(const projection_query& query) {
    std::shared_ptr<projection_service> service;
    {
        std::lock_guard<std::mutex> lock(projection_service_mutex_);
        if (!projection_service_) projection_service_ = std::make_shared<projection_service>(this);
        service = projection_service_;
        if (closed_.load() || projection_admission_paused_) service->stop_all(projection_status::snapshot_expired, true);
    }
    return service->start(query);
}
void lattice_db::cancel_projection_reads(projection_status reason) {
    std::shared_ptr<projection_service> service;
    { std::lock_guard<std::mutex> lock(projection_service_mutex_); service = projection_service_; }
    if (service) service->stop_all(reason, false);
}
size_t lattice_db::projection_resources_outstanding() const {
    std::shared_ptr<projection_service> service;
    { std::lock_guard<std::mutex> lock(projection_service_mutex_); service = projection_service_; }
    return service ? service->resources() : 0;
}
void lattice_db::shutdown_projection_reads() {
    std::shared_ptr<projection_service> service;
    { std::lock_guard<std::mutex> lock(projection_service_mutex_); service = projection_service_; }
    if (service) service->shutdown();
}
void lattice_db::pause_projection_reads() {
    std::shared_ptr<projection_service> service;
    {
        std::lock_guard<std::mutex> lock(projection_service_mutex_);
        projection_admission_paused_ = true;
        service = projection_service_;
    }
    if (service) service->shutdown(); // No topology/SQLite/registry lock held.
}
void lattice_db::resume_projection_reads() {
    std::lock_guard<std::mutex> lock(projection_service_mutex_);
    if (closed_.load()) return;
    projection_service_.reset(); // Old handles retain their terminal service.
    projection_admission_paused_ = false;
}

} // namespace lattice
