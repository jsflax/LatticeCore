#include "recovery_refresh.hpp"
#include <limits>
#include <set>
#include <map>

namespace lattice::detail {
namespace {
struct listener {
    std::atomic<bool> active{true};
    std::function<void()> callback;
    explicit listener(std::function<void()> f) : callback(std::move(f)) {}
};
// Matches instance_registry's admission protocol. Only the heap guard is
// accessed after user callbacks (which may close/destroy the facade).
struct owner_hold {
    std::shared_ptr<instance_guard> guard;
    bool admitted = false;
    explicit owner_hold(std::shared_ptr<instance_guard> g) : guard(std::move(g)) {
        ++instance_guard::tls_depths()[guard.get()]; // allocate before publishing
        guard->notify_refcount.fetch_add(1, std::memory_order_seq_cst);
        admitted = guard->alive.load(std::memory_order_seq_cst);
    }
    ~owner_hold() {
        auto& depths = instance_guard::tls_depths();
        if (--depths[guard.get()] == 0) depths.erase(guard.get());
        guard->notify_refcount.fetch_sub(1, std::memory_order_seq_cst);
    }
};
struct statement_guard {
    sqlite3_mutex* mutex;
    bool owned;
    explicit statement_guard(sqlite3* db) : mutex(sqlite3_db_mutex(db)), owned(sqlite3_mutex_try(mutex) == SQLITE_OK) {}
    ~statement_guard() { if (owned) sqlite3_mutex_leave(mutex); }
};
bool settled(database& writer, sqlite3* h, std::shared_ptr<const physical_store_identity>* identity = nullptr) {
    statement_guard guard(h);
    if (!guard.owned || writer.is_closed() || sqlite3_get_autocommit(h) == 0) return false;
    for (auto* s = sqlite3_next_stmt(h, nullptr); s; s = sqlite3_next_stmt(h, s))
        if (sqlite3_stmt_busy(s)) return false;
    if (identity) *identity = writer.physical_identity("main", {}, true);
    return true;
}
std::string text(const database::row_t& row, const char* key) {
    const auto found = row.find(key);
    if (found == row.end() || !std::holds_alternative<std::string>(found->second))
        throw db_error("recovery refresh schema text unavailable");
    return std::get<std::string>(found->second);
}
using field_map = std::map<std::string, std::set<std::string>>;
field_map fields_for(database& reader, const std::set<std::string>& subscribed) {
    field_map fields;
    for (const auto& table : subscribed) {
        const auto columns = reader.query("SELECT name FROM pragma_table_info(?, 'main') ORDER BY cid", {table});
        if (columns.empty()) throw db_error("recovery refresh subscribed schema missing");
        std::set<std::string> physical;
        for (const auto& column : columns) physical.insert(text(column, "name"));
        auto& names = fields[table];
        for (const auto& name : physical) {
            if (name != "id" && name != "globalId") names.insert(name);
            constexpr std::string_view suffix = "_minLat";
            if (name.size() <= suffix.size() || !name.ends_with(suffix)) continue;
            const auto prefix = name.substr(0, name.size() - suffix.size());
            if (physical.contains(prefix + "_maxLat") && physical.contains(prefix + "_minLon") &&
                physical.contains(prefix + "_maxLon")) names.insert(prefix);
        }
    }
    if (subscribed.empty()) return fields;
    // Schema work, not a row scan. Keep complete logical relation names for
    // current held parents, including union tables with several parent maps.
    const auto mappings = reader.query("SELECT value FROM main._lattice_meta WHERE key LIKE 'internal_table:%'");
    for (const auto& row : mappings) {
        const auto mapping = text(row, "value");
        size_t start = 0;
        while (start < mapping.size()) {
            const auto end = mapping.find(';', start);
            const auto item = mapping.substr(start, end == std::string::npos ? end : end - start);
            const auto colon = item.find(':');
            if (colon != std::string::npos && colon + 1 < item.size()) {
                const auto table = item.substr(0, colon);
                if (subscribed.contains(table)) fields[table].insert(item.substr(colon + 1));
            }
            if (end == std::string::npos) break;
            start = end + 1;
        }
    }
    return fields;
}
} // namespace

struct recovery_refresh_state {
    std::mutex mutex; // leaf: never SQL, scheduler calls, or callback destruction
    uint64_t revision = 0, acknowledged_revision = 0, next_id = 1;
    bool in_flight = false;
    bool witness_was_missing = false;
    std::optional<recovery_witness> acknowledged;
    std::map<uint64_t, std::shared_ptr<listener>> listeners;
    std::shared_ptr<database> probe_reader; // private, one admitted drain only
};

std::shared_ptr<recovery_refresh_state> recovery_refresh_access::state(lattice_db& owner, bool create) {
    auto fresh = create ? std::make_shared<recovery_refresh_state>() : nullptr;
    std::lock_guard<std::mutex> lock(owner.connection_ownership_mutex_);
    if (owner.closed_.load()) return {};
    if (!owner.recovery_refresh_ && fresh) owner.recovery_refresh_ = std::move(fresh);
    return owner.recovery_refresh_;
}
uint64_t recovery_refresh_access::subscribe(lattice_db& owner, std::function<void()> callback) {
    if (!callback) throw db_error("recovery refresh callback required");
    auto item = std::make_shared<listener>(std::move(callback));
    auto shared = state(owner, true);
    if (!shared) throw db_error("recovery refresh owner closed");
    uint64_t id;
    {
        std::lock_guard<std::mutex> lock(shared->mutex);
        if (shared->revision == UINT64_MAX || shared->next_id == UINT64_MAX)
            throw db_error("recovery refresh subscription counter exhausted");
        id = shared->next_id++;
        shared->listeners.emplace(id, item);
        ++shared->revision;
    }
    request(owner);
    return id;
}
void recovery_refresh_access::unsubscribe(lattice_db& owner, uint64_t id) {
    auto shared = state(owner, false);
    if (!shared) return;
    std::shared_ptr<listener> removed;
    {
        std::lock_guard<std::mutex> lock(shared->mutex);
        const auto found = shared->listeners.find(id);
        if (found == shared->listeners.end()) return;
        removed.swap(found->second);
        removed->active.store(false, std::memory_order_release);
        shared->listeners.erase(found);
    }
}
void recovery_refresh_access::subscriptions_changed(lattice_db& owner) {
    auto shared = state(owner, false);
    if (!shared) return; // ordinary-only observers incur no witness reads
    {
        std::lock_guard<std::mutex> lock(shared->mutex);
        if (shared->revision == UINT64_MAX) throw db_error("recovery refresh subscription counter exhausted");
        ++shared->revision;
    }
    request(owner);
}
void recovery_refresh_access::request(lattice_db& owner) noexcept {
#ifdef __EMSCRIPTEN__
    (void)owner; // Browser filesystem/thread/observer capability is unqualified.
#else
    try {
        auto shared = state(owner, false);
        if (!shared || owner.config_.is_in_memory()) return;
        auto guard = owner.guard_;
        auto scheduler = owner.scheduler_;
        {
            std::lock_guard<std::mutex> lock(shared->mutex);
            if (shared->in_flight || shared->listeners.empty()) return;
            shared->in_flight = true;
        }
        struct ticket {
            std::shared_ptr<recovery_refresh_state> shared;
            explicit ticket(std::shared_ptr<recovery_refresh_state> s) : shared(std::move(s)) {}
            ~ticket() { std::lock_guard<std::mutex> lock(shared->mutex); shared->in_flight = false; }
        };
        // Allocation failure also releases admission; rejected/discarded work
        // is not acknowledged. Only a live scheduler-owned ticket keeps it busy.
        std::shared_ptr<ticket> work;
        try { work = std::make_shared<ticket>(shared); }
        catch (...) { std::lock_guard<std::mutex> lock(shared->mutex); shared->in_flight = false; throw; }
        scheduler->invoke([shared, guard, raw = &owner, work] {
            try {
                owner_hold hold(guard);
                if (hold.admitted) deliver(*raw, shared);
            } catch (...) { /* Leave the unacknowledged tuple/revision pending. */ }
        });
    } catch (...) { /* The next explicit/notifier/poll request retries. */ }
#endif
}

void recovery_refresh_access::deliver(lattice_db& owner, const std::shared_ptr<recovery_refresh_state>& shared) {
    uint64_t subscription_revision, connection_revision;
    std::optional<recovery_witness> acknowledged;
    std::shared_ptr<database> probe, writer;
    {
        std::lock_guard<std::mutex> lock(shared->mutex);
        if (shared->listeners.empty()) return;
        subscription_revision = shared->revision;
        acknowledged = shared->acknowledged;
        probe = shared->probe_reader;
    }
    {
        std::lock_guard<std::mutex> lock(owner.connection_ownership_mutex_);
        if (owner.closed_.load()) return;
        connection_revision = owner.connection_revision_;
        writer = owner.db_;
    }
    // Native held scalar fields still use their captured physical writer.
    // Preserve explicit transaction/statement snapshots; retry after settlement.
    std::shared_ptr<const physical_store_identity> writer_identity;
    if (!writer || !settled(*writer, writer->internal_handle(), &writer_identity)) return;
    if (!probe) {
        probe = std::make_shared<database>(owner.config_.path, database::open_mode::read_only, owner.config_.busy_timeout_ms);
        std::lock_guard<std::mutex> lock(shared->mutex);
        shared->probe_reader = probe;
    }
    const auto probe_identity = probe->physical_identity("main", {}, true);
    if (!writer_identity || !probe_identity || !(*probe_identity == *writer_identity))
        throw db_error("recovery refresh physical store changed");
    probe->execute("BEGIN");
    bool reading = true;
    std::optional<recovery_witness> witness;
    field_map names;
    try {
        witness = read_recovery_witness(*probe);
        bool same;
        { std::lock_guard<std::mutex> lock(shared->mutex);
          if (!witness && acknowledged) {
              // Initial legacy absence is inert. Losing a previously observed
              // witness is not evidence that content stayed unchanged. Keep
              // the old acknowledgement and require refresh after restoration,
              // even if the restored tuple happens to equal the old one.
              shared->witness_was_missing = true;
              throw db_error("previously observed recovery witness disappeared");
          }
          same = !shared->witness_was_missing && witness == acknowledged && shared->acknowledged_revision == subscription_revision; }
        if (witness && !same) {
            std::set<std::string> tables;
            { std::lock_guard<std::mutex> lock(owner.object_observers_mutex_);
              for (const auto& [table, _] : owner.object_observers_) tables.insert(table); }
            names = fields_for(*probe, tables);
        }
        probe->execute("COMMIT"); reading = false;
        if (same || !witness) {
            std::lock_guard<std::mutex> lock(shared->mutex);
            shared->acknowledged = witness; shared->acknowledged_revision = subscription_revision;
            return;
        }
    } catch (...) {
        if (reading) {
            try { probe->execute("ROLLBACK"); }
            catch (...) {
                // A failed private reader cleanup must not retain an old view
                // or make every subsequent BEGIN fail. Retire it off-lock.
                std::shared_ptr<database> retired;
                { std::lock_guard<std::mutex> lock(shared->mutex);
                  if (shared->probe_reader == probe) retired.swap(shared->probe_reader); }
            }
        }
        throw;
    }
    auto fresh = std::make_shared<database>(owner.config_.path, database::open_mode::read_only, owner.config_.busy_timeout_ms);
    const auto fresh_identity = fresh->physical_identity("main", {}, true);
    if (!fresh_identity || !(*fresh_identity == *writer_identity))
        throw db_error("recovery refresh reader physical store changed");
    {
        // Same non-waiting topology/publication discipline as reopen_read_db.
        std::unique_lock<std::mutex> topology(owner.attach_mutex_, std::try_to_lock);
        if (!topology.owns_lock()) return;
        owner.restore_attached_views(*fresh);
        std::lock_guard<std::mutex> lock(owner.connection_ownership_mutex_);
        if (owner.closed_.load() || owner.connection_revision_ != connection_revision || owner.db_ != writer) return;
        ++owner.connection_revision_;
        fresh.swap(owner.read_db_); // old borrowers retain their own old view
    }
    // Release the old reader outside locks. No retired-reader list is retained.
    fresh.reset();
    if (!settled(*writer, writer->internal_handle())) return;

    std::vector<lattice_db::invalidation_hook_detailed_fn> invalidations;
    std::vector<std::function<void()>> objects;
    std::vector<std::shared_ptr<listener>> listeners;
    auto guard = owner.guard_;
    {
        std::lock_guard<std::mutex> lock(owner.invalidation_hooks_mutex_);
        for (const auto& [_, fn] : owner.invalidation_hooks_) invalidations.push_back(fn);
    }
    {
        std::lock_guard<std::mutex> lock(owner.object_observers_mutex_);
        for (const auto& [table, rows] : owner.object_observers_) {
            const auto found = names.find(table);
            if (found == names.end()) continue; // later registration retains newer revision
            const auto fields = nlohmann::json(found->second).dump();
            for (const auto& [_, entries] : rows) for (const auto& [id, fn] : entries)
                objects.push_back([fn, fields] { fn(fields); });
        }
    }
    {
        std::lock_guard<std::mutex> lock(shared->mutex);
        for (const auto& [_, item] : shared->listeners) listeners.push_back(item);
    }
    // No owner access after this point: a callback may release its last owner.
    // Coalescible payload-free refreshes can repeat after a failed callback.
    bool success = true;
    auto invoke = [&](auto&& fn) {
        if (!guard->alive.load(std::memory_order_seq_cst)) { success = false; return; }
        try { fn(); } catch (...) { success = false; }
    };
    for (const auto& fn : invalidations) invoke([&] { fn({}, lattice_db::invalidation_reason::recovery); });
    for (const auto& fn : objects) invoke(fn);
    for (const auto& item : listeners) if (item->active.load(std::memory_order_acquire)) invoke(item->callback);
    if (success) {
        std::lock_guard<std::mutex> lock(shared->mutex);
        shared->acknowledged = witness; shared->acknowledged_revision = subscription_revision;
        shared->witness_was_missing = false;
    }
}
} // namespace lattice::detail

namespace lattice {
uint64_t lattice_db::add_recovery_refresh_observer(std::function<void()> callback) {
    return detail::recovery_refresh_access::subscribe(*this, std::move(callback));
}
void lattice_db::remove_recovery_refresh_observer(uint64_t token) { detail::recovery_refresh_access::unsubscribe(*this, token); }
void lattice_db::request_recovery_refresh() noexcept { detail::recovery_refresh_access::request(*this); }
void lattice_db::recovery_subscriptions_changed() { detail::recovery_refresh_access::subscriptions_changed(*this); }
} // namespace lattice
