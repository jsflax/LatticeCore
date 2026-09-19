#include <lattice/lattice.hpp>
#include <atomic>
#include <limits>

namespace lattice::detail {
struct managed_observation_state::state {
    struct slot {
        std::mutex mutex;
        std::shared_ptr<const callback> callable;
        std::string table, global_id;
        int64_t row = 0;
        uint64_t id = 0;
        bool delete_queued = false; // guarded by state::mutex
        std::vector<std::string> fallback;

        void cancel() {
            std::shared_ptr<const callback> released;
            { std::lock_guard<std::mutex> lock(mutex); released.swap(callable); }
        }
        std::shared_ptr<const callback> admit(const state& owner, bool deleted) {
            std::lock_guard<std::mutex> lock(mutex);
            if (owner.retired.load(std::memory_order_acquire)) return {};
            // Only shared_ptr copies occur under the leaf lock. User capture
            // copy/destruction and invocation never run under our locks.
            auto admitted = callable;
            if (deleted) callable.reset(); // admitted retains the last capture
            return admitted;
        }
    };
    using slots = std::map<uint64_t, std::shared_ptr<slot>>;
    using rows = std::map<int64_t, slots>;
    std::mutex mutex;
    std::atomic<bool> retired{false};
    std::map<std::string, rows> tables;
    uint64_t next_id = 0;

    void erase(const slot& target) {
        // The caller retains target, so erasure cannot release its captures.
        std::lock_guard<std::mutex> lock(mutex);
        const auto t = tables.find(target.table);
        if (t == tables.end()) return;
        const auto r = t->second.find(target.row);
        if (r == t->second.end()) return;
        r->second.erase(target.id);
        if (r->second.empty()) t->second.erase(r);
        if (t->second.empty()) tables.erase(t);
    }
};

managed_observation_state::managed_observation_state() : state_(std::make_shared<state>()) {}
managed_observation_state::~managed_observation_state() { retire(); }

notification_token managed_observation_state::observe(const std::string& table, int64_t row,
    const std::string& global_id, std::vector<std::string> fallback, callback cb) {
    if (!cb) return {};
    auto target = std::make_shared<state::slot>();
    target->callable = std::make_shared<const callback>(std::move(cb));
    target->table = table; target->row = row;
    target->global_id = global_id;
    target->fallback = std::move(fallback);
    auto owner = state_;
    {
        std::lock_guard<std::mutex> lock(owner->mutex);
        if (owner->retired.load(std::memory_order_acquire)) return {};
        if (owner->next_id == std::numeric_limits<uint64_t>::max())
            throw std::overflow_error("managed observer identifiers exhausted");
        target->id = ++owner->next_id;
        owner->tables[table][row].emplace(target->id, target);
    }
    try {
        return notification_token([owner, target] {
            target->cancel();
            owner->erase(*target);
        });
    } catch (...) {
        target->cancel();
        owner->erase(*target);
        throw;
    }
}

void managed_observation_state::append(const std::string& table, const std::string& operation,
    int64_t row, const std::string& global_id, const std::string& fields_json,
    std::vector<std::function<void()>>& callbacks) {
    auto owner = state_;
    const bool deleted = operation == "DELETE";
    const bool broadcast = row == 0 && operation == "UPDATE" && !fields_json.empty();
    if ((!broadcast && row <= 0) || (operation != "UPDATE" && operation != "INSERT" && !deleted)) return;
    std::vector<std::shared_ptr<state::slot>> targets;
    {
        std::lock_guard<std::mutex> lock(owner->mutex);
        if (owner->retired.load(std::memory_order_acquire)) return;
        const auto t = owner->tables.find(table);
        if (t == owner->tables.end()) return;
        const auto collect = [&](const state::slots& slots) {
            for (const auto& [id, target] : slots) {
                // DELETE hooks have no surviving row from which to read a
                // globalId. The physical row registration is retired below.
                if (!target->delete_queued &&
                    (broadcast || (deleted && global_id.empty()) || global_id == target->global_id)) {
                    targets.push_back(target);
                    if (deleted) target->delete_queued = true;
                }
            }
        };
        if (broadcast) {
            for (const auto& [id, slots] : t->second) collect(slots);
        } else {
            const auto r = t->second.find(row);
            if (r == t->second.end()) return;
            collect(r->second);
            // Keep queued deletions indexed until dispatch/cancellation so
            // owner retirement releases their captures too. delete_queued
            // fences later reuse of this physical row before dispatch.
        }
    }
    if (targets.empty()) return;
    std::vector<std::string> fields;
    if (!deleted && !fields_json.empty()) {
        const auto parsed = nlohmann::json::parse(fields_json, nullptr, false);
        if (parsed.is_array()) {
            for (const auto& value : parsed) {
                if (!value.is_string()) { fields.clear(); break; }
                fields.push_back(value.get<std::string>());
            }
        }
    }
    for (const auto& target : targets) {
        auto names = deleted ? std::vector<std::string>{} : fields.empty() ? target->fallback : fields;
        callbacks.emplace_back([owner, target, deleted, names = std::move(names)] {
            auto admitted = target->admit(*owner, deleted);
            if (deleted) owner->erase(*target);
            if (admitted) (*admitted)(deleted, names);
        });
    }
}

void managed_observation_state::retire() {
    auto owner = state_;
    decltype(owner->tables) released;
    {
        std::lock_guard<std::mutex> lock(owner->mutex);
        owner->retired.store(true, std::memory_order_release);
        released.swap(owner->tables);
    }
    for (const auto& [table, rows] : released)
        for (const auto& [row, slots] : rows)
            for (const auto& [id, target] : slots) target->cancel();
}
} // namespace lattice::detail

namespace lattice {
void model_base::notify_managed_observers(bool deleted, const std::vector<std::string>& fields) {
    if (!is_managed() || global_id_.empty()) return;
    if (!lattice_) {
        const auto key = std::make_pair(table_name_, global_id_);
        if (deleted) object_observer_registry::instance().notify_deleted(key);
        else object_observer_registry::instance().notify(key, false, fields);
        return;
    }
    const auto route = managed_route(table_name_);
    if (attachment_token_ != 0 || route.schema_sql != "main")
        throw std::invalid_argument("typed observation of an attached model is unsupported");
    std::vector<std::function<void()>> work;
    lattice_->managed_observers_.append(route.table, deleted ? "DELETE" : "UPDATE",
        id_, global_id_, nlohmann::json(fields).dump(), work);
    if (!work.empty()) lattice_->scheduler_->invoke([work = std::move(work)] {
        for (const auto& call : work) call();
    });
}
void model_base::notify_property_change(const std::string& property_name) {
    notify_managed_observers(false, {property_name});
}
void model_base::notify_deleted() { notify_managed_observers(true, {}); }

notification_token model_base::observe_base(object_observer_t callback) {
    if (!is_managed() || global_id_.empty()) return {};
    if (!lattice_) {
        // Preserve the historical ownerless model path. Such wrappers do not
        // claim commit/sync delivery or owner-scoped observation.
        auto key = std::make_pair(table_name_, global_id_);
        auto id = object_observer_registry::instance().add_observer(key, std::move(callback));
        return notification_token([key, id] { object_observer_registry::instance().remove_observer(key, id); });
    }
    const auto route = managed_route(table_name_);
    if (attachment_token_ != 0 || route.schema_sql != "main")
        throw std::invalid_argument("typed observation of an attached model is unsupported");
    {
        std::lock_guard<std::mutex> lock(lattice_->connection_ownership_mutex_);
        if (lattice_->closed_.load(std::memory_order_acquire) || lattice_->db_.get() != db_)
            throw std::logic_error("typed observation requires the current open owner");
    }
    auto fallback = property_names_;
    if (fallback.empty()) {
        if (const auto* schema = schema_registry::instance().get_schema(route.table))
            for (const auto& property : schema->properties) fallback.push_back(property.name);
    }
    return lattice_->managed_observers_.observe(route.table, id_, global_id_, std::move(fallback), std::move(callback));
}
} // namespace lattice
