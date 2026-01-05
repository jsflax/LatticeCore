// lattice.cpp - Implementation moved to header (templates)
// This file kept for potential non-template implementations

#include "lattice/lattice.hpp"

namespace lattice {

// Singleton instance - defined here to ensure single copy across all translation units
instance_registry& instance_registry::instance() {
    static instance_registry reg;
    return reg;
}
void lattice_db::setup_change_hook() {
    LOG_DEBUG("setup_change_hook", "Setting up hooks for path: %s", config_.path.c_str());

    // Update hook - buffers changes (called for each row change)
    sqlite3_update_hook(db_->handle(),
        [](void* user_data, int operation, const char* db_name, const char* table_name, sqlite3_int64 rowid) {
            auto* self = static_cast<lattice_db*>(user_data);

            std::string op;
            switch (operation) {
                case SQLITE_INSERT: op = "INSERT"; break;
                case SQLITE_UPDATE: op = "UPDATE"; break;
                case SQLITE_DELETE: op = "DELETE"; break;
                default: return;
            }

            LOG_DEBUG("update_hook", "table=%s op=%s rowid=%lld", table_name, op.c_str(), (long long)rowid);

            // Handle internal tables specially
            std::string table(table_name);
            if (table == "_SyncControl") {
                LOG_DEBUG("update_hook", "Skipping _SyncControl table");
                return;
            }

            // For AuditLog changes:
            // - In-memory DBs: notify directly (WAL hook won't fire)
            // - File DBs: skip (let flush_changes handle it via WAL hook to avoid double notification)
            if (table == "AuditLog") {
                if (self->config_.path == ":memory:" || self->config_.path.empty()) {
                    LOG_DEBUG("update_hook", "AuditLog change (in-memory), notifying directly: op=%s rowid=%lld", op.c_str(), (long long)rowid);
                    // Get globalId for the AuditLog entry
                    std::string global_id;
                    if (operation != SQLITE_DELETE) {
                        std::string sql = "SELECT globalId FROM AuditLog WHERE id = ?";
                        auto rows = self->db_->query(sql, {static_cast<int64_t>(rowid)});
                        if (!rows.empty()) {
                            auto it = rows[0].find("globalId");
                            if (it != rows[0].end() && std::holds_alternative<std::string>(it->second)) {
                                global_id = std::get<std::string>(it->second);
                            }
                        }
                    }
                    self->notify_change("AuditLog", op, static_cast<int64_t>(rowid), global_id);
                } else {
                    LOG_DEBUG("update_hook", "AuditLog change (file DB), skipping - WAL hook will handle");
                }
                return;
            }

            // Skip link tables (they start with underscore and have no 'id' column)
            if (!table.empty() && table[0] == '_') {
                LOG_DEBUG("update_hook", "Skipping link table: %s", table_name);
                return;
            }

            // Get the globalId for this row (only for model tables with 'id' column)
            std::string global_id;
            if (operation != SQLITE_DELETE) {
                std::string sql = "SELECT globalId FROM " + table + " WHERE id = ?";
                auto rows = self->db_->query(sql, {static_cast<int64_t>(rowid)});
                if (!rows.empty()) {
                    auto it = rows[0].find("globalId");
                    if (it != rows[0].end() && std::holds_alternative<std::string>(it->second)) {
                        global_id = std::get<std::string>(it->second);
                    }
                }
            }

            // Buffer the change instead of notifying immediately
            LOG_DEBUG("update_hook", "Buffering change: table=%s op=%s rowid=%lld globalId=%s",
                   table.c_str(), op.c_str(), (long long)rowid, global_id.c_str());
            self->append_to_change_buffer(table, op, static_cast<int64_t>(rowid), global_id);

            // For in-memory databases, flush immediately since WAL hook won't fire
            if (self->config_.path == ":memory:" || self->config_.path.empty()) {
                LOG_DEBUG("update_hook", "In-memory DB, calling flush_changes()");
                self->flush_changes();
            } else {
                LOG_DEBUG("update_hook", "File DB (path=%s), waiting for WAL hook", self->config_.path.c_str());
            }
        },
        this
    );

    // WAL hook - flushes buffered changes on transaction commit (file-based DBs only)
    sqlite3_wal_hook(db_->handle(),
        [](void* user_data, sqlite3*, const char*, int) -> int {
            auto* self = static_cast<lattice_db*>(user_data);
            self->flush_changes();
            return SQLITE_OK;
        },
        this
    );
}
// Future: non-template implementations can go here
void lattice_db::setup_sync_if_configured() {
    if (!config_.is_sync_enabled()) {
        return;
    }

    // Create sync config from our configuration
    sync_config sync_cfg;
    sync_cfg.websocket_url = config_.websocket_url;
    sync_cfg.authorization_token = config_.authorization_token;

    // Create synchronizer
    synchronizer_ = std::make_unique<synchronizer>(*this, sync_cfg, scheduler_);

    // Wire up callbacks if set
    if (on_sync_state_change_) {
        synchronizer_->set_on_state_change(on_sync_state_change_);
    }
    if (on_sync_error_) {
        synchronizer_->set_on_error(on_sync_error_);
    }

    // Auto-connect (like Swift's Lattice.init)
    synchronizer_->connect();
}

void lattice_db::attach(lattice_db &lattice) {
    std::reference_wrapper<std::unique_ptr<database>> dbs[2] = {this->db_, this->read_db_};
    for (auto& db : dbs) {
        std::stringstream ss;
        ss << "ATTACH DATABASE ";
        ss << "'" << lattice.config_.path << "'";
        ss << " AS ";
        std::filesystem::path p = lattice.config_.path;
        auto alias = p.filename().replace_extension();
        ss << alias;
        
        db.get()->execute(ss.str());
        
        auto tables = lattice.db_->query(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' "
            "AND name NOT IN ('AuditLog', '_SyncControl') "
            "AND name NOT LIKE '%_vec0'");

        int64_t total_entries = 0;

        for (const auto& table_row : tables) {
            auto it = table_row.find("name");
            if (it == table_row.end() || !std::holds_alternative<std::string>(it->second))
                continue;

            std::string table_name = std::get<std::string>(it->second);
            db.get()->execute("CREATE TEMP VIEW IF NOT EXISTS " + table_name +
                    " AS SELECT * FROM \"" + alias.string() + "\"." + table_name);
        }
    }
}

// ============================================================================
// managed<std::vector<geo_bounds*>> method implementations
// ============================================================================

size_t managed<std::vector<geo_bounds*>>::size() const {
    if (!is_bound()) return unmanaged_value.size();
    load_if_needed();
    return cached_objects_.size();
}

void managed<std::vector<geo_bounds*>>::load_if_needed() const {
    if (loaded_ || !is_bound()) return;

    cached_objects_.clear();
    std::string sql = "SELECT id, minLat, maxLat, minLon, maxLon FROM " + list_table_ +
                      " WHERE parent_id = ? ORDER BY id";
    auto rows = db->query(sql, {parent_global_id_});

    for (const auto& row : rows) {
        auto get_double = [&](const std::string& col) -> double {
            auto it = row.find(col);
            if (it != row.end() && std::holds_alternative<double>(it->second)) {
                return std::get<double>(it->second);
            }
            return 0.0;
        };
        auto get_int64 = [&](const std::string& col) -> int64_t {
            auto it = row.find(col);
            if (it != row.end() && std::holds_alternative<int64_t>(it->second)) {
                return std::get<int64_t>(it->second);
            }
            return 0;
        };

        geo_bounds bounds(
            get_double("minLat"),
            get_double("maxLat"),
            get_double("minLon"),
            get_double("maxLon")
        );
        int64_t row_id = get_int64("id");

        auto wrapper = std::make_shared<managed<geo_bounds*>>(bounds);
        wrapper->bind_to_list_row(db, lattice, list_table_, rtree_table_, row_id, parent_global_id_);
        cached_objects_.push_back(wrapper);
    }
    loaded_ = true;
}

void managed<std::vector<geo_bounds*>>::push_back(const geo_bounds& bounds) {
    if (!is_bound()) {
        unmanaged_value.push_back(bounds);
        return;
    }

    // Ensure the list table exists
    if (lattice) {
        lattice->ensure_geo_bounds_list_table(table_name, column_name);
    }

    // Insert into list table using insert() which returns the row id
    primary_key_t new_row_id = db->insert(list_table_, {
        {"parent_id", parent_global_id_},
        {"minLat", bounds.min_lat},
        {"maxLat", bounds.max_lat},
        {"minLon", bounds.min_lon},
        {"maxLon", bounds.max_lon}
    });

    // Create cached wrapper
    auto wrapper = std::make_shared<managed<geo_bounds*>>(bounds);
    wrapper->bind_to_list_row(db, lattice, list_table_, rtree_table_, new_row_id, parent_global_id_);
    cached_objects_.push_back(wrapper);
}

void managed<std::vector<geo_bounds*>>::erase(size_t index) {
    if (!is_bound()) {
        if (index < unmanaged_value.size()) {
            unmanaged_value.erase(unmanaged_value.begin() + static_cast<std::ptrdiff_t>(index));
        }
        return;
    }

    load_if_needed();
    if (index >= cached_objects_.size()) return;

    // Get the row id from the cached wrapper
    int64_t row_id_to_delete = cached_objects_[index]->list_row_id_;

    // Delete from database
    db->execute("DELETE FROM " + list_table_ + " WHERE id = ?", {row_id_to_delete});

    // Update cache
    cached_objects_.erase(cached_objects_.begin() + static_cast<std::ptrdiff_t>(index));
}

void managed<std::vector<geo_bounds*>>::clear() {
    if (!is_bound()) {
        unmanaged_value.clear();
        return;
    }

    // Delete all entries for this parent
    std::string sql = "DELETE FROM " + list_table_ + " WHERE parent_id = ?";
    db->execute(sql, {parent_global_id_});

    // Clear cache
    cached_objects_.clear();
    loaded_ = true;  // Mark as loaded (empty)
}

managed<std::vector<geo_bounds*>>::iterator managed<std::vector<geo_bounds*>>::begin() {
    load_if_needed();
    return iterator(cached_objects_.begin());
}

managed<std::vector<geo_bounds*>>::iterator managed<std::vector<geo_bounds*>>::end() {
    load_if_needed();
    return iterator(cached_objects_.end());
}

managed<std::vector<geo_bounds*>>::element_proxy
managed<std::vector<geo_bounds*>>::operator[](size_t index) {
    return element_proxy(this, index);
}

geo_bounds managed<std::vector<geo_bounds*>>::operator[](size_t index) const {
    if (!is_bound()) {
        return unmanaged_value[index];
    }
    load_if_needed();
    return cached_objects_[index]->detach();
}

// Element proxy implementations
managed<std::vector<geo_bounds*>>::element_proxy::operator managed<geo_bounds*>&() const {
    list_->load_if_needed();
    return *list_->cached_objects_[index_];
}

managed<geo_bounds*>*
managed<std::vector<geo_bounds*>>::element_proxy::operator->() const {
    list_->load_if_needed();
    return list_->cached_objects_[index_].get();
}

managed<std::vector<geo_bounds*>>::element_proxy&
managed<std::vector<geo_bounds*>>::element_proxy::operator=(const geo_bounds& bounds) {
    list_->load_if_needed();
    *list_->cached_objects_[index_] = bounds;
    return *this;
}

} // namespace lattice
