// lattice.cpp - Implementation moved to header (templates)
// This file kept for potential non-template implementations

#include "lattice/lattice.hpp"
#include <set>
#include <unordered_set>

namespace lattice {

// Single definition of the global log level (declared extern in log.hpp).
std::atomic<log_level> g_log_level{log_level::off};

// Singleton instance - defined here to ensure single copy across all translation units
instance_registry& instance_registry::instance() {
    // Intentionally leaked: prevents use-after-destroy when GCD callbacks
    // fire during process teardown (after atexit handlers run).
    static instance_registry* reg = new instance_registry();
    return *reg;
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
                    // In-memory DBs: notify directly from update_hook.
                    // flush_changes() can't handle this because it runs during the
                    // model table's update_hook, BEFORE the AuditLog trigger fires.
                    if (operation == SQLITE_INSERT) {
                        LOG_DEBUG("update_hook", "AuditLog change (in-memory), notifying directly");
                        // Get globalId for the AuditLog entry
                        std::string global_id;
                        std::string sql = "SELECT globalId FROM AuditLog WHERE id = ?";
                        auto rows = self->db_->query(sql, {static_cast<int64_t>(rowid)});
                        if (!rows.empty()) {
                            auto it = rows[0].find("globalId");
                            if (it != rows[0].end() && std::holds_alternative<std::string>(it->second)) {
                                global_id = std::get<std::string>(it->second);
                            }
                        }
                        // In-memory databases each have their own isolated storage
                        // despite sharing the ":memory:" path string, so only
                        // notify this instance (matching flush_changes() behavior).
                        self->notify_change("AuditLog", "INSERT", static_cast<int64_t>(rowid), global_id);
                    }
                } else {
                    LOG_DEBUG("update_hook", "AuditLog change (file DB), skipping - WAL hook will handle");
                    // Advance the cross-process cursor on ALL instances sharing
                    // this path INSIDE the transaction, before the WAL commit
                    // makes this entry visible to readers. This prevents
                    // handle_cross_process_notification (on the inotify/GCD
                    // thread) from seeing local entries and firing duplicate
                    // observer callbacks — both on this instance and on other
                    // same-process instances sharing the same database file.
                    if (operation == SQLITE_INSERT) {
                        auto all = instance_registry::instance().get_instances(self->config_.path);
                        for (auto* inst : all) {
                            inst->last_seen_audit_id_.store(
                                static_cast<int64_t>(rowid), std::memory_order_release);
                        }
                    }
                }
                return;
            }

            // Skip SQLite internal tables (triggered during VACUUM, etc.)
            if (table.rfind("sqlite_", 0) == 0) {
                return;
            }

            // Link tables start with underscore — they don't have a globalId column
            // but we still buffer them so flush_changes() can find their AuditLog
            // entries and notify the synchronizer.
            bool is_link_table = !table.empty() && table[0] == '_';

            // Get the globalId for this row (only for model tables)
            std::string global_id;
            if (!is_link_table && operation != SQLITE_DELETE) {
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
void lattice_db::setup_cross_process_notifier() {
    xproc_notifier_ = make_cross_process_notifier(config_.path);
    if (!xproc_notifier_) return;

    // Initialize cursor to current max AuditLog id
    auto max_rows = read_db().query("SELECT MAX(id) AS max_id FROM AuditLog");
    if (!max_rows.empty()) {
        auto it = max_rows[0].find("max_id");
        if (it != max_rows[0].end() && std::holds_alternative<int64_t>(it->second)) {
            last_seen_audit_id_ = std::get<int64_t>(it->second);
        }
    }

    LOG_DEBUG("xproc", "Initialized cursor at audit id=%lld for path: %s",
              (long long)last_seen_audit_id_, config_.path.c_str());

    xproc_notifier_->start_listening([path = config_.path,
                                      mtx = xproc_callback_mutex_] {
        std::lock_guard<std::mutex> lock(*mtx);
        auto instances = instance_registry::instance().get_instances(path);
        if (!instances.empty()) {
            instances[0]->handle_cross_process_notification();
        }
    });
}

void lattice_db::handle_cross_process_notification() {
    auto cursor = last_seen_audit_id_.load(std::memory_order_acquire);
    LOG_DEBUG("xproc", "Cross-process notification received, last_seen=%lld", (long long)cursor);

    // During process teardown the database files may already be deleted while
    // this callback is still queued on the dispatch queue. Catch db_error to
    // prevent an uncaught exception from aborting the process.
    try {
        // Query AuditLog for entries newer than our cursor.
        auto rows = read_db().query(
            "SELECT id, tableName, operation, rowId, globalRowId, changedFieldsNames FROM AuditLog WHERE id > ? ORDER BY id ASC",
            {cursor}
        );

        if (rows.empty()) {
            LOG_DEBUG("xproc", "No new AuditLog entries (self-notification)");
            return;
        }

        // Re-check cursor: the update_hook may have advanced it while we were
        // querying (local write committed between our cursor read and the
        // SELECT). Filter out entries that are now below the updated cursor
        // to avoid duplicate notifications for local changes.
        auto updated_cursor = last_seen_audit_id_.load(std::memory_order_acquire);
        if (updated_cursor > cursor) {
            rows.erase(
                std::remove_if(rows.begin(), rows.end(), [updated_cursor](const auto& row) {
                    auto id_it = row.find("id");
                    return id_it != row.end() &&
                           std::holds_alternative<int64_t>(id_it->second) &&
                           std::get<int64_t>(id_it->second) <= updated_cursor;
                }),
                rows.end()
            );
            if (rows.empty()) {
                LOG_DEBUG("xproc", "All entries filtered by advanced cursor (local write race)");
                return;
            }
        }

        LOG_DEBUG("xproc", "Found %zu new AuditLog entries from other process", rows.size());

        // Only notify THIS instance's observers. Each instance sharing the
        // same database path has its own cross-process notifier, so each
        // independently receives the event and processes it. Notifying all
        // instances here would cause N^2 observer callbacks when N instances
        // share a path (e.g., migration tests that open the same DB twice).
        for (const auto& row : rows) {
            auto id_it = row.find("id");
            auto table_it = row.find("tableName");
            auto op_it = row.find("operation");
            auto rowid_it = row.find("rowId");
            auto growid_it = row.find("globalRowId");
            auto cfn_it = row.find("changedFieldsNames");

            if (id_it == row.end() || !std::holds_alternative<int64_t>(id_it->second)) continue;

            int64_t audit_id = std::get<int64_t>(id_it->second);
            std::string table = (table_it != row.end() && std::holds_alternative<std::string>(table_it->second))
                ? std::get<std::string>(table_it->second) : "";
            std::string op = (op_it != row.end() && std::holds_alternative<std::string>(op_it->second))
                ? std::get<std::string>(op_it->second) : "";
            int64_t row_id = (rowid_it != row.end() && std::holds_alternative<int64_t>(rowid_it->second))
                ? std::get<int64_t>(rowid_it->second) : 0;
            std::string global_row_id = (growid_it != row.end() && std::holds_alternative<std::string>(growid_it->second))
                ? std::get<std::string>(growid_it->second) : "";
            std::string changed_fields_names = (cfn_it != row.end() && std::holds_alternative<std::string>(cfn_it->second))
                ? std::get<std::string>(cfn_it->second) : "";

            // Notify this instance's model table and object observers
            notify_change(table, op, row_id, global_row_id, changed_fields_names);

            // Notify this instance's AuditLog observers
            auto audit_gid_rows = read_db().query(
                "SELECT globalId FROM AuditLog WHERE id = ?", {audit_id}
            );
            std::string audit_global_id;
            if (!audit_gid_rows.empty()) {
                auto git = audit_gid_rows[0].find("globalId");
                if (git != audit_gid_rows[0].end() && std::holds_alternative<std::string>(git->second)) {
                    audit_global_id = std::get<std::string>(git->second);
                }
            }

            notify_change("AuditLog", "INSERT", audit_id, audit_global_id);

            last_seen_audit_id_.store(audit_id, std::memory_order_release);
        }

        LOG_DEBUG("xproc", "Cross-process notification handled, cursor now at %lld", (long long)last_seen_audit_id_);
    } catch (const db_error&) {
        LOG_DEBUG("xproc", "Database unavailable during cross-process notification (likely teardown)");
    }
}

// Synchronizer registry — at most one synchronizer per {path, websocket_url}
static std::mutex& sync_registry_mutex() {
    static std::mutex m;
    return m;
}
static std::set<std::pair<std::string, std::string>>& active_sync_keys() {
    static std::set<std::pair<std::string, std::string>> s;
    return s;
}

bool lattice_db::try_register_sync_key(const std::string& path, const std::string& ws_url) {
    std::lock_guard<std::mutex> lock(sync_registry_mutex());
    return active_sync_keys().emplace(path, ws_url).second;
}

void lattice_db::unregister_sync_key(const std::string& path, const std::string& ws_url) {
    std::lock_guard<std::mutex> lock(sync_registry_mutex());
    active_sync_keys().erase({path, ws_url});
}

void lattice_db::setup_sync_if_configured() {
    if (!config_.is_sync_enabled()) {
        return;
    }

    // Only one synchronizer per {path, websocket_url} across all instances
    if (!try_register_sync_key(config_.path, config_.websocket_url)) {
        LOG_DEBUG("lattice_db", "Synchronizer already active for this path, skipping");
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

static std::vector<std::string> get_column_names(database* db, const std::string& schema, const std::string& table_name) {
    auto rows = db->query("PRAGMA " + schema + ".table_info(" + table_name + ")");
    std::vector<std::string> cols;
    for (const auto& row : rows) {
        auto it = row.find("name");
        if (it != row.end() && std::holds_alternative<std::string>(it->second))
            cols.push_back(std::get<std::string>(it->second));
    }
    std::sort(cols.begin(), cols.end());
    return cols;
}

void lattice_db::attach(lattice_db &lattice) {
    std::reference_wrapper<std::unique_ptr<database>> dbs[2] = {this->db_, this->read_db_};
    for (auto& db : dbs) {
        std::filesystem::path p = lattice.config_.path;
        auto alias = p.filename().replace_extension();

        {
            std::stringstream ss;
            ss << "ATTACH DATABASE ";
            ss << "'" << lattice.config_.path << "'";
            ss << " AS \"" << alias.string() << "\"";
            db.get()->execute(ss.str());
        }

        // Collect main DB's model table names for overlap detection.
        // Exclude all internal/auxiliary tables:
        //   - _-prefixed: virtual tables (_Model_col_vec, _Model_col_fts, _Model_col_rtree)
        //     and their shadow tables (_*_info, _*_chunks, _*_rowids, _*_content, etc.)
        //   - AuditLog, _SyncControl: Lattice internal bookkeeping
        //   - sqlite_*: SQLite internal
        // knn_query/combinedNearestQuery handle cross-DB vec/fts search via attached_aliases_.
        std::string table_filter =
            "SELECT name FROM %s WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%%' "
            "AND name NOT LIKE '\\_%%' ESCAPE '\\' "
            "AND name NOT IN ('AuditLog')";

        char main_sql[512];
        snprintf(main_sql, sizeof(main_sql), table_filter.c_str(), "main.sqlite_master");
        auto main_tables = db.get()->query(main_sql);
        std::unordered_set<std::string> main_table_set;
        for (const auto& row : main_tables) {
            auto it = row.find("name");
            if (it != row.end() && std::holds_alternative<std::string>(it->second))
                main_table_set.insert(std::get<std::string>(it->second));
        }

        char attached_sql[512];
        snprintf(attached_sql, sizeof(attached_sql), table_filter.c_str(), "sqlite_master");
        auto tables = lattice.db_->query(attached_sql);

        for (const auto& table_row : tables) {
            auto it = table_row.find("name");
            if (it == table_row.end() || !std::holds_alternative<std::string>(it->second))
                continue;

            std::string table_name = std::get<std::string>(it->second);

            if (main_table_set.count(table_name)) {
                // Verify schemas match before creating UNION view
                auto main_cols = get_column_names(db.get().get(), "main", table_name);
                auto attached_cols = get_column_names(db.get().get(), "\"" + alias.string() + "\"", table_name);

                if (main_cols != attached_cols) {
                    LOG_ERROR("db", "Schema mismatch for table '%s' between main and attached DB '%s'",
                              table_name.c_str(), alias.string().c_str());
                    throw std::runtime_error(
                        "Schema mismatch for table '" + table_name +
                        "' between main database and attached database '" + alias.string() + "'");
                }

                // Same table exists in both with matching schema: create UNION ALL view
                // Include _source column so hydrate can qualify table_name for lazy reads
                std::string quoted_alias = "\"" + alias.string() + "\"";
                db.get()->execute("CREATE TEMP VIEW IF NOT EXISTS " + table_name +
                    " AS SELECT *, 'main' AS _source FROM main." + table_name +
                    " UNION ALL SELECT *, '" + quoted_alias + "' AS _source FROM " + quoted_alias + "." + table_name);
            } else {
                // Table only in attached DB: simple passthrough view
                db.get()->execute("CREATE TEMP VIEW IF NOT EXISTS " + table_name +
                    " AS SELECT * FROM \"" + alias.string() + "\"." + table_name);
            }
        }
    }

    // Store alias for cross-DB knn_query
    std::filesystem::path p = lattice.config_.path;
    attached_aliases_.push_back(p.filename().replace_extension().string());
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
