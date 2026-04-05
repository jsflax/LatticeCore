// lattice.cpp - Implementation moved to header (templates)
// This file kept for potential non-template implementations

#include "lattice/lattice.hpp"
#include "lattice/ipc.hpp"
#include <set>
#include <unordered_set>
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>

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

cross_process_notifier* instance_registry::get_or_create_notifier(const std::string& path) {
    if (path.empty() || path == ":memory:") return nullptr;

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = shared_notifiers_.find(path);
    if (it != shared_notifiers_.end()) {
        return it->second.get();
    }

    auto notifier = make_cross_process_notifier(path);
    if (!notifier) return nullptr;

    notifier->start_listening([path] {
        int notified = 0;
        instance_registry::instance().for_each_alive(path,
            [&notified](lattice_db* inst) {
                inst->handle_cross_process_notification();
                ++notified;
            });
        LOG_DEBUG("xproc", "Notified %d alive instances for path", notified);
    });

    auto* raw = notifier.get();
    shared_notifiers_[path] = std::move(notifier);
    return raw;
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
            if (table == "_SyncControl" || table == "_lattice_sync_set" || table == "_lattice_sync_state") {
                LOG_DEBUG("update_hook", "Skipping internal table %s", table_name);
                return;
            }

            // For AuditLog changes:
            // - In-memory DBs: notify directly (WAL hook won't fire)
            // - Emscripten: notify directly (uses DELETE journal mode, no WAL)
            // - File DBs: skip (let flush_changes handle it via WAL hook to avoid double notification)
            if (table == "AuditLog") {
#ifdef __EMSCRIPTEN__
                // Emscripten uses DELETE journal mode — WAL hook never fires,
                // so always use the direct notification path.
                constexpr bool use_direct_notify = true;
#else
                const bool use_direct_notify = self->config_.is_in_memory();
#endif
                if (use_direct_notify) {
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
                        // For shared cache in-memory DBs, notify all instances
                        // sharing the path (the sync db and main db share state).
                        // For plain :memory:, just notify this instance.
                        if (self->config_.path.find("cache=shared") != std::string::npos) {
                            instance_registry::instance().for_each_alive(self->config_.path,
                                [rowid, &global_id](lattice_db* inst) {
                                    inst->notify_change("AuditLog", "INSERT", static_cast<int64_t>(rowid), global_id);
                                });
                        } else {
                            self->notify_change("AuditLog", "INSERT", static_cast<int64_t>(rowid), global_id);
                        }
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
                        instance_registry::instance().for_each_alive(self->config_.path,
                            [rowid](lattice_db* inst) {
                                inst->last_seen_audit_id_.store(
                                    static_cast<int64_t>(rowid), std::memory_order_release);
                            });
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

            // For in-memory databases, flush immediately since WAL hook won't fire.
            // Emscripten also needs immediate flush: it uses DELETE journal mode,
            // not WAL, so the WAL hook never fires.
#ifdef __EMSCRIPTEN__
            LOG_DEBUG("update_hook", "Emscripten: immediate flush_changes()");
            self->flush_changes();
#else
            if (self->config_.is_in_memory()) {
                LOG_DEBUG("update_hook", "In-memory DB, calling flush_changes()");
                self->flush_changes();
            } else {
                LOG_DEBUG("update_hook", "File DB (path=%s), waiting for WAL hook", self->config_.path.c_str());
            }
#endif
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
    // Use the shared per-path notifier from instance_registry.
    // Only ONE Darwin listener per path per process — prevents N^2
    // notification amplification when multiple lattice_db instances
    // share the same DB file (e.g. via LatticeThreadSafeReference.resolve()).
    shared_xproc_notifier_ = instance_registry::instance().get_or_create_notifier(config_.path);
    if (!shared_xproc_notifier_) return;

    // Initialize this instance's cursor to current max AuditLog id
    auto max_rows = read_db().query("SELECT MAX(id) AS max_id FROM AuditLog");
    if (!max_rows.empty()) {
        auto it = max_rows[0].find("max_id");
        if (it != max_rows[0].end() && std::holds_alternative<int64_t>(it->second)) {
            last_seen_audit_id_ = std::get<int64_t>(it->second);
        }
    }

    LOG_DEBUG("xproc", "Initialized cursor at audit id=%lld for path: %s",
              (long long)last_seen_audit_id_, config_.path.c_str());
}

void lattice_db::handle_cross_process_notification() {
    // KILL-SWITCH: set LATTICE_DISABLE_XPROC=1 to suppress cross-process observer dispatch.
    // Used to diagnose whether xproc notifications cause main-thread stalls.
    static bool disabled = (std::getenv("LATTICE_DISABLE_XPROC") != nullptr);
    if (disabled) return;

    auto cursor = last_seen_audit_id_.load(std::memory_order_acquire);
    LOG_DEBUG("xproc", "Cross-process notification received, last_seen=%lld", (long long)cursor);

    // During process teardown the database files may already be deleted while
    // this callback is still queued on the dispatch queue. Catch db_error to
    // prevent an uncaught exception from aborting the process.
    try {
        // Query AuditLog for entries newer than our cursor.
        // Uses the dedicated xproc read connection to avoid SQLite lock contention
        // with observer callbacks running on the scheduler (MainActor), which use
        // read_db() for existence checks.
        auto rows = xproc_read_db().query(
            "SELECT id, tableName, operation, rowId, globalRowId, changedFieldsNames FROM AuditLog WHERE id > ? ORDER BY id ASC",
            {cursor}
        );

        if (rows.empty()) {
            // No new AuditLog entries, but the notification may indicate sync
            // state changes (e.g., daemon marked entries as isSynchronized=1).
            // Fire the dedicated idle hint callback directly on this thread —
            // NOT through notify_changes_batched / scheduler, which would
            // dispatch synthetic observer callbacks to MainActor and cause
            // thread pile-ups under rapid notification load.
            std::function<void()> hint;
            {
                std::lock_guard<std::mutex> lock(xproc_idle_mutex_);
                hint = on_xproc_idle_;
            }
            if (hint) {
                LOG_DEBUG("xproc", "No new AuditLog entries — firing xproc idle hint");
                hint();
            } else {
                LOG_DEBUG("xproc", "No new AuditLog entries — no idle hint registered");
            }
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
        //
        // Batch all changes into a single scheduler dispatch to avoid
        // creating N separate Tasks on the main actor (each with overhead).
        std::vector<std::tuple<std::string, std::string, int64_t, std::string, std::string>> changes;
        changes.reserve(rows.size() * 2);  // model change + AuditLog change per row

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

            // Collect model table change
            changes.emplace_back(table, op, row_id, global_row_id, changed_fields_names);

            // Collect AuditLog change
            auto audit_gid_rows = xproc_read_db().query(
                "SELECT globalId FROM AuditLog WHERE id = ?", {audit_id}
            );
            std::string audit_global_id;
            if (!audit_gid_rows.empty()) {
                auto git = audit_gid_rows[0].find("globalId");
                if (git != audit_gid_rows[0].end() && std::holds_alternative<std::string>(git->second)) {
                    audit_global_id = std::get<std::string>(git->second);
                }
            }
            changes.emplace_back("AuditLog", "INSERT", audit_id, audit_global_id, "");

            last_seen_audit_id_.store(audit_id, std::memory_order_release);
        }

        // Resolve internal tables (link tables, geo_bounds list tables) to their
        // parent tables. Internal table changes are translated to parent UPDATE
        // notifications — e.g. a link table INSERT becomes a parent table UPDATE.
        // This mirrors the same resolution done in flush_changes() for same-process writes.
        std::unordered_map<std::string, std::string> internal_table_parents;
        for (const auto& [table, op, row_id, global_id, cfn] : changes) {
            if (table == "AuditLog" || internal_table_parents.count(table)) continue;
            auto meta = xproc_read_db().query(
                "SELECT value FROM _lattice_meta WHERE key = ?",
                {"internal_table:" + table}
            );
            if (!meta.empty()) {
                auto val_it = meta[0].find("value");
                if (val_it != meta[0].end() && std::holds_alternative<std::string>(val_it->second)) {
                    internal_table_parents[table] = std::get<std::string>(val_it->second);
                }
            }
        }

        // Rewrite internal table entries to parent UPDATE notifications.
        // Resolve the parent's actual rowId via the link table's lhs column
        // (parent globalId) so Swift table observers can validate the object.
        if (!internal_table_parents.empty()) {
            for (auto& change : changes) {
                auto& table = std::get<0>(change);
                auto it = internal_table_parents.find(table);
                if (it != internal_table_parents.end() && !it->second.empty()) {
                    auto& meta_value = it->second;
                    auto colon_pos = meta_value.find(':');
                    std::string parent_table = (colon_pos != std::string::npos)
                        ? meta_value.substr(0, colon_pos) : meta_value;
                    std::string property_name = (colon_pos != std::string::npos)
                        ? meta_value.substr(colon_pos + 1) : "";
                    std::string changed_fields = property_name.empty()
                        ? "" : "[\"" + property_name + "\"]";

                    // Resolve parent rowId: AuditLog changedFields has {"lhs": "parent-globalId", ...}
                    // Link table triggers store rowId=0 (no id column), so we extract
                    // the parent globalId from changedFields JSON and look up the parent row.
                    int64_t parent_row_id = 0;
                    std::string parent_global_id;
                    auto& link_global_id = std::get<3>(change);
                    if (!link_global_id.empty()) {
                        auto cf_rows = xproc_read_db().query(
                            "SELECT json_extract(changedFields, '$.lhs') AS lhs FROM AuditLog "
                            "WHERE globalRowId = ? AND tableName = ?",
                            {link_global_id, table}
                        );
                        if (!cf_rows.empty()) {
                            auto lhs_it = cf_rows[0].find("lhs");
                            if (lhs_it != cf_rows[0].end() && std::holds_alternative<std::string>(lhs_it->second)) {
                                parent_global_id = std::get<std::string>(lhs_it->second);
                                auto pid_rows = xproc_read_db().query(
                                    "SELECT id FROM \"" + parent_table + "\" WHERE globalId = ?",
                                    {parent_global_id}
                                );
                                if (!pid_rows.empty()) {
                                    auto pid_it = pid_rows[0].find("id");
                                    if (pid_it != pid_rows[0].end() && std::holds_alternative<int64_t>(pid_it->second)) {
                                        parent_row_id = std::get<int64_t>(pid_it->second);
                                    }
                                }
                            }
                        }
                    }

                    LOG_DEBUG("xproc", "Internal table %s -> parent UPDATE %s (prop=%s, parentRowId=%lld)",
                              table.c_str(), parent_table.c_str(), property_name.c_str(), (long long)parent_row_id);
                    table = parent_table;
                    std::get<1>(change) = "UPDATE";
                    std::get<2>(change) = parent_row_id;
                    std::get<3>(change) = parent_global_id;
                    std::get<4>(change) = changed_fields;
                }
            }
        }

        // Reconcile vec0 virtual tables for synced vector columns.
        // vec0 maintains per-connection internal state — shadow table writes
        // from the sync db connection are not visible to this connection's
        // vec0 virtual table. Re-insert on this connection so nearest()
        // queries return synced data.
        for (const auto& [table, op, row_id, global_id, cfn] : changes) {
            if (table == "AuditLog" || table.empty() || table[0] == '_') continue;
            if (op != "INSERT" && op != "UPDATE") continue;
            if (global_id.empty()) continue;

            auto vec_tables = db().query(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name LIKE ?",
                {"_" + table + "_%_vec"});

            for (const auto& vt_row : vec_tables) {
                auto name_it = vt_row.find("name");
                if (name_it == vt_row.end() || !std::holds_alternative<std::string>(name_it->second)) continue;
                auto& vec_table = std::get<std::string>(name_it->second);

                // Extract column name from vec table name: _Table_Column_vec
                auto prefix_len = table.size() + 2; // "_" + table + "_"
                auto suffix_len = 4; // "_vec"
                if (vec_table.size() <= prefix_len + suffix_len) continue;
                auto col = vec_table.substr(prefix_len, vec_table.size() - prefix_len - suffix_len);

                auto data_rows = db().query(
                    "SELECT " + col + " FROM " + table + " WHERE globalId = ?",
                    {global_id});
                if (data_rows.empty()) continue;

                auto col_it = data_rows[0].find(col);
                if (col_it == data_rows[0].end() ||
                    !std::holds_alternative<std::vector<uint8_t>>(col_it->second)) continue;
                auto& vec_data = std::get<std::vector<uint8_t>>(col_it->second);
                if (vec_data.empty()) continue;

                try {
                    db().execute("DELETE FROM " + vec_table + " WHERE global_id = ?", {global_id});
                    db().execute("INSERT INTO " + vec_table + "(global_id, embedding) VALUES (?, ?)",
                                {global_id, vec_data});
                } catch (const std::exception& e) {
                    LOG_WARN("xproc", "vec0 reconcile failed for %s: %s", vec_table.c_str(), e.what());
                }
            }
        }

        // Single batched dispatch — all observer callbacks run in one Task
        if (!changes.empty()) {
            notify_changes_batched(changes);
        }

        LOG_DEBUG("xproc", "Cross-process notification handled, cursor now at %lld", (long long)last_seen_audit_id_);
    } catch (const db_error&) {
        LOG_DEBUG("xproc", "Database unavailable during cross-process notification (likely teardown)");
    }
}

// Synchronizer registry — at most one synchronizer per {path, key}
// Used for both WSS (key = websocket_url) and IPC (key = channel name).
static std::mutex& sync_registry_mutex() {
    // Intentionally leaked: prevents use-after-destroy when destructors
    // run during process teardown (static destruction order is undefined).
    static auto* m = new std::mutex();
    return *m;
}
static std::set<std::pair<std::string, std::string>>& active_sync_keys() {
    // Intentionally leaked: same reason as sync_registry_mutex.
    static auto* s = new std::set<std::pair<std::string, std::string>>();
    return *s;
}

bool lattice_db::try_register_sync_key(const std::string& path, const std::string& key) {
    std::lock_guard<std::mutex> lock(sync_registry_mutex());
    return active_sync_keys().emplace(path, key).second;
}

void lattice_db::unregister_sync_key(const std::string& path, const std::string& key) {
    std::lock_guard<std::mutex> lock(sync_registry_mutex());
    active_sync_keys().erase({path, key});
}

// Collect all sync_ids for this database (WSS + all IPC channels).
// Used to populate all_active_sync_ids for per-synchronizer sync state.
static std::vector<std::string> collect_all_sync_ids(const configuration& config) {
    std::vector<std::string> ids;
    if (config.is_sync_enabled()) {
        ids.push_back("wss:" + config.websocket_url);
    }
    for (const auto& target : config.ipc_targets) {
        ids.push_back("ipc:" + target.channel);
    }
    return ids;
}

void lattice_db::setup_sync_if_configured() {
    if (!config_.is_sync_enabled()) {
        return;
    }

#ifndef __EMSCRIPTEN__
    // Cross-process flock: only one process may own the WSS synchronizer
    // for a given database. If another process holds the lock, skip silently.
    // Skipped on WASM — single-threaded, single-process environment.
    if (sync_lock_fd_ < 0) {
        std::string lock_path = config_.path + ".sync.lock";
        int fd = ::open(lock_path.c_str(), O_CREAT | O_RDWR, 0600);
        if (fd >= 0) {
            if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
                ::close(fd);
                LOG_DEBUG("lattice_db", "WSS sync lock held by another process, skipping");
                return;
            }
            sync_lock_fd_ = fd;
        } else {
            LOG_DEBUG("lattice_db", "Failed to open sync lock file %s, proceeding anyway", lock_path.c_str());
        }
    }
#endif

    // Only one synchronizer per {path, websocket_url} across all instances
    if (!try_register_sync_key(config_.path, config_.websocket_url)) {
        LOG_DEBUG("lattice_db", "Synchronizer already active for this path, skipping");
        return;
    }

    // Create sync config from our configuration
    sync_config sync_cfg;
    sync_cfg.websocket_url = config_.websocket_url;
    sync_cfg.authorization_token = config_.authorization_token;
    sync_cfg.sync_filter = config_.sync_filter;

    // Always use per-synchronizer sync state
    auto all_ids = collect_all_sync_ids(config_);
    sync_cfg.sync_id = "wss:" + config_.websocket_url;
    sync_cfg.all_active_sync_ids = all_ids;

#ifdef __EMSCRIPTEN__
    // Emscripten: single-threaded, so the synchronizer borrows our connection
    // directly. This avoids opening a second connection to the same OPFS file,
    // which would fail due to exclusive locking.
    LOG_INFO("lattice_db", "sync using shared db (Emscripten), path=%s", config_.path.c_str());
#else
    // Native: create a dedicated lattice_db for the synchronizer (separate
    // connection on its own thread via std_thread_scheduler).
    std::string sync_path = resolve_path(config_);
    configuration sync_db_config(sync_path,
                                 std::make_shared<std_thread_scheduler>());
    sync_db_config.target_schema_version = config_.target_schema_version;
    sync_db_config.migration_block = config_.migration_block;
    auto sync_db = std::make_unique<lattice_db>(sync_db_config);

    // Debug: verify sync db can see model tables
    LOG_INFO("lattice_db", "sync_db created for path=%s", sync_db_config.path.c_str());
    try {
        auto tables = sync_db->db().query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name");
        std::string table_list;
        for (const auto& row : tables) {
            auto it = row.find("name");
            if (it != row.end() && std::holds_alternative<std::string>(it->second)) {
                if (!table_list.empty()) table_list += ", ";
                table_list += std::get<std::string>(it->second);
            }
        }
        LOG_INFO("lattice_db", "sync_db tables: [%s]", table_list.c_str());
    } catch (const std::exception& e) {
        LOG_ERROR("lattice_db", "sync_db table list query failed: %s", e.what());
    }
#endif

    // Create synchronizer
#ifdef __EMSCRIPTEN__
    synchronizer_ = std::make_unique<synchronizer>(*this, sync_cfg);
#else
    synchronizer_ = std::make_unique<synchronizer>(std::move(sync_db), sync_cfg);
#endif

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

void lattice_db::setup_ipc_if_configured() {
#ifndef __EMSCRIPTEN__
    if (!config_.is_ipc_enabled()) {
        return;
    }

    // Only one IPC endpoint per {path, channel} across all instances
    // (mirrors the WSS dedup via try_register_sync_key).
    // Filter out channels that are already registered by another instance.
    std::vector<size_t> active_indices;
    for (size_t i = 0; i < config_.ipc_targets.size(); ++i) {
        const auto& channel = config_.ipc_targets[i].channel;
        if (try_register_sync_key(config_.path, "ipc:" + channel)) {
            active_indices.push_back(i);
        } else {
            LOG_DEBUG("lattice_db", "IPC synchronizer already active for %s on channel %s, skipping",
                      config_.path.c_str(), channel.c_str());
        }
    }
    if (active_indices.empty()) return;

    auto all_ids = collect_all_sync_ids(config_);

    // Phase 1: Create all endpoints and store them in the vector.
    // We must do this BEFORE starting any endpoint, because:
    // - The server callback fires asynchronously on the accept thread
    // - Capturing &state.sync from a local that is later moved = dangling pointer
    // - reserve() ensures push_back won't invalidate references
    ipc_synchronizers_.reserve(active_indices.size());

    for (size_t idx : active_indices) {
        ipc_sync_state state;
        state.endpoint = std::make_unique<ipc_endpoint>(config_.ipc_targets[idx].channel,
                                                        config_.ipc_targets[idx].socket_path);

        // Acquire per-channel flock — mirrors sync_lock_fd_ for WSS.
        // Set during construction so is_sync_agent() returns true before
        // the accept callback creates the synchronizer (avoids timing races).
        const auto& target_cfg = config_.ipc_targets[idx];
        std::string sock_path = target_cfg.socket_path.value_or(
            resolve_ipc_socket_path(target_cfg.channel));
        std::string lock_path = sock_path + ".lock";
        int lock_fd = ::open(lock_path.c_str(), O_CREAT | O_RDWR, 0600);
        if (lock_fd >= 0) {
            if (::flock(lock_fd, LOCK_EX | LOCK_NB) == 0) {
                state.lock_fd = lock_fd;
                LOG_INFO("ipc_sync", "Acquired IPC lock: %s (fd=%d)", lock_path.c_str(), lock_fd);
            } else {
                // Another process holds this channel — still proceed (IPC handles roles via bind)
                // but don't set lock_fd so is_sync_agent() reflects the true owner.
                ::close(lock_fd);
                LOG_INFO("ipc_sync", "IPC lock held by another process: %s", lock_path.c_str());
            }
        }

        ipc_synchronizers_.push_back(std::move(state));
    }

    // Phase 2: Start endpoints using stable references into the vector.
    for (size_t i = 0; i < ipc_synchronizers_.size(); ++i) {
        const auto& target = config_.ipc_targets[active_indices[i]];
        std::string sync_id = "ipc:" + target.channel;
        auto& sync_slot = ipc_synchronizers_[i].sync;

        ipc_synchronizers_[i].endpoint->start(
            [this, sync_id, all_ids, &target, &sync_slot](std::unique_ptr<ipc_socket_client> transport) {
                LOG_INFO("ipc_sync", "[%s] Accept callback fired (sync_slot=%s, db=%s)",
                         sync_id.c_str(), sync_slot ? "OCCUPIED" : "empty",
                         config_.path.c_str());

                sync_config ipc_cfg;
                ipc_cfg.sync_id = sync_id;
                ipc_cfg.all_active_sync_ids = all_ids;
                ipc_cfg.sync_filter = target.sync_filter;

                // Create dedicated lattice_db for this IPC synchronizer
                configuration ipc_db_config(config_.path,
                                            std::make_shared<std_thread_scheduler>());
                ipc_db_config.target_schema_version = config_.target_schema_version;
                ipc_db_config.migration_block = config_.migration_block;
                auto ipc_db = std::make_unique<lattice_db>(ipc_db_config);

                LOG_INFO("ipc_sync", "[%s] Creating synchronizer (replacing old=%p)",
                         sync_id.c_str(), sync_slot ? (void*)sync_slot.get() : nullptr);

                auto sync = std::make_unique<synchronizer>(
                    std::move(ipc_db), ipc_cfg, std::move(transport));

                // Lock ipc_callbacks_mutex_ to synchronize with set_on_sync_progress/
                // set_on_sync_state_change/set_on_sync_error on the main thread.
                // Without this, the accept thread and main thread can race: both
                // check-then-act on {on_sync_progress_, sync_slot} and miss each other.
                {
                    std::lock_guard<std::mutex> lock(ipc_callbacks_mutex_);

                    // Apply stored callbacks to the newly-created synchronizer
                    if (on_sync_progress_) {
                        sync->set_on_progress(on_sync_progress_);
                    }
                    if (on_sync_state_change_) {
                        sync->set_on_state_change(on_sync_state_change_);
                    }
                    if (on_sync_error_) {
                        sync->set_on_error(on_sync_error_);
                    }

                    sync->connect();
                    sync_slot = std::move(sync);
                }

                LOG_INFO("ipc_sync", "[%s] New synchronizer installed (this=%p)",
                         sync_id.c_str(), (void*)sync_slot.get());
            });
    }
#endif // !__EMSCRIPTEN__
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
