// lattice.cpp - Implementation moved to header (templates)
// This file kept for potential non-template implementations

#include "lattice/lattice.hpp"
#include "lattice/ipc.hpp"
#include <nlohmann/json.hpp>
#include <cstdlib>
#include <set>
#include <unordered_set>
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>

namespace lattice {

namespace {
constexpr const char* current_history_tables_sql =
    "SELECT name FROM sqlite_master WHERE type='table' "
    "AND name NOT LIKE 'sqlite_%' "
    "AND name NOT IN ('AuditLog', '_SyncControl') "
    "AND name NOT LIKE '\\_lattice\\_%' ESCAPE '\\' "
    "AND name NOT LIKE '%_vec0' AND name NOT LIKE '%_rtree%' "
    "AND name NOT LIKE '%\\_fts' ESCAPE '\\' "
    "AND name NOT LIKE '%\\_fts\\_%' ESCAPE '\\' "
    "AND name NOT LIKE '%\\_old' ESCAPE '\\' ORDER BY name";

std::string history_quote(const std::string& value, char delimiter) {
    std::string result(1, delimiter);
    for (char c : value) { result += c; if (c == delimiter) result += c; }
    return result + delimiter;
}
} // namespace

bool lattice_db::has_applied_sync_event(const std::string& global_id) {
    if (closed_.load(std::memory_order_acquire))
        throw std::runtime_error("Applied-event lookup requires an open database");
    if (txn_owner_thread_.load(std::memory_order_acquire) != std::this_thread::get_id() ||
        !db_->is_in_transaction())
        throw std::runtime_error("Applied-event lookup requires a checked transaction on this thread");
    if (global_id.empty()) return false;
    auto& reader = *db_;
    // Metadata probes choose only constant internal table/column names. The
    // final single SELECT reads the caller's checked snapshot. In particular,
    // never inspect a sync writer's uncommitted receipt on another thread.
    // No DDL, model mutation, receipt insertion, or cursor update occurs here.
    std::string sql = "SELECT 1 FROM AuditLog WHERE globalId = ?1";
    const bool receipts_exist = !reader.query(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_lattice_applied_receipts'").empty();
    if (receipts_exist) sql +=
        " UNION ALL SELECT 1 FROM _lattice_applied_receipts WHERE globalId = ?1";

    // Receipt retention can remove an old receipt. Only equality with an
    // actually persisted channel cursor is evidence for that exact event.
    // A newer UUID, timestamp, local audit id, or cursor ordering is not proof.
    for (const auto& row : reader.query("PRAGMA table_info(_lattice_replication_slots)")) {
        auto name = row.find("name");
        if (name != row.end() && std::holds_alternative<std::string>(name->second) &&
            std::get<std::string>(name->second) == "last_received_event_id") {
            sql += " UNION ALL SELECT 1 FROM _lattice_replication_slots WHERE last_received_event_id = ?1";
            break;
        }
    }
    sql += " LIMIT 1";
    return !reader.query(sql, {global_id}).empty();
}

std::string lattice_db::current_identity_snapshot() {
    if (closed_.load(std::memory_order_acquire))
        throw std::runtime_error("Identity snapshot requires an open database");
    if (txn_owner_thread_.load(std::memory_order_acquire) != std::this_thread::get_id() ||
        !db_->is_in_transaction())
        throw std::runtime_error("Identity snapshot requires a checked transaction");
    auto result = nlohmann::json::object();
    result["models"] = nlohmann::json::object();
    result["links"] = nlohmann::json::object();
    for (const auto& table_row : db_->query(current_history_tables_sql)) {
        const auto table = std::get<std::string>(table_row.at("name"));
        const auto identifier = history_quote(table, '"');
        const auto columns = db_->query("PRAGMA table_info(" + identifier + ")");
        bool has_id = false, has_global = false, has_lhs = false, has_rhs = false, has_rhs_type = false;
        for (const auto& column : columns) {
            const auto& name = std::get<std::string>(column.at("name"));
            has_id |= name == "id"; has_global |= name == "globalId";
            has_lhs |= name == "lhs"; has_rhs |= name == "rhs"; has_rhs_type |= name == "rhs_type";
        }
        const bool link = has_lhs && has_rhs && has_global && !has_id;
        if (!link && !(has_id && has_global)) continue;
        auto identities = nlohmann::json::array();
        const auto rows = db_->query("SELECT lower(globalId) AS globalId" +
            std::string(link ? ",lower(lhs) AS lhs,lower(rhs) AS rhs," + std::string(has_rhs_type ? "rhs_type" : "NULL AS rhs_type") : "") +
            " FROM " + identifier + " ORDER BY globalId COLLATE NOCASE");
        auto string_value = [](const auto& row, const char* key) -> std::string {
            auto found = row.find(key);
            if (found == row.end() || !std::holds_alternative<std::string>(found->second) || std::get<std::string>(found->second).empty())
                throw std::runtime_error("Invalid row identity in bootstrap snapshot");
            return std::get<std::string>(found->second);
        };
        for (const auto& row : rows) {
            const auto global = string_value(row, "globalId");
            if (link) {
                auto identity = nlohmann::json::array({global, string_value(row, "lhs"), string_value(row, "rhs")});
                identity.push_back(has_rhs_type ? nlohmann::json(string_value(row, "rhs_type")) : nlohmann::json(nullptr));
                identities.push_back(std::move(identity));
            } else identities.push_back(global);
        }
        result[link ? "links" : "models"][table] = std::move(identities);
    }
    return result.dump();
}

int64_t lattice_db::ensure_current_history() {
    if (closed_.load(std::memory_order_acquire))
        throw std::runtime_error("History publication requires an open database");
    if (txn_owner_thread_.load(std::memory_order_acquire) != std::this_thread::get_id() ||
        !db_->is_in_transaction())
        throw std::runtime_error("History publication requires a checked write transaction");

    // Match generate_history's model/link classification, but never batch-commit
    // or change sync state: this entire repair belongs to the caller's transaction.
    const auto tables = db_->query(current_history_tables_sql);
    // One transient index bounds the per-row baseline lookup. Do not drop an
    // index owned by a caller, and let rollback remove our DDL on any failure.
    const bool existing_index = !db_->query(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_lattice_current_history_baseline'").empty();
    if (!existing_index) db_->execute(
        "CREATE INDEX idx_lattice_current_history_baseline ON AuditLog(tableName,globalRowId,operation)");
    int64_t count = 0;
    for (const auto& table_row : tables) {
        const auto table = std::get<std::string>(table_row.at("name"));
        const auto identifier = history_quote(table, '"');
        const auto columns = db_->query("PRAGMA table_info(" + identifier + ")");
        bool has_id = false, has_global = false, has_lhs = false, has_rhs = false, has_rhs_type = false;
        for (const auto& column : columns) {
            const auto& name = std::get<std::string>(column.at("name"));
            has_id |= name == "id"; has_global |= name == "globalId";
            has_lhs |= name == "lhs"; has_rhs |= name == "rhs"; has_rhs_type |= name == "rhs_type";
        }
        const bool link = has_lhs && has_rhs && has_global && !has_id;
        if (!link && !(has_id && has_global)) continue;

        std::string fields, names;
        for (const auto& column : columns) {
            const auto name = std::get<std::string>(column.at("name"));
            if (name == "id" || name == "globalId") continue;
            if (link && name != "lhs" && name != "rhs" && !(has_rhs_type && name == "rhs_type")) continue;
            const auto value = "t." + history_quote(name, '"');
            const auto type = std::get<std::string>(column.at("type"));
            if (!fields.empty()) { fields += ","; names += ","; }
            names += history_quote(name, '\'');
            fields += history_quote(name, '\'') + ",";
            // Same flat payload as live audit triggers. REAL JSON must retain
            // all bits, and optional BLOBs must remain NULL (hex(NULL) is '').
            if (type == "BLOB") fields += "CASE WHEN " + value + " IS NULL THEN NULL ELSE hex(" + value + ") END";
            else if (type == "REAL") fields += "CASE WHEN " + value + " IS NULL THEN NULL ELSE json(printf('%!.17g'," + value + ")) END";
            else fields += value;
        }
        db_->execute(
            "INSERT INTO AuditLog(tableName,operation,rowId,globalRowId,changedFields,changedFieldsNames,"
            "timestamp,isFromRemote,isSynchronized,synthesized) "
            "SELECT ?,'INSERT'," + std::string(link ? "0" : "t.id") + ",t.globalId,json_object(" + fields + "),json_array(" + names + "),"
            "unixepoch('subsec'),0,0,0 FROM " + identifier + " t WHERE NOT EXISTS ("
            "SELECT 1 FROM AuditLog a WHERE a.tableName=? AND a.globalRowId=t.globalId AND a.operation='INSERT') "
            "ORDER BY " + std::string(link ? "t.rowid" : "t.id"), {table, table});
        count += sqlite3_changes(db_->handle());
    }
    if (!existing_index) db_->execute("DROP INDEX idx_lattice_current_history_baseline");
    return count;
}

int64_t lattice_db::append_current_snapshots(const std::vector<std::string>& tables, const std::string& snapshot_key) {
    if (closed_.load(std::memory_order_acquire))
        throw std::runtime_error("Snapshot publication requires an open database");
    if (txn_owner_thread_.load(std::memory_order_acquire) != std::this_thread::get_id() ||
        !db_->is_in_transaction())
        throw std::runtime_error("Snapshot publication requires the owning checked write transaction");
    if (snapshot_key.empty() || snapshot_key.size() > 256) throw std::runtime_error("Invalid snapshot publication key");
    const auto key = "authoritative_snapshot:" + snapshot_key;
    if (!db_->query("SELECT value FROM _lattice_meta WHERE key = ?", {key}).empty()) return 0;
    int64_t count = 0;
    std::set<std::string> seen;
    for (const auto& table : tables) {
        if (!seen.insert(table).second) continue;
        if (table.empty() || table.front() == '_' || table == "AuditLog" ||
            table.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_") != std::string::npos)
            throw std::runtime_error("Invalid model snapshot table");
        for (const auto& row : db_->query("SELECT * FROM \"" + table + "\" ORDER BY id")) {
            auto id = row.find("id"), global = row.find("globalId");
            if (id == row.end() || global == row.end() || !std::holds_alternative<int64_t>(id->second) ||
                !std::holds_alternative<std::string>(global->second)) throw std::runtime_error("Snapshot requires a model table");
            audit_log_entry entry;
            for (const auto& [name, value] : row) {
                if (name == "id" || name == "globalId") continue;
                entry.changed_fields[name] = any_property::from_column_value(value);
                entry.changed_fields_names.push_back(name);
            }
            // Match live trigger JSON: raw scalars and hex BLOB strings. This
            // preserves exact numbers without introducing the typed DATA
            // wire format, whose Swift Codable representation is different.
            auto typed = nlohmann::json::parse(entry.changed_fields_to_json());
            auto fields = nlohmann::json::object();
            for (const auto& item : typed.items()) fields[item.key()] = item.value().at("value");
            db_->execute("INSERT INTO AuditLog (tableName,operation,rowId,globalRowId,changedFields,changedFieldsNames,timestamp,isFromRemote,isSynchronized,synthesized) VALUES (?,'INSERT',?,?,?,?,unixepoch('subsec'),0,0,0)",
                {table, std::get<int64_t>(id->second), std::get<std::string>(global->second), fields.dump(), entry.changed_fields_names_to_json()});
            ++count;
        }
    }
    db_->execute("INSERT INTO _lattice_meta(key,value) VALUES(?,?)", {key, std::to_string(count)});
    return count;
}


namespace {
bool valid_local_metadata_utf8(const std::string& text) {
    size_t index = 0;
    while (index < text.size()) {
        const auto first = static_cast<unsigned char>(text[index++]);
        if (first <= 0x7f) continue;
        size_t trailing = 0;
        unsigned char second_min = 0x80, second_max = 0xbf;
        if (first >= 0xc2 && first <= 0xdf) trailing = 1;
        else if (first >= 0xe0 && first <= 0xef) {
            trailing = 2;
            if (first == 0xe0) second_min = 0xa0;  // no overlong forms
            if (first == 0xed) second_max = 0x9f;  // no UTF-16 surrogates
        } else if (first >= 0xf0 && first <= 0xf4) {
            trailing = 3;
            if (first == 0xf0) second_min = 0x90;
            if (first == 0xf4) second_max = 0x8f;  // at most U+10FFFF
        } else return false;
        if (text.size() - index < trailing) return false;
        const auto second = static_cast<unsigned char>(text[index]);
        if (second < second_min || second > second_max) return false;
        for (size_t offset = 1; offset < trailing; ++offset) {
            const auto next = static_cast<unsigned char>(text[index + offset]);
            if (next < 0x80 || next > 0xbf) return false;
        }
        index += trailing;
    }
    return true;
}

void validate_local_metadata_key(const std::string& name_space, const std::string& key) {
    // Fixed SQL identifiers; both inputs are bound values, never SQL syntax.
    // Namespaces deliberately exclude Lattice's own reserved prefix.
    if (name_space.empty() || name_space.size() > 64 ||
        name_space.front() < 'a' || name_space.front() > 'z' ||
        name_space.rfind("lattice.", 0) == 0 ||
        !std::all_of(name_space.begin(), name_space.end(), [](unsigned char c) {
            return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '-';
        }) || key.empty() || key.size() > 512 || key.find('\0') != std::string::npos ||
        !valid_local_metadata_utf8(key)) {
        throw db_error("Invalid local metadata namespace or key");
    }
}
}

std::optional<std::string> lattice_db::local_metadata(const std::string& name_space,
                                                      const std::string& key) {
    validate_local_metadata_key(name_space, key);
    const auto tables = query_read(
        "SELECT type FROM main.sqlite_master WHERE name = '_lattice_local_metadata_v1' COLLATE NOCASE");
    if (tables.empty()) return std::nullopt;
    if (tables.size() != 1 || std::get<std::string>(tables.front().at("type")) != "table")
        throw db_error("Invalid local metadata storage");
    const auto rows = query_read(
        "SELECT CASE WHEN typeof(value) = 'text' AND length(CAST(value AS BLOB)) <= 65536 "
        "THEN value ELSE NULL END AS value FROM main._lattice_local_metadata_v1 "
        "WHERE namespace = ? AND key = ? LIMIT 2",
        {name_space, key});
    if (rows.empty()) return std::nullopt;
    if (rows.size() != 1 || !std::holds_alternative<std::string>(rows.front().at("value")))
        throw db_error("Invalid local metadata value");
    const auto& value = std::get<std::string>(rows.front().at("value"));
    if (!valid_local_metadata_utf8(value)) throw db_error("Invalid local metadata text");
    return value;
}

bool lattice_db::insert_local_metadata_if_absent(const std::string& name_space,
                                                 const std::string& key,
                                                 const std::string& value) {
    validate_local_metadata_key(name_space, key);
    if (value.size() > 65'536) throw db_error("Local metadata value exceeds its size limit");
    if (!valid_local_metadata_utf8(value)) throw db_error("Invalid local metadata text");
    if (txn_owner_thread_.load(std::memory_order_acquire) != std::this_thread::get_id() ||
        !db_->is_in_transaction()) {
        throw db_error("Local metadata insertion requires the owning write transaction");
    }
    // No globalId, model registration, audit triggers or synchronization state.
    // Creation is part of the caller's transaction and rolls back with it.
    db_->execute("CREATE TABLE IF NOT EXISTS main._lattice_local_metadata_v1 ("
                 "namespace TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, "
                 "PRIMARY KEY(namespace, key)) WITHOUT ROWID");
    db_->execute("INSERT INTO main._lattice_local_metadata_v1(namespace, key, value) VALUES (?, ?, ?) "
                 "ON CONFLICT(namespace, key) DO NOTHING", {name_space, key, value});
    return sqlite3_changes(db_->handle()) == 1;
}

bool lattice_db::prepare_for_audited_writes() {
    if (config_.read_only) throw db_error("audited writes require a writable connection");
    return with_audit_prune_transaction_([&]() -> int64_t {
        // sync_disabled() resolves this name on the connection. A TEMP shadow
        // would make normalizing main's flag insufficient to enable auditing.
        if (!db_->query("SELECT 1 FROM temp.sqlite_schema WHERE name = '_SyncControl' COLLATE NOCASE").empty())
            throw db_error("audited writes reject a temporary sync control shadow");
        const auto tables = db_->query(
            "SELECT type, name, sql FROM main.sqlite_schema WHERE name = '_SyncControl' COLLATE NOCASE");
        if (tables.size() != 1 ||
            !std::holds_alternative<std::string>(tables[0].at("type")) ||
            std::get<std::string>(tables[0].at("type")) != "table" ||
            !std::holds_alternative<std::string>(tables[0].at("name")) ||
            std::get<std::string>(tables[0].at("name")) != "_SyncControl" ||
            !std::holds_alternative<std::string>(tables[0].at("sql")))
            throw db_error("audited writes require the canonical sync control table");
        // Accept formatting/case differences, but no alternate constraints,
        // extra columns, generated columns, views or virtual tables.
        std::string shape;
        for (unsigned char c : std::get<std::string>(tables[0].at("sql"))) {
            if (c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\f' || c == '\v') continue;
            shape += c >= 'A' && c <= 'Z' ? static_cast<char>(c + ('a' - 'A')) : static_cast<char>(c);
        }
        if (shape != "createtable_synccontrol(idintegerprimarykeycheck(id=1),disabledintegernotnulldefault0)" ||
            !detail::only_sync_storage_guards(*db_, "_SyncControl") ||
            !db_->query("SELECT 1 FROM temp.sqlite_schema WHERE type = 'trigger' "
                        "AND tbl_name = '_SyncControl' COLLATE NOCASE").empty())
            throw db_error("audited writes require the canonical sync control shape without triggers");

        // SQL whitespace normalization alone cannot distinguish keywords from
        // a custom declared type such as INTEGERPRIMARYKEY. Check SQLite's
        // parsed column metadata too, including generated/hidden columns.
        const std::vector<database::row_t> expected_columns = {
            {{"cid", int64_t{0}}, {"name", std::string("id")}, {"type", std::string("INTEGER")},
             {"notnull", int64_t{0}}, {"dflt_value", nullptr}, {"pk", int64_t{1}}, {"hidden", int64_t{0}}},
            {{"cid", int64_t{1}}, {"name", std::string("disabled")}, {"type", std::string("INTEGER")},
             {"notnull", int64_t{1}}, {"dflt_value", std::string("0")}, {"pk", int64_t{0}}, {"hidden", int64_t{0}}}
        };
        if (db_->query("PRAGMA main.table_xinfo('_SyncControl')") != expected_columns)
            throw db_error("audited writes require the canonical sync control column metadata");

        const auto rows = db_->query("SELECT id, disabled FROM main._SyncControl");
        if (rows.size() != 1 || !std::holds_alternative<int64_t>(rows[0].at("id")) ||
            std::get<int64_t>(rows[0].at("id")) != 1 ||
            !std::holds_alternative<int64_t>(rows[0].at("disabled")))
            throw db_error("audited writes require one integer sync control row with id 1");
        const auto disabled = std::get<int64_t>(rows[0].at("disabled"));
        if (disabled != 0 && disabled != 1)
            throw db_error("audited writes require a zero or one sync control flag");
        if (disabled == 0) return 0;
        db_->execute("UPDATE main._SyncControl SET disabled = 0 WHERE id = 1");
        if (sqlite3_changes64(db_->handle()) != 1 || sync_flag_in_owned_transaction_() != 0)
            throw db_error("audited writes did not normalize exactly one sync control row");
        return 1;
    }) != 0;
}

// Single definition of the global log level (declared extern in log.hpp).
// Seed from the LATTICE_LOG_LEVEL env var (0=off..4=debug) so logging can be
// enabled for a test/run without code changes, e.g. `LATTICE_LOG_LEVEL=4 swift test`.
static log_level initial_log_level() {
    if (const char* s = std::getenv("LATTICE_LOG_LEVEL")) {
        int v = std::atoi(s);
        if (v >= 0 && v <= static_cast<int>(log_level::debug)) {
            return static_cast<log_level>(v);
        }
    }
    return log_level::off;
}
std::atomic<log_level> g_log_level{initial_log_level()};

instance_guard::callback_frame*& instance_guard::tls_frames() noexcept {
    thread_local callback_frame* head = nullptr;
    return head;
}

// Singleton instance - defined here to ensure single copy across all translation units
instance_registry& instance_registry::instance() {
    // Intentionally leaked: prevents use-after-destroy when GCD callbacks
    // fire during process teardown (after atexit handlers run).
    static instance_registry* reg = new instance_registry();
    return *reg;
}

cross_process_notifier* instance_registry::get_or_create_notifier(const std::string& path) {
    // No cross-PROCESS notifier for any memory path (incl. named shared-cache
    // URIs): memory DBs are process-local; same-process cross-instance
    // delivery rides the instance registry (keyed on the canonical URI).
    if (configuration::path_is_memory(path)) return nullptr;

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

void lattice_db::setup_change_hook(database& connection) {
    LOG_DEBUG("setup_change_hook", "Setting up hooks for path: %s", config_.path.c_str());

    // Cache the page size for the WAL-threshold eviction check (results spec
    // §3.4): the WAL hook receives the log's FRAME count and may not run SQL
    // from its C frame, so bytes-per-frame must be known up front.
    try {
        auto rows = connection.query("PRAGMA page_size");
        if (!rows.empty()) {
            auto it = rows[0].find("page_size");
            if (it != rows[0].end() && std::holds_alternative<int64_t>(it->second)) {
                auto ps = std::get<int64_t>(it->second);
                if (ps > 0) wal_page_size_ = ps;
            }
        }
    } catch (const db_error&) {
        // keep the 4096 default
    }

    // Update hook - buffers changes (called for each row change)
    sqlite3_update_hook(connection.handle(),
        [](void* user_data, int operation, const char* db_name, const char* table_name, sqlite3_int64 rowid) {
            auto* self = static_cast<lattice_db*>(user_data);
            database::update_hook_scope callback_scope(*self->db_);

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
            if (table == "_SyncControl" || table == "_lattice_sync_set" || table == "_lattice_sync_state" ||
                table == "_lattice_local_metadata_v1") {
                LOG_DEBUG("update_hook", "Skipping internal table %s", table_name);
                return;
            }

            // Buffer exact AuditLog INSERT identities for every storage kind.
            // Delivery can be deferred past later commits, so reconstructing an
            // audit event with a latest-row lookup would substitute later history.
            if (table == "AuditLog") {
                if (operation == SQLITE_INSERT) {
                    self->append_to_change_buffer("AuditLog", "INSERT",
                                                  static_cast<int64_t>(rowid), "");
                    self->db_->mark_txn_dirty();
                    // Preserve the in-transaction xproc cursor advance: another
                    // same-path instance must not redispatch our local entries.
                    if (!self->config_.is_in_memory() &&
                        !self->defer_early_audit_cursor_.load(std::memory_order_acquire)) {
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

            // All storage kinds seal and deliver from the C++ statement tail.
            self->db_->mark_txn_dirty();
        },
        this
    );

    // WAL hook records commit bookkeeping only; never deliver from SQLite C frames.
    sqlite3_wal_hook(connection.handle(),
        [](void* user_data, sqlite3*, const char*, int nframes) -> int {
            auto* self = static_cast<lattice_db*>(user_data);

            // WAL-threshold keeper eviction (results spec §3.4): nframes is
            // the log's total frame count after this commit. Crossing the
            // threshold sets the per-instance eviction flag — a single
            // atomic store, legal in this C hook frame. Readers/maintenance
            // aggregate the flag per path and open the coordinated reader
            // gap (retire ALL same-path keepers, TRUNCATE-else-PASSIVE,
            // re-pin) off this thread.
            const int64_t threshold = self->wal_keeper_eviction_threshold_bytes_
                                          .load(std::memory_order_relaxed);
            if (threshold > 0 &&
                static_cast<int64_t>(nframes) * self->wal_page_size_ > threshold) {
                self->wal_eviction_pending_.store(true, std::memory_order_relaxed);
            }

            // Also arm empty-buffer bookkeeping commits. The post-statement
            // tail seals the current buffer before allowing another writer.
            self->db_->mark_txn_dirty();
            return SQLITE_OK;
        },
        this
    );

    // Commit sealing and delivery are separate: a later rollback can clear
    // only its current rows, while earlier committed rows await outer unlock.
    connection.set_txn_hooks(
        [this] { flush_changes(); },
        [this] {
            discard_change_buffer();
            fire_invalidation_hooks({}, invalidation_reason::rollback);
        },
        [this] { seal_change_buffer(); });
}
void lattice_db::setup_cross_process_notifier() {
    // Use the shared per-path notifier from instance_registry.
    // Only ONE Darwin listener per path per process — prevents N^2
    // notification amplification when multiple lattice_db instances
    // share the same DB file (e.g. via LatticeThreadSafeReference.resolve()).
    shared_xproc_notifier_ = instance_registry::instance().get_or_create_notifier(config_.path);
    if (!shared_xproc_notifier_) return;

    // Initialize this instance's cursor to current max AuditLog id
    auto max_rows = query_read("SELECT MAX(id) AS max_id FROM AuditLog");
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
    // Automatic notifications may arrive while the primary app store remains
    // open after a finite cycle. They must not start new background SQL.
    sync_borrow lifecycle(*this);
    if (!lifecycle.admitted() || background_sync_suspended_.load(std::memory_order_acquire)) return;
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
        auto query_notification = [this](const std::string& sql,
                const std::vector<column_value_t>& params = {}) {
            auto connection = borrow_xproc_read_connection();
            std::optional<database::cancellable_operation> cancellation;
            if (audit_maintenance_cancellable_.load(std::memory_order_acquire))
                cancellation.emplace(*connection, background_sync_suspended_);
            return connection->query(sql, params);
        };
        // Query AuditLog for entries newer than our cursor.
        // Uses the dedicated xproc read connection to avoid SQLite lock contention
        // with observer callbacks running on the scheduler (MainActor), which use
        // read_db() for existence checks.
        auto rows = query_notification(
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
            auto audit_gid_rows = query_notification(
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
            auto meta = query_notification(
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
                        auto cf_rows = query_notification(
                            "SELECT json_extract(changedFields, '$.lhs') AS lhs FROM AuditLog "
                            "WHERE globalRowId = ? AND tableName = ?",
                            {link_global_id, table}
                        );
                        if (!cf_rows.empty()) {
                            auto lhs_it = cf_rows[0].find("lhs");
                            if (lhs_it != cf_rows[0].end() && std::holds_alternative<std::string>(lhs_it->second)) {
                                parent_global_id = std::get<std::string>(lhs_it->second);
                                auto pid_rows = query_notification(
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
                    // Idempotent + atomic (refresh_vec0_row): the writer's
                    // triggers already indexed this row in the common case,
                    // so this is usually a lock-free read that skips. The
                    // old blind DELETE+INSERT here — run by EVERY process
                    // receiving the xproc notification — interleaved across
                    // processes into 28K UNIQUE failures and a write-lock
                    // storm that livelocked IPC sync (Aug 13 incident).
                    refresh_vec0_row(vec_table, global_id, vec_data);
                } catch (const std::exception& e) {
                    // Expected under write bursts (lock contention); the
                    // open-path gap reconcile and knn's count-mismatch
                    // self-heal cover any row missed here.
                    LOG_DEBUG("xproc", "vec0 reconcile failed for %s: %s", vec_table.c_str(), e.what());
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
// Used for WSS (key = effective logical peer identity) and IPC (key = channel name).
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
        ids.push_back("wss:" + config.effective_websocket_sync_identity());
    }
    for (const auto& target : config.ipc_targets) {
        ids.push_back("ipc:" + target.channel);
    }
    return ids;
}

lattice_db::sync_borrow::sync_borrow(const lattice_db& owner, bool mutation)
    : state_(owner.sync_lifecycle_) {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->transitioning || owner.is_closed()) {
        if (mutation && state_->transitioning)
            throw db_error("sync lifecycle transition is in progress");
        return;
    }
    admitted_ = true;
    pointer_ = owner.synchronizer_.get();
    ++state_->borrowers;
    ++state_->borrower_threads[std::this_thread::get_id()];
}

lattice_db::sync_borrow::~sync_borrow() {
    if (!admitted_) return;
    std::lock_guard<std::mutex> lock(state_->mutex);
    --state_->borrowers;
    auto it = state_->borrower_threads.find(std::this_thread::get_id());
    if (--it->second == 0) state_->borrower_threads.erase(it);
    state_->cv.notify_all();
}

lattice_db::sync_borrow::sync_borrow(lattice_db& owner,
        const std::optional<std::vector<sync_filter_entry>>& filter)
    : state_(owner.sync_lifecycle_) {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (owner.is_closed()) return;
    owner.config_.sync_filter = filter;
    ++state_->filter_revision;
    // A factory-time request is retained without borrowing a half-built owner.
    // Once setup publishes its policy target, normal borrow retirement applies.
    if (state_->transitioning && !state_->policy_ready) return;
    admitted_ = true;
    pointer_ = owner.synchronizer_.get();
    ++state_->borrowers;
    ++state_->borrower_threads[std::this_thread::get_id()];
}

bool lattice_db::begin_sync_transition(bool wait, bool retry_setup, bool require_empty_owner) {
    const auto state = sync_lifecycle_;
    std::unique_lock<std::mutex> lock(state->mutex);
    if (state->transitioning) {
        if (retry_setup) state->setup_retry_requested = true;
        if (!wait || state->transition_thread == std::this_thread::get_id() ||
            state->borrower_threads.count(std::this_thread::get_id()) ||
            (state->worker && state->worker->is_on_thread())) return false;
        state->cv.wait(lock, [&] { return !state->transitioning; });
    }
    // An ordinary handoff scan may visit an already-live sibling. Its no-op
    // setup must not close policy admission or strand a concurrent filter.
    if (require_empty_owner && synchronizer_) return false;
    if (!wait && state->borrower_threads.count(std::this_thread::get_id())) return false;
    state->transitioning = true;
    state->policy_ready = false;
    state->transition_thread = std::this_thread::get_id();
    // Teardown can retire its owner off-thread if reached inside a borrow.
    // Setup never runs reentrantly and must settle existing borrowers first.
    if (!state->borrower_threads.count(std::this_thread::get_id()))
        state->cv.wait(lock, [&] { return state->borrowers == 0; });
    return true;
}

void lattice_db::end_sync_transition() {
    const auto state = sync_lifecycle_;
    bool retry;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->transitioning = false;
        state->transition_thread = {};
        retry = std::exchange(state->setup_retry_requested, false);
        state->cv.notify_all();
    }
    if (retry && !is_closed() && !config_.background_sync_managed &&
        current_background_sync_generation() == 0) setup_sync_if_configured(true);
}

void lattice_db::replay_sync_state_subscription(const std::shared_ptr<sync_lifecycle_state>& state) {
    std::shared_ptr<scheduler> worker;
    uint64_t epoch, revision;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (!state->connected || !state->state_handler || !state->worker) return;
        worker = state->worker;
        epoch = state->callback_epoch;
        revision = state->state_callback_revision;
    }
    worker->invoke([state, epoch, revision] {
        std::function<void(bool)> handler;
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            if (state->callback_epoch != epoch || !state->connected ||
                state->state_callback_revision != revision ||
                state->state_delivered_revision >= revision) return;
            state->state_delivered_revision = revision;
            handler = state->state_handler;
        }
        if (handler) handler(true);
    });
}

void lattice_db::setup_sync_if_configured(bool retry_setup) {
    if (!begin_sync_transition(false, retry_setup, true)) return;
    try {
        setup_sync_owned();
        if (is_closed()) {
            stop_audit_maintenance();
            retire_wss_owned(false, false);
        } else end_sync_transition();
    } catch (...) { end_sync_transition(); throw; }
}

void lattice_db::setup_sync_owned(bool finite_cycle,
        std::shared_ptr<std::atomic<bool>> cancellation, bool allow_sibling_kick) {
    if (is_closed() || config_.read_only || !sync_start_requested_ ||
        !config_.is_sync_enabled() || synchronizer_) {
        return;
    }

#ifndef __EMSCRIPTEN__
    // Cross-process flock: only one in-process lattice_db at a time may
    // own the WSS synchronizer for a given path. If the lock is held, we
    // distinguish three cases:
    //   1. Held by another *process* — silent skip (today's behavior).
    //   2. Held by an in-process sibling with the SAME URL or logical identity — silent
    //      skip (legitimate dormant duplicate, e.g. cross-actor cache miss).
    //   3. Held by an in-process sibling with a DIFFERENT URL and logical identity — this
    //      is a URL-change scenario (e.g. peer rejoining after host
    //      republished). Kick the sibling and take over.
    //
    // Skipped on WASM — single-threaded, single-process environment.
    if (sync_lock_fd_ < 0) {
        const std::string lock_path = config_.path + ".sync.lock";

        auto try_acquire_lock = [&]() -> bool {
            int fd = ::open(lock_path.c_str(), O_CREAT | O_RDWR, 0600);
            if (fd < 0) return false;
            if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
                ::close(fd);
                return false;
            }
            sync_lock_fd_ = fd;
            return true;
        };

        if (!try_acquire_lock()) {
            // Try to find a kickable in-process sibling — one that owns
            // sync (synchronizer_ != nullptr) on a DIFFERENT URL and logical peer.
            //
            // The kick must happen INSIDE the registry callback so the
            // registry's notify_refcount keeps the victim alive while
            // teardown_sync runs. Its value-only successor request targets
            // this URL after the old callback releases the flock/key; an
            // immediate retry alone can miss that deferred release.
            bool kicked = false;
            if (allow_sibling_kick) instance_registry::instance().for_each_alive(config_.path,
                [&](lattice_db* sibling) {
                    if (kicked) return;
                    if (sibling == this) return;
                    {
                        sync_borrow current(*sibling);
                        if (!current) return;
                        // Managed lifecycles are never evicted by ordinary URL handoff.
                        if (sibling->current_background_sync_generation() != 0) return;
                    }
                    if (sibling->config_.websocket_url == config_.websocket_url ||
                        sibling->config_.effective_websocket_sync_identity() ==
                        config_.effective_websocket_sync_identity()) return;
                    LOG_INFO("lattice_db",
                             "kicking sibling=%p (URL=%s) for URL change to %s",
                             (void*)sibling, sibling->config_.websocket_url.c_str(),
                             config_.websocket_url.c_str());
                    sibling->teardown_sync(/*fire_handoff=*/true, config_.websocket_url);
                    kicked = true;
                });
            if (!kicked || !try_acquire_lock()) {
                LOG_DEBUG("lattice_db", "WSS sync lock held, skipping setup");
                return;
            }
        }
    }
#endif

    // One synchronizer per durable peer, including platforms without the file lock.
    if (!try_register_sync_key(config_.path, config_.effective_websocket_sync_identity())) {
        LOG_DEBUG("lattice_db", "Synchronizer already active for this path, skipping");
#ifndef __EMSCRIPTEN__
        if (sync_lock_fd_ >= 0) {
            ::flock(sync_lock_fd_, LOCK_UN); ::close(sync_lock_fd_); sync_lock_fd_ = -1;
        }
#endif
        return;
    }

    try {
    uint64_t callback_epoch;
    {
        std::lock_guard<std::mutex> lock(sync_lifecycle_->mutex);
        callback_epoch = ++sync_lifecycle_->callback_epoch;
        sync_lifecycle_->connected = false;
    }
    // Create sync config from our configuration
    sync_config sync_cfg;
    uint64_t filter_revision;
    sync_cfg.websocket_url = config_.websocket_url;
    sync_cfg.authorization_token = config_.authorization_token;
    sync_cfg.pinned_server_certificate_sha256 = config_.pinned_server_certificate_sha256;
    sync_cfg.uploads_enabled = config_.uploads_enabled;
    {
        std::lock_guard<std::mutex> lock(sync_lifecycle_->mutex);
        sync_cfg.sync_filter = config_.sync_filter;
        filter_revision = sync_lifecycle_->filter_revision;
    }
    sync_cfg.finite_cycle = finite_cycle;

    // Always use per-synchronizer sync state
    auto all_ids = collect_all_sync_ids(config_);
    sync_cfg.sync_id = "wss:" + config_.effective_websocket_sync_identity();
    {
        // Log-only identity (see sync_config::log_label).
        const auto slash = config_.path.find_last_of('/');
        sync_cfg.log_label = sync_cfg.sync_id + "@" +
            (slash == std::string::npos ? config_.path : config_.path.substr(slash + 1));
    }
    sync_cfg.all_active_sync_ids = all_ids;
    sync_cfg.is_observer = config_.sync_is_observer || !config_.uploads_enabled;
    config_.tuning.apply(sync_cfg);
    // Upload coalescing stays at the library default (0 = legacy immediate
    // dispatch): with the upload floor + classify gating, passes are cheap,
    // and enabling pacing here measurably collapsed relay-chain catch-up
    // throughput (each enumeration window paid the full coalesce delay).
    // Consumers with pathological tick rates can opt in via sync_config.

#ifdef __EMSCRIPTEN__
    // Emscripten: single-threaded, so the synchronizer borrows our connection
    // directly. This avoids opening a second connection to the same OPFS file,
    // which would fail due to exclusive locking.
    LOG_INFO("lattice_db", "sync using shared db (Emscripten), path=%s", config_.path.c_str());
    if (!config_.sync_autostart) heal_collapsed_sync_state();
#else
    // Native: create a dedicated lattice_db for the synchronizer (separate
    // connection on its own thread via std_thread_scheduler).
    std::string sync_path = resolve_path(config_);
    configuration sync_db_config(sync_path,
                                 std::make_shared<std_thread_scheduler>());
    sync_db_config.busy_timeout_ms = config_.busy_timeout_ms;
    sync_db_config.target_schema_version = config_.target_schema_version;
    sync_db_config.migration_block = config_.migration_block;
    sync_db_config.create_if_missing = config_.create_if_missing;
    sync_db_config.private_sync_cancellation = cancellation;
    auto sync_db = std::make_unique<lattice_db>(sync_db_config);
    {
        std::lock_guard<std::mutex> lock(sync_lifecycle_->mutex);
        sync_lifecycle_->worker = sync_db_config.sched;
    }

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

    // One forwarding wrapper per owner. Subscription changes are metadata-only
    // even while setup/retirement closes ordinary pointer admission. Captures
    // own shared state, never the primary store that a callback may close.
    const auto callback_state = sync_lifecycle_;
#ifdef __EMSCRIPTEN__
    {
        std::lock_guard<std::mutex> lock(callback_state->mutex);
        callback_state->worker = scheduler_;
    }
#endif
    synchronizer_->set_on_state_change([callback_state, callback_epoch](bool connected) {
        std::function<void(bool)> handler;
        {
            std::lock_guard<std::mutex> lock(callback_state->mutex);
            if (callback_state->callback_epoch != callback_epoch) return;
            callback_state->connected = connected;
            callback_state->state_delivered_revision = callback_state->state_callback_revision;
            handler = callback_state->state_handler;
        }
        if (handler) handler(connected);
    });
    synchronizer_->set_on_error([callback_state, callback_epoch](const std::string& error) {
        std::function<void(const std::string&)> handler;
        {
            std::lock_guard<std::mutex> lock(callback_state->mutex);
            if (callback_state->callback_epoch != callback_epoch) return;
            handler = callback_state->error_handler;
        }
        if (handler) handler(error);
    });
    synchronizer_->set_on_progress([callback_state, callback_epoch](const synchronizer::sync_progress& progress) {
        synchronizer::on_progress_handler handler;
        {
            std::lock_guard<std::mutex> lock(callback_state->mutex);
            if (callback_state->callback_epoch != callback_epoch) return;
            handler = callback_state->progress_handler;
        }
        if (handler) handler(progress);
    });
    if (cancellation && cancellation->load(std::memory_order_acquire))
        throw db_error("background sync was cancelled during setup");

#ifndef __EMSCRIPTEN__
    {
        std::lock_guard<std::mutex> lock(callback_state->mutex);
        // Native setup owns a distinct serial worker. Queue the latest policy
        // before its first connect; no SQL or user callback runs on this thread.
        // Publishing the target in the same critical section closes the gap
        // between retaining a factory-time request and admitting a live borrow.
        if (filter_revision != callback_state->filter_revision) {
            if (config_.sync_filter) synchronizer_->update_sync_filter(*config_.sync_filter);
            else synchronizer_->clear_sync_filter();
        }
        callback_state->policy_ready = true;
    }
#else
    // Browser setup is single-threaded and borrows the parent scheduler. Apply
    // outside the metadata lock because that scheduler may run inline.
    std::optional<std::vector<sync_filter_entry>> latest_filter;
    bool filter_changed;
    {
        std::lock_guard<std::mutex> lock(callback_state->mutex);
        latest_filter = config_.sync_filter;
        filter_changed = filter_revision != callback_state->filter_revision;
        callback_state->policy_ready = true;
    }
    if (filter_changed) {
        if (latest_filter) synchronizer_->update_sync_filter(*latest_filter);
        else synchronizer_->clear_sync_filter();
    }
#endif
    // Auto-connect (like Swift's Lattice.init)
    synchronizer_->connect();
    replay_sync_state_subscription(callback_state);
    } catch (...) {
        // Manual start is retryable on the same local handle after setup fails.
        // Do not strand an ownership key/flock or tear down unrelated IPC.
        {
            std::unique_lock<std::mutex> lock(sync_lifecycle_->mutex);
            sync_lifecycle_->policy_ready = false;
            sync_lifecycle_->cv.wait(lock, [&] { return sync_lifecycle_->borrowers == 0; });
        }
        synchronizer_.reset();
        unregister_sync_key(config_.path, config_.effective_websocket_sync_identity());
#ifndef __EMSCRIPTEN__
        if (sync_lock_fd_ >= 0) {
            ::flock(sync_lock_fd_, LOCK_UN); ::close(sync_lock_fd_); sync_lock_fd_ = -1;
        }
#endif
        throw;
    }
}

uint64_t lattice_db::current_background_sync_generation() const {
    // Logical close can precede callback-reentrant retirement. Keep its owner
    // receipt available until private cleanup has actually settled.
    std::lock_guard<std::mutex> lock(sync_lifecycle_->mutex);
    return sync_lifecycle_->generation;
}

synchronizer_base::finite_cycle_status lattice_db::get_background_sync_status(uint64_t generation) const {
    std::lock_guard<std::mutex> lock(sync_lifecycle_->mutex);
    if (!generation || sync_lifecycle_->generation != generation ||
        sync_lifecycle_->transitioning || !synchronizer_) return {};
    return synchronizer_->get_finite_cycle_status();
}

bool lattice_db::cancel_background_sync(uint64_t generation) {
    std::lock_guard<std::mutex> lock(sync_lifecycle_->mutex);
    if (!generation || sync_lifecycle_->generation != generation) return false;
    if (sync_lifecycle_->cancellation)
        sync_lifecycle_->cancellation->store(true, std::memory_order_release);
    // Metadata only; transitions keep private construction/destruction off this path.
    if (!sync_lifecycle_->transitioning && synchronizer_) synchronizer_->cancel_finite_cycle();
    return true;
}

lattice_db::background_sync_start lattice_db::resume_background_sync(
        std::shared_ptr<std::atomic<bool>> cancellation, bool finite_cycle) {
#ifdef __EMSCRIPTEN__
    return {background_sync_admission::unsupported, 0};
#else
    if (is_closed()) return {background_sync_admission::closed, 0};
    const bool supports_wss = !config_.read_only && config_.is_sync_enabled();
    if (config_.is_in_memory() || config_.is_ipc_enabled() ||
        (!supports_wss && (finite_cycle || !config_.background_sync_managed)))
        return {background_sync_admission::unsupported, 0};
    if (!cancellation) cancellation = std::make_shared<std::atomic<bool>>(false);
    if (cancellation->load(std::memory_order_acquire)) return {background_sync_admission::cancelled, 0};
    if (!begin_sync_transition(false)) return {background_sync_admission::already_running, 0};
    bool owns_local_generation;
    {
        std::lock_guard<std::mutex> lock(sync_lifecycle_->mutex);
        owns_local_generation = sync_lifecycle_->generation != 0;
    }
    if (is_closed() || synchronizer_ || owns_local_generation) {
        const auto result = is_closed() ? background_sync_admission::closed : background_sync_admission::already_running;
        end_sync_transition();
        return {result, 0};
    }
    // A callback may have retired the previous owner off-thread. Apply its
    // pending parent restoration under this transition before taking a new one.
    uint64_t previously_settled;
    {
        std::lock_guard<std::mutex> lock(sync_lifecycle_->mutex);
        previously_settled = sync_lifecycle_->settled_generation;
    }
    restore_parent_after_sync_owned(previously_settled);
    uint64_t generation;
    {
        std::lock_guard<std::mutex> lock(sync_lifecycle_->mutex);
        generation = ++sync_lifecycle_->sequence;
        sync_lifecycle_->generation = generation;
        sync_lifecycle_->cancellation = cancellation;
        if (finite_cycle && !config_.background_sync_managed) {
            sync_lifecycle_->restore_parent_generation = generation;
            sync_lifecycle_->prior_parent_suspended = background_sync_suspended_.load(std::memory_order_acquire);
            sync_lifecycle_->prior_retention_running = audit_maint_thread_.joinable();
        }
    }
    background_sync_suspended_.store(true, std::memory_order_release);
    audit_maintenance_cancellable_.store(true, std::memory_order_release);
    sync_start_requested_ = true;
    try {
        // Admission settles retention while private WSS work runs. Managed
        // stores stay suspended; ordinary manual calls restore their prior state.
        stop_audit_maintenance();
        setup_sync_owned(finite_cycle, cancellation, false);
        if ((!synchronizer_ && finite_cycle) || cancellation->load(std::memory_order_acquire) || is_closed()) {
            const auto result = is_closed() ? background_sync_admission::closed :
                (cancellation->load(std::memory_order_acquire) ? background_sync_admission::cancelled : background_sync_admission::not_owner);
            sync_start_requested_ = false;
            retire_wss_owned(false, false);
            return {result, generation};
        }
        if (!finite_cycle) {
            // Another same-path owner can legitimately hold WSS. Foreground
            // observation/readers/retention still belong to this local receipt;
            // suspension must settle them without touching that other owner.
            background_sync_suspended_.store(false, std::memory_order_release);
            start_audit_maintenance();
        }
        end_sync_transition();
        return {background_sync_admission::started, generation};
    } catch (...) {
        const bool was_cancelled = cancellation->load(std::memory_order_acquire);
        sync_start_requested_ = false;
        background_sync_suspended_.store(true, std::memory_order_release);
        stop_audit_maintenance();
        retire_wss_owned(false, false);
        if (was_cancelled)
            return {background_sync_admission::cancelled, generation};
        throw;
    }
#endif
}

bool lattice_db::suspend_background_sync(uint64_t generation) {
    if (!generation) return false;
    const auto state = sync_lifecycle_;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (state->generation != generation &&
            (state->generation || state->settled_generation != generation)) return false;
        if (state->generation == generation) {
            if (state->cancellation) state->cancellation->store(true, std::memory_order_release);
            // Before waiting for admitted xproc reads: their scoped busy handler
            // observes this flag and returns without waiting the ordinary budget.
            background_sync_suspended_.store(true, std::memory_order_release);
        }
    }
    if (!begin_sync_transition(true)) return false;
    bool already_settled;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        already_settled = !state->generation && state->settled_generation == generation;
        if (state->generation != generation && !already_settled) {
            state->transitioning = false;
            state->transition_thread = {};
            state->cv.notify_all();
            return false;
        }
    }
    if (already_settled) {
        restore_parent_after_sync_owned(generation);
        end_sync_transition();
        return true;
    }
    sync_start_requested_ = false;
    stop_audit_maintenance();
    if (config_.background_sync_managed) retire_all_read_generations();
    return retire_wss_owned(false, false);
}

void lattice_db::restore_parent_after_sync_owned(uint64_t generation) {
    bool restore = false, prior_suspended = true, prior_retention = false;
    {
        std::lock_guard<std::mutex> lock(sync_lifecycle_->mutex);
        if (generation && sync_lifecycle_->restore_parent_generation == generation &&
            !sync_lifecycle_->generation && sync_lifecycle_->settled_generation == generation) {
            restore = true;
            prior_suspended = sync_lifecycle_->prior_parent_suspended;
            prior_retention = sync_lifecycle_->prior_retention_running;
            sync_lifecycle_->restore_parent_generation = 0;
        }
    }
    if (!restore || is_closed()) return;
    background_sync_suspended_.store(prior_suspended, std::memory_order_release);
    if (prior_retention) start_audit_maintenance();
}

bool lattice_db::retire_wss_owned(bool drain, bool handoff, std::optional<std::string> successor_url) {
    const auto state = sync_lifecycle_;
    bool defer;
    uint64_t generation;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        generation = state->generation;
        state->connected = false;
        if (generation && state->cancellation)
            state->cancellation->store(true, std::memory_order_release);
        defer = state->borrower_threads.count(std::this_thread::get_id()) ||
                (state->worker && state->worker->is_on_thread()) ||
                (!generation && successor_url.has_value());
    }
    // Admission is already closed. Moving the owner does not invalidate an
    // existing lease; cleanup waits for every such borrow before destruction.
    auto retired = std::move(synchronizer_);
    const int lock_fd = std::exchange(sync_lock_fd_, -1);
    const auto path = config_.path;
    const auto url = config_.websocket_url;
    const auto registry_key = config_.effective_websocket_sync_identity();
    const auto next_url = successor_url.value_or(url);
    const auto notify_successor = [state, path, next_url, handoff, generation] {
        if (!handoff || generation) return;
        bool handed_off = false;
        instance_registry::instance().for_each_alive(path, [&](lattice_db* sibling) {
            if (handed_off || sibling->sync_lifecycle_ == state || sibling->is_closed() ||
                !sibling->sync_start_requested_ || sibling->config_.background_sync_managed ||
                sibling->current_background_sync_generation() != 0 ||
                !sibling->config_.is_sync_enabled() || sibling->config_.websocket_url != next_url) return;
            // If its initiating setup has not yet returned, retain exactly one
            // retry in that transition instead of losing the release edge.
            sibling->setup_sync_if_configured(true);
            sync_borrow current(*sibling);
            handed_off = bool(current);
        });
    };
    auto cleanup = [state, owner = std::move(retired), lock_fd, path, registry_key, generation, drain,
                    notify_successor](bool finish_transition) mutable {
        {
            std::unique_lock<std::mutex> lock(state->mutex);
            state->cv.wait(lock, [&] { return state->borrowers == 0; });
        }
        if (owner) {
            if (drain) owner->drain(std::chrono::steady_clock::now() + std::chrono::milliseconds(2000));
            if (generation) owner->cancel_finite_cycle();
            owner.reset(); // Fences transport, pacer and queued SQL before releasing ownership.
            unregister_sync_key(path, registry_key);
        }
        if (lock_fd >= 0) { ::flock(lock_fd, LOCK_UN); ::close(lock_fd); }
        std::shared_ptr<scheduler> settled_worker;
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            settled_worker = std::move(state->worker);
            ++state->callback_epoch;
            state->connected = false;
            state->cancellation.reset();
            state->settled_generation = generation;
            state->generation = 0;
            if (finish_transition) {
                state->transitioning = false;
                state->transition_thread = {};
                state->setup_retry_requested = false;
                state->cv.notify_all();
            }
        }
        settled_worker.reset();
        // This owns only shared lifecycle state and value identities. It is
        // valid after the primary handle has closed or been destroyed.
        try { notify_successor(); }
        catch (const std::exception& error) { LOG_ERROR("sync", "Deferred handoff failed: %s", error.what()); }
        catch (...) { LOG_ERROR("sync", "Deferred handoff failed with an unknown exception"); }
    };
#ifndef __EMSCRIPTEN__
    if (defer) {
        // A user callback may close its store. Joining that callback's worker
        // here would self-deadlock; the retirement bundle owns all its resources.
        std::thread([cleanup = std::move(cleanup)]() mutable { cleanup(true); }).detach();
        return false;
    }
#endif
    cleanup(false);
    restore_parent_after_sync_owned(generation);
    end_sync_transition();
    return true;
}

void lattice_db::setup_ipc_if_configured() {
#ifndef __EMSCRIPTEN__
    if(is_closed() || !config_.is_ipc_enabled()) return;
    const auto config=config_;
    const auto all_ids=collect_all_sync_ids(config);
    std::vector<std::shared_ptr<ipc_sync_state>> channels;
    for(const auto& target:config.ipc_targets) {
        auto resources=std::make_shared<sync_owner_resources>();
        resources->key=acquire_sync_key(config.path,"ipc:"+target.channel);
        if(!resources->key) continue;
        const auto socket_path=target.socket_path.value_or(resolve_ipc_socket_path(target.channel));
        const auto lock_path=socket_path+".lock";
        const int fd=::open(lock_path.c_str(),O_CREAT|O_RDWR,0600);
        if(fd>=0) {
            if(::flock(fd,LOCK_EX|LOCK_NB)==0) resources->file=adopt_sync_file(fd);
            else ::close(fd); // existing IPC role negotiation still works when another process owns this lock
        }
        auto channel=std::make_shared<ipc_sync_state>();channel->channel=target.channel;channel->resources=resources;
        channel->endpoint=std::make_unique<ipc_endpoint>(target.channel,target.socket_path);
        sync_config settings;settings.sync_id="ipc:"+target.channel;
        const auto slash=config.path.find_last_of('/');
        settings.log_label=settings.sync_id+"@"+(slash==std::string::npos ? config.path : config.path.substr(slash+1));
        settings.all_active_sync_ids=all_ids;settings.is_observer=config.sync_is_observer;
        settings.uploads_enabled=config.uploads_enabled;
        settings.owner_active=resources->active;
        settings.sync_filter=target.sync_filter;settings.narrowing_emits_removals=target.narrowing_emits_removals;
        config.tuning.apply(settings);
        configuration database_config(config.path);
        database_config.busy_timeout_ms=config.busy_timeout_ms;
        database_config.target_schema_version=config.target_schema_version;
        database_config.migration_block=config.migration_block;
        database_config.create_if_missing=config.create_if_missing;
        channel->publication=std::make_shared<detail::ipc_publication_state>(settings,
            [database_config,resources](std::unique_ptr<ipc_socket_client> transport,const sync_config& latest) {
                auto owned_config=database_config;owned_config.sched=std::make_shared<std_thread_scheduler>();
                auto database=std::make_unique<lattice_db>(owned_config);
                return synchronizer::create_owned(std::move(database),latest,std::move(transport),resources);
            });
        channels.push_back(std::move(channel));
    }
    if(channels.empty()) return;
    {
        std::lock_guard<std::mutex> lock(ipc_callbacks_mutex_);
        if(is_closed()) return;
        for(const auto& channel:channels) {
            channel->publication->set_progress(on_sync_progress_);
            channel->publication->set_state(on_sync_state_change_);
            channel->publication->set_error(on_sync_error_);
        }
        ipc_synchronizers_.insert(ipc_synchronizers_.end(),channels.begin(),channels.end());
    }
    // Only shared channel state is used after publication. A callback may close its parent while
    // an endpoint starts; revoke/endpoint_mutex serialize that race without parent/slot captures.
    try {for(const auto& channel:channels) channel->start();}
    catch(...) {
        std::vector<std::shared_ptr<synchronizer>> failed;
        for(const auto& channel:channels) if(auto sync=channel->publication->revoke()) failed.push_back(std::move(sync));
        for(const auto& sync:failed) sync->disconnect();
        for(const auto& channel:channels) channel->stop();
        {
            std::lock_guard<std::mutex> lock(ipc_callbacks_mutex_);
            ipc_synchronizers_.erase(std::remove_if(ipc_synchronizers_.begin(),ipc_synchronizers_.end(),
                [&](const auto& entry) {return std::find(channels.begin(),channels.end(),entry)!=channels.end();}),ipc_synchronizers_.end());
        }
        throw;
    }
#endif
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

namespace {
std::string attach_alias_for(const std::string& path) {
    std::filesystem::path p = path;
    return p.filename().replace_extension().string();
}

// Attachment aliases come from caller-controlled path stems. Use the same
// SQLite identifier escaping for view qualifiers and DETACH.
std::string quoted_attach_alias(const std::string& alias) {
    std::string quoted = "\"";
    for (char c : alias) {
        quoted += c;
        if (c == '"') quoted += '"';
    }
    quoted += '"';
    return quoted;
}

std::string attachment_uri_encode(const std::string& value, bool path) {
    constexpr char hex[] = "0123456789ABCDEF";
    std::string encoded;
    for (unsigned char c : value) {
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_' || c == '~' ||
            (path && c == '/')) {
            encoded += static_cast<char>(c);
        } else {
            encoded += '%';
            encoded += hex[c >> 4];
            encoded += hex[c & 15];
        }
    }
    return encoded;
}

std::string read_only_attachment_uri(database& source) {
    if (source.is_closed()) throw db_error("attach: the database being attached is closed");
    auto* handle = source.handle();
    auto* mutex = sqlite3_db_mutex(handle);
    sqlite3_mutex_enter(mutex);
    struct unlock { sqlite3_mutex* value; ~unlock() { sqlite3_mutex_leave(value); } } held{mutex};
    if (source.is_closed()) throw db_error("attach: the database being attached is closed");
    // Use SQLite's actual file name, not the configuration's URI text or its
    // substring-based memory classification. Copy all data while it is owned.
    const char* filename = sqlite3_db_filename(handle, "main");
    if (!filename || !*filename)
        throw db_error("attach: read-only attachments require a file-backed source");
    std::string uri = "file://" + attachment_uri_encode(filename, true) + "?mode=ro";
    std::unordered_set<std::string> parameters;
    for (int i = 0; const char* key = sqlite3_uri_key(filename, i); ++i) {
        if (!parameters.emplace(key).second)
            throw db_error("attach: duplicate source URI parameter");
        if (std::string(key) == "mode") continue;
        // Preserve the explicit VFS/cache selection. Other options need their
        // own semantics review; in particular, never introduce immutable or
        // nolock into an attachment that must see live committed WAL content.
        if (std::string(key) != "vfs" && std::string(key) != "cache")
            throw db_error("attach: unsupported read-only source URI parameter");
        const char* value = sqlite3_uri_parameter(filename, key);
        uri += "&" + attachment_uri_encode(key, false) + "=" +
               attachment_uri_encode(value ? value : "", false);
    }
    return uri;
}

// Return whether this call created the alias, so failure cleanup never
// detaches an alias that was already present on this connection.
bool attach_database(database& connection, const std::string& alias,
                     const std::string& target, bool read_only,
                     bool tolerate_existing = false) {
    try {
        connection.execute("ATTACH DATABASE ? AS " + quoted_attach_alias(alias), {target});
    } catch (const db_error& error) {
        // Explicit writable attach retains the released duplicate-alias
        // tolerance. Reopening a connection still requires a fresh alias,
        // and read-only sources must establish their own checked alias.
        if (tolerate_existing && !read_only &&
            std::string(error.what()).find("already in use") != std::string::npos)
            return false;
        throw;
    }
    if (read_only && sqlite3_db_readonly(connection.handle(), alias.c_str()) != 1) {
        connection.execute("DETACH DATABASE " + quoted_attach_alias(alias));
        throw db_error("attach: source alias did not open read-only");
    }
    return true;
}

// Model tables only — excludes sqlite internals, _-prefixed virtual/shadow
// tables, and Lattice bookkeeping. %s is the sqlite_master to scan.
constexpr const char* kAttachTableFilter =
    "SELECT name FROM %s WHERE type='table' "
    "AND name NOT LIKE 'sqlite_%%' "
    "AND name NOT LIKE '\\_%%' ESCAPE '\\' "
    "AND name NOT IN ('AuditLog')";

std::unordered_set<std::string> model_tables(lattice::database* db, const char* master) {
    char sql[512];
    snprintf(sql, sizeof(sql), kAttachTableFilter, master);
    std::unordered_set<std::string> out;
    for (const auto& row : db->query(sql)) {
        auto it = row.find("name");
        if (it != row.end() && std::holds_alternative<std::string>(it->second))
            out.insert(std::get<std::string>(it->second));
    }
    return out;
}
} // namespace

std::shared_ptr<database> lattice_db::borrow_read_connection() {
    std::shared_ptr<database> writer, reader;
    {
        std::lock_guard<std::mutex> lock(connection_ownership_mutex_);
        writer = db_;
        reader = read_db_;
    }
    // No SQLite call under the publication mutex. A retired fallback writer
    // stays owned through this test and the caller's entire query.
    if (reader && writer &&
        txn_owner_thread_.load(std::memory_order_acquire) == std::this_thread::get_id() &&
        writer->is_in_transaction()) {
        return writer;
    }
    if (reader) return reader;
    if (writer) return writer;
    throw db_error("read connection unavailable during maintenance");
}

std::shared_ptr<database> lattice_db::borrow_xproc_read_connection() {
    std::shared_ptr<database> writer, reader, xproc;
    {
        std::lock_guard<std::mutex> lock(connection_ownership_mutex_);
        writer = db_;
        reader = read_db_;
        xproc = xproc_read_db_;
    }
    if (xproc) return xproc;
    if (reader && writer &&
        txn_owner_thread_.load(std::memory_order_acquire) == std::this_thread::get_id() &&
        writer->is_in_transaction()) {
        return writer;
    }
    if (reader) return reader;
    if (writer) return writer;
    throw db_error("xproc read connection unavailable during maintenance");
}

std::vector<database::row_t> lattice_db::query_read(
    const std::string& sql, const std::vector<column_value_t>& params) {
    auto connection = borrow_read_connection();
    return connection->query(sql, params);
}

std::vector<database::row_t> lattice_db::query_read_checked(
    const std::string& sql, const std::vector<column_value_t>& params) {
    auto connection = borrow_read_connection();
    if (is_closed() || connection->is_closed())
        throw db_error("Query connection is closed");
    auto rows = connection->query(sql, params);
    if (is_closed() || connection->is_closed())
        throw db_error("Query connection is closed");
    return rows;
}

std::vector<database::row_t> lattice_db::query_xproc(
    const std::string& sql, const std::vector<column_value_t>& params) {
    auto connection = borrow_xproc_read_connection();
    return connection->query(sql, params);
}

void lattice_db::close_read_db() {
    if (config_.required_existing_schema_version != 0)
        throw db_error("Existing schema handles require a full close and fresh admission");
    std::shared_ptr<database> reader, xproc;
    {
        std::lock_guard<std::mutex> lock(connection_ownership_mutex_);
        ++connection_revision_;
        reader.swap(read_db_);
        xproc.swap(xproc_read_db_);
    }
    // In particular, do not wait for an xproc borrower: its next statement can
    // need the writer mutex already held by the maintenance caller.
}

void lattice_db::close_write_db() {
    if (config_.required_existing_schema_version != 0)
        throw db_error("Existing schema handles require a full close and fresh admission");
    std::shared_ptr<database> writer;
    {
        std::lock_guard<std::mutex> lock(connection_ownership_mutex_);
        ++connection_revision_;
        writer.swap(db_);
    }
    // Attachment operations own their handle snapshot. Retirement need not
    // take attach_mutex_ (a maintenance caller may already hold SQLite).
}

std::vector<std::shared_ptr<database>> lattice_db::view_handles() {
    std::vector<std::shared_ptr<database>> handles;
    {
        std::lock_guard<std::mutex> lock(connection_ownership_mutex_);
        if (db_) handles.push_back(db_);
        if (read_db_) handles.push_back(read_db_);
    }
    return handles;
}

void lattice_db::restore_attached_views(database& connection) {
    // Caller holds attach_mutex_; this connection is not published yet.
    for (const auto& entry : attached_dbs_) {
        attach_database(connection, entry.alias, entry.read_only_uri.value_or(entry.path),
                        entry.read_only_uri.has_value());
    }
    for (const auto& [name, sql] : attached_view_sql_) connection.execute(sql);
}

void lattice_db::reopen_write_db() {
    if (config_.required_existing_schema_version != 0)
        throw db_error("Existing schema handles require a full close and fresh admission");
    uint64_t revision;
    {
        std::lock_guard<std::mutex> lock(connection_ownership_mutex_);
        if (closed_.load(std::memory_order_seq_cst))
            throw db_error("cannot reopen a closed lattice");
        revision = connection_revision_;
    }
    // Declared before the attach guard: error cleanup and retired-owner
    // destruction must run after that guard is released.
    auto staged = std::make_shared<database>(config_.path,
        database::open_mode::read_write, config_.busy_timeout_ms,
        config_.private_sync_cancellation, config_.create_if_missing);
    register_sql_functions(*staged);
    // A maintenance caller can already own the writer SQLite mutex. Never
    // wait behind attach while it may be waiting for that writer. A busy
    // topology is an explicit failed reopen, with prior publication intact.
    std::unique_lock<std::mutex> attach_lock(attach_mutex_, std::try_to_lock);
    if (!attach_lock.owns_lock())
        throw db_error("connection reopen refused while attachment topology is busy");
    restore_attached_views(*staged);
    setup_change_hook(*staged);
    {
        std::lock_guard<std::mutex> lock(connection_ownership_mutex_);
        if (closed_.load(std::memory_order_seq_cst) || revision != connection_revision_)
            throw db_error("write reopen invalidated by concurrent maintenance");
        ++connection_revision_;
        staged.swap(db_);
    }
}

void lattice_db::reopen_read_db() {
    if (config_.required_existing_schema_version != 0)
        throw db_error("Existing schema handles require a full close and fresh admission");
    if (config_.is_in_memory() || config_.read_only) return;
    uint64_t revision;
    {
        std::lock_guard<std::mutex> lock(connection_ownership_mutex_);
        if (closed_.load(std::memory_order_seq_cst))
            throw db_error("cannot reopen a closed lattice");
        revision = connection_revision_;
    }
    // No half-pair publication if either open or topology restoration fails.
    auto reader = std::make_shared<database>(config_.path,
        database::open_mode::read_only, config_.busy_timeout_ms, config_.private_sync_cancellation);
    auto xproc = std::make_shared<database>(config_.path,
        database::open_mode::read_only, config_.busy_timeout_ms, config_.private_sync_cancellation);
    // A maintenance caller can already own the writer SQLite mutex. Never
    // wait behind attach while it may be waiting for that writer. A busy
    // topology is an explicit failed reopen, with prior publication intact.
    std::unique_lock<std::mutex> attach_lock(attach_mutex_, std::try_to_lock);
    if (!attach_lock.owns_lock())
        throw db_error("connection reopen refused while attachment topology is busy");
    restore_attached_views(*reader);
    {
        std::lock_guard<std::mutex> lock(connection_ownership_mutex_);
        if (closed_.load(std::memory_order_seq_cst) || revision != connection_revision_)
            throw db_error("read reopen invalidated by concurrent maintenance");
        ++connection_revision_;
        reader.swap(read_db_);
        xproc.swap(xproc_read_db_);
    }
}

void lattice_db::attach(lattice_db &lattice) {
    if (config_.required_existing_schema_version != 0 || lattice.config_.required_existing_schema_version != 0)
        throw db_error("Existing schema handles cannot participate in attachments");
    // Serialize attach/detach: the already-attached consult below is
    // check-then-act, and view regeneration must not interleave with a
    // concurrent attach/detach on another thread.
    std::vector<std::shared_ptr<database>> handles;
    std::shared_ptr<database> other;
    std::lock_guard<std::mutex> attach_lock(attach_mutex_);
    {
        std::lock_guard<std::mutex> lock(lattice.connection_ownership_mutex_);
        other = lattice.db_;
    }
    if (!other) {
        throw std::runtime_error("attach: the database being attached is closed");
    }

    const std::string alias = attach_alias_for(lattice.config_.path);

    // Idempotence: same alias + same path is a no-op; same alias for a
    // different path is a caller error.
    for (const auto& entry : attached_dbs_) {
        if (entry.alias == alias) {
            if (entry.path == lattice.config_.path) {
                if (entry.read_only_uri.has_value() != lattice.config_.read_only)
                    throw std::runtime_error("attach: alias '" + alias + "' already has a different access mode");
                return;
            }
            throw std::runtime_error(
                "attach: alias '" + alias + "' is already attached to a different database (" +
                entry.path + ")");
        }
    }

    // Validate schema overlap BEFORE any side effect, reading the other
    // lattice's schema through ITS OWN connection — a mismatch throws with
    // this lattice untouched (no dangling ATTACH, no half-created views).
    {
        handles = view_handles();
        if (handles.empty()) {
            throw std::runtime_error("attach: this database is closed");
        }
        auto main_set = model_tables(handles.front().get(), "main.sqlite_master");
        auto other_set = model_tables(other.get(), "main.sqlite_master");
        for (const auto& table_name : other_set) {
            if (!main_set.count(table_name)) continue;
            auto main_cols = get_column_names(handles.front().get(), "main", table_name);
            auto other_cols = get_column_names(other.get(), "main", table_name);
            // A read-only older schema can omit additive nullable/defaulted
            // columns. Its identity and required values must still be present;
            // a writable source always requires the complete current shape.
            bool compatible_legacy = lattice.config_.read_only &&
                std::includes(main_cols.begin(), main_cols.end(),
                              other_cols.begin(), other_cols.end());
            if (compatible_legacy && main_cols != other_cols) {
                std::unordered_set<std::string> key_columns{"globalId", "primaryKey"};
                for (const auto& index : handles.front()->query(
                         "PRAGMA main.index_list(" + table_name + ")")) {
                    if (std::get<int64_t>(index.at("unique")) == 0) continue;
                    const auto& index_name = std::get<std::string>(index.at("name"));
                    for (const auto& column : handles.front()->query(
                             "PRAGMA main.index_info(" + history_quote(index_name, '"') + ")")) {
                        const auto& name = column.at("name");
                        // An expression key cannot be proven independent of
                        // the absent columns from column metadata alone.
                        if (!std::holds_alternative<std::string>(name)) {
                            compatible_legacy = false;
                            break;
                        }
                        key_columns.insert(std::get<std::string>(name));
                    }
                    if (!compatible_legacy) break;
                }
                for (const auto& column : handles.front()->query(
                         "PRAGMA main.table_info(" + table_name + ")")) {
                    const auto& name = std::get<std::string>(column.at("name"));
                    if (std::binary_search(other_cols.begin(), other_cols.end(), name)) continue;
                    const bool key = std::get<int64_t>(column.at("pk")) != 0 ||
                                     key_columns.count(name) != 0;
                    const bool required = std::get<int64_t>(column.at("notnull")) != 0;
                    const bool has_default =
                        !std::holds_alternative<std::nullptr_t>(column.at("dflt_value"));
                    if (key || (required && !has_default)) {
                        compatible_legacy = false;
                        break;
                    }
                }
            }
            if (main_cols != other_cols && !compatible_legacy) {
                LOG_ERROR("db", "Schema mismatch for table '%s' between main and attached DB '%s'",
                          table_name.c_str(), alias.c_str());
                throw std::runtime_error(
                    "Schema mismatch for table '" + table_name +
                    "' between main database and attached database '" + alias + "'");
            }
        }
    }

    // Keep identity separate from SQLite's file URI, and retain this policy
    // after the source object closes and receiver connections are reopened.
    const auto read_only_uri = lattice.config_.read_only
        ? std::optional<std::string>(read_only_attachment_uri(*other)) : std::nullopt;
    const auto& target = read_only_uri ? *read_only_uri : lattice.config_.path;
    std::vector<std::shared_ptr<database>> attached_handles;
    try {
        for (const auto& handle : handles) {
            if (attach_database(*handle, alias, target, read_only_uri.has_value(),
                                /*tolerate_existing=*/true))
                attached_handles.push_back(handle);
        }
    } catch (...) {
        for (const auto& handle : attached_handles) {
            try { handle->execute("DETACH DATABASE " + quoted_attach_alias(alias)); }
            catch (const db_error& error) {
                LOG_ERROR("db", "Failed to unwind attachment '%s': %s", alias.c_str(), error.what());
            }
        }
        throw;
    }

    attached_dbs_.push_back({alias, lattice.config_.path, read_only_uri});
    attached_aliases_.push_back(alias);
    rebuild_attached_views(handles);
}

void lattice_db::detach(lattice_db &lattice) {
    // Resolve by PATH, not by recomputed alias: alias derivation truncates at
    // the filename stem, so two different paths (or dotted memory names) can
    // share an alias — a never-attached path must be a clean no-op, never a
    // detach of a same-stem victim.
    std::string alias;
    {
        std::lock_guard<std::mutex> attach_lock(attach_mutex_);
        auto it = std::find_if(attached_dbs_.begin(), attached_dbs_.end(),
            [&](const auto& entry) { return entry.path == lattice.config_.path; });
        if (it == attached_dbs_.end()) return;  // not attached: idempotent no-op
        alias = it->alias;
    }
    detach_alias(alias);
}

void lattice_db::detach_alias(const std::string& alias) {
    std::vector<std::shared_ptr<database>> handles;
    std::lock_guard<std::mutex> attach_lock(attach_mutex_);
    handles = view_handles();

    auto it = std::find_if(attached_dbs_.begin(), attached_dbs_.end(),
        [&](const auto& entry) { return entry.alias == alias; });
    if (it == attached_dbs_.end()) return;  // idempotent no-op

    // Views reference the alias — drop them all first (regeneration for the
    // remaining aliases happens after the DETACH).
    for (const auto& handle : handles) {
        for (const auto& view : attached_view_names_) {
            handle->execute("DROP VIEW IF EXISTS \"" + view + "\"");
        }
    }
    attached_view_names_.clear();
    attached_view_sql_.clear();

    // DETACH with a bounded retry: there is no prepared-statement cache —
    // every query prepares/finalizes in-call — so the only blocker is an
    // in-flight statement on another thread transiently locking the schema.
    for (const auto& handle : handles) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        for (;;) {
            try {
                handle->execute("DETACH DATABASE " + quoted_attach_alias(alias));
                break;
            } catch (const db_error& e) {
                const std::string msg = e.what();
                const bool transient = msg.find("locked") != std::string::npos ||
                                       msg.find("busy") != std::string::npos;
                if (!transient || std::chrono::steady_clock::now() >= deadline) throw;
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    }

    attached_dbs_.erase(it);
    attached_aliases_.erase(
        std::remove(attached_aliases_.begin(), attached_aliases_.end(), alias),
        attached_aliases_.end());
    rebuild_attached_views(handles);
}

// Caller holds attach_mutex_. Drops every view attach created, then
// regenerates from the CURRENT alias set — this makes multi-alias
// attach/detach order-independent (the old CREATE ... IF NOT EXISTS meant a
// second alias sharing a table name silently never joined the union view)
// and restores main-table visibility after the last detach.
void lattice_db::rebuild_attached_views(const std::vector<std::shared_ptr<database>>& handles) {
    for (const auto& handle : handles) {
        for (const auto& view : attached_view_names_) {
            handle->execute("DROP VIEW IF EXISTS \"" + view + "\"");
        }
    }
    attached_view_names_.clear();
    attached_view_sql_.clear();
    if (attached_dbs_.empty()) return;

    for (const auto& handle : handles) {
        auto main_set = model_tables(handle.get(), "main.sqlite_master");

        // table → arms. An arm is (schema-qualifier, _source label).
        std::map<std::string, std::vector<std::pair<std::string, std::string>>> arms;
        for (const auto& entry : attached_dbs_) {
            const std::string quoted = quoted_attach_alias(entry.alias);
            char master[600];
            snprintf(master, sizeof(master), "%s.sqlite_master", quoted.c_str());
            for (const auto& table_name : model_tables(handle.get(), master)) {
                arms[table_name].emplace_back(quoted, quoted);
            }
        }

        for (const auto& [table_name, alias_arms] : arms) {
            const bool in_main = main_set.count(table_name) > 0;
            std::string sql;
            if (!in_main && alias_arms.size() == 1) {
                // Attached-only table with a single source: plain passthrough
                // (matches the historical view shape — no _source column).
                sql = "CREATE TEMP VIEW IF NOT EXISTS " + table_name +
                      " AS SELECT * FROM " + alias_arms.front().first + "." + table_name;
            } else {
                // UNION ALL matches columns BY POSITION, and each database's
                // physical column order is whatever its create-time schema
                // iteration produced (an unordered map — effectively random
                // per file). `SELECT *` arms therefore SCRAMBLE values across
                // same-named columns whenever two files disagree — observed
                // live: a WHERE over the view read an attached row's
                // modifiedAt as deletedAt and filtered every spoke row out
                // of recall. Project every arm's columns explicitly, in one
                // canonical order, so the view maps by NAME. For a validated
                // legacy source overlapping main, fill only absent columns
                // with the main schema's default or NULL. Present NULLs stay
                // NULL, and no source table is altered.
                const std::string first_schema =
                    in_main ? "main" : alias_arms.front().first;
                const auto canonical_columns = handle->query(
                    "PRAGMA " + first_schema + ".table_info(" + table_name + ")");
                auto projection_for = [&](const std::string& arm_schema) {
                    const auto present = get_column_names(handle.get(), arm_schema, table_name);
                    std::string cols;
                    for (const auto& column : canonical_columns) {
                        const auto& name = std::get<std::string>(column.at("name"));
                        if (!cols.empty()) cols += ", ";
                        std::string quoted_name = "\"";
                        for (char c : name) {
                            quoted_name += c;
                            if (c == '"') quoted_name += '"';
                        }
                        quoted_name += "\"";
                        // Attached-only tables have not passed main-overlap
                        // validation. Keep their existing projection contract.
                        if (!in_main || std::binary_search(present.begin(), present.end(), name)) {
                            cols += quoted_name;
                        } else {
                            const auto& value = column.at("dflt_value");
                            cols += std::holds_alternative<std::string>(value)
                                ? "(" + std::get<std::string>(value) + ")" : "NULL";
                            cols += " AS " + quoted_name;
                        }
                    }
                    return cols;
                };
                sql = "CREATE TEMP VIEW IF NOT EXISTS " + table_name + " AS ";
                bool first = true;
                if (in_main) {
                    sql += "SELECT " + projection_for("main") + ", 'main' AS _source FROM main." + table_name;
                    first = false;
                }
                for (const auto& [qualifier, label] : alias_arms) {
                    if (!first) sql += " UNION ALL ";
                    // The label is caller-controlled (derived from the attached
                    // path) — escape single quotes for the SQL string literal.
                    std::string escaped_label;
                    escaped_label.reserve(label.size());
                    for (char c : label) {
                        escaped_label += c;
                        if (c == '\'') escaped_label += '\'';
                    }
                    sql += "SELECT " + projection_for(qualifier) + ", '" + escaped_label + "' AS _source FROM " + qualifier + "." + table_name;
                    first = false;
                }
            }
            handle->execute(sql);
            attached_view_names_.insert(table_name);
            attached_view_sql_[table_name] = sql;
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
