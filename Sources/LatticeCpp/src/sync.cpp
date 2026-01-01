#include "lattice/sync.hpp"
#include "lattice/lattice.hpp"
#include <nlohmann/json.hpp>
#include <sstream>
#include <cmath>
#include <iomanip>

namespace lattice {

using json = nlohmann::json;

// ============================================================================
// any_property implementation
// ============================================================================

column_value_t any_property::to_column_value() const {
    switch (kind) {
        case any_property_kind::null_kind:
            return nullptr;
        case any_property_kind::int_kind:
        case any_property_kind::int64_kind:
            if (std::holds_alternative<int64_t>(value)) {
                return std::get<int64_t>(value);
            }
            return nullptr;
        case any_property_kind::float_kind:
        case any_property_kind::double_kind:
        case any_property_kind::date_kind:
            if (std::holds_alternative<double>(value)) {
                return std::get<double>(value);
            }
            return nullptr;
        case any_property_kind::string_kind:
            if (std::holds_alternative<std::string>(value)) {
                return std::get<std::string>(value);
            }
            return nullptr;
        case any_property_kind::data_kind:
            if (std::holds_alternative<std::vector<uint8_t>>(value)) {
                return std::get<std::vector<uint8_t>>(value);
            }
            return nullptr;
    }
    return nullptr;
}

any_property any_property::from_column_value(const column_value_t& v) {
    if (std::holds_alternative<std::nullptr_t>(v)) {
        return any_property(nullptr);
    } else if (std::holds_alternative<int64_t>(v)) {
        return any_property(std::get<int64_t>(v));
    } else if (std::holds_alternative<double>(v)) {
        return any_property(std::get<double>(v));
    } else if (std::holds_alternative<std::string>(v)) {
        return any_property(std::get<std::string>(v));
    } else if (std::holds_alternative<std::vector<uint8_t>>(v)) {
        return any_property(std::get<std::vector<uint8_t>>(v));
    }
    // Note: bool is not a variant type in column_value_t - booleans are stored as int64_t
    return any_property(nullptr);
}

// ============================================================================
// JSON serialization helpers for any_property
// ============================================================================

static json any_property_to_json(const any_property& prop) {
    json j;
    j["kind"] = static_cast<int>(prop.kind);

    switch (prop.kind) {
        case any_property_kind::null_kind:
            j["value"] = nullptr;
            break;
        case any_property_kind::int_kind:
        case any_property_kind::int64_kind:
            if (std::holds_alternative<int64_t>(prop.value)) {
                j["value"] = std::get<int64_t>(prop.value);
            } else {
                j["value"] = nullptr;
            }
            break;
        case any_property_kind::float_kind:
        case any_property_kind::double_kind:
        case any_property_kind::date_kind:
            if (std::holds_alternative<double>(prop.value)) {
                j["value"] = std::get<double>(prop.value);
            } else {
                j["value"] = nullptr;
            }
            break;
        case any_property_kind::string_kind:
            if (std::holds_alternative<std::string>(prop.value)) {
                j["value"] = std::get<std::string>(prop.value);
            } else {
                j["value"] = nullptr;
            }
            break;
        case any_property_kind::data_kind:
            if (std::holds_alternative<std::vector<uint8_t>>(prop.value)) {
                // Encode as hex string
                const auto& data = std::get<std::vector<uint8_t>>(prop.value);
                std::ostringstream hex;
                for (auto byte : data) {
                    hex << std::setfill('0') << std::setw(2) << std::hex << static_cast<int>(byte);
                }
                j["value"] = hex.str();
            } else {
                j["value"] = nullptr;
            }
            break;
    }
    return j;
}

static any_property json_to_any_property(const json& j) {
    // Handle AnyProperty format: {"kind": int, "value": ...}
    if (j.is_object() && j.contains("kind")) {
        int kind_int = j["kind"].get<int>();
        auto kind = static_cast<any_property_kind>(kind_int);

        if (kind == any_property_kind::null_kind || !j.contains("value") || j["value"].is_null()) {
            return any_property(nullptr);
        }

        const auto& v = j["value"];
        switch (kind) {
            case any_property_kind::int_kind:
                return any_property(static_cast<int>(v.get<int>()));
            case any_property_kind::int64_kind:
                return any_property(v.get<int64_t>());
            case any_property_kind::string_kind:
                return any_property(v.get<std::string>());
            case any_property_kind::date_kind:
                return any_property::date(v.get<double>());
            case any_property_kind::float_kind: {
                any_property p;
                p.kind = any_property_kind::float_kind;
                p.value = static_cast<double>(v.get<float>());
                return p;
            }
            case any_property_kind::double_kind:
                return any_property(v.get<double>());
            case any_property_kind::data_kind:
                // Data is hex encoded string - decode it
                if (v.is_string()) {
                    std::string hex_str = v.get<std::string>();
                    std::vector<uint8_t> data;
                    data.reserve(hex_str.size() / 2);
                    for (size_t i = 0; i + 1 < hex_str.size(); i += 2) {
                        uint8_t byte = static_cast<uint8_t>(
                            std::stoi(hex_str.substr(i, 2), nullptr, 16));
                        data.push_back(byte);
                    }
                    return any_property(std::move(data));
                }
                return any_property(nullptr);
            case any_property_kind::null_kind:
                return any_property(nullptr);
        }
    }

    // Fallback: raw value (backwards compatibility)
    if (j.is_null()) {
        return any_property(nullptr);
    } else if (j.is_string()) {
        return any_property(j.get<std::string>());
    } else if (j.is_number_integer()) {
        return any_property(j.get<int64_t>());
    } else if (j.is_number_float()) {
        return any_property(j.get<double>());
    } else if (j.is_boolean()) {
        return any_property(j.get<bool>() ? 1 : 0);
    }
    return any_property(nullptr);
}

// ============================================================================
// audit_log_entry implementation
// ============================================================================

std::string audit_log_entry::changed_fields_to_json() const {
    json j = json::object();
    for (const auto& [name, prop] : changed_fields) {
        j[name] = any_property_to_json(prop);
    }
    return j.dump();
}

std::string audit_log_entry::changed_fields_names_to_json() const {
    json j = json::array();
    for (const auto& name : changed_fields_names) {
        j.push_back(name);
    }
    return j.dump();
}

changed_fields_map audit_log_entry::parse_changed_fields(const std::string& json_str) {
    changed_fields_map result;
    if (json_str.empty()) return result;

    try {
        json j = json::parse(json_str);
        if (j.is_object()) {
            for (auto& [key, value] : j.items()) {
                result[key] = json_to_any_property(value);
            }
        }
    } catch (...) {
        // Parse failed, return empty map
    }
    return result;
}

std::vector<std::string> audit_log_entry::parse_changed_fields_names(const std::string& json_str) {
    std::vector<std::string> result;
    if (json_str.empty()) return result;

    try {
        json j = json::parse(json_str);
        if (j.is_array()) {
            for (const auto& item : j) {
                if (item.is_string()) {
                    result.push_back(item.get<std::string>());
                }
            }
        }
    } catch (...) {
        // Parse failed, return empty vector
    }
    return result;
}

std::pair<std::string, std::vector<column_value_t>> audit_log_entry::generate_instruction(
        const std::unordered_map<std::string, column_type>& schema) const {
    std::vector<column_value_t> params;

    // Helper to check if a column is a BLOB type
    auto is_blob = [&schema](const std::string& col) {
        auto it = schema.find(col);
        return it != schema.end() && it->second == column_type::blob;
    };

    // Helper to get placeholder - use unhex(?) for BLOB columns
    auto placeholder = [&is_blob](const std::string& col) -> std::string {
        return is_blob(col) ? "unhex(?)" : "?";
    };

    if (operation == "INSERT") {
        // For INSERT, we need: INSERT INTO table(globalId, col1, col2...) VALUES (?, ?, ...)
        // ON CONFLICT(globalId) DO UPDATE SET col1=excluded.col1, ...

        std::vector<std::string> cols;
        cols.push_back("globalId");

        // Use changed_fields_names directly
        for (const auto& name : changed_fields_names) {
            cols.push_back(name);
        }

        // Build column list and placeholders
        std::string col_names = cols[0];
        std::string placeholders = "?";  // globalId is never a blob
        for (size_t i = 1; i < cols.size(); ++i) {
            col_names += ", " + cols[i];
            placeholders += ", " + placeholder(cols[i]);
        }

        // Build UPDATE SET clause
        std::string update_set;
        for (size_t i = 1; i < cols.size(); ++i) {
            if (i > 1) update_set += ", ";
            update_set += cols[i] + "=excluded." + cols[i];
        }

        std::string sql = "INSERT INTO " + table_name + "(" + col_names + ") VALUES (" + placeholders + ")";
        if (!update_set.empty()) {
            sql += " ON CONFLICT(globalId) DO UPDATE SET " + update_set;
        }

        // Add globalRowId as first param
        params.push_back(global_row_id);

        // Extract values from changed_fields map
        for (size_t i = 1; i < cols.size(); ++i) {
            const auto& col = cols[i];
            auto it = changed_fields.find(col);
            if (it != changed_fields.end()) {
                params.push_back(it->second.to_column_value());
            } else {
                params.push_back(nullptr);
            }
        }

        return {sql, params};

    } else if (operation == "UPDATE") {
        // UPDATE table SET col1=?, col2=? WHERE globalId=?

        if (changed_fields_names.empty()) {
            return {"", {}};  // Nothing to update
        }

        std::string set_clause = changed_fields_names[0] + " = " + placeholder(changed_fields_names[0]);
        for (size_t i = 1; i < changed_fields_names.size(); ++i) {
            set_clause += ", " + changed_fields_names[i] + " = " + placeholder(changed_fields_names[i]);
        }

        std::string sql = "UPDATE " + table_name + " SET " + set_clause + " WHERE globalId = ?";

        // Extract values from changed_fields map
        for (const auto& col : changed_fields_names) {
            auto it = changed_fields.find(col);
            if (it != changed_fields.end()) {
                params.push_back(it->second.to_column_value());
            } else {
                params.push_back(nullptr);
            }
        }
        params.push_back(global_row_id);

        return {sql, params};

    } else if (operation == "DELETE") {
        std::string sql = "DELETE FROM " + table_name + " WHERE globalId = ?";
        params.push_back(global_row_id);
        return {sql, params};
    }

    return {"", {}};
}

std::string audit_log_entry::to_json() const {
    json j;
    j["id"] = id;
    j["globalId"] = global_id;
    j["tableName"] = table_name;
    j["operation"] = operation;
    j["rowId"] = row_id;
    j["globalRowId"] = global_row_id;

    // Serialize changed_fields as object with AnyProperty format
    json fields_obj = json::object();
    for (const auto& [name, prop] : changed_fields) {
        fields_obj[name] = any_property_to_json(prop);
    }
    j["changedFields"] = fields_obj;

    // Serialize changed_fields_names as array
    j["changedFieldsNames"] = changed_fields_names;

    j["timestamp"] = timestamp;
    j["isFromRemote"] = is_from_remote;
    j["isSynchronized"] = is_synchronized;

    return j.dump();
}

std::optional<audit_log_entry> audit_log_entry::from_json(const std::string& json_str) {
    try {
        json j = json::parse(json_str);
        audit_log_entry entry;

        if (j.contains("id") && j["id"].is_number()) {
            entry.id = j["id"].get<int64_t>();
        }
        if (j.contains("globalId") && j["globalId"].is_string()) {
            entry.global_id = j["globalId"].get<std::string>();
        }
        if (j.contains("tableName") && j["tableName"].is_string()) {
            entry.table_name = j["tableName"].get<std::string>();
        }
        if (j.contains("operation") && j["operation"].is_string()) {
            entry.operation = j["operation"].get<std::string>();
        }
        if (j.contains("rowId") && j["rowId"].is_number()) {
            entry.row_id = j["rowId"].get<int64_t>();
        }
        if (j.contains("globalRowId") && j["globalRowId"].is_string()) {
            entry.global_row_id = j["globalRowId"].get<std::string>();
        }

        // Parse changedFields - can be object or JSON string
        if (j.contains("changedFields")) {
            if (j["changedFields"].is_object()) {
                for (auto& [key, value] : j["changedFields"].items()) {
                    entry.changed_fields[key] = json_to_any_property(value);
                }
            } else if (j["changedFields"].is_string()) {
                entry.changed_fields = parse_changed_fields(j["changedFields"].get<std::string>());
            }
        }

        // Parse changedFieldsNames - can be array or JSON string
        if (j.contains("changedFieldsNames")) {
            if (j["changedFieldsNames"].is_array()) {
                for (const auto& item : j["changedFieldsNames"]) {
                    if (item.is_string()) {
                        entry.changed_fields_names.push_back(item.get<std::string>());
                    }
                }
            } else if (j["changedFieldsNames"].is_string()) {
                entry.changed_fields_names = parse_changed_fields_names(j["changedFieldsNames"].get<std::string>());
            }
        }

        if (j.contains("timestamp") && j["timestamp"].is_string()) {
            entry.timestamp = j["timestamp"].get<std::string>();
        }
        if (j.contains("isFromRemote") && j["isFromRemote"].is_boolean()) {
            entry.is_from_remote = j["isFromRemote"].get<bool>();
        }
        if (j.contains("isSynchronized") && j["isSynchronized"].is_boolean()) {
            entry.is_synchronized = j["isSynchronized"].get<bool>();
        }

        return entry;
    } catch (...) {
        return std::nullopt;
    }
}

// ============================================================================
// server_sent_event implementation
// ============================================================================

std::string server_sent_event::to_json() const {
    json j;

    if (event_type == type::audit_log) {
        j["kind"] = "auditLog";
        json audit_array = json::array();
        for (const auto& entry : audit_logs) {
            // Parse the entry's JSON to include as nested object
            audit_array.push_back(json::parse(entry.to_json()));
        }
        j["auditLog"] = audit_array;
    } else {
        j["kind"] = "ack";
        j["ack"] = acked_ids;
    }

    return j.dump();
}

std::optional<server_sent_event> server_sent_event::from_json(const std::string& json_str) {
    try {
        json j = json::parse(json_str);

        if (j.contains("auditLog") && j["auditLog"].is_array()) {
            server_sent_event event;
            event.event_type = type::audit_log;

            for (const auto& entry_json : j["auditLog"]) {
                auto entry = audit_log_entry::from_json(entry_json.dump());
                if (entry) {
                    event.audit_logs.push_back(*entry);
                }
            }
            return event;

        } else if (j.contains("ack") && j["ack"].is_array()) {
            server_sent_event event;
            event.event_type = type::ack;

            for (const auto& id : j["ack"]) {
                if (id.is_string()) {
                    event.acked_ids.push_back(id.get<std::string>());
                }
            }
            return event;
        }
    } catch (...) {
        return std::nullopt;
    }

    return std::nullopt;
}

// ============================================================================
// synchronizer implementation
// ============================================================================

synchronizer::synchronizer(lattice_db& db, const sync_config& config,
                           std::shared_ptr<scheduler> sched)
    : db_(db)
    , config_(config)
    , scheduler_(sched ? sched : std::make_shared<immediate_scheduler>())
{
    auto factory = get_network_factory();
    ws_client_ = factory->create_websocket_client();

    // Set up WebSocket handlers
    ws_client_->set_on_open([this] { on_websocket_open(); });
    ws_client_->set_on_message([this](const websocket_message& msg) { on_websocket_message(msg); });
    ws_client_->set_on_error([this](const std::string& err) { on_websocket_error(err); });
    ws_client_->set_on_close([this](int code, const std::string& reason) { on_websocket_close(code, reason); });

    // Observe AuditLog for new local changes to trigger upload
    audit_log_observer_id_ = db_.add_table_observer("AuditLog",
        [this](const std::string& operation, int64_t row_id, const std::string& global_id) {
            printf("[synchronizer] AuditLog observer: op=%s row_id=%lld is_connected=%d\n",
                   operation.c_str(), (long long)row_id, is_connected_ ? 1 : 0);
            // Only upload on INSERT of local (non-remote) entries
            if (operation == "INSERT" && is_connected_) {
                // Query to check if this is a local entry (not from remote sync)
                auto rows = db_.read_db().query(
                    "SELECT isFromRemote, isSynchronized FROM AuditLog WHERE id = ?",
                    {row_id}
                );
                printf("[synchronizer] AuditLog query returned %zu rows\n", rows.size());
                if (!rows.empty()) {
                    auto from_remote_it = rows[0].find("isFromRemote");
                    auto synced_it = rows[0].find("isSynchronized");
                    bool is_from_remote = from_remote_it != rows[0].end() &&
                        std::holds_alternative<int64_t>(from_remote_it->second) &&
                        std::get<int64_t>(from_remote_it->second) != 0;
                    bool is_synced = synced_it != rows[0].end() &&
                        std::holds_alternative<int64_t>(synced_it->second) &&
                        std::get<int64_t>(synced_it->second) != 0;

                    printf("[synchronizer] AuditLog entry: isFromRemote=%d isSynchronized=%d\n",
                           is_from_remote ? 1 : 0, is_synced ? 1 : 0);

                    // Only upload if this is a new local entry
                    if (!is_from_remote && !is_synced) {
                        printf("[synchronizer] Triggering upload for local entry\n");
                        upload_pending_changes();
                    }
                }
            }
        });
}

synchronizer::~synchronizer() {
    // Remove AuditLog observer
    if (audit_log_observer_id_ != 0) {
        db_.remove_table_observer("AuditLog", audit_log_observer_id_);
    }
    disconnect();
}

void synchronizer::connect() {
    if (config_.websocket_url.empty()) return;

    should_reconnect_ = true;  // Enable auto-reconnect
    std::string url = config_.websocket_url;

    // Add last-event-id query param if we have a checkpoint
    auto last_event = get_last_received_event_id();
    if (last_event) {
        if (url.find('?') != std::string::npos) {
            url += "&last-event-id=" + *last_event;
        } else {
            url += "?last-event-id=" + *last_event;
        }
    }

    std::map<std::string, std::string> headers;
    if (!config_.authorization_token.empty()) {
        headers["Authorization"] = "Bearer " + config_.authorization_token;
    }

    ws_client_->connect(url, headers);
}

void synchronizer::disconnect() {
    should_reconnect_ = false;  // Prevent auto-reconnect
    is_connected_ = false;
    reconnect_attempts_ = 0;
    ws_client_->disconnect();
}

void synchronizer::sync_now() {
    upload_pending_changes();
}

void synchronizer::on_websocket_open() {
    printf("[synchronizer] on_websocket_open called\n");
    is_connected_ = true;
    reconnect_attempts_ = 0;

    if (on_state_change_) {
        scheduler_->invoke([this] { on_state_change_(true); });
    }

    // Upload any pending changes
    printf("[synchronizer] Calling upload_pending_changes...\n");
    upload_pending_changes();
}

void synchronizer::on_websocket_message(const websocket_message& msg) {
    std::string json_str = msg.as_string();

    auto event = server_sent_event::from_json(json_str);
    if (!event) {
        if (on_error_) {
            scheduler_->invoke([this] { on_error_("Failed to parse server message"); });
        }
        return;
    }

    if (event->event_type == server_sent_event::type::audit_log) {
        // Apply remote changes
        apply_remote_changes(event->audit_logs);

        // Send acknowledgment
        std::vector<std::string> ids;
        for (const auto& entry : event->audit_logs) {
            ids.push_back(entry.global_id);
        }
        auto ack = server_sent_event::make_ack(ids);
        auto json = ack.to_json();
        ws_client_->send(websocket_message::from_binary({json.begin(), json.end()}));

    } else if (event->event_type == server_sent_event::type::ack) {
        // Mark local entries as synchronized
        mark_as_synced(event->acked_ids);

        if (on_sync_complete_) {
            std::vector<std::string> ids = event->acked_ids;
            scheduler_->invoke([this, ids] { on_sync_complete_(ids); });
        }
    }
}

void synchronizer::on_websocket_error(const std::string& error) {
    if (on_error_) {
        scheduler_->invoke([this, error] { on_error_(error); });
    }

    // Attempt reconnection
    schedule_reconnect();
}

void synchronizer::on_websocket_close(int code, const std::string& reason) {
    is_connected_ = false;

    if (on_state_change_) {
        scheduler_->invoke([this] { on_state_change_(false); });
    }

    // Attempt reconnection
    schedule_reconnect();
}

void synchronizer::upload_pending_changes() {
    printf("[synchronizer] upload_pending_changes called\n");
    // Query unsynced audit log entries
    auto entries = query_audit_log(db_.db(), true);

    printf("[synchronizer] Found %zu unsynced entries\n", entries.size());
    if (entries.empty()) return;

    // Chunk into batches
    for (size_t i = 0; i < entries.size(); i += config_.chunk_size) {
        size_t end = std::min(i + config_.chunk_size, entries.size());
        std::vector<audit_log_entry> chunk(entries.begin() + i, entries.begin() + end);

        auto event = server_sent_event::make_audit_log(chunk);
        auto json = event.to_json();
        printf("[synchronizer] Sending %zu entries to server: %s\n", chunk.size(), json.substr(0, 200).c_str());
        ws_client_->send(websocket_message::from_binary({json.begin(), json.end()}));
    }
}

void synchronizer::apply_remote_changes(const std::vector<audit_log_entry>& entries) {
    // Disable sync triggers while applying remote changes
    db_.db().execute("UPDATE _SyncControl SET disabled = 1 WHERE id = 1");

    // Track which global IDs we actually inserted (for observer notification)
    std::vector<std::string> inserted_global_ids;

    try {
        db_.db().begin_transaction();

        for (const auto& entry : entries) {
            // Check if we already have this audit entry (prevent duplicates)
            auto existing = db_.db().query(
                "SELECT id FROM AuditLog WHERE globalId = ?",
                {entry.global_id}
            );
            if (!existing.empty()) continue;

            // If it's a link table (starts with _), ensure it exists
            if (!entry.table_name.empty() && entry.table_name[0] == '_') {
                db_.ensure_link_table(entry.table_name);
            }

            // Generate and execute the SQL instruction
            auto schema = db_.get_table_schema(entry.table_name);
            auto [sql, params] = entry.generate_instruction(schema);
            if (!sql.empty()) {
                try {
                    db_.db().execute(sql, params);
                } catch (const std::exception& e) {
                    // Log error but continue with other entries
                    if (on_error_) {
                        scheduler_->invoke([this, msg = std::string(e.what())] {
                            on_error_("Failed to apply change: " + msg);
                        });
                    }
                }
            }

            // Record the audit entry as from remote
            // Serialize changed_fields and changed_fields_names to JSON strings for DB storage
            std::string insert_sql = R"(
                INSERT INTO AuditLog (globalId, tableName, operation, rowId, globalRowId,
                    changedFields, changedFieldsNames, timestamp, isFromRemote, isSynchronized)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 1)
            )";
            db_.db().execute(insert_sql, {
                entry.global_id,
                entry.table_name,
                entry.operation,
                entry.row_id,
                entry.global_row_id,
                entry.changed_fields_to_json(),
                entry.changed_fields_names_to_json(),
                entry.timestamp
            });

            inserted_global_ids.push_back(entry.global_id);
        }

        db_.db().commit();
    } catch (...) {
        db_.db().rollback();
        throw;
    }

    // Re-enable sync triggers
    db_.db().execute("UPDATE _SyncControl SET disabled = 0 WHERE id = 1");

    // Note: AuditLog observer notifications are now handled by flush_changes()
    // which queries for AuditLog entries corresponding to each model table change
}

void synchronizer::mark_as_synced(const std::vector<std::string>& global_ids) {
    mark_audit_entries_synced(db_, global_ids);
}

void synchronizer::schedule_reconnect() {
    if (should_reconnect_ && !is_connected_ && reconnect_attempts_ < config_.max_reconnect_attempts) {
        double delay = std::pow(2.0, reconnect_attempts_) * config_.base_delay_seconds;
        ++reconnect_attempts_;

        // Schedule reconnection (simplified - in production would use proper async timer)
        scheduler_->invoke([this, delay] {
            std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(delay * 1000)));
            if (!is_connected_) {
                connect();
            }
        });
    }
}

std::optional<std::string> synchronizer::get_last_received_event_id() {
    auto rows = db_.db().query(
        "SELECT globalId FROM AuditLog WHERE isFromRemote = 1 ORDER BY id DESC LIMIT 1"
    );
    if (!rows.empty()) {
        auto it = rows[0].find("globalId");
        if (it != rows[0].end() && std::holds_alternative<std::string>(it->second)) {
            return std::get<std::string>(it->second);
        }
    }
    return std::nullopt;
}

// ============================================================================
// Helper functions
// ============================================================================

std::vector<audit_log_entry> query_audit_log(database& db,
    bool only_unsynced,
    std::optional<std::string> after_global_id)
{
    std::string sql = "SELECT * FROM AuditLog";
    std::vector<column_value_t> params;

    std::vector<std::string> conditions;
    if (only_unsynced) {
        conditions.push_back("isSynchronized = 0");
        conditions.push_back("isFromRemote = 0");
    }
    if (after_global_id) {
        conditions.push_back("id > (SELECT id FROM AuditLog WHERE globalId = ?)");
        params.push_back(*after_global_id);
    }

    if (!conditions.empty()) {
        sql += " WHERE " + conditions[0];
        for (size_t i = 1; i < conditions.size(); ++i) {
            sql += " AND " + conditions[i];
        }
    }
    sql += " ORDER BY id ASC";

    auto rows = db.query(sql, params);

    std::vector<audit_log_entry> entries;
    for (const auto& row : rows) {
        audit_log_entry entry;

        auto get_str = [&row](const std::string& key) -> std::string {
            auto it = row.find(key);
            if (it != row.end() && std::holds_alternative<std::string>(it->second)) {
                return std::get<std::string>(it->second);
            }
            return "";
        };
        auto get_int = [&row](const std::string& key) -> int64_t {
            auto it = row.find(key);
            if (it != row.end() && std::holds_alternative<int64_t>(it->second)) {
                return std::get<int64_t>(it->second);
            }
            return 0;
        };

        entry.id = get_int("id");
        entry.global_id = get_str("globalId");
        entry.table_name = get_str("tableName");
        entry.operation = get_str("operation");
        entry.row_id = get_int("rowId");
        entry.global_row_id = get_str("globalRowId");

        // Parse JSON strings into structured types
        entry.changed_fields = audit_log_entry::parse_changed_fields(get_str("changedFields"));
        entry.changed_fields_names = audit_log_entry::parse_changed_fields_names(get_str("changedFieldsNames"));

        entry.timestamp = get_str("timestamp");
        entry.is_from_remote = get_int("isFromRemote") != 0;
        entry.is_synchronized = get_int("isSynchronized") != 0;

        entries.push_back(entry);
    }

    return entries;
}

void mark_audit_entries_synced(lattice_db& db, const std::vector<std::string>& global_ids) {
    for (const auto& gid : global_ids) {
        // Get the row ID before updating
        auto rows = db.db().query(
            "SELECT id FROM AuditLog WHERE globalId = ?",
            {gid}
        );

        db.db().execute("UPDATE AuditLog SET isSynchronized = 1 WHERE globalId = ?", {gid});

        // Notify observers of the update (update hook skips AuditLog table)
        if (!rows.empty()) {
            auto it = rows[0].find("id");
            if (it != rows[0].end() && std::holds_alternative<int64_t>(it->second)) {
                int64_t row_id = std::get<int64_t>(it->second);
                db.notify_change("AuditLog", "UPDATE", row_id, gid);
            }
        }
    }
}

std::vector<audit_log_entry> events_after(database& db, const std::optional<std::string>& checkpoint_global_id) {
    if (checkpoint_global_id) {
        return query_audit_log(db, false, checkpoint_global_id);
    } else {
        return query_audit_log(db, false, std::nullopt);
    }
}

} // namespace lattice
