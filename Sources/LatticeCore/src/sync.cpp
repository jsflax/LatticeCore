#include "lattice/sync.hpp"
#include "lattice/lattice.hpp"
#include <nlohmann/json.hpp>
#include <sstream>
#include <cmath>
#include <iomanip>

namespace lattice {

using json = nlohmann::json;

static std::atomic<int64_t> g_sync_instance_count{0};

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
            // Hex-encoded BLOB from build_insert_entry_from_current_row —
            // return as string for use with unhex(?) placeholder.
            if (std::holds_alternative<std::string>(value)) {
                return std::get<std::string>(value);
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

synchronizer::synchronizer(std::unique_ptr<lattice_db> db, const sync_config& config)
    : db_(std::move(db))
    , config_(config)
    , scheduler_(db_->get_scheduler() ? db_->get_scheduler() : std::make_shared<immediate_scheduler>())
{
    auto n = g_sync_instance_count.fetch_add(1, std::memory_order_relaxed) + 1;
    LOG_INFO("synchronizer", "[%s] CREATED (WSS ctor, this=%p, db=%s, alive=%lld)",
             config_.sync_id.c_str(), (void*)this, db_->config().path.c_str(), (long long)n);
    auto factory = get_network_factory();
    ws_client_ = factory->create_sync_transport();
    setup_transport_handlers();
    setup_observer();
}

synchronizer::synchronizer(std::unique_ptr<lattice_db> db, const sync_config& config,
                           std::unique_ptr<sync_transport> transport)
    : db_(std::move(db))
    , config_(config)
    , scheduler_(db_->get_scheduler() ? db_->get_scheduler() : std::make_shared<immediate_scheduler>())
    , ws_client_(std::move(transport))
{
    auto n = g_sync_instance_count.fetch_add(1, std::memory_order_relaxed) + 1;
    LOG_INFO("synchronizer", "[%s] CREATED (IPC ctor, this=%p, db=%s, alive=%lld)",
             config_.sync_id.c_str(), (void*)this, db_->config().path.c_str(), (long long)n);
    setup_transport_handlers();
    setup_observer();
}

void synchronizer::setup_transport_handlers() {
    ws_client_->set_on_open([this] { on_websocket_open(); });
    ws_client_->set_on_message([this](const transport_message& msg) { on_transport_message(msg); });
    ws_client_->set_on_error([this](const std::string& err) { on_websocket_error(err); });
    ws_client_->set_on_close([this](int code, const std::string& reason) { on_websocket_close(code, reason); });
}

void synchronizer::setup_observer() {
    // Observe AuditLog for new local changes to trigger upload.
    // IMPORTANT: Dispatch through scheduler to avoid concurrent upload_pending_changes()
    // calls from observer threads competing for SQLite locks with an already-running upload.
    LOG_DEBUG("synchronizer", "Registering AuditLog observer on instance %p", (void*)db_.get());
    audit_log_observer_id_ = db_->add_table_observer("AuditLog",
        [this](const std::string& operation, int64_t row_id, const std::string& global_id) {
            LOG_DEBUG("synchronizer", "AuditLog observer: op=%s row_id=%lld is_connected=%d",
                   operation.c_str(), (long long)row_id, is_connected_ ? 1 : 0);
            // Only upload on INSERT when connected
            if (operation == "INSERT" && is_connected_) {
                // Check if this entry is pending for OUR sync_id.
                // This includes entries from other transports (relay).
                auto rows = db_->read_db().query(
                    "SELECT ss.is_synchronized FROM _lattice_sync_state ss "
                    "WHERE ss.audit_entry_id = ? AND ss.sync_id = ?",
                    {row_id, config_.sync_id}
                );
                // No row = pending for us; row with is_synchronized=0 = also pending
                bool is_synced_for_us = !rows.empty() &&
                    std::holds_alternative<int64_t>(rows[0].begin()->second) &&
                    std::get<int64_t>(rows[0].begin()->second) != 0;

                if (!is_synced_for_us) {
                    LOG_DEBUG("synchronizer", "Requesting upload for entry pending on sync_id=%s",
                              config_.sync_id.c_str());
                    // Coalesce: flag the request, schedule one upload.
                    // The single-threaded scheduler naturally batches — by the time
                    // the lambda runs, all rapid inserts have committed.
                    upload_requested_.store(true, std::memory_order_release);
                    scheduler_->invoke([this] {
                        if (is_destroyed_) return;
                        if (upload_requested_.exchange(false, std::memory_order_acq_rel)) {
                            upload_pending_changes();
                        }
                    });
                }
            }
        });
}

synchronizer::~synchronizer() {
    LOG_INFO("synchronizer", "[%s] ~synchronizer START (this=%p, db=%s)",
             config_.sync_id.c_str(), (void*)this,
             db_ ? db_->config().path.c_str() : "null");
    // Mark as destroyed so scheduled lambdas bail out.
    is_destroyed_ = true;
    // Remove AuditLog observer
    if (audit_log_observer_id_ != 0) {
        db_->remove_table_observer("AuditLog", audit_log_observer_id_);
    }
    disconnect();

    // Drain the scheduler: wait for any in-flight work to complete before
    // implicit member destruction invalidates the state that work accesses.
    // Without this, a lambda mid-way through upload_pending_changes() can
    // access db_, config_, ws_client_ etc. after they're destroyed.
    // Skip if we're on the scheduler thread (destructor called from within
    // a callback) — the work will finish as part of the current call stack.
    if (scheduler_ && !scheduler_->is_on_thread()) {
        LOG_INFO("synchronizer", "[%s] ~synchronizer: draining scheduler...", config_.sync_id.c_str());
        scheduler_->shutdown();
    }
    auto n = g_sync_instance_count.fetch_sub(1, std::memory_order_relaxed) - 1;
    LOG_INFO("synchronizer", "[%s] ~synchronizer END (this=%p, alive=%lld)", config_.sync_id.c_str(), (void*)this, (long long)n);
}

void synchronizer::connect() {
    LOG_INFO("synchronizer", "[%s] connect() (this=%p, db=%s)",
             config_.sync_id.c_str(), (void*)this, db_->config().path.c_str());
    if (config_.websocket_url.empty()) {
        // IPC or injected transport — connect without URL/headers.
        // No auto-reconnect: IPC reconnection is handled at the endpoint
        // level (server re-accepts, client retries via endpoint).
        should_reconnect_ = false;
        ws_client_->connect("", {});
        return;
    }

    should_reconnect_ = true;  // Enable auto-reconnect for WSS

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
    LOG_INFO("synchronizer", "[%s] disconnect() (this=%p, is_connected=%d, db=%s)",
             config_.sync_id.c_str(), (void*)this, is_connected_ ? 1 : 0,
             db_->config().path.c_str());
    should_reconnect_ = false;  // Prevent auto-reconnect
    is_connected_ = false;
    reconnect_attempts_ = 0;

    // Clear in-flight set — entries will be re-queried on reconnect
    {
        std::lock_guard<std::mutex> lock(in_flight_mutex_);
        in_flight_ids_.clear();
    }

    ws_client_->disconnect();
}

void synchronizer::sync_now() {
    upload_pending_changes();
}

void synchronizer::on_websocket_open() {
    LOG_INFO("synchronizer", "[%s] on_websocket_open (this=%p, db=%s)",
             config_.sync_id.c_str(), (void*)this, db_->config().path.c_str());
    is_connected_ = true;
    reconnect_attempts_ = 0;

    if (on_state_change_) {
        scheduler_->invoke([this] { on_state_change_(true); });
    }

    // Dispatch to scheduler — on_open may fire synchronously on the calling
    // thread (e.g., IPC accept on the main thread). Reconciliation and the
    // initial upload must not block the caller.
    scheduler_->invoke([this] {
        if (is_destroyed_) return;

        register_replication_slot(db_->db(), config_.sync_id);

        if (config_.sync_filter) {
            LOG_DEBUG("synchronizer", "Reconciling sync filter on connect...");
            reconcile_sync_filter();
        }

        LOG_DEBUG("synchronizer", "Calling upload_pending_changes...");
        upload_pending_changes();
    });
}

void synchronizer::on_transport_message(const transport_message& msg) {
    // Guard against use-after-free: the NIO/IPC callback thread may deliver
    // a message after the destructor has started tearing down members.
    if (is_destroyed_) return;

    // Wrap in try-catch: this is called from Swift via the WebSocket callback.
    // C++ exceptions must NOT propagate across the Swift/C++ boundary — that
    // causes std::terminate() → SIGTRAP.
    try {
        std::string json_str = msg.as_string();

        auto event = server_sent_event::from_json(json_str);
        if (!event) {
            if (on_error_) {
                scheduler_->invoke([this] { on_error_("Failed to parse server message"); });
            }
            return;
        }

        LOG_INFO("synchronizer", "[%s] on_transport_message: type=%s entries=%zu (this=%p, db=%s)",
                 config_.sync_id.c_str(),
                 event->event_type == server_sent_event::type::audit_log ? "audit_log" : "ack",
                 event->event_type == server_sent_event::type::audit_log
                     ? event->audit_logs.size() : event->acked_ids.size(),
                 (void*)this, db_->config().path.c_str());

        if (event->event_type == server_sent_event::type::audit_log) {
            // Dispatch to scheduler to serialize with upload_pending_changes.
            // Running apply_remote_changes on the IPC read thread causes
            // SQLite write contention (busy/locked) with the scheduler thread.
            auto entries = std::move(event->audit_logs);
            auto entry_count = entries.size();
            scheduler_->invoke([this, entries = std::move(entries), entry_count] {
                if (is_destroyed_) {
                    LOG_INFO("synchronizer", "[%s] scheduler lambda: is_destroyed_, skipping apply of %zu entries",
                             config_.sync_id.c_str(), entry_count);
                    return;
                }
                LOG_INFO("synchronizer", "[%s] scheduler lambda: applying %zu entries (db=%s)",
                         config_.sync_id.c_str(), entries.size(), db_->config().path.c_str());
                auto applied_ids = apply_remote_changes(entries);
                LOG_INFO("synchronizer", "[%s] apply_remote_changes returned %zu/%zu (db=%s)",
                         config_.sync_id.c_str(), applied_ids.size(), entries.size(),
                         db_->config().path.c_str());

                // Send acknowledgment only for successfully applied entries
                if (!applied_ids.empty()) {
                    auto ack = server_sent_event::make_ack(applied_ids);
                    auto json_ack = ack.to_json();
                    LOG_INFO("synchronizer", "[%s] sending ACK for %zu entries (transport_state=%d, db=%s)",
                             config_.sync_id.c_str(), applied_ids.size(),
                             static_cast<int>(ws_client_->state()),
                             db_->config().path.c_str());
                    ws_client_->send(transport_message::from_binary({json_ack.begin(), json_ack.end()}));
                }

                // If some entries failed, schedule a retry so the sender re-uploads them
                if (applied_ids.size() < entries.size()) {
                    LOG_WARN("synchronizer", "Partial apply: %zu/%zu entries succeeded, %zu failed — sender will retry on next upload cycle",
                             applied_ids.size(), entries.size(),
                             entries.size() - applied_ids.size());
                }
            });

        } else if (event->event_type == server_sent_event::type::ack) {
            // Dispatch to scheduler to serialize with upload_pending_changes.
            auto ids = std::move(event->acked_ids);
            scheduler_->invoke([this, ids = std::move(ids)] {
                if (is_destroyed_) return;
                mark_as_synced(ids);

                if (on_sync_complete_) {
                    on_sync_complete_(ids);
                }
            });
        }
    } catch (const std::exception& e) {
        LOG_ERROR("synchronizer", "on_transport_message failed: %s", e.what());
        if (on_error_) {
            std::string error_msg = e.what();
            scheduler_->invoke([this, error_msg] { on_error_("WebSocket message handling failed: " + error_msg); });
        }
    } catch (...) {
        LOG_ERROR("synchronizer", "on_transport_message failed with unknown error");
    }
}

void synchronizer::on_websocket_error(const std::string& error) {
    LOG_ERROR("synchronizer", "[%s] WebSocket error: %s (this=%p, db=%s)",
              config_.sync_id.c_str(), error.c_str(), (void*)this, db_->config().path.c_str());
    if (on_error_) {
        scheduler_->invoke([this, error] { on_error_(error); });
    }

    // Attempt reconnection
    schedule_reconnect();
}

void synchronizer::on_websocket_close(int code, const std::string& reason) {
    LOG_INFO("synchronizer", "[%s] WebSocket closed (code=%d, reason=%s, this=%p, db=%s)",
             config_.sync_id.c_str(), code, reason.c_str(), (void*)this, db_->config().path.c_str());
    is_connected_ = false;

    if (on_state_change_) {
        scheduler_->invoke([this] { on_state_change_(false); });
    }

    // Attempt reconnection
    schedule_reconnect();
}

// ============================================================================
// Sync Filter helpers
// ============================================================================

bool synchronizer::is_table_in_filter(const std::string& table_name) const {
    if (!config_.sync_filter) return true;  // No filter = everything passes
    for (const auto& entry : *config_.sync_filter) {
        if (entry.table_name == table_name) return true;
    }
    return false;
}

std::optional<std::optional<std::string>> synchronizer::get_filter_for_table(const std::string& table_name) const {
    if (!config_.sync_filter) return std::nullopt;
    for (const auto& entry : *config_.sync_filter) {
        if (entry.table_name == table_name) return entry.where_clause;
    }
    return std::nullopt;  // Table not in filter
}

bool synchronizer::row_matches_filter(const std::string& table_name, const std::string& global_row_id) {
    if (!config_.sync_filter) return true;
    if (!is_table_in_filter(table_name)) return false;

    // get_filter_for_table returns the where_clause for this table.
    // nullopt means table not found (shouldn't happen — we just checked is_table_in_filter).
    // A returned value that is itself nullopt means "all rows".
    // A returned value with a string means "rows matching this predicate".
    auto where_clause = get_filter_for_table(table_name);
    if (!where_clause) return false;  // shouldn't happen
    if (!where_clause->has_value()) {
        // Table in filter with no predicate = all rows match
        return true;
    }

    std::string sql = "SELECT 1 FROM " + table_name +
                      " WHERE globalId = ? AND (" + where_clause->value() + ") LIMIT 1";
    auto rows = db_->db().query(sql, {global_row_id});
    return !rows.empty();
}

std::optional<audit_log_entry> synchronizer::build_insert_entry_from_current_row(
    const std::string& table_name, const std::string& global_row_id) {

    // Get column info
    auto cols = db_->db().query("PRAGMA table_info(" + table_name + ")");

    // Build SELECT with hex() for BLOB columns
    std::ostringstream select_cols;
    std::vector<std::string> col_names;
    bool first = true;
    for (const auto& col : cols) {
        auto name_it = col.find("name");
        auto type_it = col.find("type");
        if (name_it == col.end() || !std::holds_alternative<std::string>(name_it->second))
            continue;
        std::string col_name = std::get<std::string>(name_it->second);
        if (col_name == "id" || col_name == "globalId") continue;

        std::string col_type;
        if (type_it != col.end() && std::holds_alternative<std::string>(type_it->second))
            col_type = std::get<std::string>(type_it->second);

        if (!first) select_cols << ", ";
        first = false;

        if (col_type == "BLOB") {
            select_cols << "hex(" << col_name << ") AS " << col_name;
        } else {
            select_cols << col_name;
        }
        col_names.push_back(col_name);
    }

    // Fetch the row
    std::string sql = "SELECT id, globalId, " + select_cols.str() +
                      " FROM " + table_name + " WHERE globalId = ?";
    auto rows = db_->db().query(sql, {global_row_id});
    if (rows.empty()) {
        LOG_DEBUG("synchronizer", "build_insert_entry_from_current_row: row not found for globalId=%s (deleted?)", global_row_id.c_str());
        return std::nullopt;
    }
    const auto& row = rows[0];

    // Build audit_log_entry
    audit_log_entry entry;
    entry.global_id = db_->generate_global_id();
    entry.table_name = table_name;
    entry.operation = "INSERT";

    // Get rowId
    auto id_it = row.find("id");
    if (id_it != row.end() && std::holds_alternative<int64_t>(id_it->second))
        entry.row_id = std::get<int64_t>(id_it->second);

    entry.global_row_id = global_row_id;
    entry.timestamp = std::to_string(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count() / 1000.0);

    // Populate changed_fields from current row values
    auto schema = db_->get_table_schema(table_name);
    for (const auto& col_name : col_names) {
        auto val_it = row.find(col_name);
        if (val_it == row.end()) continue;

        const auto& cv = val_it->second;
        // Determine the column type from schema
        auto schema_it = schema.find(col_name);
        bool is_blob = (schema_it != schema.end() && schema_it->second == column_type::blob);

        if (std::holds_alternative<std::nullptr_t>(cv)) {
            entry.changed_fields[col_name] = any_property(nullptr);
        } else if (std::holds_alternative<int64_t>(cv)) {
            entry.changed_fields[col_name] = any_property(std::get<int64_t>(cv));
        } else if (std::holds_alternative<double>(cv)) {
            entry.changed_fields[col_name] = any_property(std::get<double>(cv));
        } else if (std::holds_alternative<std::string>(cv)) {
            // hex()-encoded BLOBs are stored as string_kind — matches the
            // AuditLog trigger format. generate_instruction() uses the table
            // schema to determine when unhex(?) is needed.
            entry.changed_fields[col_name] = any_property(std::get<std::string>(cv));
        } else if (std::holds_alternative<std::vector<uint8_t>>(cv)) {
            entry.changed_fields[col_name] = any_property(std::get<std::vector<uint8_t>>(cv));
        }
        entry.changed_fields_names.push_back(col_name);
    }

    return entry;
}

// Sync set CRUD
void synchronizer::sync_set_add(const std::string& table_name, const std::string& global_row_id) {
    db_->db().execute(
        "INSERT OR IGNORE INTO _lattice_sync_set (table_name, global_row_id) VALUES (?, ?)",
        {table_name, global_row_id});
}

void synchronizer::sync_set_remove(const std::string& table_name, const std::string& global_row_id) {
    db_->db().execute(
        "DELETE FROM _lattice_sync_set WHERE table_name = ? AND global_row_id = ?",
        {table_name, global_row_id});
}

bool synchronizer::sync_set_contains(const std::string& table_name, const std::string& global_row_id) {
    auto rows = db_->db().query(
        "SELECT 1 FROM _lattice_sync_set WHERE table_name = ? AND global_row_id = ? LIMIT 1",
        {table_name, global_row_id});
    return !rows.empty();
}

// ============================================================================
// Sync filter update / clear
// ============================================================================

void synchronizer::update_sync_filter(std::vector<sync_filter_entry> filter) {
    // Bump version BEFORE scheduling — if another update arrives before this
    // lambda runs, the version will have advanced and we skip the stale reconcile.
    // This prevents intermediate filter states (e.g., rapid sync→local→sync toggles)
    // from generating DELETE+INSERT churn on the receiving database.
    auto version = filter_version_.fetch_add(1, std::memory_order_acq_rel) + 1;
    scheduler_->invoke([this, filter = std::move(filter), version]() mutable {
        if (filter_version_.load(std::memory_order_acquire) != version) {
            LOG_DEBUG("synchronizer", "Skipping stale filter reconcile (version %llu, current %llu)",
                      (unsigned long long)version,
                      (unsigned long long)filter_version_.load(std::memory_order_acquire));
            // Still update the filter config so the NEXT reconcile uses the latest
            // filter known at the time this lambda was created. But a newer lambda
            // is already queued, so let IT reconcile.
            config_.sync_filter = std::move(filter);
            return;
        }
        config_.sync_filter = std::move(filter);
        reconcile_sync_filter();
    });
}

void synchronizer::clear_sync_filter() {
    scheduler_->invoke([this] {
        config_.sync_filter = std::nullopt;
        // Clear the sync set — without a filter, everything syncs via normal path
        db_->db().execute("DELETE FROM _lattice_sync_set");
    });
}

// ============================================================================
// Reconciliation — called when filter changes at runtime
// ============================================================================

void synchronizer::reconcile_sync_filter() {
    LOG_DEBUG("synchronizer", "reconcile_sync_filter called, is_connected=%d", is_connected_ ? 1 : 0);
    if (!is_connected_) return;

    bool has_changes = false;

    // Phase 1: Removals — check every row in sync set against new filter.
    // Build a set of all currently-matching globalIds per table (one query per filter entry),
    // then iterate the sync set and DELETE anything not in the match set.
    std::unordered_set<std::string> current_matches;  // "table\0globalId"
    if (config_.sync_filter) {
        for (const auto& fe : *config_.sync_filter) {
            std::string sql = "SELECT globalId FROM " + fe.table_name;
            if (fe.where_clause) {
                sql += " WHERE (" + *fe.where_clause + ")";
            }
            auto rows = db_->db().query(sql);
            for (const auto& r : rows) {
                auto gid_it = r.find("globalId");
                if (gid_it != r.end()) {
                    current_matches.insert(fe.table_name + '\0'
                                         + std::get<std::string>(gid_it->second));
                }
            }
        }
    }
    auto sync_set_rows = db_->db().query("SELECT table_name, global_row_id FROM _lattice_sync_set");
    LOG_INFO("synchronizer", "reconcile Phase 1: %zu sync_set rows, %zu current matches",
             sync_set_rows.size(), current_matches.size());
    for (const auto& row : sync_set_rows) {
        auto tn_it = row.find("table_name");
        auto gid_it = row.find("global_row_id");
        if (tn_it == row.end() || gid_it == row.end()) continue;

        std::string tn = std::get<std::string>(tn_it->second);
        std::string gid = std::get<std::string>(gid_it->second);

        if (current_matches.count(tn + '\0' + gid) == 0) {
            db_->db().execute(
                "INSERT INTO AuditLog (globalId, tableName, operation, rowId, globalRowId, "
                "changedFields, changedFieldsNames, isFromRemote, isSynchronized) "
                "VALUES (?, ?, 'DELETE', 0, ?, '{}', '[]', 0, 0)",
                {db_->generate_global_id(), tn, gid});
            has_changes = true;
        }
    }

    // Phase 2: Additions — for each table in filter, find matching rows not in sync set.
    // Insert synthetic INSERT entries into AuditLog so the normal upload pipeline
    // handles delivery, sync set management, and ACK confirmation.
    // Optimized: batch-fetch all matching rows per table instead of per-row queries.
    if (config_.sync_filter) {
        for (const auto& fe : *config_.sync_filter) {
            // Get column metadata once per table (not per row)
            auto cols = db_->db().query("PRAGMA table_info(" + fe.table_name + ")");
            auto schema = db_->get_table_schema(fe.table_name);

            std::ostringstream select_cols;
            std::vector<std::string> col_names;
            bool first = true;
            for (const auto& col : cols) {
                auto name_it = col.find("name");
                auto type_it = col.find("type");
                if (name_it == col.end() || !std::holds_alternative<std::string>(name_it->second))
                    continue;
                std::string col_name = std::get<std::string>(name_it->second);
                if (col_name == "id" || col_name == "globalId") continue;

                std::string col_type;
                if (type_it != col.end() && std::holds_alternative<std::string>(type_it->second))
                    col_type = std::get<std::string>(type_it->second);

                if (!first) select_cols << ", ";
                first = false;
                if (col_type == "BLOB") {
                    select_cols << "hex(" << col_name << ") AS " << col_name;
                } else {
                    select_cols << col_name;
                }
                col_names.push_back(col_name);
            }

            // Batch-fetch all matching rows not in sync set
            std::string sql = "SELECT id, globalId, " + select_cols.str() +
                              " FROM " + fe.table_name +
                              " WHERE globalId NOT IN (SELECT global_row_id FROM _lattice_sync_set WHERE table_name = ?)";
            std::vector<column_value_t> params = {fe.table_name};
            if (fe.where_clause) {
                sql += " AND (" + *fe.where_clause + ")";
            }
            auto all_rows = db_->db().query(sql, params);
            LOG_INFO("synchronizer", "reconcile Phase 2: %s — %zu new rows to synthesize",
                     fe.table_name.c_str(), all_rows.size());

            // Synthesize INSERT audit entries from the batch-fetched rows
            db_->db().begin_transaction();
            try {
                for (const auto& fetched_row : all_rows) {
                    auto gid_it = fetched_row.find("globalId");
                    if (gid_it == fetched_row.end()) continue;
                    std::string gid = std::get<std::string>(gid_it->second);

                    audit_log_entry entry;
                    entry.global_id = db_->generate_global_id();
                    entry.table_name = fe.table_name;
                    entry.operation = "INSERT";
                    entry.global_row_id = gid;

                    auto id_it = fetched_row.find("id");
                    if (id_it != fetched_row.end() && std::holds_alternative<int64_t>(id_it->second))
                        entry.row_id = std::get<int64_t>(id_it->second);

                    entry.timestamp = std::to_string(
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch()).count() / 1000.0);

                    for (const auto& col_name : col_names) {
                        auto val_it = fetched_row.find(col_name);
                        if (val_it == fetched_row.end()) continue;
                        const auto& cv = val_it->second;

                        if (std::holds_alternative<std::nullptr_t>(cv)) {
                            entry.changed_fields[col_name] = any_property(nullptr);
                        } else if (std::holds_alternative<int64_t>(cv)) {
                            entry.changed_fields[col_name] = any_property(std::get<int64_t>(cv));
                        } else if (std::holds_alternative<double>(cv)) {
                            entry.changed_fields[col_name] = any_property(std::get<double>(cv));
                        } else if (std::holds_alternative<std::string>(cv)) {
                            entry.changed_fields[col_name] = any_property(std::get<std::string>(cv));
                        } else if (std::holds_alternative<std::vector<uint8_t>>(cv)) {
                            entry.changed_fields[col_name] = any_property(std::get<std::vector<uint8_t>>(cv));
                        }
                        entry.changed_fields_names.push_back(col_name);
                    }

                    db_->db().execute(
                        "INSERT INTO AuditLog (globalId, tableName, operation, rowId, globalRowId, "
                        "changedFields, changedFieldsNames, isFromRemote, isSynchronized) "
                        "VALUES (?, ?, 'INSERT', ?, ?, ?, ?, 0, 0)",
                        {entry.global_id, entry.table_name,
                         entry.row_id, entry.global_row_id,
                         entry.changed_fields_to_json(),
                         entry.changed_fields_names_to_json()});
                    has_changes = true;
                }
                db_->db().commit();
            } catch (...) {
                if (db_->db().is_in_transaction()) {
                    try { db_->db().rollback(); } catch (...) {}
                }
                throw;
            }
        }
    }

    // Trigger normal upload pipeline — entries get in-flight tracking,
    // chunked sending, ACK confirmation, and retry on reconnect.
    if (has_changes) {
        upload_pending_changes();
    }
}

// ============================================================================
// Upload pending changes (with sync filter support)
// ============================================================================

std::vector<audit_log_entry> synchronizer::query_pending_entries() {
    auto entries = query_audit_log_for_sync(db_->db(), config_.sync_id, config_.sync_filter);

    // Filter out entries already in-flight (sent but not yet ACK'd)
    {
        std::lock_guard<std::mutex> lock(in_flight_mutex_);
        if (!in_flight_ids_.empty()) {
            auto original_size = entries.size();
            entries.erase(
                std::remove_if(entries.begin(), entries.end(),
                    [this](const audit_log_entry& e) {
                        return in_flight_ids_.count(e.global_id) > 0;
                    }),
                entries.end());
            if (entries.size() < original_size) {
                LOG_DEBUG("synchronizer", "Filtered %zu in-flight entries, %zu remaining",
                          original_size - entries.size(), entries.size());
            }
        }
    }

    LOG_DEBUG("synchronizer", "Found %zu unsynced entries to upload", entries.size());
    return entries;
}

void synchronizer::classify_delete(audit_log_entry& entry, classified_entries& result) {
    if (sync_set_contains(entry.table_name, entry.global_row_id)) {
        // Row was in sync set: forward DELETE, remove from sync set
        LOG_DEBUG("synchronizer", "  -> DELETE, in sync set: forwarding");
        result.to_send.push_back(entry);
        sync_set_remove(entry.table_name, entry.global_row_id);
    } else {
        // Row was never synced: mark as processed
        LOG_DEBUG("synchronizer", "  -> DELETE, NOT in sync set: marking synced (skip)");
        result.to_mark_synced.push_back(entry.id);
    }
}

void synchronizer::classify_insert_or_update(audit_log_entry& entry,
                                              const std::string& filter_table,
                                              bool is_link_table,
                                              classified_entries& result) {
    bool matches = is_link_table || row_matches_filter(entry.table_name, entry.global_row_id);
    bool in_set = sync_set_contains(entry.table_name, entry.global_row_id);

    LOG_DEBUG("synchronizer", "  -> INSERT/UPDATE: matches=%d in_set=%d", matches ? 1 : 0, in_set ? 1 : 0);

    if (matches && !in_set) {
        // Row entered the sync set.
        if (entry.operation == "INSERT") {
            // Original is already a full INSERT — send as-is so the ACK matches the original global_id.
            LOG_DEBUG("synchronizer", "  -> entering sync set: sending original INSERT as-is");
            result.to_send.push_back(entry);
            sync_set_add(entry.table_name, entry.global_row_id);
        } else {
            // UPDATE/DELETE moved row into filter — build synthetic full INSERT.
            LOG_DEBUG("synchronizer", "  -> entering sync set: building full INSERT (from %s)", entry.operation.c_str());
            auto insert_entry = build_insert_entry_from_current_row(entry.table_name, entry.global_row_id);
            if (insert_entry) {
                LOG_DEBUG("synchronizer", "  -> built INSERT with %zu changed_fields", insert_entry->changed_fields.size());
                result.to_send.push_back(std::move(*insert_entry));
                sync_set_add(entry.table_name, entry.global_row_id);
                result.to_mark_synced.push_back(entry.id);  // Original superseded
            } else {
                LOG_DEBUG("synchronizer", "  -> row deleted, marking synced (skip)");
                result.to_mark_synced.push_back(entry.id);
            }
        }
    } else if (matches && in_set) {
        // Row still matches: send as-is (normal)
        LOG_DEBUG("synchronizer", "  -> still in sync set: sending as-is");
        result.to_send.push_back(entry);
    } else if (!matches && in_set) {
        // Row left the sync set: send synthetic DELETE, remove from sync set.
        LOG_DEBUG("synchronizer", "  -> leaving sync set: sending synthetic DELETE");
        audit_log_entry del;
        del.global_id = db_->generate_global_id();
        del.table_name = entry.table_name;
        del.operation = "DELETE";
        del.global_row_id = entry.global_row_id;
        del.timestamp = entry.timestamp;
        result.to_send.push_back(std::move(del));
        sync_set_remove(entry.table_name, entry.global_row_id);
        result.to_mark_synced.push_back(entry.id);  // Original entry superseded
    } else {
        // !matches && !in_set: mark as processed
        LOG_DEBUG("synchronizer", "  -> no match, not in set: marking synced (skip)");
        result.to_mark_synced.push_back(entry.id);
    }
}

synchronizer::classified_entries synchronizer::classify_entries(std::vector<audit_log_entry>& entries) {
    // No sync filter: all entries go to to_send
    if (!config_.sync_filter) {
        return {std::move(entries), {}};
    }

    LOG_DEBUG("synchronizer", "FILTERED upload path — filter has %zu entries, %zu audit entries to classify",
              config_.sync_filter->size(), entries.size());

    // Pre-load sync set into memory — O(1) hash lookups instead of O(N) SQL queries.
    // Key is "table_name\0global_row_id" for compact hashing.
    std::unordered_set<std::string> sync_set_cache;
    {
        auto rows = db_->db().query("SELECT table_name, global_row_id FROM _lattice_sync_set");
        sync_set_cache.reserve(rows.size());
        for (const auto& row : rows) {
            auto tn_it = row.find("table_name");
            auto gid_it = row.find("global_row_id");
            if (tn_it != row.end() && gid_it != row.end()) {
                std::string key = std::get<std::string>(tn_it->second) + '\0'
                                + std::get<std::string>(gid_it->second);
                sync_set_cache.insert(std::move(key));
            }
        }
        LOG_DEBUG("synchronizer", "Loaded sync set cache: %zu entries", sync_set_cache.size());
    }

    // Pre-compute filter matches per table — one SQL query per filter entry
    // instead of one per audit entry. Key is "table_name\0global_row_id".
    std::unordered_set<std::string> filter_matches;
    for (const auto& fe : *config_.sync_filter) {
        LOG_DEBUG("synchronizer", "  filter entry: table=%s where=%s",
                  fe.table_name.c_str(),
                  fe.where_clause ? fe.where_clause->c_str() : "(all rows)");

        std::string sql = "SELECT globalId FROM " + fe.table_name;
        if (fe.where_clause) {
            sql += " WHERE (" + *fe.where_clause + ")";
        }
        auto rows = db_->db().query(sql);
        for (const auto& row : rows) {
            auto gid_it = row.find("globalId");
            if (gid_it != row.end()) {
                std::string key = fe.table_name + '\0'
                                + std::get<std::string>(gid_it->second);
                filter_matches.insert(std::move(key));
            }
        }
    }
    LOG_DEBUG("synchronizer", "Pre-computed filter matches: %zu rows", filter_matches.size());

    // Lambda replacements for sync_set_contains and row_matches_filter
    // that use the in-memory caches instead of SQL queries.
    auto cached_sync_set_contains = [&](const std::string& table_name,
                                         const std::string& global_row_id) -> bool {
        return sync_set_cache.count(table_name + '\0' + global_row_id) > 0;
    };
    auto cached_row_matches_filter = [&](const std::string& table_name,
                                          const std::string& global_row_id) -> bool {
        return filter_matches.count(table_name + '\0' + global_row_id) > 0;
    };
    // Keep sync_set_add/remove updating both the cache and the DB.
    auto cached_sync_set_add = [&](const std::string& table_name,
                                    const std::string& global_row_id) {
        sync_set_cache.insert(table_name + '\0' + global_row_id);
        sync_set_add(table_name, global_row_id);
    };
    auto cached_sync_set_remove = [&](const std::string& table_name,
                                       const std::string& global_row_id) {
        sync_set_cache.erase(table_name + '\0' + global_row_id);
        sync_set_remove(table_name, global_row_id);
    };

    classified_entries result;
    for (auto& entry : entries) {
        // Determine the "parent" table for filter lookup.
        // Link tables (start with _) follow their parent model table.
        std::string filter_table = entry.table_name;
        bool is_link_table = !filter_table.empty() && filter_table[0] == '_';
        if (is_link_table) {
            auto underscore_pos = filter_table.find('_', 1);
            if (underscore_pos != std::string::npos) {
                filter_table = filter_table.substr(1, underscore_pos - 1);
            }
        }

        if (!is_table_in_filter(filter_table)) {
            result.to_mark_synced.push_back(entry.id);
            continue;
        }

        if (entry.operation == "DELETE") {
            // Inline classify_delete with cached lookups
            if (cached_sync_set_contains(entry.table_name, entry.global_row_id)) {
                result.to_send.push_back(entry);
                cached_sync_set_remove(entry.table_name, entry.global_row_id);
            } else {
                result.to_mark_synced.push_back(entry.id);
            }
        } else {
            // Inline classify_insert_or_update with cached lookups
            bool matches = is_link_table || cached_row_matches_filter(entry.table_name, entry.global_row_id);
            bool in_set = cached_sync_set_contains(entry.table_name, entry.global_row_id);

            if (matches && !in_set) {
                if (entry.operation == "INSERT") {
                    result.to_send.push_back(entry);
                    cached_sync_set_add(entry.table_name, entry.global_row_id);
                } else {
                    auto insert_entry = build_insert_entry_from_current_row(entry.table_name, entry.global_row_id);
                    if (insert_entry) {
                        result.to_send.push_back(std::move(*insert_entry));
                        cached_sync_set_add(entry.table_name, entry.global_row_id);
                        result.to_mark_synced.push_back(entry.id);
                    } else {
                        result.to_mark_synced.push_back(entry.id);
                    }
                }
            } else if (matches && in_set) {
                result.to_send.push_back(entry);
            } else if (!matches && in_set) {
                audit_log_entry del;
                del.global_id = db_->generate_global_id();
                del.table_name = entry.table_name;
                del.operation = "DELETE";
                del.global_row_id = entry.global_row_id;
                del.timestamp = entry.timestamp;
                result.to_send.push_back(std::move(del));
                cached_sync_set_remove(entry.table_name, entry.global_row_id);
                result.to_mark_synced.push_back(entry.id);
            } else {
                result.to_mark_synced.push_back(entry.id);
            }
        }
    }

    LOG_DEBUG("synchronizer", "Classified: to_send=%zu to_mark_synced=%zu",
              result.to_send.size(), result.to_mark_synced.size());
    return result;
}

void synchronizer::mark_skipped_synced(const std::vector<int64_t>& to_mark_synced) {
    if (to_mark_synced.empty()) return;

    // Mark entries that were skipped as synchronized in the per-sync-id table.
    // Must use _lattice_sync_state (not global AuditLog.isSynchronized) because
    // query_audit_log_for_sync uses _lattice_sync_state to find pending entries.
    // Chunk into transactions of 50 to avoid holding the write lock too long.
    constexpr size_t kMarkChunkSize = 50;
    for (size_t i = 0; i < to_mark_synced.size(); i += kMarkChunkSize) {
        size_t end = std::min(i + kMarkChunkSize, to_mark_synced.size());
        db_->db().begin_transaction();
        try {
            for (size_t j = i; j < end; ++j) {
                int64_t entry_id = to_mark_synced[j];
                db_->db().execute(
                    "INSERT INTO _lattice_sync_state (audit_entry_id, sync_id, is_synchronized) "
                    "VALUES (?, ?, 1) "
                    "ON CONFLICT(audit_entry_id, sync_id) DO UPDATE SET is_synchronized = 1",
                    {entry_id, config_.sync_id});

                // Eager cleanup: if all active sync_ids have synced this entry,
                // collapse to isSynchronized=1 on AuditLog and remove sync_state rows.
                auto count_rows = db_->db().query(
                    "SELECT COUNT(*) as cnt FROM _lattice_sync_state "
                    "WHERE audit_entry_id = ? AND is_synchronized = 1",
                    {entry_id});

                int64_t synced_count = 0;
                if (!count_rows.empty()) {
                    auto cnt_it = count_rows[0].find("cnt");
                    if (cnt_it != count_rows[0].end() && std::holds_alternative<int64_t>(cnt_it->second)) {
                        synced_count = std::get<int64_t>(cnt_it->second);
                    }
                }

                if (synced_count >= static_cast<int64_t>(config_.all_active_sync_ids.size())) {
                    db_->db().execute(
                        "DELETE FROM _lattice_sync_state WHERE audit_entry_id = ?",
                        {entry_id});
                    db_->db().execute(
                        "UPDATE AuditLog SET isSynchronized = 1 WHERE id = ?",
                        {entry_id});
                }
            }
            db_->db().commit();
        } catch (...) {
            if (db_->db().is_in_transaction()) {
                try { db_->db().rollback(); } catch (...) {}
            }
            throw;
        }
    }

    // No progress decrement needed — skipped entries are not counted in
    // progress_pending_upload_ (only to_send entries are counted).
}

void synchronizer::send_entries(std::vector<audit_log_entry>& entries) {
    if (entries.empty()) return;

    // Track sent entries as in-flight
    {
        std::lock_guard<std::mutex> lock(in_flight_mutex_);
        for (const auto& e : entries) {
            in_flight_ids_.insert(e.global_id);
        }
    }

    for (size_t i = 0; i < entries.size(); i += config_.chunk_size) {
        size_t end = std::min(i + config_.chunk_size, entries.size());
        std::vector<audit_log_entry> chunk(entries.begin() + i, entries.begin() + end);

        auto event = server_sent_event::make_audit_log(chunk);
        auto json_str = event.to_json();
        LOG_DEBUG("synchronizer", "Sending %zu entries to server: %s",
                  chunk.size(), json_str.substr(0, 200).c_str());
        ws_client_->send(transport_message::from_binary({json_str.begin(), json_str.end()}));
    }
}

void synchronizer::upload_pending_changes() {
    LOG_DEBUG("synchronizer", "upload_pending_changes called");
    auto entries = query_pending_entries();
    if (entries.empty()) return;
    auto classified = classify_entries(entries);

    // Update progress counters AFTER classification.
    // Only count to_send entries as pending — skipped entries resolve immediately
    // and shouldn't contribute to the pending/total counts.
    // This avoids double-counting: query_pending_entries filters in-flight entries,
    // but if we counted all queried entries, in-flight entries from prior cycles
    // would inflate the total and cause negative pending when ACK'd.
    if (!classified.to_send.empty()) {
        auto count = static_cast<int64_t>(classified.to_send.size());
        progress_pending_upload_.fetch_add(count, std::memory_order_relaxed);
        progress_total_upload_.fetch_add(count, std::memory_order_relaxed);
        fire_progress();
    }

    mark_skipped_synced(classified.to_mark_synced);
    send_entries(classified.to_send);

    // Tail-call: if new entries arrived during upload, re-schedule.
    // Goes to back of scheduler queue — doesn't starve other work.
    if (upload_requested_.exchange(false, std::memory_order_acq_rel)) {
        scheduler_->invoke([this] {
            if (is_destroyed_) return;
            upload_pending_changes();
        });
    }
}

std::vector<std::string> synchronizer::apply_remote_changes(const std::vector<audit_log_entry>& entries) {
    auto applied = lattice::apply_remote_changes_for(*db_, entries, config_.sync_id);
    progress_received_.fetch_add(static_cast<int64_t>(applied.size()), std::memory_order_relaxed);
    fire_progress();
    return applied;
}

void synchronizer::mark_as_synced(const std::vector<std::string>& global_ids) {
    LOG_INFO("synchronizer", "[%s] mark_as_synced: %zu entries ACK'd (progress_acked was %lld)",
             config_.sync_id.c_str(), global_ids.size(),
             (long long)progress_acked_.load(std::memory_order_relaxed));
    mark_audit_entries_synced_for(*db_, global_ids, config_.sync_id, config_.all_active_sync_ids);

    // Remove ACK'd entries from in-flight set
    {
        std::lock_guard<std::mutex> lock(in_flight_mutex_);
        for (const auto& id : global_ids) {
            in_flight_ids_.erase(id);
        }
    }

    auto count = static_cast<int64_t>(global_ids.size());
    progress_acked_.fetch_add(count, std::memory_order_relaxed);
    progress_pending_upload_.fetch_sub(count, std::memory_order_relaxed);
    fire_progress();

    // After processing ACKs, check if more pending entries exist that aren't
    // already in-flight. Without this, entries created during sync (e.g. from
    // WSS relay) stall indefinitely since the observer only fires on new INSERTs.
    bool should_upload = false;
    {
        std::lock_guard<std::mutex> lock(in_flight_mutex_);
        should_upload = in_flight_ids_.empty();
    }
    if (should_upload) {
        scheduler_->invoke([this] {
            if (is_destroyed_) return;
            upload_pending_changes();
        });
    }
}

void synchronizer::schedule_reconnect() {
    bool within_limit = config_.max_reconnect_attempts == 0
        || reconnect_attempts_ < config_.max_reconnect_attempts;
    if (should_reconnect_ && !is_connected_ && within_limit) {
        double delay = std::min(
            std::pow(2.0, reconnect_attempts_) * config_.base_delay_seconds,
            config_.max_delay_seconds);
        ++reconnect_attempts_;
        LOG_INFO("synchronizer", "[%s] Scheduling reconnect attempt %d in %.1fs",
                 config_.sync_id.c_str(), reconnect_attempts_.load(), delay);

        // Schedule reconnection with an interruptible sleep.
        // Short-sleep loop checks is_destroyed_ so that scheduler_->shutdown()
        // in the destructor doesn't block for the full backoff delay.
        scheduler_->invoke([this, delay] {
            auto end = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds(static_cast<int>(delay * 1000));
            while (std::chrono::steady_clock::now() < end) {
                if (is_destroyed_) return;
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            if (is_destroyed_) return;
            if (!is_connected_) {
                connect();
            }
        });
    }
}

std::optional<std::string> synchronizer::get_last_received_event_id() {
    auto rows = db_->db().query(
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
// Progress
// ============================================================================

synchronizer::sync_progress synchronizer::get_progress() const {
    return {
        progress_pending_upload_.load(std::memory_order_relaxed),
        progress_total_upload_.load(std::memory_order_relaxed),
        progress_acked_.load(std::memory_order_relaxed),
        progress_received_.load(std::memory_order_relaxed)
    };
}

void synchronizer::set_on_progress(on_progress_handler handler) {
    LOG_INFO("synchronizer", "[%s] set_on_progress: handler=%s (this=%p, db=%s)",
             config_.sync_id.c_str(),
             handler ? "SET" : "CLEARED",
             (void*)this, db_->config().path.c_str());
    std::lock_guard<std::mutex> lock(progress_handler_mutex_);
    on_progress_ = std::move(handler);
}

void synchronizer::fire_progress() {
    on_progress_handler handler;
    {
        std::lock_guard<std::mutex> lock(progress_handler_mutex_);
        handler = on_progress_;
    }
    auto p = get_progress();
    LOG_INFO("synchronizer", "[%s] fire_progress: pending=%lld total=%lld acked=%lld received=%lld handler=%s (this=%p)",
             config_.sync_id.c_str(),
             (long long)p.pending_upload, (long long)p.total_upload,
             (long long)p.acked, (long long)p.received,
             handler ? "SET" : "NULL",
             (void*)this);
    if (handler) {
        handler(p);
    }
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

// Notify AuditLog observers on ALL instances sharing this database path.
// The synchronizer lives on one instance, but changeStream listeners may
// be on other instances (different isolation contexts).
static void notify_observers(lattice_db& db,
                             const std::vector<std::pair<int64_t, std::string>>& notify_list) {
    if (notify_list.empty()) return;

    if (db.config().path == ":memory:" || db.config().path.empty()) {
        for (const auto& [row_id, gid] : notify_list) {
            db.notify_change("AuditLog", "UPDATE", row_id, gid);
        }
    } else {
        for (const auto& [row_id, gid] : notify_list) {
            instance_registry::instance().for_each_alive(db.config().path,
                [&](lattice_db* instance) {
                    instance->notify_change("AuditLog", "UPDATE", row_id, gid);
                });
        }
    }
}

void mark_audit_entries_synced(lattice_db& db, const std::vector<std::string>& global_ids) {
    // Collect row IDs for observer notification after commit
    std::vector<std::pair<int64_t, std::string>> notify_list;

    // If we're already inside a transaction (e.g. called from receive_sync_data),
    // don't start a nested one — just do the work in the existing transaction.
    bool own_transaction = false;
    if (!db.db().is_in_transaction()) {
        db.db().begin_transaction();
        own_transaction = true;
    }

    try {
        for (const auto& gid : global_ids) {
            // Check current state — skip if already synced (avoids spurious
            // observer notifications when ACKs are forwarded by the server)
            auto rows = db.db().query(
                "SELECT id, isSynchronized FROM AuditLog WHERE globalId = ?",
                {gid}
            );

            if (rows.empty()) continue;

            auto synced_it = rows[0].find("isSynchronized");
            bool already_synced = synced_it != rows[0].end() &&
                std::holds_alternative<int64_t>(synced_it->second) &&
                std::get<int64_t>(synced_it->second) != 0;

            if (already_synced) continue;

            db.db().execute("UPDATE AuditLog SET isSynchronized = 1 WHERE globalId = ?", {gid});

            auto it = rows[0].find("id");
            if (it != rows[0].end() && std::holds_alternative<int64_t>(it->second)) {
                notify_list.emplace_back(std::get<int64_t>(it->second), gid);
            }
        }

        if (own_transaction) {
            db.db().commit();
        }
    } catch (...) {
        if (own_transaction && db.db().is_in_transaction()) {
            try { db.db().rollback(); } catch (...) {}
        }
        throw;
    }

    notify_observers(db, notify_list);
}

void mark_audit_entries_synced_for(lattice_db& db,
                                   const std::vector<std::string>& global_ids,
                                   const std::string& sync_id,
                                   const std::vector<std::string>& all_active_sync_ids) {
    std::vector<std::pair<int64_t, std::string>> notify_list;

    // Process in chunks of 50 to avoid holding the write lock too long.
    constexpr size_t kChunkSize = 50;
    for (size_t i = 0; i < global_ids.size(); i += kChunkSize) {
        size_t end = std::min(i + kChunkSize, global_ids.size());
        int64_t chunk_max_entry_id = 0;

        db.db().begin_transaction();
        try {
            for (size_t j = i; j < end; ++j) {
                const auto& gid = global_ids[j];

                // Get the AuditLog entry id
                auto rows = db.db().query(
                    "SELECT id FROM AuditLog WHERE globalId = ?", {gid});
                if (rows.empty()) continue;

                auto id_it = rows[0].find("id");
                if (id_it == rows[0].end() || !std::holds_alternative<int64_t>(id_it->second))
                    continue;
                int64_t entry_id = std::get<int64_t>(id_it->second);

                if (entry_id > chunk_max_entry_id) {
                    chunk_max_entry_id = entry_id;
                }

                // Insert or update sync state for this sync_id
                db.db().execute(R"(
                    INSERT INTO _lattice_sync_state (audit_entry_id, sync_id, is_synchronized)
                    VALUES (?, ?, 1)
                    ON CONFLICT(audit_entry_id, sync_id) DO UPDATE SET is_synchronized = 1
                )", {entry_id, sync_id});

                // Eager cleanup: check if ALL active sync_ids have synced this entry
                auto count_rows = db.db().query(
                    "SELECT COUNT(*) as cnt FROM _lattice_sync_state "
                    "WHERE audit_entry_id = ? AND is_synchronized = 1",
                    {entry_id});

                int64_t synced_count = 0;
                if (!count_rows.empty()) {
                    auto cnt_it = count_rows[0].find("cnt");
                    if (cnt_it != count_rows[0].end() && std::holds_alternative<int64_t>(cnt_it->second)) {
                        synced_count = std::get<int64_t>(cnt_it->second);
                    }
                }

                if (synced_count >= static_cast<int64_t>(all_active_sync_ids.size())) {
                    // All synchronizers have synced — clean up and collapse to isSynchronized=1
                    db.db().execute(
                        "DELETE FROM _lattice_sync_state WHERE audit_entry_id = ?",
                        {entry_id});
                    db.db().execute(
                        "UPDATE AuditLog SET isSynchronized = 1 WHERE id = ?",
                        {entry_id});
                    notify_list.emplace_back(entry_id, gid);
                }
            }

            db.db().commit();
        } catch (...) {
            if (db.db().is_in_transaction()) {
                try { db.db().rollback(); } catch (...) {}
            }
            throw;
        }

        // Advance replication slot outside the transaction — crash between
        // commit and advance means the slot is slightly behind, which is safe
        // (conservative compaction).
        if (chunk_max_entry_id > 0) {
            advance_replication_slot(db.db(), sync_id, chunk_max_entry_id);
        }
    }

    notify_observers(db, notify_list);
}

std::vector<audit_log_entry> query_audit_log_for_sync(
        database& db,
        const std::string& sync_id,
        const std::optional<std::vector<sync_filter_entry>>& sync_filter) {
    // Query entries not yet synced for this specific sync_id.
    // No isFromRemote filter — any entry not synced by this sync_id is pending,
    // enabling cross-transport relay (IPC→WSS, WSS→BLE, etc.)
    //
    // The WHERE clause handles four cases:
    //   1. sync_state row exists, is_synchronized=0 → pending for this sync_id
    //   2. sync_state row exists, is_synchronized=1 → synced, EXCLUDED
    //   3. No sync_state row, isSynchronized=0 → never tracked, pending
    //   4. No sync_state row, isSynchronized=1 → eagerly cleaned up (all sync_ids
    //      have synced), EXCLUDED. Without this check, entries bounce in an infinite
    //      loop: sent → ACK'd → eager cleanup deletes row → re-queried as pending.
    std::string sql = R"(
        SELECT a.* FROM AuditLog a
        LEFT JOIN _lattice_sync_state ss
            ON ss.audit_entry_id = a.id AND ss.sync_id = ?
        WHERE (ss.is_synchronized = 0
               OR (ss.is_synchronized IS NULL AND a.isSynchronized = 0))
    )";

    // Pre-filter at the SQL level by both table name AND where_clause predicate.
    // This avoids loading thousands of irrelevant entries from unrelated tables
    // or entries for rows that don't match the sync filter.
    //
    // For tables with a where_clause, we still pass through:
    //   1. DELETE entries (row is gone — app layer checks sync_set membership)
    //   2. Rows in _lattice_sync_set (may need synthetic DELETE if they left the filter)
    //   3. Rows matching the where_clause predicate
    // This is safe because reconcile_sync_filter() runs BEFORE upload_pending_changes()
    // in on_websocket_open, so sync_set is already up-to-date by the time we query.
    if (sync_filter && !sync_filter->empty()) {
        sql += " AND (";
        for (size_t i = 0; i < sync_filter->size(); ++i) {
            if (i > 0) sql += " OR ";
            const auto& entry = (*sync_filter)[i];
            if (!entry.where_clause || entry.where_clause->empty()) {
                // No predicate — all rows for this table
                sql += "a.tableName = '" + entry.table_name + "'";
            } else {
                // Predicate filter: pass DELETEs, sync-set members, and matching rows
                sql += "(a.tableName = '" + entry.table_name + "' AND ("
                       "a.operation = 'DELETE'"
                       " OR EXISTS (SELECT 1 FROM _lattice_sync_set"
                       " WHERE table_name = a.tableName AND global_row_id = a.globalRowId)"
                       " OR EXISTS (SELECT 1 FROM " + entry.table_name +
                       " WHERE globalId = a.globalRowId AND (" + *entry.where_clause + "))"
                       "))";
            }
        }
        // Include link tables (start with _) — they follow their parent model
        sql += " OR a.tableName LIKE '\\_%' ESCAPE '\\'";
        sql += ")";
    }

    sql += " ORDER BY a.id ASC";

    auto rows = db.query(sql, {sync_id});

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
        entry.changed_fields = audit_log_entry::parse_changed_fields(get_str("changedFields"));
        entry.changed_fields_names = audit_log_entry::parse_changed_fields_names(get_str("changedFieldsNames"));
        entry.timestamp = get_str("timestamp");
        entry.is_from_remote = get_int("isFromRemote") != 0;
        entry.is_synchronized = get_int("isSynchronized") != 0;

        entries.push_back(entry);
    }

    return entries;
}

std::vector<audit_log_entry> events_after(database& db, const std::optional<std::string>& checkpoint_global_id) {
    if (checkpoint_global_id) {
        return query_audit_log(db, false, checkpoint_global_id);
    } else {
        return query_audit_log(db, false, std::nullopt);
    }
}

// =========================================================================
// Replication slot management
// =========================================================================

void register_replication_slot(database& db, const std::string& sync_id) {
    db.execute(R"(
        INSERT INTO _lattice_replication_slots (sync_id, last_active_at)
        VALUES (?, datetime('now'))
        ON CONFLICT(sync_id) DO UPDATE SET last_active_at = datetime('now')
    )", {sync_id});
}

void advance_replication_slot(database& db, const std::string& sync_id, int64_t confirmed_audit_id) {
    db.execute(R"(
        UPDATE _lattice_replication_slots
        SET confirmed_audit_id = MAX(confirmed_audit_id, ?),
            last_active_at = datetime('now')
        WHERE sync_id = ?
    )", {confirmed_audit_id, sync_id});
}

void remove_replication_slot(database& db, const std::string& sync_id) {
    db.execute("DELETE FROM _lattice_replication_slots WHERE sync_id = ?", {sync_id});
}

// Unified implementation for applying remote changes.
// receiving_sync_id = nullopt → global mode (server path): isSynchronized=1, no _lattice_sync_state row.
// receiving_sync_id = value   → per-sync mode (relay path): isSynchronized=0, insert _lattice_sync_state row.
static std::vector<std::string> apply_remote_changes_impl(
    lattice_db& db,
    const std::vector<audit_log_entry>& entries,
    const std::optional<std::string>& receiving_sync_id)
{
    std::vector<std::string> applied_ids;
    if (entries.empty()) return applied_ids;

    // Process in chunks to avoid holding the write lock for too long.
    const size_t chunk_size = 50;

    for (size_t chunk_start = 0; chunk_start < entries.size(); chunk_start += chunk_size) {
        size_t chunk_end = std::min(chunk_start + chunk_size, entries.size());

        try {
            db.db().begin_transaction();

            // Disable sync triggers for this chunk
            db.db().execute("UPDATE _SyncControl SET disabled = 1 WHERE id = 1");

            for (size_t i = chunk_start; i < chunk_end; ++i) {
                const auto& entry = entries[i];

                // Re-check inside transaction (entry may have been inserted concurrently)
                auto existing = db.db().query(
                    "SELECT id FROM AuditLog WHERE globalId = ?",
                    {entry.global_id}
                );
                if (!existing.empty()) {
                    applied_ids.push_back(entry.global_id);
                    continue;
                }

                // If it's a link table (starts with _), ensure it exists
                if (!entry.table_name.empty() && entry.table_name[0] == '_') {
                    db.ensure_link_table(entry.table_name);
                }

                // Generate and execute the SQL instruction
                auto schema = db.get_table_schema(entry.table_name);
                auto [sql, params] = entry.generate_instruction(schema);

                // Resolve the local rowId and operation for this object.
                // flush_changes correlates model changes with AuditLog entries by
                // (tableName, rowId, operation). After compaction, the sender's
                // rowId and operation may differ from the receiver's:
                //   - rowId diverges when INSERT...ON CONFLICT DO UPDATE bumps
                //     the autoincrement counter
                //   - operation diverges when sender says INSERT but receiver
                //     already has the row (ON CONFLICT takes the UPDATE path)
                int64_t local_row_id = entry.row_id;
                std::string local_operation = entry.operation;
                bool is_model_table = !entry.table_name.empty() && entry.table_name[0] != '_';

                bool row_existed = false;
                if (is_model_table && !entry.global_row_id.empty()) {
                    auto pre_rows = db.db().query(
                        "SELECT id FROM " + entry.table_name + " WHERE globalId = ?",
                        {entry.global_row_id}
                    );
                    if (!pre_rows.empty()) {
                        row_existed = true;
                        auto it = pre_rows[0].find("id");
                        if (it != pre_rows[0].end() && std::holds_alternative<int64_t>(it->second)) {
                            local_row_id = std::get<int64_t>(it->second);
                        }
                        if (entry.operation == "INSERT") {
                            local_operation = "UPDATE";
                        }
                    }
                }

                bool sql_succeeded = true;
                if (!sql.empty()) {
                    try {
                        db.db().execute(sql, params);
                    } catch (const std::exception& e) {
                        LOG_ERROR("apply_remote", "Failed: %s (SQL: %s)", e.what(), sql.c_str());
                        sql_succeeded = false;
                    }
                }

                if (!sql_succeeded) {
                    continue;
                }

                // For genuine INSERTs (row didn't exist), get the new local rowId
                if (is_model_table && !row_existed && entry.operation != "DELETE" && !entry.global_row_id.empty()) {
                    auto post_rows = db.db().query(
                        "SELECT id FROM " + entry.table_name + " WHERE globalId = ?",
                        {entry.global_row_id}
                    );
                    if (!post_rows.empty()) {
                        auto it = post_rows[0].find("id");
                        if (it != post_rows[0].end() && std::holds_alternative<int64_t>(it->second)) {
                            local_row_id = std::get<int64_t>(it->second);
                        }
                    }
                }

                // Record the audit entry as from remote.
                // Global mode: isSynchronized=1 (fully synced).
                // Per-sync mode: isSynchronized=0 (other sync_ids still need to relay it).
                int is_synchronized = receiving_sync_id ? 0 : 1;
                std::string insert_sql = R"(
                    INSERT INTO AuditLog (globalId, tableName, operation, rowId, globalRowId,
                        changedFields, changedFieldsNames, timestamp, isFromRemote, isSynchronized)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                )";
                db.db().execute(insert_sql, {
                    entry.global_id,
                    entry.table_name,
                    local_operation,
                    local_row_id,
                    entry.global_row_id,
                    entry.changed_fields_to_json(),
                    entry.changed_fields_names_to_json(),
                    entry.timestamp,
                    static_cast<int64_t>(is_synchronized)
                });

                // Per-sync mode: mark this entry as synced for the receiving sync_id (loop prevention).
                if (receiving_sync_id) {
                    auto audit_rows = db.db().query(
                        "SELECT id FROM AuditLog WHERE globalId = ?", {entry.global_id});
                    if (!audit_rows.empty()) {
                        auto id_it = audit_rows[0].find("id");
                        if (id_it != audit_rows[0].end() && std::holds_alternative<int64_t>(id_it->second)) {
                            int64_t audit_id = std::get<int64_t>(id_it->second);
                            db.db().execute(R"(
                                INSERT INTO _lattice_sync_state (audit_entry_id, sync_id, is_synchronized)
                                VALUES (?, ?, 1)
                                ON CONFLICT(audit_entry_id, sync_id) DO UPDATE SET is_synchronized = 1
                            )", {audit_id, *receiving_sync_id});
                        }
                    }
                }

                applied_ids.push_back(entry.global_id);
            }

            // Re-enable sync triggers
            db.db().execute("UPDATE _SyncControl SET disabled = 0 WHERE id = 1");

            db.db().commit();
        } catch (...) {
            db.db().rollback();
            throw;
        }
    }

    return applied_ids;
}

std::vector<std::string> apply_remote_changes(lattice_db& db, const std::vector<audit_log_entry>& entries) {
    return apply_remote_changes_impl(db, entries, std::nullopt);
}

std::vector<std::string> apply_remote_changes_for(lattice_db& db,
                              const std::vector<audit_log_entry>& entries,
                              const std::string& sync_id) {
    return apply_remote_changes_impl(db, entries, sync_id);
}

} // namespace lattice
