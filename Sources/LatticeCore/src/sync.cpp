#include "lattice/sync.hpp"
#include "lattice/lattice.hpp"
#include <nlohmann/json.hpp>
#include <algorithm>
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

    // Helper to get placeholder - use unhex(?) only when the value is a hex
    // string. When the value is already raw bytes (data_kind with vector<uint8_t>,
    // e.g. after JSON deserialization), bind directly with ? to avoid
    // unhex(raw_bytes) producing garbage.
    auto placeholder = [&is_blob, this](const std::string& col) -> std::string {
        if (!is_blob(col)) return "?";
        auto it = changed_fields.find(col);
        if (it != changed_fields.end() &&
            it->second.kind == any_property_kind::data_kind &&
            std::holds_alternative<std::vector<uint8_t>>(it->second.value)) {
            return "?";  // Raw bytes — bind directly
        }
        return "unhex(?)";  // Hex string — needs decoding
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
    } else if (event_type == type::replay_request) {
        j["kind"] = "replayRequest";
        j["replayRequest"] = true;
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

        } else if (j.contains("replayRequest")) {
            server_sent_event event;
            event.event_type = type::replay_request;
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

// ============================================================================
// synchronizer_base implementation
// ============================================================================

void synchronizer_base::init_sync(const sync_config& config, std::shared_ptr<scheduler> sched) {
    config_ = config;
    scheduler_ = sched;
    auto n = g_sync_instance_count.fetch_add(1, std::memory_order_relaxed) + 1;
    LOG_INFO("synchronizer", "[%s] CREATED (WSS, this=%p, db=%s, alive=%lld)",
             config_.sync_id.c_str(), (void*)this, db().config().path.c_str(), (long long)n);
    auto factory = get_network_factory();
    ws_client_ = factory->create_sync_transport();
    setup_transport_handlers();
    setup_observer();
#ifndef __EMSCRIPTEN__
    start_pacer();
#endif
}

void synchronizer_base::init_sync(const sync_config& config, std::shared_ptr<scheduler> sched,
                                   std::unique_ptr<sync_transport> transport) {
    config_ = config;
    scheduler_ = sched;
    ws_client_ = std::move(transport);
    auto n = g_sync_instance_count.fetch_add(1, std::memory_order_relaxed) + 1;
    LOG_INFO("synchronizer", "[%s] CREATED (IPC, this=%p, db=%s, alive=%lld)",
             config_.sync_id.c_str(), (void*)this, db().config().path.c_str(), (long long)n);
    setup_transport_handlers();
    setup_observer();
#ifndef __EMSCRIPTEN__
    start_pacer();
#endif
}

void synchronizer_base::request_upload() {
    if (is_destroyed_) return;
#ifdef __EMSCRIPTEN__
    // Single-threaded build: no pacer thread. Legacy immediate dispatch —
    // without this, browser builds would never upload.
    upload_requested_.store(true, std::memory_order_release);
    scheduler_->invoke([this] {
        if (is_destroyed_) return;
        if (upload_requested_.exchange(false, std::memory_order_acq_rel)) {
            upload_pending_changes();
        }
    });
#else
    if (config_.upload_coalesce_ms <= 0) {
        // Legacy behavior: one dispatch per request, exchange-guarded so
        // bursts still collapse to one pass per queued invoke.
        upload_requested_.store(true, std::memory_order_release);
        scheduler_->invoke([this] {
            if (is_destroyed_) return;
            if (upload_requested_.exchange(false, std::memory_order_acq_rel)) {
                upload_pending_changes();
            }
        });
        return;
    }
    bool fire_now = false;
    {
        std::lock_guard<std::mutex> lock(pacer_mutex_);
        const auto now = std::chrono::steady_clock::now();
        if (now >= next_allowed_tick_) {
            // Leading edge: dispatch immediately (inline enqueue on the
            // requesting thread — preserves sync_now()'s synchronous
            // semantics under immediate_scheduler and adds zero latency to
            // isolated IPC writes) and open a coalescing window.
            next_allowed_tick_ = now + std::chrono::milliseconds(config_.upload_coalesce_ms);
            fire_now = true;
        } else {
            // In-window: absorbed into the pacer's single trailing-edge tick.
            upload_requested_.store(true, std::memory_order_release);
        }
    }
    if (fire_now) {
        scheduler_->invoke([this] {
            if (is_destroyed_) return;
            upload_pending_changes();
        });
    } else {
        pacer_cv_.notify_one();
    }
#endif
}

#ifndef __EMSCRIPTEN__
void synchronizer_base::maybe_checkpoint() {
    if (config_.checkpoint_passive_interval_ms <= 0) return;
    const auto now = std::chrono::steady_clock::now();

    // TRUNCATE first when due AND idle — a successful truncate subsumes the
    // passive pass. "Idle" = nothing sent-but-unACKed; pending mirrors the
    // in-flight set (see send_entries), so this gate is actually reachable
    // during/after catch-up, unlike the old whole-backlog counter.
    const bool truncate_due =
        config_.checkpoint_truncate_interval_ms > 0 &&
        now - last_truncate_ckpt_ >=
            std::chrono::milliseconds(config_.checkpoint_truncate_interval_ms);
    const bool idle =
        progress_pending_upload_.load(std::memory_order_relaxed) == 0;
    const bool passive_due =
        now - last_passive_ckpt_ >=
            std::chrono::milliseconds(config_.checkpoint_passive_interval_ms);

    LOG_DEBUG("synchronizer", "[%s] maybe_checkpoint: pending=%lld truncate_due=%d idle=%d passive_due=%d",
              config_.sync_id.c_str(),
              (long long)progress_pending_upload_.load(std::memory_order_relaxed),
              truncate_due ? 1 : 0, idle ? 1 : 0, passive_due ? 1 : 0);
    if (!(passive_due || (truncate_due && idle))) return;
    last_passive_ckpt_ = now;
    const bool try_truncate = truncate_due && idle;
    if (try_truncate) last_truncate_ckpt_ = now;

    // Run on the scheduler: the checkpoint uses the synchronizer's WRITE
    // connection, and the scheduler serializes it against upload passes on
    // the same connection. Enqueue-only from here — never blocks the pacer.
    scheduler_->invoke([this, try_truncate] {
        if (is_destroyed_) return;
        auto res = db().db().wal_checkpoint(try_truncate, /*busy_budget_ms=*/250);
        if (try_truncate && res.busy != 0) {
            // Readers held the WAL — fall back to PASSIVE in the same cycle
            // so backfill progress always happens; truncate retries next due.
            res = db().db().wal_checkpoint(false);
            LOG_DEBUG("synchronizer", "[%s] TRUNCATE checkpoint busy — PASSIVE fallback: log=%lld ckpt=%lld",
                      config_.sync_id.c_str(), (long long)res.log_frames, (long long)res.checkpointed);
        } else if (try_truncate) {
            LOG_INFO("synchronizer", "[%s] WAL TRUNCATE checkpoint: log=%lld ckpt=%lld",
                     config_.sync_id.c_str(), (long long)res.log_frames, (long long)res.checkpointed);
        } else {
            LOG_DEBUG("synchronizer", "[%s] WAL PASSIVE checkpoint: busy=%d log=%lld ckpt=%lld",
                      config_.sync_id.c_str(), res.busy, (long long)res.log_frames, (long long)res.checkpointed);
        }
    });
}

void synchronizer_base::start_pacer() {
    if (config_.upload_coalesce_ms <= 0) return;
    pacer_thread_ = std::thread([this] {
        std::unique_lock<std::mutex> lock(pacer_mutex_);
        last_passive_ckpt_ = std::chrono::steady_clock::now();
        last_truncate_ckpt_ = last_passive_ckpt_;
        for (;;) {
            // Timed wait: requests wake us for coalescing; the timeout drives
            // periodic WAL maintenance even when fully idle or disconnected.
            const auto heartbeat = std::chrono::milliseconds(
                config_.checkpoint_passive_interval_ms > 0
                    ? std::min(config_.checkpoint_passive_interval_ms, 60'000)
                    : 60'000);
            pacer_cv_.wait_for(lock, heartbeat, [this] {
                return pacer_stop_ || upload_requested_.load(std::memory_order_acquire);
            });
            if (pacer_stop_) return;
            maybe_checkpoint();
            if (!upload_requested_.load(std::memory_order_acquire)) continue;
            // Trailing edge: wait out the remainder of the window opened by
            // the leading edge, then fire ONE coalesced tick for however many
            // requests landed meanwhile.
            while (!pacer_stop_ &&
                   std::chrono::steady_clock::now() < next_allowed_tick_) {
                pacer_cv_.wait_until(lock, next_allowed_tick_);
            }
            if (pacer_stop_) return;
            if (upload_requested_.exchange(false, std::memory_order_acq_rel)) {
                next_allowed_tick_ = std::chrono::steady_clock::now() +
                                     std::chrono::milliseconds(config_.upload_coalesce_ms);
                scheduler_->invoke([this] {
                    if (is_destroyed_) return;
                    upload_pending_changes();
                });
            }
        }
    });
}

void synchronizer_base::stop_pacer() {
    {
        std::lock_guard<std::mutex> lock(pacer_mutex_);
        pacer_stop_ = true;
    }
    pacer_cv_.notify_all();
    if (pacer_thread_.joinable()) pacer_thread_.join();
}
#endif  // !__EMSCRIPTEN__

void synchronizer_base::setup_transport_handlers() {
    ws_client_->set_on_open([this] { on_websocket_open(); });
    ws_client_->set_on_message([this](const transport_message& msg) { on_transport_message(msg); });
    ws_client_->set_on_error([this](const std::string& err) { on_websocket_error(err); });
    ws_client_->set_on_close([this](int code, const std::string& reason) { on_websocket_close(code, reason); });
}

void synchronizer_base::setup_observer() {
    LOG_DEBUG("synchronizer", "Registering AuditLog observer on instance %p", (void*)&db());
    audit_log_observer_id_ = db().add_table_observer("AuditLog",
        [this](const std::vector<lattice_db::change_event>& batch) {
            // The synchronizer only cares about INSERTs (new entries
            // pending upload). Walk the batch and decide whether to
            // request an upload. Whether the batch has 1 or N rows, we
            // need at most one upload request per fire — the upload
            // path drains all pending changes regardless of which entry
            // triggered it.
            bool any_unsynced_insert = false;
            for (const auto& [_, op, row_id, __, ___] : batch) {
                if (op != "INSERT") continue;
                if (!is_connected_) continue;
                auto rows = db().read_db().query(
                    "SELECT ss.is_synchronized FROM _lattice_sync_state ss "
                    "WHERE ss.audit_entry_id = ? AND ss.sync_id = ?",
                    {row_id, config_.sync_id}
                );
                bool is_synced_for_us = !rows.empty() &&
                    std::holds_alternative<int64_t>(rows[0].begin()->second) &&
                    std::get<int64_t>(rows[0].begin()->second) != 0;
                if (!is_synced_for_us) {
                    any_unsynced_insert = true;
                    LOG_DEBUG("synchronizer", "AuditLog batch has unsynced INSERT row_id=%lld",
                              (long long)row_id);
                    break;
                }
            }
            if (any_unsynced_insert) {
                LOG_DEBUG("synchronizer", "Requesting upload for entries pending on sync_id=%s",
                          config_.sync_id.c_str());
                request_upload();
            }
        });
}

synchronizer_base::~synchronizer_base() {
    LOG_INFO("synchronizer", "[%s] ~synchronizer START (this=%p, db=%s)",
             config_.sync_id.c_str(), (void*)this,
             db().config().path.c_str());
    // Mark as destroyed so scheduled lambdas bail out.
    is_destroyed_ = true;
#ifndef __EMSCRIPTEN__
    // Stop the pacer BEFORE scheduler shutdown: it only enqueues to the
    // scheduler (never blocks on it), and requests arriving after
    // is_destroyed_ are no-ops, so join order is deadlock-free.
    stop_pacer();
#endif
    // Invalidate the ack-timeout guard before ANY teardown: the detached
    // retry thread only touches `this` under this mutex while alive==true.
    {
        std::lock_guard<std::mutex> g(ack_guard_->m);
        ack_guard_->alive = false;
    }
    // Remove AuditLog observer
    if (audit_log_observer_id_ != 0) {
        db().remove_table_observer("AuditLog", audit_log_observer_id_);
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

void synchronizer_base::connect() {
    LOG_INFO("synchronizer", "[%s] connect() (this=%p, db=%s)",
             config_.sync_id.c_str(), (void*)this, db().config().path.c_str());
    if (config_.websocket_url.empty()) {
        // IPC or injected transport — connect without URL/headers.
        // Dialer-side IPC clients auto-reconnect with backoff (they redial
        // the endpoint socket, picking up a restarted server). Server-accepted
        // connections cannot redial — the accept loop replaces them — so their
        // synchronizers never reconnect. (The old blanket `false` here left
        // IPC clients permanently dead after a peer restart: the claimed
        // "client retries via endpoint" path never existed.)
        should_reconnect_ = ws_client_->supports_reconnect();
        LOG_INFO("synchronizer", "[%s] IPC connect: supports_reconnect=%d",
                 config_.sync_id.c_str(), should_reconnect_ ? 1 : 0);
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

void synchronizer_base::disconnect() {
    LOG_INFO("synchronizer", "[%s] disconnect() (this=%p, is_connected=%d, db=%s)",
             config_.sync_id.c_str(), (void*)this, is_connected_ ? 1 : 0,
             db().config().path.c_str());
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

void synchronizer_base::sync_now() {
    // Routed through request_upload: (a) inherits coalescing, (b) fixes a
    // pre-existing hazard — the direct call ran the whole upload path on the
    // caller's thread, unserialized with the scheduler that every other
    // upload pass runs on. Under immediate_scheduler (tests) the leading
    // edge dispatches inline, preserving synchronous semantics.
    request_upload();
}

void synchronizer_base::drain(std::chrono::steady_clock::time_point deadline) {
    if (!is_connected_ || is_destroyed_) return;

    // Fast path: nothing sent-and-unACKed and no unsynchronized AuditLog
    // entries at all — definitively idle, skip the scheduler round-trip.
    // drain runs on the CALLER's thread during teardown: when dozens of
    // instances tear down concurrently (test suites, bulk close), holding
    // each caller in the poll loop just to learn "nothing to send" starves
    // the calling thread pool. A false positive here (an entry pending for a
    // different sync_id) falls through to the slow path, which resolves it.
    bool maybe_pending = progress_pending_upload_.load(std::memory_order_relaxed) > 0;
    if (!maybe_pending) {
        std::lock_guard<std::mutex> lock(in_flight_mutex_);
        maybe_pending = !in_flight_ids_.empty();
    }
    if (!maybe_pending) {
        auto rows = db().db().query("SELECT 1 FROM AuditLog WHERE isSynchronized = 0 LIMIT 1");
        if (rows.empty()) return;
    }

    // Run one upload pass on the scheduler so entries written since the last
    // cycle are picked up and sent. The flag tells us the pass has actually
    // executed — progress_pending_upload_ being 0 before that is meaningless.
    auto pass_done = std::make_shared<std::atomic<bool>>(false);
    scheduler_->invoke([this, pass_done] {
        if (!is_destroyed_) upload_pending_changes();
        pass_done->store(true, std::memory_order_release);
    });

    while (std::chrono::steady_clock::now() < deadline) {
        if (is_destroyed_ || !is_connected_) return;  // connection died — nothing to wait for
        if (pass_done->load(std::memory_order_acquire) &&
            progress_pending_upload_.load(std::memory_order_relaxed) <= 0) {
            return;  // upload pass ran and everything sent has been ACKed
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    LOG_INFO("synchronizer", "[%s] drain: deadline reached with pending=%lld — disconnecting anyway",
             config_.sync_id.c_str(),
             (long long)progress_pending_upload_.load(std::memory_order_relaxed));
}

void synchronizer_base::on_websocket_open() {
    LOG_INFO("synchronizer", "[%s] on_websocket_open (this=%p, db=%s)",
             config_.sync_id.c_str(), (void*)this, db().config().path.c_str());
    is_connected_ = true;
    // Do NOT reset reconnect_attempts_ here: a flapping endpoint fires
    // open→error every cycle, and resetting on open pinned the backoff at
    // base_delay forever (~1s reconnect storm). The counter resets in
    // on_websocket_close/error instead, and only after the connection
    // proved stable (see kStableConnectionMs).
    last_open_time_ms_ = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();

    if (on_state_change_) {
        scheduler_->invoke([this] { on_state_change_(true); });
    }

    // Dispatch to scheduler — on_open may fire synchronously on the calling
    // thread (e.g., IPC accept on the main thread). Reconciliation and the
    // initial upload must not block the caller.
    scheduler_->invoke([this] {
        if (is_destroyed_) return;

        register_replication_slot(db().db(), config_.sync_id);

        // Fresh floor-bookkeeping baseline for this connection: unresolved
        // ids will be re-enumerated by the (bounded) queries; carrying stale
        // enumeration state across connects risks advancing the floor from a
        // last_enumerated_id_ that no longer reflects pending reality.
        {
            std::lock_guard<std::mutex> lock(in_flight_mutex_);
            open_audit_ids_.clear();
            last_enumerated_id_ = 0;
            stall_min_open_ = -1;
        }

        // Fresh-peer catch-up (IPC only — never on WSS, where the deployed
        // server may not parse the event): if this side has never applied a
        // remote entry, its DB is virgin for this channel. Ask the peer to
        // re-arm its filtered snapshot so we receive the full filtered subset
        // — without this, a recreated synced DB receives nothing because the
        // peer's _lattice_sync_set still marks every row as already-synced.
        if (config_.sync_id.rfind("ipc:", 0) == 0 && !get_last_received_event_id()) {
            LOG_INFO("synchronizer", "[%s] fresh peer (no applied remote entries) — sending replay request",
                     config_.sync_id.c_str());
            auto req = server_sent_event::make_replay_request().to_json();
            ws_client_->send(transport_message::from_binary({req.begin(), req.end()}));
        }

        if (config_.sync_filter) {
            LOG_DEBUG("synchronizer", "Reconciling sync filter on connect...");
            reconcile_sync_filter();
        }

        LOG_DEBUG("synchronizer", "Calling upload_pending_changes...");
        upload_pending_changes();
    });
}

void synchronizer_base::on_transport_message(const transport_message& msg) {
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
                 event->event_type == server_sent_event::type::audit_log ? "audit_log"
                     : event->event_type == server_sent_event::type::replay_request ? "replay_request" : "ack",
                 event->event_type == server_sent_event::type::audit_log
                     ? event->audit_logs.size() : event->acked_ids.size(),
                 (void*)this, db().config().path.c_str());

        if (event->event_type == server_sent_event::type::audit_log) {
            // Dispatch to scheduler to serialize with upload_pending_changes.
            // Running apply_remote_changes on the IPC read thread causes
            // SQLite write contention (busy/locked) with the scheduler thread.
            auto entries = std::move(event->audit_logs);
            // Defense in depth: filter-removal DELETEs arriving over a WSS hop
            // are never applied (an old peer or relay may still forward them —
            // unshare must not delete this device's local rows). They ARE
            // acked below so the sender doesn't retry them forever.
            std::vector<std::string> skipped_filter_removals;
            if (config_.sync_id.rfind("wss:", 0) == 0) {
                auto it = std::remove_if(entries.begin(), entries.end(),
                    [&](const audit_log_entry& e) {
                        bool marked = e.operation == "DELETE" &&
                            std::find(e.changed_fields_names.begin(),
                                      e.changed_fields_names.end(),
                                      "__lattice_filter_removal") != e.changed_fields_names.end();
                        if (marked) skipped_filter_removals.push_back(e.global_id);
                        return marked;
                    });
                if (it != entries.end()) {
                    LOG_INFO("synchronizer", "[%s] skipping %zu filter-removal DELETE(s) from WSS peer",
                             config_.sync_id.c_str(), skipped_filter_removals.size());
                    entries.erase(it, entries.end());
                }
            }
            auto entry_count = entries.size();
            scheduler_->invoke([this, entries = std::move(entries), entry_count,
                                skipped_filter_removals = std::move(skipped_filter_removals)] {
                if (is_destroyed_) {
                    LOG_INFO("synchronizer", "[%s] scheduler lambda: is_destroyed_, skipping apply of %zu entries",
                             config_.sync_id.c_str(), entry_count);
                    return;
                }
                LOG_INFO("synchronizer", "[%s] scheduler lambda: applying %zu entries (db=%s)",
                         config_.sync_id.c_str(), entries.size(), db().config().path.c_str());
                auto applied_ids = apply_remote_changes(entries);
                // Ack skipped filter-removals as if applied (see above).
                applied_ids.insert(applied_ids.end(),
                                   skipped_filter_removals.begin(),
                                   skipped_filter_removals.end());
                LOG_INFO("synchronizer", "[%s] apply_remote_changes returned %zu/%zu (db=%s)",
                         config_.sync_id.c_str(), applied_ids.size(), entries.size(),
                         db().config().path.c_str());

                // Send acknowledgment only for successfully applied entries
                if (!applied_ids.empty()) {
                    auto ack = server_sent_event::make_ack(applied_ids);
                    auto json_ack = ack.to_json();
                    LOG_INFO("synchronizer", "[%s] sending ACK for %zu entries (transport_state=%d, db=%s)",
                             config_.sync_id.c_str(), applied_ids.size(),
                             static_cast<int>(ws_client_->state()),
                             db().config().path.c_str());
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

        } else if (event->event_type == server_sent_event::type::replay_request) {
            // Peer announced a fresh DB. Re-arm the filtered snapshot so
            // reconcile re-synthesizes INSERTs for the filtered subset.
            // Gated on having a filter: reset_sync_state's sync-set wipe is
            // global, and an unfiltered sync has no synthesis path to re-send
            // from anyway (see reset_sync_state's documented limitation).
            scheduler_->invoke([this] {
                if (is_destroyed_) return;
                if (!config_.sync_filter) {
                    LOG_INFO("synchronizer", "[%s] replay request ignored (no sync filter on this side)",
                             config_.sync_id.c_str());
                    return;
                }
                LOG_INFO("synchronizer", "[%s] replay request from fresh peer — re-arming filtered snapshot",
                         config_.sync_id.c_str());
                db().reset_sync_state(config_.sync_id);
                // reset_sync_state zeroed the persisted upload_floor and made
                // previously-resolved entries pending again — the in-memory
                // floor bookkeeping must reset with it, or a stale
                // last_enumerated_id_ could re-advance the floor past them.
                {
                    std::lock_guard<std::mutex> lock(in_flight_mutex_);
                    open_audit_ids_.clear();
                    last_enumerated_id_ = 0;
                    stall_min_open_ = -1;
                }
                reconcile_sync_filter();
                upload_pending_changes();
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

void synchronizer_base::on_websocket_error(const std::string& error) {
    LOG_ERROR("synchronizer", "[%s] WebSocket error: %s (this=%p, db=%s)",
              config_.sync_id.c_str(), error.c_str(), (void*)this, db().config().path.c_str());

    // The transport is dead: mark disconnected BEFORE scheduling the
    // reconnect. A transport error without a close event (connection reset
    // mid-send) left is_connected_ true, so schedule_reconnect's
    // !is_connected_ gate refused to reconnect and the synchronizer sat
    // dead until a process restart (production: every drop needed a manual
    // daemon kickstart).
    //
    // exchange() distinguishes "an open connection died" from "a connect
    // attempt failed" (on_error also fires for failed attempts, when
    // is_connected_ is already false — last_open_time_ms_ would be stale).
    const bool was_open = is_connected_.exchange(false);
    maybe_reset_backoff_after_stable_connection(was_open);

    // Clear in-flight tracking so entries can be re-sent after reconnect.
    // Without this, entries added to in_flight_ids_ before the error remain
    // permanently stuck — query_pending_entries() filters them out, and the
    // ACK that would remove them never arrives. Nothing is outstanding on a
    // dead transport, so pending drops to 0 with it.
    {
        std::lock_guard<std::mutex> lock(in_flight_mutex_);
        if (!in_flight_ids_.empty()) {
            LOG_INFO("synchronizer", "[%s] Clearing %zu in-flight entries after error",
                     config_.sync_id.c_str(), in_flight_ids_.size());
            in_flight_ids_.clear();
        }
        progress_pending_upload_.store(0, std::memory_order_relaxed);
    }
    fire_progress();

    if (on_error_) {
        scheduler_->invoke([this, error] { on_error_(error); });
    }

    // Attempt reconnection
    schedule_reconnect();
}

void synchronizer_base::on_websocket_close(int code, const std::string& reason) {
    LOG_INFO("synchronizer", "[%s] WebSocket closed (code=%d, reason=%s, this=%p, db=%s)",
             config_.sync_id.c_str(), code, reason.c_str(), (void*)this, db().config().path.c_str());
    const bool was_open = is_connected_.exchange(false);
    maybe_reset_backoff_after_stable_connection(was_open);

    // Clear in-flight tracking so entries can be re-sent after reconnect.
    // Pending mirrors the (now empty) in-flight set.
    {
        std::lock_guard<std::mutex> lock(in_flight_mutex_);
        if (!in_flight_ids_.empty()) {
            LOG_INFO("synchronizer", "[%s] Clearing %zu in-flight entries after close",
                     config_.sync_id.c_str(), in_flight_ids_.size());
            in_flight_ids_.clear();
        }
        progress_pending_upload_.store(0, std::memory_order_relaxed);
    }
    fire_progress();

    if (on_state_change_) {
        scheduler_->invoke([this] { on_state_change_(false); });
    }

    // Attempt reconnection
    schedule_reconnect();
}

// ============================================================================
// Sync Filter helpers
// ============================================================================

bool synchronizer_base::is_table_in_filter(const std::string& table_name) const {
    if (!config_.sync_filter) return true;  // No filter = everything passes
    for (const auto& entry : *config_.sync_filter) {
        if (entry.table_name == table_name) return true;
    }
    return false;
}

std::optional<std::optional<std::string>> synchronizer_base::get_filter_for_table(const std::string& table_name) const {
    if (!config_.sync_filter) return std::nullopt;
    for (const auto& entry : *config_.sync_filter) {
        if (entry.table_name == table_name) return entry.where_clause;
    }
    return std::nullopt;  // Table not in filter
}

bool synchronizer_base::row_matches_filter(const std::string& table_name, const std::string& global_row_id) {
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
    auto rows = db().db().query(sql, {global_row_id});
    return !rows.empty();
}

std::optional<audit_log_entry> synchronizer_base::build_insert_entry_from_current_row(
    const std::string& table_name, const std::string& global_row_id) {

    // Get column info
    auto cols = db().db().query("PRAGMA table_info(" + table_name + ")");

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
    auto rows = db().db().query(sql, {global_row_id});
    if (rows.empty()) {
        LOG_DEBUG("synchronizer", "build_insert_entry_from_current_row: row not found for globalId=%s (deleted?)", global_row_id.c_str());
        return std::nullopt;
    }
    const auto& row = rows[0];

    // Build audit_log_entry
    audit_log_entry entry;
    entry.global_id = db().generate_global_id();
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
    auto schema = db().get_table_schema(table_name);
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
void synchronizer_base::sync_set_add(const std::string& table_name, const std::string& global_row_id) {
    db().db().execute(
        "INSERT OR IGNORE INTO _lattice_sync_set (table_name, global_row_id) VALUES (?, ?)",
        {table_name, global_row_id});
}

void synchronizer_base::sync_set_remove(const std::string& table_name, const std::string& global_row_id) {
    db().db().execute(
        "DELETE FROM _lattice_sync_set WHERE table_name = ? AND global_row_id = ?",
        {table_name, global_row_id});
}

bool synchronizer_base::sync_set_contains(const std::string& table_name, const std::string& global_row_id) {
    auto rows = db().db().query(
        "SELECT 1 FROM _lattice_sync_set WHERE table_name = ? AND global_row_id = ? LIMIT 1",
        {table_name, global_row_id});
    return !rows.empty();
}

// ============================================================================
// Sync filter update / clear
// ============================================================================

void synchronizer_base::update_sync_filter(std::vector<sync_filter_entry> filter) {
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
        // A filter change can make ALREADY-PENDING entries newly eligible
        // (reconcile's Phase-2 dedup intentionally does not re-synthesize rows
        // whose INSERT entries are still pending). Reconcile only uploads when
        // it synthesized something, so kick the pipeline unconditionally —
        // a no-op when nothing is pending.
        upload_pending_changes();
    });
}

void synchronizer_base::clear_sync_filter() {
    scheduler_->invoke([this] {
        config_.sync_filter = std::nullopt;
        // Clear the sync set — without a filter, everything syncs via normal path
        db().db().execute("DELETE FROM _lattice_sync_set");
    });
}

// ============================================================================
// Reconciliation — called when filter changes at runtime
// ============================================================================

void synchronizer_base::reconcile_sync_filter() {
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
            auto rows = db().db().query(sql);
            for (const auto& r : rows) {
                auto gid_it = r.find("globalId");
                if (gid_it != r.end()) {
                    current_matches.insert(fe.table_name + '\0'
                                         + std::get<std::string>(gid_it->second));
                }
            }
        }
    }
    auto sync_set_rows = db().db().query("SELECT table_name, global_row_id FROM _lattice_sync_set");
    LOG_INFO("synchronizer", "reconcile Phase 1: %zu sync_set rows, %zu current matches",
             sync_set_rows.size(), current_matches.size());
    for (const auto& row : sync_set_rows) {
        auto tn_it = row.find("table_name");
        auto gid_it = row.find("global_row_id");
        if (tn_it == row.end() || gid_it == row.end()) continue;

        std::string tn = std::get<std::string>(tn_it->second);
        std::string gid = std::get<std::string>(gid_it->second);

        if (current_matches.count(tn + '\0' + gid) == 0) {
            // Marked as a filter removal: this DELETE means "stop syncing this
            // row here", not "the user deleted it". It propagates at most one
            // IPC hop (local -> the device's own synced DB) and never crosses
            // a WSS hop — unshare must not become a fleet-wide delete on the
            // user's other devices (see classify_entries / apply gates).
            db().db().execute(
                "INSERT INTO AuditLog (globalId, tableName, operation, rowId, globalRowId, "
                "changedFields, changedFieldsNames, isFromRemote, isSynchronized) "
                "VALUES (?, ?, 'DELETE', 0, ?, '{}', '[\"__lattice_filter_removal\"]', 0, 0)",
                {db().generate_global_id(), tn, gid});
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
            auto cols = db().db().query("PRAGMA table_info(" + fe.table_name + ")");
            auto schema = db().get_table_schema(fe.table_name);

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

            // Batch-fetch all matching rows not in sync set. Also skip rows
            // that already have an INSERT entry still pending for THIS sync
            // (compacted snapshot or a prior synthesis): synthesizing again
            // would double-send — the pending entry already carries the row
            // through the normal upload pipeline.
            std::string sql = "SELECT id, globalId, " + select_cols.str() +
                              " FROM " + fe.table_name +
                              " WHERE globalId NOT IN (SELECT global_row_id FROM _lattice_sync_set WHERE table_name = ?)"
                              " AND globalId NOT IN ("
                              "   SELECT a.globalRowId FROM AuditLog a"
                              "   WHERE a.tableName = ?"
                              "     AND a.operation = 'INSERT'"
                              "     AND a.isSynchronized = 0"
                              "     AND NOT EXISTS ("
                              "       SELECT 1 FROM _lattice_sync_state ss"
                              "       WHERE ss.audit_entry_id = a.id"
                              "         AND ss.sync_id = ?"
                              "         AND ss.is_synchronized = 1))";
            std::vector<column_value_t> params = {fe.table_name, fe.table_name, config_.sync_id};
            if (fe.where_clause) {
                sql += " AND (" + *fe.where_clause + ")";
            }
            auto all_rows = db().db().query(sql, params);
            LOG_INFO("synchronizer", "reconcile Phase 2: %s — %zu new rows to synthesize",
                     fe.table_name.c_str(), all_rows.size());

            // Synthesize INSERT audit entries from the batch-fetched rows
            db().db().begin_transaction();
            try {
                for (const auto& fetched_row : all_rows) {
                    auto gid_it = fetched_row.find("globalId");
                    if (gid_it == fetched_row.end()) continue;
                    std::string gid = std::get<std::string>(gid_it->second);

                    audit_log_entry entry;
                    entry.global_id = db().generate_global_id();
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

                    db().db().execute(
                        "INSERT INTO AuditLog (globalId, tableName, operation, rowId, globalRowId, "
                        "changedFields, changedFieldsNames, isFromRemote, isSynchronized) "
                        "VALUES (?, ?, 'INSERT', ?, ?, ?, ?, 0, 0)",
                        {entry.global_id, entry.table_name,
                         entry.row_id, entry.global_row_id,
                         entry.changed_fields_to_json(),
                         entry.changed_fields_names_to_json()});
                    has_changes = true;
                }
                db().db().commit();
            } catch (...) {
                if (db().db().is_in_transaction()) {
                    try { db().db().rollback(); } catch (...) {}
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

std::vector<audit_log_entry> synchronizer_base::query_pending_entries() {
    int64_t floor = 0;
    size_t limit = 0;
    if (config_.use_upload_floor) {
        // Read the floor from the DB every pass (never cache across passes —
        // accept-side synchronizers are replaced per client reconnect) and
        // cap the scan at one send window plus whatever is already in flight.
        floor = read_upload_floor(db().db(), config_.sync_id);
        size_t in_flight_count;
        {
            std::lock_guard<std::mutex> lock(in_flight_mutex_);
            in_flight_count = in_flight_ids_.size();
        }
        limit = config_.chunk_size * 2 + in_flight_count;
    }
    auto entries = query_audit_log_for_sync(
        db().db(), config_.sync_id, config_.sync_filter, floor, limit);

    // Filter out entries already in-flight (sent but not yet ACK'd), and
    // record every enumerated real audit id as OPEN — the floor may only
    // pass ids once they resolve (ACK or skip).
    {
        std::lock_guard<std::mutex> lock(in_flight_mutex_);
        if (config_.use_upload_floor) {
            for (const auto& e : entries) {
                if (e.id > 0) {
                    open_audit_ids_.insert(e.id);
                    if (e.id > last_enumerated_id_) last_enumerated_id_ = e.id;
                }
            }
        }
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

void synchronizer_base::classify_delete(audit_log_entry& entry, classified_entries& result) {
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

void synchronizer_base::classify_insert_or_update(audit_log_entry& entry,
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
        del.global_id = db().generate_global_id();
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

synchronizer_base::classified_entries synchronizer_base::classify_entries(std::vector<audit_log_entry>& entries) {
    // No sync filter: all entries go to to_send
    if (!config_.sync_filter) {
        return {std::move(entries), {}};
    }

    LOG_DEBUG("synchronizer", "FILTERED upload path — filter has %zu entries, %zu audit entries to classify",
              config_.sync_filter->size(), entries.size());

    // The preloads below scan the entire _lattice_sync_set and run a full
    // SELECT over every filtered table — worth it for bulk catch-up batches,
    // pure overhead for the steady state (a paced tick carries a handful of
    // entries; point lookups are indexed). Gate on batch size.
    constexpr size_t kClassifyPreloadThreshold = 64;
    const bool preload = entries.size() > kClassifyPreloadThreshold;

    // Pre-load sync set into memory — O(1) hash lookups instead of O(N) SQL queries.
    // Key is "table_name\0global_row_id" for compact hashing.
    std::unordered_set<std::string> sync_set_cache;
    if (preload) {
        auto rows = db().db().query("SELECT table_name, global_row_id FROM _lattice_sync_set");
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
    if (preload) {
        for (const auto& fe : *config_.sync_filter) {
            LOG_DEBUG("synchronizer", "  filter entry: table=%s where=%s",
                      fe.table_name.c_str(),
                      fe.where_clause ? fe.where_clause->c_str() : "(all rows)");

            std::string sql = "SELECT globalId FROM " + fe.table_name;
            if (fe.where_clause) {
                sql += " WHERE (" + *fe.where_clause + ")";
            }
            auto rows = db().db().query(sql);
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
    }

    // Lambda seams: in-memory caches for bulk batches, indexed per-row SQL
    // lookups for small (steady-state) batches.
    auto cached_sync_set_contains = [&](const std::string& table_name,
                                         const std::string& global_row_id) -> bool {
        if (preload) return sync_set_cache.count(table_name + '\0' + global_row_id) > 0;
        return sync_set_contains(table_name, global_row_id);
    };
    auto cached_row_matches_filter = [&](const std::string& table_name,
                                          const std::string& global_row_id) -> bool {
        if (preload) return filter_matches.count(table_name + '\0' + global_row_id) > 0;
        return row_matches_filter(table_name, global_row_id);
    };
    // Keep sync_set_add/remove updating both the cache (when active) and the DB.
    auto cached_sync_set_add = [&](const std::string& table_name,
                                    const std::string& global_row_id) {
        if (preload) sync_set_cache.insert(table_name + '\0' + global_row_id);
        sync_set_add(table_name, global_row_id);
    };
    auto cached_sync_set_remove = [&](const std::string& table_name,
                                       const std::string& global_row_id) {
        if (preload) sync_set_cache.erase(table_name + '\0' + global_row_id);
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
            // Filter-removal DELETEs stop at the device's own synced DB: over
            // IPC they remove the row from the local synced mirror, but they
            // must never ride the WSS uplink — the server would relay them to
            // the user's other devices and destroy their local copies.
            bool is_filter_removal =
                std::find(entry.changed_fields_names.begin(),
                          entry.changed_fields_names.end(),
                          "__lattice_filter_removal") != entry.changed_fields_names.end();
            if (is_filter_removal && config_.sync_id.rfind("wss:", 0) == 0) {
                // Still leave the shared set so a later filter-widen re-syncs
                // the row via reconcile Phase 2.
                cached_sync_set_remove(entry.table_name, entry.global_row_id);
                result.to_mark_synced.push_back(entry.id);
            }
            // Inline classify_delete with cached lookups
            else if (cached_sync_set_contains(entry.table_name, entry.global_row_id)) {
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
                del.global_id = db().generate_global_id();
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

// Number of sync channels an audit entry must be marked synced for before it
// collapses to AuditLog.isSynchronized=1. Uses the LIVE replication-slot
// registry: the config snapshot (all_active_sync_ids) goes stale when a
// configured sync never actually runs — production accumulated thousands of
// marked-but-uncollapsed state rows (and a permanently wrong passive pending
// count) because a snapshot listed a channel that never registered. Newly
// configured syncs that haven't registered yet don't need audit-history
// waiting: they bootstrap via reconcile/replay, not audit replay.
static int64_t required_sync_count_for_collapse(lattice_db& db,
                                                const std::vector<std::string>& all_active_sync_ids) {
    auto rows = db.db().query("SELECT COUNT(*) AS cnt FROM _lattice_replication_slots", {});
    int64_t live = 0;
    if (!rows.empty()) {
        auto it = rows[0].find("cnt");
        if (it != rows[0].end() && std::holds_alternative<int64_t>(it->second)) {
            live = std::get<int64_t>(it->second);
        }
    }
    if (live >= 1) return live;
    return std::max<int64_t>(1, static_cast<int64_t>(all_active_sync_ids.size()));
}

void synchronizer_base::mark_skipped_synced(const std::vector<int64_t>& to_mark_synced) {
    if (to_mark_synced.empty()) return;

    // Mark entries that were skipped as synchronized in the per-sync-id table.
    // Must use _lattice_sync_state (not global AuditLog.isSynchronized) because
    // query_audit_log_for_sync uses _lattice_sync_state to find pending entries.
    // Chunk into transactions of 50 to avoid holding the write lock too long.
    constexpr size_t kMarkChunkSize = 50;
    for (size_t i = 0; i < to_mark_synced.size(); i += kMarkChunkSize) {
        size_t end = std::min(i + kMarkChunkSize, to_mark_synced.size());
        db().db().begin_transaction();
        try {
            for (size_t j = i; j < end; ++j) {
                int64_t entry_id = to_mark_synced[j];
                db().db().execute(
                    "INSERT INTO _lattice_sync_state (audit_entry_id, sync_id, is_synchronized) "
                    "VALUES (?, ?, 1) "
                    "ON CONFLICT(audit_entry_id, sync_id) DO UPDATE SET is_synchronized = 1",
                    {entry_id, config_.sync_id});

                // Eager cleanup: if all active sync_ids have synced this entry,
                // collapse to isSynchronized=1 on AuditLog and remove sync_state rows.
                auto count_rows = db().db().query(
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

                if (synced_count >= required_sync_count_for_collapse(db(), config_.all_active_sync_ids)) {
                    db().db().execute(
                        "DELETE FROM _lattice_sync_state WHERE audit_entry_id = ?",
                        {entry_id});
                    db().db().execute(
                        "UPDATE AuditLog SET isSynchronized = 1 WHERE id = ?",
                        {entry_id});
                }
            }
            db().db().commit();
        } catch (...) {
            if (db().db().is_in_transaction()) {
                try { db().db().rollback(); } catch (...) {}
            }
            throw;
        }
    }

    // No progress decrement needed — skipped entries are not counted in
    // progress_pending_upload_ (only to_send entries are counted).

    // Skipped entries are resolved for the upload floor: their sync_state
    // transactions committed above, so they can never be pending again.
    if (config_.use_upload_floor && !to_mark_synced.empty()) {
        resolve_audit_ids(to_mark_synced);
    }
}

void synchronizer_base::resolve_audit_ids(const std::vector<int64_t>& audit_ids) {
    int64_t new_floor = 0;
    {
        std::lock_guard<std::mutex> lock(in_flight_mutex_);
        for (auto id : audit_ids) {
            if (id > 0) open_audit_ids_.erase(id);
        }
        if (open_audit_ids_.empty()) {
            new_floor = last_enumerated_id_;
            stall_min_open_ = -1;
        } else {
            new_floor = *open_audit_ids_.begin() - 1;
            // Poison-entry observability: an entry that never resolves pins
            // the floor and silently re-enters every bounded pass. Surface it.
            const auto now = std::chrono::steady_clock::now();
            const int64_t min_open = *open_audit_ids_.begin();
            if (min_open != stall_min_open_) {
                stall_min_open_ = min_open;
                stall_since_ = now;
            } else if (now - stall_since_ >= std::chrono::minutes(5)) {
                LOG_WARN("synchronizer", "[%s] upload floor stalled below audit id %lld for >5min — an entry is repeatedly failing to resolve",
                         config_.sync_id.c_str(), (long long)min_open);
                stall_since_ = now;  // rate-limit the warning
            }
        }
    }
    if (new_floor > 0) {
        advance_upload_floor(db().db(), config_.sync_id, new_floor);
    }
}

void synchronizer_base::send_entries(std::vector<audit_log_entry>& entries) {
    if (entries.empty()) return;

    // Flow control: send at most a small window of chunks per invocation.
    // Blasting the whole backlog (25 x 1MB frames in one burst) overran the
    // server, which applies each frame synchronously on its socket's event
    // loop — production reset the connection mid-burst. Entries beyond the
    // window stay PENDING (not in-flight): when the window's ACKs drain the
    // in-flight set, mark_as_synced re-invokes upload_pending_changes and the
    // next window ships. The ack-timeout resend covers a lost window.
    // The window caps TOTAL in-flight, not sends-per-invocation: against a
    // stalled server every pass used to ship ANOTHER window on top of the
    // un-ACKed ones (in-flight grew by 2*chunk_size per tick, unbounded).
    // With the cap, a stalled transport plateaus at one window and the next
    // ships only as ACKs (or the ack-timeout release) free space.
    const size_t window_cap = config_.chunk_size * 2;
    size_t window_end = 0;

    // Track only the entries actually sent as in-flight, and account progress
    // for exactly the windowed portion: pending mirrors the in-flight set
    // (store, not add — self-healing across reconnect clears), total counts
    // real sends only (a resend after a drop re-counts at most one window,
    // never the whole backlog).
    {
        std::lock_guard<std::mutex> lock(in_flight_mutex_);
        const size_t in_flight = in_flight_ids_.size();
        const size_t space = in_flight >= window_cap ? 0 : window_cap - in_flight;
        window_end = std::min(entries.size(), space);
        for (size_t i = 0; i < window_end; ++i) {
            // Map to the AuditLog id so an ACK can resolve the upload floor;
            // synthetic entries carry id 0 and never gate the floor.
            in_flight_ids_[entries[i].global_id] = entries[i].id;
        }
        progress_pending_upload_.store(
            static_cast<int64_t>(in_flight_ids_.size()), std::memory_order_relaxed);
    }
    if (window_end == 0) {
        LOG_DEBUG("synchronizer", "[%s] send window full (%zu in flight) — %zu entries stay pending",
                  config_.sync_id.c_str(), window_cap, entries.size());
        entries.clear();
        return;
    }
    progress_total_upload_.fetch_add(
        static_cast<int64_t>(window_end), std::memory_order_relaxed);
    fire_progress();
    if (window_end < entries.size()) {
        LOG_INFO("synchronizer", "[%s] send window: %zu of %zu entries (rest pend on ACKs)",
                 config_.sync_id.c_str(), window_end, entries.size());
    }

    for (size_t i = 0; i < window_end; i += config_.chunk_size) {
        // Bail out early if connection dropped mid-send — remaining chunks
        // will be re-queried and sent on the next successful connection.
        if (!is_connected_) {
            LOG_INFO("synchronizer", "send_entries: connection lost after %zu/%zu entries, stopping",
                     i, entries.size());
            break;
        }

        size_t end = std::min(i + config_.chunk_size, window_end);
        std::vector<audit_log_entry> chunk(entries.begin() + i, entries.begin() + end);

        auto event = server_sent_event::make_audit_log(chunk);
        auto json_str = event.to_json();
        LOG_DEBUG("synchronizer", "Sending %zu entries to server: %s",
                  chunk.size(), json_str.substr(0, 200).c_str());
        ws_client_->send(transport_message::from_binary({json_str.begin(), json_str.end()}));
    }
    entries.resize(window_end);

    // At-least-once delivery: a sent frame can vanish without any error —
    // e.g. the peer registers its frame handlers a beat after the upgrade
    // completes (WebSocketKit discards unhandled frames), or plain network
    // loss. If these entries are still unACKed after the timeout, release
    // them from in-flight and re-upload; applies are idempotent
    // (ON CONFLICT(globalId) DO UPDATE), so re-delivery is safe.
    //
    // Runs on a detached thread, NOT the scheduler: ACK processing is
    // serialized through the (single-threaded) scheduler, so a blocking
    // wait there would deadlock its own exit condition. Lifetime is
    // guarded by ack_guard_ (see its declaration).
    //
    // Not on Emscripten: the WASM build has no pthreads. Browser clients
    // forgo the client-side resend — a dropped frame is recovered on the
    // next reconnect (tab visibility / socket cycle) instead.
#ifndef __EMSCRIPTEN__
    std::vector<std::string> sent_ids;
    sent_ids.reserve(entries.size());
    for (const auto& e : entries) sent_ids.push_back(e.global_id);
    std::thread([guard = ack_guard_, self = this, sent_ids = std::move(sent_ids)] {
        // Consecutive-failure backoff: a server that stalls (accepts frames,
        // never ACKs) must not be re-hammered with the same window every 10s
        // while each resend pass re-queries the audit log. 10s, 20s, 40s...
        // capped at 5 min; reset to 10s by any ACK (mark_as_synced).
        constexpr auto kAckTimeoutBase = std::chrono::seconds(10);
        constexpr auto kAckTimeoutMax = std::chrono::minutes(5);
        const int failures = self->ack_resend_failures_.load(std::memory_order_relaxed);
        const auto timeout = std::min<std::chrono::steady_clock::duration>(
            kAckTimeoutBase * (int64_t(1) << std::min(failures, 5)), kAckTimeoutMax);
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            {
                std::lock_guard<std::mutex> g(guard->m);
                if (!guard->alive) return;
                std::lock_guard<std::mutex> lock(self->in_flight_mutex_);
                bool any = false;
                for (const auto& id : sent_ids) {
                    if (self->in_flight_ids_.count(id)) { any = true; break; }
                }
                if (!any) return;  // everything ACKed
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        std::lock_guard<std::mutex> g(guard->m);
        if (!guard->alive || !self->is_connected_) return;
        size_t released = 0;
        {
            std::lock_guard<std::mutex> lock(self->in_flight_mutex_);
            for (const auto& id : sent_ids) released += self->in_flight_ids_.erase(id);
            self->progress_pending_upload_.store(
                static_cast<int64_t>(self->in_flight_ids_.size()), std::memory_order_relaxed);
        }
        if (released > 0) {
            const int f = self->ack_resend_failures_.fetch_add(1, std::memory_order_relaxed) + 1;
            LOG_WARN("synchronizer", "[%s] %zu entries unACKed after timeout — resending (consecutive failures: %d)",
                     self->config_.sync_id.c_str(), released, f);
            self->request_upload();
        }
    }).detach();
#endif  // !__EMSCRIPTEN__
}

void synchronizer_base::upload_pending_changes() {
    LOG_DEBUG("synchronizer", "upload_pending_changes called");
    if (!is_connected_) {
        LOG_DEBUG("synchronizer", "upload_pending_changes: not connected, skipping");
        return;
    }
    auto entries = query_pending_entries();
    if (entries.empty()) return;
    auto classified = classify_entries(entries);

    // Progress accounting happens in send_entries AFTER windowing: counting
    // the full classified backlog here (pre-window) meant `pending` never
    // returned to 0 during a multi-window catch-up — every pass re-counted
    // the un-sent remainder, inflating pending/total unboundedly (observed
    // 180MB "pending"), permanently blocking drain()'s pending==0 exit and
    // any idle-gated work. The invariant is now: pending == in-flight
    // (sent-but-unACKed) exactly; total counts only entries actually sent.

    mark_skipped_synced(classified.to_mark_synced);
    send_entries(classified.to_send);

    // No tail-call: requests that arrived during this pass were routed
    // through request_upload(), which either queued its own scheduler pass
    // (leading edge / legacy mode) or armed the pacer's trailing-edge tick.
    // The old zero-delay tail-call was one of four sites that, combined,
    // busy-spun the daemon.
}

std::vector<std::string> synchronizer_base::apply_remote_changes(const std::vector<audit_log_entry>& entries) {
    auto applied = lattice::apply_remote_changes_for(db(), entries, config_.sync_id);
    progress_received_.fetch_add(static_cast<int64_t>(applied.size()), std::memory_order_relaxed);
    fire_progress();
    return applied;
}

void synchronizer_base::mark_as_synced(const std::vector<std::string>& global_ids) {
    LOG_INFO("synchronizer", "[%s] mark_as_synced: %zu entries ACK'd (progress_acked was %lld)",
             config_.sync_id.c_str(), global_ids.size(),
             (long long)progress_acked_.load(std::memory_order_relaxed));
    mark_audit_entries_synced_for(db(), global_ids, config_.sync_id, config_.all_active_sync_ids);

    // Remove ACK'd entries from in-flight set; pending mirrors the set size
    // (store, not fetch_sub — an ACK straddling a reconnect clear must not
    // drive the counter negative). Capture the resolved audit ids while the
    // map still has them: mark_audit_entries_synced_for's transactions have
    // already committed above, so advancing the floor past them is safe.
    std::vector<int64_t> resolved_audit_ids;
    {
        std::lock_guard<std::mutex> lock(in_flight_mutex_);
        for (const auto& id : global_ids) {
            auto it = in_flight_ids_.find(id);
            if (it != in_flight_ids_.end()) {
                if (it->second > 0) resolved_audit_ids.push_back(it->second);
                in_flight_ids_.erase(it);
            }
        }
        progress_pending_upload_.store(
            static_cast<int64_t>(in_flight_ids_.size()), std::memory_order_relaxed);
    }
    if (config_.use_upload_floor) {
        resolve_audit_ids(resolved_audit_ids);
    }

    progress_acked_.fetch_add(static_cast<int64_t>(global_ids.size()), std::memory_order_relaxed);
    fire_progress();

    // After processing ACKs, check if more pending entries exist that aren't
    // already in-flight. Without this, entries created during sync (e.g. from
    // WSS relay) stall indefinitely since the observer only fires on new INSERTs.
    bool should_upload = false;
    {
        std::lock_guard<std::mutex> lock(in_flight_mutex_);
        should_upload = in_flight_ids_.empty();
    }
    // ACKs mean the server is alive and consuming — reset the resend backoff.
    ack_resend_failures_.store(0, std::memory_order_relaxed);
    if (should_upload) {
        request_upload();
    }
}

void synchronizer_base::maybe_reset_backoff_after_stable_connection(bool was_open) {
    if (!was_open) return;  // failed connect attempt — stale last_open_time_ms_
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    const auto open_ms = last_open_time_ms_.load(std::memory_order_relaxed);
    if (open_ms > 0 && now_ms - open_ms >= config_.stable_connection_ms) {
        const int prior = reconnect_attempts_.exchange(0);
        if (prior > 0) {
            LOG_INFO("synchronizer", "[%s] connection was stable %llds — backoff reset (attempts were %d)",
                     config_.sync_id.c_str(), (long long)((now_ms - open_ms) / 1000), prior);
        }
    }
}

void synchronizer_base::schedule_reconnect() {
    bool within_limit = config_.max_reconnect_attempts == 0
        || reconnect_attempts_ < config_.max_reconnect_attempts;
    if (!(should_reconnect_ && !is_connected_ && within_limit)) {
        LOG_INFO("synchronizer", "[%s] schedule_reconnect: NOT reconnecting (should=%d connected=%d within_limit=%d)",
                 config_.sync_id.c_str(), should_reconnect_ ? 1 : 0, is_connected_ ? 1 : 0, within_limit ? 1 : 0);
    }
    if (should_reconnect_ && !is_connected_ && within_limit) {
#ifdef __EMSCRIPTEN__
        // The browser build is single-threaded (immediate_scheduler runs
        // inline on the main thread): the blocking backoff below would freeze
        // the tab AND prevent the async WebSocket handshake from ever
        // completing. Browser clients reconnect at the app layer (tab
        // visibility / reload) instead.
        LOG_INFO("synchronizer", "[%s] schedule_reconnect: skipped on Emscripten (app-level reconnect)",
                 config_.sync_id.c_str());
        return;
#endif
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

std::optional<std::string> synchronizer_base::get_last_received_event_id() {
    auto rows = db().db().query(
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

synchronizer_base::sync_progress synchronizer_base::get_progress() const {
    return {
        progress_pending_upload_.load(std::memory_order_relaxed),
        progress_total_upload_.load(std::memory_order_relaxed),
        progress_acked_.load(std::memory_order_relaxed),
        progress_received_.load(std::memory_order_relaxed)
    };
}

void synchronizer_base::set_on_progress(on_progress_handler handler) {
    LOG_INFO("synchronizer", "[%s] set_on_progress: handler=%s (this=%p, db=%s)",
             config_.sync_id.c_str(),
             handler ? "SET" : "CLEARED",
             (void*)this, db().config().path.c_str());
    std::lock_guard<std::mutex> lock(progress_handler_mutex_);
    on_progress_ = std::move(handler);
}

void synchronizer_base::fire_progress() {
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
// Platform-specific synchronizer subclass constructors
// ============================================================================

#ifdef __EMSCRIPTEN__

synchronizer::synchronizer(lattice_db& db_ref, const sync_config& config)
{
    db_ptr_ = &db_ref;
    auto sched = db_ref.get_scheduler() ? db_ref.get_scheduler() : std::make_shared<immediate_scheduler>();
    init_sync(config, sched);
}

synchronizer::synchronizer(lattice_db& db_ref, const sync_config& config,
                           std::unique_ptr<sync_transport> transport)
{
    db_ptr_ = &db_ref;
    auto sched = db_ref.get_scheduler() ? db_ref.get_scheduler() : std::make_shared<immediate_scheduler>();
    init_sync(config, sched, std::move(transport));
}

#else

synchronizer::synchronizer(std::unique_ptr<lattice_db> db, const sync_config& config)
{
    owned_db_ = std::move(db);
    db_ptr_ = owned_db_.get();
    auto sched = owned_db_->get_scheduler() ? owned_db_->get_scheduler() : std::make_shared<immediate_scheduler>();
    init_sync(config, sched);
}

synchronizer::synchronizer(std::unique_ptr<lattice_db> db, const sync_config& config,
                           std::unique_ptr<sync_transport> transport)
{
    owned_db_ = std::move(db);
    db_ptr_ = owned_db_.get();
    auto sched = owned_db_->get_scheduler() ? owned_db_->get_scheduler() : std::make_shared<immediate_scheduler>();
    init_sync(config, sched, std::move(transport));
}

#endif

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
// be on other instances (different isolation contexts). Builds a single
// batched event vector so all UPDATEs from this synchronizer pass reach
// each observer in one fire — keeps wire-relay framing aligned with the
// rest of the notification path (see notify_changes_batched).
static void notify_observers(lattice_db& db,
                             const std::vector<std::pair<int64_t, std::string>>& notify_list) {
    if (notify_list.empty()) return;

    std::vector<lattice_db::change_event> events;
    events.reserve(notify_list.size());
    for (const auto& [row_id, gid] : notify_list) {
        events.emplace_back("AuditLog", "UPDATE", row_id, gid, "");
    }

    if (db.config().is_in_memory()) {
        db.notify_changes_batched(events);
    } else {
        instance_registry::instance().for_each_alive(db.config().path,
            [&events](lattice_db* instance) {
                instance->notify_changes_batched(events);
            });
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

                if (synced_count >= required_sync_count_for_collapse(db, all_active_sync_ids)) {
                    // All registered synchronizers have synced — clean up and
                    // collapse to isSynchronized=1
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
        const std::optional<std::vector<sync_filter_entry>>& sync_filter,
        int64_t min_id_exclusive,
        size_t limit) {
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
    // Build the sync filter predicate (shared by both UNION branches).
    // Pre-filters by table name AND where_clause predicate to avoid loading
    // thousands of irrelevant entries from unrelated tables.
    //
    // For tables with a where_clause, we still pass through:
    //   1. DELETE entries (row is gone — app layer checks sync_set membership)
    //   2. Rows in _lattice_sync_set (may need synthetic DELETE if they left the filter)
    //   3. Rows matching the where_clause predicate
    // This is safe because reconcile_sync_filter() runs BEFORE upload_pending_changes()
    // in on_websocket_open, so sync_set is already up-to-date by the time we query.
    std::string filter_clause;
    if (sync_filter && !sync_filter->empty()) {
        filter_clause = " AND (";
        for (size_t i = 0; i < sync_filter->size(); ++i) {
            if (i > 0) filter_clause += " OR ";
            const auto& entry = (*sync_filter)[i];
            if (!entry.where_clause || entry.where_clause->empty()) {
                filter_clause += "a.tableName = '" + entry.table_name + "'";
            } else {
                filter_clause += "(a.tableName = '" + entry.table_name + "' AND ("
                       "a.operation = 'DELETE'"
                       " OR EXISTS (SELECT 1 FROM _lattice_sync_set"
                       " WHERE table_name = a.tableName AND global_row_id = a.globalRowId)"
                       " OR EXISTS (SELECT 1 FROM " + entry.table_name +
                       " WHERE globalId = a.globalRowId AND (" + *entry.where_clause + "))"
                       "))";
            }
        }
        filter_clause += " OR a.tableName LIKE '\\_%' ESCAPE '\\'";
        filter_clause += ")";
    }

    // UNION query: two branches that each use indexed lookups instead of a
    // full AuditLog scan. The LEFT JOIN formulation forced SQLite to scan every
    // row; splitting into JOIN + NOT EXISTS lets each branch use its own index.
    //
    // Branch 1: entries explicitly marked pending in _lattice_sync_state
    // Branch 2: entries with no sync_state row, using global isSynchronized flag
    //           (uses partial index idx_audit_log_pending_sync)
    // Branch 2 is additionally bounded below by the caller's upload floor
    // (`a.id > ?`): explicit is_synchronized=0 sync_state rows (branch 1)
    // ride idx_sync_state_pending and are ~empty in practice, so the floor
    // invariant only has to hold for the no-sync-state flow we control. The
    // LIMIT caps a pass at one send window — without it, a stalled server
    // (nothing resolving, floor pinned) still re-reads and JSON-parses the
    // whole backlog every resend cycle.
    std::string sql =
        "SELECT a.* FROM AuditLog a"
        " JOIN _lattice_sync_state ss"
        "   ON ss.audit_entry_id = a.id AND ss.sync_id = ?"
        " WHERE ss.is_synchronized = 0" + filter_clause +

        " UNION ALL"

        " SELECT a.* FROM AuditLog a"
        " WHERE a.isSynchronized = 0"
        " AND a.id > ?"
        " AND NOT EXISTS ("
        "   SELECT 1 FROM _lattice_sync_state ss"
        "   WHERE ss.audit_entry_id = a.id AND ss.sync_id = ?"
        ")" + filter_clause +

        " ORDER BY id ASC";

    std::vector<column_value_t> params = {sync_id, min_id_exclusive, sync_id};
    if (limit > 0) {
        sql += " LIMIT ?";
        params.push_back(static_cast<int64_t>(limit));
    }
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

int64_t read_upload_floor(database& db, const std::string& sync_id) {
    auto rows = db.query(
        "SELECT upload_floor FROM _lattice_replication_slots WHERE sync_id = ?",
        {sync_id});
    if (rows.empty()) return 0;
    auto it = rows[0].find("upload_floor");
    if (it != rows[0].end() && std::holds_alternative<int64_t>(it->second)) {
        return std::get<int64_t>(it->second);
    }
    return 0;
}

void advance_upload_floor(database& db, const std::string& sync_id, int64_t floor) {
    // Monotonic: a stale synchronizer instance (accept-side replacement race)
    // can only under-advance, never regress the floor.
    db.execute(R"(
        UPDATE _lattice_replication_slots
        SET upload_floor = MAX(upload_floor, ?)
        WHERE sync_id = ?
    )", {floor, sync_id});
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

    // Signal flush_changes to look up changedFieldsNames from AuditLog for
    // object observers. Without this, IPC sync changes bypass Swift setters
    // and the Observation registrar is never triggered.
    db.applying_remote_changes_.store(true, std::memory_order_release);

    // Process in chunks to avoid holding the write lock for too long.
    const size_t chunk_size = 50;

    // Restore the user's persistent disabled flag after each chunk rather
    // than hardcoding 0 — deliberately disabled auditing must stay disabled.
    const int64_t prev_disabled = db.read_sync_disabled_flag();

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
                    bool is_virtual = std::find(entry.changed_fields_names.begin(),
                                                entry.changed_fields_names.end(),
                                                "rhs_type") != entry.changed_fields_names.end();
                    if (is_virtual) {
                        db.ensure_virtual_link_table(entry.table_name);
                    } else {
                        db.ensure_link_table(entry.table_name);
                    }
                }

                // Ensure vec0 tables exist for vector columns BEFORE the INSERT,
                // so triggers can populate the shadow tables.
                if (!entry.table_name.empty() && entry.table_name[0] != '_') {
                    auto* model_schema = schema_registry::instance().get_schema(entry.table_name);
                    if (model_schema) {
                        for (const auto& prop : model_schema->properties) {
                            if (prop.is_vector && prop.type == column_type::blob) {
                                auto it = entry.changed_fields.find(prop.name);
                                if (it != entry.changed_fields.end() &&
                                    it->second.kind == any_property_kind::data_kind &&
                                    std::holds_alternative<std::vector<uint8_t>>(it->second.value)) {
                                    auto& vec_data = std::get<std::vector<uint8_t>>(it->second.value);
                                    if (!vec_data.empty()) {
                                        int dims = static_cast<int>(vec_data.size() / sizeof(float));
                                        if (dims > 0) {
                                            db.ensure_vec0_table(entry.table_name, prop.name, dims);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Generate and execute the SQL instruction
                LOG_INFO("apply_remote", "entry[%zu]: table=%s op=%s globalRowId=%s changedFields=%zu changedFieldsNames=%zu",
                         i, entry.table_name.c_str(), entry.operation.c_str(),
                         entry.global_row_id.c_str(),
                         entry.changed_fields.size(), entry.changed_fields_names.size());
                for (const auto& [key, val] : entry.changed_fields) {
                    LOG_INFO("apply_remote", "  field: %s kind=%d", key.c_str(), static_cast<int>(val.kind));
                }
                for (const auto& name : entry.changed_fields_names) {
                    LOG_INFO("apply_remote", "  fieldName: %s", name.c_str());
                }
                auto schema = db.get_table_schema(entry.table_name);
                auto [sql, params] = entry.generate_instruction(schema);
                LOG_INFO("apply_remote", "  SQL: %s (params=%zu)", sql.c_str(), params.size());

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

            // Re-enable sync triggers (restore the pre-existing flag value)
            db.db().execute("UPDATE _SyncControl SET disabled = ? WHERE id = 1", {prev_disabled});

            db.db().commit();
        } catch (...) {
            db.db().rollback();
            db.applying_remote_changes_.store(false, std::memory_order_release);
            throw;
        }
    }

    db.applying_remote_changes_.store(false, std::memory_order_release);

    // vec0 INSERT/UPDATE/DELETE triggers fire inside the transaction above on
    // THIS connection, so the shadow table is already up-to-date. Other
    // connections will reconcile lazily via knn_query's count-mismatch check.

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
