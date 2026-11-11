#include "sync_immediate_scheduler.hpp"
#include "lattice/sync.hpp"
#include "lattice/lattice.hpp"
#include <nlohmann/json.hpp>
#include <algorithm>
#include <cctype>
#include <sstream>
#include <cmath>
#include <iomanip>
#include <string_view>
#include <stdexcept>
#include <sys/stat.h>

#include "lattice/sync_delivery.hpp"

namespace lattice {

namespace test_hooks {
std::function<void(size_t chunk_start)> apply_chunk_begin;
std::function<void()> sync_outbox_after_insert;
}  // namespace test_hooks

// Keep the complete chunk's SQLite ownership private to Core. Notifications
// drain only after its mutex/store gate are released by the common helper.
struct sync_apply_transaction_access {
    template<typename F>
    static void run(lattice_db& db, F&& body) {
        db.with_sync_suppressed_transaction_([&]() -> int64_t {
            std::forward<F>(body)();
            return 0;
        });
    }
};

using json = nlohmann::json;

namespace {
// Core emits lowercase UUIDs; Foundation's UUID encoder emits uppercase.
// Compare canonical UUID keys without changing wire bytes or opaque IDs.
bool is_sync_uuid(const std::string& value) {
    if (value.size() != 36) return false;
    for (size_t index = 0; index < value.size(); ++index) {
        if (index == 8 || index == 13 || index == 18 || index == 23) {
            if (value[index] != '-') return false;
        } else {
            const char c = value[index];
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')))
                return false;
        }
    }
    return true;
}

std::string sync_event_key(const std::string& value) {
    std::string key = value;
    if (is_sync_uuid(key))
        for (auto& c : key) if (c >= 'A' && c <= 'F') c += 'a' - 'A';
    return key;
}

std::vector<std::string> no_history_columns(database& db, const std::string& table_name);
void mark_no_history_nulls(audit_log_entry& entry, const std::vector<std::string>& columns);
}


namespace detail {
namespace {
struct channel_registry {
    std::recursive_mutex mutex;
    std::map<std::pair<std::string,std::string>,size_t> lifetimes;
    std::set<std::pair<std::string,std::string>> retiring;
};
channel_registry& channel_lifetimes() {static auto* registry=new channel_registry;return *registry;}
std::string channel_store_identity(lattice_db& store,bool require_verified_file) {
    auto& db=store.db();
    if(db.is_closed()) throw db_error("Cannot retire sync state on a closed database");
    if(store.config().is_in_memory()) {
        // SQLite's pager owns this sqlite3_file object. Named shared-cache connections share
        // that pager; private memory stores have separate objects even with the same URI text.
        sqlite3_file* file=nullptr;
        if(sqlite3_file_control(db.handle(),"main",SQLITE_FCNTL_FILE_POINTER,&file)!=SQLITE_OK || !file)
            throw db_error("Cannot establish in-memory sync store identity");
        return "memory-pager:"+std::to_string(reinterpret_cast<uintptr_t>(file));
    }
    const char* filename=sqlite3_db_filename(db.handle(),"main");
    struct stat before{},after{};int moved=0;
    const bool first_stat=filename && *filename && ::stat(filename,&before)==0;
    const int control=sqlite3_file_control(db.handle(),"main",SQLITE_FCNTL_HAS_MOVED,&moved);
    if(control==SQLITE_NOTFOUND && !require_verified_file) {
        // Unsupported VFSes retain ordinary sync. Destructive retirement/rewind refuses because
        // a path-only key could overlook another handle to the same store through an alias.
        sqlite3_file* file=nullptr;
        if(sqlite3_file_control(db.handle(),"main",SQLITE_FCNTL_FILE_POINTER,&file)!=SQLITE_OK || !file)
            throw db_error("Cannot establish sync handle identity");
        return "unverified-pager:"+std::to_string(reinterpret_cast<uintptr_t>(file));
    }
    if(!first_stat || control!=SQLITE_OK || moved ||
       ::stat(filename,&after)!=0 || before.st_dev!=after.st_dev || before.st_ino!=after.st_ino)
        throw db_error("Cannot establish unchanged admitted sync store identity");
    // Device/inode makes ordinary path, symlink and hard-link aliases share one reservation.
    // Out-of-band file replacement is refused; this is not a cross-process lock or rename sandbox.
    return "file:"+std::to_string(static_cast<uint64_t>(after.st_dev))+":"+std::to_string(static_cast<uint64_t>(after.st_ino));
}
}
struct channel_lifetime_token {
    std::pair<std::string,std::string> key;
    bool registered=false;
    explicit channel_lifetime_token(std::pair<std::string,std::string> value):key(std::move(value)) {}
    ~channel_lifetime_token() {
        if(!registered) return;
        auto& registry=channel_lifetimes();std::lock_guard<std::recursive_mutex> lock(registry.mutex);
        const auto found=registry.lifetimes.find(key);
        if(found!=registry.lifetimes.end() && --found->second==0) registry.lifetimes.erase(found);
    }
};
std::shared_ptr<void> acquire_sync_channel_lifetime(lattice_db& store,const std::string& sync_id) {
    (void)sync_reset_meta_key(sync_id);
    // Complete control-block allocation before registration. Exactly one destructor unregisters,
    // including allocation failure; no catch path can double-decrement another owner's count.
    auto token=std::make_shared<channel_lifetime_token>(std::make_pair(channel_store_identity(store,false),sync_id));
    auto& registry=channel_lifetimes();std::lock_guard<std::recursive_mutex> lock(registry.mutex);
    if(registry.retiring.count(token->key)) throw db_error("Sync channel retirement is in progress");
    ++registry.lifetimes[token->key];token->registered=true;
    return token;
}
void with_quiescent_sync_channel(lattice_db& store,const std::string& sync_id,const std::function<void()>& operation) {
    (void)sync_reset_meta_key(sync_id);
    const auto key=std::make_pair(channel_store_identity(store,true),sync_id);
    auto& registry=channel_lifetimes();
    {
        std::lock_guard<std::recursive_mutex> lock(registry.mutex);
        if(registry.lifetimes.count(key) || registry.retiring.count(key))
            throw db_error("Sync channel is still active; destroy its synchronizers before resetting or retiring state");
        registry.retiring.insert(key);
    }
    // Never hold the registry mutex while taking the store's write gate or SQLite locks.
    // The reservation blocks new lifetime admission until the operation commits or rolls back.
    auto release=[&] {std::lock_guard<std::recursive_mutex> lock(registry.mutex);registry.retiring.erase(key);};
    try {operation();release();} catch(...) {release();throw;}
}
} // namespace detail

static std::atomic<int64_t> g_sync_instance_count{0};
static std::vector<int64_t> mark_audit_entries_synced_for_impl(lattice_db&,
    const std::vector<std::string>&, const std::string&, const std::vector<std::string>&,
    const std::function<bool(int64_t, const std::string&)>&);

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

    // Union storage has a discriminator named `case`, a SQL keyword. Quote
    // keywords and unusual identifiers while retaining the ordinary statement
    // shape for existing callers. Values are still bound separately below.
    auto identifier = [](const std::string& name) {
        bool plain = !name.empty() && (std::isalpha(static_cast<unsigned char>(name[0])) || name[0] == '_');
        for (char c : name) plain = plain && (std::isalnum(static_cast<unsigned char>(c)) || c == '_');
        if (plain && !sqlite3_keyword_check(name.data(), static_cast<int>(name.size()))) return name;
        std::string quoted = "\"";
        for (char c : name) { quoted += c; if (c == '"') quoted += c; }
        return quoted + "\"";
    };

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
            col_names += ", " + identifier(cols[i]);
            placeholders += ", " + placeholder(cols[i]);
        }

        // Build UPDATE SET clause
        std::string update_set;
        for (size_t i = 1; i < cols.size(); ++i) {
            if (i > 1) update_set += ", ";
            update_set += identifier(cols[i]) + "=excluded." + identifier(cols[i]);
        }

        std::string sql = "INSERT INTO " + identifier(table_name) + "(" + col_names + ") VALUES (" + placeholders + ")";
        if (!update_set.empty()) {
            // Value-guarded upsert: the DO UPDATE arm fires only when some
            // incoming value actually differs (IS NOT is NULL-safe; excluded.*
            // already carries the decoded value, so blob columns compare
            // BLOB-vs-BLOB with no re-binding). A value-identical redundant
            // delivery then reports changes()==0 and the apply path can
            // suppress its relay mint — the Aug 2026 flood minted 2.3M audit
            // rows from exactly these no-op re-applies. Row-level triggers
            // (FTS5/vec0 shadows) also skip, which is consistent: identical
            // values mean the shadows are already correct.
            std::string change_pred;
            for (size_t i = 1; i < cols.size(); ++i) {
                if (i > 1) change_pred += " OR ";
                change_pred += identifier(cols[i]) + " IS NOT excluded." + identifier(cols[i]);
            }
            sql += " ON CONFLICT(globalId) DO UPDATE SET " + update_set +
                   " WHERE " + change_pred;
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

        std::string set_clause = identifier(changed_fields_names[0]) + " = " + placeholder(changed_fields_names[0]);
        for (size_t i = 1; i < changed_fields_names.size(); ++i) {
            set_clause += ", " + identifier(changed_fields_names[i]) + " = " + placeholder(changed_fields_names[i]);
        }

        // Value-guarded update: only fire when some value actually differs,
        // so changes()==0 identifies a genuine no-op. The guard re-binds the
        // same values through placeholder() — a blob column whose wire value
        // is a hex string must compare via unhex(?) (BLOB-vs-BLOB); a bare ?
        // would compare BLOB IS NOT TEXT-hex, always true, and every
        // embedding-bearing UPDATE would look like a change forever.
        std::string change_pred;
        for (size_t i = 0; i < changed_fields_names.size(); ++i) {
            if (i > 0) change_pred += " OR ";
            change_pred += identifier(changed_fields_names[i]) + " IS NOT " +
                           placeholder(changed_fields_names[i]);
        }
        std::string sql = "UPDATE " + identifier(table_name) + " SET " + set_clause +
                          " WHERE globalId = ? AND (" + change_pred + ")";

        // Params in SQL order: SET values, globalId, then the guard's
        // re-bound copies of the same values.
        for (const auto& col : changed_fields_names) {
            auto it = changed_fields.find(col);
            if (it != changed_fields.end()) {
                params.push_back(it->second.to_column_value());
            } else {
                params.push_back(nullptr);
            }
        }
        params.push_back(global_row_id);
        for (const auto& col : changed_fields_names) {
            auto it = changed_fields.find(col);
            if (it != changed_fields.end()) {
                params.push_back(it->second.to_column_value());
            } else {
                params.push_back(nullptr);
            }
        }

        return {sql, params};

    } else if (operation == "DELETE") {
        std::string sql = "DELETE FROM " + identifier(table_name) + " WHERE globalId = ?";
        params.push_back(global_row_id);
        return {sql, params};
    }

    return {"", {}};
}

// Build the json OBJECT once — to_json() dumps it, and callers embedding
// entries into a larger payload (server_sent_event::to_json) use it
// directly instead of dump-then-reparse per entry.
static json entry_to_json_obj(const audit_log_entry& e);

// These are transport identities and the operations this receiver implements,
// independent of local schema knowledge. Schema-skew no-ops are still valid.
static bool no_history_null_metadata_valid(const audit_log_entry& entry) {
    if (!entry.no_history_nulls.empty()) {
        if (entry.operation != "INSERT" && entry.operation != "UPDATE") return false;
        std::unordered_set<std::string> seen;
        for (const auto& name : entry.no_history_nulls) {
            const auto value = entry.changed_fields.find(name);
            if (name.empty() || !seen.insert(name).second ||
                std::find(entry.changed_fields_names.begin(), entry.changed_fields_names.end(), name)
                    == entry.changed_fields_names.end() ||
                value == entry.changed_fields.end() ||
                value->second.kind != any_property_kind::null_kind ||
                !std::holds_alternative<std::nullptr_t>(value->second.value)) return false;
        }
    }
    return true;
}

static bool remote_entry_envelope_valid(const audit_log_entry& entry) {
    // This namespace belongs to the local receipt primitive, never a peer.
    // Check before link-table creation and schema-skew/dedup ACK shortcuts.
    // Match case-insensitively even inside a quoted or qualified identifier.
    std::string table = entry.table_name;
    for (auto& c : table) if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
    if (table.find("_lattice_local_metadata") != std::string::npos) return false;
    return no_history_null_metadata_valid(entry) &&
           !entry.global_id.empty() && !entry.table_name.empty() &&
           !entry.global_row_id.empty() &&
           (entry.operation == "INSERT" || entry.operation == "UPDATE" ||
            entry.operation == "DELETE");
}

static std::optional<audit_log_entry> entry_from_parsed(const json& j);

std::string audit_log_entry::to_json() const {
    return entry_to_json_obj(*this).dump();
}

static json entry_to_json_obj(const audit_log_entry& entry) {
    if (!no_history_null_metadata_valid(entry))
        throw std::invalid_argument("Invalid noHistoryNulls completeness metadata");
    const auto& id = entry.id;
    const auto& global_id = entry.global_id;
    const auto& table_name = entry.table_name;
    const auto& operation = entry.operation;
    const auto& row_id = entry.row_id;
    const auto& global_row_id = entry.global_row_id;
    const auto& changed_fields = entry.changed_fields;
    const auto& changed_fields_names = entry.changed_fields_names;
    const auto& timestamp = entry.timestamp;
    const auto& is_from_remote = entry.is_from_remote;
    const auto& is_synchronized = entry.is_synchronized;
    const auto& synthesized = entry.synthesized;
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
    if (synthesized) {
        // Optional key — only on the wire when set. Old receivers ignore it.
        j["synthesized"] = true;
    }
    if (!entry.no_history_nulls.empty()) j["noHistoryNulls"] = entry.no_history_nulls;

    return j;
}

std::optional<audit_log_entry> audit_log_entry::from_json(const std::string& json_str) {
    try {
        return entry_from_parsed(json::parse(json_str));
    } catch (...) {
        return std::nullopt;
    }
}

// The parse-once body: server_sent_event::from_json hands each auditLog
// array element here DIRECTLY. It used to entry_json.dump() and re-parse —
// every entry on the wire was parsed twice (and its changedFields a third
// time), which is pure overhead on the exact path catch-up hammers.
static std::optional<audit_log_entry> entry_from_parsed(const json& j) {
    try {
        if (!j.is_object()) return std::nullopt;
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

        if ((entry.operation == "INSERT" || entry.operation == "UPDATE") &&
            (!j.contains("changedFields") || !j.contains("changedFieldsNames"))) {
            return std::nullopt;
        }

        if (j.contains("noHistoryNulls")) {
            if (!j["noHistoryNulls"].is_array()) return std::nullopt;
            for (const auto& name : j["noHistoryNulls"]) {
                if (!name.is_string()) return std::nullopt;
                entry.no_history_nulls.push_back(name.get<std::string>());
            }
        }

        // Both wire spellings are supported, but malformed JSON or a wrong
        // container/element type must not become an ACKable empty mutation.
        // Decode already-parsed containers by reference (no dump/reparse).
        auto fields = [&](const json& value) {
            if (!value.is_object()) return false;
            for (const auto& [key, item] : value.items()) {
                // Do not let the permissive legacy AnyProperty decoder turn
                // a malformed value into a marked, intentional NULL.
                if (std::find(entry.no_history_nulls.begin(), entry.no_history_nulls.end(), key)
                    != entry.no_history_nulls.end()) {
                    const bool typed_null = item.is_object() && item.contains("kind") &&
                        item["kind"].is_number_integer() && item["kind"] == 4 &&
                        item.contains("value") && item["value"].is_null();
                    if (!item.is_null() && !typed_null) return false;
                }
                entry.changed_fields[key] = json_to_any_property(item);
            }
            return true;
        };
        if (j.contains("changedFields")) {
            const auto& value = j["changedFields"];
            if (value.is_string()) {
                if (!fields(json::parse(value.get<std::string>()))) return std::nullopt;
            } else if (!fields(value)) {
                return std::nullopt;
            }
        }
        auto names = [&](const json& value) {
            if (!value.is_array()) return false;
            for (const auto& item : value) {
                // UPDATE audit triggers leave null slots for unchanged columns. Swift's
                // AuditLog encoder preserves them; they are not malformed field names.
                if (item.is_null()) continue;
                if (!item.is_string()) return false;
                entry.changed_fields_names.push_back(item.get<std::string>());
            }
            return true;
        };
        if (j.contains("changedFieldsNames")) {
            const auto& value = j["changedFieldsNames"];
            if (value.is_string()) {
                if (!names(json::parse(value.get<std::string>()))) return std::nullopt;
            } else if (!names(value)) {
                return std::nullopt;
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
        if (j.contains("synthesized") && j["synthesized"].is_boolean()) {
            entry.synthesized = j["synthesized"].get<bool>();
        }

        if (!remote_entry_envelope_valid(entry)) return std::nullopt;
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
            // Build the object directly — dump-then-reparse doubled the
            // serialization cost of every relayed entry.
            audit_array.push_back(entry_to_json_obj(entry));
        }
        j["auditLog"] = audit_array;
    } else if (event_type == type::floor_reset) {
        j["kind"] = "floorReset";
        if (floor_reset_head) j["floorReset"] = *floor_reset_head;
        j["floorReason"] = floor_reset_reason;
    } else if (event_type == type::nack) {
        j["kind"] = "nack";
        j["nack"] = nacked_ids;
        j["nackReason"] = nack_reason;
    } else if (event_type == type::catch_up_complete) {
        j["kind"] = "catchUpComplete";
        j["catchUpComplete"] = true;
    } else if (event_type == type::replay_request) {
        j["kind"] = "replayRequest";
        j["replayRequest"] = true;
    } else {
        j["kind"] = "ack";
        j["ack"] = acked_ids;
    }

    return j.dump();
}

std::optional<server_sent_event> server_sent_event::from_json_immutable(const std::string& text) {
    try {
        const auto root = json::parse(text);
        if (!root.is_object()) return std::nullopt;
        if (root.contains("auditLog")) {
            if (!root["auditLog"].is_array()) return std::nullopt;
            for (const auto& entry : root["auditLog"]) {
                if (!entry.is_object()) return std::nullopt;
                for (const auto& field : {"globalId", "globalRowId", "tableName", "operation"}) {
                    if (!entry.contains(field) || !entry[field].is_string() || entry[field].get<std::string>().empty()) {
                        return std::nullopt;
                    }
                }
                if (!entry.contains("changedFields") || !entry.contains("changedFieldsNames")) return std::nullopt;
                auto fields = entry["changedFields"];
                auto names = entry["changedFieldsNames"];
                if (fields.is_string()) fields = json::parse(fields.get<std::string>());
                if (names.is_string()) names = json::parse(names.get<std::string>());
                if (!fields.is_object() || !names.is_array()) return std::nullopt;
                // Swift AuditLog stores SQLite UPDATE name lists as [String?]:
                // unchanged columns are null placeholders. Preserve the ordinary
                // parser's compacted names without accepting arbitrary nonstrings
                // or inferring extra writes from the full changedFields dictionary.
                std::unordered_set<std::string> selected;
                for (const auto& name : names) {
                    if (name.is_null() && entry["operation"] == "UPDATE") continue;
                    if (!name.is_string()) return std::nullopt;
                    const auto column = name.get<std::string>();
                    if (column.empty() || !fields.contains(column) || !selected.insert(column).second) return std::nullopt;
                }
                if (entry["operation"] == "UPDATE" && selected.empty()) return std::nullopt;
                for (const auto& [key, value] : fields.items()) {
                    if (value.is_array()) return std::nullopt;
                    if (!value.is_object()) continue;
                    if (!value.contains("kind") || !value["kind"].is_number_integer()) return std::nullopt;
                    const int kind = value["kind"].get<int>();
                    if (kind < 0 || kind > 7) return std::nullopt;
                    if (kind == 4) continue;
                    if (!value.contains("value")) return std::nullopt;
                    const auto& v = value["value"];
                    if ((kind == 2 || kind == 6) ? !v.is_string() : !v.is_number()) return std::nullopt;
                }
                if (!audit_log_entry::from_json(entry.dump())) return std::nullopt;
            }
        }
        auto result = from_json(text);
        if (result && root.contains("auditLog") && result->audit_logs.size() != root["auditLog"].size()) return std::nullopt;
        return result;
    } catch (...) { return std::nullopt; }
}

std::optional<server_sent_event> server_sent_event::from_json(const std::string& json_str) {
    return from_json(json_str, nullptr);
}

std::optional<server_sent_event> server_sent_event::from_json(
    const std::string& json_str, std::string* error_out) {
    if (error_out) error_out->clear();
    auto reject = [error_out](const std::string& reason) -> std::optional<server_sent_event> {
        if (error_out) *error_out = reason;
        return std::nullopt;
    };
    try {
        json j = json::parse(json_str);
        if (!j.is_object()) return reject("sync frame is not an object");

        // A replay boundary is informational. It requires an explicit kind,
        // a reason, and an optional UUID head; mixed control/data frames are
        // still invalid. Check it before legacy payload counting because an
        // empty server history can omit the floorReset key entirely.
        if ((j.contains("kind") && j["kind"] == "floorReset") ||
            j.contains("floorReset") || j.contains("floorReason")) {
            if (!j.contains("kind") || j["kind"] != "floorReset" ||
                !j.contains("floorReason") || !j["floorReason"].is_string())
                return reject("invalid floorReset control");
            for (const auto& [key, value] : j.items()) {
                if (key != "kind" && key != "floorReset" && key != "floorReason")
                    return reject("ambiguous floorReset control");
            }
            server_sent_event event;
            event.event_type = type::floor_reset;
            event.floor_reset_reason = j["floorReason"].get<std::string>();
            if (j.contains("floorReset") && !j["floorReset"].is_null()) {
                if (!j["floorReset"].is_string()) return reject("invalid floorReset head");
                const auto head = j["floorReset"].get<std::string>();
                if (!is_sync_uuid(head)) return reject("invalid floorReset head");
                event.floor_reset_head = head;
            }
            return event;
        }

        // Legacy frames can omit kind. When it is supplied, it must agree
        // with the sole payload key. In particular a NACK must never reach
        // the ACK branch just because an ambiguous frame also carries ack.
        const char* payload = nullptr;
        size_t payload_count = 0;
        for (const char* key : {"auditLog", "ack", "replayRequest", "nack", "rejected", "floorReset", "catchUpComplete"}) {
            if (j.contains(key)) {
                payload = key;
                ++payload_count;
            }
        }
        if (payload_count > 1) return reject("ambiguous sync frame payload");
        if (payload_count == 0) return reject("unsupported sync frame");
        if (j.contains("kind") && (!j["kind"].is_string() || j["kind"] != payload))
            return reject("sync frame kind does not match payload");

        if (j.contains("auditLog")) {
            if (!j["auditLog"].is_array()) return reject("auditLog is not an array");
            server_sent_event event;
            event.event_type = type::audit_log;

            size_t entry_index = 0;
            for (const auto& entry_json : j["auditLog"]) {
                // Parse once: the sub-object is already parsed — dumping it
                // back to a string re-parsed every entry a second time.
                auto entry = entry_from_parsed(entry_json);
                // Dropping a bad position would let the resume cursor pass
                // it. Reject this entire delivery before any apply or ACK.
                if (!entry) return reject("invalid auditLog entry at index " + std::to_string(entry_index));
                event.audit_logs.push_back(std::move(*entry));
                ++entry_index;
            }
            return event;

        } else if (j.contains("ack")) {
            if (!j["ack"].is_array()) return reject("ack is not an array");
            server_sent_event event;
            event.event_type = type::ack;

            for (const auto& id : j["ack"]) {
                if (id.is_string()) {
                    event.acked_ids.push_back(id.get<std::string>());
                }
            }
            return event;

        } else if (j.contains("nack")) {
            if (!j["nack"].is_array()) return reject("nack is not an array");
            server_sent_event event;
            event.event_type = type::nack;
            for (const auto& id : j["nack"]) {
                if (!id.is_string() || id.get_ref<const std::string&>().empty())
                    return reject("invalid nack entry");
                event.nacked_ids.push_back(id.get<std::string>());
            }
            if (j.contains("nackReason") && !j["nackReason"].is_null()) {
                if (!j["nackReason"].is_string()) return reject("nackReason is not a string");
                event.nack_reason = j["nackReason"].get<std::string>();
            }
            return event;

        } else if (j.contains("catchUpComplete")) {
            if (!j["catchUpComplete"].is_boolean() || !j["catchUpComplete"].get<bool>())
                return reject("invalid catchUpComplete marker");
            return make_catch_up_complete();
        } else if (j.contains("replayRequest")) {
            server_sent_event event;
            event.event_type = type::replay_request;
            return event;
        }
    } catch (...) {
        // nlohmann's exception text can quote untrusted wire data. Keep the
        // error safe to forward through the bridge and operational logs.
        return reject("malformed sync JSON");
    }

    return reject("unsupported sync frame");
}

// ============================================================================
// synchronizer implementation
// ============================================================================

// ============================================================================
// synchronizer_base implementation
// ============================================================================

void synchronizer_base::init_sync(const sync_config& config, std::shared_ptr<scheduler> sched) {
    config_ = config;
    log_label_cache_ = config_.log_label;
#ifndef __EMSCRIPTEN__
    // A native pacer introduces concurrent callers even when the owner chose
    // inline dispatch. Serialize this synchronizer's scheduled operations;
    // custom/actor/worker schedulers retain their existing execution policy.
    scheduler_ = detail::make_synchronizer_scheduler(std::move(sched));
#else
    scheduler_ = sched;
#endif
    counted_sync_=true;
    auto n = g_sync_instance_count.fetch_add(1, std::memory_order_relaxed) + 1;
    channel_lifetime_=detail::acquire_sync_channel_lifetime(db(),config_.sync_id);
    callback_gate_=std::make_shared<detail::sync_callback_gate>(channel_lifetime_,callback_owner_,callback_owner_required_);
    LOG_INFO("synchronizer", "[%s] CREATED (WSS, this=%p, db=%s, alive=%lld)",
             log_id(), (void*)this, db().config().path.c_str(), (long long)n);
    auto factory = get_network_factory();
    ws_client_ = factory->create_sync_transport(scheduler_);
    setup_transport_handlers();
    setup_observer();
#ifndef __EMSCRIPTEN__
    start_pacer();
#endif
}

void synchronizer_base::init_sync(const sync_config& config, std::shared_ptr<scheduler> sched,
                                   std::unique_ptr<sync_transport> transport) {
    config_ = config;
    log_label_cache_ = config_.log_label;
#ifndef __EMSCRIPTEN__
    // A native pacer introduces concurrent callers even when the owner chose
    // inline dispatch. Serialize this synchronizer's scheduled operations;
    // custom/actor/worker schedulers retain their existing execution policy.
    scheduler_ = detail::make_synchronizer_scheduler(std::move(sched));
#else
    scheduler_ = sched;
#endif
    ws_client_ = std::move(transport);
    counted_sync_=true;
    auto n = g_sync_instance_count.fetch_add(1, std::memory_order_relaxed) + 1;
    channel_lifetime_=detail::acquire_sync_channel_lifetime(db(),config_.sync_id);
    callback_gate_=std::make_shared<detail::sync_callback_gate>(channel_lifetime_,callback_owner_,callback_owner_required_);
    LOG_INFO("synchronizer", "[%s] CREATED (IPC, this=%p, db=%s, alive=%lld)",
             log_id(), (void*)this, db().config().path.c_str(), (long long)n);
    setup_transport_handlers();
    setup_observer();
#ifndef __EMSCRIPTEN__
    start_pacer();
#endif
}

void synchronizer_base::invoke_on_scheduler(std::function<void()> work) {
    scheduler_->invoke(detail::guard_sync_callback(callback_gate_,std::move(work)));
}

void synchronizer_base::request_upload() {
    const auto call_owner=callback_owner_.lock(); // retain managed native owner across inline callbacks
    if (is_destroyed_) return;
#ifdef __EMSCRIPTEN__
    // Single-threaded build: no pacer thread. Legacy immediate dispatch —
    // without this, browser builds would never upload.
    upload_requested_.store(true, std::memory_order_release);
    invoke_on_scheduler([this] {
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
        invoke_on_scheduler([this] {
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
        invoke_on_scheduler([this] {
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
              log_id(),
              (long long)progress_pending_upload_.load(std::memory_order_relaxed),
              truncate_due ? 1 : 0, idle ? 1 : 0, passive_due ? 1 : 0);
    if (!(passive_due || (truncate_due && idle))) return;
    last_passive_ckpt_ = now;
    const bool try_truncate = truncate_due && idle;
    if (try_truncate) last_truncate_ckpt_ = now;

    // Run on the scheduler: the checkpoint uses the synchronizer's WRITE
    // connection, and the scheduler serializes it against upload passes on
    // the same connection. Enqueue-only from here — never blocks the pacer.
    invoke_on_scheduler([this, try_truncate] {
        if (is_destroyed_) return;
        auto res = db().db().wal_checkpoint(try_truncate, /*busy_budget_ms=*/250);
        if (try_truncate && res.busy != 0) {
            // Readers held the WAL — fall back to PASSIVE in the same cycle
            // so backfill progress always happens; truncate retries next due.
            res = db().db().wal_checkpoint(false);
            LOG_DEBUG("synchronizer", "[%s] TRUNCATE checkpoint busy — PASSIVE fallback: log=%lld ckpt=%lld",
                      log_id(), (long long)res.log_frames, (long long)res.checkpointed);
            // Results spec §3.3: the truncate was beaten by a held read
            // snapshot (a keeper generation). Ask the coordinators to
            // advance so facades re-pin at their next access and the NEXT
            // cycle truncates behind them. Fanned per PATH through the
            // instance registry — this synchronizer owns its OWN lattice_db
            // instance (owned_db_), so a local-only signal would never
            // reach the app handles that actually hold the keepers
            // (adversarial-review finding #4). Note there is deliberately
            // NO pre-gate on outstanding generations: TRUNCATE always
            // attempts with its bounded budget and requests the advance
            // only when actually beaten.
            db().request_generation_advance();
        } else if (try_truncate) {
            LOG_INFO("synchronizer", "[%s] WAL TRUNCATE checkpoint: log=%lld ckpt=%lld",
                     log_id(), (long long)res.log_frames, (long long)res.checkpointed);
        } else {
            LOG_DEBUG("synchronizer", "[%s] WAL PASSIVE checkpoint: busy=%d log=%lld ckpt=%lld",
                      log_id(), res.busy, (long long)res.log_frames, (long long)res.checkpointed);
        }
    });
}

void synchronizer_base::start_pacer() {
    // The pacer owns trailing-edge coalescing AND the checkpoint heartbeat —
    // it must run for checkpointing even when coalescing is disabled
    // (upload requests then take the legacy direct path in request_upload,
    // and a spurious pacer wake at most duplicates one exchange-guarded
    // dispatch).
    if (config_.upload_coalesce_ms <= 0 && config_.checkpoint_passive_interval_ms <= 0) return;
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
            // DB work (checkpoints, upload ticks) must run with pacer_mutex_
            // RELEASED: a writer mid-sqlite3_step holds the connection mutex
            // while its change-hook observer calls request_upload(), which
            // takes pacer_mutex_ — doing connection work under pacer_mutex_
            // here is an ABBA deadlock (observed live as a hung
            // PacerDrivenTruncateWhenIdle: main thread in __psynch_mutexwait
            // inside the update hook, pacer in sqlite3LockAndPrepare).
            lock.unlock();
            maybe_checkpoint();
            // Results spec §3.2 second enforcement caller (item-A
            // adversarial finding 3): read-pool maintenance at the
            // maybe_checkpoint cadence, aggregated per PATH — the keepers
            // live on the app handles, not this synchronizer's own
            // lattice_db instance, so a local-only call would police
            // nothing (same instance scoping as the §3.3 TRUNCATE gate).
            // Runs with pacer_mutex_ RELEASED: maintenance COMMITs keeper
            // transactions and may attempt a checkpoint — connection work
            // under pacer_mutex_ is the ABBA hang documented above.
            db().run_read_pool_maintenance_all_instances();
            lock.lock();
            if (pacer_stop_) return;
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
                lock.unlock();
                invoke_on_scheduler([this] {
                    if (is_destroyed_) return;
                    upload_pending_changes();
                });
                lock.lock();
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
    ws_client_->set_on_open(detail::guard_sync_callback(callback_gate_, [this] { on_websocket_open(); }));
    ws_client_->set_on_message(detail::guard_sync_callback(callback_gate_, [this](const transport_message& msg) { on_transport_message(msg); }));
    ws_client_->set_on_error(detail::guard_sync_callback(callback_gate_, [this](const std::string& err) { on_websocket_error(err); }));
    ws_client_->set_on_close(detail::guard_sync_callback(callback_gate_, [this](int code, const std::string& reason) { on_websocket_close(code, reason); }));
}

void synchronizer_base::setup_observer() {
    LOG_DEBUG("synchronizer", "Registering AuditLog observer on instance %p", (void*)&db());
    audit_log_observer_id_ = db().add_table_observer("AuditLog",
        detail::guard_sync_callback(callback_gate_, [this](const std::vector<lattice_db::change_event>& batch) {
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
                auto rows = db().query_read(
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
                          log_id());
                request_upload();
            }
        }));
}

synchronizer_base::~synchronizer_base() {
    // Revoke before touching transport/observer/scheduler state. Queued closures own the gate
    // and channel reservation; after revocation they never dereference this, even if their
    // scheduler has no-op shutdown. Already-admitted callbacks drain before normal teardown.
    is_destroyed_=true;
    if(callback_gate_ && !callback_gate_->revoke_and_wait()) {
        // Still unresolved: callback-local strong ownership is required for self-destruction.
        // This branch preserves prior non-aborting behavior; candidate adoption is blocked.
        LOG_ERROR("synchronizer", "Inline destruction from a sync callback requires deferred owner release");
    }
    LOG_INFO("synchronizer", "[%s] ~synchronizer START (this=%p, db=%s)",
             log_id(), (void*)this,
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
    if (scheduler_) {
#ifndef __EMSCRIPTEN__
        const bool owns_inline_adapter =
            dynamic_cast<detail::sync_immediate_scheduler*>(scheduler_.get()) != nullptr;
#else
        const bool owns_inline_adapter = false;
#endif
        if (owns_inline_adapter || !scheduler_->is_on_thread()) {
            LOG_INFO("synchronizer", "[%s] ~synchronizer: draining scheduler...", log_id());
            scheduler_->shutdown();
        }
    }
    auto n = counted_sync_ ? g_sync_instance_count.fetch_sub(1, std::memory_order_relaxed) - 1
                           : g_sync_instance_count.load(std::memory_order_relaxed);
    LOG_INFO("synchronizer", "[%s] ~synchronizer END (this=%p, alive=%lld)", log_id(), (void*)this, (long long)n);
}

synchronizer_base::finite_cycle_status synchronizer_base::get_finite_cycle_status() const {
    std::lock_guard<std::mutex> lock(finite_cycle_mutex_);
    return finite_cycle_status_;
}

void synchronizer_base::cancel_finite_cycle() {
    std::lock_guard<std::mutex> lock(finite_cycle_mutex_);
    if (finite_cycle_status_.phase == finite_cycle_phase::running)
        finite_cycle_status_.phase = finite_cycle_phase::cancelled;
}

void synchronizer_base::fail_finite_cycle(finite_cycle_failure reason, uint64_t generation) {
    std::lock_guard<std::mutex> lock(finite_cycle_mutex_);
    if (finite_cycle_status_.phase == finite_cycle_phase::running &&
        finite_cycle_status_.connection_generation == generation) {
        finite_cycle_status_.phase = finite_cycle_phase::failed;
        finite_cycle_status_.failure = reason;
    }
}

bool synchronizer_base::finite_cycle_accepts_work(uint64_t generation, bool download) const {
    if (!config_.finite_cycle) return true;
    std::lock_guard<std::mutex> lock(finite_cycle_mutex_);
    return finite_cycle_status_.connection_generation == generation &&
        finite_cycle_status_.phase == finite_cycle_phase::running &&
        (!download || !finite_cycle_status_.catch_up_received);
}

void synchronizer_base::complete_finite_cycle_if_drained() {
    if (!config_.finite_cycle || !is_connected_) return;
    const auto status = get_finite_cycle_status();
    if (status.phase != finite_cycle_phase::running || !status.horizon_captured ||
        !status.catch_up_received || (config_.uploads_enabled && reconciliation_pending_)) return;
    {
        std::lock_guard<std::mutex> lock(in_flight_mutex_);
        if (!in_flight_ids_.empty()) return;
    }
    // In-flight progress alone is insufficient: the next upload window can
    // still be queued, and outbox sources may already have been compacted.
    if (config_.uploads_enabled) {
        if (!db().db().query("SELECT 1 FROM _lattice_sync_outbox WHERE sync_id=? LIMIT 1",
                            {config_.sync_id}).empty()) return;
        const int64_t floor = config_.use_upload_floor
            ? read_upload_floor(db().db(), config_.sync_id) : 0;
        if (!query_audit_log_for_sync(db().db(), config_.sync_id, config_.sync_filter,
                                     floor, 1, status.upload_horizon).empty()) return;
    }
    std::lock_guard<std::mutex> lock(finite_cycle_mutex_);
    if (finite_cycle_status_.phase == finite_cycle_phase::running &&
        finite_cycle_status_.connection_generation == status.connection_generation &&
        connection_generation_ == status.connection_generation && is_connected_)
        finite_cycle_status_.phase = finite_cycle_phase::work_complete;
}

void synchronizer_base::connect() {
    const auto call_owner=callback_owner_.lock(); // retain managed native owner across inline callbacks
    std::lock_guard<std::recursive_mutex> lifecycle(connection_mutex_);
    if (is_destroyed_ || (config_.owner_active && !config_.owner_active->load(std::memory_order_acquire))) return;
    // Read in the opposite order to on_open's connecting -> connected handoff.
    // Otherwise an old connected=false and new connecting=false can straddle it.
    if (is_connecting_ || is_connected_) return;
    is_connecting_ = true;
    const auto generation = ++connection_generation_;
    if (config_.finite_cycle) {
        std::lock_guard<std::mutex> lock(finite_cycle_mutex_);
        finite_cycle_status_ = {};
        finite_cycle_status_.phase = finite_cycle_phase::running;
        finite_cycle_status_.connection_generation = generation;
        finite_cycle_ready_ = false;
    }
    try {
    if (config_.finite_cycle && config_.websocket_url.empty())
        throw std::invalid_argument("finite sync requires a WSS endpoint");
    LOG_INFO("synchronizer", "[%s] connect() (this=%p, db=%s)",
             log_id(), (void*)this, db().config().path.c_str());
    // Admit the pin before either the WSS or injected/IPC connect branch. A
    // nonempty pin must never be silently discarded by an unsupported transport.
    if ((!config_.pinned_server_certificate_sha256.empty() &&
         config_.websocket_url.compare(0, 6, "wss://") != 0) ||
        !ws_client_->configure_certificate_pin(config_.pinned_server_certificate_sha256)) {
        should_reconnect_ = false;
        on_websocket_error("This transport cannot enforce the configured server certificate pin.");
        return;
    }
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
                 log_id(), should_reconnect_ ? 1 : 0);
        ws_client_->connect("", {});
        return;
    }

    should_reconnect_ = !config_.finite_cycle;

    std::string url = config_.websocket_url;
    if (config_.finite_cycle)
        url += (url.find('?') == std::string::npos ? "?" : "&") +
            std::string("lattice-catch-up-complete=1");

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
    } catch (...) {
        fail_finite_cycle(finite_cycle_failure::transport, generation);
        is_connecting_ = false;
        throw;
    }
}

void synchronizer_base::disconnect() {
    const auto call_owner=callback_owner_.lock(); // retain managed native owner across inline callbacks
    std::lock_guard<std::recursive_mutex> lifecycle(connection_mutex_);
    cancel_finite_cycle();
    ++connection_generation_;
    LOG_INFO("synchronizer", "[%s] disconnect() (this=%p, is_connected=%d, db=%s)",
             log_id(), (void*)this, is_connected_ ? 1 : 0,
             db().config().path.c_str());
    should_reconnect_ = false;  // Prevent auto-reconnect
    is_connecting_ = false;
    is_connected_ = false;
    reconnect_attempts_ = 0;

    // Clear in-flight set — entries will be re-queried on reconnect
    {
        std::lock_guard<std::mutex> lock(in_flight_mutex_);
        in_flight_ids_.clear();
    }

    // Transport construction can throw before assignment; base cleanup still runs.
    if (ws_client_) ws_client_->disconnect();
    // Transport disconnect fences/drains its callbacks. An already-admitted
    // open may have completed while we waited on that fence, after the early
    // clear above. Publish the final state after it, without taking a callback
    // lock in on_websocket_open (which would invert transport/lifecycle locks).
    is_connecting_ = false;
    is_connected_ = false;
    const auto generation = connection_generation_.load();
    invoke_on_scheduler([this, generation] {
        if (!is_destroyed_ && connection_generation_ == generation && !is_connected_)
            fire_state_change(false);
    });

    // An observer's slot carries no upload state — evict it on a clean
    // disconnect so it never lingers as a participant in anyone's floor.
    if (config_.is_observer && !is_destroyed_) {
        try {
            remove_replication_slot(db().db(), config_.sync_id);
        } catch (const std::exception& e) {
            LOG_DEBUG("synchronizer", "[%s] observer slot eviction skipped: %s", log_id(), e.what());
        }
    }
}

void synchronizer_base::sync_now() {
    const auto call_owner=callback_owner_.lock(); // retain managed native owner across inline callbacks
    // Routed through request_upload: (a) inherits coalescing, (b) fixes a
    // pre-existing hazard — the direct call ran the whole upload path on the
    // caller's thread, unserialized with the scheduler that every other
    // upload pass runs on. Under immediate_scheduler (tests) the leading
    // edge dispatches inline, preserving synchronous semantics.
    request_upload();
}

void synchronizer_base::drain(std::chrono::steady_clock::time_point deadline) {
    const auto call_owner=callback_owner_.lock(); // retain managed native owner across inline callbacks
    if (!is_connected_ || is_destroyed_) return;
    // A callback cannot wait for another job on its own serial scheduler. This is only a
    // best-effort flush: returning here reports no delivery completion; durable work stays pending.
    if(scheduler_ && scheduler_->is_on_thread()) return;

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
    invoke_on_scheduler([this, pass_done] {
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
             log_id(),
             (long long)progress_pending_upload_.load(std::memory_order_relaxed));
}

void synchronizer_base::on_websocket_open() {
    if (is_destroyed_) return;
    // Publish the successor state before retiring the old one. connect() reads
    // connecting first, so these seq_cst flags never admit a duplicate attempt.
    is_connected_ = true;
    is_connecting_ = false;
    LOG_INFO("synchronizer", "[%s] on_websocket_open (this=%p, db=%s)",
             log_id(), (void*)this, db().config().path.c_str());
    // Do NOT reset reconnect_attempts_ here: a flapping endpoint fires
    // open→error every cycle, and resetting on open pinned the backoff at
    // base_delay forever (~1s reconnect storm). The counter resets in
    // on_websocket_close/error instead, and only after the connection
    // proved stable (see kStableConnectionMs).
    last_open_time_ms_ = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();

    const auto generation = connection_generation_.load();
    invoke_on_scheduler([this, generation] {
        if (!is_destroyed_ && connection_generation_ == generation && is_connected_)
            fire_state_change(true);
    });

    // Dispatch to scheduler — on_open may fire synchronously on the calling
    // thread (e.g., IPC accept on the main thread). Reconciliation and the
    // initial upload must not block the caller.
    invoke_on_scheduler([this, generation] {
        if (is_destroyed_ || connection_generation_ != generation || !is_connected_ ||
            !finite_cycle_accepts_work(generation)) return;

        try {
        register_replication_slot(db().db(), config_.sync_id, config_.is_observer);

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
                     log_id());
            auto req = server_sent_event::make_replay_request().to_json();
            ws_client_->send(transport_message::from_binary({req.begin(), req.end()}));
        }

        reconciliation_pending_=config_.sync_filter.has_value();
        if (config_.sync_filter) {
            LOG_DEBUG("synchronizer", "Reconciling sync filter on connect...");
            reconciliation_pending_=true;
        }

        {
            std::lock_guard<std::mutex> lock(finite_cycle_mutex_);
            if (finite_cycle_status_.connection_generation == generation)
                finite_cycle_ready_ = true;
        }
        LOG_DEBUG("synchronizer", "Calling upload_pending_changes...");
        upload_pending_changes();
        } catch (...) {
            fail_finite_cycle(finite_cycle_failure::storage, generation);
            throw;
        }
    });
}

void synchronizer_base::on_transport_message(const transport_message& msg) {
    // Guard against use-after-free: the NIO/IPC callback thread may deliver
    // a message after the destructor has started tearing down members.
    if (is_destroyed_) return;
    const auto generation = connection_generation_.load();

    // Wrap in try-catch: this is called from Swift via the WebSocket callback.
    // C++ exceptions must NOT propagate across the Swift/C++ boundary — that
    // causes std::terminate() → SIGTRAP.
    try {
        std::string json_str = msg.as_string();

        std::string parse_error;
        auto event = server_sent_event::from_json(json_str, &parse_error);
        if (!event) {
            invoke_on_scheduler([this, generation, parse_error = std::move(parse_error)] {
                if (is_destroyed_ || connection_generation_ != generation) return;
                // Late live traffic is outside a completed finite receipt.
                if (!get_finite_cycle_status().catch_up_received)
                    fail_finite_cycle(finite_cycle_failure::invalid_frame, generation);
                fire_error("Failed to parse server message: " + parse_error);
            });
            return;
        }

        if (event->event_type == server_sent_event::type::floor_reset) {
            // Only the following replay may earn receipts and a checkpoint.
            // The advertised head changes no rows, upload state or finite
            // cycle completion state, and must never become an ACK.
            LOG_INFO("synchronizer", "[%s] floor reset control; awaiting full replay", log_id());
            return;
        }

        LOG_INFO("synchronizer", "[%s] on_transport_message: type=%s entries=%zu (this=%p, db=%s)",
                 log_id(),
                 event->event_type == server_sent_event::type::audit_log ? "audit_log"
                     : event->event_type == server_sent_event::type::replay_request ? "replay_request"
                     : event->event_type == server_sent_event::type::nack ? "nack"
                     : event->event_type == server_sent_event::type::catch_up_complete ? "catch_up_complete" : "ack",
                 event->event_type == server_sent_event::type::audit_log ? event->audit_logs.size()
                     : event->event_type == server_sent_event::type::nack ? event->nacked_ids.size()
                     : event->acked_ids.size(),
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
                             log_id(), skipped_filter_removals.size());
                    entries.erase(it, entries.end());
                }
            }
            auto entry_count = entries.size();
            invoke_on_scheduler([this, generation, entries = std::move(entries), entry_count,
                                skipped_filter_removals = std::move(skipped_filter_removals)] {
                if (is_destroyed_ || connection_generation_ != generation ||
                    !finite_cycle_accepts_work(generation, true)) {
                    LOG_INFO("synchronizer", "[%s] scheduler lambda: is_destroyed_, skipping apply of %zu entries",
                             log_id(), entry_count);
                    return;
                }
                try {
                LOG_INFO("synchronizer", "[%s] scheduler lambda: applying %zu entries (db=%s)",
                         log_id(), entries.size(), db().config().path.c_str());
                auto applied_ids = apply_remote_changes(entries);
                const bool partial = applied_ids.size() < entries.size();
                if (partial) fail_finite_cycle(finite_cycle_failure::partial_apply, generation);
                // Ack skipped filter-removals as if applied (see above).
                applied_ids.insert(applied_ids.end(),
                                   skipped_filter_removals.begin(),
                                   skipped_filter_removals.end());
                LOG_INFO("synchronizer", "[%s] apply_remote_changes returned %zu/%zu (db=%s)",
                         log_id(), applied_ids.size(), entries.size(),
                         db().config().path.c_str());

                // Send acknowledgment only for successfully applied entries
                if (!applied_ids.empty()) {
                    auto ack = server_sent_event::make_ack(applied_ids);
                    auto json_ack = ack.to_json();
                    LOG_INFO("synchronizer", "[%s] sending ACK for %zu entries (transport_state=%d, db=%s)",
                             log_id(), applied_ids.size(),
                             static_cast<int>(ws_client_->state()),
                             db().config().path.c_str());
                    ws_client_->send(transport_message::from_binary({json_ack.begin(), json_ack.end()}));
                }

                // If some entries failed, schedule a retry so the sender re-uploads them
                if (partial) {
                    LOG_WARN("synchronizer", "Partial apply: %zu/%zu entries succeeded, %zu failed — sender will retry on next upload cycle",
                             applied_ids.size(), entries.size(),
                             entry_count - (applied_ids.size() - skipped_filter_removals.size()));
                }
                } catch (...) {
                    fail_finite_cycle(finite_cycle_failure::storage, generation);
                    throw;
                }
            });

        } else if (event->event_type == server_sent_event::type::ack) {
            // Dispatch to scheduler to serialize with upload_pending_changes.
            auto ids = std::move(event->acked_ids);
            invoke_on_scheduler([this, generation, ids = std::move(ids)] {
                if (is_destroyed_ || connection_generation_ != generation ||
                    !finite_cycle_accepts_work(generation)) return;
                // A download-only owner makes no upload attempts. Even a late
                // exact receipt stays pending for the next enabled owner; an
                // unsolicited ACK must not cancel receipts, settle sources or
                // report upload completion. ACKs we send for downloads are separate.
                if (!config_.uploads_enabled) return;
                try {
                    mark_as_synced(ids);
                    complete_finite_cycle_if_drained();
                } catch (...) {
                    fail_finite_cycle(finite_cycle_failure::storage, generation);
                    throw;
                }

                fire_sync_complete(ids);
            });

        } else if (event->event_type == server_sent_event::type::nack) {
            // A rejection is not durability progress. Leave persisted sync
            // state, upload floors and in-flight IDs intact; the existing
            // window-local ACK watchdog retains its bounded backoff. Releasing
            // IDs and uploading here would create a hot retry loop against a
            // relay whose deterministic apply failure cannot be cured by retry.
            auto reason = std::move(event->nack_reason);
            invoke_on_scheduler([this, generation, reason = std::move(reason)] {
                if (is_destroyed_ || connection_generation_ != generation) return;
                fail_finite_cycle(finite_cycle_failure::partial_apply, generation);
                fire_error("Sync upload rejected: " +
                              (reason.empty() ? std::string("no reason supplied") : reason));
            });

        } else if (event->event_type == server_sent_event::type::catch_up_complete) {
            invoke_on_scheduler([this, generation] {
                if (is_destroyed_ || connection_generation_ != generation || !config_.finite_cycle) return;
                {
                    std::lock_guard<std::mutex> lock(finite_cycle_mutex_);
                    if (finite_cycle_status_.phase != finite_cycle_phase::running ||
                        finite_cycle_status_.connection_generation != generation) return;
                    finite_cycle_status_.catch_up_received = true;
                }
                upload_pending_changes();
            });
        } else if (event->event_type == server_sent_event::type::replay_request) {
            // Peer announced a fresh DB. Re-arm the filtered snapshot so
            // reconcile re-synthesizes INSERTs for the filtered subset.
            // Gated on having a filter: reset_sync_state's sync-set wipe is
            // global, and an unfiltered sync has no synthesis path to re-send
            // from anyway (see reset_sync_state's documented limitation).
            invoke_on_scheduler([this, generation] {
                if (is_destroyed_ || connection_generation_ != generation) return;
                if (config_.finite_cycle) {
                    fail_finite_cycle(finite_cycle_failure::invalid_frame, generation);
                    return;
                }
                if (!config_.uploads_enabled) return; // Preserve any old outbound receipt.
                if (!config_.sync_filter) {
                    LOG_INFO("synchronizer", "[%s] replay request ignored (no sync filter on this side)",
                             log_id());
                    return;
                }
                LOG_INFO("synchronizer", "[%s] replay request from fresh peer — re-arming filtered snapshot",
                         log_id());
                db().reset_filtered_peer_snapshot(config_.sync_id);
                reconciliation_pending_=true;
                delivery_outbox_.clear();
                { std::lock_guard<std::mutex> lock(in_flight_mutex_); in_flight_ids_.clear(); }
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
                reconciliation_pending_=true;
                upload_pending_changes();
            });
        }
    } catch (const std::exception& e) {
        fail_finite_cycle(finite_cycle_failure::storage, generation);
        LOG_ERROR("synchronizer", "on_transport_message failed: %s", e.what());
        std::string error_msg = e.what();
        invoke_on_scheduler([this, generation, error_msg] {
            if (!is_destroyed_ && connection_generation_ == generation)
                fire_error("WebSocket message handling failed: " + error_msg);
        });
    } catch (...) {
        fail_finite_cycle(finite_cycle_failure::storage, generation);
        LOG_ERROR("synchronizer", "on_transport_message failed with unknown error");
    }
}

void synchronizer_base::on_websocket_error(const std::string& error) {
    if (is_destroyed_) return;
    fail_finite_cycle(finite_cycle_failure::transport, connection_generation_.load());
    const bool was_connecting = is_connecting_.exchange(false);
    LOG_ERROR("synchronizer", "[%s] WebSocket error: %s (this=%p, db=%s)",
              log_id(), error.c_str(), (void*)this, db().config().path.c_str());

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
                     log_id(), in_flight_ids_.size());
            in_flight_ids_.clear();
        }
        progress_pending_upload_.store(0, std::memory_order_relaxed);
    }
    const auto generation = connection_generation_.load();
    invoke_on_scheduler([this,generation] {
        if(!is_destroyed_ && connection_generation_==generation) fire_progress();
    });
    // A terminal transport error need not be followed by a close callback.
    // Publish down here, once for the active attempt, before the error detail.
    if (was_open || was_connecting) {
        invoke_on_scheduler([this, generation] {
            if (!is_destroyed_ && connection_generation_ == generation && !is_connected_)
                fire_state_change(false);
        });
    }
    invoke_on_scheduler([this, error, generation] {
        if (!is_destroyed_ && connection_generation_ == generation) fire_error(error);
    });

    // Attempt reconnection
    schedule_reconnect();
}

void synchronizer_base::on_websocket_close(int code, const std::string& reason) {
    if (is_destroyed_) return;
    fail_finite_cycle(finite_cycle_failure::transport, connection_generation_.load());
    const bool was_connecting = is_connecting_.exchange(false);
    LOG_INFO("synchronizer", "[%s] WebSocket closed (code=%d, reason=%s, this=%p, db=%s)",
             log_id(), code, reason.c_str(), (void*)this, db().config().path.c_str());
    const bool was_open = is_connected_.exchange(false);
    maybe_reset_backoff_after_stable_connection(was_open);

    // Clear in-flight tracking so entries can be re-sent after reconnect.
    // Pending mirrors the (now empty) in-flight set.
    {
        std::lock_guard<std::mutex> lock(in_flight_mutex_);
        if (!in_flight_ids_.empty()) {
            LOG_INFO("synchronizer", "[%s] Clearing %zu in-flight entries after close",
                     log_id(), in_flight_ids_.size());
            in_flight_ids_.clear();
        }
        progress_pending_upload_.store(0, std::memory_order_relaxed);
    }
    const auto generation = connection_generation_.load();
    invoke_on_scheduler([this,generation] {
        if(!is_destroyed_ && connection_generation_==generation) fire_progress();
    });
    if (was_open || was_connecting) {
        invoke_on_scheduler([this, generation] {
            if (!is_destroyed_ && connection_generation_ == generation && !is_connected_)
                fire_state_change(false);
        });
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

    const auto quote=[](const std::string& value) {
        std::string result="\"";
        for(char c:value) {if(c=='\"') result+='\"'; result+=c;}
        return result+'\"';
    };
    auto cols=db().db().query("PRAGMA table_info("+quote(table_name)+")");
    std::vector<std::string> col_names, projected;
    bool has_id=false;
    std::string budget="64+COALESCE(length(CAST(globalId AS BLOB)),0)";
    for(const auto& col:cols) {
        const auto name_it=col.find("name"),type_it=col.find("type");
        if(name_it==col.end() || !std::holds_alternative<std::string>(name_it->second)) continue;
        const auto name=std::get<std::string>(name_it->second);
        if(name=="id") {has_id=true;continue;} if(name=="globalId") continue;
        const bool blob=type_it!=col.end() && std::holds_alternative<std::string>(type_it->second) &&
            std::get<std::string>(type_it->second)=="BLOB";
        col_names.push_back(name);
        projected.push_back(blob ? "hex("+quote(name)+")" : quote(name));
        budget+="+"+std::to_string(name.size()+8)+"+"+(blob ? "2*" : "")+
            "COALESCE(length(CAST("+quote(name)+" AS BLOB)),0)";
    }
    std::string sentinel="__lattice_snapshot_bytes";
    while(std::find(col_names.begin(),col_names.end(),sentinel)!=col_names.end()) sentinel+='_';
    const auto allowed="("+budget+")<="+std::to_string(2*sync_max_encoded_message_bytes);
    // Every variable projected value is gated by the length sentinel in the SAME SQL statement.
    // This bounds C++ hydration, not SQLite page-cache/RSS or CPU; encoded JSON is bounded later.
    std::string sql="SELECT "+std::string(has_id ? "id" : "0 AS id")+", ("+budget+") AS "+quote(sentinel)+
        ", CASE WHEN "+allowed+" THEN globalId END AS globalId";
    for(size_t i=0;i<col_names.size();++i)
        sql+=", CASE WHEN "+allowed+" THEN "+projected[i]+" END AS "+quote(col_names[i]);
    sql+=" FROM "+quote(table_name)+" WHERE globalId=?";
    auto rows=db().db().query(sql,{global_row_id});
    if(!rows.empty()) {
        const auto& bytes=rows[0].at(sentinel);
        if(!std::holds_alternative<int64_t>(bytes) || std::get<int64_t>(bytes)<0 ||
            std::get<int64_t>(bytes)>static_cast<int64_t>(2*sync_max_encoded_message_bytes))
            throw std::length_error("Sync current-row snapshot exceeds materialized byte limit");
    }
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
    // A5: this is a full-row snapshot of LOCAL state (classify's
    // UPDATE→INSERT conversion for rows entering the filter) — receivers
    // apply it insert-if-absent, never clobbering a newer remote copy.
    entry.synthesized = true;

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

    mark_no_history_nulls(entry, no_history_columns(db().db(), table_name));

    return entry;
}

// Sync set CRUD — all scoped to this synchronizer's sync_id. Membership is
// per-channel state: two filtered synchronizers on one database must never
// see (or clobber) each other's rows.
void synchronizer_base::sync_set_add(const std::string& table_name, const std::string& global_row_id) {
    db().db().execute(
        "INSERT OR IGNORE INTO _lattice_sync_set (sync_id, table_name, global_row_id) VALUES (?, ?, ?)",
        {config_.sync_id, table_name, global_row_id});
}

void synchronizer_base::sync_set_remove(const std::string& table_name, const std::string& global_row_id) {
    db().db().execute(
        "DELETE FROM _lattice_sync_set WHERE sync_id = ? AND table_name = ? AND global_row_id = ?",
        {config_.sync_id, table_name, global_row_id});
}

bool synchronizer_base::sync_set_contains(const std::string& table_name, const std::string& global_row_id) {
    auto rows = db().db().query(
        "SELECT 1 FROM _lattice_sync_set WHERE sync_id = ? AND table_name = ? AND global_row_id = ? LIMIT 1",
        {config_.sync_id, table_name, global_row_id});
    return !rows.empty();
}

// ============================================================================
// Sync filter update / clear
// ============================================================================

void synchronizer_base::update_sync_filter(std::vector<sync_filter_entry> filter) {
    fail_finite_cycle(finite_cycle_failure::configuration_changed, connection_generation_.load());
    const auto call_owner=callback_owner_.lock();
    // Bump version BEFORE scheduling — if another update arrives before this
    // lambda runs, the version will have advanced and we skip the stale reconcile.
    // This prevents intermediate filter states (e.g., rapid sync→local→sync toggles)
    // from generating DELETE+INSERT churn on the receiving database.
    uint64_t version;
    { std::lock_guard<std::mutex> gate(policy_admission_mutex_);
      version = filter_version_.fetch_add(1, std::memory_order_acq_rel) + 1; }
    invoke_on_scheduler([this, filter = std::move(filter), version]() mutable {
        if (filter_version_.load(std::memory_order_acquire) != version) {
            LOG_DEBUG("synchronizer", "Skipping stale filter reconcile (version %llu, current %llu)",
                      (unsigned long long)version,
                      (unsigned long long)filter_version_.load(std::memory_order_acquire));
            // Request increments and scheduler enqueue can interleave across callers. A stale
            // callback may run AFTER the newest one; it must never overwrite authoritative config.
            return;
        }
        config_.sync_filter = std::move(filter);
        applied_filter_version_ = version;
        if (!config_.uploads_enabled) return;
        reconciliation_pending_=true;
        refresh_delivery_outbox();
        cancel_disallowed_deliveries();
        // REWIND the scan floor to the oldest entry this channel has not
        // durably resolved. The floor is a scan bound
        // (query_audit_log_for_sync skips id <= floor), and the empty-pass
        // advance below can push it PAST entries that were skipped only
        // because the old filter excluded them. Widening the filter is
        // exactly the case that must see those entries again — reconcile's
        // Phase-2 dedup deliberately does NOT re-synthesize rows whose
        // INSERT entries are still pending, so without this rewind they are
        // unreachable forever (observed: exposing a project to a group
        // relayed nothing, because the group channel's empty passes had
        // already advanced its floor past the row).
        //
        // Derived from _lattice_sync_state, not from in-memory bookkeeping,
        // so it is correct across daemon restarts too.
        if (config_.use_upload_floor) {
            auto rows = db().db().query(
                "SELECT MIN(a.id) AS min_pending FROM AuditLog a "
                "LEFT JOIN _lattice_sync_state s "
                "  ON s.audit_entry_id = a.id AND s.sync_id = ? "
                "WHERE s.audit_entry_id IS NULL OR s.is_synchronized=0", {config_.sync_id});
            if (!rows.empty()) {
                auto it = rows[0].find("min_pending");
                if (it != rows[0].end() && std::holds_alternative<int64_t>(it->second)) {
                    const int64_t rewind_to = std::get<int64_t>(it->second) - 1;
                    db().db().execute(
                        "UPDATE _lattice_replication_slots SET upload_floor = ? "
                        "WHERE sync_id = ? AND upload_floor > ?",
                        {rewind_to, config_.sync_id, rewind_to});
                }
            }
            std::lock_guard<std::mutex> lock(in_flight_mutex_);
            last_enumerated_id_ = 0;  // re-enumerate from the rewound floor
        }
        reconciliation_pending_=true;
        // A filter change can make ALREADY-PENDING entries newly eligible
        // (reconcile's Phase-2 dedup intentionally does not re-synthesize rows
        // whose INSERT entries are still pending). Reconcile only uploads when
        // it synthesized something, so kick the pipeline unconditionally —
        // a no-op when nothing is pending.
        upload_pending_changes();
    });
}

void synchronizer_base::clear_sync_filter() {
    fail_finite_cycle(finite_cycle_failure::configuration_changed, connection_generation_.load());
    const auto call_owner=callback_owner_.lock(); // retain managed native owner across inline callbacks
    uint64_t version;
    { std::lock_guard<std::mutex> gate(policy_admission_mutex_);
      version = filter_version_.fetch_add(1, std::memory_order_acq_rel) + 1; }
    invoke_on_scheduler([this, version] {
        if (is_destroyed_ || filter_version_.load(std::memory_order_acquire) != version) return;
        config_.sync_filter = std::nullopt; // widening, never authority revocation
        reconciliation_pending_=false;
        applied_filter_version_ = version;
        if (!config_.uploads_enabled) return;
        refresh_delivery_outbox();
        cancel_disallowed_deliveries();
        // Retain uncertain predecessors until ACK even after widening. Their exact replay must
        // precede subsequent unfiltered operations; membership bookkeeping is otherwise unused.
        db().db().execute("DELETE FROM _lattice_sync_set WHERE sync_id = ?", {config_.sync_id});
        upload_pending_changes();
    });
}

// ============================================================================
// Reconciliation — called when filter changes at runtime
// ============================================================================

void synchronizer_base::reconcile_sync_filter() {
    if (!config_.uploads_enabled) return;
    if(!is_connected_ || !config_.sync_filter ||
       filter_version_.load(std::memory_order_acquire)!=applied_filter_version_) return;
    const auto quote=[](const std::string& value) {
        std::string result="\""; for(char c:value) {if(c=='\"') result+='\"';result+=c;} return result+'\"';
    };
    const int64_t cutoff=detail::read_sync_reset_cutoff(db().db(),config_.sync_id);
    const auto routing_key=current_routing_key();
    // Membership is scanned as keyset pages, never a full in-memory catalog/current-match set.
    // The complete scan still costs SQL work proportional to membership and needs timing evidence.
    std::string last_table,last_row;
    while(true) {
        const auto page=db().db().query("SELECT CASE WHEN length(CAST(table_name AS BLOB))<=4096 THEN table_name END AS tn, "
            "CASE WHEN length(CAST(global_row_id AS BLOB))<=4096 THEN global_row_id END AS gid "
            "FROM main._lattice_sync_set WHERE sync_id=? AND (table_name>? OR (table_name=? AND global_row_id>?)) "
            "ORDER BY table_name,global_row_id LIMIT 128",{config_.sync_id,last_table,last_table,last_row});
        for(const auto& row:page) {
            if(!std::holds_alternative<std::string>(row.at("tn")) || !std::holds_alternative<std::string>(row.at("gid")))
                throw std::length_error("Sync membership identity exceeds byte limit");
            const auto table=std::get<std::string>(row.at("tn")),gid=std::get<std::string>(row.at("gid"));
            last_table=table;last_row=gid;
            if(has_pending_delivery(table,gid)) continue;
            std::string parent=table;
            const bool link=!parent.empty() && parent[0]=='_';
            if(link) {const auto split=parent.find('_',1);if(split!=std::string::npos) parent=parent.substr(1,split-1);}
            const bool matches=is_table_in_filter(parent) && (link || row_matches_filter(table,gid)) &&
                !db().db().query("SELECT 1 FROM "+quote(table)+" WHERE globalId=? LIMIT 1",{gid}).empty();
            if(matches) continue;
            if(!config_.narrowing_emits_removals) {sync_set_remove(table,gid);continue;}
            // Reconnects do not synthesize another removal if the previous source is still queued.
            db().db().execute("INSERT INTO AuditLog(globalId,tableName,operation,rowId,globalRowId,changedFields,"
                "changedFieldsNames,isFromRemote,isSynchronized) "
                "SELECT ?,?,'DELETE',0,?,'{}','[\"__lattice_filter_removal\"]',0,0 WHERE NOT EXISTS("
                "SELECT 1 FROM AuditLog a WHERE a.tableName=? AND a.globalRowId=? AND a.operation='DELETE' "
                "AND a.id>? AND (EXISTS(SELECT 1 FROM _lattice_sync_state st WHERE st.audit_entry_id=a.id "
                "AND st.sync_id=? AND st.is_synchronized=0) OR (a.isSynchronized=0 AND NOT EXISTS("
                "SELECT 1 FROM _lattice_sync_state st WHERE st.audit_entry_id=a.id AND st.sync_id=?))))",
                {db().generate_global_id(),table,gid,table,gid,cutoff,config_.sync_id,config_.sync_id});
        }
        if(page.size()<128) break;
        if(filter_version_.load(std::memory_order_acquire)!=applied_filter_version_) return;
    }

    // Current-state additions: select only bounded identities, prepare one bounded row, then
    // atomically add its audit source and exact outbox receipt. No full-catalog payload read.
    // Retention pressure rolls back that source and leaves reconciliation pending for an ACK pass.
    // The page bound applies to retained receipts across every reconnect/upload pass, not
    // only to newly synthesized rows in this invocation. Existing ordinary deliveries also
    // consume this capacity; no uncertain receipt is discarded to make room for a snapshot.
    constexpr size_t snapshot_retention_limit=128;
    const auto retained_count=[&]() {
        const auto rows=db().db().query("SELECT COUNT(*) AS n FROM main._lattice_sync_outbox WHERE sync_id=?",{config_.sync_id});
        if(rows.size()!=1 || !std::holds_alternative<int64_t>(rows[0].at("n")))
            throw db_error("Cannot read snapshot retention count");
        const auto count=std::get<int64_t>(rows[0].at("n"));
        if(count<0 || static_cast<uint64_t>(count)>detail::sync_outbox_rows)
            throw db_error("Invalid snapshot retention count");
        return static_cast<size_t>(count);
    };
    const auto retained=retained_count();
    if(retained>=snapshot_retention_limit) {reconciliation_pending_=true;return;}
    const auto available=snapshot_retention_limit-retained;
    size_t processed=0;
    bool more=false;
    const int64_t floor=config_.use_upload_floor ? read_upload_floor(db().db(),config_.sync_id) : 0;
    for(const auto& parent_filter:*config_.sync_filter) {
        // Relationship rows live in junction tables, not in the parent model snapshot. Preserve
        // the existing implicit parent-table routing policy and rebuild their current state too.
        std::vector<std::pair<sync_filter_entry,bool>> targets={{parent_filter,false}};
        const auto prefix="_"+parent_filter.table_name+"_";
        // Only registered internal data tables participate. Vector/FTS/RTree index artifacts
        // share the parent prefix but have no sync globalId. Registration is also ensured for
        // existing link/virtual-link/geo-list tables; its value may be empty or multi-parent.
        const auto links=db().db().query("SELECT CASE WHEN length(CAST(s.name AS BLOB))<=4096 THEN s.name END AS name "
            "FROM main.sqlite_schema s WHERE s.type='table' AND substr(s.name,1,?)=? "
            "AND EXISTS(SELECT 1 FROM main._lattice_meta m WHERE m.key='internal_table:'||s.name) "
            "ORDER BY s.name LIMIT 129",
            {static_cast<int64_t>(prefix.size()),prefix});
        if(links.size()>128) throw std::length_error("Sync parent exceeds 128 relationship-table limit");
        for(const auto& link:links) {
            if(!std::holds_alternative<std::string>(link.at("name"))) throw std::length_error("Sync relationship table identity exceeds byte limit");
            targets.push_back({{std::get<std::string>(link.at("name")),std::nullopt},true});
        }
        for(const auto& target:targets) {
        const auto& filter=target.first;const bool link=target.second;
        if(processed>=available) {more=true;break;}
        const auto remaining=available-processed;
        std::string sql="SELECT CASE WHEN length(CAST(t.globalId AS BLOB))<=4096 THEN t.globalId END AS gid "
            "FROM "+quote(filter.table_name)+" t WHERE NOT EXISTS(SELECT 1 FROM _lattice_sync_set s "
            "WHERE s.sync_id=? AND s.table_name=? AND s.global_row_id=t.globalId) "
            "AND NOT EXISTS(SELECT 1 FROM _lattice_sync_outbox o WHERE o.sync_id=? AND o.table_name=? AND o.global_row_id=t.globalId) "
            "AND NOT EXISTS(SELECT 1 FROM AuditLog a WHERE a.tableName=? AND a.globalRowId=t.globalId AND a.id>? "
            "AND (EXISTS(SELECT 1 FROM _lattice_sync_state st WHERE st.audit_entry_id=a.id AND st.sync_id=? AND st.is_synchronized=0) "
            "OR (a.id>? AND a.isSynchronized=0 AND NOT EXISTS(SELECT 1 FROM _lattice_sync_state st "
            "WHERE st.audit_entry_id=a.id AND st.sync_id=?))))";
        std::vector<column_value_t> args={config_.sync_id,filter.table_name,config_.sync_id,filter.table_name,
            filter.table_name,cutoff,config_.sync_id,floor,config_.sync_id};
        if(filter.where_clause && !filter.where_clause->empty()) sql+=" AND ("+*filter.where_clause+")";
        sql+=" ORDER BY t.globalId LIMIT "+std::to_string(remaining);
        const auto page=db().db().query(sql,args);
        if(page.size()==remaining) more=true;
        for(const auto& row:page) {
            if(!std::holds_alternative<std::string>(row.at("gid"))) throw std::length_error("Sync row identity exceeds byte limit");
            const auto gid=std::get<std::string>(row.at("gid")); ++processed;
            if(filter_version_.load(std::memory_order_acquire)!=applied_filter_version_) return;
            bool capacity_refused = false;
            std::optional<detail::sync_delivery> admitted;
            detail::sync_delivery_transaction(db(), [&] {
                // A different handle may have admitted work since the initial page query.
                // Recheck while holding the same transaction that inserts this receipt.
                if(retained_count()>=snapshot_retention_limit) {
                    capacity_refused=true;return false;
                }
                if(!link && !row_matches_filter(filter.table_name,gid)) {return false;}
                auto snapshot=build_insert_entry_from_current_row(filter.table_name,gid);
                if(!snapshot) {return false;}
                detail::sync_delivery delivery;
                delivery.wire=std::move(*snapshot);delivery.membership=1;delivery.routing_key=routing_key;
                delivery.source_global_id=delivery.wire.global_id;
                db().db().execute("INSERT INTO AuditLog(globalId,tableName,operation,rowId,globalRowId,changedFields,"
                    "changedFieldsNames,isFromRemote,isSynchronized,synthesized) VALUES(?,?,'INSERT',?,?,?, ?,0,0,1)",
                    {delivery.wire.global_id,delivery.wire.table_name,delivery.wire.row_id,delivery.wire.global_row_id,
                     delivery.wire.changed_fields_to_json(),delivery.wire.changed_fields_names_to_json()});
                const auto source=db().db().query("SELECT id FROM AuditLog WHERE globalId=?",{delivery.source_global_id});
                delivery.source_id=std::get<int64_t>(source.at(0).at("id"));delivery.wire.id=delivery.source_id;
                if(!detail::admit_sync_delivery_in_transaction(db(),config_.sync_id,delivery)) {
                    capacity_refused=true;return false;
                }
                delivery.wire_json=delivery.wire.to_json();
                delivery.charge=delivery.wire_json.size()+sizeof("{\"kind\":\"auditLog\",\"auditLog\":[")-1+2;
                admitted=std::move(delivery);
                return true;
            });
            if(capacity_refused) {refresh_delivery_outbox();reconciliation_pending_=true;return;}
            if(admitted) delivery_outbox_.push_back(std::move(*admitted));
        }
        }
        if(processed>=available) {more=true;break;}
    }
    reconciliation_pending_=more;
    refresh_delivery_outbox();
}

// ============================================================================
// Upload pending changes (with sync filter support)
// ============================================================================

std::vector<audit_log_entry> synchronizer_base::query_pending_entries(bool& enumeration_hit_limit) {
    int64_t floor = 0;
    // Scalar candidate enumeration is bounded even with the optional floor off.
    // The SQL payload budget below independently bounds materialized audit bytes.
    size_t limit = std::min<size_t>(config_.chunk_size, 2048) * 2;
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
        limit = std::min<size_t>(4096, limit + in_flight_count);
    }
    std::optional<int64_t> horizon;
    if (config_.finite_cycle) horizon = get_finite_cycle_status().upload_horizon;
    bool hit_byte_limit = false;
    auto entries = query_audit_log_for_sync(
        db().db(), config_.sync_id, config_.sync_filter, floor, limit, horizon,
        2 * sync_max_encoded_message_bytes, &hit_byte_limit);
    enumeration_hit_limit = hit_byte_limit || (limit > 0 && entries.size() >= limit);

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
                        return in_flight_ids_.count(sync_event_key(e.global_id)) > 0;
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

void synchronizer_base::sync_outbox_transaction(const std::function<void()>& body) {
    detail::sync_delivery_transaction(db(), [&] { body(); return true; });
}

synchronizer_base::classified_entries synchronizer_base::classify_entries(std::vector<audit_log_entry>& entries) {
    classified_entries result;
    std::unordered_set<std::string> claimed_entities;
    for (const auto& pending : delivery_outbox_)
        claimed_entities.insert(pending.wire.table_name + '\0' + pending.wire.global_row_id);
    for (const auto& entry : entries) {
        const auto entity = entry.table_name + '\0' + entry.global_row_id;
        // A later operation on this entity cannot overtake an uncertain predecessor. No skip or
        // membership mutation occurs; the original remains pending and pins the contiguous floor.
        if (claimed_entities.count(entity)) continue;
        if (!config_.sync_filter) { result.to_send.push_back(entry); continue; }
        const bool removal = entry.operation == "DELETE" &&
            std::find(entry.changed_fields_names.begin(), entry.changed_fields_names.end(),
                      "__lattice_filter_removal") != entry.changed_fields_names.end();
        if (removal && config_.sync_id.rfind("wss:", 0) == 0) {
            sync_set_remove(entry.table_name, entry.global_row_id); // policy-only, never network deletion
            result.to_mark_synced.push_back(entry.id); continue;
        }
        std::string parent = entry.table_name;
        const bool link = !parent.empty() && parent[0] == '_';
        if (link) { const auto split = parent.find('_', 1); if (split != std::string::npos) parent = parent.substr(1, split - 1); }
        // Explicit one-hop removal remains allowed after the table itself leaves this filter.
        if (!is_table_in_filter(parent) && !removal) { result.to_mark_synced.push_back(entry.id); continue; }
        const bool in_set = sync_set_contains(entry.table_name, entry.global_row_id);
        detail::sync_delivery delivery;
        delivery.source_id = entry.id; delivery.source_global_id = entry.global_id;
        delivery.routing_key = current_routing_key();
        if (entry.operation == "DELETE") {
            if (!in_set && !removal) { result.to_mark_synced.push_back(entry.id); continue; }
            delivery.wire = entry; delivery.membership = -1;
        } else {
            const bool matches = link || row_matches_filter(entry.table_name, entry.global_row_id);
            if (!matches && !in_set) { result.to_mark_synced.push_back(entry.id); continue; }
            if (matches && !in_set && entry.operation != "INSERT") {
                auto snapshot = build_insert_entry_from_current_row(entry.table_name, entry.global_row_id);
                if (!snapshot) { result.to_mark_synced.push_back(entry.id); continue; }
                delivery.wire = std::move(*snapshot); delivery.membership = 1;
            } else if (matches) {
                delivery.wire = entry; delivery.membership = in_set ? 0 : 1;
            } else {
                delivery.wire.global_id = db().generate_global_id();
                delivery.wire.table_name = entry.table_name; delivery.wire.global_row_id = entry.global_row_id;
                delivery.wire.operation = "DELETE"; delivery.wire.timestamp = entry.timestamp;
                delivery.membership = -1;
            }
        }
        delivery.wire.id = entry.id; // local source identity; remote apply resolves its own row id
        claimed_entities.insert(entity);
        result.to_send.push_back(delivery.wire);
        result.to_admit.emplace(delivery.wire.global_id, std::move(delivery));
    }
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
        sync_outbox_transaction([&] {
            for (size_t j = i; j < end; ++j) {
                int64_t entry_id = to_mark_synced[j];
                db().db().execute(
                    "INSERT INTO _lattice_sync_state (audit_entry_id, sync_id, is_synchronized) "
                    "VALUES (?, ?, 1) "
                    "ON CONFLICT(audit_entry_id, sync_id) DO UPDATE SET is_synchronized = 1",
                    {entry_id, config_.sync_id});

                // Eager cleanup: if all active sync_ids have synced this entry,
                // collapse to isSynchronized=1 on AuditLog and remove sync_state rows.
                // A6: count only sync_ids with a LIVE replication slot — the
                // threshold is derived from live slots, so a stale confirmed
                // row from a REMOVED channel would otherwise inflate the count
                // and collapse the entry before a still-active channel relayed
                // it (silent relay loss).
                auto count_rows = db().db().query(
                    "SELECT COUNT(*) as cnt FROM _lattice_sync_state ss "
                    "WHERE ss.audit_entry_id = ? AND ss.is_synchronized = 1 "
                    "AND EXISTS (SELECT 1 FROM _lattice_replication_slots rs "
                    "            WHERE rs.sync_id = ss.sync_id)",
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
        });
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
                         log_id(), (long long)min_open);
                stall_since_ = now;  // rate-limit the warning
            }
        }
    }
    if (new_floor > 0) {
        advance_upload_floor(db().db(), config_.sync_id, new_floor);
    }
}

void synchronizer_base::reconcile_open_with_db() {
    if (!config_.use_upload_floor) return;
    // Walk only the RESOLVED PREFIX: stop at the first genuinely-open id (the
    // floor is correctly pinned there). Part A keeps this prefix short in
    // steady state; after a late-ack burst it is bounded by one send window.
    bool erased_missing = false;
    while (true) {
        int64_t min_open = 0;
        {
            std::lock_guard<std::mutex> lock(in_flight_mutex_);
            if (open_audit_ids_.empty()) break;
            min_open = *open_audit_ids_.begin();
        }
        auto rows = db().db().query(R"(
            SELECT
              (SELECT COUNT(*) FROM AuditLog WHERE id = ?1) AS row_present,
              COALESCE((SELECT isSynchronized FROM AuditLog WHERE id = ?1), 0) AS row_synced,
              COALESCE((SELECT is_synchronized FROM _lattice_sync_state
                        WHERE audit_entry_id = ?1 AND sync_id = ?2), 0) AS chan_synced
        )", {min_open, config_.sync_id});
        if (rows.empty()) break;
        const auto get_i64 = [&](const char* key) -> int64_t {
            auto it = rows[0].find(key);
            return (it != rows[0].end() && std::holds_alternative<int64_t>(it->second))
                ? std::get<int64_t>(it->second) : 0;
        };
        const bool missing = get_i64("row_present") == 0;
        if (std::any_of(delivery_outbox_.begin(), delivery_outbox_.end(),
            [&](const auto& receipt) { return receipt.source_id == min_open; })) break;
        const bool resolved = missing || get_i64("row_synced") == 1 || get_i64("chan_synced") == 1;
        if (!resolved) break;
        if (missing) erased_missing = true;
        resolve_audit_ids({min_open});
    }
    // Out-of-band wipe guard. Historically force_compact_audit_log reset the
    // AUTOINCREMENT sequence, so a stale high last_enumerated_id_ could
    // advance the floor ABOVE regenerated history — silently skipping it.
    // Since 1.5.0 the sequence is kept (regenerated rows take ids above the
    // old maximum), so the clamp below is a no-op for that path; it stays as
    // defense against any other out-of-band deletion (a hand-run DELETE, a
    // restored backup) that leaves MAX(id) under an enumerated id. When the
    // sweep erased MISSING rows, clamp the floor back to current MAX(AuditLog.id).
    if (erased_missing) {
        db().db().execute(R"(
            UPDATE _lattice_replication_slots
            SET upload_floor = MIN(upload_floor,
                                   (SELECT COALESCE(MAX(id), 0) FROM AuditLog))
            WHERE sync_id = ?
        )", {config_.sync_id});
        std::lock_guard<std::mutex> lock(in_flight_mutex_);
        last_enumerated_id_ = 0;
    }
}

namespace {
constexpr std::string_view audit_log_frame_prefix = R"({"kind":"auditLog","auditLog":[)";
constexpr std::string_view audit_log_frame_suffix = "]}";
}

std::string detail::single_encoded_audit_log_json(const encoded_audit_log_prefix& encoded) {
    constexpr size_t envelope = audit_log_frame_prefix.size() + audit_log_frame_suffix.size();
    if (encoded.count != 1 || encoded.entry_charges.size() != 1 || encoded.json.size() <= envelope ||
        encoded.entry_charges[0] != encoded.json.size() ||
        encoded.json.compare(0, audit_log_frame_prefix.size(), audit_log_frame_prefix) != 0 ||
        encoded.json.compare(encoded.json.size() - audit_log_frame_suffix.size(),
                             audit_log_frame_suffix.size(), audit_log_frame_suffix) != 0)
        throw std::invalid_argument("Expected one bounded audit entry");
    return encoded.json.substr(audit_log_frame_prefix.size(), encoded.json.size() - envelope);
}

encoded_audit_log_prefix encode_audit_log_prefix(
        const std::vector<audit_log_entry>& entries, size_t start, size_t max_count,
        size_t max_bytes, size_t available_window_bytes) {
    constexpr std::string_view prefix = audit_log_frame_prefix;
    constexpr std::string_view suffix = audit_log_frame_suffix;
    constexpr size_t envelope = prefix.size() + suffix.size();
    if (max_bytes <= envelope) throw std::invalid_argument("Sync message byte limit is too small");
    encoded_audit_log_prefix result;
    if (start >= entries.size() || max_count == 0) return result;
    result.json = prefix;
    size_t reserved = 0;
    const size_t end = start + std::min(max_count, entries.size() - start);
    for (size_t index = start; index < end; ++index) {
        // The single-row encoder can allocate one row; this is an encoded-send
        // limit, not a claim of a pre-materialization or parser allocation bound.
        auto row = entries[index].to_json();
        if (row.size() > max_bytes - envelope) {
            if (result.count != 0) break;
            throw std::length_error("Sync audit row exceeds encoded message byte limit");
        }
        const size_t charge = row.size() + envelope;
        if (charge > available_window_bytes - reserved) break;
        const size_t separator = result.count == 0 ? 0 : 1;
        if (row.size() + separator > max_bytes - suffix.size() - result.json.size()) break;
        if (separator) result.json += ',';
        result.json += row;
        result.entry_charges.push_back(charge);
        reserved += charge;
        ++result.count;
    }
    if (result.count == 0) result.json.clear();
    else result.json += suffix;
    return result;
}


std::string synchronizer_base::current_routing_key() const {
    json value = nullptr;
    if (config_.sync_filter) {
        value = json::array();
        for (const auto& entry : *config_.sync_filter)
            value.push_back({{"table",entry.table_name}, {"where",entry.where_clause ? json(*entry.where_clause) : json(nullptr)}});
    }
    auto key = value.dump();
    if (key.size()>65536) throw std::length_error("delivery filter exceeds metadata limit");
    return key;
}

bool synchronizer_base::has_pending_delivery(const std::string& table, const std::string& row) const {
    return std::any_of(delivery_outbox_.begin(), delivery_outbox_.end(), [&](const auto& delivery) {
        return delivery.wire.table_name == table && delivery.wire.global_row_id == row;
    });
}

void synchronizer_base::refresh_delivery_outbox() {
    delivery_outbox_ = detail::read_sync_outbox(db().db(), config_.sync_id, delivery_outbox_);
    if (config_.use_upload_floor) {
        std::lock_guard<std::mutex> lock(in_flight_mutex_);
        for (const auto& delivery : delivery_outbox_) {
            if (!delivery.legacy_receipt && delivery.source_id > 0) {
                open_audit_ids_.insert(delivery.source_id);
                last_enumerated_id_ = std::max(last_enumerated_id_, delivery.source_id);
            }
        }
    }
}

bool synchronizer_base::delivery_content_allowed(const detail::sync_delivery& delivery) {
    const auto& wire = delivery.wire;
    std::string parent = wire.table_name;
    const bool link = !parent.empty() && parent[0] == '_';
    if (link) { const auto split = parent.find('_', 1); if (split != std::string::npos) parent = parent.substr(1, split - 1); }
    const bool table_allowed = is_table_in_filter(parent);
    // Released v7 committed the delivery and membership at staging. Its
    // INSERT/UPDATE/DELETE sequence survives later row-filter/content changes;
    // reconciliation follows that sequence. Owner/channel and current table
    // scope still gate it. Modern receipts below retain Q2's stricter live-row
    // revalidation and cancellation behavior.
    if (delivery.legacy_receipt) return table_allowed;
    const bool exists = !db().db().query("SELECT 1 FROM " + wire.table_name + " WHERE globalId=? LIMIT 1", {wire.global_row_id}).empty();
    const bool matches = table_allowed && exists && (link || row_matches_filter(wire.table_name, wire.global_row_id));
    if (wire.operation != "DELETE") return matches;
    const bool removal = std::find(wire.changed_fields_names.begin(), wire.changed_fields_names.end(),
                                  "__lattice_filter_removal") != wire.changed_fields_names.end();
    if (removal && config_.sync_id.rfind("wss:", 0) == 0) return false;
    // A saved filter-removal/derived DELETE is stale if the row is shared again. Reconciliation
    // must repair uncertain remote absence with an insert-if-absent snapshot, never replay DELETE.
    if (matches) return false;
    if (!table_allowed) return removal && config_.narrowing_emits_removals;
    return true; // actual deletion, or the row still fails the current row predicate
}

void synchronizer_base::cancel_disallowed_deliveries() {
    if (filter_version_.load(std::memory_order_acquire) != applied_filter_version_) return;
    const auto routing_key = current_routing_key();
    bool rebound = false;
    std::vector<int64_t> cancelled;
    std::vector<std::string> cancelled_wire_ids;
    for (const auto& delivery : delivery_outbox_) {
        if (delivery_content_allowed(delivery)) {
            if (delivery.routing_key != routing_key) {
                rebound = detail::sync_delivery_transaction(db(), [&] {
                    // A sibling connection can edit a row between the outer check and BEGIN.
                    // Never relabel a disallowed old receipt as admitted under this new policy.
                    if (!delivery_content_allowed(delivery)) return false;
                    const auto budget=db().db().query("SELECT COALESCE(SUM(length(CAST(wire_json AS BLOB))+"
                        "COALESCE(length(CAST(source_global_id AS BLOB)),0)+length(CAST(wire_global_id AS BLOB))+"
                        "length(CAST(table_name AS BLOB))+length(CAST(global_row_id AS BLOB))+"
                        "length(CAST(routing_key AS BLOB))+128),0) AS bytes FROM main._lattice_sync_outbox WHERE sync_id=?",
                        {config_.sync_id});
                    const auto retained=std::get<int64_t>(budget.at(0).at("bytes"));
                    if (retained<0 || retained-static_cast<int64_t>(delivery.routing_key.size())+
                        static_cast<int64_t>(routing_key.size())>static_cast<int64_t>(detail::sync_outbox_bytes))
                        throw std::length_error("delivery routing metadata exceeds retained budget");
                    db().db().execute("UPDATE main._lattice_sync_outbox SET routing_key=? WHERE sync_id=? "
                        "AND receipt_id=? AND wire_global_id=?", {routing_key,config_.sync_id,delivery.receipt_id,delivery.wire.global_id});
                    return true;
                }) || rebound;
            }
            continue;
        }
        std::vector<std::pair<int64_t,std::string>> retired;
        const bool committed = detail::sync_delivery_transaction(db(), [&] {
            const auto present = db().db().query("SELECT 1 FROM main._lattice_sync_outbox "
                "WHERE sync_id=? AND receipt_id=? AND wire_global_id=?",
                {config_.sync_id, delivery.receipt_id, delivery.wire.global_id});
            if (present.empty()) return false;
            // Recheck the current row inside the same write transaction as its cancellation.
            if (delivery_content_allowed(delivery)) return false;
            std::string parent = delivery.wire.table_name;
            const bool link = !parent.empty() && parent[0] == '_';
            if (link) { const auto split = parent.find('_', 1); if (split != std::string::npos) parent = parent.substr(1, split - 1); }
            const bool table_allowed = is_table_in_filter(parent);
            const bool exists = !db().db().query("SELECT 1 FROM " + delivery.wire.table_name + " WHERE globalId=? LIMIT 1", {delivery.wire.global_row_id}).empty();
            const bool matches = table_allowed && exists && (link || row_matches_filter(delivery.wire.table_name, delivery.wire.global_row_id));
            std::optional<detail::sync_delivery> successor;
            if (delivery.wire.operation == "DELETE" && matches) {
                // It may already have applied remotely before its ACK was lost. Persist a full
                // repair snapshot even when widening to unfiltered (which has no reconcile pass).
                auto replacement=build_insert_entry_from_current_row(delivery.wire.table_name,delivery.wire.global_row_id);
                if (replacement) { successor.emplace(); successor->wire=std::move(*replacement); successor->membership=1; }
            } else {
                const bool retract=table_allowed && delivery.routing_key==routing_key;
                if ((config_.narrowing_emits_removals || retract) &&
                    (retract || config_.sync_id.rfind("wss:",0)!=0)) {
                    successor.emplace();
                    successor->wire.global_id=db().generate_global_id();
                    successor->wire.table_name=delivery.wire.table_name;
                    successor->wire.global_row_id=delivery.wire.global_row_id;
                    successor->wire.operation="DELETE";
                    successor->wire.timestamp=delivery.wire.timestamp;
                    successor->membership=-1;
                    // Row-edit retracts retain their existing ordinary DELETE semantics. A
                    // configuration narrowing uses only the existing one-hop removal marker.
                    if(!retract) successor->wire.changed_fields_names={"__lattice_filter_removal"};
                }
            }
            db().db().execute("DELETE FROM main._lattice_sync_set WHERE sync_id=? AND table_name=? AND global_row_id=?",
                {config_.sync_id,delivery.wire.table_name,delivery.wire.global_row_id});
            if (!delivery.legacy_receipt && delivery.source_id > 0) db().db().execute("INSERT INTO main._lattice_sync_state(audit_entry_id,sync_id,is_synchronized) VALUES(?,?,1) "
                "ON CONFLICT(audit_entry_id,sync_id) DO UPDATE SET is_synchronized=1", {delivery.source_id,config_.sync_id});
            if (delivery.legacy_receipt) {
                // The released format may have several already-admitted operations
                // for this entity. A policy cancellation retires that complete old
                // sequence atomically before admitting one current repair snapshot.
                const auto old = db().db().query("SELECT source_audit_id,wire_global_id FROM main._lattice_sync_outbox "
                    "WHERE sync_id=? AND table_name=? AND global_row_id=? AND legacy_receipt=1",
                    {config_.sync_id,delivery.wire.table_name,delivery.wire.global_row_id});
                for(const auto& row:old) retired.emplace_back(0,
                    std::get<std::string>(row.at("wire_global_id")));
                db().db().execute("DELETE FROM main._lattice_sync_outbox WHERE sync_id=? AND table_name=? "
                    "AND global_row_id=? AND legacy_receipt=1",
                    {config_.sync_id,delivery.wire.table_name,delivery.wire.global_row_id});
            } else {
                db().db().execute("DELETE FROM main._lattice_sync_outbox WHERE sync_id=? AND source_audit_id=? "
                    "AND wire_global_id=?", {config_.sync_id,delivery.source_id,delivery.wire.global_id});
                retired.emplace_back(delivery.source_id,delivery.wire.global_id);
            }
            if (successor) {
                auto& next=*successor;
                next.routing_key=routing_key;
                next.source_global_id=next.wire.global_id;
                db().db().execute("INSERT INTO AuditLog(globalId,tableName,operation,rowId,globalRowId,changedFields,"
                    "changedFieldsNames,isFromRemote,isSynchronized,synthesized) VALUES(?,?,?,?,?,?,?,0,0,?)",
                    {next.wire.global_id,next.wire.table_name,next.wire.operation,next.wire.row_id,next.wire.global_row_id,
                     next.wire.changed_fields_to_json(),next.wire.changed_fields_names_to_json(),static_cast<int64_t>(next.wire.synthesized)});
                const auto source=db().db().query("SELECT id FROM AuditLog WHERE globalId=?",{next.source_global_id});
                next.source_id=std::get<int64_t>(source.at(0).at("id")); next.wire.id=next.source_id;
                if(!detail::admit_sync_delivery_in_transaction(db(),config_.sync_id,next))
                    throw std::length_error("delivery cancellation successor exceeds retained budget");
            }
            return true;
        });
        if(committed) for(const auto& [source,wire]:retired) {
            cancelled.push_back(source); cancelled_wire_ids.push_back(wire);
        }
    }
    if (cancelled.empty()) { if(rebound) refresh_delivery_outbox(); return; }
    { std::lock_guard<std::mutex> lock(in_flight_mutex_);
      for (const auto& id : cancelled_wire_ids) in_flight_ids_.erase(sync_event_key(id));
      progress_pending_upload_.store(static_cast<int64_t>(in_flight_ids_.size()),std::memory_order_relaxed); }
    refresh_delivery_outbox();
    if (config_.use_upload_floor) {
        cancelled.erase(std::remove(cancelled.begin(),cancelled.end(),0),cancelled.end());
        resolve_audit_ids(cancelled);
    }
}

void synchronizer_base::resend_delivery_outbox() {
    std::vector<audit_log_entry> pending;
    std::set<std::pair<std::string, std::string>> selected;
    { std::lock_guard<std::mutex> lock(in_flight_mutex_);
      for (const auto& delivery : delivery_outbox_) {
        // A released predecessor can contain several accepted operations for
        // one entity. Preserve their durable predecessor FIFO across restart/ACK.
        if (!selected.emplace(delivery.wire.table_name, delivery.wire.global_row_id).second) continue;
        if (!in_flight_ids_.count(sync_event_key(delivery.wire.global_id))) pending.push_back(delivery.wire);
      } }
    send_entries(pending);
}

void synchronizer_base::send_entries(std::vector<audit_log_entry>& entries,
        const std::unordered_map<std::string, detail::sync_delivery>* admissions) {
    if (!config_.uploads_enabled) return;
    if (!finite_cycle_accepts_work(connection_generation_.load())) return;
    if (entries.empty()) return;

    const size_t window_cap = config_.chunk_size * 2;
    constexpr size_t byte_window_cap = 2 * sync_max_encoded_message_bytes;
    constexpr std::string_view prefix = "{\"kind\":\"auditLog\",\"auditLog\":[";
    size_t sent = 0;
    while (sent < entries.size() && is_connected_) {
        if (filter_version_.load(std::memory_order_acquire) != applied_filter_version_) break;
        size_t available_events = 0, available_bytes = 0;
        {
            std::lock_guard<std::mutex> lock(in_flight_mutex_);
            size_t occupied = 0;
            for (const auto& [_, entry] : in_flight_ids_) occupied += entry.encoded_bytes;
            available_events = in_flight_ids_.size() >= window_cap ? 0 : window_cap - in_flight_ids_.size();
            available_bytes = occupied >= byte_window_cap ? 0 : byte_window_cap - occupied;
        }
        if (!available_events || !available_bytes) break;
        encoded_audit_log_prefix frame;
        frame.json = std::string(prefix);
        size_t charged = 0;
        const size_t count_limit = std::min(config_.chunk_size,available_events);
        try {
            while (frame.count < count_limit && sent+frame.count < entries.size()) {
                const auto& entry = entries[sent+frame.count];
                const auto receipt = std::find_if(delivery_outbox_.begin(),delivery_outbox_.end(),
                    [&](const auto& value) { return value.wire.global_id == entry.global_id; });
                std::string wire;
                size_t charge;
                if (receipt != delivery_outbox_.end()) {
                    if (!delivery_content_allowed(*receipt)) break;
                    wire = receipt->wire_json; charge = receipt->charge;
                } else {
                    const auto encoded = encode_audit_log_prefix({entry},0,1);
                    wire = detail::single_encoded_audit_log_json(encoded); charge = encoded.entry_charges.at(0);
                }
                const auto appended = wire.size() + (frame.count ? 1 : 0) + 2;
                if (appended > sync_max_encoded_message_bytes - frame.json.size() || charge > available_bytes-charged) break;
                if (admissions) {
                    const auto candidate = admissions->find(entry.global_id);
                    if (candidate != admissions->end()) {
                        if (!detail::admit_sync_delivery(db(),config_.sync_id,candidate->second)) break;
                        auto admitted = candidate->second;
                        admitted.wire_json = wire; admitted.charge = charge;
                        delivery_outbox_.push_back(std::move(admitted));
                    }
                }
                if (frame.count) frame.json += ',';
                frame.json += wire; frame.entry_charges.push_back(charge);
                charged += charge; ++frame.count;
            }
        } catch (const std::exception& error) {
            fire_error(error.what());
            // Any already-admitted rows survive; only the complete bounded prefix may be sent.
        }
        if (!frame.count) break;
        frame.json += "]}";
        // Connection retirement still joins transport enqueue. Filtering is a separate,
        // nonblocking publication request: the short policy gate below atomically checks its
        // generation and reserves this bounded frame. That is the admission point. A request
        // arriving afterward cannot recall this running enqueue; every later frame must recheck.
        std::optional<std::string> send_failure;
        {
        std::lock_guard<std::recursive_mutex> routing(connection_mutex_);
        {
            std::lock_guard<std::mutex> policy(policy_admission_mutex_);
            if (!finite_cycle_accepts_work(connection_generation_.load()) || !is_connected_ || filter_version_.load(std::memory_order_acquire) != applied_filter_version_) break;
            std::lock_guard<std::mutex> lock(in_flight_mutex_);
            size_t occupied = 0;
            for (const auto& [_, entry] : in_flight_ids_) occupied += entry.encoded_bytes;
            bool duplicate = false;
            for (size_t i=0;i<frame.count;++i) duplicate |= in_flight_ids_.count(sync_event_key(entries[sent+i].global_id))!=0;
            if (duplicate || in_flight_ids_.size()+frame.count>window_cap || occupied>byte_window_cap || charged>byte_window_cap-occupied) break;
            for (size_t i=0;i<frame.count;++i) {
                const auto& entry=entries[sent+i];
                in_flight_ids_[sync_event_key(entry.global_id)]={entry.id,frame.entry_charges[i]};
            }
            progress_pending_upload_.store(static_cast<int64_t>(in_flight_ids_.size()),std::memory_order_relaxed);
        }
        progress_total_upload_.fetch_add(static_cast<int64_t>(frame.count),std::memory_order_relaxed);
        // A throwing transport may have accepted bytes before failing. Retain the exact
        // in-flight attempt and arm its normal watchdog; never dispose a durable receipt here.
        sent += frame.count;
        try { ws_client_->send(transport_message::from_binary({frame.json.begin(),frame.json.end()})); }
        catch(const std::exception& error) { send_failure=error.what(); }
        catch(...) { send_failure="Transport enqueue failed with an unknown exception"; }
        }
        // User callbacks can request a filter or close the owner. Never invoke them while
        // either the transport lifecycle lock or the short policy admission lock is held.
        fire_progress();
        if(send_failure) {
            fire_error(*send_failure);
            break;
        }
    }
    entries.resize(sent); // attempted rows feed progress/watchdog; throws do not imply delivery
    if (sent == 0) return;

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
    // Read immutable timeout inputs while this operation still owns the
    // synchronizer. A detached worker may first run after teardown finishes.
    const auto ack_timeout_base_ms = config_.ack_timeout_base_ms;
    const int resend_failures = ack_resend_failures_.load(std::memory_order_relaxed);
    std::thread([guard = ack_guard_, self = this, lifetime = callback_gate_, sent_ids = std::move(sent_ids),
                 ack_timeout_base_ms, resend_failures] {
        // Consecutive-failure backoff: a server that stalls (accepts frames,
        // never ACKs) must not be re-hammered with the same window every 10s
        // while each resend pass re-queries the audit log. 10s, 20s, 40s...
        // capped at 5 min; reset to 10s by any ACK (mark_as_synced).
        const auto kAckTimeoutBase =
            std::chrono::milliseconds(ack_timeout_base_ms);
        constexpr auto kAckTimeoutMax = std::chrono::minutes(5);
        const int failures = resend_failures;
        const auto timeout = std::min<std::chrono::steady_clock::duration>(
            kAckTimeoutBase * (int64_t(1) << std::min(failures, 5)), kAckTimeoutMax);
        // ACK-PROGRESS deadline, WINDOW-LOCAL: the peer acks a frame only
        // after synchronously APPLYING it (11-20s+ per 1000-entry frame
        // observed), so a fixed send-time deadline re-sent every window
        // before its acks could land — ~16-26x upload amplification in
        // production. Progress is measured on THIS window's own sent_ids
        // (their in-flight count decreasing), not on any global ack signal:
        // a channel-wide timestamp let unrelated acks (progressive per-chunk
        // acks, other windows) extend the deadline of a window the peer had
        // actually skipped, leaving its entries in-flight — excluded from
        // enumeration and pinning the upload floor — for an entire drain.
        // A window being actively consumed is never re-sent; a window with
        // no progress for `timeout` is released and re-sent.
        auto deadline = std::chrono::steady_clock::now() + timeout;
        size_t last_remaining = sent_ids.size();
        while (std::chrono::steady_clock::now() < deadline) {
            {
                std::lock_guard<std::mutex> g(guard->m);
                if (!guard->alive) return;
                std::lock_guard<std::mutex> lock(self->in_flight_mutex_);
                size_t remaining = 0;
                for (const auto& id : sent_ids) {
                    if (self->in_flight_ids_.count(sync_event_key(id))) ++remaining;
                }
                if (remaining == 0) return;  // everything ACKed
                if (remaining < last_remaining) {
                    last_remaining = remaining;
                    deadline = std::chrono::steady_clock::now() + timeout;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        std::lock_guard<std::mutex> g(guard->m);
        if (!guard->alive || !self->is_connected_) return;
        size_t released = 0;
        {
            std::lock_guard<std::mutex> lock(self->in_flight_mutex_);
            for (const auto& id : sent_ids) released += self->in_flight_ids_.erase(sync_event_key(id));
            self->progress_pending_upload_.store(
                static_cast<int64_t>(self->in_flight_ids_.size()), std::memory_order_relaxed);
        }
        if (released > 0) {
            const int f = self->ack_resend_failures_.fetch_add(1, std::memory_order_relaxed) + 1;
            LOG_WARN("synchronizer", "[%s] %zu entries unACKed after timeout — resending (consecutive failures: %d)",
                     self->log_id(), released, f);
            self->request_upload();
        }
    }).detach();
#endif  // !__EMSCRIPTEN__
}

void synchronizer_base::upload_pending_changes() {
    const auto generation = connection_generation_.load();
    if (!finite_cycle_accepts_work(generation)) return;
    if (config_.finite_cycle) {
        std::lock_guard<std::mutex> lock(finite_cycle_mutex_);
        if (!finite_cycle_ready_) return;
    }
    try {
        if (!config_.uploads_enabled) {
            // A finite download-only cycle still waits for catch-up, but must not
            // enumerate, resolve or resend retained outbound work to finish.
            if (config_.finite_cycle) {
                std::lock_guard<std::mutex> lock(finite_cycle_mutex_);
                finite_cycle_status_.horizon_captured = true;
                finite_cycle_status_.upload_horizon = 0;
            }
            complete_finite_cycle_if_drained();
            return;
        }
        upload_pending_changes_impl();
        complete_finite_cycle_if_drained();
    } catch (...) {
        fail_finite_cycle(finite_cycle_failure::storage, generation);
        throw;
    }
}

void synchronizer_base::upload_pending_changes_impl() {
    LOG_DEBUG("synchronizer", "upload_pending_changes called");
    if (!is_connected_) {
        LOG_DEBUG("synchronizer", "upload_pending_changes: not connected, skipping");
        return;
    }
    if (filter_version_.load(std::memory_order_acquire) != applied_filter_version_) return;
    try {
        refresh_delivery_outbox();
        cancel_disallowed_deliveries();
        if (std::any_of(delivery_outbox_.begin(),delivery_outbox_.end(),
                        [](const auto& receipt) {return receipt.legacy_receipt;})) {
            // Released FIFO obligations precede both latest-filter reconciliation
            // and a new finite upload horizon, even after source compaction.
            resend_delivery_outbox();
            return;
        }
        if(reconciliation_pending_) reconcile_sync_filter();
        resend_delivery_outbox();
    } catch (const std::exception& error) {
        fail_finite_cycle(finite_cycle_failure::storage, connection_generation_.load());
        fire_error(error.what());
        return;
    }
    if (config_.finite_cycle && !get_finite_cycle_status().horizon_captured) {
        const auto rows = db().db().query("SELECT COALESCE(MAX(id),0) AS m FROM AuditLog");
        const int64_t horizon = std::get<int64_t>(rows[0].at("m"));
        std::lock_guard<std::mutex> lock(finite_cycle_mutex_);
        if (finite_cycle_status_.phase != finite_cycle_phase::running) return;
        finite_cycle_status_.upload_horizon = horizon;
        finite_cycle_status_.horizon_captured = true;
    }
    // DB-truth belt for the floor (runs before the horizon read so a swept
    // prefix can advance the floor this same pass).
    reconcile_open_with_db();
    // Horizon BEFORE the scan: if the pass then enumerates nothing, no
    // entry <= horizon is pending for this channel (writes that land after
    // the horizon read are past it and unaffected).
    int64_t scan_horizon = 0;
    if (config_.use_upload_floor) {
        auto h = db().db().query("SELECT COALESCE(MAX(id), 0) AS m FROM AuditLog", {});
        if (!h.empty() && std::holds_alternative<int64_t>(h[0].at("m"))) {
            scan_horizon = std::get<int64_t>(h[0].at("m"));
        }
    }
    if (config_.finite_cycle)
        scan_horizon = std::min(scan_horizon, get_finite_cycle_status().upload_horizon);
    bool enumeration_hit_limit = false;
    std::vector<audit_log_entry> entries;
    try { entries = query_pending_entries(enumeration_hit_limit); }
    catch (const std::exception& error) {
        fail_finite_cycle(finite_cycle_failure::storage, connection_generation_.load());
        fire_error(error.what());
        return; // failed enumeration is never an empty-success floor advance
    }
    if (entries.empty()) {
        // A channel whose filter matches nothing NEVER enumerates a row, so
        // its floor never advances and its slot pins compaction at 0 forever
        // (production: two filtered-to-nothing group slots vetoed collapse
        // of 4.67M rows). An empty pass with nothing open/in-flight proves
        // everything <= horizon is resolved for this channel — advance the
        // floor so compaction can pass it. Safe on filter WIDENING: newly
        // matching rows are re-delivered via reconcile Phase-2 full-row
        // snapshots, never by audit replay.
        if (config_.use_upload_floor && scan_horizon > 0) {
            bool nothing_open;
            {
                std::lock_guard<std::mutex> lock(in_flight_mutex_);
                nothing_open = open_audit_ids_.empty() && in_flight_ids_.empty();
            }
            if (nothing_open) {
                db().db().execute(R"(
                    INSERT INTO _lattice_replication_slots (sync_id, upload_floor, last_active_at)
                    VALUES (?, ?, datetime('now'))
                    ON CONFLICT(sync_id) DO UPDATE SET
                        upload_floor = MAX(upload_floor, excluded.upload_floor),
                        last_active_at = excluded.last_active_at
                )", {config_.sync_id, scan_horizon});
            }
        }
        if(reconciliation_pending_ && delivery_outbox_.empty()) {
            invoke_on_scheduler([this] {if(!is_destroyed_) upload_pending_changes();});
        }
        return;
    }
    classified_entries classified;
    try { classified=classify_entries(entries); }
    catch(const std::exception& error) {fail_finite_cycle(finite_cycle_failure::storage, connection_generation_.load());fire_error(error.what());return;}

    // Progress accounting happens in send_entries AFTER windowing: counting
    // the full classified backlog here (pre-window) meant `pending` never
    // returned to 0 during a multi-window catch-up — every pass re-counted
    // the un-sent remainder, inflating pending/total unboundedly (observed
    // 180MB "pending"), permanently blocking drain()'s pending==0 exit and
    // any idle-gated work. The invariant is now: pending == in-flight
    // (sent-but-unACKed) exactly; total counts only entries actually sent.

    const size_t skipped = classified.to_mark_synced.size();
    mark_skipped_synced(classified.to_mark_synced);
    send_entries(classified.to_send, &classified.to_admit);
    const size_t sent = classified.to_send.size();  // resized to the window by send_entries

    // Catch-up continuation: a full enumeration window with real progress
    // (something sent or skipped-resolved) means more backlog waits beyond
    // the LIMIT — continue IMMEDIATELY. Paying the coalesce delay per window
    // turned an N-window catch-up into N*coalesce_ms of added latency
    // (observed: a 3s relay pipeline stretched past 15s). This cannot spin:
    // each pass does bounded real work, the floor advances past skips, and
    // a full send window with nothing sent (stalled server) makes progress
    // false — the ACK / ack-timeout path owns continuation there.
    //
    // New-event bursts (observer requests) still coalesce via request_upload.
    if (enumeration_hit_limit && (sent > 0 || skipped > 0)) {
        invoke_on_scheduler([this] {
            if (is_destroyed_) return;
            upload_pending_changes();
        });
    }
}

std::vector<std::string> synchronizer_base::apply_remote_changes(const std::vector<audit_log_entry>& entries) {
    auto applied = lattice::apply_remote_changes_for(db(), entries, config_.sync_id);
    progress_received_.fetch_add(static_cast<int64_t>(applied.size()), std::memory_order_relaxed);
    fire_progress();
    return applied;
}

void synchronizer_base::mark_as_synced(const std::vector<std::string>& incoming_ids) {
    // A requested policy change revokes old receipt effects before its queued reconciliation runs.
    // Dropped ACKs are safe: exact receipts remain pending and will be re-ACKed on retry if allowed.
    if (filter_version_.load(std::memory_order_acquire) != applied_filter_version_) return;
    refresh_delivery_outbox();
    cancel_disallowed_deliveries();
    std::vector<std::string> global_ids, normal_ids;
    std::vector<int64_t> durable_resolved;
    for (const auto& wire_id : incoming_ids) {
        const auto found = std::find_if(delivery_outbox_.begin(),delivery_outbox_.end(),
            [&](const auto& receipt) { return sync_event_key(receipt.wire.global_id) == sync_event_key(wire_id); });
        if (found == delivery_outbox_.end()) {
            // An ACK for the original cannot skip its differently identified derived operation.
            if (std::none_of(delivery_outbox_.begin(),delivery_outbox_.end(),
                [&](const auto& receipt) { return sync_event_key(receipt.source_global_id) == sync_event_key(wire_id); })) normal_ids.push_back(wire_id);
            continue;
        }
        const auto receipt = *found;
        if (receipt.legacy_receipt) {
            const auto remove_exact = [&] {
                if (filter_version_.load(std::memory_order_acquire) != applied_filter_version_ ||
                    !delivery_content_allowed(receipt)) return false;
                db().db().execute("DELETE FROM main._lattice_sync_outbox WHERE sync_id=? AND receipt_id=? "
                    "AND wire_global_id=? AND wire_json=? AND legacy_receipt=1",
                    {config_.sync_id, receipt.receipt_id, receipt.wire.global_id, receipt.wire_json});
                return sqlite3_changes(db().db().handle()) == 1;
            };
            // Released ACKs settle a surviving matching source, but optional or
            // compacted provenance never becomes a prerequisite for receipt ACK.
            // Both source disposition and removal share the existing owned chunk.
            if (!receipt.source_global_id.empty()) {
                const auto settled=mark_audit_entries_synced_for_impl(db(),{receipt.source_global_id},
                    config_.sync_id,config_.all_active_sync_ids,[&](int64_t source,const std::string&) {
                        if (receipt.source_id>0 && receipt.source_id!=source) return false;
                        if (db().db().query("SELECT 1 FROM main.AuditLog WHERE id=? AND tableName=? AND globalRowId=?",
                            {source,receipt.wire.table_name,receipt.wire.global_row_id}).size()!=1) return false;
                        return remove_exact();
                    });
                if (!settled.empty()) {
                    durable_resolved.insert(durable_resolved.end(),settled.begin(),settled.end());
                    global_ids.push_back(wire_id);
                    continue;
                }
            }
            bool removed=false;
            sync_outbox_transaction([&] {removed=remove_exact();});
            if (removed) global_ids.push_back(wire_id);
            continue;
        }
        auto committed = mark_audit_entries_synced_for_impl(db(),{receipt.source_global_id},
            config_.sync_id,config_.all_active_sync_ids,[&](int64_t source, const std::string&) {
                if (source != receipt.source_id ||
                    filter_version_.load(std::memory_order_acquire) != applied_filter_version_) return false;
                if (db().db().query("SELECT 1 FROM main._lattice_sync_outbox WHERE sync_id=? "
                    "AND source_audit_id=? AND source_global_id=? AND wire_global_id=?",
                    {config_.sync_id,source,receipt.source_global_id,receipt.wire.global_id}).size()!=1) return false;
                if (!delivery_content_allowed(receipt)) return false;
                if (receipt.membership < 0) sync_set_remove(receipt.wire.table_name,receipt.wire.global_row_id);
                else if (receipt.membership > 0 && config_.sync_filter)
                    sync_set_add(receipt.wire.table_name,receipt.wire.global_row_id);
                db().db().execute("DELETE FROM main._lattice_sync_outbox WHERE sync_id=? AND source_audit_id=? "
                    "AND wire_global_id=?",{config_.sync_id,source,receipt.wire.global_id});
                return true;
            });
        if (!committed.empty()) {
            durable_resolved.insert(durable_resolved.end(),committed.begin(),committed.end());
            global_ids.push_back(wire_id);
        }
    }
    mark_audit_entries_synced_for(db(),normal_ids,config_.sync_id,config_.all_active_sync_ids);
    global_ids.insert(global_ids.end(),normal_ids.begin(),normal_ids.end());
    refresh_delivery_outbox();
    // Remove ACK'd entries from in-flight set; pending mirrors the set size
    // (store, not fetch_sub — an ACK straddling a reconnect clear must not
    // drive the counter negative). Capture the resolved audit ids while the
    // map still has them: mark_audit_entries_synced_for's transactions have
    // already committed above, so advancing the floor past them is safe.
    std::vector<int64_t> resolved_audit_ids = std::move(durable_resolved);
    std::vector<std::string> unmatched;
    size_t matched = 0;
    bool any_open = false;
    {
        std::lock_guard<std::mutex> lock(in_flight_mutex_);
        for (const auto& id : global_ids) {
            auto it = in_flight_ids_.find(sync_event_key(id));
            if (it != in_flight_ids_.end()) {
                if (it->second.audit_id > 0) resolved_audit_ids.push_back(it->second.audit_id);
                in_flight_ids_.erase(it);
                ++matched;
            } else {
                unmatched.push_back(id);
            }
        }
        any_open = !open_audit_ids_.empty();
        progress_pending_upload_.store(
            static_cast<int64_t>(in_flight_ids_.size()), std::memory_order_relaxed);
    }

    // LATE ACKS (Aug 2026 floor-pin incident): an entry released by the ack
    // deadline — or sent during a previous process tenure — still gets its ack
    // delivered here. The DB row was marked synchronized above, but without an
    // in_flight mapping the audit id never left open_audit_ids_: the floor
    // pinned below DB-synchronized rows for the life of the connection
    // (production: 245K such rows above a frozen floor), enumeration skips
    // synchronized rows so nothing ever re-opened them, and every compaction
    // was vetoed. Resolve late acks by looking their audit ids up directly.
    // Gated on the open set being non-empty — in steady state it is empty and
    // this costs nothing; unmatched acks are otherwise common (server re-acks,
    // other devices' traffic) and must stay cheap.
    if (config_.use_upload_floor && any_open && !unmatched.empty()) {
        constexpr size_t kLookupChunk = 500;
        for (size_t start = 0; start < unmatched.size(); start += kLookupChunk) {
            const size_t end = std::min(start + kLookupChunk, unmatched.size());
            std::string placeholders;
            std::vector<column_value_t> params;
            params.reserve(end - start);
            for (size_t i = start; i < end; ++i) {
                placeholders += (i == start ? "?" : ",?");
                params.push_back(unmatched[i]);
            }
            auto rows = db().db().query(
                "SELECT id FROM AuditLog WHERE globalId IN (" + placeholders + ")",
                params);
            std::lock_guard<std::mutex> lock(in_flight_mutex_);
            for (const auto& row : rows) {
                auto it = row.find("id");
                if (it == row.end() || !std::holds_alternative<int64_t>(it->second)) continue;
                const int64_t audit_id = std::get<int64_t>(it->second);
                if (open_audit_ids_.count(audit_id)) {
                    resolved_audit_ids.push_back(audit_id);
                    ++matched;  // a late ack IS progress on our window
                }
            }
        }
    }

    if (matched > 0) {
        last_matching_ack_ns_.store(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count(),
            std::memory_order_relaxed);
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
    // Backoff resets ONLY on acks that matched in-flight entries. During
    // the Aug 2026 incident the channel carried ~2,400 unmatched acked-ids
    // per second (server re-acks + the other device's traffic), each batch
    // resetting the backoff to base — the resend loop never slowed down.
    // Unmatched acks are not progress on OUR window.
    if (matched > 0) {
        ack_resend_failures_.store(0, std::memory_order_relaxed);
    }
    if (should_upload) {
        // The drained in-flight window is a continuation signal for KNOWN
        // pending work — dispatch directly (the pass no-ops if nothing is
        // pending). Coalescing here throttled ACK-driven window progression
        // to one window per coalesce interval.
        invoke_on_scheduler([this] {
            if (is_destroyed_) return;
            upload_pending_changes();
        });
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
                     log_id(), (long long)((now_ms - open_ms) / 1000), prior);
        }
    }
}

void synchronizer_base::schedule_reconnect() {
    bool within_limit = config_.max_reconnect_attempts == 0
        || reconnect_attempts_ < config_.max_reconnect_attempts;
    if (!(should_reconnect_ && !is_connected_ && within_limit)) {
        LOG_INFO("synchronizer", "[%s] schedule_reconnect: NOT reconnecting (should=%d connected=%d within_limit=%d)",
                 log_id(), should_reconnect_ ? 1 : 0, is_connected_ ? 1 : 0, within_limit ? 1 : 0);
    }
    if (should_reconnect_ && !is_connected_ && within_limit) {
#ifdef __EMSCRIPTEN__
        // The browser build is single-threaded (immediate_scheduler runs
        // inline on the main thread): the blocking backoff below would freeze
        // the tab AND prevent the async WebSocket handshake from ever
        // completing. Browser clients reconnect at the app layer (tab
        // visibility / reload) instead.
        LOG_INFO("synchronizer", "[%s] schedule_reconnect: skipped on Emscripten (app-level reconnect)",
                 log_id());
        return;
#endif
        double delay = std::min(
            std::pow(2.0, reconnect_attempts_) * config_.base_delay_seconds,
            config_.max_delay_seconds);
        ++reconnect_attempts_;
        LOG_INFO("synchronizer", "[%s] Scheduling reconnect attempt %d in %.1fs",
                 log_id(), reconnect_attempts_.load(), delay);

        // Schedule reconnection with an interruptible sleep.
        // Short-sleep loop checks is_destroyed_ so that scheduler_->shutdown()
        // in the destructor doesn't block for the full backoff delay.
        const auto generation = connection_generation_.load();
        invoke_on_scheduler([this, delay, generation] {
            auto end = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds(static_cast<int>(delay * 1000));
            while (std::chrono::steady_clock::now() < end) {
                if (is_destroyed_ || !should_reconnect_ ||
                    connection_generation_ != generation) return;
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            std::lock_guard<std::recursive_mutex> lifecycle(connection_mutex_);
            if (is_destroyed_ || !should_reconnect_ ||
                connection_generation_ != generation) return;
            if (!is_connected_) connect();
        });
    }
}

std::optional<std::string> synchronizer_base::get_last_received_event_id() {
    // Slot cursor first (written per applied chunk; survives no-op-suppressed
    // deliveries and compaction). NULL slot → legacy newest-isFromRemote scan,
    // self-seeding the slot so the fallback is only ever taken once per
    // channel and repairs can eventually drop their cursor-row preservation.
    ensure_cursor_column(db().db());
    auto slot = db().db().query(
        "SELECT last_received_event_id FROM _lattice_replication_slots "
        "WHERE sync_id = ? AND last_received_event_id IS NOT NULL",
        {config_.sync_id});
    if (!slot.empty()) {
        auto it = slot[0].find("last_received_event_id");
        if (it != slot[0].end() && std::holds_alternative<std::string>(it->second)) {
            return std::get<std::string>(it->second);
        }
    }
    auto rows = db().db().query(
        "SELECT globalId FROM AuditLog WHERE isFromRemote = 1 ORDER BY id DESC LIMIT 1"
    );
    if (!rows.empty()) {
        auto it = rows[0].find("globalId");
        if (it != rows[0].end() && std::holds_alternative<std::string>(it->second)) {
            const auto& gid = std::get<std::string>(it->second);
            db().db().execute(R"(
                UPDATE _lattice_replication_slots
                SET last_received_event_id = ?
                WHERE sync_id = ? AND last_received_event_id IS NULL
            )", {gid, config_.sync_id});
            return gid;
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
        progress_received_.load(std::memory_order_relaxed),
        config_.sync_id
    };
}

void synchronizer_base::fire_sync_complete(const std::vector<std::string>& ids) {
    on_sync_complete_handler handler;
    {std::lock_guard<std::mutex> lock(event_handler_mutex_);handler=on_sync_complete_;}
    if(handler) handler(ids);
}

void synchronizer_base::fire_error(const std::string& error) {
    on_error_handler handler;
    {std::lock_guard<std::mutex> lock(event_handler_mutex_);handler=on_error_;}
    if(handler) handler(error);
}

void synchronizer_base::fire_state_change(bool connected) {
    on_state_change_handler handler;
    {std::lock_guard<std::mutex> lock(event_handler_mutex_);handler=on_state_change_;}
    if(handler) handler(connected);
}

void synchronizer_base::set_on_progress(on_progress_handler handler) {
    const auto call_owner=callback_owner_.lock();
    LOG_INFO("synchronizer", "[%s] set_on_progress: handler=%s (this=%p, db=%s)",
             log_id(),
             handler ? "SET" : "CLEARED",
             (void*)this, db().config().path.c_str());
    {std::lock_guard<std::mutex> lock(progress_handler_mutex_);on_progress_.swap(handler);}
    // Destroy the previous callback after unlocking; its captures may release/reenter an owner.
}

void synchronizer_base::fire_progress() {
    on_progress_handler handler;
    {
        std::lock_guard<std::mutex> lock(progress_handler_mutex_);
        handler = on_progress_;
    }
    auto p = get_progress();
    LOG_INFO("synchronizer", "[%s] fire_progress: pending=%lld total=%lld acked=%lld received=%lld handler=%s (this=%p)",
             log_id(),
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

synchronizer::synchronizer(std::unique_ptr<lattice_db> db,deferred_init) {
    owned_db_=std::move(db);db_ptr_=owned_db_.get();
}
std::shared_ptr<synchronizer> synchronizer::create_owned_impl(std::unique_ptr<lattice_db> db,const sync_config& config,
                                                            std::unique_ptr<sync_transport> transport,bool supplied,
                                                            std::shared_ptr<void> resources) {
    if(!db) throw std::invalid_argument("Owned synchronizer requires a database");
    auto owner=std::shared_ptr<synchronizer>(new synchronizer(std::move(db),deferred_init{}));
    owner->owner_resources_=std::move(resources);
    owner->callback_owner_=owner;owner->callback_owner_required_=true;
    auto sched=owner->owned_db_->get_scheduler();
    if(!sched) sched=std::make_shared<immediate_scheduler>();
    if(supplied) owner->init_sync(config,std::move(sched),std::move(transport));
    else owner->init_sync(config,std::move(sched));
    return owner;
}
std::shared_ptr<synchronizer> synchronizer::create_owned(std::unique_ptr<lattice_db> db,const sync_config& config,
                                                       std::shared_ptr<void> resources) {
    return create_owned_impl(std::move(db),config,nullptr,false,std::move(resources));
}
std::shared_ptr<synchronizer> synchronizer::create_owned(std::unique_ptr<lattice_db> db,const sync_config& config,
                                                       std::unique_ptr<sync_transport> transport,
                                                       std::shared_ptr<void> resources) {
    return create_owned_impl(std::move(db),config,std::move(transport),true,std::move(resources));
}

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

// ---------------------------------------------------------------------------
// no_history late-binding — see sync.hpp.
// ---------------------------------------------------------------------------
namespace {

/// The no_history column names of `table_name` — read from the
/// `_lattice_meta['trigger_flags:<table>']` marker the trigger installers
/// write (a sorted, comma-joined list). That marker, not a schema registry,
/// is the source of truth: it is what the INSTALLED triggers do, it exists
/// for schemas registered from Swift/C-API (which never touch the C++
/// registry), and it is visible to every process on the file. Empty for
/// link tables, unmarked tables, and stores without a meta table.
std::vector<std::string> no_history_columns(database& db, const std::string& table_name) {
    std::vector<std::string> out;
    if (table_name.empty() || table_name[0] == '_') return out;
    try {
        auto rows = db.query("SELECT value FROM _lattice_meta WHERE key = ?",
                             {std::string("trigger_flags:") + table_name});
        if (rows.empty()) return out;
        auto it = rows[0].find("value");
        if (it == rows[0].end() || !std::holds_alternative<std::string>(it->second)) return out;
        const auto& csv = std::get<std::string>(it->second);
        size_t start = 0;
        while (start <= csv.size()) {
            const size_t comma = csv.find(',', start);
            const auto piece = csv.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
            if (!piece.empty()) out.push_back(piece);
            if (comma == std::string::npos) break;
            start = comma + 1;
        }
    } catch (...) {
        // No meta table (schemaless/raw store) — nothing is flagged.
    }
    return out;
}

std::string quote_sync_identifier(const std::string& name) {
    std::string quoted = "\"";
    for (char c : name) { if (c == '\"') quoted += '\"'; quoted += c; }
    return quoted + "\"";
}

bool value_is_absent(const audit_log_entry& e, const std::string& col) {
    auto it = e.changed_fields.find(col);
    return it == e.changed_fields.end() || it->second.kind == any_property_kind::null_kind;
}

void mark_no_history_nulls(audit_log_entry& entry, const std::vector<std::string>& columns) {
    entry.no_history_nulls.clear();
    for (const auto& col : columns) {
        const auto value = entry.changed_fields.find(col);
        if (value != entry.changed_fields.end() && value->second.kind == any_property_kind::null_kind &&
            std::find(entry.changed_fields_names.begin(), entry.changed_fields_names.end(), col)
                != entry.changed_fields_names.end()) entry.no_history_nulls.push_back(col);
    }
}

} // namespace

std::string no_history_live_values_json(database& db, const std::string& table_name,
                                        const std::string& global_row_id,
                                        const std::vector<std::string>& columns) {
    json out = json::object();
    if (columns.empty() || table_name.empty()) return out.dump();
    std::string select = "SELECT ";
    for (size_t i = 0; i < columns.size(); ++i) {
        if (i) select += ", ";
        select += columns[i];
    }
    select += " FROM " + table_name + " WHERE globalId = ? LIMIT 1";
    auto rows = db.query(select, {global_row_id});
    if (rows.empty()) return out.dump();
    for (const auto& col : columns) {
        auto it = rows[0].find(col);
        if (it == rows[0].end()) continue;
        out[col] = any_property_to_json(any_property::from_column_value(it->second));
    }
    return out.dump();
}

static void late_bind_no_history_impl(database& db, std::vector<audit_log_entry>& entries, bool strict_reads) {
    std::unordered_map<std::string, std::vector<std::string>> flagged_by_table;
    for (auto& e : entries) {
        e.no_history_nulls.clear();
        if (e.operation != "UPDATE" && e.operation != "INSERT") continue;
        auto cached = flagged_by_table.find(e.table_name);
        if (cached == flagged_by_table.end()) {
            cached = flagged_by_table.emplace(e.table_name, no_history_columns(db, e.table_name)).first;
        }
        const auto& cols = cached->second;
        if (cols.empty()) continue;
        if (e.operation == "INSERT") {
            // INSERT history is a complete payload, never a placeholder.
            mark_no_history_nulls(e, cols);
            continue;
        }
        std::vector<std::string> need;
        for (const auto& col : cols) {
            const bool listed = std::find(e.changed_fields_names.begin(), e.changed_fields_names.end(), col)
                                != e.changed_fields_names.end();
            if (listed && value_is_absent(e, col)) need.push_back(col);
        }
        if (need.empty()) continue;

        std::string select = "SELECT ";
        for (size_t i = 0; i < need.size(); ++i) { if (i) select += ", "; select += "live." + quote_sync_identifier(need[i]); }
        select += " FROM " + quote_sync_identifier(e.table_name) + " AS live WHERE globalId = ? LIMIT 1";
        std::vector<database::row_t> rows;
        try {
            rows = db.query(select, {e.global_row_id});
        } catch (const std::exception& ex) {
            if (strict_reads) throw;
            LOG_WARN("sync", "late_bind_no_history: %s.%s read failed: %s",
                     e.table_name.c_str(), e.global_row_id.c_str(), ex.what());
        }
        for (const auto& col : need) {
            auto it = rows.empty() ? database::row_t::const_iterator{} : rows[0].find(col);
            if (!rows.empty() && it != rows[0].end()) {
                e.changed_fields[col] = any_property::from_column_value(it->second);
            } else {
                // Row gone (or column unreadable): never ship a null for a
                // column the receiver may have declared NOT NULL — drop it.
                e.changed_fields.erase(col);
                e.changed_fields_names.erase(
                    std::remove(e.changed_fields_names.begin(), e.changed_fields_names.end(), col),
                    e.changed_fields_names.end());
            }
        }
        mark_no_history_nulls(e, cols);
    }
}

void late_bind_no_history(database& db, std::vector<audit_log_entry>& entries) {
    late_bind_no_history_impl(db, entries, false);
}

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
        entry.synthesized = get_int("synthesized") != 0;

        entries.push_back(entry);
    }

    late_bind_no_history(db, entries);   // fill no_history columns before anyone ships them
    return entries;
}

namespace {
// Selected export is strict: corrupt stored JSON must not quietly become an
// empty mutation through the legacy permissive history parser.
bool selected_value_valid(const json& value) {
    auto integer64 = [](const json& v) {
        return v.is_number_integer() && (!v.is_number_unsigned() || v.get<uint64_t>() <= INT64_MAX);
    };
    if (!value.is_object()) return value.is_null() || value.is_string() || value.is_boolean() ||
        integer64(value) || (value.is_number_float() && std::isfinite(value.get<double>()));
    if (!value.contains("kind") || !value["kind"].is_number_integer() || !value.contains("value")) return false;
    const auto& kind = value["kind"];
    const auto& v = value["value"];
    if (kind == static_cast<int>(any_property_kind::null_kind)) return v.is_null();
    if (kind == static_cast<int>(any_property_kind::string_kind)) return v.is_string();
    if (kind == static_cast<int>(any_property_kind::int_kind))
        return integer64(v) && v.get<int64_t>() >= INT32_MIN && v.get<int64_t>() <= INT32_MAX;
    if (kind == static_cast<int>(any_property_kind::int64_kind)) return integer64(v);
    if (kind == static_cast<int>(any_property_kind::float_kind))
        return v.is_number() && std::isfinite(v.get<float>());
    if (kind == static_cast<int>(any_property_kind::date_kind) ||
        kind == static_cast<int>(any_property_kind::double_kind))
        return v.is_number() && std::isfinite(v.get<double>());
    if (kind == static_cast<int>(any_property_kind::data_kind) && v.is_string()) {
        const auto& hex = v.get_ref<const std::string&>();
        return hex.size() % 2 == 0 && std::all_of(hex.begin(), hex.end(), [](char c) {
            return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f');
        });
    }
    return false;
}

audit_log_entry selected_audit_row(const database::row_t& row) {
    json wire = json::object();
    for (const auto* name : {"id", "rowId"}) {
        const auto& v = row.at(name);
        if (!std::holds_alternative<int64_t>(v)) throw std::invalid_argument("Invalid selected audit integer");
        wire[name] = std::get<int64_t>(v);
    }
    for (const auto* name : {"globalId", "tableName", "operation", "globalRowId", "changedFields", "changedFieldsNames"}) {
        const auto& v = row.at(name);
        if (!std::holds_alternative<std::string>(v)) throw std::invalid_argument("Invalid selected audit text");
        wire[name] = std::get<std::string>(v);
    }
    for (const auto* name : {"isFromRemote", "isSynchronized", "synthesized"}) {
        const auto& v = row.at(name);
        if (!std::holds_alternative<int64_t>(v) ||
            (std::get<int64_t>(v) != 0 && std::get<int64_t>(v) != 1))
            throw std::invalid_argument("Invalid selected audit flag");
        wire[name] = std::get<int64_t>(v) == 1;
    }
    const auto& time = row.at("timestamp");
    if (std::holds_alternative<std::string>(time)) wire["timestamp"] = std::get<std::string>(time);
    else if (std::holds_alternative<int64_t>(time)) wire["timestamp"] = std::to_string(std::get<int64_t>(time));
    else if (std::holds_alternative<double>(time) && std::isfinite(std::get<double>(time)))
        wire["timestamp"] = json(std::get<double>(time)).dump();
    else throw std::invalid_argument("Invalid selected audit timestamp");
    try {
        const auto fields = json::parse(wire["changedFields"].get<std::string>());
        if (!fields.is_object()) throw std::invalid_argument("Invalid selected audit fields");
        for (const auto& [name, value] : fields.items())
            if (name.empty() || !selected_value_valid(value))
                throw std::invalid_argument("Invalid selected audit value");
    } catch (const json::exception&) {
        throw std::invalid_argument("Malformed selected audit fields JSON");
    }
    auto result = entry_from_parsed(wire);
    if (!result) throw std::invalid_argument("Malformed selected audit entry");
    return std::move(*result);
}

std::string selected_identifier(const std::string& value) {
    if (value.empty() || !std::all_of(value.begin(), value.end(), [](unsigned char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
               (c >= '0' && c <= '9') || c == '_';
    })) throw std::invalid_argument("Invalid selected audit identifier");
    return "\"" + value + "\"";
}

size_t selected_length(const database::row_t& row, const char* key, size_t budget) {
    const auto& value = row.at(key);
    if (!std::holds_alternative<int64_t>(value) || std::get<int64_t>(value) < 0 ||
        static_cast<uint64_t>(std::get<int64_t>(value)) > budget)
        throw std::length_error("Selected audit export exceeds byte limit");
    return static_cast<size_t>(std::get<int64_t>(value));
}
} // namespace

std::vector<audit_log_entry> query_audit_log_selected(database& db,
    const std::vector<std::pair<int64_t, std::string>>& identities, size_t max_bytes) {
    constexpr size_t limit = 16 * 1024 * 1024;
    if (identities.size() > 4096 || max_bytes == 0 || max_bytes > limit)
        throw std::length_error("Selected audit export exceeds bounds");
    std::unordered_set<int64_t> ids;
    std::unordered_set<std::string> globals;
    size_t identity_bytes = 0;
    for (const auto& [id, global] : identities) {
        if (global.size() > max_bytes - identity_bytes)
            throw std::length_error("Selected audit identities exceed byte limit");
        identity_bytes += global.size();
        if (id <= 0 || global.empty() ||
            !ids.insert(id).second || !globals.insert(global).second)
            throw std::invalid_argument("Invalid selected audit identity");
    }
    if (identities.empty()) return {};
    // The established statement scope holds the connection and suppresses
    // delivery until this complete read snapshot has unlocked. A bare mutex
    // would allow RELEASE to deliver previously sealed observers under it.
    std::vector<audit_log_entry> result;
    try {
      {
        database::statement_scope capture(db);
        if (db.is_closed()) throw std::invalid_argument("Selected audit store is closed");
        db.execute("SAVEPOINT lattice_selected_audit");
        try {
        size_t used = 0, materialized_bytes = 0;
        for (const auto& [id, global] : identities) {
            const size_t remaining = max_bytes - used;
            // Fetch lengths first: a corrupt/huge row is never materialized.
            const auto sizes = db.query(
                "SELECT coalesce(length(CAST(globalId AS BLOB)),0)+coalesce(length(CAST(tableName AS BLOB)),0)"
                "+coalesce(length(CAST(operation AS BLOB)),0)+coalesce(length(CAST(globalRowId AS BLOB)),0)"
                "+coalesce(length(CAST(changedFields AS BLOB)),0)+coalesce(length(CAST(changedFieldsNames AS BLOB)),0)"
                "+coalesce(length(CAST(timestamp AS BLOB)),0) AS bytes FROM AuditLog WHERE id=? AND globalId=?", {id, global});
            if (sizes.size() != 1) throw std::invalid_argument("Selected audit identity not found");
            const auto raw_bytes = selected_length(sizes[0], "bytes", std::min(remaining, max_bytes - materialized_bytes));
            materialized_bytes += raw_bytes;
            auto rows = db.query("SELECT id,globalId,tableName,operation,rowId,globalRowId,changedFields,"
                "changedFieldsNames,timestamp,isFromRemote,isSynchronized,synthesized FROM AuditLog WHERE id=? AND globalId=?", {id, global});
            if (rows.size() != 1) throw std::invalid_argument("Selected audit identity changed");
            auto entry = selected_audit_row(rows[0]);
            rows.clear();
            if ((entry.operation == "INSERT" || entry.operation == "UPDATE") && entry.table_name[0] != '_') {
                // Bound metadata before reading its CSV, then live values
                // before late binding. A placeholder can expand substantially.
                const auto meta = db.query("SELECT typeof(value) AS valueType, length(CAST(value AS BLOB)) AS bytes, "
                    "length(value)-length(replace(value,',',''))+1 AS columns FROM _lattice_meta WHERE key=?",
                    {std::string("trigger_flags:") + entry.table_name});
                if (!meta.empty()) {
                    const auto& type = meta[0].at("valueType");
                    if (!std::holds_alternative<std::string>(type) || std::get<std::string>(type) != "text")
                        throw std::invalid_argument("Selected NoHistory metadata must be text");
                    selected_length(meta[0], "columns", 4096);
                    // The bounded CSV is parsed here and by the shared late
                    // binder. Charge both materializations before either read.
                    materialized_bytes += 2 * selected_length(meta[0], "bytes", (max_bytes - materialized_bytes) / 2);
                }
                const auto columns = no_history_columns(db, entry.table_name);
                if (columns.size() > 4096) throw std::length_error("Too many selected NoHistory columns");
                selected_identifier(entry.table_name);
                std::string lengths;
                for (const auto& column : columns) {
                    if (column.size() > 1024) throw std::length_error("Selected NoHistory column name exceeds limit");
                    selected_identifier(column);
                    if (entry.operation != "UPDATE" || !value_is_absent(entry, column) ||
                        std::find(entry.changed_fields_names.begin(), entry.changed_fields_names.end(), column)
                          == entry.changed_fields_names.end()) continue;
                    if (!lengths.empty()) lengths += "+";
                    lengths += "coalesce(length(CAST(live." + selected_identifier(column) + " AS BLOB)),0)";
                }
                if (!lengths.empty()) {
                    const auto live = db.query("SELECT " + lengths + " AS bytes FROM " + selected_identifier(entry.table_name)
                        + " AS live WHERE globalId=? LIMIT 1", {entry.global_row_id});
                    if (!live.empty()) materialized_bytes += selected_length(live[0], "bytes",
                        std::min(remaining - raw_bytes, max_bytes - materialized_bytes));
                }
            }
            std::vector<audit_log_entry> single{std::move(entry)};
            late_bind_no_history_impl(db, single, true);
            const size_t expanded = single[0].to_json().size();
            if (expanded > remaining) throw std::length_error("Selected audit export exceeds expanded byte limit");
            used += expanded;
            result.push_back(std::move(single[0]));
        }
        db.execute("RELEASE SAVEPOINT lattice_selected_audit");
        } catch (...) {
            db.execute("ROLLBACK TO SAVEPOINT lattice_selected_audit");
            db.execute("RELEASE SAVEPOINT lattice_selected_audit");
            throw;
        }
      }
    } catch (...) {
        const auto failure = std::current_exception();
        try { db.drain_if_settled(); } catch (...) {}
        std::rethrow_exception(failure);
    }
    db.drain_if_settled();
    return result;
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

    if (db.db().is_in_transaction()) {
        // Caller holds the transaction (nested mark-synced, e.g. from
        // receive_sync_data): delivering now would notify observers
        // mid-transaction — on other handles that read is SQLITE_LOCKED
        // territory. Ride the flush at the caller's commit instead: buffered
        // AuditLog UPDATEs flow through flush_changes pass 1 verbatim and
        // fan out post-commit (docs/design-deferred-memory-delivery.md).
        for (const auto& [row_id, gid] : notify_list) {
            db.append_to_change_buffer("AuditLog", "UPDATE", row_id, gid);
        }
        db.db().mark_txn_dirty();
        return;
    }

    std::vector<lattice_db::change_event> events;
    events.reserve(notify_list.size());
    for (const auto& [row_id, gid] : notify_list) {
        events.emplace_back("AuditLog", "UPDATE", row_id, gid, "");
    }

    // Results spec §2.3: memory stores have no WAL hook, and the mark-synced
    // UPDATE committed above never set the txn-dirty flag (the update hook
    // ignores AuditLog UPDATEs), so the settled drain won't signal either —
    // fire the synchronous invalidation hooks here so coordinators observe
    // the commit (AuditLog-shaped results, e.g. sync-progress UIs, re-derive
    // at the next access). File DBs already signalled from their WAL hook.
    if (db.config().is_in_memory()) {
        db.fire_invalidation_hooks({"AuditLog"},
                                   lattice_db::invalidation_reason::commit);
    }

    if (db.storage_shared_across_instances()) {
        // Shared storage (file DB or shared-cache memory DB): every same-name
        // handle can read the marked rows — fan out exactly like flush_changes.
        instance_registry::instance().for_each_alive(db.config().path,
            [&events](lattice_db* instance) {
                instance->notify_changes_batched(events);
            });
    } else {
        // Isolated :memory: — registry keys collide (":memory:") but storage
        // doesn't; fanning out would deliver phantom events.
        db.notify_changes_batched(events);
    }
}

void mark_audit_entries_synced(lattice_db& db, const std::vector<std::string>& global_ids) {
    // Collect row IDs for observer notification after commit
    std::vector<std::pair<int64_t, std::string>> notify_list;

    // Per-store write gate (results spec §4.1): on shared-cache stores this
    // transaction must not interleave a sibling handle's generation capture
    // (SQLITE_LOCKED both directions). No-op elsewhere.
    lattice_db::store_write_gate_hold write_gate(db);

    // If we're already inside a transaction (e.g. called from receive_sync_data),
    // don't start a nested one — just do the work in the existing transaction.
    bool own_transaction = false;
    if (!db.db().is_in_transaction()) {
        db.db().begin_transaction();
        own_transaction = true;
    }

    try {
        // Chunked commits (only when we own the transaction): a 1000-id ack
        // used to run 2000+ statements in ONE transaction — on the relay that
        // transaction executes on the socket's event loop and starves every
        // other connection sharing it (B3.7).
        constexpr size_t kAckChunk = 100;
        size_t since_commit = 0;
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
            if (own_transaction && ++since_commit >= kAckChunk) {
                db.db().commit();
                db.db().begin_transaction();
                since_commit = 0;
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



static std::vector<int64_t> mark_audit_entries_synced_for_impl(lattice_db& db,
                                   const std::vector<std::string>& global_ids,
                                   const std::string& sync_id,
                                   const std::vector<std::string>& all_active_sync_ids,
                                   const std::function<bool(int64_t, const std::string&)>& before_mark) {
    std::vector<int64_t> committed;
    std::vector<std::pair<int64_t, std::string>> notify_list;

    // Process in chunks of 50 to avoid holding the write lock too long.
    constexpr size_t kChunkSize = 50;
    for (size_t i = 0; i < global_ids.size(); i += kChunkSize) {
        size_t end = std::min(i + kChunkSize, global_ids.size());
        int64_t chunk_max_entry_id = 0;
        std::vector<int64_t> chunk_ids;

        std::vector<std::pair<int64_t,std::string>> chunk_notifications;
        // Same released maintenance ownership as durable admission: refuse an
        // already-owned transaction, settle atomically, then drain off locks.
        detail::sync_delivery_transaction(db, [&] {
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
                if (before_mark && !before_mark(entry_id, gid)) continue;
                chunk_ids.push_back(entry_id);

                if (entry_id > chunk_max_entry_id) {
                    chunk_max_entry_id = entry_id;
                }

                // Insert or update sync state for this sync_id
                db.db().execute(R"(
                    INSERT INTO _lattice_sync_state (audit_entry_id, sync_id, is_synchronized)
                    VALUES (?, ?, 1)
                    ON CONFLICT(audit_entry_id, sync_id) DO UPDATE SET is_synchronized = 1
                )", {entry_id, sync_id});

                // Eager cleanup: check if ALL active sync_ids have synced this entry.
                // A6: count only sync_ids with a LIVE replication slot (see
                // mark_skipped_synced) — stale rows from removed channels must
                // not collapse entries before live channels relay them.
                auto count_rows = db.db().query(
                    "SELECT COUNT(*) as cnt FROM _lattice_sync_state ss "
                    "WHERE ss.audit_entry_id = ? AND ss.is_synchronized = 1 "
                    "AND EXISTS (SELECT 1 FROM _lattice_replication_slots rs "
                    "            WHERE rs.sync_id = ss.sync_id)",
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
                    chunk_notifications.emplace_back(entry_id, gid);
                }
            }

            return true;
        });
        committed.insert(committed.end(), chunk_ids.begin(), chunk_ids.end());
        notify_list.insert(notify_list.end(),chunk_notifications.begin(),chunk_notifications.end());

        // Advance replication slot outside the transaction — crash between
        // commit and advance means the slot is slightly behind, which is safe
        // (conservative compaction).
        if (chunk_max_entry_id > 0) {
            advance_replication_slot(db.db(), sync_id, chunk_max_entry_id);
        }
    }

    notify_observers(db, notify_list);
    return committed;
}

void mark_audit_entries_synced_for(lattice_db& db,
                                   const std::vector<std::string>& global_ids,
                                   const std::string& sync_id,
                                   const std::vector<std::string>& all_active_sync_ids) {
    mark_audit_entries_synced_for_impl(db, global_ids, sync_id, all_active_sync_ids,
        [&](int64_t id, const std::string&) {
            // An unrelated/old original ACK cannot bypass an uncertain derived delivery.
            if(id<=detail::read_sync_reset_cutoff(db.db(),sync_id)) return false;
            return db.db().query("SELECT 1 FROM main._lattice_sync_outbox WHERE sync_id=? "
                "AND source_audit_id=? LIMIT 1", {sync_id,id}).empty();
        });
}

std::vector<audit_log_entry> query_audit_log_for_sync(
        database& db,
        const std::string& sync_id,
        const std::optional<std::vector<sync_filter_entry>>& sync_filter,
        int64_t min_id_exclusive,
        size_t limit, std::optional<int64_t> max_id_inclusive,
        size_t materialized_byte_limit, bool* hit_byte_limit) {
    if (db.is_closed()) throw db_error("Sync pending audit read on closed database");
    if (hit_byte_limit) *hit_byte_limit = false;
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
        // sync_id is embedded in the string-built clause (it repeats per
        // filter entry, so a positional bind would be fragile against the
        // params the caller appends). Escape single quotes — sync_ids carry
        // URLs ("wss:...") and channel names, never trusted input, but
        // correctness costs nothing.
        std::string escaped_sync_id = sync_id;
        for (size_t pos = 0; (pos = escaped_sync_id.find('\'', pos)) != std::string::npos; pos += 2) {
            escaped_sync_id.replace(pos, 1, "''");
        }
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
                       " WHERE sync_id = '" + escaped_sync_id + "'"
                       " AND table_name = a.tableName AND global_row_id = a.globalRowId)"
                       " OR EXISTS (SELECT 1 FROM " + entry.table_name +
                       " WHERE globalId = a.globalRowId AND (" + *entry.where_clause + "))"
                       "))";
            }
        }
        filter_clause += " OR a.tableName LIKE '\\_%' ESCAPE '\\'";
        filter_clause += " OR (a.operation='DELETE' AND a.changedFieldsNames='[\"__lattice_filter_removal\"]')";
        filter_clause += ")";
    }

    // Fresh-peer cohort identity is durable, independent of the optional scan-floor optimization.
    filter_clause+=" AND a.id>"+std::to_string(detail::read_sync_reset_cutoff(db,sync_id));

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
    const std::string upper_bound = max_id_inclusive ? " AND a.id <= ?" : "";
    // Header-only budget candidates, then payload selection under the SAME SQL
    // snapshot. No check-length/getter race and no SELECT * across the backlog.
    std::string projection = "a.*";
    if (materialized_byte_limit > 0) {
        projection = "a.id, 64";
        for (const char* name : {"globalId", "tableName", "operation", "globalRowId",
                                 "changedFields", "changedFieldsNames", "timestamp"}) {
            projection += " + COALESCE(length(CAST(a." + std::string(name) + " AS BLOB)), 0)";
        }
        projection += " AS bytes";
    }
    std::string sql =
        "SELECT " + projection + " FROM AuditLog a"
        " JOIN _lattice_sync_state ss"
        "   ON ss.audit_entry_id = a.id AND ss.sync_id = ?"
        " WHERE ss.is_synchronized = 0" + upper_bound + filter_clause +

        " UNION ALL"

        " SELECT " + projection + " FROM AuditLog a"
        " WHERE a.isSynchronized = 0"
        " AND a.id > ?"
        " AND NOT EXISTS ("
        "   SELECT 1 FROM _lattice_sync_state ss"
        "   WHERE ss.audit_entry_id = a.id AND ss.sync_id = ?"
        ")" + upper_bound + filter_clause +

        " ORDER BY id ASC";

    std::vector<column_value_t> params = {sync_id};
    if (max_id_inclusive) params.push_back(*max_id_inclusive);
    params.push_back(min_id_exclusive);
    params.push_back(sync_id);
    if (max_id_inclusive) params.push_back(*max_id_inclusive);
    if (limit > 0) {
        sql += " LIMIT ?";
        params.push_back(static_cast<int64_t>(limit));
    }
    if (materialized_byte_limit > 0) {
        // The first over-budget row is represented by a payload-free sentinel.
        // A later over-budget row truncates the prefix and reports a continuation
        // even if every returned row is subsequently skipped by the sync filter.
        std::string select = "a.id";
        for (const char* name : {"globalId", "tableName", "operation", "rowId", "globalRowId",
                                 "changedFields", "changedFieldsNames", "timestamp", "isFromRemote",
                                 "isSynchronized", "synthesized"}) {
            select += ", CASE WHEN r.total <= " + std::to_string(materialized_byte_limit) +
                      " THEN a." + name + " ELSE NULL END AS " + name;
        }
        const auto cap = std::to_string(materialized_byte_limit);
        sql = "WITH pending AS (" + sql + "), ranked AS ("
            "SELECT id, SUM(bytes) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) AS total, "
            "ROW_NUMBER() OVER (ORDER BY id) AS ordinal FROM pending) SELECT " + select +
            ", (r.total > " + cap + ") AS __byte_refused, "
            "EXISTS(SELECT 1 FROM ranked WHERE total > " + cap + ") AS __byte_truncated "
            "FROM ranked r JOIN AuditLog a ON a.id = r.id WHERE r.total <= " + cap +
            " OR r.ordinal = 1 ORDER BY a.id";
    }
    auto rows = db.query(sql, params);
    if (db.is_closed()) throw db_error("Sync pending audit read on closed database");
    if (materialized_byte_limit > 0 && !rows.empty()) {
        if (std::get<int64_t>(rows.front().at("__byte_refused")) != 0)
            throw std::length_error("Sync audit row exceeds materialized byte limit");
        if (hit_byte_limit)
            *hit_byte_limit = std::get<int64_t>(rows.front().at("__byte_truncated")) != 0;
    }

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
        entry.synthesized = get_int("synthesized") != 0;

        entries.push_back(entry);
    }

    late_bind_no_history(db, entries);   // the upload path: latest value at upload time
    return entries;
}

std::vector<audit_log_entry> events_after(database& db, const std::optional<std::string>& checkpoint_global_id) {
    if (checkpoint_global_id) {
        // Resolve the checkpoint EXPLICITLY. The filter's correlated subquery
        // ("id > (SELECT id ... WHERE globalId = ?)") yields NULL for an
        // unknown globalId and "id > NULL" matches nothing — a client whose
        // checkpoint was compacted away on this side would receive an EMPTY
        // catch-up forever (silent, permanent divergence). Unknown checkpoint
        // ⇒ serve full history; the receiver's globalId dedup + value-guarded
        // no-op applies make the replay safe and cheap.
        auto rows = db.query("SELECT id FROM AuditLog WHERE globalId = ?",
                             {*checkpoint_global_id});
        if (rows.empty()) {
            LOG_WARN("sync", "events_after: unknown checkpoint '%s' — serving full history",
                     checkpoint_global_id->c_str());
            return query_audit_log(db, false, std::nullopt);
        }
        return query_audit_log(db, false, checkpoint_global_id);
    } else {
        return query_audit_log(db, false, std::nullopt);
    }
}

// =========================================================================
// Replication slot management
// =========================================================================

// Externalized download-resume cursor (guarded ALTER + EAGER seed). The
// cursor used to be derived from the newest isFromRemote AuditLog row — which
// forced every compaction/repair to preserve that row and stalled whenever
// no-op suppression stopped minting. Called from every cursor read/write
// entry point: connect-time reads can run before slot registration, so no
// single site owns the migration. No explicit transaction: the caller may
// already hold one, ALTER is atomic on its own, and a crash between ALTER
// and seed just means the NULL-cursor legacy fallback self-seeds later.
void ensure_cursor_column(database& db) {
    for (const auto& row : db.query("PRAGMA table_info(_lattice_replication_slots)", {})) {
        auto it = row.find("name");
        if (it != row.end() && std::holds_alternative<std::string>(it->second) &&
            std::get<std::string>(it->second) == "last_received_event_id") {
            return;
        }
    }
    try {
        db.execute("ALTER TABLE _lattice_replication_slots "
                   "ADD COLUMN last_received_event_id TEXT", {});
        db.execute(R"(
            UPDATE _lattice_replication_slots
            SET last_received_event_id =
                (SELECT globalId FROM AuditLog
                 WHERE isFromRemote = 1 ORDER BY id DESC LIMIT 1)
            WHERE last_received_event_id IS NULL
        )", {});
    } catch (const std::exception& e) {
        // Racing ALTERs from two connections: one wins, the loser lands here.
        LOG_DEBUG("sync", "ensure_cursor_column: %s", e.what());
    }
}

void ensure_observer_column(database& db) {
    for (const auto& row : db.query("PRAGMA table_info(_lattice_replication_slots)", {})) {
        auto it = row.find("name");
        if (it != row.end() && std::holds_alternative<std::string>(it->second) &&
            std::get<std::string>(it->second) == "is_observer") {
            return;
        }
    }
    try {
        db.execute("ALTER TABLE _lattice_replication_slots "
                   "ADD COLUMN is_observer INTEGER NOT NULL DEFAULT 0", {});
    } catch (const std::exception& e) {
        // Racing ALTERs from two connections: one wins, the loser lands here.
        LOG_DEBUG("sync", "ensure_observer_column: %s", e.what());
    }
}

void register_replication_slot(database& db, const std::string& sync_id, bool is_observer) {
    ensure_cursor_column(db);
    ensure_observer_column(db);
    db.execute(R"(
        INSERT INTO _lattice_replication_slots (sync_id, last_active_at, is_observer)
        VALUES (?, datetime('now'), ?)
        ON CONFLICT(sync_id) DO UPDATE SET last_active_at = datetime('now'),
                                          is_observer = excluded.is_observer
    )", {sync_id, static_cast<int64_t>(is_observer ? 1 : 0)});
}

void set_replication_slot_observer(database& db, const std::string& sync_id, bool is_observer) {
    ensure_observer_column(db);
    db.execute("UPDATE _lattice_replication_slots SET is_observer = ? WHERE sync_id = ?",
               {static_cast<int64_t>(is_observer ? 1 : 0), sync_id});
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

// Wire timestamps arrive as strings (ISO-8601 from older peers, or a
// numeric epoch rendered as text), but AuditLog.timestamp is REAL
// (unixepoch('subsec') on locally-minted rows). Binding the string put
// TEXT into the REAL column — 2.0M production rows invisible to every
// piece of date math (age-based compaction, timeline queries). Convert;
// unparseable input degrades to "now" (a bounded lie beats a typed one).
static double wire_timestamp_to_epoch(const std::string& ts) {
    if (ts.empty()) return static_cast<double>(::time(nullptr));
    // Numeric-as-text (current peers): parse directly.
    {
        char* end = nullptr;
        double v = std::strtod(ts.c_str(), &end);
        if (end && *end == '\0' && v > 0) return v;
    }
    // ISO-8601 "YYYY-MM-DDTHH:MM:SS[.sss]Z" (older peers).
    std::tm tm{};
    if (::strptime(ts.c_str(), "%Y-%m-%dT%H:%M:%S", &tm) != nullptr) {
        double frac = 0.0;
        if (auto dot = ts.find('.'); dot != std::string::npos) {
            frac = std::strtod(ts.c_str() + dot, nullptr);
        }
        return static_cast<double>(::timegm(&tm)) + frac;
    }
    return static_cast<double>(::time(nullptr));
}

// Immutable policy is deliberately enforced INSIDE the model-write transaction,
// before audit-id dedup. INSERT uses an upsert in the ordinary protocol, so an
// operation allowlist alone cannot make a command/event table append-only.
static std::string immutable_lower(std::string text) {
    for (auto& c : text) if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
    return text;
}

static std::string immutable_quote(const std::string& name) {
    std::string result = "\"";
    for (char c : name) { result += c; if (c == '"') result += '"'; }
    return result + "\"";
}

static json immutable_value(const column_value_t& value) {
    return std::visit([](const auto& v) -> json {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            // Distinguish blob bytes from application strings/arrays.
            return json{{"blob", v}};
        } else {
            return json(v);
        }
    }, value);
}

static void enforce_immutable_entry(lattice_db& db, const audit_log_entry& entry,
                                   const std::unordered_set<std::string>& tables) {
    const auto table = immutable_lower(entry.table_name);
    auto fail = [&](const std::string& reason) -> void {
        throw std::runtime_error("immutable sync violation: " + entry.table_name + "." +
                                 entry.global_row_id + ": " + reason);
    };
    // A payload must not overwrite the policy's own durable receipts (or
    // forge an AuditLog that a catching-up peer will execute later).
    if (table.rfind("_lattice_", 0) == 0 || table == "auditlog" || table == "_synccontrol") {
        fail("internal table write");
    }
    auto events = db.db().query(
        "SELECT tableName, globalRowId FROM _lattice_immutable_events WHERE globalId = ?",
        {entry.global_id});
    if (!events.empty() &&
        (std::get<std::string>(events[0].at("tableName")) != table ||
         std::get<std::string>(events[0].at("globalRowId")) != entry.global_row_id)) {
        fail("audit identity was already used for another record");
    }
    if (!tables.count(table)) return;
    if (entry.operation != "INSERT") fail("only complete INSERT snapshots are permitted");
    if (entry.global_id.empty() || entry.global_row_id.empty()) fail("missing identity");
    if (table.empty() || table.front() == '_') fail("only model tables are supported");

    const auto schema = db.get_table_schema(entry.table_name);
    if (schema.empty()) fail("unknown table");
    std::unordered_set<std::string> names(entry.changed_fields_names.begin(), entry.changed_fields_names.end());
    if (names.size() != entry.changed_fields_names.size() || names.size() != entry.changed_fields.size()) {
        fail("field names must match the complete payload without duplicates");
    }
    // No skew dropping or absent-field defaults: an immutable record is a
    // complete snapshot whose meaning cannot change after first acceptance.
    for (const auto& [name, type] : schema) {
        if (name == "id" || name == "globalId") continue;
        if (!names.count(name) || !entry.changed_fields.count(name)) fail("missing field " + name);
    }
    for (const auto& name : names) {
        if (name == "id" || name == "globalId" || !schema.count(name) || !entry.changed_fields.count(name)) {
            fail("unknown, reserved, or absent field " + name);
        }
    }

    // Normalize by SQL representation, not wire order, any_property integer
    // width, sender row id, timestamp, or synthesized flag. TEXT containing
    // JSON remains application text; the application owns its canonicalization.
    json payload = json::object();
    std::string predicate = "globalId = ?";
    std::vector<column_value_t> params{entry.global_row_id};
    for (const auto& name : names) {
        auto value = entry.changed_fields.at(name).to_column_value();
        const auto type = schema.at(name);
        if (type == column_type::blob && std::holds_alternative<std::string>(value)) {
            const auto hex = std::get<std::string>(value);
            if (hex.size() % 2 || hex.find_first_not_of("0123456789abcdefABCDEF") != std::string::npos) {
                fail("invalid hex blob " + name);
            }
            value = db.db().query("SELECT unhex(?) AS value", {value})[0].at("value");
        }
        if (!std::holds_alternative<std::nullptr_t>(value)) {
            const bool integer = std::holds_alternative<int64_t>(value);
            const bool real = std::holds_alternative<double>(value);
            if ((type == column_type::text && !std::holds_alternative<std::string>(value)) ||
                (type == column_type::blob && !std::holds_alternative<std::vector<uint8_t>>(value)) ||
                ((type == column_type::integer || type == column_type::real) && !integer && !real)) {
                fail("field type does not match schema: " + name);
            }
            if (real) {
                const auto number = std::get<double>(value);
                if (!std::isfinite(number) || (type == column_type::integer &&
                    (std::trunc(number) != number || number < -9223372036854775808.0 || number >= 9223372036854775808.0))) {
                    fail("invalid numeric value: " + name);
                }
            }
        }
        // SQLite numeric affinity makes 1 and 1.0 equal; json comparison
        // also compares integer/float values numerically.
        payload[name] = immutable_value(value);
        predicate += " AND " + immutable_quote(name) + " IS ?";
        params.push_back(value);
    }
    const auto quoted_table = immutable_quote(entry.table_name);
    auto rows = db.db().query("SELECT 1 FROM " + quoted_table + " WHERE globalId = ?", {entry.global_row_id});
    if (!rows.empty() && db.db().query("SELECT 1 FROM " + quoted_table + " WHERE " + predicate, params).empty()) {
        fail("existing record has different content");
    }
    auto records = db.db().query(
        "SELECT payload FROM _lattice_immutable_records WHERE tableName = ? AND globalRowId = ?",
        {table, entry.global_row_id});
    if (!records.empty() && json::parse(std::get<std::string>(records[0].at("payload"))) != payload) {
        fail("record identity was already accepted with different content");
    }
    // Do not acknowledge a forged audit-id collision without storing its row.
    auto audits = db.db().query("SELECT tableName, globalRowId FROM AuditLog WHERE globalId = ?", {entry.global_id});
    if (!audits.empty() &&
        (immutable_lower(std::get<std::string>(audits[0].at("tableName"))) != table ||
         std::get<std::string>(audits[0].at("globalRowId")) != entry.global_row_id)) {
        fail("audit identity was already used for another record");
    }
    if (events.empty() && audits.empty() && !db.db().query(
            "SELECT 1 FROM _lattice_applied_receipts WHERE globalId = ?", {entry.global_id}).empty()) {
        fail("legacy audit receipt cannot prove immutable identity");
    }
    db.db().execute("INSERT OR IGNORE INTO _lattice_immutable_records(tableName, globalRowId, payload) VALUES (?, ?, ?)",
                    {table, entry.global_row_id, payload.dump()});
    db.db().execute("INSERT OR IGNORE INTO _lattice_immutable_events(globalId, tableName, globalRowId) VALUES (?, ?, ?)",
                    {entry.global_id, table, entry.global_row_id});
}

enum class audit_apply_mode { global, per_sync, pending_import };
enum class pending_outcome { rejected, imported, already_present };

// One guarded data/journal path. Pending import deliberately has neither the
// receiving channel's ACK/cursor nor its value-identical suppression policy.
static std::vector<std::string> apply_remote_changes_impl(
    lattice_db& db,
    const std::vector<audit_log_entry>& entries,
    audit_apply_mode mode,
    const std::optional<std::string>& receiving_sync_id,
    std::vector<pending_outcome>* outcomes = nullptr,
    const std::vector<std::string>& immutable_tables = {},
    const std::function<bool()>& authorize = {})
{
    std::unordered_set<std::string> immutable;
    for (const auto& table : immutable_tables) immutable.insert(immutable_lower(table));
    const bool atomic_frame = !immutable.empty() || static_cast<bool>(authorize);
    std::vector<std::string> applied_ids;
    if (entries.empty()) {
        if (authorize) sync_apply_transaction_access::run(db, [&] {
            if (!authorize()) throw std::runtime_error("sync authorization denied: this writer lease is no longer active");
        });
        return applied_ids;
    }
    const bool pending_import = mode == audit_apply_mode::pending_import;
    std::unordered_map<std::string, size_t> pending_counts;
    if (pending_import) {
        for (const auto& entry : entries) ++pending_counts[entry.global_id];
    }

    // Signal flush_changes to look up changedFieldsNames from AuditLog for
    // object observers. Only this delivery may clear its flag: an observer
    // can reenter apply, and other callers need not use our scheduler.
    struct applying_flag_scope {
        std::atomic<bool>& flag;
        bool acquired;

        explicit applying_flag_scope(std::atomic<bool>& value) noexcept
            : flag(value), acquired(false) {
            bool expected = false;
            acquired = flag.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel, std::memory_order_acquire);
        }
        ~applying_flag_scope() noexcept {
            if (acquired) flag.store(false, std::memory_order_release);
        }
    } applying_flag(db.applying_remote_changes_);
    if (!applying_flag.acquired) {
        if (atomic_frame) throw std::runtime_error("immutable sync violation: another delivery is applying");
        LOG_WARN("apply_remote", "another delivery is still applying; returning no ACKable entries");
        return applied_ids;
    }

    struct pending_cursor_scope {
        lattice_db& db;
        bool active;
        explicit pending_cursor_scope(lattice_db& owner, bool pending) : db(owner), active(pending) {
            if (active) db.defer_early_audit_cursor_.store(true, std::memory_order_release);
        }
        ~pending_cursor_scope() {
            if (active) db.defer_early_audit_cursor_.store(false, std::memory_order_release);
        }
    } pending_cursor(db, pending_import);

    // Process in chunks to avoid holding the write lock for too long.
    const size_t chunk_size = atomic_frame ? entries.size() : 50;

    // Durable idempotency receipts for suppressed no-op applies: suppression
    // (below) mints no AuditLog row, which is what the globalId dedup keys
    // on — without a receipt, a suppressed entry re-delivered AFTER an
    // intervening local edit would re-apply stale values and relay the
    // reversion fleet-wide. Receipts are globalId-only (~tens of bytes vs a
    // full audit row) and pruned by bounded rowid horizon in compaction.
    // Download-resume cursor (externalized): advanced per chunk to the last
    // CONTIGUOUSLY-acked entry of the delivery. Only meaningful in per-sync
    // mode (clients resuming a download stream); the column is added by
    // register_replication_slot, so probe rather than assume.
    bool cursor_column_ok = false;
    try {
        if (!atomic_frame) db.db().execute(
            "CREATE TABLE IF NOT EXISTS _lattice_applied_receipts ("
            "  globalId TEXT PRIMARY KEY)", {});
        if (receiving_sync_id) {
            auto cols = db.db().query("PRAGMA table_info(_lattice_replication_slots)", {});
            for (const auto& row : cols) {
                auto it = row.find("name");
                if (it != row.end() && std::holds_alternative<std::string>(it->second) &&
                    std::get<std::string>(it->second) == "last_received_event_id") {
                    cursor_column_ok = true;
                    break;
                }
            }
        }
    } catch (const std::exception& e) {
        if (atomic_frame) throw;
        // No apply transaction or entry effects exist yet. Do not roll back
        // an unrelated caller's transaction or ACK an unprocessed entry.
        LOG_WARN("apply_remote", "delivery setup failed (%s); returning no ACKable entries", e.what());
        return applied_ids;
    } catch (...) {
        if (atomic_frame) throw;
        LOG_WARN("apply_remote", "delivery setup failed (non-std exception); returning no ACKable entries");
        return applied_ids;
    }
    std::optional<std::string> cursor_candidate;
    bool cursor_halted = false;

    // B3.4 per-delivery memos: table ensures and schema lookups are per-TABLE
    // facts hoisted out of the per-entry loop.
    std::unordered_set<std::string> tables_ensured;
    std::unordered_set<std::string> vec0_ensured;
    std::unordered_map<std::string, std::unordered_map<std::string, column_type>> schema_cache;
    std::unordered_set<std::string> owned_union_tables;
    std::unordered_map<std::string, std::unordered_set<std::string>> nullable_cache;

    bool chunk_retried = false;
    for (size_t chunk_start = 0; chunk_start < entries.size(); ) {
        size_t chunk_end = std::min(chunk_start + chunk_size, entries.size());
        const size_t applied_before_chunk = applied_ids.size();
        // Snapshots for the failure path: a rolled-back chunk must not leak
        // its cursor progress (advancing the resume cursor past an entry
        // that never committed would create a permanent download hole).
        const auto cursor_candidate_before_chunk = cursor_candidate;
        const bool cursor_halted_before_chunk = cursor_halted;

        try {
            if (test_hooks::apply_chunk_begin) test_hooks::apply_chunk_begin(chunk_start);
            sync_apply_transaction_access::run(db, [&] {
            if (authorize && !authorize()) throw std::runtime_error("sync authorization denied: this writer lease is no longer active");
            if (atomic_frame) {
                db.db().execute("CREATE TABLE IF NOT EXISTS _lattice_applied_receipts (globalId TEXT PRIMARY KEY)");
                db.db().execute("CREATE TABLE IF NOT EXISTS _lattice_immutable_records ("
                    "tableName TEXT NOT NULL, globalRowId TEXT NOT NULL, payload TEXT NOT NULL, "
                    "PRIMARY KEY(tableName, globalRowId))");
                db.db().execute("CREATE TABLE IF NOT EXISTS _lattice_immutable_events ("
                    "globalId TEXT PRIMARY KEY, tableName TEXT NOT NULL, globalRowId TEXT NOT NULL)");
            }

            for (size_t i = chunk_start; i < chunk_end; ++i) {
                const auto& entry = entries[i];
                bool pending_savepoint = false;
                size_t pending_change_checkpoint = 0;

                // Per-entry error isolation — the contract the C ABI and the
                // bridge document: an entry that fails to apply for ANY reason
                // (unknown table from schema drift, instruction generation,
                // bookkeeping inserts) is skipped — NOT acked, so the sender
                // re-sends it — and the remaining entries still apply.
                // Previously only the instruction EXECUTE was isolated: an
                // unknown-table entry threw out of the pre-apply row lookup and
                // aborted the whole batch, and entries applied in earlier
                // committed chunks were never reported (the B-2 pathology at
                // chunk granularity).
                try {
                    // An ambiguous repeated identity cannot become two effects
                    // or two contradictory outcomes, even across chunk bounds.
                    if (pending_import && pending_counts[entry.global_id] != 1) continue;
                    // Typed Core callers bypass wire decoding. Reject invalid
                    // envelopes before dedup and schema-skew ACK shortcuts.
                    if (!remote_entry_envelope_valid(entry)) {
                        if (atomic_frame) throw std::runtime_error("immutable sync violation: invalid entry envelope");
                        LOG_WARN("apply_remote", "invalid entry envelope at position %zu; not ACKing", i);
                        continue;
                    }
                    // Classify from local schema and ownership metadata before
                    // dedup. A malformed discriminator must not be ACKed merely
                    // because its event identity was accepted previously.
                    auto schema_it = schema_cache.find(entry.table_name);
                    if (schema_it == schema_cache.end()) {
                        schema_it = schema_cache.emplace(
                            entry.table_name, db.get_table_schema(entry.table_name)).first;
                        const auto& columns = schema_it->second;
                        if (entry.table_name[0] == '_' && columns.count("id") &&
                            columns.count("globalId") && columns.count("case") &&
                            !db.db().query("SELECT 1 FROM _lattice_meta WHERE key = ? AND value <> ''",
                                {std::string("internal_table:") + entry.table_name}).empty()) {
                            owned_union_tables.insert(entry.table_name);
                        }
                    }
                    const bool is_owned_union = owned_union_tables.count(entry.table_name) != 0;
                    const auto has_field_name = [&](const char* name) {
                        return entry.changed_fields.count(name) ||
                            std::find(entry.changed_fields_names.begin(),
                                      entry.changed_fields_names.end(), name) != entry.changed_fields_names.end();
                    };
                    const bool legacy_discriminator = is_owned_union && has_field_name("\"case\"");
                    if (legacy_discriminator && has_field_name("case")) {
                        throw std::runtime_error("Duplicate union discriminator in sync entry");
                    }
                    if (atomic_frame) enforce_immutable_entry(db, entry, immutable);
                    // Re-check inside transaction (entry may have been inserted
                    // concurrently). A hit in EITHER AuditLog or the receipts
                    // table means already-applied: ack and move on.
                    auto existing = db.db().query(
                        "SELECT 1 FROM AuditLog WHERE globalId = ?1 "
                        "UNION ALL "
                        "SELECT 1 FROM _lattice_applied_receipts WHERE globalId = ?1 "
                        "LIMIT 1",
                        {entry.global_id}
                    );
                    if (!existing.empty()) {
                        applied_ids.push_back(entry.global_id);
                        if (outcomes) (*outcomes)[i] = pending_outcome::already_present;
                        continue;
                    }

                    // Ordinary receive retains its existing isolation policy.
                    // Rescue must keep the model write and upload journal
                    // atomic, including trigger/schema side effects.
                    if (pending_import) {
                        pending_change_checkpoint = db.pending_change_buffer_checkpoint();
                        db.db().execute("SAVEPOINT lattice_pending_entry");
                        pending_savepoint = true;
                    }

                    // An underscore alone grants no internal-table authority.
                    // Retain current physical/wire link classification and retry
                    // behavior; an owned union is a model, never a link.
                    if (!entry.table_name.empty() && entry.table_name[0] == '_' &&
                        !is_owned_union && !schema_registry::instance().get_schema(entry.table_name)) {
                        const auto& columns = schema_it->second;
                        const bool missing = columns.empty();
                        auto wire_has = [&](const char* name) {
                            return entry.changed_fields.count(name) &&
                                std::find(entry.changed_fields_names.begin(),
                                          entry.changed_fields_names.end(), name)
                                    != entry.changed_fields_names.end();
                        };
                        const bool is_link = missing
                            ? wire_has("lhs") && wire_has("rhs") &&
                                !wire_has("case") && !wire_has("\"case\"")
                            : columns.count("lhs") && columns.count("rhs") &&
                                !columns.count("case") && !columns.count("id");
                        if (is_link) {
                            const bool is_virtual = missing ? wire_has("rhs_type")
                                                            : columns.count("rhs_type") != 0;
                            const auto key = entry.table_name + (is_virtual ? "|v" : "|l");
                            if (!tables_ensured.count(key)) {
                                if (is_virtual) db.ensure_virtual_link_table(entry.table_name);
                                else db.ensure_link_table(entry.table_name);
                                // Failed ensures must be retried, not treated as
                                // successful on the next entry in this delivery.
                                if (missing) schema_it->second = db.get_table_schema(entry.table_name);
                                tables_ensured.insert(key);
                            }
                        } else {
                            throw std::runtime_error("Unrecognized internal table in sync entry");
                        }
                    }

                    // Ensure vec0 tables exist for vector columns BEFORE the INSERT,
                    // so triggers can populate the shadow tables — once per
                    // (table, prop) per delivery.
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
                                            if (dims > 0 &&
                                                vec0_ensured.insert(entry.table_name + "|" + prop.name).second) {
                                                db.ensure_vec0_table(entry.table_name, prop.name, dims);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Generate and execute the SQL instruction
                    LOG_DEBUG("apply_remote", "entry[%zu]: table=%s op=%s globalRowId=%s changedFields=%zu changedFieldsNames=%zu",
                             i, entry.table_name.c_str(), entry.operation.c_str(),
                             entry.global_row_id.c_str(),
                             entry.changed_fields.size(), entry.changed_fields_names.size());
                    for (const auto& [key, val] : entry.changed_fields) {
                        LOG_DEBUG("apply_remote", "  field: %s kind=%d", key.c_str(), static_cast<int>(val.kind));
                    }
                    for (const auto& name : entry.changed_fields_names) {
                        LOG_DEBUG("apply_remote", "  fieldName: %s", name.c_str());
                    }
                    // Existing tables use one schema read per delivery. The
                    // on-demand link path refreshes it after successful creation.
                    const auto& schema = schema_it->second;
                    bool is_model_table = entry.table_name[0] != '_' || is_owned_union;
                    // Live union triggers historically wrote the quoted name;
                    // PRAGMA-based snapshots write the canonical raw name. Copy
                    // only this legacy case, retaining original wire bytes for
                    // the audit/relay record and all ordinary entries by reference.
                    std::optional<audit_log_entry> union_normalized;
                    const audit_log_entry* normalized_entry = &entry;
                    if (legacy_discriminator) {
                        union_normalized.emplace(entry);
                        auto discriminator = union_normalized->changed_fields.extract("\"case\"");
                        if (!discriminator.empty()) {
                            discriminator.key() = "case";
                            union_normalized->changed_fields.insert(std::move(discriminator));
                        }
                        for (auto& name : union_normalized->changed_fields_names) {
                            if (name == "\"case\"") name = "case";
                        }
                        normalized_entry = &*union_normalized;
                    }

                    // A3 — schema-skew tolerance. A NEWER peer's entries may
                    // reference a table or columns this build doesn't have.
                    // The old behavior generated SQL naming them; the execute
                    // failed; the entry was skipped WITHOUT ack — the sender
                    // re-sent the same page forever (a silent sync wedge) and
                    // the receiver lost the entire row, not just the new column.
                    //
                    // Unknown TABLE: this build can never apply the entry —
                    // skip it and ACK (loudly). The row appears after upgrade
                    // via catch-up/reconcile; the schema handshake surfaces
                    // the drift as a visible warning instead of a wedge.
                    if (is_model_table && schema.empty()) {
                        if (atomic_frame) throw std::runtime_error("immutable sync violation: unknown table");
                        if (pending_import)
                            throw std::invalid_argument("Pending entry has an unsupported table");
                        LOG_ERROR("apply_remote",
                                  "unknown table '%s' (schema drift — newer peer); "
                                  "acking entry %s as inapplicable no-op",
                                  entry.table_name.c_str(), entry.global_id.c_str());
                        applied_ids.push_back(entry.global_id);
                        continue;
                    }

                    if (pending_import) {
                        for (const auto& name : normalized_entry->changed_fields_names) {
                            if (name == "__lattice_filter_removal" && entry.operation == "DELETE") continue;
                            if (!schema.count(name))
                                throw std::invalid_argument("Pending entry has an unsupported column");
                        }
                        for (const auto& [name, value] : normalized_entry->changed_fields) {
                            (void)value;
                            if (!schema.count(name))
                                throw std::invalid_argument("Pending entry has an unsupported value column");
                        }
                    }

                    // Unknown COLUMNS: drop them (loudly) and apply the rest.
                    // An UPDATE whose fields are ALL unknown becomes an acked
                    // no-op — the accepted skew semantic (e.g. a v2 client
                    // never sees v3 tombstone fields until it upgrades).
                    audit_log_entry skew_filtered;
                    const audit_log_entry* apply_entry = normalized_entry;
                    if (is_model_table &&
                        (entry.operation == "INSERT" || entry.operation == "UPDATE")) {
                        bool has_unknown = false;
                        for (const auto& name : normalized_entry->changed_fields_names) {
                            if (schema.find(name) == schema.end()) { has_unknown = true; break; }
                        }
                        if (has_unknown) {
                            if (atomic_frame) throw std::runtime_error("immutable sync violation: unknown column");
                            skew_filtered = *normalized_entry;
                            auto& names = skew_filtered.changed_fields_names;
                            for (auto it = names.begin(); it != names.end();) {
                                if (schema.find(*it) == schema.end()) {
                                    LOG_WARN("apply_remote",
                                             "dropping unknown column '%s.%s' (schema drift — newer peer)",
                                             entry.table_name.c_str(), it->c_str());
                                    skew_filtered.changed_fields.erase(*it);
                                    it = names.erase(it);
                                } else {
                                    ++it;
                                }
                            }
                            apply_entry = &skew_filtered;
                        }
                    }

                    // Unmarked NULL is still a legacy history placeholder.
                    // Marked NULL is an intentional clear only when the local
                    // physical column permits it. Keep the NOT NULL anti-wedge
                    // guard even for newer peers with a different schema.
                    if (is_model_table &&
                        (entry.operation == "INSERT" || entry.operation == "UPDATE")) {
                        std::vector<std::string> drop;
                        auto nullable = nullable_cache.find(entry.table_name);
                        if (nullable == nullable_cache.end()) {
                            std::unordered_set<std::string> columns;
                            for (const auto& row : db.db().query("PRAGMA table_info(" + entry.table_name + ")")) {
                                const auto name = row.find("name"), required = row.find("notnull"), pk = row.find("pk");
                                if (name != row.end() && required != row.end() && pk != row.end() &&
                                    std::holds_alternative<std::string>(name->second) &&
                                    std::holds_alternative<int64_t>(required->second) &&
                                    std::holds_alternative<int64_t>(pk->second) &&
                                    std::get<int64_t>(required->second) == 0 && std::get<int64_t>(pk->second) == 0)
                                    columns.insert(std::get<std::string>(name->second));
                            }
                            nullable = nullable_cache.emplace(entry.table_name, std::move(columns)).first;
                        }
                        for (const auto& col : no_history_columns(db.db(), entry.table_name)) {
                            const auto& names = apply_entry->changed_fields_names;
                            if (std::find(names.begin(), names.end(), col) != names.end() &&
                                value_is_absent(*apply_entry, col) &&
                                !(nullable->second.count(col) &&
                                  std::find(entry.no_history_nulls.begin(), entry.no_history_nulls.end(), col)
                                    != entry.no_history_nulls.end())) {
                                drop.push_back(col);
                            }
                        }
                        if (!drop.empty()) {
                            if (apply_entry != &skew_filtered) skew_filtered = *normalized_entry;
                            for (const auto& col : drop) {
                                skew_filtered.changed_fields.erase(col);
                                auto& names = skew_filtered.changed_fields_names;
                                names.erase(std::remove(names.begin(), names.end(), col), names.end());
                            }
                            apply_entry = &skew_filtered;
                        }
                    }

                    auto [sql, params] = apply_entry->generate_instruction(schema);
                    LOG_DEBUG("apply_remote", "  SQL: %s (params=%zu)", sql.c_str(), params.size());

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

                    // A5 — synthesized full-row snapshots are insert-if-absent:
                    // the sender built them from ITS local state, which may be
                    // stale relative to this copy (re-expose/re-join/nuclear-
                    // compact). If the row already exists here, skip the SQL —
                    // an unconditional upsert would revert newer edits and
                    // resurrect tombstones. Bookkeeping + ack still run.
                    bool sql_succeeded = true;
                    bool sql_executed = false;
                    if (entry.synthesized && row_existed) {
                        LOG_DEBUG("apply_remote",
                                  "synthesized INSERT for existing row %s.%s — insert-if-absent skip",
                                  entry.table_name.c_str(), entry.global_row_id.c_str());
                    } else if (!sql.empty()) {
                        try {
                            db.db().execute(sql, params);
                            sql_executed = true;
                        } catch (const std::exception& e) {
                            LOG_ERROR("apply_remote", "Failed: %s (SQL: %s)", e.what(), sql.c_str());
                            sql_succeeded = false;
                        }
                    }

                    if (!sql_succeeded) {
                        if (atomic_frame) throw std::runtime_error("immutable sync violation: model write failed");
                        if (pending_import) throw std::runtime_error("Pending model write failed");
                        continue;
                    }

                    // Check the stored representation too: no-history/default handling
                    // must not commit a different row from the accepted immutable snapshot.
                    if (atomic_frame) enforce_immutable_entry(db, entry, immutable);

                    // NO-OP SUPPRESSION (per-sync/client mode only). With the
                    // value-guarded instructions, changes()==0 on an EXISTING
                    // row means every incoming value equals the stored value —
                    // there is nothing to relay, so minting a full audit row
                    // per redundant delivery (2.3M rows in 4h during the Aug
                    // 2026 flood) is pure damage. Scope guards, each one
                    // red-team-mandated:
                    //   - per-sync mode only: the server's global-mode path
                    //     must keep minting for every acked entry (fan-out +
                    //     last-event-id resume depend on ack'd globalIds being
                    //     checkpoint-resolvable server-side);
                    //   - row_existed: an ABSENT row can be a filter-removal
                    //     value-hole with the row live downstream — today's
                    //     phantom mint is what relays the edit onward;
                    //   - sql_executed: the A5 synthesized-skip branch never
                    //     ran SQL, so changes() would be stale garbage there.
                    // A durable receipt replaces the audit row for dedup, and
                    // the entry is still ACKED (the sender must retire it).
                    if (receiving_sync_id && is_model_table && row_existed &&
                        sql_executed && db.db().changes() == 0) {
                        db.db().execute(
                            "INSERT OR IGNORE INTO _lattice_applied_receipts (globalId) VALUES (?)",
                            {entry.global_id});
                        applied_ids.push_back(entry.global_id);
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
                    //
                    // A4 — marked filter-removal DELETEs never relay further:
                    // recorded fully-synchronized so no other synchronizer on
                    // this database ever enumerates them as pending. They
                    // clear this mirror (one IPC hop) and stop dead — with
                    // per-sync bookkeeping alone, an unfiltered uplink (the
                    // spoke's WSS) would enumerate the entry as pending and
                    // upload the DELETE to the shared server DB.
                    bool is_filter_removal = entry.operation == "DELETE" &&
                        std::find(entry.changed_fields_names.begin(),
                                  entry.changed_fields_names.end(),
                                  "__lattice_filter_removal") != entry.changed_fields_names.end();
                    int is_synchronized = (mode != audit_apply_mode::global && !is_filter_removal) ? 0 : 1;
                    std::string insert_sql = R"(
                        INSERT INTO AuditLog (globalId, tableName, operation, rowId, globalRowId,
                            changedFields, changedFieldsNames, timestamp, isFromRemote, isSynchronized, synthesized)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                    )";
                    db.db().execute(insert_sql, {
                        entry.global_id,
                        entry.table_name,
                        local_operation,
                        local_row_id,
                        entry.global_row_id,
                        entry.changed_fields_to_json(),
                        entry.changed_fields_names_to_json(),
                        wire_timestamp_to_epoch(entry.timestamp),
                        static_cast<int64_t>(is_synchronized),
                        static_cast<int64_t>(entry.synthesized ? 1 : 0)
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

                    if (pending_savepoint) {
                        db.db().execute("RELEASE SAVEPOINT lattice_pending_entry");
                        pending_savepoint = false;
                    }
                    applied_ids.push_back(entry.global_id);
                    if (outcomes) (*outcomes)[i] = pending_outcome::imported;
                } catch (const std::exception& e) {
                    if (atomic_frame) throw;
                    if (pending_savepoint) {
                        // If cleanup itself fails, let the enclosing chunk
                        // roll back; continuing could commit an unjournalled
                        // model mutation. Its outcomes are rolled back too.
                        db.db().execute("ROLLBACK TO SAVEPOINT lattice_pending_entry");
                        db.restore_pending_change_buffer(pending_change_checkpoint);
                        db.db().execute("RELEASE SAVEPOINT lattice_pending_entry");
                        tables_ensured.clear(); vec0_ensured.clear();
                        schema_cache.clear(); nullable_cache.clear(); owned_union_tables.clear();
                    }
                    LOG_ERROR("apply_remote", "entry[%zu] skipped (table=%s, op=%s): %s",
                              i, entry.table_name.c_str(), entry.operation.c_str(), e.what());
                    continue;
                }
            }

            // Advance the download-resume cursor to the last CONTIGUOUSLY
            // acked entry of this delivery, inside the same transaction as
            // the applies it covers. Contiguous-prefix semantics: a failed
            // (un-acked) entry halts the cursor so the server re-delivers
            // from it on the next reconnect; suppressed no-ops DO advance it
            // (they are acked and receipt-backed), which is what keeps a
            // fully-suppressed chunk from stalling resume.
            if (receiving_sync_id && cursor_column_ok && !cursor_halted) {
                std::unordered_set<std::string> chunk_acked;
                for (size_t k = applied_before_chunk; k < applied_ids.size(); ++k) {
                    chunk_acked.insert(applied_ids[k]);
                }
                for (size_t k = chunk_start; k < chunk_end; ++k) {
                    // An invalid typed entry may reuse an earlier valid ID;
                    // set membership alone is not proof of this position.
                    if (!remote_entry_envelope_valid(entries[k]) ||
                        !chunk_acked.count(entries[k].global_id)) {
                        cursor_halted = true;
                        break;
                    }
                    cursor_candidate = entries[k].global_id;
                }
                if (cursor_candidate) {
                    db.db().execute(R"(
                        UPDATE _lattice_replication_slots
                        SET last_received_event_id = ?, last_active_at = datetime('now')
                        WHERE sync_id = ?
                    )", {*cursor_candidate, *receiving_sync_id});
                }
            }

            if (authorize && !authorize()) throw std::runtime_error("sync authorization denied: this writer lease is no longer active");
            });
        } catch (const std::exception& e) {
            if (atomic_frame) throw;
            // A CHUNK-level failure (BEGIN lock timeout, COMMIT busy, the
            // trigger-gate UPDATEs) rolls back this chunk only. It must
            // NEVER throw out of the apply: entries committed by EARLIER
            // chunks are durably applied, and losing the applied_ids vector
            // to an unwind acks them to nobody — the sender then re-sends
            // the whole window forever (the Aug 13 IPC livelock: spoke→hub
            // frozen at progress_acked=215008, four windows cycling at
            // their 300s backoff caps while committed work went unacked).
            // Policy: retry the chunk once, then stop the DELIVERY and
            // return what committed; the sender re-sends only the rest.
            // The owned helper rolls back only a transaction it admitted.
            // A failed BEGIN must never roll back another caller's work.
            applied_ids.resize(applied_before_chunk);
            if (outcomes) std::fill(outcomes->begin() + chunk_start,
                                    outcomes->begin() + chunk_end, pending_outcome::rejected);
            cursor_candidate = cursor_candidate_before_chunk;
            cursor_halted = cursor_halted_before_chunk;
            // The per-delivery memos may record DDL that the rollback undid.
            tables_ensured.clear();
            vec0_ensured.clear();
            schema_cache.clear();
            nullable_cache.clear();
            owned_union_tables.clear();
            if (!chunk_retried) {
                chunk_retried = true;
                LOG_WARN("apply_remote", "chunk [%zu,%zu) failed (%s) — retrying once",
                         chunk_start, chunk_end, e.what());
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                continue;
            }
            LOG_WARN("apply_remote",
                     "chunk [%zu,%zu) failed twice (%s) — stopping this delivery; "
                     "%zu already-committed entries will be acked, the sender re-sends the rest",
                     chunk_start, chunk_end, e.what(), applied_ids.size());
            break;
        } catch (...) {
            if (atomic_frame) throw;
            // Non-std throw: same containment policy, no retry (unknown cause).
            applied_ids.resize(applied_before_chunk);
            if (outcomes) std::fill(outcomes->begin() + chunk_start,
                                    outcomes->begin() + chunk_end, pending_outcome::rejected);
            cursor_candidate = cursor_candidate_before_chunk;
            cursor_halted = cursor_halted_before_chunk;
            tables_ensured.clear();
            vec0_ensured.clear();
            schema_cache.clear();
            nullable_cache.clear();
            owned_union_tables.clear();
            LOG_WARN("apply_remote",
                     "chunk [%zu,%zu) failed (non-std exception) — stopping this delivery; "
                     "%zu already-committed entries will be acked",
                     chunk_start, chunk_end, applied_ids.size());
            break;
        }
        chunk_start = chunk_end;
        chunk_retried = false;
    }

    // vec0 INSERT/UPDATE/DELETE triggers fire inside the transaction above on
    // THIS connection, so the shadow table is already up-to-date. Other
    // connections will reconcile lazily via knn_query's count-mismatch check.

    return applied_ids;
}

std::vector<std::string> apply_remote_changes(lattice_db& db, const std::vector<audit_log_entry>& entries) {
    return apply_remote_changes_impl(db, entries, audit_apply_mode::global, std::nullopt);
}

std::vector<std::string> apply_remote_changes_immutable(lattice_db& db,
    const std::vector<audit_log_entry>& entries,
    const std::vector<std::string>& immutable_tables,
    const std::function<bool()>& authorize) {
    return apply_remote_changes_impl(db, entries, audit_apply_mode::global,
                                    std::nullopt, nullptr, immutable_tables, authorize);
}

void mark_audit_entries_synced_authorized(lattice_db& db,
    const std::vector<std::string>& global_ids, const std::function<bool()>& authorize) {
    sync_apply_transaction_access::run(db, [&] {
        if (authorize && !authorize()) throw std::runtime_error("sync authorization denied: this writer lease is no longer active");
        mark_audit_entries_synced(db, global_ids);
        if (authorize && !authorize()) throw std::runtime_error("sync authorization denied: this writer lease is no longer active");
    });
}

std::vector<std::string> apply_remote_changes_for(lattice_db& db,
                              const std::vector<audit_log_entry>& entries,
                              const std::string& sync_id) {
    return apply_remote_changes_impl(db, entries, audit_apply_mode::per_sync, sync_id);
}

pending_import_result apply_pending_changes(lattice_db& db,
                              const std::vector<audit_log_entry>& entries) {
    // Never roll back or commit a transaction owned by the caller. The
    // shared receive implementation owns complete write chunks.
    if (db.db().is_in_transaction())
        throw std::invalid_argument("Pending import requires no active transaction");
    std::vector<pending_outcome> outcomes(entries.size(), pending_outcome::rejected);
    apply_remote_changes_impl(db, entries, audit_apply_mode::pending_import, std::nullopt, &outcomes);
    pending_import_result result;
    std::unordered_set<std::string> seen;
    for (size_t i = 0; i < entries.size(); ++i) {
        const auto& id = entries[i].global_id;
        if (!seen.insert(id).second) continue;
        switch (outcomes[i]) {
            case pending_outcome::imported: result.imported_ids.push_back(id); break;
            case pending_outcome::already_present: result.already_present_ids.push_back(id); break;
            case pending_outcome::rejected: result.rejected_ids.push_back(id); break;
        }
    }
    return result;
}

} // namespace lattice
