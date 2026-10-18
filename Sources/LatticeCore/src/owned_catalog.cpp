#include <lattice/store_physical_api.hpp>
#include "database_sql_dispatcher.hpp"
#include <lattice/owned_catalog.hpp>
#include <lattice/lattice.hpp>
#include <nlohmann/json.hpp>
#include <algorithm>
#include <array>
#include <cstring>
#include <exception>
#include <limits>
#include <set>
#include <thread>

namespace lattice {
namespace {
using json = nlohmann::json;
using clock = std::chrono::steady_clock;
constexpr size_t chunk_bytes = 65536;
constexpr size_t metadata_bytes = 1048576;
// The existing shared SDK executor runs two native jobs. The same native cap
// also bounds retained UNKNOWN originals; it does not increase worker/queue caps.
constexpr size_t native_slots = 2;
struct capture_bank {
    const uint64_t creator = detail::database_native_process();
    std::mutex mutex;
    std::array<std::shared_ptr<owned_catalog_state>, native_slots> originals;
};
capture_bank& bank() { static auto* value = new capture_bank; return *value; }
struct callback_frame { owned_catalog_state* state; callback_frame* previous; };
thread_local callback_frame* callback_top = nullptr;
void require(bool value, const char* message) { if (!value) throw db_error(message); }
void require_sql(int actual, int expected, const char* stage) {
    if (actual != expected) throw db_error(std::string(stage) + " [sqlite=" + std::to_string(actual) + ']');
}
std::string quote(const std::string& value) {
    std::string out = "\"";
    for (char c : value) { out += c; if (c == '"') out += '"'; }
    return out + '"';
}
struct utf8_state {
    uint32_t code = 0, minimum = 0;
    unsigned pending = 0;
    void accept(const uint8_t* data, size_t length) {
        for (size_t i = 0; i != length; ++i) {
            const auto byte = data[i];
            if (pending) {
                require((byte & 0xc0) == 0x80, "catalog text is not UTF8");
                code = (code << 6) | (byte & 63);
                if (--pending == 0)
                    require(code >= minimum && code <= 0x10ffff && !(code >= 0xd800 && code <= 0xdfff),
                            "catalog text is not UTF8");
            } else if (byte < 0x80) { /* Stored NUL remains a real value byte. */ }
            else if (byte >= 0xc2 && byte <= 0xdf) { pending = 1; minimum = 0x80; code = byte & 31; }
            else if (byte >= 0xe0 && byte <= 0xef) { pending = 2; minimum = 0x800; code = byte & 15; }
            else if (byte >= 0xf0 && byte <= 0xf4) { pending = 3; minimum = 0x10000; code = byte & 7; }
            else require(false, "catalog text is not UTF8");
        }
    }
    void finish() const { require(pending == 0, "catalog text ends inside a UTF8 scalar"); }
};
bool identifier(const std::string& value) {
    if (value.empty() || value.size() > 1024 || value.find('\0') != std::string::npos) return false;
    try { utf8_state state; state.accept(reinterpret_cast<const uint8_t*>(value.data()), value.size()); state.finish(); }
    catch (...) { return false; }
    return true;
}
bool uuid(const std::string& value) {
    if (value.size() != 36) return false;
    for (size_t i = 0; i != value.size(); ++i) {
        const char c = value[i];
        if (i == 8 || i == 13 || i == 18 || i == 23) { if (c != '-') return false; }
        else if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) return false;
    }
    return true;
}
std::vector<std::string> parse_tables(const std::string& data) {
    require(data.size() <= chunk_bytes, "catalog table request exceeds its bound");
    std::vector<std::set<std::string>> keys;
    const auto object = json::parse(data, [&](int depth, json::parse_event_t event, json& value) {
        require(depth <= 8, "catalog table request nesting is invalid");
        if (event == json::parse_event_t::object_start) keys.emplace_back();
        if (event == json::parse_event_t::key)
            require(!keys.empty() && keys.back().insert(value.get<std::string>()).second, "catalog table request repeats a key");
        if (event == json::parse_event_t::object_end) keys.pop_back();
        return true;
    });
    require(object.is_object() && object.size() == 2 && object.contains("version") && object.contains("tables") &&
        object.at("version").is_number_integer() && object.at("version").get<int64_t>() == 1 &&
        object.at("tables").is_array() && !object.at("tables").empty() && object.at("tables").size() <= 64,
        "catalog table request shape is invalid");
    std::vector<std::string> tables;
    std::set<std::string> seen;
    for (const auto& entry : object.at("tables")) {
        require(entry.is_string(), "catalog table request requires names");
        auto name = entry.get<std::string>();
        require(identifier(name) && seen.insert(name).second, "catalog table name is invalid or repeated");
        tables.push_back(std::move(name));
    }
    return tables;
}
}

struct owned_catalog_state {
    const uint64_t creator = detail::database_native_process();
    std::shared_ptr<lattice_db> owner;
    const std::vector<std::string> tables;
    std::shared_ptr<database_read_control> control = std::make_shared<database_read_control>();
    std::atomic<bool> claimed{false}, callback_violation{false};
    std::shared_ptr<database> writer, ordinary_reader, reader;
    std::shared_ptr<const physical_store_identity> identity;
    projection_store_ticket stores;
    uint64_t revision = 0;
    sqlite3* handle = nullptr;
    sqlite3_stmt* statement = nullptr;
    sqlite3_blob* blob = nullptr;
    bool began = false, installed = false, slot_held = false;
    size_t slot = native_slots;
    std::exception_ptr primary;
    catalog_stream_result outcome;

    owned_catalog_state(std::shared_ptr<lattice_db> value, std::vector<std::string> names, clock::time_point end)
        : owner(std::move(value)), tables(std::move(names)) {
        control->deadline = end;
    }
    bool current_process() const noexcept { return creator == detail::database_native_process(); }
    void check_owner() {
        require(current_process() && owner && owner->owns_creator_process(), "catalog owner was inherited");
        require(!callback_violation.load(std::memory_order_acquire), "catalog callback attempted owner reentry");
        require(!control->stopped(), "catalog operation stopped or expired");
        require(!owner->closed_.load(std::memory_order_acquire), "catalog owner closed during capture");
    }
    std::unique_lock<std::mutex> ownership() {
        std::unique_lock<std::mutex> lock(owner->connection_ownership_mutex_, std::defer_lock);
        while (!lock.try_lock()) { check_owner(); std::this_thread::sleep_for(std::chrono::milliseconds(1)); }
        check_owner(); return lock;
    }
    void check() {
        check_owner();
        if (reader) {
            require(!reader->is_closed() && !reader->raw_handle_escaped_.load(std::memory_order_acquire),
                    "catalog original reader closed or escaped");
            const auto lock = ownership();
            require(owner->connection_revision_ == revision && owner->xproc_read_db_ == reader,
                    "catalog original reader was retired or replaced");
        }
    }
    void reserve(const std::shared_ptr<owned_catalog_state>& self) {
        auto& active = bank();
        require(active.creator == creator, "catalog admission belongs to another process");
        std::lock_guard<std::mutex> lock(active.mutex);
        for (size_t i = 0; i != native_slots; ++i) if (!active.originals[i]) {
            active.originals[i] = self; slot = i; slot_held = true; return;
        }
        throw db_error("catalog native admission full or retained cleanup debt");
    }
    void release_known() noexcept {
        if (!slot_held || !current_process()) return;
        std::shared_ptr<owned_catalog_state> retired;
        {
            auto& active = bank();
            std::lock_guard<std::mutex> lock(active.mutex);
            if (slot < native_slots && active.originals[slot].get() == this) {
                retired = std::move(active.originals[slot]); slot_held = false;
            }
        }
        // Final parent/control/string/custody destruction occurs OFF the leaf.
    }
    void remember_message() noexcept {
        const char* message = outcome.cleanup_failed_ ? "catalog cleanup failed; inspect actual outcome codes" :
            (outcome.stop_ != 0 ? "catalog operation stopped before accepted completion" : "catalog capture failed with a nonstandard exception");
        try { if (primary) std::rethrow_exception(primary); }
        catch (const std::exception& failure) { message = failure.what(); }
        catch (...) {}
        if (!message) message = "catalog exception message is unknown";
        size_t count = 0;
        while (count < 2048 && message[count]) { outcome.message_[count] = message[count]; ++count; }
        if (count == 2048) {
            constexpr char marker[] = "[truncated]";
            std::memcpy(outcome.message_.data() + 2048 - sizeof(marker) + 1, marker, sizeof(marker) - 1);
        }
        outcome.message_[count] = 0;
    }
    void finalize_statement() noexcept {
        if (!statement || !current_process()) return;
        auto* original = statement; statement = nullptr;
        const int rc = ::lattice::detail::store_physical_finalize(original);
        if (rc != SQLITE_OK) {
            if (outcome.finalize_ == -1 || outcome.finalize_ == SQLITE_OK) outcome.finalize_ = rc;
            outcome.cleanup_failed_ = true;
        } else if (outcome.finalize_ == -1) outcome.finalize_ = rc;
    }
    void close_blob() noexcept {
        if (!blob || !current_process()) return;
        auto* original = blob; blob = nullptr;
        const int rc = ::lattice::detail::store_physical_blob_close(original,handle);
        if (rc != SQLITE_OK) {
            if (outcome.finalize_ == -1 || outcome.finalize_ == SQLITE_OK) outcome.finalize_ = rc;
            outcome.cleanup_failed_ = true;
        } else if (outcome.finalize_ == -1) outcome.finalize_ = rc;
    }
    using parameter = std::variant<int64_t, std::string>;
    std::vector<json> metadata(const std::string& sql, const std::vector<parameter>& parameters = {}, size_t max_rows = 2048) {
        check();
        require(!statement, "catalog statement overlap");
        database::record_statement();
        const char* tail = nullptr;
        const int prepared = ::lattice::detail::store_physical_prepare_v3(handle, sql.c_str(), static_cast<int>(sql.size() + 1), 0, &statement, &tail);
        try {
            require_sql(prepared, SQLITE_OK, "catalog metadata prepare failed");
            require(statement && tail && *tail == 0 && sqlite3_stmt_readonly(statement),
                    "catalog metadata statement shape refused");
            require(sqlite3_bind_parameter_count(statement) == static_cast<int>(parameters.size()), "catalog parameter mismatch");
            for (size_t i = 0; i != parameters.size(); ++i) {
                int rc;
                if (const auto* number = std::get_if<int64_t>(&parameters[i])) rc = sqlite3_bind_int64(statement, static_cast<int>(i + 1), *number);
                else { const auto& text = std::get<std::string>(parameters[i]);
                    require(text.size() <= 1024, "catalog metadata parameter exceeds bound");
                    rc = sqlite3_bind_text(statement, static_cast<int>(i + 1), text.data(), static_cast<int>(text.size()), SQLITE_TRANSIENT); }
                require_sql(rc, SQLITE_OK, "catalog metadata bind failed");
            }
            std::vector<json> rows;
            size_t copied = 0;
            for (;;) {
                check();
                const int rc = ::lattice::detail::store_physical_step(statement);
                if (rc == SQLITE_DONE) break;
                require_sql(rc, SQLITE_ROW, "catalog metadata step failed");
                require(rows.size() < max_rows, "catalog metadata row bound exceeded");
                const int count = sqlite3_column_count(statement);
                require(count > 0 && count <= 16, "catalog metadata column bound failed");
                json row = json::object();
                for (int i = 0; i != count; ++i) {
                    const char* label = sqlite3_column_name(statement, i);
                    require(label, "catalog metadata column name missing");
                    switch (sqlite3_column_type(statement, i)) {
                    case SQLITE_NULL: row[label] = nullptr; break;
                    case SQLITE_INTEGER: row[label] = sqlite3_column_int64(statement, i); copied += 8; break;
                    case SQLITE_FLOAT: row[label] = sqlite3_column_double(statement, i); copied += 8; break;
                    case SQLITE_TEXT: {
                        const auto* bytes = sqlite3_column_text(statement, i);
                        const int length = sqlite3_column_bytes(statement, i);
                        require(length >= 0 && length <= static_cast<int>(chunk_bytes) && (bytes || length == 0), "catalog metadata text invalid");
                        copied += static_cast<size_t>(length);
                        require(copied <= metadata_bytes, "catalog metadata copy bound exceeded");
                        row[label] = length ? std::string(reinterpret_cast<const char*>(bytes), static_cast<size_t>(length)) : std::string{};
                        break;
                    }
                    default: throw db_error("catalog metadata unexpectedly returned a payload blob");
                    }
                    require(copied <= metadata_bytes, "catalog metadata copy bound exceeded");
                }
                rows.push_back(std::move(row));
            }
            finalize_statement();
            require(!outcome.cleanup_failed_, "catalog metadata finalization failed");
            return rows;
        } catch (...) { finalize_statement(); throw; }
    }
    void emit(void* context, owned_catalog_callback callback, int32_t kind, const uint8_t* bytes = nullptr, size_t size = 0) {
        check(); require(size <= chunk_bytes && (bytes || size == 0), "catalog frame exceeds its bound");
        callback_frame frame{this, callback_top}; callback_top = &frame;
        bool accepted;
        try { accepted = callback(context, kind, bytes, static_cast<int64_t>(size)); }
        catch (...) { callback_top = frame.previous; throw; }
        callback_top = frame.previous;
        require(accepted, "catalog consumer stopped capture");
        check();
    }
    void frame(void* context, owned_catalog_callback callback, int32_t kind, const json& value) {
        const auto bytes = value.dump();
        emit(context, callback, kind, reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
    }
    template<class Begin, class Bytes>
    void stream_cell(const std::string& table, const std::string& column, int64_t row, Begin&& begin, Bytes&& consume) {
        check(); require(!blob, "catalog blob overlap");
        const int opened = sqlite3_blob_open(handle, "main", table.c_str(), column.c_str(), row, 0, &blob);
        try {
            require_sql(opened, SQLITE_OK, "catalog incremental cell open failed");
            require(blob, "catalog incremental cell returned no object");
            const int length = sqlite3_blob_bytes(blob);
            require(length >= 0 && begin(static_cast<int64_t>(length)), "catalog cell length refused");
            std::array<uint8_t, chunk_bytes> scratch;
            for (int offset = 0; offset < length;) {
                check(); const int count = std::min<int>(static_cast<int>(scratch.size()), length - offset);
                const int rc = sqlite3_blob_read(blob, scratch.data(), count, offset);
                require_sql(rc, SQLITE_OK, "catalog incremental cell read failed");
                check(); consume(scratch.data(), static_cast<size_t>(count)); offset += count;
            }
            close_blob(); require(!outcome.cleanup_failed_, "catalog cell close failed");
        } catch (...) { close_blob(); throw; }
    }
    void stream(void* context, owned_catalog_callback callback) {
        const auto encoding = metadata("SELECT encoding FROM pragma_encoding()", {}, 1);
        require(encoding.size() == 1 && encoding[0].at("encoding") == "UTF-8", "catalog requires observed UTF8 storage encoding");
        for (const auto& row : metadata("SELECT name FROM pragma_database_list() LIMIT 3", {}, 3))
            require(row.at("name") == "main" || row.at("name") == "temp", "catalog refuses attached sources");
        for (const auto& table : tables) {
            const auto relation = "main." + quote(table);
            // This actual MAIN btree read anchors the DEFERRED snapshot before
            // any provisional schema callback. PRAGMA metadata alone is not
            // treated as proof that a reader transaction was acquired.
            const auto installed_table = metadata("SELECT type,name,rootpage FROM main.sqlite_schema WHERE name=?1 COLLATE BINARY LIMIT 2", {table}, 2);
            require(installed_table.size() == 1 && installed_table[0].at("type") == "table" &&
                installed_table[0].at("name") == table, "catalog physical table is absent");
            // table_list enumerates/connects unrelated virtual tables even
            // with a named filter, creating their private prepared caches on
            // this original. Prove the selected real btree directly instead.
            require(installed_table[0].at("rootpage").is_number_integer() &&
                installed_table[0].at("rootpage").get<int64_t>() > 0,
                "catalog requires an actual MAIN table btree");
            require(metadata("SELECT name FROM sqlite_temp_master WHERE name=?1 COLLATE NOCASE LIMIT 1", {table}, 1).empty(), "catalog refuses TEMP shadows");
            const auto columns = metadata("SELECT name,type,hidden,pk FROM pragma_table_xinfo(?1,'main') ORDER BY cid", {table});
            std::set<std::string> names; json wire = json::array(); bool primary_key = false; size_t primary_columns = 0;
            for (const auto& column : columns) {
                const auto name = column.at("name").get<std::string>();
                const auto type = column.at("type").get<std::string>();
                require(identifier(name) && type.size() <= 1024 && type.find('\0') == std::string::npos &&
                    column.at("hidden") == 0 && names.insert(name).second, "catalog schema columns invalid");
                utf8_state text; text.accept(reinterpret_cast<const uint8_t*>(type.data()), type.size()); text.finish();
                std::string lower = name; for (auto& c : lower) if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
                require(lower != "rowid" && lower != "oid" && lower != "_rowid_", "catalog row identity is shadowed");
                if (column.at("pk") != 0) ++primary_columns;
                if (name == "id") {
                    auto upper = type; for (auto& c : upper) if (c >= 'a' && c <= 'z') c -= 'a' - 'A';
                    primary_key = upper == "INTEGER" && column.at("pk") == 1;
                }
                wire.push_back({{"name", name}, {"type", type}});
            }
            require(primary_key && primary_columns == 1 && names.count("globalId"), "catalog model identity columns invalid");
            // Every spelling of rowid was refused above. Preparation resolves
            // the unquoted physical _rowid_ even for an empty target; WITHOUT
            // ROWID therefore fails before any provisional schema callback.
            require(metadata("SELECT _rowid_ AS physical FROM " + relation + " LIMIT 0", {}, 1).empty(),
                    "catalog physical rowid validation returned a row");
            require(metadata("SELECT 1 AS nonalias FROM pragma_index_list(?1,'main') WHERE origin='pk' LIMIT 1", {table}, 1).empty(), "catalog INTEGER PRIMARY KEY is not rowid alias");
            bool unique_uuid = false;
            for (const auto& index : metadata("SELECT name FROM pragma_index_list(?1,'main') WHERE \"unique\"=1 AND partial=0", {table})) {
                const auto keys = metadata("SELECT name,coll FROM pragma_index_xinfo(?1,'main') WHERE key=1 LIMIT 2", {index.at("name").get<std::string>()}, 2);
                if (keys.size() == 1 && keys[0].at("name") == "globalId" && keys[0].at("coll") == "NOCASE") unique_uuid = true;
            }
            require(unique_uuid, "catalog requires actual unique NOCASE global identity");
            frame(context, callback, 0, {{"table", table}, {"columns", wire}});
            int64_t last = 0;
            for (;;) {
                const auto rows = metadata("SELECT id,_rowid_ AS physical,typeof(globalId) AS globalKind FROM " + relation + " WHERE id>?1 ORDER BY id LIMIT 32", {last}, 32);
                if (rows.empty()) break;
                for (const auto& row : rows) {
                    require(row.at("id").is_number_integer() && row.at("physical").is_number_integer() &&
                        row.at("id").get<int64_t>() > last && row.at("id") == row.at("physical") && row.at("globalKind") == "text", "catalog row identity invalid");
                    last = row.at("id").get<int64_t>();
                    std::string gid;
                    stream_cell(table, "globalId", last, [](int64_t length) { return length == 36; },
                        [&](const uint8_t* bytes, size_t count) { check(); require(count <= 36 - gid.size(), "catalog global identity length changed"); gid.append(reinterpret_cast<const char*>(bytes), count); });
                    require(uuid(gid), "catalog global UUID invalid");
                    frame(context, callback, 1, {{"id", std::to_string(last)}, {"globalId", gid}});
                    for (const auto& column : columns) {
                        const auto name = column.at("name").get<std::string>(); const auto field = quote(name);
                        const auto cell = metadata("SELECT typeof(" + field + ") AS kind,CASE WHEN typeof(" + field + ") IN ('integer','real') THEN " + field + " END AS scalar FROM " + relation + " WHERE id=?1", {last}, 1);
                        require(cell.size() == 1, "catalog row disappeared inside snapshot");
                        const auto kind = cell[0].at("kind").get<std::string>(); uint64_t scalar = 0; int64_t size = 0;
                        if (kind == "text" || kind == "blob") {
                            utf8_state text;
                            stream_cell(table, name, last, [&](int64_t length) {
                                frame(context, callback, 2, {{"column", name}, {"kind", kind}, {"bytes", std::to_string(length)}}); return true;
                            }, [&](const uint8_t* bytes, size_t count) {
                                if (kind == "text") text.accept(bytes, count);
                                emit(context, callback, 3, bytes, count);
                            });
                            if (kind == "text") text.finish();
                        } else {
                            if (kind == "integer") { scalar = static_cast<uint64_t>(cell[0].at("scalar").get<int64_t>()); size = 8; }
                            else if (kind == "real") { const auto number = cell[0].at("scalar").get<double>(); std::memcpy(&scalar, &number, sizeof(scalar)); size = 8; }
                            else require(kind == "null", "catalog storage kind invalid");
                            frame(context, callback, 2, {{"column", name}, {"kind", kind}, {"bytes", std::to_string(size)}});
                            if (size) { uint8_t bytes[8]; for (size_t i = 0; i != 8; ++i) bytes[i] = static_cast<uint8_t>(scalar >> ((7 - i) * 8)); emit(context, callback, 3, bytes, 8); }
                        }
                        emit(context, callback, 4);
                    }
                    emit(context, callback, 5);
                }
            }
            require(metadata("SELECT 1 AS invalid FROM " + relation + " WHERE id<=0 LIMIT 1", {}, 1).empty(), "catalog contains nonpositive primary identity");
            emit(context, callback, 6);
        }
    }
    catalog_stream_result capture(const std::shared_ptr<owned_catalog_state>& self, void* context, owned_catalog_callback callback) noexcept {
        if (!current_process() || claimed.exchange(true)) return {};
        // First local: survives all direct Native cleanup/policy restoration,
        // mutex/attachment release and captured-reader return in this method.
        // Later whole state destruction is a distinct physical-ending tail.
        std::optional<detail::store_physical_native_scope> physical_call;
        std::unique_lock<std::mutex> attachment;
        sqlite3_mutex* sqlite_mutex = nullptr;
        bool sqlite_locked = false, policy_known = true;
        try {
            check(); require(context && callback, "catalog callback is absent");
            reserve(self);
            detail::ordinary_require_no_attachment(owner->config_.ordinary_context);
            require(!owner->config_.is_in_memory() && !owner->config_.read_only &&
                !owner->config_.ordinary_context && !owner->config_.managed_product_credentials &&
                !owner->ordinary_managed_profile_.load() && !owner->recovery_continuous_ &&
                !owner->administrative_owner_ && !owner->checked_existing_unpublished_,
                "catalog strict ordinary route unsupported");
            require(owner->txn_owner_thread_.load(std::memory_order_acquire) != std::this_thread::get_id(), "catalog cannot borrow an enclosing writer turn");
            {
                const auto lock = ownership();
                writer = owner->db_; ordinary_reader = owner->read_db_; reader = owner->xproc_read_db_; revision = owner->connection_revision_;
            }
            require(writer && reader && reader != writer && (!ordinary_reader || reader != ordinary_reader), "catalog strict original xproc reader unavailable");
            physical_call.emplace(*reader);
            require(physical_call->entered(), "resource Native caller original unavailable");
            attachment = std::unique_lock<std::mutex>(owner->attach_mutex_, std::defer_lock);
            while (!attachment.try_lock()) { check(); std::this_thread::sleep_for(std::chrono::milliseconds(1)); }
            check(); require(owner->attached_dbs_.empty() && owner->attached_aliases_.empty() && owner->attached_view_names_.empty(), "catalog attached topology unsupported");
            reader->require_creator_();
            handle = reader->db_; require(handle && !reader->read_control_, "catalog reader is not a free original wrapper");
            sqlite_mutex = sqlite3_db_mutex(handle); require(sqlite_mutex, "catalog reader requires serialized SQLite");
            while (sqlite3_mutex_try(sqlite_mutex) != SQLITE_OK) { check(); std::this_thread::sleep_for(std::chrono::milliseconds(1)); }
            sqlite_locked = true;
            reader->require_catalog_clean_locked_(); // Precheck before mutex admission is not a lease.
            require(reader->retirement_ != nullptr, "catalog original has no physical retirement owner");
            check();
            require(sqlite3_db_readonly(handle, "main") == 1 && sqlite3_get_autocommit(handle) == 1, "catalog requires an idle actual read-only original");
            require(sqlite3_next_stmt(handle, nullptr) == nullptr,
                    "catalog refuses every already-issued original-reader statement");
            require(!reader->raw_handle_escaped_.load(std::memory_order_acquire) &&
                !writer->raw_handle_escaped_.load(std::memory_order_acquire) &&
                (!ordinary_reader || !ordinary_reader->raw_handle_escaped_.load(std::memory_order_acquire)), "catalog parent has an escaped raw handle");
            identity = reader->physical_identity("main", control, true); require(identity != nullptr, "catalog original physical identity unavailable");
            stores = capture_projection_stores({identity});
            require(stores.publish_unique(control) == projection_status::batch, "catalog physical-store pressure or retirement refused admission");
            control->publish(handle);
            require_sql(reader->set_sql_progress_(detail::database_handler_slot::catalog,[](void* value)noexcept {
                return static_cast<database_read_control*>(value)->stopped()?1:0;
            },control.get()),SQLITE_OK,"catalog progress policy installation failed");
            installed = true;
            require_sql(reader->set_sql_busy_(detail::database_handler_slot::catalog,[](void* value,int)noexcept {
                auto* control = static_cast<database_read_control*>(value); if (control->stopped()) return 0;
                std::this_thread::sleep_for(std::chrono::milliseconds(1)); return control->stopped() ? 0 : 1;
            }, control.get()), SQLITE_OK, "catalog busy policy installation failed");
            check(); database::record_statement();
            outcome.transaction_ = ::lattice::detail::store_physical_exec(handle, "BEGIN DEFERRED", nullptr, nullptr, nullptr);
            began = outcome.transaction_ == SQLITE_OK;
            require(began, "catalog BEGIN failed");
            stream(context, callback);
            check();
            const auto final_identity = reader->physical_identity("main", control, true);
            require(final_identity && *final_identity == *identity && final_identity->filename == identity->filename,
                    "catalog physical source changed");
            require(sqlite3_get_autocommit(handle) == 0, "catalog lost its read snapshot");
            database::record_statement(); outcome.transaction_ = ::lattice::detail::store_physical_exec(handle, "COMMIT", nullptr, nullptr, nullptr);
            if (outcome.transaction_ == SQLITE_OK) began = false;
            require(outcome.transaction_ == SQLITE_OK, "catalog COMMIT failed");
            check(); outcome.succeeded_ = true;
        } catch (...) { primary = std::current_exception(); outcome.succeeded_ = false; }
        if (!current_process()) {
            if (attachment.owns_lock()) attachment.release();
            // Do not lock, interrupt, finalize, unlock or destroy inherited originals.
            outcome.succeeded_ = false; outcome.cleanup_failed_ = true; return outcome;
        }
        close_blob(); finalize_statement();
        if (installed) {
            // Wait for the actual last cancellation signal before unpublishing;
            // it can never address cleanup or the reader's successor operation.
            control->unpublish(handle);
            if(reader->clear_sql_progress_(detail::database_handler_slot::catalog,control.get())!=SQLITE_OK) {
                outcome.cleanup_failed_=true;policy_known=false;
            }
            if (reader->clear_sql_busy_(detail::database_handler_slot::catalog,control.get(),0) != SQLITE_OK) { outcome.cleanup_failed_ = true; policy_known = false; }
        }
        if (began) {
            database::record_statement(); outcome.rollback_ = ::lattice::detail::store_physical_exec(handle, "ROLLBACK", nullptr, nullptr, nullptr);
            if (outcome.rollback_ == SQLITE_OK) began = false;
            else outcome.cleanup_failed_ = true;
        }
        if (installed && sqlite3_busy_timeout(handle, reader->busy_timeout_ms_) != SQLITE_OK) { outcome.cleanup_failed_ = true; policy_known = false; }
        // A refusal before installing this operation's policy/BEGIN owns no
        // snapshot to inspect or roll back (including a queued quarantined
        // reader). Only an admitted original can owe Native settlement.
        const bool snapshot_ended = !sqlite_locked || (!installed && !began) || sqlite3_get_autocommit(handle) == 1;
        if (!snapshot_ended) outcome.cleanup_failed_ = true;
        if (sqlite_locked && (!snapshot_ended || !policy_known))
            reader->quarantine_catalog_locked_(); // Exact connection, before any queued normal admission.
        (void)control->stopped(); // Same original end also covers actual cleanup returns.
        outcome.stop_ = control->stop_code.load(std::memory_order_acquire);
        if (outcome.cleanup_failed_ || outcome.stop_ != 0) outcome.succeeded_ = false;
        if (!outcome.succeeded_) remember_message();
        if (sqlite_locked) sqlite3_mutex_leave(sqlite_mutex);
        if (attachment.owns_lock()) attachment.unlock();
        if (snapshot_ended && policy_known && !statement && !blob) {
            stores.release(); // Only after real cursor/transaction/policy cleanup and SQLite unlock.
            handle = nullptr; identity.reset(); reader.reset(); ordinary_reader.reset(); writer.reset();
            release_known();
        }
        // A nonzero finalize/blob-close result remains a cleanup ERROR even
        // though SQLite destroys those exact objects. Only an observed ended
        // snapshot and restored policy permit ticket release. UNKNOWN cleanup
        // retains the exact state, reader and physical
        // ticket in its bounded bank slot. No retry or counter-based refund.
        return outcome;
    }
};

namespace detail {
void require_catalog_database_callback_absent(const database* value) {
    for (auto* frame = callback_top; frame; frame = frame->previous) {
        auto* state = frame->state;
        if (state->writer.get() == value || state->ordinary_reader.get() == value || state->reader.get() == value) {
            state->callback_violation.store(true, std::memory_order_release);
            throw db_error("catalog callback cannot reenter its database owner");
        }
    }
}
}
void lattice_db::require_catalog_callback_absent() const {
    if (!callback_top) return; // Preserve ordinary entry/creator semantics and a cheap no-callback path.
    require_creator_process(); // Before observing any active callback state.
    for (auto* frame = callback_top; frame; frame = frame->previous) {
        if (frame->state->owner.get() == this) {
            frame->state->callback_violation.store(true, std::memory_order_release);
            throw db_error("catalog callback cannot reenter its parent owner");
        }
    }
}
catalog_stream_operation make_catalog_stream_operation(std::shared_ptr<lattice_db> owner, const std::string& request, int64_t timeout) {
    require(owner && !owner->is_closed(), "catalog requires its live original creator owner");
    owner->require_catalog_callback_absent();
    require(timeout > 0 && timeout <= 30000000000LL, "catalog timeout must be positive and at most30s");
    const auto end = clock::now() + std::chrono::nanoseconds(timeout);
    auto names = parse_tables(request);
    require(clock::now() < end, "catalog control construction exhausted its original end");
    const auto process = detail::database_native_process();
    auto state = std::shared_ptr<owned_catalog_state>(new owned_catalog_state(std::move(owner), std::move(names), end),
        [process](owned_catalog_state* original) { if (process == detail::database_native_process()) delete original; });
    return catalog_stream_operation(std::move(state));
}
bool catalog_stream_operation::valid() const noexcept { return state_ && state_->current_process() && !state_->claimed.load(); }
void catalog_stream_operation::cancel() const noexcept {
    if (state_ && state_->current_process()) state_->control->stop(static_cast<int32_t>(projection_status::cancelled));
}
catalog_stream_result catalog_stream_operation::capture(void* context, owned_catalog_callback callback) const noexcept {
    // A synchronous consumer may drop every public operation copy. This real
    // local share must outlive member capture AND its bank-release tail.
    const auto retained = state_;
    if (!retained) return {};
    return retained->capture(retained, context, callback);
}
}
