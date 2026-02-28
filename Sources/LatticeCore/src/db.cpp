#include "lattice/db.hpp"
#include "lattice/log.hpp"
#include <sqlite-vec.h>
#include <sstream>
#include <iostream>
#include <thread>
#include <chrono>

namespace lattice {

database::database(const std::string& path, open_mode mode) : path_(path), mode_(mode) {
    // Determine SQLite open flags based on mode
    int flags = SQLITE_OPEN_FULLMUTEX;  // Always use serialized threading mode
    int rc;

    if (mode == open_mode::read_only_immutable) {
        // Use URI mode with immutable=1 for truly immutable databases.
        // This tells SQLite to skip WAL/journal file checks, which is required
        // for bundled databases in app resources where -wal/-shm files don't exist.
        flags |= SQLITE_OPEN_READONLY | SQLITE_OPEN_URI;
        std::string uri = "file:" + path + "?immutable=1";
        rc = sqlite3_open_v2(uri.c_str(), &db_, flags, nullptr);
    } else if (mode == open_mode::read_only) {
        // Regular read-only for WAL concurrent readers (no immutable flag)
        flags |= SQLITE_OPEN_READONLY;
        rc = sqlite3_open_v2(path.c_str(), &db_, flags, nullptr);
    } else {
        flags |= SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
        rc = sqlite3_open_v2(path.c_str(), &db_, flags, nullptr);
    }
    if (rc != SQLITE_OK) {
        std::string error = sqlite3_errmsg(db_);
        sqlite3_close(db_);
        db_ = nullptr;
        LOG_ERROR("db", "Failed to open database: %s", error.c_str());
        throw db_error("Failed to open database: " + error);
    }

    // Enable foreign keys
    execute("PRAGMA foreign_keys = ON");

#ifdef __EMSCRIPTEN__
    // WASM/OPFS mode: Use DELETE journal mode (WAL requires mmap/shm which OPFS doesn't support)
    // Also skip mmap since OPFS uses SyncAccessHandle instead
    if (mode == open_mode::read_write) {
        execute("PRAGMA journal_mode = DELETE");
    }
    execute("PRAGMA cache_size = 50000");       // Large cache for performance
    execute("PRAGMA temp_store = MEMORY");      // Temp tables in RAM
#else
    // Native mode: Enable WAL mode for better concurrency (only on read-write connection)
    if (mode == open_mode::read_write) {
        execute("PRAGMA journal_mode = WAL");
    }

    // Performance optimizations (matching Lattice.swift)
    execute("PRAGMA cache_size = 50000");       // Large cache for performance
    execute("PRAGMA mmap_size = 300000000");    // Memory-mapped I/O (~300MB)
    execute("PRAGMA temp_store = MEMORY");      // Temp tables in RAM
#endif

    // Set busy timeout to handle lock contention (5 seconds)
    // Must be set before ANALYZE or any query that might contend with other connections.
    sqlite3_busy_timeout(db_, 5000);

    // Only run ANALYZE on read-write connections (best-effort — don't fail init if locked)
    if (mode == open_mode::read_write) {
        try {
            execute("ANALYZE");
        } catch (const db_error&) {
            LOG_WARN("db", "ANALYZE skipped (database busy)");
        }
    }

    // Initialize sqlite-vec extension for vector search
    int vec_rc = sqlite3_vec_init(db_, nullptr, nullptr);
    if (vec_rc != SQLITE_OK) {
        sqlite3_close(db_);
        db_ = nullptr;
        LOG_ERROR("db", "Failed to initialize sqlite-vec extension");
        throw db_error("Failed to initialize sqlite-vec extension");
    }
}

database::~database() {
    if (db_) {
        if (mode_ == open_mode::read_write) {
            sqlite3_wal_checkpoint_v2(db_, nullptr, SQLITE_CHECKPOINT_TRUNCATE, nullptr, nullptr);
        }
        sqlite3_close(db_);
    }
}

database::database(database&& other) noexcept
    : db_(other.db_), path_(std::move(other.path_)) {
    other.db_ = nullptr;
}

database& database::operator=(database&& other) noexcept {
    if (this != &other) {
        if (db_) {
            sqlite3_close(db_);
        }
        db_ = other.db_;
        path_ = std::move(other.path_);
        other.db_ = nullptr;
    }
    return *this;
}

void database::execute(const std::string& sql, const std::vector<column_value_t>& params) {
    if (params.empty()) {
        // Fast path for parameterless queries
        char* errmsg = nullptr;
        int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &errmsg);
        if (rc != SQLITE_OK) {
            std::string error = errmsg ? errmsg : "Unknown error";
            sqlite3_free(errmsg);
            LOG_ERROR("db", "SQL execution failed: %s (SQL: %s)", error.c_str(), sql.c_str());
            throw db_error("SQL execution failed: " + error + " (SQL: " + sql + ")");
        }
    } else {
        // Prepared statement path for parameterized queries
        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            LOG_ERROR("db", "Failed to prepare statement: %s (SQL: %s)", sqlite3_errmsg(db_), sql.c_str());
            throw db_error("Failed to prepare statement: " + std::string(sqlite3_errmsg(db_)));
        }

        int index = 1;
        for (const auto& param : params) {
            bind_value(stmt, index++, param);
        }

        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);

        if (rc != SQLITE_DONE) {
            LOG_ERROR("db", "Execution failed: %s (SQL: %s)", sqlite3_errmsg(db_), sql.c_str());
            throw db_error("Execution failed: " + std::string(sqlite3_errmsg(db_)));
        }
    }
}

bool database::table_exists(const std::string& name) const {
    const char* sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        LOG_ERROR("db", "Failed to prepare table_exists statement: %s", sqlite3_errmsg(db_));
        throw db_error("Failed to prepare statement");
    }

    sqlite3_bind_text(stmt, 1, name.c_str(), -1, SQLITE_TRANSIENT);
    bool exists = (sqlite3_step(stmt) == SQLITE_ROW);
    sqlite3_finalize(stmt);

    return exists;
}

std::unordered_map<std::string, std::string> database::get_table_info(const std::string& table) const {
    std::unordered_map<std::string, std::string> columns;

    std::string sql = "PRAGMA table_info(" + table + ")";
    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        LOG_ERROR("db", "Failed to prepare table_info statement: %s", sqlite3_errmsg(db_));
        throw db_error("Failed to prepare table_info statement");
    }

    // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char* name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
        const char* type = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));

        if (name && type) {
            // Normalize type to uppercase for comparison
            std::string type_str(type);
            for (char& c : type_str) {
                c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
            }
            columns[name] = type_str;
        }
    }

    sqlite3_finalize(stmt);
    return columns;
}

void database::create_table(const table_schema& schema) {
    std::ostringstream sql;
    sql << "CREATE TABLE " << schema.name << " (";

    bool first = true;
    if (!schema.is_link_table) {
        // Regular tables get id and globalId
        sql << "id INTEGER PRIMARY KEY AUTOINCREMENT, ";
        sql << "globalId TEXT UNIQUE NOT NULL";
        first = false;
    }

    for (const auto& col : schema.columns) {
        // Skip id and globalId - they're already added above for non-link tables
        if (!schema.is_link_table && (col.name == "id" || col.name == "globalId")) {
            continue;
        }
        if (!first) sql << ", ";
        sql << col.name << " ";
        first = false;

        switch (col.type) {
            case column_type::integer: sql << "INTEGER"; break;
            case column_type::real: sql << "REAL"; break;
            case column_type::text: sql << "TEXT"; break;
            case column_type::blob: sql << "BLOB"; break;
        }

        if (!col.nullable) {
            sql << " NOT NULL";
        }
        if (col.is_unique) {
            sql << " UNIQUE";
        }
        if (col.foreign_key_table) {
            sql << " REFERENCES " << *col.foreign_key_table
                << "(" << col.foreign_key_column.value_or("id") << ")";
        }
    }

    sql << ")";
    execute(sql.str());
}

void database::ensure_table(const table_schema& schema) {
    if (!table_exists(schema.name)) {
        create_table(schema);
    }
}

void database::bind_value(sqlite3_stmt* stmt, int index, const column_value_t& value) {
    std::visit([&](auto&& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::nullptr_t>) {
            sqlite3_bind_null(stmt, index);
        } else if constexpr (std::is_same_v<T, int64_t>) {
            sqlite3_bind_int64(stmt, index, v);
        } else if constexpr (std::is_same_v<T, double>) {
            sqlite3_bind_double(stmt, index, v);
        } else if constexpr (std::is_same_v<T, std::string>) {
            sqlite3_bind_text(stmt, index, v.c_str(), -1, SQLITE_TRANSIENT);
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            if (v.empty()) {
                sqlite3_bind_zeroblob(stmt, index, 0);
            } else {
                sqlite3_bind_blob(stmt, index, v.data(), static_cast<int>(v.size()), SQLITE_TRANSIENT);
            }
        }
    }, value);
}

column_value_t database::extract_column(sqlite3_stmt* stmt, int index) {
    int type = sqlite3_column_type(stmt, index);
    switch (type) {
        case SQLITE_INTEGER:
            return sqlite3_column_int64(stmt, index);
        case SQLITE_FLOAT:
            return sqlite3_column_double(stmt, index);
        case SQLITE_TEXT: {
            const char* text = reinterpret_cast<const char*>(sqlite3_column_text(stmt, index));
            return std::string(text ? text : "");
        }
        case SQLITE_BLOB: {
            const void* data = sqlite3_column_blob(stmt, index);
            int size = sqlite3_column_bytes(stmt, index);
            const uint8_t* bytes = static_cast<const uint8_t*>(data);
            return std::vector<uint8_t>(bytes, bytes + size);
        }
        case SQLITE_NULL:
        default:
            return nullptr;
    }
}

primary_key_t database::insert(const std::string& table,
                               const std::vector<std::pair<std::string, column_value_t>>& values,
                               const std::vector<std::string>& conflict_columns) {
    std::ostringstream sql;
    sql << "INSERT INTO main." << table << " (";

    bool first = true;
    for (const auto& [col, _] : values) {
        if (!first) sql << ", ";
        sql << col;
        first = false;
    }

    sql << ") VALUES (";
    first = true;
    for (size_t i = 0; i < values.size(); ++i) {
        if (!first) sql << ", ";
        sql << "?";
        first = false;
    }
    sql << ")";

    // Add ON CONFLICT clause for upsert if conflict_columns provided
    if (!conflict_columns.empty()) {
        sql << " ON CONFLICT (";
        first = true;
        for (const auto& col : conflict_columns) {
            if (!first) sql << ", ";
            sql << col;
            first = false;
        }
        sql << ") DO UPDATE SET ";
        first = true;
        for (const auto& [col, _] : values) {
            // Skip conflict columns and globalId in UPDATE
            if (col == "globalId") continue;
            bool is_conflict = false;
            for (const auto& cc : conflict_columns) {
                if (cc == col) { is_conflict = true; break; }
            }
            if (is_conflict) continue;
            if (!first) sql << ", ";
            sql << col << " = excluded." << col;
            first = false;
        }
    }

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql.str().c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        LOG_ERROR("db", "Failed to prepare insert: %s", sqlite3_errmsg(db_));
        throw db_error("Failed to prepare insert: " + std::string(sqlite3_errmsg(db_)));
    }

    int index = 1;
    for (const auto& [_, val] : values) {
        bind_value(stmt, index++, val);
    }

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        auto err = std::string(sqlite3_errmsg(db_));
        LOG_ERROR("db", "Insert failed: %s", err.c_str());
        throw db_error("Insert failed: " + err);
    }

    return sqlite3_last_insert_rowid(db_);
}

void database::update(const std::string& table,
                       primary_key_t id,
                       const std::vector<std::pair<std::string, column_value_t>>& values) {
    if (values.empty()) return;

    std::ostringstream sql;
    sql << "UPDATE " << table << " SET ";

    bool first = true;
    for (const auto& [col, _] : values) {
        if (!first) sql << ", ";
        sql << col << " = ?";
        first = false;
    }

    sql << " WHERE id = ?";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql.str().c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        auto error = std::string(sqlite3_errmsg(db_));
        LOG_ERROR("db", "Failed to prepare update: %s", error.c_str());
        throw db_error("Failed to prepare update: " + error);
    }

    int index = 1;
    for (const auto& [_, val] : values) {
        bind_value(stmt, index++, val);
    }
    sqlite3_bind_int64(stmt, index, id);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        LOG_ERROR("db", "Update failed: %s", sqlite3_errmsg(db_));
        throw db_error("Update failed: " + std::string(sqlite3_errmsg(db_)));
    }
}

void database::remove(const std::string& table, primary_key_t id) {
    std::string sql = "DELETE FROM " + table + " WHERE id = ?";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        LOG_ERROR("db", "Failed to prepare delete: %s", sqlite3_errmsg(db_));
        throw db_error("Failed to prepare delete: " + std::string(sqlite3_errmsg(db_)));
    }

    sqlite3_bind_int64(stmt, 1, id);
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        LOG_ERROR("db", "Delete failed: %s", sqlite3_errmsg(db_));
        throw db_error("Delete failed: " + std::string(sqlite3_errmsg(db_)));
    }
}

std::vector<database::row_t> database::query(const std::string& sql,
                                             const std::vector<column_value_t>& params) {
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        LOG_ERROR("db", "%s in %s", sqlite3_errmsg(db_), sql.c_str());
        std::cerr<<"db: "<<sqlite3_errmsg(db_)<<" "<<sql.c_str()<<std::endl;
        throw db_error("Failed to prepare query: " + std::string(sqlite3_errmsg(db_)));
    }

    int index = 1;
    for (const auto& param : params) {
        bind_value(stmt, index++, param);
    }

    std::vector<row_t> results;
    int col_count = sqlite3_column_count(stmt);

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        row_t row;
        for (int i = 0; i < col_count; ++i) {
            const char* name = sqlite3_column_name(stmt, i);
            row[name] = extract_column(stmt, i);
        }
        results.push_back(std::move(row));
    }

    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        auto error = std::string(sqlite3_errmsg(db_));
        LOG_ERROR("db", "Query failed: %s", error.c_str());
        throw db_error("Query failed: " + error);
    }

    return results;
}

void database::begin_transaction(bool exclusive) {
    // IMMEDIATE: acquires write lock, readers still allowed (WAL mode).
    // EXCLUSIVE: acquires write lock AND blocks all readers.
    // Use exclusive for migrations so stale connections can't read mid-migration.
    const char* sql = exclusive ? "BEGIN EXCLUSIVE" : "BEGIN IMMEDIATE";
    int rc = sqlite3_exec(db_, sql, nullptr, nullptr, nullptr);

    // Retry with exponential backoff for transient errors:
    // - SQLITE_BUSY/SQLITE_LOCKED: another connection holds the WAL write lock
    // - SQLITE_ERROR + already in transaction: another thread on this same serialized
    //   connection (SQLITE_OPEN_FULLMUTEX) started a transaction between our caller's
    //   is_in_transaction() check and this call. The other thread's transaction will
    //   finish shortly, so we retry.
    int backoff_ms = 1;
    const int max_backoff_ms = 1000;
    const int max_total_wait_ms = 30000;  // 30 seconds total
    int total_waited_ms = 0;

    while (rc != SQLITE_OK && total_waited_ms < max_total_wait_ms) {
        bool should_retry = false;
        if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
            should_retry = true;
        } else if (rc == SQLITE_ERROR && is_in_transaction()) {
            // "cannot start a transaction within a transaction" — another thread
            // on this connection has an active transaction. Wait for it to finish.
            should_retry = true;
        }
        if (!should_retry) break;

        std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
        total_waited_ms += backoff_ms;
        backoff_ms = std::min(backoff_ms * 2, max_backoff_ms);  // Exponential backoff, capped
        rc = sqlite3_exec(db_, sql, nullptr, nullptr, nullptr);
    }

    if (rc != SQLITE_OK) {
        auto error = std::string(sqlite3_errmsg(db_));
        LOG_ERROR("db", "Failed to begin transaction: %s", error.c_str());
        throw db_error("Failed to begin transaction: " + error);
    }
}

void database::commit() {
    execute("COMMIT");
}

void database::rollback() {
    execute("ROLLBACK");
}

bool database::is_in_transaction() const {
    // sqlite3_get_autocommit returns 0 if a transaction is active, non-zero otherwise
    return sqlite3_get_autocommit(db_) == 0;
}

// Transaction RAII guard
transaction::transaction(database& db, bool exclusive) : db_(db) {
    db_.begin_transaction(exclusive);
}

transaction::~transaction() {
    if (!completed_) {
        try {
            db_.rollback();
        } catch (...) {
            // Suppress exceptions in destructor
        }
    }
}

void transaction::commit() {
    db_.commit();
    completed_ = true;
}

void transaction::rollback() {
    db_.rollback();
    completed_ = true;
}

} // namespace lattice
