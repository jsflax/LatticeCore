#pragma once

#ifdef __cplusplus

#include "types.hpp"
#include <sqlite3.h>
#include <stdexcept>
#include <unordered_map>

namespace lattice {

class db_error : public std::runtime_error {
public:
    explicit db_error(const std::string& msg) : std::runtime_error(msg) {}
};

class database {
public:
    /// Open mode for database connections
    enum class open_mode {
        read_write,  ///< Full read/write access (default)
        read_only    ///< Read-only access (for concurrent readers)
    };

    explicit database(const std::string& path, open_mode mode = open_mode::read_write);
    ~database();

    // Non-copyable
    database(const database&) = delete;
    database& operator=(const database&) = delete;

    // Moveable
    database(database&& other) noexcept;
    database& operator=(database&& other) noexcept;

    // Schema management
    void create_table(const table_schema& schema);
    void ensure_table(const table_schema& schema);
    bool table_exists(const std::string& name) const;

    // Get existing column names and types from a table (for migration)
    // Returns map of column_name -> SQL_TYPE (uppercase)
    std::unordered_map<std::string, std::string> get_table_info(const std::string& table) const;

    // CRUD operations
    // conflict_columns: if non-empty, generates ON CONFLICT (...) DO UPDATE SET for upsert
    primary_key_t insert(const std::string& table,
                         const std::vector<std::pair<std::string, column_value_t>>& values,
                         const std::vector<std::string>& conflict_columns = {});

    void update(const std::string& table,
                primary_key_t id,
                const std::vector<std::pair<std::string, column_value_t>>& values);

    void remove(const std::string& table, primary_key_t id);

    // Query - returns rows as vector of column maps
    using row_t = std::unordered_map<std::string, column_value_t>;
    std::vector<row_t> query(const std::string& sql,
                             const std::vector<column_value_t>& params = {});

    // Transaction support
    void begin_transaction();
    void commit();
    void rollback();
    bool is_in_transaction() const;

    // Execute SQL with optional params (for INSERT/UPDATE/DELETE without return)
    void execute(const std::string& sql,
                 const std::vector<column_value_t>& params = {});

    // Raw access (use sparingly)
    sqlite3* handle() const { return db_; }

    // Bind a value to a prepared statement (public for lattice_db bulk insert)
    void bind_value(sqlite3_stmt* stmt, int index, const column_value_t& value);

private:
    sqlite3* db_ = nullptr;
    std::string path_;
    open_mode mode_;
    column_value_t extract_column(sqlite3_stmt* stmt, int index);
};

// RAII transaction guard
class transaction {
public:
    explicit transaction(database& db);
    ~transaction();

    void commit();
    void rollback();

private:
    database& db_;
    bool completed_ = false;
};

} // namespace lattice

#endif // __cplusplus
