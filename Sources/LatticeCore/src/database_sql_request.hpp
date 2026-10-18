#pragma once
#include <lattice/types.hpp>
#include <lattice/store_physical_api.hpp>
#include <optional>
#include <sqlite3.h>
#include <chrono>
#include <cstdint>
#include <limits>
#include <string_view>
#include <vector>
namespace lattice { class database; }
namespace lattice::detail {
// Closed producer purpose. This is not an SQL string/ambient-thread permit.
// The actual engine node pins it to precisely one prepared statement.
enum class database_sql_read_kind : unsigned char {
    diagnostic, managed_cell, table_exists, table_info,
    function_inventory, busy_timeout_getter
};
// Logical structured payload/row declarations, not allocator/RSS or SQLite
// page-cache accounting. Whole-domain credit/factory binding remains separate.
struct database_sql_read_limits {
    uint64_t sql_bytes=UINT64_MAX,parameter_bytes=UINT64_MAX,
        columns=UINT64_MAX,rows=UINT64_MAX,result_bytes=UINT64_MAX;
    std::chrono::steady_clock::time_point deadline=std::chrono::steady_clock::time_point::max();
};
class database_sql_dispatcher;
class database_initialization_budget;
class database_initialization_episode;
class observer_delivery_initialization_sql;
class database_sql_request final {
    friend class ::lattice::database;
    friend class database_initialization_episode;
    friend class database_sql_dispatcher;
    friend struct database_sql_request_test_access;
    friend struct database_sql_read_admission_test_access;
    friend struct database_initialization_budget_test_access;
    struct admission_data {
        const void* budget=nullptr;
        const void* request=nullptr;
        uint64_t used=0;
        bool borrowed=false;
        std::chrono::steady_clock::time_point deadline{};
    };
    admission_data inspect_admission_for_test_()const noexcept;
    const uint64_t creator_;
    database* const owner_;
    const database_sql_read_kind kind_;
    database_sql_read_limits limits_;
    uint64_t materialized_columns_=0;
    struct read_budget;
    struct custody;
    custody* admission_=nullptr;
    struct initialization_custody;
    initialization_custody* initialization_=nullptr;
    database_sql_request(database&,const observer_delivery_initialization_sql&,
        const std::shared_ptr<database_initialization_episode>& = {},bool cleanup=false);
    database_sql_request(database&,const database_sql_request& parent);
    bool initialization_current_()const noexcept;
    bool initialization_admits_(sqlite3_stmt*)const noexcept;
    bool initialization_entry_matches_(const observer_delivery_initialization_sql&)const noexcept;
    bool initialization_cleanup_()const noexcept;
    void note_initialization_api_error_(int,bool cleanup=false)noexcept;
    void arm_initialization_schema_attempt_();
    bool retire_initialization_schema_attempt_()noexcept;
    static std::uint64_t initialization_shell_()noexcept;
    static bool transaction_recipe_(const observer_delivery_initialization_sql&)noexcept;
    void admit_(std::string_view);
    void workspace_(uint64_t);
    void parameter_bytes_before_copy_(uint64_t);
    void validate_admission_locked_()const;
    sqlite3_stmt* statement_=nullptr;
    uint64_t rows_=0,result_bytes_=0,name_bytes_=0,parameter_bytes_=0;
    bool bounded_=false;
    mutable bool metadata_observed_=false;
    void add_result_(uint64_t);
    void remember_busy_(sqlite3_stmt*);
public:
    database_sql_request(database&,database_sql_read_kind,std::string_view,
        const std::vector<column_value_t>&,const database_sql_read_limits* = nullptr);
    ~database_sql_request()noexcept;
    database_sql_request(const database_sql_request&)=delete;
    database_sql_request& operator=(const database_sql_request&)=delete;
    bool current()const noexcept;
    bool stopped()const noexcept;
    void check()const;
    int authorize(int,const char*,const char*,const char* schema=nullptr,const char* origin=nullptr)const noexcept;
    void adopt(sqlite3_stmt*);
    bool matches(sqlite3_stmt*)const noexcept;
    void columns(sqlite3_stmt*);
    void parameter_before_copy(const column_value_t&);
    void parameter_text_before_copy(std::string_view);
    void parameter_blob_before_copy(std::string_view);
    void sql_before_copy(uint64_t);
    // Call before each owning map/string/blob copy, including first-cell copy.
    // Native decoding/conversion needs the separate reviewed VM/encoding scope.
    void row(sqlite3_stmt*,bool materialize=true,int selected=-1);
    class binding {
        std::optional<store_physical_native_scope> native_;
        std::optional<store_physical_cleanup_scope> cleanup_;
        void pin_native_(database&,bool);
        database_sql_dispatcher* cookie_=nullptr;
        database_sql_request* previous_=nullptr;
        uint64_t creator_=0;
        bool previous_cleanup_=false;
    public:
        explicit binding(database_sql_request*,sqlite3_stmt* = nullptr,bool cleanup=false);
        binding(database&,database_sql_request*,sqlite3_stmt* = nullptr,bool cleanup=false);
        ~binding()noexcept;
        binding(const binding&)=delete;
        binding& operator=(const binding&)=delete;
    };
};
}
