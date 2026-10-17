#pragma once
#include "observer_delivery_original.hpp"
#include <memory>
#include <string_view>
struct sqlite3;
struct sqlite3_stmt;
namespace lattice { class database; }
namespace lattice::detail {
class observer_delivery_connection;
class observer_delivery_plan;
struct observer_delivery_sql_test_access;
// Private actual-SQLite meter implementation. This does not enroll a schema,
// grant a writer, accept raw SQL, or replace any existing authorizer. Only the
// versioned connection/executor can create it after complete schema proof.
class observer_delivery_sql_runtime final {
    struct state;
    const std::shared_ptr<state> state_;
    explicit observer_delivery_sql_runtime(std::shared_ptr<state>);
    static std::shared_ptr<observer_delivery_sql_runtime> install(database&);
    friend class observer_delivery_connection;
    friend class observer_delivery_write;
    friend class observer_delivery_update;
    friend class lattice::database;
    friend struct observer_delivery_sql_test_access;
    sqlite3* native(database&)const;
    bool admits(database&,sqlite3_stmt*)const noexcept;
    bool has_original(const std::shared_ptr<const observer_delivery_original>&)const noexcept;
    // Only actual Core control producers name this private type and supply
    // their *already prepared exact statement*. There is no thread trust flag
    // which could authorize reentrant or same-spelled caller SQL.
    class control_scope {
        const uint64_t creator_;const std::shared_ptr<state> state_;sqlite3_stmt* const statement_;
    public:
        control_scope(observer_delivery_sql_runtime&,database&,sqlite3_stmt*);
        ~control_scope()noexcept;
        control_scope(const control_scope&)=delete;
        control_scope& operator=(const control_scope&)=delete;
    };
    void moved()noexcept;
    class prepared {
        struct payload;payload* value_=nullptr;
        explicit prepared(payload* value):value_(value){}
        friend class observer_delivery_sql_runtime;
    public:
        ~prepared()noexcept;
        prepared(prepared&&)noexcept;
        prepared(const prepared&)=delete;
        prepared& operator=(const prepared&)=delete;
        prepared& operator=(prepared&&)=delete;
        void execute(database&);
    };
    // Preallocate/pin the actual immutable input BEFORE genuine BEGIN or any
    // store/Native leaf. Execution accepts only that exact plan's issued
    // original and generated SQL; it finalizes before outer settlement.
    prepared prepare(database&,const observer_delivery_plan&);
    void original_preparing(std::shared_ptr<const observer_delivery_original>);
    void original_unissued(const std::shared_ptr<const observer_delivery_original>&);
    void original_captured(const std::shared_ptr<const observer_delivery_original>&);
    // A closed generated statement consumes ONE exact actual stmt. No caller
    // string/token/row count is a permit. The complete executor must prove the
    // generated profile before entering this scope; generic public SQL cannot.
    class statement_scope {
        const uint64_t creator_;
        const std::shared_ptr<state> state_;
        sqlite3_stmt* const statement_;
        friend class prepared; // The private issued executor constructs this scope.
        friend class observer_delivery_connection;
        friend struct observer_delivery_sql_test_access;
        statement_scope(observer_delivery_sql_runtime&,database&,sqlite3_stmt*);
    public:
        ~statement_scope()noexcept;
        void retire()noexcept;
        statement_scope(const statement_scope&)=delete;
        statement_scope& operator=(const statement_scope&)=delete;
    };
public:
    observer_delivery_sql_runtime(const observer_delivery_sql_runtime&)=delete;
    observer_delivery_sql_runtime& operator=(const observer_delivery_sql_runtime&)=delete;
};
}
