#pragma once
#include "observer_delivery_meter.hpp"
#include "observer_delivery_store.hpp"
#include <lattice/detail/observer_delivery_values.hpp>
#include <lattice/detail/observer_delivery_write.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace lattice::detail {
class observer_delivery_original;
class observer_delivery_workspace;
class observer_delivery_sql_runtime;
class observer_delivery_update;
// Structured producer input, not an arbitrary collect_values() callback.
// Borrowed views obey normal synchronous span/string_view lifetime rules until
// prepare() returns. Only the checked owning postimage survives admission.
class observer_delivery_plan {
public:
    using value_view=observer_delivery_values::value_view;
    using value=std::variant<std::nullptr_t,int64_t,double,std::string,std::vector<uint8_t>>;
    using field_view=observer_delivery_values::field_view;
    using row_view=observer_delivery_values::row_view;
    struct result;
    struct field {std::string name;value contents;};
    struct row {
        std::string table;std::vector<field> fields;
        // Produced only after admission by this closed generator. Neither a
        // caller SQL string nor a mutable DTO can replace it after prepare().
        std::string insert_sql;
        uint64_t bind_bytes=0,largest_value_bytes=0;
        // Closed UPDATE only: bind-issued identity and generated comparison.
        // Generic prepare() cannot set this operation.
        bool is_update=false;
        int64_t target_rowid=0;
        std::string target_global_id,identity_sql;
    };
private:
    struct state;
    std::shared_ptr<const state> state_;
    explicit observer_delivery_plan(std::shared_ptr<const state> value):state_(std::move(value)){}
    observer_delivery_store::lease take_credit()const;
    friend class observer_delivery_original;
    friend class observer_delivery_sql_runtime;
    friend class observer_delivery_connection;
    friend class observer_delivery_write;
    friend class observer_delivery_update;
    friend struct observer_delivery_sql_test_access;
    std::weak_ptr<const void> input_identity()const;
    std::shared_ptr<observer_delivery_workspace> workspace()const;
    void claim_preparation()const;
    void release_unissued_preparation()const noexcept;
    template<typename Read>static result prepare_rows(const observer_delivery_store::client&,
        observer_delivery_meter_budget,observer_delivery_store::clock::time_point,size_t,
        const Read&,const observer_delivery_store::cancellation*,bool,
        const std::shared_ptr<const observer_delivery_original>& = {});
    static result prepare_insert(const observer_delivery_store::client&,observer_delivery_meter_budget,
        observer_delivery_store::clock::time_point,const observer_delivery_values::cursor&,
        const observer_delivery_store::cancellation* = nullptr);
    static result prepare_insert(const observer_delivery_store::client&,observer_delivery_meter_budget,
        observer_delivery_store::clock::time_point,std::span<const row_view>,
        const observer_delivery_store::cancellation* = nullptr);
    static result prepare_insert(const observer_delivery_store::client&,observer_delivery_meter_budget,
        const observer_delivery_values::cursor&,const std::shared_ptr<const observer_delivery_original>&);
    static result prepare_insert(const observer_delivery_store::client&,observer_delivery_meter_budget,
        std::span<const row_view>,const std::shared_ptr<const observer_delivery_original>&);
    static result prepare_update(const observer_delivery_store::client&,observer_delivery_meter_budget,
        observer_delivery_store::clock::time_point,std::string_view table,std::string_view column,
        int64_t rowid,std::string_view global_id,size_t kind,uint64_t maximum_value_bytes,
        const std::shared_ptr<const observer_delivery_original>&);
    void supply_update(const value_view&)const;
    bool update_row_present()const;
    void record_update_present(bool)const;
    const std::shared_ptr<const observer_delivery_original>& borrowed_original()const;
    void reserve_output_workspace(uint64_t)const;
    uint64_t remaining_workspace()const;
    std::span<const observer_delivery_write::inserted_row> inserted_rows()const;
    std::string_view global_id(size_t)const;
    void record_inserted(size_t,int64_t)const;
public:
    // Fixed private shell charge; the actual implementation asserts sizeof.
    // Excludes SQLite's internal VM/page cache; exact supported producer/length
    // profile remains a factory requirement before public activation.
    static constexpr uint64_t begin_storage_bytes=128,write_storage_bytes=128,prepared_storage_bytes=256;
    static constexpr uint64_t staging_storage_bytes=begin_storage_bytes+write_storage_bytes+prepared_storage_bytes;
    static uint64_t minimum_workspace_bytes()noexcept;
    observer_delivery_plan()=default;
    static result prepare(const observer_delivery_store::client&,observer_delivery_meter_budget,
        observer_delivery_store::clock::time_point,std::span<const row_view>,
        const observer_delivery_store::cancellation* = nullptr);
    static result prepare(const observer_delivery_store::client&,observer_delivery_meter_budget,
        observer_delivery_store::clock::time_point,const observer_delivery_values::cursor&,
        const observer_delivery_store::cancellation* = nullptr);
    const std::vector<row>& rows()const;
    uint64_t input_bytes()const;
    observer_delivery_meter_budget declared_budget()const;
    bool valid()const noexcept;
    // No method drops/renews an admission independently of its actual owning
    // input plan. Executor/tail wiring must transfer this same original.
};
struct observer_delivery_plan::result {
    observer_delivery_store::status code=observer_delivery_store::status::invalid;
    observer_delivery_plan plan;
};
}
