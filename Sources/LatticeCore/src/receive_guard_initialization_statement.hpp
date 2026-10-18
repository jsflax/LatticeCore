#pragma once
#include <lattice/db.hpp>
#include <lattice/ordinary_schema_recipe.hpp>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

namespace lattice::detail {
class observer_delivery_initialization_sql;
struct receive_delivery_guard_access;
// The existing three-literal producer, shared by legacy and initializing code.
// Namespace inventory helpers acquire no writer or arbitrary SQL capability.
struct ordinary_receive_recipe_access final {
    static constexpr const char* store() noexcept {return ordinary_schema_recipe::receive_store();}
    static constexpr const char* channel() noexcept {return ordinary_schema_recipe::receive_channel();}
    static constexpr const char* index() noexcept {return ordinary_schema_recipe::receive_index();}
};
class receive_guard_initialization_reader;
// Immutable output of the actual receive initializer. There is no caller-SQL
// constructor, ambient purpose, transaction grant or Native owner in this DATA.
class receive_guard_initialization_statement final {
    friend class receive_guard_initialization_reader;
    friend class observer_delivery_initialization_sql;
public:
    enum class kind : std::uint8_t {
        schema_inventory,store_row,channel_count_bytes,channel_keys,channel_row,
        create_store,create_channel,create_state_index,seed_store
    };
    bool creator_current()const noexcept;
    kind kind_id()const;
    std::string_view sql()const;
    const std::vector<column_value_t>& parameters()const;
    std::uint64_t column_limit()const;
    std::uint64_t row_limit()const;
    std::uint64_t result_byte_limit()const;
    ~receive_guard_initialization_statement()noexcept;
    receive_guard_initialization_statement(const receive_guard_initialization_statement&)=delete;
    receive_guard_initialization_statement& operator=(const receive_guard_initialization_statement&)=delete;
private:
    struct snapshot;
    struct storage;
    const std::uint64_t creator_;
    storage* storage_=nullptr;
    receive_guard_initialization_statement(std::uint64_t,storage*)noexcept;
    void check_for_(const database&)const;
    bool work_current_for_(const database&)const noexcept;
    bool schema_may_change_()const noexcept;
    int authorize_(int,const char*,const char*,const char* schema,const char* origin)const noexcept;
};
// Issued only inside the actual schema initializer for an already-pending open
// and its genuine live private WRITE episode. It never begins or ends one.
class receive_guard_initialization_reader final {
    friend struct receive_delivery_guard_access;
    friend class observer_delivery_open_startup;
    enum class mode {initialize,final_observation};
    struct state;
    const std::uint64_t creator_;
    state* state_=nullptr;
    receive_guard_initialization_reader(std::uint64_t,state*)noexcept;
    static bool initialize_if_pending_(database&,bool legacy_origin);
    static bool observe_final_pending_(database&);
    static bool run_pending_(database&,bool legacy_origin,mode);
    void require_current_()const;
    void require_write_()const;
    void capture_(std::uint8_t phase);
    const std::vector<database::row_t>& query_(receive_guard_initialization_statement::kind,
        const std::vector<std::uint8_t>* channel=nullptr);
    void execute_(receive_guard_initialization_statement::kind,bool legacy_origin=false);
    void issue_(receive_guard_initialization_statement::kind,const std::vector<std::uint8_t>*,bool,bool);
    void initialize_(bool legacy_origin);
    void fail_()const noexcept;
public:
    ~receive_guard_initialization_reader()noexcept;
    receive_guard_initialization_reader(const receive_guard_initialization_reader&)=delete;
    receive_guard_initialization_reader& operator=(const receive_guard_initialization_reader&)=delete;
};
}
