#pragma once
#include <array>
#include <lattice/types.hpp>
#include <lattice/recovery_schema.hpp>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace lattice {
class database;
class lattice_db;
class swift_lattice;
struct configuration;
}
namespace lattice::detail {
class observer_delivery_open_startup;
class observer_delivery_connection;
class observer_delivery_sql_runtime;
struct ordinary_receive_recipe_access;
class recovery_continuous_producer;
class store_resource_initialization_original;
struct recovery_continuous_admission;

class database_sql_request;
class database_sql_dispatcher;
class database_initialization_budget;
class database_initialization_episode;
struct database_initialization_budget_test_access;
class observer_delivery_initialization_plan;
class observer_delivery_initialization_preimage;
class observer_delivery_initialization_stage;
class observer_delivery_initialization_meter;
class ordinary_preimage_statement;
class recovery_obligation_bootstrap_statement;
class recovery_obligation_bootstrap_reader;
class receive_guard_initialization_statement;
class receive_guard_initialization_reader;
struct observer_delivery_startup_test_access;

// An actual private plan-entry handle, not a purpose+caller-SQL constructor.
// Only the immutable library recipe plan can issue it. The retained exact open
// pins the catalog/plan generation; the request retains this SAME entry through
// reprepare, stepping and actual finalization. Public SQL cannot acquire it.
class observer_delivery_initialization_sql final {
    friend class ::lattice::database;
    friend class ::lattice::lattice_db;
    friend class observer_delivery_startup;
    friend class observer_delivery_open_startup;
    friend class observer_delivery_initialization_plan;
    friend class database_sql_request;
    friend class database_initialization_budget;
    friend class database_initialization_episode;
    friend struct database_initialization_budget_test_access;
    friend class observer_delivery_sql_runtime;
    friend class recovery_continuous_producer;
    friend class recovery_obligation_bootstrap_reader;
    friend class receive_guard_initialization_reader;
    enum class recipe : std::uint16_t {
        busy_timeout_getter,function_inventory,
        schema_cookie,metadata_table_discovery,metadata_marker_read,metadata_version_read,static_sync_inventory,audit_inventory,
        receive_prior_table_presence,receive_initializer,
        connection_foreign_keys,connection_journal,
        connection_recursive_triggers_on,connection_recursive_triggers_getter,
        connection_cache_small,connection_cache_large,
        connection_mmap_off,connection_mmap_on,
        connection_temp_file,connection_temp_memory,
        connection_analysis,connection_journal_size,connection_schema_count,continuous_schema_discovery,
        continuous_synchronous_full,continuous_synchronous_getter,continuous_fresh_schema_presence,fresh_catalog_schema_presence,
        schema_transaction_begin,schema_transaction_deferred_begin,
        schema_transaction_exclusive_begin,schema_transaction_commit,
        schema_transaction_rollback,
        receive_store,receive_channel,receive_index,receive_initial,
        metadata_table,metadata_initial_version,sync_control_table,sync_control_initial,
        audit_table,audit_pending_index,audit_lookup_index,
        model_table,model_program,model_index,model_unique_index,model_shadow_column,model_metadata_store,model_metadata_read,model_version_write,
        link_table,virtual_link_table,union_table,union_cascade,
        rtree_table,rtree_insert,rtree_update,rtree_delete,rtree_population,
        geo_list_table,geo_list_parent_index,geo_list_rtree_table,
        geo_list_rtree_insert,geo_list_rtree_update,geo_list_rtree_delete,
        geo_list_audit,virtual_link_audit,
        sync_set,sync_state,sync_state_pending,replication_slots,replication_upload_floor,
        schema_metadata,schema_marker,schema_marker_prune,delivery_ledger,delivery_trigger,preimage_metadata,bootstrap_read,delivery_initial_seed,delivery_marker_cas
    };
    struct entry;
    const std::uint64_t creator_;
    entry* entry_=nullptr;
    observer_delivery_initialization_sql(std::uint64_t,entry*)noexcept;
    observer_delivery_initialization_sql retain_()const;
    // The only static-control issuer accepts a closed ID, never SQL bytes.
    static observer_delivery_initialization_sql fixed_(const observer_delivery_open_startup&,database&,recipe);
    static observer_delivery_initialization_sql from_plan_(const observer_delivery_initialization_plan&,
        std::size_t actual_entry,const observer_delivery_open_startup&,database&,
        std::string_view marker_key={},std::string_view marker_cookie={},std::string_view version={});


    static observer_delivery_initialization_sql from_meter_(
        const std::shared_ptr<observer_delivery_initialization_meter>&,std::size_t,
        const observer_delivery_open_startup&,database&);
    static observer_delivery_initialization_sql from_preimage_(
        const std::shared_ptr<const ordinary_preimage_statement>&,
        const observer_delivery_open_startup&,database&);
    static observer_delivery_initialization_sql from_bootstrap_(
        const std::shared_ptr<const recovery_obligation_bootstrap_statement>&,
        const observer_delivery_open_startup&,database&);
    static observer_delivery_initialization_sql from_receive_guard_(
        const std::shared_ptr<const receive_guard_initialization_statement>&,
        const observer_delivery_open_startup&,database&);
    static observer_delivery_initialization_sql metadata_read_(
        const observer_delivery_open_startup&,database&,recipe,std::string_view marker_key={});
    bool schema_may_change_()const noexcept;
    // Work denial only; current cleanup identity remains a separate contract.
    bool work_current_for_(const database&)const noexcept;
    bool metadata_work_current_for_(const database&)const noexcept;
    bool model_work_current_for_(const database&)const noexcept;
    bool sync_control_work_current_for_(const database&)const noexcept;
    bool static_sync_work_current_for_(const database&)const noexcept;
    bool audit_work_current_for_(const database&)const noexcept;
    void check_for_(const database&)const;
    std::string_view sql_()const;
    const std::vector<column_value_t>& parameters_()const;
    bool bootstrap_parameters_()const;
    std::size_t bootstrap_parameter_count_()const;
    int bootstrap_parameter_kind_(std::size_t)const;
    std::int64_t bootstrap_parameter_integer_(std::size_t)const;
    std::string_view bootstrap_parameter_blob_(std::size_t)const;
    bool preimage_parameters_()const;
    std::size_t preimage_parameter_count_()const;
    std::string_view preimage_parameter_text_(std::size_t)const;
    recipe recipe_id_()const;
    const void* entry_identity_()const noexcept;
    int authorize_(int,const char*,const char*,const char* schema,const char* origin)const noexcept;
    void constrain_result_(std::uint64_t& columns,std::uint64_t& rows,std::uint64_t& bytes)const;
public:
    ~observer_delivery_initialization_sql()noexcept;
    observer_delivery_initialization_sql(observer_delivery_initialization_sql&&)noexcept;
    observer_delivery_initialization_sql(const observer_delivery_initialization_sql&)=delete;
    observer_delivery_initialization_sql& operator=(const observer_delivery_initialization_sql&)=delete;
};

// This is owned only by a real, unpublished normal factory construction. Its
// immutable selection comes from the installed role/image/root issuer. It
// never holds a strong database/owner reference and cannot retain a Native
// original after its actual wrapper has gone away.
class observer_delivery_startup final : public std::enable_shared_from_this<observer_delivery_startup> {
    friend class recovery_continuous_producer;
    friend class ::lattice::database;
    friend class ::lattice::lattice_db;
    friend class ::lattice::swift_lattice;
    friend class observer_delivery_connection;
    friend class observer_delivery_sql_runtime;
    friend class observer_delivery_open_startup;
    friend class observer_delivery_initialization_sql;
    friend class observer_delivery_initialization_plan;
    struct state;
    struct replacement_episode;
    friend struct observer_delivery_startup_test_access;
    struct inspection {
        bool current=false;
        std::uint64_t count=0,bytes=0,leases=0;
        // Immutable actual per-client declared upper bounds; DATA only.
        std::uint64_t setup_count=0,setup_bytes=0,client_count_ceiling=0,client_byte_ceiling=0;
    };
    static inspection inspect_for_test_(const lattice_db&)noexcept;
    static std::shared_ptr<observer_delivery_startup> select_with_deadline_(
        const configuration&,const recovery_owner_schema&,std::chrono::steady_clock::time_point);
    const std::uint64_t creator_;
    state* state_=nullptr;
    observer_delivery_startup(std::uint64_t,state*)noexcept;
    static void destroy(observer_delivery_startup*)noexcept;
    enum class open_kind : std::uint8_t { writer,reader,cross_process_reader,replacement_writer,replacement_reader,replacement_cross_process_reader,keeper };
    // One separately issued token per real open. Initial writer/read/xproc
    // share the original construction deadline. A later replacement is a new
    // finite operation with freshly verified SAME issuer/profile, not renewal
    // of a previous constructor, statement, BEGIN or admission episode.
    // One budget is reserved BEFORE any replacement open. Reader+xproc share
    // this exact episode, fixed end and two-slot census; neither can renew it.
    std::shared_ptr<replacement_episode> begin_replacement_(lattice_db&,database& actual_anchor,
        bool read_pair,std::chrono::steady_clock::time_point);
    static void destroy_replacement_(replacement_episode*)noexcept;
    std::shared_ptr<observer_delivery_open_startup> make_open_(open_kind,
        const std::string&,std::chrono::steady_clock::time_point,
        const std::shared_ptr<replacement_episode>& = {});
    // Actual closed continuous producer, before the first Native open. This
    // records ONLY the same selected constructor family; the original budget,
    // plan and profile are still created by the actual writer opened_ seam.
    static constexpr std::uint64_t continuous_custody_charge_=65536;
    std::shared_ptr<observer_delivery_open_startup> make_continuous_writer_(
        const recovery_continuous_admission*,const recovery_owner_schema&,const std::string&);
    // Actual constructor, after ALL owned_resources initial opens returned.
    // Identity/custody only: cannot seal, finish or publish this owner.
    void bind_pending_owner_(lattice_db&);
    // Actual continuous producer AFTER constructor retention, before its owned
    // enrollment BEGIN. Comparison/currentness only; no finishing/SQL grant.
    void require_continuous_enrollment_owner_(lattice_db&,const recovery_continuous_admission*,
        std::chrono::steady_clock::time_point actual_entry_end);
    // Private DATA staging only. Reuses the same installed pending runtime,
    // original client/profile and immutable owner catalog; cannot publish or
    // finish an open. Actual final proof/sealing remains a separate step.
    std::shared_ptr<const observer_delivery_connection> stage_connection_(
        lattice_db&,database&,observer_delivery_open_startup&);
    void finish_owner_(lattice_db&);
    void require_finished_(const lattice_db&)const;
    bool requires_initialization_(const lattice_db&)const noexcept;
    // Prepublication replacements bind their actual Native profile/schema
    // before any owner field is swapped. Failure leaves the old publication.
    void finish_replacement_(lattice_db&,database&);
    void require_completed_role_(const lattice_db&,const database&,open_kind,const replacement_episode*)const;
    void require_replacement_writer_publication_(lattice_db&,database& next,database& previous)const;
    void publish_replacement_writer_(lattice_db&,database&)noexcept;
    void require_replacement_reader_publication_(lattice_db&,database&,database&)const;
    void publish_replacement_reader_pair_(database&)noexcept;
    void retire_replacement_publication_(const std::shared_ptr<replacement_episode>&)noexcept;
    std::chrono::steady_clock::time_point initial_deadline_()const;
public:
    // Empty only for the unchanged genuinely uninstalled/profile-free route.
    // An installed incomplete policy throws before any Native open.
    static std::shared_ptr<observer_delivery_startup> select(
        const configuration&,const recovery_owner_schema&);
    ~observer_delivery_startup()noexcept;
    observer_delivery_startup(const observer_delivery_startup&)=delete;
    observer_delivery_startup& operator=(const observer_delivery_startup&)=delete;
};

// Private passive column DATA. The implementation retains one actual
// preimage, borrows its immutable names, and stores bounded uppercase type
// bytes. It owns no Native wrapper or SQL/finish/publication authority.
class observer_delivery_initialization_columns final {
    friend class observer_delivery_initialization_preimage;
    struct state;
    const std::uint64_t creator_;
    state* state_=nullptr;
    observer_delivery_initialization_columns(std::uint64_t,state*)noexcept;
public:
    ~observer_delivery_initialization_columns()noexcept;
    observer_delivery_initialization_columns(observer_delivery_initialization_columns&&)noexcept;
    observer_delivery_initialization_columns(const observer_delivery_initialization_columns&)=delete;
    observer_delivery_initialization_columns& operator=(const observer_delivery_initialization_columns&)=delete;
    std::size_t size()const;
    std::size_t count(std::string_view)const;
    std::string_view at(std::string_view)const;
};
// One-use constructor custody for an ACTUAL open original. The token owns only
// creator-fenced DATA and the common profile. No strong database/owner is held.
// Copying a private initialization_key cannot issue a second open: claim_open_
// atomically consumes this token BEFORE the real Native open begins.
class observer_delivery_open_startup final {
    friend class ::lattice::swift_lattice;
    friend class observer_delivery_initialization_preimage;
    friend class receive_guard_initialization_statement;
    friend class recovery_obligation_bootstrap_reader;
    friend class recovery_obligation_bootstrap_statement;
    friend class receive_guard_initialization_reader;
    friend class observer_delivery_startup;
    friend class ::lattice::database;
    friend class ::lattice::lattice_db;
    friend class observer_delivery_sql_runtime;
    friend class database_sql_request;
    friend class database_initialization_budget;
    friend class database_initialization_episode;
    friend struct database_initialization_budget_test_access;
    friend class observer_delivery_initialization_sql;
    friend class observer_delivery_initialization_plan;
    friend class recovery_continuous_producer;
    friend class store_resource_initialization_original;
    struct state;
    const std::uint64_t creator_;
    state* state_=nullptr;
    observer_delivery_open_startup(std::uint64_t,state*)noexcept;
    static void destroy(observer_delivery_open_startup*)noexcept;
    bool creator_current_()const noexcept;
    std::chrono::steady_clock::time_point original_deadline_()const;
    void claim_open_(const std::string&);
    // Actual retained open return, before constructor SQL and outside all
    // Native/store leaves. No fresh domain/client or admission clock per reader.
    void opened_(database&);
    void execute_(database&,const observer_delivery_initialization_sql&,
        const std::vector<column_value_t>&);
    void require_initializing_(const database&)const;
    void require_work_current_(const database&)const;
    std::shared_ptr<const observer_delivery_open_startup> initialization_original_(const database&)const;
    const std::shared_ptr<database_initialization_budget>& initialization_budget_(const database&)const;
    bool pending_for_(const database&)const noexcept;
    // Denial only, including failed/expired unpublished startup. Public SQL
    // cannot acquire a fresh domain lease while private raw BEGIN is live.
    bool public_sql_pending_for_(const database&)const noexcept;
    bool cleanup_for_(const database&)const noexcept;
    void claim_workspace_(const database&,std::uint64_t)const;
    // Same private retained Open/database debit used by every synchronous
    // accounting scope. Never a profile, SQL or transaction issuer.
    static void debit_initialization_storage_(void* actual_open,void* actual_database,std::uint64_t);
    void fail_initialization_()const noexcept;
    // Actual bootstrap reader calls after its own successful private raw BEGIN.
    // The returned proof owns DATA only and cannot authorize ordinary SQL.
    std::shared_ptr<const observer_delivery_initialization_preimage> capture_preimage_(database&,const recovery_obligation_bootstrap_reader* =nullptr);
    // Closed fresh continuous ledger family only. These receipts retain the
    // same actual enrollment Reader/Open/BEGIN/revision; no caller projection.
    enum class enrollment_ledger_phase {construction,complete};
    enum class enrollment_ledger_operator {create_table,create_index,seed_store,bind_receiver};
    std::shared_ptr<const observer_delivery_initialization_preimage> capture_enrollment_ledger_preimage_(
        database&,const recovery_obligation_bootstrap_reader&,enrollment_ledger_phase);
    bool preimage_enrollment_ledger_for_(const database&,
        const std::shared_ptr<const observer_delivery_initialization_preimage>&,
        const recovery_obligation_bootstrap_reader*,enrollment_ledger_operator,
        std::string_view table,std::string_view index)const noexcept;
    // Real receive initializer only: a library-controlled construction prefix,
    // not a caller boolean/SQL grant. Each later step reissues actual preimage
    // after the previous Native return; strict_seed includes complete index,
    // namespace/FK/trigger closure before the one fixed INSERT.
    enum class receive_preimage_phase {read_existing,before_create_store,before_create_channel,before_create_index,strict_seed};
    std::shared_ptr<const observer_delivery_initialization_preimage> capture_receive_guard_preimage_(
        database&,receive_preimage_phase);
    bool preimage_receive_seed_for_(const database&,
        const std::shared_ptr<const observer_delivery_initialization_preimage>&)const noexcept;
    bool preimage_receive_phase_for_(const database&,
        const std::shared_ptr<const observer_delivery_initialization_preimage>&,receive_preimage_phase)const noexcept;
    observer_delivery_initialization_sql metadata_table_recipe_(database&)const;
    observer_delivery_initialization_sql metadata_initial_version_recipe_(database&)const;
    enum class model_preimage_phase {construction,complete};
    void require_model_write_(database&,const std::shared_ptr<const observer_delivery_initialization_preimage>&,model_preimage_phase)const;
    void require_model_owner_(::lattice::lattice_db&,database&)const;
    std::int64_t initialization_schema_version_(database&)const;
    void initialize_model_metadata_(::lattice::lattice_db&,database&,std::string_view,std::string_view)const;
    void initialize_model_version_(::lattice::lattice_db&,database&,std::int64_t)const;
    void initialize_model_recipe_(::lattice::lattice_db&,database&,std::string_view)const;
    bool model_table_present_(::lattice::lattice_db&,database&,std::string_view)const;
    void require_model_catalog_present_(::lattice::lattice_db&,database&)const;
    void require_current_model_(::lattice::lattice_db&,database&,const model_schema&)const;
    void initialize_current_model_catalog_(::lattice::lattice_db&,database&,bool swift_builder)const;
    void initialize_captured_model_catalog_(::lattice::lattice_db&,database&,bool swift_builder)const;
    void initialize_fresh_model_catalog_(::lattice::lattice_db&,database&,bool swift_builder,const recovery_continuous_admission*,
        const store_resource_initialization_original* normal_original=nullptr)const;
    void require_and_mark_current_catalog_(::lattice::lattice_db&,database&,bool swift_builder)const;
    bool fresh_catalog_active_for_(const database&)const noexcept;
    std::shared_ptr<const observer_delivery_initialization_preimage> capture_model_preimage_(database&,
        const std::shared_ptr<const observer_delivery_initialization_preimage>& predecessor={},std::size_t after_operator=std::size_t(-1))const;
    observer_delivery_initialization_columns model_table_info_(::lattice::lattice_db&,database&,std::string_view)const;
    enum class metadata_preimage_phase {create_table,seed_version,marker_write};
    // Exact pending owner only; returns comparison DATA from the unchanged
    // actual Core fingerprint producer, never a SQL/open/finish grant.
    std::array<char,35> fingerprint_marker_bytes_(::lattice::lattice_db&,database&)const;
    std::string fingerprint_marker_key_(::lattice::lattice_db&,database&)const;
    void initialize_fingerprint_marker_(::lattice::lattice_db&,database&,std::string_view,std::int64_t)const;
    void require_metadata_write_(database&,
        const std::shared_ptr<const observer_delivery_initialization_preimage>&,metadata_preimage_phase)const;
    observer_delivery_initialization_sql sync_control_table_recipe_(database&)const;
    observer_delivery_initialization_sql sync_control_initial_recipe_(database&)const;
    enum class sync_control_preimage_phase {create_table,seed_row};
    void require_sync_control_write_(database&,
        const std::shared_ptr<const observer_delivery_initialization_preimage>&,sync_control_preimage_phase)const;
    enum class audit_preimage_phase {table_create,table_present,pending_create,pending_present,lookup_create,lookup_present};
    enum class audit_operation {table,pending_index,lookup_index};
    void require_audit_write_(database&,
        const std::shared_ptr<const observer_delivery_initialization_preimage>&,audit_preimage_phase)const;
    void initialize_audit_(database&,audit_operation)const;
    bool preimage_plain_table_for_(const database&,
        const std::shared_ptr<const observer_delivery_initialization_preimage>&,std::string_view)const noexcept;
    bool preimage_shape_for_(const database&,
        const std::shared_ptr<const observer_delivery_initialization_preimage>&,
        std::string_view,std::uint64_t& actual_columns,bool& actual_without_rowid)const noexcept;
    bool preimage_system_metadata_for_(const database&,
        const std::shared_ptr<const observer_delivery_initialization_preimage>&,std::string_view)const noexcept;
    enum class static_sync_operation {set_table,state_table,state_pending_index,slots_table,upload_floor};
    enum class static_sync_preimage_phase {set_create,set_present,state_create,state_present,
        state_index_create,state_index_present,slots_create,slots_present,upload_floor,upload_floor_present};
    void initialize_static_sync_(database&,static_sync_operation)const;
    // Closed observations of current producers only; these issue no DDL,
    // migration, cursor seed or caller-SQL permission.
    enum class static_sync_observation {per_sync_set,cursor};
    void require_current_static_sync_(database&,static_sync_observation)const;
    void require_static_sync_write_(database&,
        const std::shared_ptr<const observer_delivery_initialization_preimage>&,static_sync_preimage_phase)const;
    const void* catalog_generation_()const noexcept;
    const recovery_owner_schema& initialization_catalog_(const database&)const;
    const recovery_owner_schema& final_catalog_(const database&)const;
    void finish_open_(database&);
    void require_final_meter_writer_(database&)const;
    std::array<char,35> model_meter_marker_key_(database&)const;
    const std::shared_ptr<const observer_delivery_initialization_plan>& initialization_plan_(const database&)const;
public:
    ~observer_delivery_open_startup()noexcept;
    observer_delivery_open_startup(const observer_delivery_open_startup&)=delete;
    observer_delivery_open_startup& operator=(const observer_delivery_open_startup&)=delete;
};
}
