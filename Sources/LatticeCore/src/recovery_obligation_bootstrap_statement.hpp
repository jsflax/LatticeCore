#pragma once
#include <lattice/db.hpp>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

namespace lattice::detail {
class observer_delivery_initialization_sql;
class recovery_obligation_producer_store;
class receive_install_store;
struct receive_install_binding;
struct receive_install_limits;
class recovery_local_producer_adapter;
struct recovery_obligation_producer_discovery_limits;
struct recovery_obligation_bootstrap_test_access;
struct recovery_continuous_row_test_access;
class recovery_continuous_producer;
struct recovery_writer_access;
struct recovery_continuous_admission;
class recovery_obligation_bootstrap_reader;
class observer_delivery_initialization_plan;
struct ordinary_schema_projection;

// Private, immutable output of the actual bootstrap recipe generator. Neither
// arbitrary SQL nor a caller purpose can construct this object. Its snapshot
// pins only DATA and the exact preissued raw-BEGIN receipt, never Native owner.
class recovery_obligation_bootstrap_statement final {
    friend class recovery_obligation_bootstrap_reader;
    friend class observer_delivery_initialization_sql;
public:
    enum class kind : std::uint8_t {
        family,definition,definition_else_null,obligation_shape,
        obligation_policy,installation_policy,producer_policy,
        obligation_config,producer_config,scope,entry,actual_audit,entry_page,
        scope_page_first,scope_page_after,record_count,scope_count,
        distinct_scopes,distinct_entries,alias_channels,
        producer_profile,producer_stamp,profile_page_first,profile_page_after,
        stamp_page_first,stamp_page_after,
        trigger_budget,trigger_programs,producer_present,
        model_definition,model_columns,index_budget,indexes,
        canonical_coverage,databases,temporary_presence,metadata_triggers,
        continuous_presence,continuous_inventory,continuous_row,
        journal_mode,synchronous,
        enrollment_audit_presence,enrollment_ledger_presence,
        enrollment_body_savepoint,enrollment_body_release,
        enrollment_install_channel_presence,
        enrollment_install_store_create,enrollment_install_channel_create,enrollment_install_domain_create,
        enrollment_obligation_store_create,enrollment_obligation_scope_create,enrollment_obligation_entry_create,
        enrollment_obligation_audit_create,enrollment_obligation_original_create,enrollment_obligation_domain_create,
        enrollment_producer_store_create,enrollment_producer_profile_create,enrollment_producer_stamp_create,
        enrollment_install_seed,enrollment_obligation_seed,enrollment_producer_seed,
        enrollment_receiver_configuration,enrollment_receiver_shared_schema,enrollment_receiver_row_sizes,
        enrollment_receiver_row_values,enrollment_receiver_aliases,enrollment_receiver_unique_domain,
        enrollment_receiver_insert,enrollment_receiver_usage,enrollment_receiver_changes
    };
    bool creator_current()const noexcept;
    kind kind_id()const;
    std::string_view sql()const;
    const std::vector<column_value_t>& parameters()const;
    std::uint64_t column_limit()const;
    std::uint64_t row_limit()const;
    std::uint64_t result_byte_limit()const;
    ~recovery_obligation_bootstrap_statement()noexcept;
    recovery_obligation_bootstrap_statement(const recovery_obligation_bootstrap_statement&)=delete;
    recovery_obligation_bootstrap_statement& operator=(const recovery_obligation_bootstrap_statement&)=delete;
private:
    struct snapshot;
    struct storage;
    const std::uint64_t creator_;
    storage* storage_=nullptr;
    recovery_obligation_bootstrap_statement(std::uint64_t,storage*)noexcept;
    void check_for_(const database&)const;
    bool work_current_for_(const database&)const noexcept;
    static bool enrollment_schema_kind_(kind)noexcept;
    bool enrollment_schema_()const noexcept;
    static bool enrollment_receiver_kind_(kind)noexcept;
    bool enrollment_receiver_write_()const noexcept;
    bool receiver_parameters_()const;
    std::size_t receiver_parameter_count_()const;
    int receiver_parameter_kind_(std::size_t)const;
    std::int64_t receiver_parameter_integer_(std::size_t)const;
    std::string_view receiver_parameter_blob_(std::size_t)const;
    bool schema_may_change_()const noexcept;
    int authorize_(int,const char*,const char*,const char* schema,const char* origin)const noexcept;
};

// This capability is issued only by the actual constructor adapter/store for
// an initializing database. Generic validation callbacks receive no reader.
// Queries select a closed regenerated recipe, never SQL text or a collector.
class recovery_obligation_bootstrap_reader final {
    friend class recovery_obligation_producer_store;
    friend class receive_install_store;
    friend class observer_delivery_initialization_plan;
    friend class recovery_local_producer_adapter;
    friend struct recovery_obligation_bootstrap_test_access;
    friend struct recovery_continuous_row_test_access;
    friend class recovery_continuous_producer;
    friend class observer_delivery_open_startup;
    friend class recovery_obligation_bootstrap_statement;
    friend struct recovery_writer_access;
    struct state;
    const std::uint64_t creator_;
    state* state_=nullptr;
    recovery_obligation_bootstrap_reader(std::uint64_t,state*)noexcept;
    static std::unique_ptr<recovery_obligation_bootstrap_reader> create_(
        database&,const recovery_obligation_producer_discovery_limits&,bool shared_domains);
    void begin_();
    void rollback_();
    static std::unique_ptr<recovery_obligation_bootstrap_reader> create_continuous_enrollment_(lattice_db&,database&);
    void require_continuous_enrollment_(const lattice_db&,const database&)const;
    void begin_continuous_enrollment_(std::exception_ptr& actual_cleanup_error);
    void commit_continuous_enrollment_();
    void rollback_continuous_enrollment_();
    void retire_failed_enrollment_rollback_()noexcept;
    void settle_continuous_enrollment_body_();
    void observe_continuous_enrollment_reads_(bool existing)const;
    void require_enrollment_current_()const;
    const recovery_obligation_producer_discovery_limits& enrollment_declaration_()const;
    database* state_owner_()const;
    void initialize_enrollment_ledgers_();
    void bind_enrollment_receiver_(std::size_t);
    const receive_install_binding& enrollment_receiver_binding_()const;
    const receive_install_limits& enrollment_receiver_limits_()const;
    std::vector<database::row_t> enrollment_receiver_run_(recovery_obligation_bootstrap_statement::kind,
        std::string_view channel={},std::int64_t count=0,std::int64_t bytes=0,
        std::int64_t prior_count=0,std::int64_t prior_bytes=0,std::int64_t row_bytes=0);
    enum class enrollment_ledger_proof {construction,complete};
    void capture_enrollment_ledger_prefix_(enrollment_ledger_proof);
    void execute_enrollment_schema_(recovery_obligation_bootstrap_statement::kind);
    std::string enrollment_schema_sql_(recovery_obligation_bootstrap_statement::kind)const;
    const char* enrollment_schema_table_(recovery_obligation_bootstrap_statement::kind)const;
    const char* enrollment_schema_index_(recovery_obligation_bootstrap_statement::kind)const;
    void append_enrollment_ledger_tables_(ordinary_schema_projection&)const;
    void verify_seeded_enrollment_ledgers_();
    bool enrollment_transaction_issued_()const noexcept;
    bool enrollment_transaction_live_()const noexcept;
    bool enrollment_begin_debt_()const noexcept;
    std::vector<database::row_t> run_(recovery_obligation_bootstrap_statement::kind,const std::vector<column_value_t>&);
    void validated_family_();
    void stored_limits_(const recovery_obligation_producer_discovery_limits&);
    void registration_enable_();
    std::vector<database::row_t> registration_getter_();
    void append_preimage_tables_(ordinary_schema_projection&)const;
    bool shared_domains_()const;
    void claim_workspace_(std::uint64_t)const;
    void require_owner_(const database&)const;
    void fail_()const noexcept;
public:
    using kind=recovery_obligation_bootstrap_statement::kind;
    ~recovery_obligation_bootstrap_reader()noexcept;
    recovery_obligation_bootstrap_reader(const recovery_obligation_bootstrap_reader&)=delete;
    recovery_obligation_bootstrap_reader& operator=(const recovery_obligation_bootstrap_reader&)=delete;
    std::vector<database::row_t> query_(kind,const std::vector<column_value_t>& parameters={});
};
}
