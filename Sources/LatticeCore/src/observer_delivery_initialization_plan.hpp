#pragma once
#include <lattice/detail/observer_delivery_startup.hpp>
#include <lattice/recovery_schema.hpp>
#include "ordinary_initialization_storage.hpp"
#include <memory>
#include <string_view>
#include <unordered_map>

namespace lattice::detail {
// The initializer authority is this actual immutable library plan, not SQL
// spelling observed in sqlite_schema. Every entry below is regenerated from
// the exact retained pre-open catalog by the normal Core recipe producers.
// Selection compares bytes to one admitted entry; public SQL has no entry.
class database_initialization_episode;
// A genuine synchronous closed installation continuation. Only the actual
// Plan can create it after strict old-or-complete schema/row metadata proof.
// It owns DATA originals only; the installer separately retains actual physical
// and Native leaves throughout every finite transition. It cannot escape that
// call or be reconstructed from a bool, current pointer or observed SQL.
class observer_delivery_initialization_meter final {
    friend class observer_delivery_initialization_plan;
    friend class observer_delivery_initialization_sql;
    struct state;
    const std::uint64_t creator_;
    state* const state_;
    observer_delivery_initialization_meter(std::uint64_t,state*)noexcept;
    static void destroy_(observer_delivery_initialization_meter*)noexcept;
public:
    ~observer_delivery_initialization_meter()noexcept;
    observer_delivery_initialization_meter(const observer_delivery_initialization_meter&)=delete;
    observer_delivery_initialization_meter& operator=(const observer_delivery_initialization_meter&)=delete;
};
// Immutable physical-construction expectation only. It holds a WEAK episode
// identity and DATA column stages, never an Open/Native owner, SQL grant or
// mutable published-store authority. Only the same actual Plan issues it.
class observer_delivery_initialization_stage final {
    friend class observer_delivery_initialization_plan;
    friend class observer_delivery_open_startup;
    struct state;
    const std::uint64_t creator_;
    state* const state_;
    observer_delivery_initialization_stage(std::uint64_t,state*)noexcept;
    static void destroy_(const observer_delivery_initialization_stage*)noexcept;
public:
    ~observer_delivery_initialization_stage()noexcept;
    observer_delivery_initialization_stage(const observer_delivery_initialization_stage&)=delete;
    observer_delivery_initialization_stage& operator=(const observer_delivery_initialization_stage&)=delete;
};
class observer_delivery_initialization_preimage final {
    friend class observer_delivery_initialization_plan;
    friend class observer_delivery_initialization_sql;
    friend class observer_delivery_open_startup;
    struct state;
    const std::uint64_t creator_;
    state* const state_;
    observer_delivery_initialization_preimage(std::uint64_t,state*)noexcept;
    static void destroy_(const observer_delivery_initialization_preimage*)noexcept;
    bool current_snapshot_(const database&)const noexcept;
    bool enrollment_ledger_for_(const database&,const recovery_obligation_bootstrap_reader*,
        observer_delivery_open_startup::enrollment_ledger_operator,std::string_view,std::string_view)const noexcept;
    bool receive_phase_for_(const database&,observer_delivery_open_startup::receive_preimage_phase)const noexcept;
    bool metadata_phase_for_(const database&,observer_delivery_open_startup::metadata_preimage_phase)const noexcept;
    bool sync_control_phase_for_(const database&,observer_delivery_open_startup::sync_control_preimage_phase)const noexcept;
    bool static_sync_phase_for_(const database&,observer_delivery_open_startup::static_sync_preimage_phase)const noexcept;
    bool static_sync_upload_current_for_(const database&)const noexcept;
    bool audit_phase_for_(const database&,observer_delivery_open_startup::audit_preimage_phase)const noexcept;
    bool plain_table_for_(const database&,std::string_view)const noexcept;
    bool shape_for_(const database&,std::string_view,std::uint64_t&,bool&)const noexcept;
    bool system_metadata_for_(const database&,std::string_view)const noexcept;
    bool model_phase_for_(const database&,observer_delivery_open_startup::model_preimage_phase)const noexcept;
    bool model_table_present_for_(const database&,std::string_view)const;
    observer_delivery_initialization_columns model_table_info_for_(const database&,std::string_view,
        const std::shared_ptr<const observer_delivery_initialization_preimage>&)const;
public:
    ~observer_delivery_initialization_preimage()noexcept;
    observer_delivery_initialization_preimage(const observer_delivery_initialization_preimage&)=delete;
    observer_delivery_initialization_preimage& operator=(const observer_delivery_initialization_preimage&)=delete;
};
class observer_delivery_initialization_plan final {
    friend class observer_delivery_startup;
    friend class observer_delivery_open_startup;
    friend class observer_delivery_initialization_sql;
    friend class recovery_obligation_bootstrap_reader;
    struct state;
    const std::uint64_t creator_;
    state* state_=nullptr;
    observer_delivery_initialization_plan(std::uint64_t,state*)noexcept;
    static void destroy(const observer_delivery_initialization_plan*)noexcept;
    static std::shared_ptr<const observer_delivery_initialization_plan> build_(
        const observer_delivery_open_startup&,database&);
    observer_delivery_initialization_sql select_(const observer_delivery_open_startup&,
        database&,std::string_view,const std::vector<column_value_t>&)const;
    enum class preimage_selection {complete,final_required_present,meter_install_entry,enrollment_ledger_prefix,enrollment_ledger_complete,model_construction_prefix,model_required_present,schema_metadata_read,receive_read_existing,receive_before_store,receive_before_channel,receive_before_index,receive_strict_seed,schema_metadata_create,schema_metadata_seed,schema_metadata_marker_present,sync_control_create,sync_control_seed,
        static_sync_set_create,static_sync_set_present,static_sync_state_create,static_sync_state_present,
        static_sync_state_index_create,static_sync_state_index_present,static_sync_slots_create,static_sync_slots_present,
        static_sync_upload_floor,static_sync_upload_floor_present,
        audit_create,audit_present,audit_pending_create,audit_pending_present,audit_lookup_create,audit_lookup_present};
    std::shared_ptr<const observer_delivery_initialization_preimage> capture_preimage_(
        const observer_delivery_open_startup&,database&,const recovery_obligation_bootstrap_reader*,
        preimage_selection=preimage_selection::complete,
        const observer_delivery_initialization_stage* model_stage=nullptr,
        const observer_delivery_initialization_preimage* predecessor=nullptr,std::size_t after_operator=std::size_t(-1))const;
    void require_final_(const observer_delivery_open_startup&,database&,const recovery_owner_schema&,
        std::int64_t actual_target,std::string_view actual_marker_key)const;
    struct meter_entry_evidence {
        std::shared_ptr<const observer_delivery_initialization_preimage> proof;
        std::array<char,35> marker{};
        std::int64_t cookie=0;
        bool present=false; // actual strictly validated snapshot fact, no SQL grant
    };
    void require_normal_state_(const observer_delivery_open_startup&,database&,const recovery_owner_schema&,
        std::int64_t,std::string_view,meter_entry_evidence*)const;
    void install_final_meter_(const observer_delivery_open_startup&,database&,const recovery_owner_schema&,
        std::int64_t,std::string_view)const;
    bool meter_current_for_(const observer_delivery_initialization_meter&,const observer_delivery_open_startup&,
        const database&,std::size_t)const noexcept;
    void meter_entry_(const observer_delivery_initialization_meter&,std::size_t,const observer_delivery_open_startup&,
        database&,observer_delivery_initialization_sql::recipe&,std::string_view&)const;
    void meter_parameters_(const observer_delivery_initialization_meter&,std::size_t,
        const observer_delivery_open_startup&,database&,ordinary_initialization_vector<ordinary_initialization_text>&)const;
    int meter_authorize_(const observer_delivery_initialization_meter&,std::size_t,const observer_delivery_open_startup&,
        const database&,int,const char*,const char*,const char*)const noexcept;
    static ordinary_initialization_vector<ordinary_initialization_text> enrollment_implicit_indexes_(std::string_view,std::string_view);
    bool model_stage_for_(const observer_delivery_initialization_stage&,const observer_delivery_initialization_preimage&)const noexcept;
    std::shared_ptr<const observer_delivery_initialization_stage> model_stage_from_(const observer_delivery_open_startup&,database&,
        const observer_delivery_initialization_preimage&)const;
    observer_delivery_initialization_sql metadata_table_recipe_(const observer_delivery_open_startup&,database&)const;
    observer_delivery_initialization_sql metadata_initial_version_recipe_(const observer_delivery_open_startup&,database&)const;
    observer_delivery_initialization_sql metadata_marker_recipe_(const observer_delivery_open_startup&,database&,
        bool prune,std::string_view key,std::string_view cookie)const;
    bool metadata_operator_for_(std::size_t,const observer_delivery_open_startup&,const database&,
        observer_delivery_initialization_sql::recipe)const noexcept;
    observer_delivery_initialization_sql sync_control_table_recipe_(const observer_delivery_open_startup&,database&)const;
    observer_delivery_initialization_sql sync_control_initial_recipe_(const observer_delivery_open_startup&,database&)const;
    bool sync_control_operator_for_(std::size_t,const observer_delivery_open_startup&,const database&,
        observer_delivery_initialization_sql::recipe)const noexcept;
    observer_delivery_initialization_sql static_sync_recipe_(const observer_delivery_open_startup&,database&,
        observer_delivery_initialization_sql::recipe)const;
    bool static_sync_operator_for_(std::size_t,const observer_delivery_open_startup&,const database&,
        observer_delivery_initialization_sql::recipe)const noexcept;
    observer_delivery_initialization_sql audit_recipe_(const observer_delivery_open_startup&,database&,
        observer_delivery_initialization_sql::recipe)const;
    bool audit_operator_for_(std::size_t,const observer_delivery_open_startup&,const database&,
        observer_delivery_initialization_sql::recipe)const noexcept;
    bool model_operator_for_(std::size_t,const observer_delivery_open_startup&,const database&,
        observer_delivery_initialization_sql::recipe)const noexcept;
    void require_current_model_metadata_(const observer_delivery_open_startup&,database&)const;
    void require_fresh_catalog_empty_(const observer_delivery_open_startup&,database&)const;
    void initialize_fresh_catalog_families_(const observer_delivery_open_startup&,lattice_db&,database&)const;
    observer_delivery_initialization_sql model_metadata_recipe_(const observer_delivery_open_startup&,database&,bool read,std::string_view key,std::string_view value)const;
    observer_delivery_initialization_sql model_version_recipe_(const observer_delivery_open_startup&,database&,std::int64_t)const;
    bool model_preimage_for_(std::size_t,const observer_delivery_open_startup&,const database&,const observer_delivery_initialization_preimage&)const noexcept;
    bool model_postimage_for_(std::size_t,const observer_delivery_open_startup&,const database&,
        const observer_delivery_initialization_preimage&)const noexcept;
    int model_authorize_(std::size_t,const observer_delivery_open_startup&,const database&,
        observer_delivery_initialization_sql::recipe,int,const char*,const char*,const char*)const noexcept;
    void entry_(std::size_t,const observer_delivery_open_startup&,database&,
        observer_delivery_initialization_sql::recipe&,std::string_view&)const;

public:
    ~observer_delivery_initialization_plan()noexcept;
    observer_delivery_initialization_plan(const observer_delivery_initialization_plan&)=delete;
    observer_delivery_initialization_plan& operator=(const observer_delivery_initialization_plan&)=delete;
};
}
