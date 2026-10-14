#pragma once
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_EXTRA_ENTRY) && !defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
#error "Extra-entry qualification requires the receiving-owner fixture build"
#endif
#include "ordinary_owned_launch.hpp"
#include "ordinary_target_history_spool.hpp"
#include <memory>
#include <cstddef>
#include <string>
#include <cstdint>
#include <utility>
#include <sqlite3.h>

namespace lattice { class database; }
namespace lattice::detail {
class ordinary_export_access;
class ordinary_managed_child_admission;
class ordinary_unpublished_group_target;
class ordinary_target_staging_io;
class ordinary_target_history_spool;
class ordinary_target_history_page_append;
class ordinary_target_history_page_reader;
struct ordinary_target_history_read_span;
template<bool Receiving> class ordinary_resource_reservation;
class database_retirement_state;
class ordinary_group_target_schema_original;
class ordinary_group_target_receiving_owner;
struct ordinary_history_progress;
struct ordinary_history_descriptor;
struct ordinary_history_decoded_page;

// Private exact-original descriptor custody. No SQL/import/current/publication
// accessor or caller-selected path/profile/quota is created by this type.
class ordinary_target_staging_connection final {
#ifdef LATTICE_MANAGED_TARGET_RECLAMATION_FIXTURE
    friend struct ordinary_target_reclamation_fixture;
#endif
    friend class ordinary_export_access;
    friend class ordinary_managed_child_admission;
    friend class ordinary_unpublished_group_target;
    friend class ordinary_target_staging_io;
    friend class ordinary_target_history_spool;
    friend class ordinary_target_history_page_append;
    friend class ordinary_target_history_page_reader;
    friend class ::lattice::database;
    friend class ordinary_group_target_receiving_owner;
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
    friend class ordinary_receiver_reclamation_fixture;
    std::uint64_t qualification_disposition_observation()const noexcept;
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_EXTRA_ENTRY)
    bool qualification_create_extra_entry()noexcept;
#endif
#endif
    bool retire_receiving_shell()noexcept;
    // Explicit terminal constructor-only disposition. Ordinary retirement
    // never selects deletion, and this entry creates no new target authority.
    enum class abandoned_retirement_result {not_applicable,joined,retained_debt};
    abandoned_retirement_result retire_abandoned_receiving_shell()noexcept;
    // Separate general unpublished election and files-only disposition.
    // These private entries bind real producers, never a caller supplied reason.
    bool elect_terminal_database_failure(const ::lattice::database&)noexcept;
    bool terminal_cleanup_pending(const ::lattice::database&)const noexcept;
    bool terminal_descriptor_calls_known(std::uint64_t)const noexcept;
    std::uint64_t terminal_environment_ending_generation(const ordinary_target_staging_io&,const void*,std::uint64_t)const noexcept;
    bool end_terminal_environment(const ::lattice::database&)noexcept;
    enum class terminal_raw_retirement_result {not_issued,joined,retained_debt};
    terminal_raw_retirement_result retire_terminal_raw(const ::lattice::database&)noexcept;
    bool terminal_namespace_binding_admissible(const ordinary_unpublished_group_target*)const noexcept;
    bool prepare_terminal_namespace()noexcept;
    enum class terminal_files_retirement_result {disposed,retained_debt};
    terminal_files_retirement_result retire_terminal_files(const ::lattice::database&)noexcept;
    bool terminal_raw_ending_historical_known(const ::lattice::database&)const noexcept;
    bool terminal_general_native_known(const ::lattice::database&)const noexcept;
    bool terminal_general_files_context_known()const noexcept;
    bool terminal_general_files_ended(const ::lattice::database&)const noexcept;
    int terminal_general_file_kind(const ordinary_target_staging_io&,const void*)const noexcept;
    enum class terminal_vfs_retirement_result {disposed,retained_debt};
    terminal_vfs_retirement_result retire_terminal_vfs(const ::lattice::database&)noexcept;
    bool terminal_vfs_ended(const ::lattice::database&)const noexcept;
    bool terminal_vfs_scope_matches(const ordinary_target_staging_io&,const void*,const void*,std::uint64_t)const noexcept;
    bool terminal_vfs_files_before_known(const ordinary_target_staging_io&,const void*,const void*,std::uint64_t)const noexcept;
    bool terminal_vfs_files_after_known(const ordinary_target_staging_io&,const void*,const void*,std::uint64_t)const noexcept;
    bool terminal_general_native_after_vfs_known(const ::lattice::database&)const noexcept;
    bool terminal_general_files_context_after_vfs_known()const noexcept;
    int terminal_general_file_kind_after_vfs(const ordinary_target_staging_io&,const void*)const noexcept;
    bool terminal_general_files_after_vfs_known(const ::lattice::database&)const noexcept;
    enum class terminal_scope_retirement_result {detached,retained_debt};
    terminal_scope_retirement_result detach_terminal_scope(const ::lattice::database&)noexcept;
    bool terminal_scope_ended(const ::lattice::database&)const noexcept;
    bool terminal_scope_history_binding_known(const ::lattice::database&)const noexcept;
    bool terminal_descriptor_calls_after_scope_known(std::uint64_t)const noexcept;
    std::uint64_t terminal_environment_ending_generation_after_scope(const ordinary_target_staging_io&,const void*,std::uint64_t)const noexcept;
    bool terminal_raw_ending_after_scope_known(const ::lattice::database&)const noexcept;
    bool terminal_general_native_after_scope_known(const ::lattice::database&)const noexcept;
    bool terminal_general_files_context_after_scope_known()const noexcept;
    int terminal_general_file_kind_after_scope(const ordinary_target_staging_io&,const void*)const noexcept;
    bool terminal_general_files_after_scope_known(const ::lattice::database&)const noexcept;
    bool terminal_vfs_scope_after_scope_matches(const ordinary_target_staging_io&,const void*,const void*,std::uint64_t)const noexcept;
    bool terminal_vfs_files_after_scope_known(const ordinary_target_staging_io&,const void*,const void*,std::uint64_t)const noexcept;
    bool terminal_vfs_after_scope_known(const ::lattice::database&)const noexcept;
    // Private pre-release observation and separate post-input identity DATA.
    // Neither observer supplies files/owner/registry retirement authority.
    bool terminal_native_decoder_context_known(const ordinary_group_target_receiving_owner&,const void*&,std::uint64_t&)const noexcept;
    bool retire_terminal_native_decoder_payloads(const ordinary_group_target_receiving_owner&)noexcept;
    bool terminal_native_decoder_payloads_ended(const ordinary_group_target_receiving_owner&)const noexcept;
    bool retire_terminal_native_phases(const ordinary_group_target_receiving_owner&)noexcept;
    bool terminal_native_phases_ended(const ordinary_group_target_receiving_owner&)const noexcept;
    bool terminal_receiver_input_context_known(const ordinary_group_target_receiving_owner&,
        const void*&,std::uint64_t&)const noexcept;
    bool terminal_receiver_input_history_matches(const ordinary_group_target_receiving_owner&,
        const void*,std::uint64_t)const noexcept;
    bool history_spool_abandonment_admissible(const ordinary_target_history_spool::abandoned_ending_scope&)const noexcept;
    bool recheck_abandoned_raw_namespace(const ordinary_target_history_spool::abandoned_ending_scope&)const noexcept;
    abandoned_retirement_result retire_terminal_receiving_shell(const ordinary_group_target_receiving_owner&)noexcept;
    void require_history_spool_observation(const ordinary_target_history_spool&,const ordinary_group_target_receiving_owner&,ordinary_launch::deadline)const;
    const ordinary_history_descriptor& history_spool_header_observation(const ordinary_target_history_spool&,const ordinary_group_target_receiving_owner&,ordinary_launch::deadline)const;
    void require_history_spool_current(const ordinary_target_history_spool&,const ordinary_group_target_receiving_owner&,ordinary_launch::deadline)const;
    const ordinary_history_descriptor& history_spool_header_original(const ordinary_target_history_spool&,const ordinary_group_target_receiving_owner&,ordinary_launch::deadline)const;
    const ordinary_history_decoded_page& history_spool_page_original(const ordinary_target_history_spool&,const ordinary_group_target_receiving_owner&,ordinary_launch::deadline)const;
    std::shared_ptr<const ordinary_history_decoded_page> history_spool_page_input_pin(const ordinary_target_history_spool&,const ordinary_group_target_receiving_owner&,ordinary_launch::deadline)const;
    const std::shared_ptr<ordinary_resource_reservation<true>>& history_spool_decoder_charge(const ordinary_target_history_spool&,const ordinary_group_target_receiving_owner&,ordinary_launch::deadline)const;
    std::uint64_t history_spool_page_before_bytes(const ordinary_target_history_spool&,const ordinary_group_target_receiving_owner&,ordinary_history_progress&,ordinary_launch::deadline)const;
    std::shared_ptr<const ordinary_target_history_page_append> append_history_page(const std::shared_ptr<ordinary_group_target_receiving_owner>&,ordinary_launch::deadline);
    // Unmounted explicit raw recipe/writer; source/capture and normal receive
    // still select the existing BLOB programme until both raw readers compose.
    void stage_raw_history_header(const std::shared_ptr<ordinary_group_target_receiving_owner>&,ordinary_launch::deadline);
    void stage_raw_history_page(const std::shared_ptr<ordinary_group_target_receiving_owner>&,ordinary_launch::deadline);
    void end_raw_history_page_input(const ordinary_group_target_receiving_owner&,ordinary_launch::deadline);
    void require_history_raw_locator_ending(const ordinary_group_target_receiving_owner&,
        const ordinary_target_history_page_append&,ordinary_launch::deadline)const;
    void end_history_raw_input(const ordinary_group_target_receiving_owner&,
        std::shared_ptr<const ordinary_history_decoded_page>&,ordinary_launch::deadline)const;
    ordinary_target_history_read_span history_spool_reader_span(const ordinary_target_history_spool&,
        const ordinary_group_target_receiving_owner&,const ::lattice::database&,const void*,const void*,
        const ordinary_target_history_page_reader*,ordinary_launch::deadline)const;
    const std::shared_ptr<ordinary_resource_reservation<true>>& history_spool_reader_charge(const ordinary_target_history_spool&,
        const ordinary_group_target_receiving_owner&,const ::lattice::database&,const void*,const void*,
        const ordinary_target_history_page_reader*,ordinary_launch::deadline)const;
    void require_history_spool_reader_ending(const ordinary_target_history_spool&,
        const ordinary_group_target_receiving_owner&,const ::lattice::database&,const void*,
        const ordinary_target_history_page_reader&,ordinary_launch::deadline)const;
    bool history_spool_retirement_admissible(const ordinary_target_history_spool*)const noexcept;
    struct state;
    std::shared_ptr<state> state_;
    explicit ordinary_target_staging_connection(std::shared_ptr<state>);
    class database_scope final {
        friend class ordinary_target_staging_connection;
        friend class ordinary_target_staging_io;
        friend class ::lattice::database;
        friend class ordinary_target_history_page_reader;
        struct scope_state;
        std::shared_ptr<scope_state> held_;
        explicit database_scope(std::shared_ptr<scope_state>);
        void require_current(const ::lattice::database&)const;
        void require_catalog_ending(const ::lattice::database&,sqlite3*)const;
        void require_history_page_role(const ::lattice::database&)const;
        void require_row_import_role(const ::lattice::database&)const;
        void require_import_suspension_role(const ::lattice::database&)const;
        void require_history_chain_role(const ::lattice::database&)const;
        void require_projection_role(const ::lattice::database&)const;
        void require_history_role(const ::lattice::database&)const;
        void require_catalog_role(const ::lattice::database&)const;
        void require_metadata_ending(const ::lattice::database&,sqlite3*)const;
        void require_schema_work(const ::lattice::database&)const;
        void require_constructor_ending(const ::lattice::database&,sqlite3*)const;
        void record_sqlite_open(const ::lattice::database&,sqlite3*,int)noexcept;
        void record_configuration_result(const ::lattice::database&,int)noexcept;
        void begin_schema_ending(const ::lattice::database&);
        void record_schema_begin(const ::lattice::database&,int)noexcept;
        void record_schema_commit(const ::lattice::database&,int)noexcept;
        void record_schema_rollback(const ::lattice::database&,int)noexcept;
        void record_schema_finalized(const ::lattice::database&,int)noexcept;
        void record_sqlite_close(const ::lattice::database&,int)noexcept;
        bool checked_native_close_known(const ::lattice::database&)const noexcept;
        bool abandon_unpublished_main(const ::lattice::database&)noexcept;
        bool join_native_files(const ::lattice::database&)noexcept;
        bool leave(const ::lattice::database&)noexcept;
    public:
        ~database_scope();
        database_scope(const database_scope&)=delete;
        database_scope& operator=(const database_scope&)=delete;
    };
    enum class descriptor_callback_kind {open,close,read,write,truncate,sync,size,lock,unlock,reserved,control,sector,device,optional,unsupported,remove,access,path,randomness,sleep,time};
    void read_history_page(::lattice::database&,const std::shared_ptr<ordinary_group_target_receiving_owner>&,
        database_scope&,std::shared_ptr<ordinary_target_history_page_reader>&,ordinary_launch::deadline);
    class descriptor_callback_scope final {
        friend class ordinary_target_staging_connection;
        friend class ordinary_target_staging_io;
        std::shared_ptr<state> original_;
        std::shared_ptr<database_scope::scope_state> phase_;
        std::size_t slot_=64;
        std::uint64_t generation_=0;
        descriptor_callback_scope()=default;
        void record_body_return(int)noexcept;
        void record_body_exception_return(int)noexcept;
        bool leave()noexcept;
    public:
        ~descriptor_callback_scope();
        descriptor_callback_scope(const descriptor_callback_scope&)=delete;
        descriptor_callback_scope& operator=(const descriptor_callback_scope&)=delete;
    };
    void begin_descriptor_callback(descriptor_callback_kind,std::uint64_t,sqlite3_file*,descriptor_callback_scope&);
    std::uint64_t active_descriptor_callback_generation()const;
    std::uint64_t settling_descriptor_callback_generation()const;
    std::uint64_t settling_descriptor_callback_high_generation()const;
    static void prepare(const std::shared_ptr<const ordinary_unpublished_group_target>&,
        const std::shared_ptr<ordinary_target_staging_io>&,
        std::shared_ptr<ordinary_target_staging_connection>&,ordinary_launch::deadline);
    void require_original(ordinary_launch::deadline)const;
    void require_target_identity(const ordinary_unpublished_group_target*,ordinary_launch::deadline)const;
    void verify_member_census(int,ordinary_launch::deadline)const;
    ordinary_launch::deadline original_deadline()const noexcept;
    std::string main_path(ordinary_launch::deadline)const;
    int main_descriptor(ordinary_launch::deadline)const;
    std::shared_ptr<ordinary_target_staging_io> io_original(ordinary_launch::deadline)const;
    void claim_database_shell(ordinary_launch::deadline);
    void construct_database_shell(const std::shared_ptr<ordinary_target_staging_connection>&,
        ordinary_launch::deadline);
    void require_preopen_constructor(ordinary_launch::deadline)const;
    void begin_database_constructor(::lattice::database&,std::unique_ptr<database_scope>&,ordinary_launch::deadline);
    void begin_database_schema(::lattice::database&,const std::shared_ptr<ordinary_group_target_schema_original>&,
        std::unique_ptr<database_scope>&,ordinary_launch::deadline);
    void begin_database_catalog(::lattice::database&,const std::shared_ptr<ordinary_group_target_schema_original>&,
        std::unique_ptr<database_scope>&,ordinary_launch::deadline);
    void begin_database_history_header(::lattice::database&,const std::shared_ptr<ordinary_group_target_receiving_owner>&,
        std::unique_ptr<database_scope>&,ordinary_launch::deadline);
    void begin_database_history_page(::lattice::database&,const std::shared_ptr<ordinary_group_target_receiving_owner>&,
        std::unique_ptr<database_scope>&,ordinary_launch::deadline);
    void begin_database_history_chain(::lattice::database&,const std::shared_ptr<ordinary_group_target_receiving_owner>&,
        std::unique_ptr<database_scope>&,ordinary_launch::deadline);
    void begin_database_row_import(::lattice::database&,const std::shared_ptr<ordinary_group_target_receiving_owner>&,
        std::unique_ptr<database_scope>&,ordinary_launch::deadline);
    void import_history_rows(const std::shared_ptr<ordinary_group_target_receiving_owner>&,ordinary_launch::deadline);
    void require_row_import_ending(const ordinary_group_target_receiving_owner&,ordinary_launch::deadline)const;
    void begin_database_import_suspension(::lattice::database&,const std::shared_ptr<ordinary_group_target_receiving_owner>&,
        std::unique_ptr<database_scope>&,ordinary_launch::deadline);
    void suspend_import(const std::shared_ptr<ordinary_group_target_receiving_owner>&,ordinary_launch::deadline);
    void require_import_suspension_ending(const ordinary_group_target_receiving_owner&,ordinary_launch::deadline)const;
    void verify_history_chain(const std::shared_ptr<ordinary_group_target_receiving_owner>&,ordinary_launch::deadline);
    void require_history_chain_ending(const ordinary_group_target_receiving_owner&,ordinary_launch::deadline)const;
    void begin_database_projection(::lattice::database&,const std::shared_ptr<ordinary_group_target_receiving_owner>&,
        std::unique_ptr<database_scope>&,ordinary_launch::deadline);
    void validate_history_projection(const std::shared_ptr<ordinary_group_target_receiving_owner>&,ordinary_launch::deadline);
    void require_history_projection_ending(const ordinary_group_target_receiving_owner&,ordinary_launch::deadline)const;
    void stage_history_page(const std::shared_ptr<ordinary_group_target_receiving_owner>&,ordinary_launch::deadline);
    ordinary_history_progress history_page_progress(const ordinary_group_target_receiving_owner&,ordinary_launch::deadline)const;
    void stage_history_header(const std::shared_ptr<ordinary_group_target_receiving_owner>&,ordinary_launch::deadline);
    void require_history_header_ending(const ordinary_group_target_receiving_owner&,ordinary_launch::deadline)const;
    std::pair<std::uint64_t,std::uint64_t> history_main_identity(ordinary_launch::deadline)const;
    void install_catalog(const std::shared_ptr<ordinary_group_target_schema_original>&,ordinary_launch::deadline);
    void bootstrap_metadata(const std::shared_ptr<ordinary_group_target_schema_original>&,ordinary_launch::deadline);
    bool descriptor_schema_ending_admissible()const noexcept;
    std::uint64_t active_descriptor_phase_generation()const;
    std::uint64_t settling_descriptor_phase_generation()const;
    bool descriptor_environment_phase_admissible(std::uint64_t)const noexcept;
    bool begin_database_cleanup(::lattice::database&,std::unique_ptr<database_scope>&)noexcept;
    bool descriptor_callback_admissible()const noexcept;
    bool descriptor_retirement_admissible()const noexcept;
    int descriptor_open(bool,const char*,sqlite3_file*,int,int*,std::uint64_t&)noexcept;
    int descriptor_close(std::uint64_t,sqlite3_file*)noexcept;
    int descriptor_read(std::uint64_t,sqlite3_file*,void*,int,sqlite3_int64)noexcept;
    int descriptor_write(std::uint64_t,sqlite3_file*,const void*,int,sqlite3_int64)noexcept;
    int descriptor_truncate(std::uint64_t,sqlite3_file*,sqlite3_int64)noexcept;
    int descriptor_sync(std::uint64_t,sqlite3_file*,int)noexcept;
    int descriptor_size(std::uint64_t,sqlite3_file*,sqlite3_int64*)noexcept;
    int descriptor_lock(std::uint64_t,sqlite3_file*,int)noexcept;
    int descriptor_unlock(std::uint64_t,sqlite3_file*,int)noexcept;
    int descriptor_reserved(std::uint64_t,sqlite3_file*,int*)noexcept;
    int descriptor_control(std::uint64_t,sqlite3_file*,int,void*)noexcept;
    int descriptor_remove(const char*,int)noexcept;
    int descriptor_access(const char*,int,int*)noexcept;
    int descriptor_full_path(const char*,int,char*)noexcept;
public:
    ~ordinary_target_staging_connection();
    ordinary_target_staging_connection(const ordinary_target_staging_connection&)=delete;
    ordinary_target_staging_connection& operator=(const ordinary_target_staging_connection&)=delete;
};
} // namespace lattice::detail
