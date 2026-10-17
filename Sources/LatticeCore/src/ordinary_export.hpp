#pragma once
#include "ordinary_managed_accepted_baseline.hpp"
#include "ordinary_managed_receiver_preservation.hpp"
#include "ordinary_managed_receiver_wss.hpp"
#include "ordinary_history_final_install.hpp"
#include "ordinary_managed_baseline_paging.hpp"
#include "ordinary_managed_wss_binding.hpp"
#include "ordinary_managed_product_route.hpp"
#include <exception>
#include <array>
#include <lattice/ordinary_history.hpp>
#include "ordinary_managed_peer_owner.hpp"
#include "ordinary_managed_peer_request.hpp"
#include "ordinary_managed_ipc_delivery.hpp"
#include "ordinary_managed_peer_test.hpp"
#include "ordinary_managed_upstream_capture.hpp"
#include "ordinary_managed_ipc_route.hpp"
#include "ordinary_managed_contribution_ledger.hpp"
#include <lattice/owned_server_export.hpp>
#include <lattice/ordinary_sync.hpp>
#include "recovery_writer_access.hpp"
#include "database_retirement.hpp"
#include "database_open_ledger.hpp"
#include "ordinary_source_upload.hpp"
#include "ordinary_managed_history.hpp"
#include "ordinary_target_staging_connection.hpp"
#include "ordinary_group_target_schema.hpp"
#include "ordinary_managed_profile_pair.hpp"

// Actual trusted Swift owner classes are defined in the bridge namespace.
namespace lattice { class swift_lattice; class swift_lattice_ref; }

namespace lattice::detail {
void require_ordered_receiver_absent(database&);
struct ordinary_order_write_error : db_error {
    recovery_install_result settlement;
    explicit ordinary_order_write_error(recovery_install_result value)
        :db_error("ordinary owned transaction did not finish cleanly"),settlement(std::move(value)){}
};
struct ordinary_checked_open_error : db_error {
    const std::exception_ptr primary_error,cleanup_error;
    const ordinary_context retained_context;
    const std::shared_ptr<const database_retirement_state> retirement;
    ordinary_checked_open_error(std::exception_ptr primary,std::exception_ptr cleanup,
        ordinary_context context,std::shared_ptr<const database_retirement_state> close)
        :db_error("checked existing ordinary open did not settle cleanly"),primary_error(std::move(primary)),
         cleanup_error(std::move(cleanup)),retained_context(std::move(context)),retirement(std::move(close)) {}
};
// Actual factory-owned per-open ledger, created before child native construction.
class ordinary_history_child_connections final : public database_open_ledger {
    friend class ordinary_export_access;
    ordinary_history_child_connections()=default;
    static void dispose(ordinary_history_child_connections* value) noexcept {
        if(!value||!value->disposable())return; // Unproved native phases keep the SAME charged tail.
        delete value;
    }
};
struct ordinary_export_table {
    std::string name;
    std::vector<std::string> columns;
    std::vector<column_type> types;
    std::vector<std::vector<column_value_t>> rows;
    // Local schema observations only. These are neither wire fields nor
    // authority supplied by a received descriptor. Remote originals are
    // checked against the receiver's actual validated projection.
    std::vector<std::string> audit_names;
    bool local_id = true;
};
// Passive source observation made before UPDATE no_history late binding.
// Its digest is bound to the actual source/epoch/scope/schema; it grants no
// producer authority. Missing historical facts cannot be minted from a cut.
struct ordinary_audit_original_descriptor final {
    std::vector<std::string> fields; // Sorted raw changedFields keys.
    std::vector<std::string> names;  // Exact raw changedFieldsNames order.
    std::string digest;
};
struct ordinary_export_frame final {
    std::string source, scope, schema, device, inode;
    int64_t epoch = 0, cut = 0;
    size_t rows = 0;
    // Set only by the actual v2 capture/authenticated parser. This selects
    // finite local resource limits; it is not source or receive authority.
    bool wire_profile = false;
    // Root model scope is distinct from the generated link/union projection
    // tables. scope encodes these names, not a list inferred from row payloads.
    std::vector<std::string> models;
    std::vector<ordinary_export_table> tables;
    std::vector<audit_log_entry> entries;
    std::vector<ordinary_audit_original_descriptor> originals; // v2, entry-aligned.
};
struct ordinary_export_batch final {
    std::optional<owned_server_export> complete;
    std::vector<audit_log_entry> entries;
};
// Passive canonical-observation custody. These values never issue source,
// producer, replacement or ACK authority. Descriptor bytes use a separate
// source-bound ordinary codec; zero/empty means genuinely unavailable.
struct ordinary_wire_original_stamp {
    std::string scope, source, schema, digest;
    int64_t epoch=0, cut=0;
};
struct ordinary_wire_original_record {
    ordinary_wire_original_stamp stamp;
    std::string gid, projection_seal, record_seal;
    int64_t codec=1, descriptor_codec=0, local_audit_id=0;
    std::vector<uint8_t> envelope, descriptor;
    // Built only after checking the stored association in the retained view.
    // Semantic facts come from envelope; local ID/rowId/provenance/ACK flags
    // come independently from the actual current receiver AuditLog row.
    audit_log_entry local_view;
};
// Root constructs only inside its closed actual owning view after the real
// local schema/catalog preflight. Nothing in this interface is a source grant.
class ordinary_wire_original_turn final {
    friend class ordinary_export_access;
    struct state;
    state* state_;
    explicit ordinary_wire_original_turn(state& value):state_(&value){}
public:
    ordinary_wire_original_turn(const ordinary_wire_original_turn&)=delete;
    ordinary_wire_original_turn& operator=(const ordinary_wire_original_turn&)=delete;
    // Before leaving the closed owning turn, verify total durable accounting.
    // No unrelated writer/callback may mutate these tables within this turn.
    void bind_delivery(const std::string& scope,const std::string& gid,const std::string& receiving_channel);
    void finish();
    std::optional<ordinary_wire_original_record> current(const std::string& scope,const std::string& gid);
    // Capture actual prior envelope, projection, flags, channel/slot/binding
    // observations before the caller changes any of them. No absent history
    // or session generation is inferred. This does not fence live channels.
    std::string archive(const std::string& scope,const std::string& gid,const std::string& reason);
    // Call only after the real canonical policy/materialization and accepted
    // receipt/frontier have been staged in this same actual owned WRITE.
    // Existing records require an archive of their exact prior sealed row.
    // The caller separately proves original continuity and channel repair.
    void store(const ordinary_wire_original_stamp&,const audit_log_entry&,
        int64_t descriptor_codec,const std::vector<uint8_t>& descriptor,int64_t local_audit_id);
};
struct ordinary_history_progress;
class ordinary_history_receive_operation;
struct ordinary_wire_original_test_access;

struct ordinary_delivery_outcome {std::string gid,version;int64_t audit_id=0;bool settled=false;};
class ordinary_export_access final {
    // Only the existing owning history wrappers may call their private implementation.
    friend class ::lattice::ordinary_history_export;
    friend auto ::lattice::capture_ordinary_history_export(lattice_db&,
        const std::vector<std::string>&,const ordinary_history_limits&) -> ::lattice::ordinary_history_export;
    friend auto ::lattice::resume_ordinary_history_export(lattice_db&,
        const std::string&,const ordinary_history_limits&) -> ::lattice::ordinary_history_export;
    // These actual retained owner classes already perform managed clone/reference checks.
    friend class ::lattice::swift_lattice;
    friend class ::lattice::swift_lattice_ref;
    friend class ordinary_export_storage_access;
    // Private exact-pointer borrow inside actual retained/owned views; never a source grant.
    enum class checked_reference_phase { before_enrollment, enrolled };
    static void validate_checked_managed_schema(lattice_db&,checked_reference_phase,
        const std::shared_ptr<const ordinary_managed_child_admission>& bounded_admission = {},
        ordinary_launch::deadline end = {});
    static void claim_managed_source_open(const std::shared_ptr<const ordinary_managed_child_admission>&,const std::string&);
    static void remember_managed_open_failure(const std::shared_ptr<const ordinary_managed_child_admission>&,
        const std::string&,std::exception_ptr) noexcept;
    static void claim_managed_group_open(const ordinary_context&,
        const std::shared_ptr<const ordinary_managed_child_admission>&,const std::string&,
        std::uint64_t,std::uint64_t,const recovery_owner_schema&,ordinary_launch::deadline);
    static void validate_managed_group_open(const ordinary_context&,
        const std::shared_ptr<const ordinary_managed_child_admission>&,const std::string&,
        std::uint64_t,std::uint64_t,ordinary_launch::deadline);
    static void validate_checked_mounted_server_schema(lattice_db&,const physical_store_identity&);
    static void validate_checked_open_admission(const ordinary_context&,
        const std::shared_ptr<const ordinary_managed_child_admission>&,const std::string&);
    static sqlite3* borrowed_storage_handle(database&) noexcept;
    static std::shared_ptr<void>& history_capacity_connection_witness(database&);
    static uint64_t history_capacity_connection_epoch(database&);
    friend struct ordinary_history_capacity_test_access;
    static uint64_t history_capacity_for_test(lattice_db&,const ordinary_history_limits&,bool);
    friend struct ordinary_managed_baseline_paging_test_access;
    static std::optional<ordinary_managed_baseline_cursor> prepare_managed_baseline_cursor(
        const std::shared_ptr<const ordinary_managed_ipc_local_route>&,const std::string&,ordinary_launch::deadline);
    static ordinary_managed_baseline_page next_managed_baseline_page(ordinary_managed_baseline_cursor&);
    static ordinary_managed_baseline_stage begin_managed_baseline_stage(ordinary_managed_baseline_cursor&);
    static void append_managed_baseline_stage(ordinary_managed_baseline_stage&,const ordinary_managed_baseline_page&);
    static ordinary_managed_baseline_staging_result finish_managed_baseline_stage(ordinary_managed_baseline_stage&);
    static void validate_managed_baseline_current(ordinary_managed_baseline_cursor&,bool verify_values=false);
    static ordinary_managed_baseline_cursor baseline_cursor_for_test(lattice_db&,const std::string&,ordinary_launch::deadline);
    static ordinary_managed_baseline_stage baseline_stage_for_test(lattice_db&,ordinary_managed_baseline_cursor&);
    static void baseline_append_for_test(lattice_db&,ordinary_managed_baseline_stage&,const ordinary_managed_baseline_page&);
    static ordinary_managed_baseline_staging_result baseline_finish_for_test(lattice_db&,ordinary_managed_baseline_stage&);
    static void baseline_reopen_for_test(lattice_db&,const std::string&);
    static void baseline_capacity_drift_for_test(lattice_db&);
    static std::shared_ptr<void>& baseline_capacity_connection_witness(database&);
    static std::optional<ordinary_managed_accepted_baseline> capture_managed_accepted_baseline(
        const std::shared_ptr<const ordinary_managed_ipc_local_route>&,const std::string&,ordinary_launch::deadline);
    friend struct ordinary_managed_accepted_baseline_test_access;
    friend struct ordinary_receiver_preservation_test_access;
    static std::string preserve_receiver_for_test(lattice_db&,const std::vector<std::string>&,const ordinary_history_limits&);
    static void verify_receiver_preservation_for_test(lattice_db&,const std::string&,const ordinary_history_limits&);
    static ordinary_receiver_preservation_record preserve_receiver_storage(lattice_db&,database&,
        const std::vector<std::string>&,const std::string&,const ordinary_history_limits&,ordinary_launch::deadline);
    friend class ordinary_managed_receiver_preservation;
    friend class ordinary_receiver_wss_access;
    friend class ordinary_group_target_schema_original;
    friend class ordinary_managed_wss_binding;
    static std::shared_ptr<lattice_db> receiver_restart_parent(const ordinary_managed_receiver_preservation&);
    static ordinary_launch::deadline receiver_restart_deadline(const ordinary_managed_receiver_preservation&);
    static std::string receiver_restart_member(const ordinary_managed_receiver_preservation&);
    static void retain_receiver_group_target(const ordinary_managed_receiver_preservation&,
        std::shared_ptr<const ordinary_unpublished_group_target>&,ordinary_launch::deadline);
    static void prepare_receiver_group_target_io(const std::shared_ptr<const ordinary_unpublished_group_target>&,
        std::shared_ptr<ordinary_target_staging_io>&,ordinary_launch::deadline);
    static void prepare_receiver_group_target_connection(const std::shared_ptr<const ordinary_unpublished_group_target>&,
        const std::shared_ptr<ordinary_target_staging_io>&,
        std::shared_ptr<ordinary_target_staging_connection>&,ordinary_launch::deadline);
    static void prepare_receiver_group_target_schema(const std::shared_ptr<const ordinary_receiver_wss_request>&,
        std::shared_ptr<ordinary_group_target_schema_original>&,ordinary_launch::deadline);
    static void validate_receiver_group_target_schema(const ordinary_group_target_schema_original&,
        const ordinary_target_staging_connection&,ordinary_launch::deadline);
    static void install_receiver_group_target_catalog(const std::shared_ptr<ordinary_group_target_schema_original>&,
        ordinary_launch::deadline);
    static void prepare_receiver_group_target_catalog(const std::shared_ptr<ordinary_group_target_schema_original>&,
        ordinary_launch::deadline);
    static void prepare_receiver_group_target_row_import(const ordinary_group_target_schema_original&,
        const ordinary_target_staging_connection&,ordinary_launch::deadline);
    static void prepare_receiver_group_target_import_suspension(const ordinary_group_target_schema_original&,
        const ordinary_target_staging_connection&,ordinary_launch::deadline);
    static void prepare_receiver_group_target_projection(const ordinary_group_target_schema_original&,
        const ordinary_target_staging_connection&,ordinary_launch::deadline);
    static void validate_receiver_group_target_projection(const ordinary_group_target_schema_original&,
        const ordinary_target_staging_connection&,const ordinary_metadata_query&,ordinary_launch::deadline);
    static void bootstrap_receiver_group_target_metadata(const std::shared_ptr<ordinary_group_target_schema_original>&,
        ordinary_launch::deadline);
    static void validate_receiver_restart_until(const ordinary_managed_receiver_preservation&,ordinary_launch::deadline);
    static void publish_receiver_restart_child(const std::shared_ptr<const ordinary_managed_receiver_preservation>&,
        const std::shared_ptr<const ordinary_receiver_wss_request>&,lattice_db&,lattice_db&);
    static std::shared_ptr<const ordinary_receiver_wss_request> receiver_wss_request(lattice_db&);
    static void validate_receiver_preservation(const ordinary_managed_receiver_preservation&,lattice_db&,database&,ordinary_launch::deadline);
    static void validate_receiver_clone_admission(const ordinary_managed_receiver_preservation&,lattice_db&,database&);
    static void validate_receiver_clone_publication(const ordinary_managed_receiver_preservation&,lattice_db&,lattice_db&);
    static void admit_history_restart(lattice_db&,database&,ordinary_history_receive_state&);
    static void install_history_accepted(lattice_db&,const std::shared_ptr<ordinary_history_receive_state>&);
    friend class ordinary_history_final_install_guard;
    static void validate_history_final_guard(const ordinary_history_final_install_guard&,lattice_db&,database&);
    friend struct ordinary_history_accepted_storage_test_access;
    static void install_history_accepted_for_test(lattice_db&,const std::string&,const std::string&);
    static void verify_history_accepted_for_test(lattice_db&,const std::string&);
    static ordinary_managed_accepted_baseline capture_managed_accepted_baseline_for_test(lattice_db&,const std::string&);

    friend struct ordinary_managed_peer_deadline_test_access;
    static ordinary_managed_peer_deadline_probe exercise_managed_peer_deadline_for_test(lattice_db&,bool deny_commit);
    friend class ordinary_managed_peer_owner;
    static std::unique_ptr<ordinary_managed_peer_request> make_managed_peer_request(
        const std::shared_ptr<ordinary_managed_peer_owner>&,const std::shared_ptr<const ordinary_managed_ipc_local_route>&,
        const std::string&,const std::string&,std::int64_t,ordinary_launch::deadline);
    static std::unique_ptr<ordinary_managed_peer_request> next_managed_peer_request(
        const std::shared_ptr<ordinary_managed_peer_owner>&,const std::shared_ptr<const ordinary_managed_ipc_local_route>&,
        std::string&,std::string&,std::int64_t,ordinary_launch::deadline);
    static void authorize_managed_peer_request(ordinary_managed_peer_request&);
    static void settle_managed_peer_request(ordinary_managed_peer_request&,const std::string&,ordinary_launch::deadline);
    static void reconcile_managed_peer_stage(ordinary_managed_peer_staged&,ordinary_launch::deadline);
    static ordinary_managed_contribution_observation_result apply_managed_peer_request(
        ordinary_managed_peer_staged&,ordinary_launch::deadline);
    static void retain_managed_peer_owner(std::shared_ptr<lattice_db>,ordinary_launch::deadline,
        std::shared_ptr<ordinary_managed_peer_owner>&);
    static bool managed_peer_owner_current(const ordinary_managed_peer_owner&)noexcept;
    static void validate_managed_peer_owner(const ordinary_managed_peer_owner&,ordinary_launch::deadline);
    friend struct ordinary_managed_peer_retry_test_access;
    static std::string record_managed_peer_retry_for_test(lattice_db&,const std::string&,const std::string&,const std::string&,const std::string&,const std::vector<std::uint8_t>&);
    static void settle_managed_peer_retry_for_test(lattice_db&,const std::string&,const std::string&);
    static ordinary_managed_upstream_capture capture_managed_upstream_until(
        const ordinary_managed_ipc_local_route&,const std::string&,const std::string&,ordinary_launch::deadline);
    static ordinary_managed_contribution_observation_result record_managed_upstream_observation(
        const ordinary_managed_ipc_local_route&,const ordinary_managed_upstream_capture&);
    static ordinary_managed_contribution_observation_result record_managed_upstream_observation_for_test(
        lattice_db&,const ordinary_managed_ipc_route_description&,const ordinary_managed_upstream_capture&);
    static managed_engram_policy capture_managed_engram_policy(const ordinary_managed_profile_pair&,
        managed_engram_direction,const std::string&,const std::string&,ordinary_launch::deadline);
    static void validate_managed_product_policy(const ordinary_managed_ipc_local_route&,lattice_db&,database&);
    friend class ordinary_managed_wss_binding;
    friend class ordinary_managed_wss_admission;
    static std::shared_ptr<const ordinary_managed_profile> managed_wss_profile(lattice_db&);
    static ordinary_history_progress stage_history(lattice_db&,const std::shared_ptr<ordinary_history_receive_operation>&);
    static recovery_owner_schema managed_clone_catalog(lattice_db& parent,const configuration&);
    static void admit_managed_clone(lattice_db& parent,lattice_db& unpublished_child);
    static void validate_managed_reference(lattice_db&);
    friend class ::lattice::ordinary_mounted_history_source;
    static ordinary_mounted_history_source register_mounted_history(std::shared_ptr<lattice_db>,const std::vector<std::string>&,const ordinary_history_limits&);
    static ordinary_history_export capture_mounted_history(const std::shared_ptr<ordinary_mounted_history_source_state>&,const std::vector<std::string>&,bool observe_lineage=false);
    static ordinary_history_export capture_history_impl(lattice_db&,const std::vector<std::string>&,const ordinary_history_limits&,const std::shared_ptr<ordinary_mounted_history_source_state>&,bool observe_lineage=false);
    static ordinary_history_export capture_history(lattice_db&,const std::vector<std::string>&,const ordinary_history_limits&);
    static ordinary_history_export resume_history(lattice_db&,const std::string&,const ordinary_history_limits&);
    static std::shared_ptr<const ordinary_history_page_state> history_page(const std::shared_ptr<ordinary_history_export_state>&,uint64_t);
    friend struct ordinary_managed_upstream_test_access;
    static void seed_managed_upstream_for_test(lattice_db&,const owned_server_export&,const std::string&);
    static ordinary_managed_upstream_capture capture_managed_upstream_for_test(lattice_db&,const std::string&,const std::string&);
    static ordinary_managed_upstream_capture capture_managed_upstream(
        const ordinary_managed_ipc_local_route&,const std::string& scope,const std::string& gid);
    friend struct ordinary_managed_contribution_test_access;
    static recovery_install_result record_contribution_observations_for_test(lattice_db&,
        const ordinary_managed_ipc_route_description&,
        const std::vector<ordinary_managed_contribution_observation>&,
        std::vector<ordinary_managed_contribution_observation_result>&);
    friend class ordinary_managed_ipc_delivery;
    static ordinary_managed_contribution_observation_result record_managed_contribution_observation(
        const ordinary_managed_ipc_local_route&,const ordinary_managed_contribution_observation&);
    friend class ordinary_managed_profile;
    friend class ordinary_managed_profile_pair;
    friend class ordinary_managed_ipc_access;
    friend struct ordinary_managed_profile_pair_test_access;
    static ordinary_managed_profile_pair retain_managed_pair(std::shared_ptr<lattice_db>,
        std::shared_ptr<lattice_db>, ordinary_launch::deadline);
    static void validate_managed_pair(const ordinary_managed_profile_pair&, ordinary_launch::deadline);
    static void validate_managed_profile_until(const ordinary_managed_profile&,lattice_db&,database&,ordinary_launch::deadline);
    static void validate_managed_profile_impl(const ordinary_managed_profile&,lattice_db&,database&,
        std::optional<ordinary_launch::deadline>);
    static void validate_managed_profile(const ordinary_managed_profile&,lattice_db&,database&);
    static void require_managed_profile_if_present(lattice_db&,database&,const ordinary_history_limits* = nullptr);
    friend struct ordinary_wire_original_test_access;
    static recovery_install_result with_original_store_for_test(lattice_db&,const std::function<void(ordinary_wire_original_turn&)>&);
    friend class ordinary_receiver_session;
    friend class ::lattice::lattice_db;
    static void note_history_uncaptured(lattice_db&);
    static std::unique_ptr<lattice_db> child(lattice_db&,const configuration&,
        std::shared_ptr<ordinary_history_child_connections>* history_custody=nullptr);
    static bool fresh_origin(const lattice_db&,const physical_store_identity&);
    static ordinary_export_frame snapshot(lattice_db&,database&,const std::vector<std::string>&, bool include_values = true);
    static std::optional<owned_server_export> capture_impl(lattice_db&,
        const std::vector<std::string>&, database*, bool, bool wire = false);
    enum class original_policy { foundation, authenticated_session };
    // Only the existing authenticated receiver-session friend enters this
    // route. Neither public arguments nor decoded metadata select it.
    static std::vector<std::string> apply_session(lattice_db&,const owned_server_export&,const std::string&,ordinary_managed_wss_acceptance* = nullptr);
    static std::vector<std::string> apply_impl(lattice_db&, const owned_server_export&,
        const std::optional<std::string>&, bool, const std::string* wire_channel = nullptr,
        original_policy = original_policy::foundation);
public:
    static owned_server_export capture(lattice_db&, const std::vector<std::string>&);
    // Explicit ordinary-required, synchronous source intake. Caller retains
    // the actual owner until return; passive upload data is never a grant.
    // Trusted managed early-entry only: actual retained child capability is
    // mandatory. A durable profile row is continuity evidence, never its issuer.
    // Separate receiver early entry: preserves actual local observations in
    // an atomic owned WRITE using a real inherited restart capability. It does
    // not issue source, accepted baseline, rebind, materialization or ACK rights.
    static std::shared_ptr<const ordinary_managed_receiver_preservation> preserve_managed_receiver(
        std::shared_ptr<lattice_db>,const std::shared_ptr<const ordinary_managed_child_admission>&,
        ordinary_launch::deadline,const ordinary_history_limits& = {});
    static std::shared_ptr<const ordinary_managed_profile> enroll_managed_history(
        lattice_db&,const std::shared_ptr<const ordinary_managed_child_admission>&,
        const ordinary_history_limits& = {});
    static std::vector<std::string> receive_upload(lattice_db&,std::string_view,const std::vector<std::string>&);
    static ordinary_upload_acknowledgment_result receive_upload_with_acknowledgment(lattice_db&,std::string_view,const std::vector<std::string>&);
    static void admit(class ordinary_source_upload_delivery&,lattice_db&,database&);
    static owned_server_export capture_wire(lattice_db&, const std::vector<std::string>&);
    // Selection only: validate the current logical projection in one retained
    // read view; do not allocate a source cut or return a receiving grant.
    static std::vector<std::string> observation_tables(lattice_db&, const std::vector<std::string>&);
    static std::vector<std::string> apply(lattice_db&, const owned_server_export&);
    static server_export_batch events(database&, const std::optional<std::string>&);
    static std::vector<std::string> apply_helper(lattice_db&, const server_export_batch&,
        const std::optional<std::string>&);
    static std::optional<std::vector<std::string>> provisional(lattice_db&,
        const std::vector<audit_log_entry>&, const std::optional<std::string>&);
    // Synchronous borrowed query only: caller retains the exact wrapper/parent
    // through return. The closed engine callback cannot settle a caller's turn.
    static std::vector<audit_log_entry> upload_query(database&, bool,
        const std::function<std::vector<audit_log_entry>()>&,bool exact_originals=false);
    // Actual caller-owned WRITE only; retains unresolved original payloads
    // independently of legacy ACK flags and history reclamation.
    static void preserve(lattice_db&, database&);
    static bool legacy_settlement_allowed(database&);
    static bool has_delivery_obligations(database&);
    static bool delivery_floor_allowed(database&,const std::string&);
    static std::vector<ordinary_delivery_outcome> acknowledge_upload(lattice_db&,const std::string&,const std::vector<ordinary_upload_ack>&);
    static void admit(class ordinary_provisional_delivery&, lattice_db&, database&);
    static void finish(class ordinary_provisional_delivery&, database&,
        const std::vector<std::string>&, size_t);
};
class ordinary_source_upload_delivery final {
    friend class ordinary_export_access;
    struct state;
    std::unique_ptr<state> state_;
    explicit ordinary_source_upload_delivery(lattice_db&,std::shared_ptr<database>,ordinary_source_upload_batch&&,std::vector<std::string>);
public:
    ~ordinary_source_upload_delivery();
    const std::vector<audit_log_entry>& entries()const;
    void admit(lattice_db& owner,database& writer){ordinary_export_access::admit(*this,owner,writer);}
    // A verified pre-existing committed record of this immutable original
    // proves retry, independently of mutable sender-local/ACK observations.
    // A legacy AuditLog/receipt duplicate is never retroactively promoted.
    bool duplicate(database&,const audit_log_entry&);
    void inserted(database&,const audit_log_entry&);
    void rolled_back_entry(const audit_log_entry&)noexcept;
    void finish(database&,const std::vector<std::string>&,size_t);
};
class ordinary_child_origin final {
    friend class ordinary_export_access;
    const physical_store_identity physical_;
    explicit ordinary_child_origin(physical_store_identity physical):physical_(std::move(physical)){}
};
class ordinary_provisional_delivery final {
    friend class ordinary_export_access;
    lattice_db* owner_;
    std::shared_ptr<database> writer_;
    std::vector<audit_log_entry> entries_;
    std::optional<std::string> channel_;
    std::vector<column_value_t> binding_;
    std::map<std::string,std::string> applied_;
    ordinary_provisional_delivery(lattice_db&,std::shared_ptr<database>,const std::vector<audit_log_entry>&,
        const std::optional<std::string>&);
public:
    void admit(lattice_db& owner,database& writer){ordinary_export_access::admit(*this,owner,writer);}
    void finish(database& writer,const std::vector<std::string>& ids,size_t begin){ordinary_export_access::finish(*this,writer,ids,begin);}
    bool already_applied(const audit_log_entry&) const;
};
}
