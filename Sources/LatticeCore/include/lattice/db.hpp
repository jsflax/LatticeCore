#pragma once

#ifdef __cplusplus

#include "types.hpp"
#include "ordinary_context.hpp"
#include <sqlite3.h>
#include <stdexcept>
#include <unordered_map>
#include <atomic>
#include <functional>
#include <chrono>
#include <memory>
#include <mutex>
#include <algorithm>
#include <thread>
#include <exception>
#include <vector>
#include <optional>

namespace lattice {

/// Default statement-level busy timeout. Headless/server processes tolerate long
/// waits; interactive apps should pass a smaller value (e.g. 5000) via
/// configuration::busy_timeout_ms so a stuck writer can't hang the UI thread.
inline constexpr int kDefaultBusyTimeoutMs = 30000;

namespace detail { class wal_extent_vfs;struct wal_extent_test_access;uint64_t database_native_process() noexcept; struct ordinary_target_scope_native_fact; struct ordinary_target_scope_root_observation; }

class db_error : public std::runtime_error {
public:
    explicit db_error(const std::string& msg) : std::runtime_error(msg) {}
};

// An operation's first failure is independent of separately retained Native
// cleanup debt. No diagnostic allocation is needed to retain these originals.
// Callers may rethrow first; cleanup debt must never be replaced by that text.
class mutation_operation_error final : public std::exception {
    std::exception_ptr first_, rollback_, release_, delivery_, finalize_, capture_, publication_;
    bool unit_settled_ = false, transaction_committed_ = false;
public:
    mutation_operation_error(std::exception_ptr first, std::exception_ptr rollback,
        std::exception_ptr release, std::exception_ptr delivery = {}, std::exception_ptr finalize = {},
        std::exception_ptr capture = {}, std::exception_ptr publication = {},
        bool unit_settled = false, bool transaction_committed = false) noexcept
        : first_(std::move(first)), rollback_(std::move(rollback)), release_(std::move(release)),
          delivery_(std::move(delivery)), finalize_(std::move(finalize)), capture_(std::move(capture)),
          publication_(std::move(publication)), unit_settled_(unit_settled),
          transaction_committed_(transaction_committed) {}
    const char* what() const noexcept override {
        // Preserve the real first diagnostic, including a valid empty what().
        // Diagnostic availability never substitutes for operation status/debt.
        try { if (first_) std::rethrow_exception(first_); }
        catch (const std::exception& error) { return error.what(); }
        catch (...) {}
        return "mutation operation failed; cleanup debt retained";
    }
    const std::exception_ptr& first() const noexcept { return first_; }
    const std::exception_ptr& rollback_failure() const noexcept { return rollback_; }
    const std::exception_ptr& release_failure() const noexcept { return release_; }
    const std::exception_ptr& delivery_failure() const noexcept { return delivery_; }
    const std::exception_ptr& finalize_failure() const noexcept { return finalize_; }
    const std::exception_ptr& capture_failure() const noexcept { return capture_; }
    const std::exception_ptr& publication_failure() const noexcept { return publication_; }
    bool immediate_unit_settled() const noexcept { return unit_settled_; }
    bool outer_transaction_committed() const noexcept { return transaction_committed_; }
};

/// Only installed on a connection exclusively owned by one bounded operation.
/// Dormant managed preparation privately spans its checked open and owned archive
/// WRITE; only its known clean success may detach that exact deadline control.
/// Target publication protects sqlite3_interrupt from late cancellation/UAF.
struct database_read_control {
    std::atomic<int32_t> stop_code{0};
    std::chrono::steady_clock::time_point deadline;
    std::mutex target_mutex;
    sqlite3* target = nullptr;
    // A managed inherited control cannot lock or interrupt its parent target.
    const uint64_t creator_process = detail::database_native_process();
    bool stopped() noexcept;
    void stop(int32_t reason) noexcept;
    void publish(sqlite3* handle) noexcept;
    void unpublish(sqlite3* handle) noexcept;
};

/// Native live-file identity. filename is SQLite's decoded absolute filename,
/// canonicalized for diagnostics; matching uses device/inode, not spelling.
/// Capture validates pathname stability with HAS_MOVED, not an atomic fstat of
/// SQLite's descriptor. Concurrent external replacement during capture is
/// unsupported; detected movement/replacement fails projected reads closed.
struct physical_store_identity {
    uint64_t device = 0, inode = 0;
    std::string filename;
    bool operator==(const physical_store_identity& other) const noexcept {
        return device == other.device && inode == other.inode;
    }
};

class lattice_db;
class database;
struct owned_catalog_state;
namespace detail { void require_catalog_database_callback_absent(const database*); }
namespace detail {
uint64_t database_native_process() noexcept;
struct exact_vector_rows_access;
struct recovery_writer_access;
struct recovery_witness_access;
struct recovery_refresh_access;
struct receive_delivery_guard_access;
class canonical_writer_adapter;
class authenticated_relay_setup;
struct canonical_writer_custody_test_access;
class recovery_local_producer_adapter;
class recovery_continuous_producer;
struct recovery_continuous_admission;
class database_retirement_state;
class database_open_ledger;
class ordinary_target_staging_connection;
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
struct ordinary_target_lifetime_observation;
#endif
class ordinary_target_history_page_reader;
struct ordinary_target_history_read_span;
class ordinary_target_staging_io;
class ordinary_target_scope_root_tail;
struct ordinary_target_scope_root_record;
class ordinary_target_history_page_append;
class ordinary_group_target_schema_original;
class ordinary_group_target_receiving_owner;
struct ordinary_history_progress;
class ordinary_export_access;
class ordinary_cohort_attachment_access;
struct database_retirement_access;
void require_continuous_raw_handle_absent(database&);
void require_continuous_legacy_export_absent(database&);
class recovery_obligation_producer_store;
void require_canonical_relation(database&, const std::string&);
bool prepare_recovery_local_producer(lattice_db&, const std::shared_ptr<database>&);
void publish_recovery_local_producer(lattice_db&, database&) noexcept;
bool preserve_recovery_local_producer_relation(database&, const std::string&);
void require_recovery_local_producer_maintenance_absent(database&);
void reset_sync_channel_with_producer_fence(lattice_db&, const std::string&, bool retire);
void initialize_receive_guard_schema(database&, bool legacy_origin);
bool receive_guard_manages_cursor(database&);
void require_receive_guard_history_unblocked(database&);

// One ordinary attached-field operation. Main/manual database fields keep
// their existing path. The implementation never acquires a topology mutex
// beneath SQLite; it validates the published generation while holding SQLite.
class managed_route_scope {
    database* db_ = nullptr;
    std::shared_ptr<database> writer_owner_;
    lattice_db* owner_ = nullptr;
    sqlite3_mutex* mutex_ = nullptr;
    std::unique_lock<std::recursive_timed_mutex> vector_gate_;
    managed_route_scope* previous_ = nullptr;
    int exceptions_ = 0;
    static thread_local managed_route_scope* current_;
public:
    managed_route_scope(database*, lattice_db*, const std::string& table,
                        int64_t token, const std::weak_ptr<database>& writer,
                        bool vector_write = false);
    ~managed_route_scope() noexcept(false);
    managed_route_scope(const managed_route_scope&) = delete;
    managed_route_scope& operator=(const managed_route_scope&) = delete;
    static bool active_for(const database*) noexcept;
    static bool active_for(const lattice_db*) noexcept;
};
}

class database {
public:
    enum class open_mode; // Declared before private factories use the public type.
private:
    friend class lattice_db;
    friend class detail::ordinary_target_staging_connection;
    friend struct topology_admission_test_access;
    friend struct detail::exact_vector_rows_access;
    friend struct detail::recovery_writer_access;
    friend struct detail::recovery_witness_access;
    friend struct detail::recovery_refresh_access;
    friend struct detail::receive_delivery_guard_access;
    friend class detail::canonical_writer_adapter;
    friend class detail::authenticated_relay_setup;
    friend struct detail::canonical_writer_custody_test_access;
    friend class detail::recovery_local_producer_adapter;
    friend class detail::recovery_continuous_producer;
    friend struct detail::database_retirement_access;
    friend class detail::ordinary_export_access;
    friend class detail::ordinary_cohort_attachment_access;
    friend void detail::require_continuous_raw_handle_absent(database&);
    friend void detail::require_continuous_legacy_export_absent(database&);
    friend class detail::recovery_obligation_producer_store;
    friend void detail::require_canonical_relation(database&, const std::string&);
    friend bool detail::preserve_recovery_local_producer_relation(database&, const std::string&);
    friend void detail::require_recovery_local_producer_maintenance_absent(database&);
    struct terminal_database_shell_tail {
        const uint64_t process_;const std::thread::id thread_;const database* const original_;
        bool construction_returned_=false;
        std::shared_ptr<detail::ordinary_target_scope_root_record> first_;
        explicit terminal_database_shell_tail(const database*)noexcept;
        ~terminal_database_shell_tail()noexcept;
        bool bind(const std::shared_ptr<detail::ordinary_target_scope_root_record>&)noexcept;
        terminal_database_shell_tail(const terminal_database_shell_tail&)=delete;
        terminal_database_shell_tail& operator=(const terminal_database_shell_tail&)=delete;
    };
    // First inline member: no separate early heap allocation, no private-src include.
    terminal_database_shell_tail target_terminal_shell_tail_{this};
    // Private fixed-scope trigger qualification lacks upstream receipt settlement.
    bool canonical_trigger_only_ = false;
    std::shared_ptr<void> canonical_callback_custody_;
    std::shared_ptr<std::atomic<bool>> canonical_write_allowed_;
    // Physical policy custody, serialized by SQLite's connection mutex.
    // Bootstrap closes the interval before the canonical context is published.
    bool canonical_custody_bootstrap_ = false;
    // Failed producer bootstrap must not let optional close-time ANALYZE
    // mutate the schema it just refused. This policy follows the physical
    // handle through moves, even before producer callback custody exists.
    bool suppress_destructor_optimize_ = false;
    bool administrative_connection_ = false;
    bool checked_existing_connection_ = false; // Private one-shot open: no optional close maintenance.
    // Writable constructors classify eagerly. Read-only internal/keeper paths
    // defer until a guarded raw/legacy export boundary. Unknown never means
    // absence; this denial-only state cannot grant producer/source authority.
    enum class continuous_classification { unknown, ordinary, protected_file };
    std::atomic<continuous_classification> continuous_file_{continuous_classification::unknown};
    bool txn_hooks_external_ = false;
    void set_txn_hooks_owned_(std::function<void()>, std::function<void()>,
        std::function<std::function<void()>(database&, bool)> = {});
    void rebind_txn_hooks_owned_() noexcept;
    // Private receiver producer admission. Heap custody follows the physical
    // connection across wrapper moves; no all-route capability is implied.
    std::shared_ptr<void> local_producer_callback_custody_;
    std::shared_ptr<std::atomic<bool>> local_producer_write_allowed_;
    // A failed channel-reset cleanup cannot leave partial work committable.
    // Only an explicit successful rollback clears this physical-writer fence.
    std::atomic<bool> channel_reset_unsettled_{false};
    // A fresh wrapper SQL entry offers one bounded turn to an actual topology
    // mutex waiter. Recursive owned SQL never waits behind its own mutex.
    void wait_for_topology_turn_() const;
    size_t topology_waiter_count_() const noexcept;
    struct core_transaction_original;
    class sql_entry_scope {
        const database* owner_;
        sql_entry_scope* previous_;
        static thread_local sql_entry_scope* current_;
    public:
        explicit sql_entry_scope(const database&);
        sql_entry_scope(const database&, const std::shared_ptr<const core_transaction_original>&);
        ~sql_entry_scope() noexcept { release(); }
        void release() noexcept;
        static bool active_for(const database*) noexcept;
        sql_entry_scope(const sql_entry_scope&) = delete;
        sql_entry_scope& operator=(const sql_entry_scope&) = delete;
    };
    int step_statement_(sqlite3_stmt*) const;
    int bind_value_checked_(sqlite3_stmt*, int, const column_value_t&);
    static int finalize_mutation_statement_(sqlite3_stmt*) noexcept;
    // Only a genuine failed step on this exact live statement may supply
    // its already retained primary. Unknown/different final results stay debt.
    static int finalize_failed_mutation_statement_(sqlite3_stmt*, int,
        const std::exception_ptr&) noexcept;
    static int finalize_mutation_statement_impl_(sqlite3_stmt*, int,
        const std::exception_ptr&) noexcept;
    // Connection-owned observation only; never changes authorizer/trace/hooks.
    void note_history_mutation_(sqlite3_stmt*) const noexcept;
    int execute_observed_sql_(const char*);
    // Only materializing engine queries can prove that an autocommit read
    // cursor belongs to another thread and will be finalized by its caller.
    // The connection mutex guards the intrusive list; TLS rejects reentry
    // even from prepare/step callbacks before a cursor becomes busy.
    struct engine_query_scope {
        database& owner;
        sqlite3_stmt* statement = nullptr;
        engine_query_scope* previous;
        engine_query_scope* next_read = nullptr;
        const std::thread::id thread = std::this_thread::get_id();
        bool registered = false, active = true;
        const bool defers_delivery;
        static inline thread_local engine_query_scope* current = nullptr;
        explicit engine_query_scope(database&, bool defer_delivery = false);
        ~engine_query_scope() noexcept;
        void adopt(sqlite3_stmt*) noexcept;
        void reset() noexcept;
        static bool active_for(const database&) noexcept;
        static bool defers_delivery_for(const database&) noexcept;
        engine_query_scope(const engine_query_scope&) = delete;
        engine_query_scope& operator=(const engine_query_scope&) = delete;
    };
    engine_query_scope* engine_reads_ = nullptr;
    // Physical Core BEGIN provenance, guarded by this connection's SQLite
    // mutex. A parent routing stamp alone cannot identify a raw successor.
    mutable std::thread::id core_transaction_thread_{};
    // A same-thread successor BEGIN is a different physical transaction even
    // when it reuses the same live TLS lifetime original.
    uint64_t core_transaction_serial_ = 0;
    // A reusable native thread id is not a lifetime. Only a genuine Core
    // BEGIN may issue this private original; it captures no parent or writer.
    struct core_thread_lifetime { std::atomic<bool> alive{true}; };
    static thread_local bool core_thread_lifetime_ended_;
    struct core_thread_holder {
        std::shared_ptr<core_thread_lifetime> original;
        ~core_thread_holder() noexcept {
            core_thread_lifetime_ended_=true;
            if(original)original->alive.store(false,std::memory_order_release);
        }
    };
    static core_thread_holder& core_thread_lifetime_() noexcept;
    // Resource-free ending DATA for the exact actual Core BEGIN. Receipts may
    // retain this cell after writer/original destruction without retaining any
    // Native connection, checkpoint, callback or retirement payload.
    struct core_transaction_ending { std::atomic<bool> ended{true}; };
    struct core_transaction_original {
        const database* const owner;
        sqlite3* const connection;
        const uint64_t creator;
        const std::thread::id thread;
        const std::shared_ptr<const core_thread_lifetime> lifetime;
        const std::shared_ptr<detail::database_retirement_state> native;
        // Produced by the engine-owned ordinary hook before this BEGIN's
        // first attempt. It contains Native tuples/flags, never user callbacks.
        std::shared_ptr<void> native_checkpoint;
        uint64_t serial = 0; // issued once under SQLite, before publication
        std::atomic<bool> local_ended{true}; // non-receipt operations add no allocation
        const std::shared_ptr<core_transaction_ending> ending;
        std::atomic<bool>& ended; // exact existing BEGIN/Native-ending stores
        core_transaction_original(const core_transaction_original&) = delete;
        core_transaction_original& operator=(const core_transaction_original&) = delete;
        core_transaction_original(core_transaction_original&&) = delete;
        core_transaction_original& operator=(core_transaction_original&&) = delete;
        core_transaction_original(database* value, sqlite3* handle,
            std::shared_ptr<const core_thread_lifetime> live,
            std::shared_ptr<detail::database_retirement_state> original,
            std::shared_ptr<core_transaction_ending> receipt_ending) noexcept
            : owner(value), connection(handle), creator(detail::database_native_process()),
              thread(std::this_thread::get_id()), lifetime(std::move(live)), native(std::move(original)),
              ending(std::move(receipt_ending)), ended(ending ? ending->ended : local_ended) {}
        bool same_native_for(const database&) const noexcept;
        bool current_for(const database&) const noexcept;
    };
    mutable std::shared_ptr<core_transaction_original> core_transaction_original_;
    // Includes the live transaction AND its not-yet-captured ended tail. A
    // generic off-Native drain must not read a successor's uncommitted buffer.
    std::atomic<bool> core_native_tail_owned_{false};
    mutable std::weak_ptr<const core_thread_lifetime> core_transaction_lifetime_;
    bool core_transaction_origin_live_locked_() const noexcept;
    bool core_transaction_origin_current_locked_() const noexcept;
    void note_core_transaction_end_locked_(int native_result = SQLITE_ERROR) const noexcept;
    // Private complete mutation unit. Its caller first offers a public SQL
    // turn, then takes the existing store/vec0 gate; this scope retains the
    // physical mutex through BEGIN/savepoint, body and actual settlement.
    // A routing stamp alone never grants a caller-transaction savepoint.
    uint64_t mutation_operation_serial_ = 0; // physical SQLite mutex guarded
    std::atomic<bool> mutation_operation_unsettled_{false};
    class mutation_sql_scope {
        friend class lattice_db;
        friend class database;
        const uint64_t creator_ = detail::database_native_process();
        database& owner_;
        std::atomic<std::thread::id>& route_;
        sqlite3* connection_ = nullptr;
        sqlite3_mutex* mutex_ = nullptr;
        std::shared_ptr<detail::database_retirement_state> retirement_;
        std::shared_ptr<const core_thread_lifetime> origin_;
        std::shared_ptr<const core_transaction_original> transaction_;
        std::exception_ptr finalization_failure_;
        mutation_sql_scope* previous_ = nullptr;
        static thread_local mutation_sql_scope* current_;
        enum class phase { admission, active, settling, committed, rolled_back, lost };
        phase phase_ = phase::admission;
        bool owns_transaction_ = false, issued_ = false, installed_ = false;
        bool locked_ = false;
        uint64_t core_serial_ = 0;
        char savepoint_[80]{}, rollback_[96]{}, release_[96]{};
        bool same_native_() const noexcept;
        bool same_transaction_() const noexcept;
        void require_transaction_() const;
        void control_(const char*);
        void observe_end_() noexcept;
    public:
        struct cleanup {
            std::exception_ptr rollback, release;
            bool transaction_ended = false;
        };
        mutation_sql_scope(database&, std::atomic<std::thread::id>&);
        ~mutation_sql_scope() noexcept;
        void start(); // only after the complete owner/cleanup object exists
        void commit();
        cleanup abort() noexcept;
        void unlock() noexcept;
        static bool active_for(const database*) noexcept;
        mutation_sql_scope(const mutation_sql_scope&) = delete;
        mutation_sql_scope& operator=(const mutation_sql_scope&) = delete;
    };
    friend class detail::managed_route_scope;
    // A live owned transaction or its actual ended Native tail. The capture
    // phase cannot grant a new transaction; it only lets the already retained
    // writer materialize that original committed/rolled-back delivery.
    class core_settlement_scope {
        database& owner_;
        std::shared_ptr<const core_transaction_original> original_;
        core_settlement_scope* previous_;
        static thread_local core_settlement_scope* current_;
    public:
        core_settlement_scope(database&, std::shared_ptr<const core_transaction_original>, bool capture);
        ~core_settlement_scope() noexcept;
        static bool active_for(const database*) noexcept;
        core_settlement_scope(const core_settlement_scope&) = delete;
    };
    // Only the database and named closed engine factories construct this key.
    // The keyed overload stays accessible to make_shared for one allocation.
    class initialization_key {
        friend class database;
        friend class detail::recovery_continuous_producer;
        friend class detail::authenticated_relay_setup;
        friend class swift_lattice;
        friend class detail::ordinary_target_staging_connection;
        const bool keeper_cache_;
        const bool administrative_ = false;
        std::shared_ptr<const physical_store_identity> administrative_identity_;
        const bool checked_existing_ = false;
        const bool checked_mounted_server_ = false;
        std::shared_ptr<const physical_store_identity> checked_identity_;
        detail::ordinary_context checked_context_;
        std::shared_ptr<detail::database_retirement_state> checked_retirement_;
        std::shared_ptr<detail::database_open_ledger> open_ledger_;
        size_t open_slot_=0; uint64_t open_serial_=0;
        // Only the real target connection derives this unopened shell key.
        std::weak_ptr<detail::ordinary_target_staging_connection> target_connection_;
        std::shared_ptr<detail::ordinary_target_staging_io> target_io_;
        initialization_key(const std::shared_ptr<detail::ordinary_target_staging_connection>& connection,
            const std::shared_ptr<detail::ordinary_target_staging_io>& io,
            std::shared_ptr<detail::database_retirement_state> retirement,
            std::shared_ptr<detail::database_open_ledger> ledger,size_t slot,uint64_t serial)
            :keeper_cache_(false),checked_retirement_(std::move(retirement)),open_ledger_(std::move(ledger)),
             open_slot_(slot),open_serial_(serial),target_connection_(connection),target_io_(io) {}
        initialization_key(bool keeper,std::shared_ptr<detail::database_retirement_state> retirement,
            std::shared_ptr<detail::database_open_ledger> ledger,size_t slot,uint64_t serial)
            :keeper_cache_(keeper),checked_retirement_(std::move(retirement)),open_ledger_(std::move(ledger)),open_slot_(slot),open_serial_(serial) {}
        initialization_key(std::shared_ptr<const physical_store_identity> identity,
            detail::ordinary_context context,std::shared_ptr<detail::database_retirement_state> retirement)
            :keeper_cache_(false),checked_existing_(true),checked_identity_(std::move(identity)),
             checked_context_(std::move(context)),checked_retirement_(std::move(retirement)) {}
        struct mounted_server_key { private: friend class swift_lattice; mounted_server_key() = default; };
        initialization_key(std::shared_ptr<const physical_store_identity> identity,
            std::shared_ptr<detail::database_retirement_state> retirement,mounted_server_key)
            :keeper_cache_(false),checked_existing_(true),checked_mounted_server_(true),
             checked_identity_(std::move(identity)),checked_retirement_(std::move(retirement)) {}
        explicit initialization_key(std::shared_ptr<const physical_store_identity> value)
            : keeper_cache_(false), administrative_(true), administrative_identity_(std::move(value)) {}
        std::shared_ptr<detail::recovery_continuous_admission> continuous_;
        explicit initialization_key(std::shared_ptr<detail::recovery_continuous_admission> value) : keeper_cache_(false), continuous_(std::move(value)) {}
        explicit initialization_key(bool keeper_cache) : keeper_cache_(keeper_cache) {}
    public:
        initialization_key(const initialization_key&) = default;
    };
    static std::shared_ptr<database> make_read_keeper(const std::string& path,
                                                    int busy_timeout_ms,
                                                    detail::ordinary_context context = {},
                                                    std::shared_ptr<detail::database_open_ledger> ledger = {});
    // Internal lifetime custody only. Empty ledger preserves ordinary factories.
    struct owned_connection_deleter {
        std::shared_ptr<detail::database_open_ledger> ledger;
        size_t slot=0; uint64_t serial=0;
        void operator()(database*) const noexcept;
    };
    using private_connection=std::unique_ptr<database,owned_connection_deleter>;
    static std::shared_ptr<database> make_owned_connection(const std::string&,open_mode,int,
        std::shared_ptr<database_read_control>,detail::ordinary_context,
        std::shared_ptr<detail::database_open_ledger>,bool keeper=false,
        std::optional<sqlite3_int64> wal_extent_limit = {});
    static private_connection make_private_connection(const std::string&,open_mode,int,
        std::shared_ptr<database_read_control>,detail::ordinary_context,
        std::shared_ptr<detail::database_open_ledger>);
    struct target_staging_deferred_t { private: friend class database; target_staging_deferred_t() = default; };
    enum class terminal_decoder_payload_result {not_held,captures_ended,retained_debt};
    terminal_decoder_payload_result retire_target_terminal_decoder_payloads_(
        const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)noexcept;
    bool target_terminal_decoder_payloads_ended_(
        const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)const noexcept;
    enum class terminal_native_phase_result {not_held,phases_ended,retained_debt};
    terminal_native_phase_result retire_target_terminal_native_phases_(
        const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)noexcept;
    bool target_terminal_native_phases_ended_(
        const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)const noexcept;
    enum class terminal_native_outer_result {not_held,roots_ended,retained_debt};
    terminal_native_outer_result retire_target_terminal_native_outer_roots_(
        const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)noexcept;
    bool target_terminal_native_outer_roots_ended_(
        const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)const noexcept;
    bool target_terminal_phase_history_after_outer_known_(
        const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)const noexcept;
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
    detail::ordinary_target_lifetime_observation qualification_target_lifetime_observation_(
        const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)const noexcept;
#endif
    enum class terminal_native_statics_result {not_held,objects_ended,retained_debt};
    terminal_native_statics_result retire_target_terminal_native_statics_(
        const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)noexcept;
    bool target_terminal_native_static_history_known_(
        const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)const noexcept;
    bool target_terminal_native_statics_ended_(
        const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)const noexcept;
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
    detail::ordinary_target_scope_root_observation qualification_target_scope_root_observation_(const detail::ordinary_target_staging_connection&,
        const detail::ordinary_group_target_receiving_owner&)const noexcept;
#endif
    bool retire_target_terminal_scope_roots_(detail::ordinary_target_staging_connection&,detail::ordinary_group_target_receiving_owner&)noexcept;
    bool target_terminal_scope_root_context_snapshot_(const detail::ordinary_target_staging_connection&,
        detail::ordinary_target_scope_native_fact&)const noexcept;
    bool target_terminal_scope_root_context_matches_(const detail::ordinary_target_staging_connection&,
        const detail::ordinary_target_scope_native_fact&)const noexcept;
    bool target_terminal_programme_originals_known_(const detail::ordinary_target_staging_connection&,
        const detail::ordinary_group_target_receiving_owner&,const detail::ordinary_group_target_schema_original&,
        const std::array<const void*,4>&,const std::array<std::exception_ptr,4>&)const noexcept;
    bool target_terminal_programme_history_known_(const detail::ordinary_target_staging_connection&,
        const detail::ordinary_group_target_receiving_owner&,const void*,
        const std::array<const void*,4>&,const std::array<std::exception_ptr,4>&)const noexcept;
    enum class terminal_native_pages_result {not_held,pages_ended,retained_debt};
    terminal_native_pages_result retire_target_terminal_native_pages_(
        const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)noexcept;
    bool target_terminal_native_pages_ended_(
        const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)const noexcept;
    bool target_terminal_native_page_history_known_(
        const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)const noexcept;
    bool target_terminal_outer_history_before_pages_known_(
        const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)const noexcept;
    bool bind_target_terminal_shell_destruction_(const detail::ordinary_target_staging_connection&,
        const detail::ordinary_target_scope_native_fact&,
        const std::shared_ptr<detail::ordinary_target_scope_root_record>&,
        const std::shared_ptr<detail::ordinary_target_scope_root_record>&,
        std::shared_ptr<detail::database_open_ledger>&)noexcept;
    struct target_staging_state;
    static void wake_target_terminal_native_static_(const std::weak_ptr<target_staging_state>&,
        const std::weak_ptr<detail::ordinary_group_target_receiving_owner>&,
        const detail::ordinary_target_staging_connection*)noexcept;
    static void wake_target_terminal_native_page_(const std::weak_ptr<target_staging_state>&,
        const std::weak_ptr<detail::ordinary_group_target_receiving_owner>&,
        const detail::ordinary_target_staging_connection*)noexcept;
    static void wake_target_terminal_native_outer_(const std::weak_ptr<target_staging_state>&,
        const std::weak_ptr<detail::ordinary_group_target_receiving_owner>&,
        const detail::ordinary_target_staging_connection*)noexcept;
    static void wake_target_terminal_native_phase_(const std::weak_ptr<target_staging_state>&,
        const std::weak_ptr<detail::ordinary_group_target_receiving_owner>&,
        const detail::ordinary_target_staging_connection*)noexcept;
    static void wake_target_terminal_decoder_payload_(const std::weak_ptr<target_staging_state>&,
        const std::weak_ptr<detail::ordinary_group_target_receiving_owner>&,
        const detail::ordinary_target_staging_connection*)noexcept;
    std::shared_ptr<target_staging_state> target_staging_;
    database(initialization_key,target_staging_deferred_t);
    static std::shared_ptr<database> make_target_staging_shell(
        const std::shared_ptr<detail::ordinary_target_staging_connection>&);
    void initialize_target_staging_();
    void require_target_fixed_constructor_(const detail::ordinary_target_staging_connection&)const;
    void bootstrap_target_metadata_(const std::shared_ptr<detail::ordinary_group_target_schema_original>&);
    void require_target_metadata_ending_(const detail::ordinary_target_staging_connection&,
        const detail::ordinary_group_target_schema_original&)const;
    void install_target_catalog_(const std::shared_ptr<detail::ordinary_group_target_schema_original>&);
    bool target_catalog_native_ending_()const noexcept;
    void require_target_catalog_ending_(const detail::ordinary_target_staging_connection&,
        const detail::ordinary_group_target_schema_original&)const;
    void stage_target_history_header_(const std::shared_ptr<detail::ordinary_group_target_receiving_owner>&);
    void stage_target_raw_history_header_(const std::shared_ptr<detail::ordinary_group_target_receiving_owner>&);
    void stage_target_history_header_program_(const std::shared_ptr<detail::ordinary_group_target_receiving_owner>&,bool);
    bool target_history_raw_storage_(const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)const;
    bool target_history_header_native_ending_()const noexcept;
    bool target_history_guard_destroyed_()const noexcept;
    bool target_history_page_guard_destroyed_()const noexcept;
    bool target_history_page_native_ending_()const noexcept;
    void stage_target_history_page_(const std::shared_ptr<detail::ordinary_group_target_receiving_owner>&);
    void stage_target_raw_history_page_(const std::shared_ptr<detail::ordinary_group_target_receiving_owner>&,
        const std::shared_ptr<const detail::ordinary_target_history_page_append>&);
    void stage_target_history_page_program_(const std::shared_ptr<detail::ordinary_group_target_receiving_owner>&,
        const std::shared_ptr<const detail::ordinary_target_history_page_append>&);
    void require_target_raw_history_page_ending_(const detail::ordinary_target_staging_connection&,
        const detail::ordinary_group_target_receiving_owner&,const detail::ordinary_target_history_page_append&)const;
    void require_target_raw_history_page_observation_(const detail::ordinary_target_staging_connection&,
        const detail::ordinary_group_target_receiving_owner&,const detail::ordinary_target_history_page_append&)const;
    detail::ordinary_history_progress target_history_page_progress_(const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)const;
    uint64_t target_history_page_append_before_bytes_(const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&,detail::ordinary_history_progress&)const;
    void require_target_history_header_ending_(const detail::ordinary_target_staging_connection&,
        const detail::ordinary_group_target_receiving_owner&)const;
    void import_target_history_rows_(const std::shared_ptr<detail::ordinary_group_target_receiving_owner>&);
    void require_target_row_import_issued_(const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)const;
    void require_target_row_import_ending_(const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)const;
    bool target_row_import_native_ending_()const noexcept;
    bool target_row_import_cleanup_ending_()const noexcept;
    void suspend_target_import_(const std::shared_ptr<detail::ordinary_group_target_receiving_owner>&);
    void require_target_import_suspension_issued_(const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)const;
    void require_target_import_suspension_ending_(const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)const;
    bool target_import_suspension_native_ending_()const noexcept;
    bool target_import_suspension_cleanup_ending_()const noexcept;
    void verify_target_history_chain_(const std::shared_ptr<detail::ordinary_group_target_receiving_owner>&);
    void require_target_history_chain_ending_(const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)const;
    void require_target_history_chain_issued_(const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)const;
    bool target_history_chain_native_ending_()const noexcept;
    bool target_history_chain_cleanup_ending_()const noexcept;
    detail::ordinary_target_history_read_span target_history_chain_raw_span_(const detail::ordinary_target_staging_connection&,
        const detail::ordinary_group_target_receiving_owner&,const void*,const void*,const detail::ordinary_target_history_page_reader*)const;
    detail::ordinary_target_history_read_span target_row_import_raw_span_(const detail::ordinary_target_staging_connection&,
        const detail::ordinary_group_target_receiving_owner&,const void*,const void*,const detail::ordinary_target_history_page_reader*)const;
    void require_target_history_chain_raw_retired_(const detail::ordinary_target_staging_connection&,
        const detail::ordinary_group_target_receiving_owner&,const void*,const detail::ordinary_target_history_page_reader&)const;
    void require_target_row_import_raw_retired_(const detail::ordinary_target_staging_connection&,
        const detail::ordinary_group_target_receiving_owner&,const void*,const detail::ordinary_target_history_page_reader&)const;
    void validate_target_history_projection_(const std::shared_ptr<detail::ordinary_group_target_receiving_owner>&);
    void require_target_history_complete_(const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)const;
    bool target_history_projection_native_ending_()const noexcept;
    bool target_history_projection_cleanup_ending_()const noexcept;
    void require_target_history_projection_ending_(const detail::ordinary_target_staging_connection&,const detail::ordinary_group_target_receiving_owner&)const;
    void retire_target_staging_() noexcept;
    // Actual retained first failure, available only before first retirement.
    std::exception_ptr target_staging_failure_original_(const detail::ordinary_target_staging_connection&)const noexcept;
    bool target_staging_native_cleanup_known_(const detail::ordinary_target_staging_connection&)const noexcept;
    bool target_staging_cleanup_joined_(const detail::ordinary_target_staging_connection&)const noexcept;
    bool target_staging_creator_current_() const noexcept;
    template<typename T, typename Enable> friend struct managed;
    friend class swift_lattice;
    friend class projection_service;
    friend struct projection_operation_state;
    friend struct database_projection_capture;
    friend struct owned_catalog_state;
    // The update hook may query globalId through database::query(). That
    // nested query must not drain a prior row's dirty state while the outer
    // SQLite statement still owns its connection mutex. Track the actual
    // callback frame, including callers that step SQLite directly.
    struct update_hook_scope {
        sqlite3* connection;
        update_hook_scope* previous;
        static inline thread_local update_hook_scope* current = nullptr;
        explicit update_hook_scope(database& db) noexcept
            : update_hook_scope(db.db_) {}
        explicit update_hook_scope(sqlite3* handle) noexcept
            : connection(handle), previous(current) { current = this; }
        ~update_hook_scope() noexcept { current = previous; }
        update_hook_scope(const update_hook_scope&) = delete;
        update_hook_scope& operator=(const update_hook_scope&) = delete;
        static bool active_for(sqlite3* connection) noexcept {
            for (auto* frame = current; frame; frame = frame->previous) {
                if (frame->connection == connection) return true;
            }
            return false;
        }
    };

    // SQLite retains this address as update-hook userdata. Heap ownership
    // keeps it stable when a database wrapper moves; physical identity does
    // not depend on which writer the lattice currently publishes. The raw
    // lattice owner must still outlive all uses of the connection.
    struct sync_apply_chunk_state {
        enum class phase { not_started, active, committed, rolled_back };
        phase state = phase::not_started;
        // A pre-commit callback is not proof of durable settlement. It only
        // prevents an admitted reset from following COMMIT into a successor,
        // including memory and no-write transactions with no WAL callback.
        bool commit_attempted = false;
        // Only self-owned private maintenance uses this restriction. Sync and
        // caller-owned reset markers remain observational by default.
        enum class commit_policy { observational, owner_body, owner_finalizing };
        commit_policy policy = commit_policy::observational;
        bool premature_commit = false;
        // A rollback consumes this reservation before a successor can write.
        // The operation's separate owner-lifetime hold lasts through unwind.
        bool owns_recovery_reservation = false;
    };
    struct lattice_update_hook_context {
        lattice_db* owner = nullptr;
        sqlite3* connection = nullptr;
        // Only touched while owning this physical connection's SQLite mutex.
        // The added hook path uses POD, with no allocation/SQL/user callback.
        sync_apply_chunk_state* sync_chunk = nullptr;
        bool recovery_delivery_deferred = false;
        // Distinct ordinary Core transaction tail. Never borrow the receiver
        // success bits, synthesize MAX(AuditLog), or publish at a savepoint.
        std::shared_ptr<const core_transaction_original> mutation_original;
        bool mutation_commit_attempted = false, mutation_rollback_observed = false;
        bool mutation_tail_pending = false, mutation_tail_committed = false;
        bool mutation_cursor_present = false;
        int64_t mutation_cursor_last = 0;
        std::exception_ptr mutation_first_delivery_failure;
        void (*mutation_preparing)(lattice_update_hook_context&, core_transaction_original&) = nullptr;
        void (*mutation_started)(lattice_update_hook_context&, const std::shared_ptr<const core_transaction_original>&) noexcept = nullptr;
        void (*mutation_ended)(lattice_update_hook_context&, bool, bool) noexcept = nullptr;
        bool entry_cursor_active = false;
        bool entry_cursor_present = false;
        int64_t entry_cursor_last = 0;
        bool consume_recovery_reservation(sync_apply_chunk_state* settlement) noexcept {
            if (!settlement || !settlement->owns_recovery_reservation) return false;
            settlement->owns_recovery_reservation = false;
            entry_cursor_active = false;
            entry_cursor_present = false;
            recovery_delivery_deferred = false;
            return true;
        }
        void note_settled(bool committed) noexcept {
            if (!sync_chunk) return;
            sync_chunk->state = committed ? sync_apply_chunk_state::phase::committed
                                          : sync_apply_chunk_state::phase::rolled_back;
            // Consume before callbacks can open a successor transaction.
            sync_chunk = nullptr;
        }
    };
    std::unique_ptr<lattice_update_hook_context> lattice_update_hook_context_;

    // Private multi-statement maintenance ownership. FULLMUTEX alone only
    // serializes individual SQLite calls; another thread must not join this
    // owner's transaction between its safety reads and writes. The caller
    // takes any store gate first and drains notifications after both scopes.
    struct maintenance_scope {
        database& owner;
        sqlite3_mutex* mutex;
        maintenance_scope* previous = nullptr;
        static inline thread_local maintenance_scope* current = nullptr;
        bool entered = false;
        enum class admission { ready, mutex_busy, engine_read_busy, core_write_busy };
        static bool idle(database& db) noexcept;
        // The optional stamp comes only from the retained actual Core owner.
        // It permits no SQL/transaction borrowing: a genuine other-thread
        // Core WRITE is a no-effect busy result for bounded automatic work.
        static admission controller_probe(database& db,
            const std::atomic<std::thread::id>* core_transaction = nullptr);
        static admission controller_classify_locked(database& db,
            const std::atomic<std::thread::id>* core_transaction = nullptr);
        maintenance_scope(database& db, admission* controller_admission,
            const std::atomic<std::thread::id>* core_transaction = nullptr);
        static void probe_before_store_gate(database& db);
        // False is only the initial no-effect SQLite mutex contention probe.
        static bool try_probe_before_store_gate(database& db);
        explicit maintenance_scope(database& db);
        ~maintenance_scope() noexcept;
        maintenance_scope(const maintenance_scope&) = delete;
        maintenance_scope& operator=(const maintenance_scope&) = delete;
        static bool active_for(sqlite3* connection) noexcept {
            for (auto* frame = current; frame; frame = frame->previous) {
                if (frame->owner.db_ == connection) return true;
            }
            return false;
        }
    };

public:
    /// Open mode for database connections
    enum class open_mode {
        read_write,  ///< Full read/write access (default)
        read_only    ///< Read-only; joins a concurrent writer's WAL (sees committed WAL rows)
    };

    explicit database(const std::string& path, open_mode mode = open_mode::read_write,
                      int busy_timeout_ms = kDefaultBusyTimeoutMs,
                      std::shared_ptr<database_read_control> read_control = {},
                   detail::ordinary_context context = {});
    // Private construction capability; no caller can manufacture the key.
    database(const std::string& path, open_mode mode, int busy_timeout_ms,
             std::shared_ptr<database_read_control> read_control, initialization_key key,
             detail::ordinary_context context = {}, std::optional<sqlite3_int64> wal_extent_limit = {});
    ~database();

    /// No SQL statements. Best-effort for legacy callers; nullptr means an
    /// unsupported/moved/nonfilesystem store or interrupted metadata wait.
    /// The main identity is cached lazily; validation is required on a newly
    /// opened private lease. Callers never hold a global registry lock here.
    std::shared_ptr<const physical_store_identity> physical_identity(
        const std::string& schema = "main",
        const std::shared_ptr<database_read_control>& control = {},
        bool validate_current = false) const;


    /// Logically close the connection: subsequent ops short-circuit to empty/no-op.
    /// The underlying sqlite3* is NOT freed here — it is released in ~database (which
    /// is single-threaded), so a reader on another thread can never deref a freed
    /// handle. Use instead of destroying the wrapper while readers may still hold it.
    void close();

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
    void begin_transaction(bool exclusive = false);
    /// Try to begin an IMMEDIATE transaction with a short timeout.
    /// Returns true if the transaction was started, false if the DB is busy.
    /// Use for optional write paths (e.g. vec0 reconciliation) where blocking is worse than skipping.
    bool try_begin_immediate(int timeout_ms = 100);
    void commit();
    void rollback();
    bool is_in_transaction() const;

    // Execute SQL with optional params (for INSERT/UPDATE/DELETE without return)
    void execute(const std::string& sql,
                 const std::vector<column_value_t>& params = {});

    /// Rows changed by the most recent INSERT/UPDATE/DELETE on this
    /// connection (sqlite3_changes64). With value-guarded writes (a DO
    /// UPDATE arm or UPDATE gated on actual value inequality) 0 means the
    /// statement was a genuine no-op — the sync apply path uses this to
    /// suppress relay minting for value-identical redundant deliveries.
    int64_t changes() const;

    /// Advance this connection's WAL read snapshot to see the latest committed data.
    /// Needed when another connection wrote and this connection's mmap'd WAL index is stale.
    void refresh_wal_snapshot();

    /// Interrupt any in-flight statement on this connection
    /// (sqlite3_interrupt). Safe to call from another thread. Used by the
    /// read-generation force-retire protocol: SQLite refuses COMMIT while
    /// statements are in progress, so a wedged in-flight read must be kicked
    /// before the keeper transaction can close (results spec §3.4).
    void interrupt();

    /// Result of a wal_checkpoint() call. rc retains the PRAGMA-style result
    /// code; busy is 1 when the checkpoint could not complete because a
    /// reader/writer held the WAL; log_frames/checkpointed mirror the PRAGMA
    /// row (-1 when unavailable). Native SQLITE_BUSY maps to rc=SQLITE_OK,
    /// busy=1; other native errors retain rc=SQLITE_ERROR and unavailable frames.
    struct checkpoint_result {
        int rc = 0;
        int busy = 1;
        int64_t log_frames = -1;
        int64_t checkpointed = -1;
    };

    /// Run a WAL checkpoint on this (read-write) connection.
    /// PASSIVE (truncate=false) backfills as far as the oldest live reader
    /// allows and never blocks anyone. TRUNCATE (truncate=true) additionally
    /// resets the -wal file to zero length, but must wait out readers — the
    /// busy_budget_ms bounds that wait so a held snapshot makes it FAIL FAST
    /// instead of stalling writers. No-op (busy=1) on read-only connections,
    /// closed connections, and Emscripten (DELETE journal mode).
    checkpoint_result wal_checkpoint(bool truncate, int busy_budget_ms = 250);

    /// Process-global count of SQL statements issued through the public
    /// funnels (query/execute/insert/update/remove) across ALL connections.
    /// Test/bench primitive: recall-style code paths span multiple
    /// connections (read/write/xproc, attached lattices), so a per-connection
    /// counter undercounts — tests assert on deltas of this global.
    static uint64_t total_statement_count();

    /// Thread-local twin of total_statement_count(): counts only statements
    /// issued from the calling thread. Exact budgets for single-threaded
    /// read paths, immune to parallel test suites in the same process.
    static uint64_t thread_statement_count();
    /// Raw bounded read cursors use the same statement accounting funnel.
    static void record_statement();

    /// Mark this connection dirty: buffered row changes await delivery once
    /// the enclosing transaction settles. Relaxed store — callable from inside
    /// sqlite3_update_hook (C frame: no locks, nothing that can throw).
    void mark_txn_dirty() { txn_dirty_.store(true, std::memory_order_relaxed); }

    /// Install the transaction-settled drain and rollback-discard callbacks
    /// (docs/design-deferred-memory-delivery.md). `settled` runs after any
    /// successful statement that leaves the connection in autocommit mode
    /// with the dirty flag set — i.e. at the close of every top-level
    /// transaction (including the explicit COMMIT, which funnels through
    /// execute()), on the writing thread, outside all SQLite frames.
    /// `rolled_back` is invoked from sqlite3_rollback_hook (C frame — it must
    /// only clear state, never touch SQLite or throw) and defensively on
    /// failed statements whose implicit transaction already rolled back.
    void set_txn_hooks(std::function<void()> settled, std::function<void()> rolled_back);

    /// Raw access retires canonical admission before pointer publication and
    /// refuses during its bootstrap, transaction or active statement. Public
    /// transaction-hook replacement likewise refuses attached policy custody.
    /// Raw access permanently opts this connection out of strict borrowed
    /// memory projection capture: external SQLite handlers cannot be restored
    /// or proven read-only. Waits behind an active capture before exposing it.
    sqlite3* handle() const;

    // Bind a value to a prepared statement (public for lattice_db bulk insert)
    void bind_value(sqlite3_stmt* stmt, int index, const column_value_t& value);

    /// Whether close() has been called. Ops check this and short-circuit.
    bool is_closed() const { return closed_.load(std::memory_order_acquire); }

private:
    // Trusted Core/bridge callers only; never return this pointer to a client.
    // Only lattice_db supplies its existing transaction stamp. Publication
    // occurs inside the successful BEGIN call's same SQLite mutex custody.
    void begin_transaction_owned_(std::atomic<std::thread::id>& owner_thread,
        std::shared_ptr<const core_transaction_original>* issued = nullptr, bool strict_receipt = false,
        const std::shared_ptr<core_transaction_ending>& receipt_ending = {});
    void begin_transaction_impl_(bool exclusive, std::atomic<std::thread::id>* owner_thread,
        std::shared_ptr<const core_transaction_original>* issued = nullptr, bool strict_receipt = false,
        const std::shared_ptr<core_transaction_ending>& receipt_ending = {});
    struct core_settlement_result {
        std::exception_ptr first, publication, capture;
        std::function<void()> delivery;
        bool ended = false, committed = false;
    };
    void capture_core_tail_locked_(core_settlement_result&);
    core_settlement_result end_transaction_owned_(bool rollback, std::atomic<std::thread::id>& owner_thread,
        const std::shared_ptr<const core_transaction_original>& expected);
    void end_transaction_owned_into_(bool rollback, std::atomic<std::thread::id>& owner_thread,
        const std::shared_ptr<const core_transaction_original>& expected, core_settlement_result& result,
        bool strict_receipt = false);
    void execute_impl_(const std::string& sql, const std::vector<column_value_t>& params,
        std::atomic<std::thread::id>* owner_thread);
    sqlite3* internal_handle() const noexcept { return !target_staging_ && creator_current_() && !catalog_quarantined_() ? db_ : nullptr; }
    // A real SQLite admission, not a precheck lease. Finalization/destructor
    // cleanup uses its existing paths and may settle quarantined originals.
    struct catalog_native_admission_ {
        sqlite3_mutex* mutex = nullptr;
        explicit catalog_native_admission_(const database&);
        ~catalog_native_admission_() noexcept;
        catalog_native_admission_(const catalog_native_admission_&) = delete;
    };
    bool catalog_quarantined_() const noexcept;
    void require_catalog_clean_locked_() const;
    void quarantine_catalog_locked_() noexcept;
    int prepare_catalog_checked_(const char*, int, sqlite3_stmt**, const char**) const;
    bool creator_current_() const noexcept;
    void require_creator_() const;
    static sqlite3* note_owning_wrapper_move_(database&) noexcept;
    void validate_ordinary_current_() const;
    // Private to the unpublished checked owner; no rearm/reset after failure.
    void finish_checked_preparation_read_control_(std::chrono::steady_clock::time_point);
    detail::ordinary_context ordinary_context_;
    bool ordinary_guarded_ = false;
    sqlite3* db_ = nullptr;
    // Follows this exact SQLite connection through moves. A retained state
    // outlives the wrapper; close_v2 success is never physical-close proof.
    std::shared_ptr<detail::database_retirement_state> retirement_;
    void retire_connection_(bool detach_callbacks = true) noexcept;
    mutable std::atomic<bool> raw_handle_escaped_{false};
    std::string path_;
    open_mode mode_;
    // Set by close(); ops short-circuit when set. db_ stays valid until ~database,
    // so this is a logical-close flag, not a lifetime guard.
    std::atomic<bool> closed_{false};
    int busy_timeout_ms_ = kDefaultBusyTimeoutMs;
    std::shared_ptr<database_read_control> read_control_;
    friend struct detail::wal_extent_test_access;
    std::shared_ptr<detail::wal_extent_vfs> wal_extent_vfs_;
    mutable std::shared_ptr<const physical_store_identity> main_physical_identity_;
    // Deferred delivery (docs/design-deferred-memory-delivery.md): set by the
    // update hook via mark_txn_dirty(); consumed by drain_if_settled() at the
    // success tail of every statement wrapper; cleared by the rollback hook.
    std::atomic<bool> txn_dirty_{false};
    struct txn_hook_callbacks {
        std::function<void()> settled, rolled_back;
        // Engine-only state capture. It may inspect the already admitted exact
        // writer, but returns all observer delivery for the off-lock tail.
        // Public opaque hooks do not gain an in-lock callback through this API.
        std::function<std::function<void()>(database&, bool)> capture_owned;
        txn_hook_callbacks(std::function<void()>&& success, std::function<void()>&& rollback,
                           std::function<std::function<void()>(database&, bool)>&& capture = {})
            : settled(std::move(success)), rolled_back(std::move(rollback)),
              capture_owned(std::move(capture)) {}
    };
    // One absolute admission/retry budget for a complete public topology call.
    // This is not a timer for SQL, filesystem work, callbacks or destruction.
    struct attachment_operation {
        using clock = std::chrono::steady_clock;
        static int budget_ms(int configured) noexcept {
            return configured > 0 ? std::min(configured, 2000) : 2000;
        }
        const clock::time_point deadline;
        explicit attachment_operation(int recipient_timeout_ms, int source_timeout_ms = 0)
            : deadline(clock::now() + std::chrono::milliseconds(
                std::min(budget_ms(recipient_timeout_ms), budget_ms(source_timeout_ms)))) {}
        explicit attachment_operation(clock::time_point end) : deadline(end) {}
        attachment_operation(const attachment_operation&) = delete;
        attachment_operation& operator=(const attachment_operation&) = delete;
        struct delivery {
            std::shared_ptr<txn_hook_callbacks> hooks;
            bool rollback = false, active = false;
            std::function<void()> captured;
        };
        std::vector<delivery> deliveries;
        database* capturing_owner = nullptr; // Internal read tails cannot settle recursively.
        // Retain exact wrappers and retired extension captures through off-lock
        // delivery; final releases must not occur beneath topology/SQLite locks.
        std::vector<std::shared_ptr<database>> handles;
        std::shared_ptr<const void> retired_metadata; // At most one invalidated alias per public call.
        void check() const;
        void wait(std::chrono::milliseconds interval = std::chrono::milliseconds(1)) const;
        std::unique_lock<std::mutex> lock(std::mutex&) const;
        void capture_settlement_locked(database&, bool failed);
        void settle_locked(database&, bool failed, std::exception_ptr primary);
        void finish(std::exception_ptr primary);
    };
    struct attachment_delivery_error : db_error {
        std::exception_ptr primary, delivery;
        size_t delivery_failures;
        attachment_delivery_error(std::exception_ptr p, std::exception_ptr d, size_t count)
            : db_error("topology operation and off-lock delivery both failed"),
              primary(std::move(p)), delivery(std::move(d)), delivery_failures(count) {}
    };
    class topology_wait_scope {
        std::shared_ptr<detail::database_retirement_state> original_;
        size_t slot_ = static_cast<size_t>(-1);
        bool published_ = false;
        uint64_t token_ = 0;
    public:
        // This bypass names one actual denial cell, not a database-wide permit.
        // The controller keeps it only on its own synchronous worker stack.
        class bypass {
            friend class database;
            friend class topology_wait_scope;
            // Immutable identity retains the retirement original against ABA.
            // An old callback must never bypass a successor turn's new cell.
            const std::shared_ptr<detail::database_retirement_state> original_;
            const size_t slot_;
            const uint64_t token_;
            bypass* previous_;
            static thread_local bypass* current_;
            static bool excludes(const detail::database_retirement_state*,size_t,uint64_t) noexcept;
        public:
            explicit bypass(const topology_wait_scope& value) noexcept : original_(value.original_),
                slot_(value.published_?value.slot_:static_cast<size_t>(-1)),token_(value.published_?value.token_:0),previous_(current_) { current_=this; }
            ~bypass() noexcept { current_=previous_; }
            bypass(const bypass&)=delete;
            bypass& operator=(const bypass&)=delete;
        };
        topology_wait_scope() = default;
        ~topology_wait_scope() noexcept { release(); }
        void begin(database&, attachment_operation&);
        // Private denial-only controller retry: same physical cells and fixed
        // deadline; no attachment, transaction or producer authority is issued.
        void begin_until(database&, attachment_operation::clock::time_point);
        void tighten_until(attachment_operation::clock::time_point) noexcept;
        bool active() const noexcept { return published_; }
        // Only after release(); transfer possible final private witness
        // destruction to the caller's off-leaf retired-custody tail.
        std::shared_ptr<detail::database_retirement_state> take_retired_original() noexcept { return std::move(original_); }
        bool matches(const bypass& value) const noexcept { return published_&&token_!=0&&
            original_==value.original_&&slot_==value.slot_&&token_==value.token_; }
        void release() noexcept;
        topology_wait_scope(const topology_wait_scope&) = delete;
        topology_wait_scope& operator=(const topology_wait_scope&) = delete;
    };
    class attachment_sql_scope {
        database& owner_;
        attachment_operation& operation_;
        topology_wait_scope waiting_;
        sqlite3_mutex* mutex_ = nullptr;
        attachment_sql_scope* previous_ = nullptr;
        static thread_local attachment_sql_scope* current_;
    public:
        attachment_sql_scope(database&, attachment_operation&);
        ~attachment_sql_scope();
        attachment_sql_scope(const attachment_sql_scope&) = delete;
        attachment_sql_scope& operator=(const attachment_sql_scope&) = delete;
        static attachment_operation* operation_for(const database*) noexcept;
    };
    void execute_attachment_sql(const std::string&, attachment_operation&);
    std::shared_ptr<const physical_store_identity> attachment_physical_identity(attachment_operation&);
    // Construct/destroy callable targets outside SQLite. Under its mutex only
    // shared_ptr ownership moves; std::function moves/swaps may run user code.
    std::shared_ptr<txn_hook_callbacks> txn_hooks_;
    column_value_t extract_column(sqlite3_stmt* stmt, int index);
    // Internal live primitive getter path. Preserve query()'s first-row/name
    // and stored-type conventions without building generic result containers.
    // Empty optional means no matching first-row cell; a present nullptr is
    // SQL NULL. The owning connection, fresh statement and settled tail remain.
    std::optional<column_value_t> query_managed_cell(
        const std::string& sql, const std::string& column, primary_key_t row_id);
    // ATTACH-only internal operation. Capture metadata in the same SQLite
    // execution scope, before a competing writer can win a second acquisition.
    // This captures only internal metadata; deferred user delivery stays after it.
    std::shared_ptr<const physical_store_identity> attach_and_capture_identity(
        const std::string& attach_sql, const std::string& schema, attachment_operation&);
    // Caller owns this handle's recursive SQLite mutex.
    std::shared_ptr<const physical_store_identity> physical_identity_locked(
        const std::string& schema,
        const std::shared_ptr<database_read_control>& control) const;
    // Same checks and wait policy; failure is a borrowed static label only.
    // Private diagnostics cannot issue identity or weaken current-file custody.
    struct physical_identity_details {
        enum class stage { entry, cached_identity, connection_mutex, file_name, vfs,
            first_move, canonical_path, initial_stat, second_move, final_stat,
            identity_comparison, cache_comparison, complete };
        enum class vfs_kind { unobserved, unavailable_or_other, unix_vfs, unix_excl };
        struct move_result {
            bool observed = false;
            int rc = 0;
            bool moved_valid = false;
            int moved = 0;
            void record(int result, int output) noexcept {
                observed = true; rc = result; moved_valid = result == SQLITE_OK;
                moved = moved_valid ? output : 0;
            }
        };
        stage last_stage = stage::entry;
        bool mutex_acquired = false;
        vfs_kind vfs = vfs_kind::unobserved;
        bool vfs_result_observed = false;
        int vfs_rc = 0;
        move_result first_move, second_move;
    };
    std::shared_ptr<const physical_store_identity> physical_identity_observed(
        const std::string& schema,
        const std::shared_ptr<database_read_control>& control,
        bool validate_current, const char*& failure, physical_identity_details* details = nullptr) const;
    std::shared_ptr<const physical_store_identity> physical_identity_locked_observed(
        const std::string& schema,
        const std::shared_ptr<database_read_control>& control,
        const char*& failure, physical_identity_details* details = nullptr) const;
    struct physical_identity_attempt {
        enum class status { captured, mutex_busy, refused };
        status state = status::refused;
        std::shared_ptr<const physical_store_identity> identity;
        const char* failure = nullptr;
    };
    // Automatic source setup only. One mutex try, no wait/cached fast path/SQL/retry;
    // successful acquisition uses the same complete current-file checks.
    physical_identity_attempt try_current_physical_identity(const std::string& schema) const;
    // Attachment schema metadata only. Run the existing single read statement
    // inside one SQLite execution scope; keep original SQLite types and names.
    std::vector<std::string> query_attachment_text_metadata(
        const std::string& sql, const std::string& column, attachment_operation&);
    void drain_if_settled();
    void discard_if_rolled_back();
};

// RAII transaction guard
class transaction {
public:
    explicit transaction(database& db, bool exclusive = false);
    ~transaction();

    void commit();
    void rollback();

private:
    database& db_;
    bool completed_ = false;
};

} // namespace lattice

#endif // __cplusplus
