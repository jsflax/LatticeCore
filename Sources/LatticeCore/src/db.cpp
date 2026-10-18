#include <cstdio>
#include "ordinary_target_scope_root_tail.hpp"
#include "recovery_producer_continuity.hpp"
#include "lattice/db.hpp"
#include "lattice/native_retirement_storage.hpp"
#include "ordinary_cohort_attachment.hpp"
#include "lattice/projection.hpp"
#include "lattice/log.hpp"
#include "checkpoint_test_probe.hpp"
#include "recovery_admission_test_probe.hpp"
#include "database_open_test_probe.hpp"
#include "database_retirement.hpp"
#include "database_sql_dispatcher.hpp"
#include <lattice/detail/observer_delivery_startup.hpp>
#include "database_initialization_test_probe.hpp"
#include "database_sql_request.hpp"
#include "database_sql_read_admission.hpp"
#include "database_initialization_budget.hpp"
#include <new>
#include <climits>
#include "observer_delivery_original.hpp"
#include "observer_delivery_sql_runtime.hpp"
#include "recovery_topology_wait_observation.hpp"
#include "catalog_test_probe.hpp"
#include "database_open_ledger.hpp"
#include <lattice/store_resource_context.hpp>
#include <lattice/store_physical_api.hpp>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include "store_physical_api.hpp"
#include "wal_extent_vfs.hpp"
#endif
#include <sqlite-vec.h>
#include "../../SqliteVec/src/sqlite-vec-registration.h"
#include "ordinary_initialization_storage.hpp"
#include <sstream>
#include <iostream>
#include <thread>
#include <chrono>
#include <filesystem>
#include <algorithm>
#include <sys/stat.h>
#include <exception>
#include <utility>
#include <limits>

namespace lattice {
void detail::store_resource_database_code_anchor() noexcept {}
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
namespace detail {
const std::uint64_t wal_extent_registry_process=database_native_process();
wal_extent_slot_registry* const wal_extent_registry=new wal_extent_slot_registry;
}
#endif

bool detail::wal_extent_attachment_guard_required(sqlite3* native) noexcept {
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    return wal_extent_vfs::requires_attachment_guard(native)||store_physical_is_enrolled(native);
#else
    (void)native;return false;
#endif
}

// Each actual registration owns one SQLite-retained cookie. The wrapper holds
// one independent DATA reference; no receipt owns a Native pointer/close grant.
struct database::initialization_registrations_ final {
    static constexpr unsigned vec_count=SQLITE_VEC_LATTICE_REGISTRATION_COUNT,core_slot=vec_count;
    static constexpr std::uint32_t vec_mask=(std::uint32_t(1)<<vec_count)-1;
    static constexpr std::uint32_t complete_mask=(std::uint32_t(1)<<(vec_count+1))-1;
    const std::uint64_t creator;
    database* const owner;
    sqlite3* const native;
    const detail::observer_delivery_open_startup* const open;
    const detail::store_physical_open_original* const physical;
    std::atomic<unsigned> references{1};
    std::atomic<std::uint32_t> attempted{0},live{0};
    std::atomic<int> first_api_rc{SQLITE_OK};
    std::atomic<bool> retired{false},vec_complete{false};
    initialization_registrations_(std::uint64_t issuer,database& actual)noexcept
        :creator(issuer),owner(&actual),native(issuer==detail::database_native_process()?actual.db_:nullptr),
         open(issuer==detail::database_native_process()?actual.delivery_startup_.get():nullptr),physical(&actual.physical_open_original_){}
    void release()noexcept {
        if(creator==detail::database_native_process()&&references.fetch_sub(1,std::memory_order_acq_rel)==1)delete this;
    }
    bool same_original()const noexcept {
        if(creator!=detail::database_native_process())return false;
        return owner&&owner->creator_current_()&&owner->initialization_registrations_value_==this&&
            owner->db_==native&&owner->delivery_startup_.get()==open&&
            &owner->physical_open_original_==physical&&physical->matches_open_(native)&&
            !owner->closed_.load(std::memory_order_acquire)&&!owner->raw_handle_escaped_.load(std::memory_order_acquire);
    }
    struct cookie final {
        sqlite_vec_lattice_cookie declaration{};
        const std::uint64_t creator;
        initialization_registrations_* const original;
        const unsigned slot;
        cookie(std::uint64_t issuer,initialization_registrations_& value,unsigned index)noexcept:creator(issuer),original(&value),slot(index) {
            declaration.creator=creator;
            if(creator!=detail::database_native_process())return;
            original->references.fetch_add(1,std::memory_order_relaxed); // bounded21 cookies, one wrapper
            declaration.payload=this;declaration.retire=destroy;
        }
        ~cookie()noexcept {
            if(creator!=detail::database_native_process())return;
            original->retired.store(true,std::memory_order_release);
            original->live.fetch_and(~(std::uint32_t(1)<<slot),std::memory_order_acq_rel);
            original->release();
        }
        static void destroy(void* opaque)noexcept {
            auto* actual=static_cast<cookie*>(opaque);
            if(actual&&actual->creator==detail::database_native_process())delete actual;
        }
        static void sqlite_destroy(void* opaque)noexcept {
            auto* declaration=static_cast<sqlite_vec_lattice_cookie*>(opaque);
            // Every actual cookie puts its creator-fenced enclosing address in
            // payload. The callback never follows a database/Open/Native owner.
            if(declaration&&declaration->creator==detail::database_native_process())destroy(declaration->payload);
        }
    };
    struct batch final {
        const std::uint64_t creator=detail::database_native_process();
        initialization_registrations_* original=nullptr;
        std::array<cookie*,vec_count> values{};
        char* error=nullptr;
        // The actual retirement shell calls reset(), then destroys this batch.
        // Clear each owned address before ending it so both calls are idempotent.
        void reset()noexcept {
            if(creator!=detail::database_native_process())return;
            for(auto*& value:values) {
                if(creator!=detail::database_native_process())return;
                cookie::destroy(std::exchange(value,nullptr));
            }
            if(creator!=detail::database_native_process())return;
            original=nullptr; // Borrow only; never an owning reference to release.
            if(auto* actual=std::exchange(error,nullptr))sqlite3_free(actual);
        }
        ~batch()noexcept {reset();}
        static sqlite_vec_lattice_cookie* take(void* opaque,unsigned slot)noexcept {
            auto* self=static_cast<batch*>(opaque);
            if(!self||self->creator!=detail::database_native_process()||!self->original||
               slot>=vec_count||!self->original->same_original()||self->original->retired.load(std::memory_order_acquire))return nullptr;
            auto* value=self->values[slot];if(!value)return nullptr;
            const auto bit=std::uint32_t(1)<<slot;
            if(self->original->attempted.fetch_or(bit,std::memory_order_acq_rel)&bit)return nullptr;
            self->values[slot]=nullptr; // actual next v2 call owns ending, including failure
            return &value->declaration;
        }
        static int returned(void* opaque,unsigned slot,int actual_rc)noexcept {
            auto* self=static_cast<batch*>(opaque);
            if(!self||self->creator!=detail::database_native_process()||!self->original)return 0;
            auto& original=*self->original;
            if(actual_rc!=SQLITE_OK){int expected=SQLITE_OK;original.first_api_rc.compare_exchange_strong(expected,actual_rc);}
            else if(slot<vec_count)original.live.fetch_or(std::uint32_t(1)<<slot,std::memory_order_acq_rel);
            return original.same_original()&&(actual_rc!=SQLITE_OK||!original.retired.load(std::memory_order_acquire));
        }
    };
    static void sync_disabled(sqlite3_context* ctx,int,sqlite3_value**) {
        const auto* declaration=static_cast<const sqlite_vec_lattice_cookie*>(sqlite3_user_data(ctx));
        if(!declaration||declaration->creator!=detail::database_native_process())return;
        const auto* actual=static_cast<const cookie*>(declaration->payload);
        if(!actual||actual->creator!=detail::database_native_process())return;
        sqlite3* db=actual->original->native;
        sqlite3_stmt* stmt=nullptr;int disabled=0;
        // Original ordinary callback SQL/step/default/finalization semantics.
        if(detail::store_physical_prepare_v2(db,"SELECT disabled FROM _SyncControl WHERE id=1",-1,&stmt,nullptr)==SQLITE_OK) {
            if(detail::store_physical_step(stmt)==SQLITE_ROW)disabled=sqlite3_column_int(stmt,0);
            detail::store_physical_finalize(stmt);
        }
        sqlite3_result_int(ctx,disabled);
    }
};
void database::initialize_vec_registrations_() {
    require_creator_();
    if(!delivery_startup_)throw db_error("pending vector registrations require their actual Open");
    auto& open=*delivery_startup_;
    const auto creator=detail::database_native_process();
    try {
        open.require_initializing_(*this);
        if(initialization_registrations_value_)throw db_error("pending vector registration original already issued");
        detail::ordinary_initialization_storage_scope storage(&open,this,
            &detail::observer_delivery_open_startup::debit_initialization_storage_);
        detail::ordinary_initialization_storage_scope::charge(sizeof(initialization_registrations_));
        initialization_registrations_value_=new initialization_registrations_(creator,*this);
        if(creator!=detail::database_native_process())throw db_error("inherited registration original allocation");
        using custody=detail::native_retirement_storage<initialization_registrations_::batch>;
        detail::ordinary_initialization_storage_scope::charge(custody::allocation_bytes());custody held;
        if(creator!=detail::database_native_process())throw db_error("inherited registration cookie custody");
        auto& batch=held.get<0>();batch.original=initialization_registrations_value_;
        for(unsigned index=0;index<initialization_registrations_::vec_count;++index) {
            detail::ordinary_initialization_storage_scope::charge(sizeof(initialization_registrations_::cookie));
            batch.values[index]=new initialization_registrations_::cookie(creator,*initialization_registrations_value_,index);
            if(creator!=detail::database_native_process())throw db_error("inherited registration cookie allocation");
        }
        const sqlite_vec_lattice_issuer issuer{&batch,initialization_registrations_::batch::take,
            initialization_registrations_::batch::returned};
        const int rc=sqlite3_vec_init_with_lattice_receipts(db_,&batch.error,nullptr,&issuer);
        if(creator!=detail::database_native_process())throw db_error("inherited checked vector registration return");
        const int first=initialization_registrations_value_->first_api_rc.load(std::memory_order_acquire);
        if(first!=SQLITE_OK)open.initialization_budget_(*this)->note_api_error_(first,false);
        if(rc!=SQLITE_OK||first!=SQLITE_OK||!initialization_registrations_value_->same_original()||
           initialization_registrations_value_->retired.load(std::memory_order_acquire)||
           initialization_registrations_value_->live.load(std::memory_order_acquire)!=initialization_registrations_::vec_mask)
            throw db_error("actual checked vector registration failed");
        open.require_initializing_(*this);
        initialization_registrations_value_->vec_complete.store(true,std::memory_order_release);
    }catch(...) {open.fail_initialization_();throw;}
}
void database::register_initialization_sync_disabled_() {
    require_creator_();
    if(!delivery_startup_)throw db_error("checked sync_disabled requires its actual pending Open");
    auto& open=*delivery_startup_;
    try {
        open.require_initializing_(*this);
        auto* original=initialization_registrations_value_;
        if(!original||!original->same_original()||!original->vec_complete.load(std::memory_order_acquire)||
           original->retired.load(std::memory_order_acquire))
            throw db_error("pending Native registration was retired or failed");
        const auto bit=std::uint32_t(1)<<initialization_registrations_::core_slot;
        if(original->live.load(std::memory_order_acquire)&bit)return; // actual receipt reuse, no Native replacement
        detail::ordinary_initialization_storage_scope storage(&open,this,
            &detail::observer_delivery_open_startup::debit_initialization_storage_);
        detail::ordinary_initialization_storage_scope::charge(sizeof(initialization_registrations_::cookie));
        const auto creator=detail::database_native_process();
        struct pending_cookie final {
            const std::uint64_t creator;
            initialization_registrations_::cookie* value=nullptr;
            ~pending_cookie()noexcept{if(creator==detail::database_native_process())initialization_registrations_::cookie::destroy(value);}
        } held{creator};
        held.value=new initialization_registrations_::cookie(creator,*original,initialization_registrations_::core_slot);
        if(creator!=detail::database_native_process())throw db_error("inherited Core registration cookie");
        catalog_native_admission_ admitted(*this);open.require_initializing_(*this);
        if(!original->same_original()||original->retired.load(std::memory_order_acquire))throw db_error("Core registration original changed before actual Native call");
        if(original->attempted.fetch_or(bit,std::memory_order_acq_rel)&bit)throw db_error("Core registration already attempted");
        auto* value=std::exchange(held.value,nullptr); // v2 owns ending even on actual API failure
        bool actual_native_entered=false;
        const int rc=detail::store_physical_create_function_v2_checked(db_,"sync_disabled",0,SQLITE_UTF8,&value->declaration,
            initialization_registrations_::sync_disabled,nullptr,nullptr,initialization_registrations_::cookie::sqlite_destroy,&actual_native_entered);
        if(creator!=detail::database_native_process())throw db_error("inherited Core registration return");
        if(!actual_native_entered)throw db_error("checked sync_disabled registration refused before Native entry");
        if(rc!=SQLITE_OK){int expected=SQLITE_OK;original->first_api_rc.compare_exchange_strong(expected,rc);
            open.initialization_budget_(*this)->note_api_error_(rc,false);throw db_error("actual checked sync_disabled registration failed");}
        original->live.fetch_or(bit,std::memory_order_acq_rel);
        if(!original->same_original()||original->retired.load(std::memory_order_acquire))
            throw db_error("pending Native registration was retired or failed");
        open.require_initializing_(*this);
    }catch(...) {open.fail_initialization_();throw;}
}
bool database::initialization_registrations_current_(const detail::observer_delivery_open_startup& open)const noexcept {
    // Caller owns the actual catalog Native leaf. Comparison DATA only: this
    // never proves builtin module provenance or publishes a finished owner.
    if(!creator_current_())return false;
    const auto* original=initialization_registrations_value_;
    return original&&original->same_original()&&original->open==&open&&
        !original->retired.load(std::memory_order_acquire)&&original->vec_complete.load(std::memory_order_acquire)&&
        original->first_api_rc.load(std::memory_order_acquire)==SQLITE_OK&&
        original->attempted.load(std::memory_order_acquire)==initialization_registrations_::complete_mask&&
        original->live.load(std::memory_order_acquire)==initialization_registrations_::complete_mask&&
        observer_delivery_sql_&&(observer_delivery_sql_->pending_original_for_(*this,open)||
            observer_delivery_sql_->sealed_original_for_(*this,open));
}
void database::require_initialization_registrations_(const detail::observer_delivery_open_startup& open)const {
    require_creator_();
    try {
        catalog_native_admission_ native(*this);open.require_work_current_(const_cast<database&>(*this));
        if(!initialization_registrations_current_(open))
            throw db_error("pending Native registration was retired or failed");
        // REQUIRED genuine Raw final-catalog accessor validates the actual
        // initial or replacement owner/catalog. No stub/default grant exists.
        const auto& catalog=open.final_catalog_(*this);
        for(const auto& [name,model]:catalog.models)for(const auto& property:model.properties)
            if(property.is_geo_bounds||property.is_full_text)
                throw db_error("actual selected builtin module lacks its trusted registration provider receipt");
    }catch(...) {open.fail_initialization_();throw;}
}

// Process-global statement counter (see db.hpp::total_statement_count).
static std::atomic<uint64_t> g_statement_count{0};
// Thread-local twin: exact statement budgets for single-threaded read paths,
// immune to parallel test suites sharing the process.
static thread_local uint64_t t_statement_count = 0;
thread_local bool database::core_thread_lifetime_ended_=false;
database::core_thread_holder& database::core_thread_lifetime_() noexcept {
    static thread_local core_thread_holder holder;
    return holder;
}

uint64_t database::total_statement_count() {
    return g_statement_count.load(std::memory_order_relaxed);
}

uint64_t database::thread_statement_count() {
    return t_statement_count;
}


uint64_t detail::database_native_process() noexcept {
#if defined(__APPLE__) || defined(__linux__)
    return static_cast<uint64_t>(::getpid());
#else
    return 0;
#endif
}
bool database::creator_current_() const noexcept {
    if(!detail::store_physical_process_current())return false;
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    if(wal_extent_vfs_&&!wal_extent_vfs_->current())return false;
#endif
    if(target_staging_)return target_staging_creator_current_();
    if(!ordinary_context_&&!ordinary_guarded_&&!checked_existing_connection_)return true;
    return retirement_&&retirement_->ordinary_history_creator_pid_==detail::database_native_process()&&
        detail::ordinary_creator_current(ordinary_context_);
}
bool database::catalog_quarantined_() const noexcept {
    return retirement_ && retirement_->catalog_cleanup_quarantined_.load(std::memory_order_acquire);
}
void database::require_catalog_clean_locked_() const {
    if (catalog_quarantined_()) throw db_error("original catalog reader has unsettled cleanup");
}
void database::quarantine_catalog_locked_() noexcept {
    // Called by the real catalog cleanup producer with THIS SQLite mutex held.
    // The retirement original, not a replaceable facade, owns the permanent denial.
    if (retirement_) retirement_->catalog_cleanup_quarantined_.store(true, std::memory_order_release);
}
void database::require_creator_() const {
    if(target_staging_)throw db_error("unpublished target has no ordinary database access");
    if(!creator_current_())throw std::runtime_error("managed database creator process required");
    require_catalog_clean_locked_(); // Early refusal only; actual admissions recheck below.
    detail::require_catalog_database_callback_absent(this);
}
database::catalog_native_admission_::catalog_native_admission_(const database& owner)
    : physical_scope_(owner.physical_open_original_,(owner.require_creator_(),owner.db_)),owner_(owner) {
    if(!physical_scope_.entered())throw db_error("resource Native caller original unavailable");
    owner.require_creator_();
    mutex = owner.db_ ? sqlite3_db_mutex(owner.db_) : nullptr;
    if (mutex) sqlite3_mutex_enter(mutex);
    try { owner.require_catalog_clean_locked_(); }
    catch (...) { detail::store_physical_mutex_return release(owner_,mutex); mutex = nullptr; throw; }
}
database::catalog_native_admission_::~catalog_native_admission_() noexcept {
    detail::store_physical_mutex_return release(owner_,mutex);
}
int database::prepare_catalog_checked_(const char* sql, int bytes, sqlite3_stmt** statement, const char** tail, detail::database_sql_request* request) const {
    require_creator_();
    detail::store_physical_native_scope physical_call(*this);
    if(!physical_call.entered())throw db_error("resource Native caller original unavailable");
    if (const auto* observer = detail::catalog_test_probe::current)
        if (observer->before_prepare_admission) observer->before_prepare_admission(observer->context, this);
    require_creator_(); // Arbitrary private probe may have returned in a fork child.
    // The observer above is AFTER the last early check. A normal query may
    // already be waiting here when catalog cleanup poisons this original.
    auto* mutex = db_ ? sqlite3_db_mutex(db_) : nullptr;
    if (mutex) sqlite3_mutex_enter(mutex);
    detail::store_physical_mutex_return held(*this,mutex);
    require_catalog_clean_locked_();
    detail::database_sql_request::binding requested(const_cast<database&>(*this),request);
    const int result=::lattice::detail::store_physical_prepare_v2(db_, sql, bytes, statement, tail);
    if(result==SQLITE_OK&&statement&&*statement&&request&&request->initialization_) {
        require_creator_();
        try {request->adopt(*statement);} // same actual private prepare, before pending runtime admission
        catch(...) {
            if(creator_current_()) {
                const int ended=finalize_mutation_statement_(std::exchange(*statement,nullptr));
                request->note_initialization_api_error_(ended,true);
            }
            throw;
        }
    }
    if(result==SQLITE_OK&&statement&&*statement&&observer_delivery_sql_&&
       !observer_delivery_sql_->admits(const_cast<database&>(*this),*statement)) {
        // Unsupported public SQL refuses before parameter copies or execution.
        // The closed executor prepares its own immutable generated statement
        // directly under its already-held declaration and then pins that exact
        // stmt. It never accepts a caller string as a permit.
        const int ended=finalize_mutation_statement_(std::exchange(*statement,nullptr));
        if(request)request->note_initialization_api_error_(ended,true);
        refuse_delivery_statement_();
    }
    return result;
}
int database::set_sql_authorizer_(detail::database_authorizer_slot slot,sql_authorizer_callback_ callback,void* context) noexcept {
    return detail::database_sql_dispatcher::bind(*this,slot,callback,context);
}
int database::clear_sql_authorizer_(detail::database_authorizer_slot slot,void* context) noexcept {
    return detail::database_sql_dispatcher::clear(*this,slot,context);
}
int database::install_sql_dispatcher_()noexcept {return detail::database_sql_dispatcher::install(*this);}
int database::set_sql_progress_(detail::database_handler_slot slot,int(*call)(void*)noexcept,void* context)noexcept {
    return detail::database_sql_dispatcher::set_progress(*this,slot,call,context);
}
int database::clear_sql_progress_(detail::database_handler_slot slot,void* context)noexcept {
    return detail::database_sql_dispatcher::clear_progress(*this,slot,context);
}
int database::set_sql_busy_(detail::database_handler_slot slot,int(*call)(void*,int)noexcept,void* context)noexcept {
    return detail::database_sql_dispatcher::set_busy(*this,slot,call,context);
}
int database::clear_sql_busy_(detail::database_handler_slot slot,void* context,int timeout)noexcept {
    return detail::database_sql_dispatcher::clear_busy(*this,slot,context,timeout);
}
void database::validate_ordinary_current_() const {
    require_creator_();
    if(ordinary_context_){
        detail::ordinary_before_open(path_,ordinary_context_);
        const auto actual=physical_identity("main",{},true);
        if(!actual)throw std::runtime_error("managed database physical owner unavailable");
        ordinary_context_->validate_physical(actual->device,actual->inode);
    }
}
void database::finish_checked_preparation_read_control_(std::chrono::steady_clock::time_point end) {
    // Check creator before any inherited SQLite/control mutex. This function is
    // called only after the same unpublished owner recorded its clean archive
    // COMMIT; it does not issue cleanup/retirement or a fresh operation deadline.
    require_creator_();
    detail::store_physical_native_scope physical_call(*this);
    if(!physical_call.entered())throw db_error("resource Native caller original unavailable");
    if(!checked_existing_connection_||!ordinary_context_||!db_||closed_.load(std::memory_order_acquire)||
       !read_control_||read_control_->creator_process!=detail::database_native_process()||
       read_control_->deadline!=end||read_control_->stopped())
        throw db_error("checked preparation original deadline control unavailable");
    auto* mutex=sqlite3_db_mutex(db_);
    if(!mutex||sqlite3_mutex_try(mutex)!=SQLITE_OK)
        throw db_error("checked preparation connection is still in use");
    detail::store_physical_mutex_return held(*this,mutex);
    if(sqlite3_get_autocommit(db_)!=1||engine_reads_||read_control_->stopped())
        throw db_error("checked preparation native operation is unsettled or expired");
    for(auto* statement=sqlite3_next_stmt(db_,nullptr);statement;statement=sqlite3_next_stmt(db_,statement)) {
        if(read_control_->stopped()||sqlite3_stmt_busy(statement))
            throw db_error("checked preparation retains an active statement or expired control");
    }
    // Native callbacks no longer borrow this control before its last strong
    // reference is dropped. A failed reset keeps the original owner/control and
    // is propagated through its existing first-failure registry, never retried.
    const int progress_restored=clear_sql_progress_(detail::database_handler_slot::checked_read,read_control_.get());
    const int busy_restored=clear_sql_busy_(detail::database_handler_slot::checked_read,read_control_.get(),busy_timeout_ms_);
    if(progress_restored!=SQLITE_OK||busy_restored!=SQLITE_OK)
        throw db_error("checked preparation busy policy restoration failed");
    read_control_->unpublish(db_);
    read_control_.reset();
}
bool database_read_control::stopped() noexcept {
    if (stop_code.load(std::memory_order_acquire) != 0) return true;
    if (std::chrono::steady_clock::now() < deadline) return false;
    int32_t expected = 0;
    stop_code.compare_exchange_strong(expected, static_cast<int32_t>(projection_status::deadline_exceeded));
    return true;
}
void database_read_control::stop(int32_t reason) noexcept {
    if(!detail::store_physical_process_current()||!detail::ordinary_creator_current({}) ||
       (detail::ordinary_requires_attachment_guard()&&creator_process!=detail::database_native_process()))return;
    int32_t expected = 0;
    stop_code.compare_exchange_strong(expected, reason, std::memory_order_acq_rel);
    std::lock_guard<std::mutex> lock(target_mutex);
    if (target) sqlite3_interrupt(target);
}
void database_read_control::publish(sqlite3* handle) noexcept {
    if(!detail::store_physical_process_current()||!detail::ordinary_creator_current({}) ||
       (detail::ordinary_requires_attachment_guard()&&creator_process!=detail::database_native_process()))return;
    std::lock_guard<std::mutex> lock(target_mutex);
    target = handle;
    if (stopped()) sqlite3_interrupt(target);
}
void database_read_control::unpublish(sqlite3* handle) noexcept {
    if(!detail::store_physical_process_current()||!detail::ordinary_creator_current({}) ||
       (detail::ordinary_requires_attachment_guard()&&creator_process!=detail::database_native_process()))return;
    std::lock_guard<std::mutex> lock(target_mutex);
    if (target == handle) target = nullptr;
}
void database::record_statement() {
    g_statement_count.fetch_add(1, std::memory_order_relaxed);
    ++t_statement_count;
}

detail::store_resource_name_observation database::capture_resource_name_() const {
    require_creator_();
    if(closed_.load(std::memory_order_acquire)||!db_)throw db_error("closed owner has no file-name observation");
    const auto actual=physical_identity("main",{},true);
    if(!actual)throw db_error("actual Native MAIN identity unavailable for observation");
    return detail::store_resource_name_observation::capture_(*actual,std::chrono::steady_clock::now()+std::chrono::seconds(2));
}

std::shared_ptr<const physical_store_identity> database::physical_identity(
    const std::string& schema, const std::shared_ptr<database_read_control>& control,
    bool validate_current) const {
    const char* failure = nullptr;
    return physical_identity_observed(schema, control, validate_current, failure);
}

std::shared_ptr<const physical_store_identity> database::physical_identity_observed(
    const std::string& schema, const std::shared_ptr<database_read_control>& control,
    bool validate_current, const char*& failure, physical_identity_details* details) const {
    require_creator_();
    failure = nullptr;
    if (details) *details = {};
    const auto failed = [&](const char* label) -> std::shared_ptr<const physical_store_identity> {
        failure = label; return {};
    };
    detail::store_physical_native_scope physical_call(*this);
    if(!physical_call.entered())return failed("resource_native_original_unavailable");
#if defined(__EMSCRIPTEN__) || (!defined(__APPLE__) && !defined(__linux__))
    return failed("unsupported_platform");
#else
    if (schema == "main" && !validate_current) {
        if (auto cached = std::atomic_load(&main_physical_identity_)) {
            if (details) details->last_stage = physical_identity_details::stage::cached_identity;
            return cached;
        }
    }
    if (!db_) return failed("missing_handle");
    if (details) details->last_stage = physical_identity_details::stage::connection_mutex;
    auto* mutex = sqlite3_db_mutex(db_);
    if (!mutex) return failed("missing_connection_mutex");
    const auto wait_end = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(std::max(0, busy_timeout_ms_));
    while (sqlite3_mutex_try(mutex) != SQLITE_OK) {
        if (control && control->stopped()) return failed("metadata_cancelled");
        if (!control && std::chrono::steady_clock::now() >= wait_end) return failed("metadata_busy");
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    detail::store_physical_mutex_return unlock(*this,mutex);
    if (details) details->mutex_acquired = true;
    return physical_identity_locked_observed(schema, control, failure, details);
#endif
}

std::shared_ptr<const physical_store_identity> database::physical_identity_locked(
    const std::string& schema, const std::shared_ptr<database_read_control>& control) const {
    const char* failure = nullptr;
    return physical_identity_locked_observed(schema, control, failure);
}

std::shared_ptr<const physical_store_identity> database::physical_identity_locked_observed(
    const std::string& schema, const std::shared_ptr<database_read_control>& control,
    const char*& failure, physical_identity_details* details) const {
    failure = nullptr;
    const auto failed = [&](const char* label) -> std::shared_ptr<const physical_store_identity> {
        failure = label; return {};
    };
#if defined(__EMSCRIPTEN__) || (!defined(__APPLE__) && !defined(__linux__))
    return failed("unsupported_platform");
#else
    if(!creator_current_())return failed("metadata_creator_changed");
    require_catalog_clean_locked_();
    if (control && control->stopped()) return failed("metadata_cancelled");
    if (details) details->last_stage = physical_identity_details::stage::file_name;
    const char* filename = sqlite3_db_filename(db_, schema.c_str());
    if(!creator_current_())return failed("metadata_creator_changed");
    if (!filename || !*filename) return failed("missing_file_name");
    const int immutable=sqlite3_uri_boolean(filename,"immutable",0);
    if(!creator_current_())return failed("metadata_creator_changed");
    if(immutable)return failed("immutable_file");
    sqlite3_vfs* vfs = nullptr;
    if (details) details->last_stage = physical_identity_details::stage::vfs;
    const int vfs_rc = sqlite3_file_control(db_, schema.c_str(), SQLITE_FCNTL_VFS_POINTER, &vfs);
    if (details) {
        details->vfs_result_observed = true; details->vfs_rc = vfs_rc;
        details->vfs = physical_identity_details::vfs_kind::unavailable_or_other;
    }
    if(!creator_current_())return failed("metadata_creator_changed");
    // Only the same retained native/file owner may unwrap this private VFS.
    // The original exact unix backend guard below is unchanged.
    if(wal_extent_vfs_)vfs=wal_extent_vfs_->original_vfs_for_identity(db_,schema,vfs);
    else {sqlite3_file* physical_inner=nullptr;vfs=detail::store_physical_identity_vfs(db_,schema.c_str(),vfs,physical_inner);}
    if(!creator_current_())return failed("metadata_creator_changed");
    if (vfs_rc != SQLITE_OK ||
        !vfs || !vfs->zName || (std::string(vfs->zName) != "unix" && std::string(vfs->zName) != "unix-excl")) return failed("unsupported_vfs");
    // This discriminator is read only after the original exact-name guard.
    if (details) details->vfs = vfs->zName[4] == '\0' ? physical_identity_details::vfs_kind::unix_vfs
                                                     : physical_identity_details::vfs_kind::unix_excl;
    if(!creator_current_())return failed("metadata_creator_changed");
    int moved = 1;
    if (details) details->last_stage = physical_identity_details::stage::first_move;
    const int first_move_rc = sqlite3_file_control(db_, schema.c_str(), SQLITE_FCNTL_HAS_MOVED, &moved);
    if (details) details->first_move.record(first_move_rc, first_move_rc == SQLITE_OK ? moved : 0);
    if(!creator_current_())return failed("metadata_creator_changed");
    if (first_move_rc != SQLITE_OK) return failed("move_check_unavailable");
    if (moved) return failed("file_moved");
    std::error_code error;
    if (details) details->last_stage = physical_identity_details::stage::canonical_path;
    const auto canonical = std::filesystem::canonical(filename, error);
    if(!creator_current_())return failed("metadata_creator_changed");
    if (error) return failed("canonical_path_unavailable");
    struct stat before{}, after{};
    if (details) details->last_stage = physical_identity_details::stage::initial_stat;
    if (::stat(canonical.c_str(), &before) != 0) return failed("initial_stat_unavailable");
    if (!S_ISREG(before.st_mode)) return failed("not_regular_file");
    if(!creator_current_())return failed("metadata_creator_changed");
    moved = 1;
    if (details) details->last_stage = physical_identity_details::stage::second_move;
    const int second_move_rc = sqlite3_file_control(db_, schema.c_str(), SQLITE_FCNTL_HAS_MOVED, &moved);
    if (details) details->second_move.record(second_move_rc, second_move_rc == SQLITE_OK ? moved : 0);
    if(!creator_current_())return failed("metadata_creator_changed");
    if (second_move_rc != SQLITE_OK) return failed("recheck_unavailable");
    if (moved) return failed("file_moved_during_capture");
    if (details) details->last_stage = physical_identity_details::stage::final_stat;
    if (::stat(canonical.c_str(), &after) != 0) return failed("final_stat_unavailable");
    if (details) details->last_stage = physical_identity_details::stage::identity_comparison;
    if (before.st_dev != after.st_dev || before.st_ino != after.st_ino) return failed("identity_changed_during_capture");
    if (control && control->stopped()) return failed("metadata_cancelled");
    auto identity = std::make_shared<physical_store_identity>();
    if(!creator_current_())return failed("metadata_creator_changed");
    identity->device = static_cast<uint64_t>(after.st_dev);
    identity->inode = static_cast<uint64_t>(after.st_ino);
    identity->filename = canonical.string();
    if(!creator_current_())return failed("metadata_creator_changed");
    if (schema == "main") {
        if (details) details->last_stage = physical_identity_details::stage::cache_comparison;
        auto cached = std::atomic_load(&main_physical_identity_);
        if (cached && !(*cached == *identity)) return failed("cached_identity_changed");
        if (!cached) std::atomic_store(&main_physical_identity_, std::shared_ptr<const physical_store_identity>(identity));
    }
    if (details) details->last_stage = physical_identity_details::stage::complete;
    return identity;
#endif
}


database::physical_identity_attempt database::try_current_physical_identity(const std::string& schema) const {
    require_creator_();
    physical_identity_attempt result;
    detail::store_physical_native_scope physical_call(*this);
    if(!physical_call.entered()){result.failure="resource_native_original_unavailable";return result;}
#if defined(__EMSCRIPTEN__) || (!defined(__APPLE__) && !defined(__linux__))
    result.failure = "unsupported_platform";
#else
    if (!db_) { result.failure = "missing_handle"; return result; }
    auto* mutex = sqlite3_db_mutex(db_);
    if (!mutex) { result.failure = "missing_connection_mutex"; return result; }
    const int rc = sqlite3_mutex_try(mutex);
    if (rc != SQLITE_OK) {
        if (rc == SQLITE_BUSY) result.state = physical_identity_attempt::status::mutex_busy;
        else result.failure = "metadata_mutex_unavailable";
        return result;
    }
    detail::store_physical_mutex_return unlock(*this,mutex);
    result.identity = physical_identity_locked_observed(schema, {}, result.failure);
    if (result.identity) result.state = physical_identity_attempt::status::captured;
#endif
    return result;
}

void database::attachment_operation::check() const {
    if (clock::now() >= deadline)
        throw db_error("topology admission deadline exceeded");
}

void database::attachment_operation::wait(std::chrono::milliseconds interval) const {
    check();
    const auto remaining = deadline - clock::now();
    if (remaining > clock::duration::zero())
        std::this_thread::sleep_for(std::min(remaining,
            std::chrono::duration_cast<clock::duration>(interval)));
    check();
}

std::unique_lock<std::mutex> database::attachment_operation::lock(std::mutex& mutex) const {
    std::unique_lock<std::mutex> held(mutex, std::defer_lock);
    for (;;) {
        check();
        if (held.try_lock()) { check(); return held; }
        wait();
    }
}

// No admission mutex/callback/heap waiter is introduced. Each of the fixed
// cells belongs to one real retry scope and is erased on first admission or
// first unwind. A writer observes one cell, not a stream of later arrivals.
void database::topology_wait_scope::begin(database& owner, attachment_operation& operation) {
    begin_until(owner,operation.deadline); // Preserve original creator-before-deadline ordering.
}
void database::topology_wait_scope::begin_until(database& owner,attachment_operation::clock::time_point end) {
    original_ = owner.retirement_;
    if (!original_ || original_->ordinary_history_creator_pid_ != detail::database_native_process()) return;
    const auto check=[&]{if(attachment_operation::clock::now()>=end)throw db_error("topology admission deadline exceeded");};
    check();
    const auto deadline = end.time_since_epoch().count();
    if (deadline == detail::database_retirement_state::no_topology_waiter_)
        throw db_error("topology admission deadline is not representable");
    for (size_t index = 0; index != original_->topology_wait_cells_.size(); ++index) {
        uint64_t empty = 0;
        auto& cell = original_->topology_wait_cells_[index];
        if (!cell.token.compare_exchange_strong(empty, UINT64_MAX,
                std::memory_order_acq_rel, std::memory_order_relaxed)) continue;
        slot_ = index; // Member unwind owns even an unissued reserved cell.
        auto serial = original_->topology_wait_serial_.load(std::memory_order_relaxed);
        for (;;) {
            check();
            if (serial >= UINT64_MAX - 1)
                throw db_error("topology admission waiter identity exhausted");
            if (original_->topology_wait_serial_.compare_exchange_weak(serial, serial + 1,
                    std::memory_order_relaxed, std::memory_order_relaxed)) break;
        }
        cell.deadline.store(deadline, std::memory_order_relaxed);
        cell.token.store(serial + 1, std::memory_order_release);
        original_->topology_waiter_count_.fetch_add(1, std::memory_order_release);
        token_ = serial + 1;
        published_ = true;
        return;
    }
    check();
    // Bounded overload refusal, before this SQLite admission or a new effect.
    throw db_error("topology admission waiter capacity exceeded");
}
void database::topology_wait_scope::tighten_until(attachment_operation::clock::time_point end) noexcept {
    if(!published_||!original_||original_->ordinary_history_creator_pid_!=detail::database_native_process())return;
    auto& cell=original_->topology_wait_cells_[slot_];
    const auto prior=cell.deadline.load(std::memory_order_acquire);
    cell.deadline.store(std::min(prior,end.time_since_epoch().count()),std::memory_order_release);
}
thread_local database::topology_wait_scope::bypass* database::topology_wait_scope::bypass::current_=nullptr;
bool database::topology_wait_scope::bypass::excludes(const detail::database_retirement_state* original,size_t slot,uint64_t token) noexcept {
    for(auto* entry=current_;entry;entry=entry->previous_)
        if(entry->token_!=0&&entry->original_.get()==original&&entry->slot_==slot&&entry->token_==token)return true;
    return false;
}
void database::topology_wait_scope::release() noexcept {
    if (slot_ == static_cast<size_t>(-1)) return;
    if (original_->ordinary_history_creator_pid_ == detail::database_native_process()) {
        // Remove the hint before freeing the cell, so a delayed old unregister
        // cannot inflate the count after another operation reuses its cell.
        if (published_) original_->topology_waiter_count_.fetch_sub(1, std::memory_order_release);
        original_->topology_wait_cells_[slot_].token.store(0, std::memory_order_release);
    }
    slot_ = static_cast<size_t>(-1);
    published_ = false;token_=0;
}
size_t database::topology_waiter_count_() const noexcept {
    if (!retirement_ || retirement_->ordinary_history_creator_pid_ != detail::database_native_process()) return 0;
    return retirement_->topology_waiter_count_.load(std::memory_order_acquire);
}
void database::wait_for_topology_turn_(std::optional<std::chrono::steady_clock::time_point> admission_end) const {
    auto* const observation=detail::recovery_topology_wait_observation::current;
    if(observation)++observation->calls;
    const auto original = retirement_; // Exact custody during the bounded wait.
    if (!original || original->ordinary_history_creator_pid_ != detail::database_native_process() ||
        original->topology_waiter_count_.load(std::memory_order_acquire) == 0) return;
    using clock = attachment_operation::clock;
    const auto selection_now = clock::now().time_since_epoch().count();
    auto selected = original->topology_wait_cells_.size();
    auto deadline = detail::database_retirement_state::no_topology_waiter_;
    uint64_t selected_token = 0;
    bool selected_excluded = false;
    for (size_t index = 0; index != original->topology_wait_cells_.size(); ++index) {
        const auto& cell = original->topology_wait_cells_[index];
        const auto token = cell.token.load(std::memory_order_acquire);
        if (token == 0 || token == UINT64_MAX) continue;
        const bool excluded=topology_wait_scope::bypass::excludes(original.get(),index,token);
        if (excluded) continue;
        const auto candidate = cell.deadline.load(std::memory_order_acquire);
        if (cell.token.load(std::memory_order_acquire) != token || candidate <= selection_now) continue;
        if (selected == original->topology_wait_cells_.size() || candidate < deadline) {
            selected = index; deadline = candidate; selected_token = token;selected_excluded=excluded;
        }
    }
    if (selected == original->topology_wait_cells_.size()) return;
    if(observation){
        ++observation->selected_waits;
        observation->wrapper=reinterpret_cast<std::uintptr_t>(this);
        observation->original=reinterpret_cast<std::uintptr_t>(original.get());
        observation->slot=selected;observation->token=selected_token;
        observation->selection_now=selection_now;observation->deadline=deadline;
        observation->excluded=selected_excluded;observation->selected=true;
        observation->deadline_observed=false;observation->deadline_exit=0;
        observation->returned=false;observation->sleeps=0;
    }
    // Optional passive test handoff after the genuine selection copy. This
    // function holds no topology/Native metadata leaf here. A child cannot
    // dereference a parent's inherited signal, even if the scope was copied.
    if(detail::recovery_topology_wait_observation::signal_creator==detail::database_native_process())
        if(auto* signal=detail::recovery_topology_wait_observation::selected_signal)
            signal->store(true,std::memory_order_release);
    const auto selected_end = clock::time_point(clock::duration(deadline));
    // A managed mutation carries ONE original admission deadline through
    // this pre-existing optional topology handoff as well. Ordinary SQL
    // callers pass no cap and keep the exact selected-waiter behavior.
    const auto end = admission_end?std::min(selected_end,*admission_end):selected_end;
    while (!closed_.load(std::memory_order_acquire) &&
           original->topology_wait_cells_[selected].token.load(std::memory_order_acquire) == selected_token) {
        const auto now = clock::now();
        if (now >= end) { // Same original waiter deadline, never refreshed.
            if(observation){observation->deadline_observed=true;observation->deadline_exit=now.time_since_epoch().count();}
            break;
        }
        if(observation)++observation->sleeps;
        std::this_thread::sleep_for(std::min(end - now,
            std::chrono::duration_cast<clock::duration>(std::chrono::milliseconds(1))));
    }
    if(observation)observation->returned=true;
}
thread_local database::sql_entry_scope* database::sql_entry_scope::current_ = nullptr;
bool database::sql_entry_scope::active_for(const database* owner) noexcept {
    for (auto* scope = current_; scope; scope = scope->previous_)
        if (scope->owner_ == owner) return true;
    return false;
}
database::sql_entry_scope::sql_entry_scope(const database& owner)
    : sql_entry_scope(owner, {}) {}
database::sql_entry_scope::sql_entry_scope(const database& owner,
    const std::shared_ptr<const core_transaction_original>& continuation)
    : sql_entry_scope(owner,continuation,{}) {}
database::sql_entry_scope::sql_entry_scope(const database& owner,
    const std::shared_ptr<const core_transaction_original>& continuation,
    std::optional<std::chrono::steady_clock::time_point> original_admission_end)
    : physical_scope_(owner.physical_open_original_,(owner.require_creator_(),owner.db_)), owner_(&owner), previous_(current_) {
    if(!physical_scope_.entered())throw db_error("resource Native caller original unavailable");
    owner.require_creator_();
    // Raw native callers can own SQLite recursively without a wrapper frame.
    // Their existing escape remains legacy behavior, not a guessed free mutex.
    if (owner.topology_waiter_count_() != 0 &&
        !(continuation && continuation->current_for(owner)) &&
        !owner.raw_handle_escaped_.load(std::memory_order_acquire) &&
        !active_for(&owner) && !attachment_sql_scope::operation_for(&owner) &&
        !engine_query_scope::active_for(owner) && !update_hook_scope::active_for(owner.db_) &&
        !maintenance_scope::active_for(owner.db_) && !detail::managed_route_scope::active_for(&owner))
        owner.wait_for_topology_turn_(original_admission_end);
    current_ = this;
}
void database::sql_entry_scope::release() noexcept {
    if (!owner_) return;
    current_ = previous_;
    owner_ = nullptr;
}

thread_local database::attachment_sql_scope* database::attachment_sql_scope::current_ = nullptr;
database::attachment_sql_scope::attachment_sql_scope(database& owner, attachment_operation& operation)
    : physical_scope_(owner.physical_open_original_,(owner.require_creator_(),owner.db_)), owner_(owner), operation_(operation) {
    if(!physical_scope_.entered())throw db_error("resource Native caller original unavailable");
    owner_.require_creator_();
    operation_.check();
    if (!owner_.db_ || owner_.closed_.load(std::memory_order_acquire))
        throw db_error("topology admission: database closed");
    // Reading the mutex pointer does not enter SQLite. The operation's strong
    // wrapper custody keeps the handle alive even during logical close.
    auto* mutex = sqlite3_db_mutex(owner_.db_);
    if (mutex) waiting_.begin(owner_, operation_);
    for (;;) {
        operation_.check();
        if (!mutex || sqlite3_mutex_try(mutex) == SQLITE_OK) break;
        operation_.wait();
    }
    try {
        operation_.check();
        owner_.require_catalog_clean_locked_();
        if (owner_.closed_.load(std::memory_order_acquire))
            throw db_error("topology admission: database closed");
    } catch (...) {
        if (mutex) sqlite3_mutex_leave(mutex);
        throw;
    }
    waiting_.release(); // Actual Native admission returned; fresh SQL may resume.
    mutex_ = mutex;
    previous_ = current_;
    current_ = this;
}
database::attachment_sql_scope::~attachment_sql_scope() {
    if(!owner_.creator_current_()){if(current_==this)current_=previous_;return;}
    current_ = previous_;
    if (mutex_) sqlite3_mutex_leave(mutex_);
}
database::attachment_operation* database::attachment_sql_scope::operation_for(const database* owner) noexcept {
    for (auto* scope = current_; scope; scope = scope->previous_)
        if (&scope->owner_ == owner) return &scope->operation_;
    return nullptr;
}

void database::attachment_operation::capture_settlement_locked(database& owner, bool failed) {
    // Resolving an engine-owned committed snapshot performs read-only queries
    // on this already held writer. Their wrapper tails do not create a second
    // success claim or defensively discard the original committed buffer.
    if (capturing_owner == &owner) return;
    // The successful tail keeps every existing outer-operation deferral rule.
    if (!failed && (update_hook_scope::active_for(owner.db_) ||
        maintenance_scope::active_for(owner.db_) || detail::managed_route_scope::active_for(&owner) ||
        engine_query_scope::active_for(owner))) return;
    if (sqlite3_get_autocommit(owner.db_) == 0) return;
    const auto hooks = owner.txn_hooks_;
    if (!hooks || !(failed ? static_cast<bool>(hooks->rolled_back)
                         : (hooks->settled && owner.txn_dirty_.load(std::memory_order_relaxed)))) {
        if (failed) owner.txn_dirty_.store(false, std::memory_order_relaxed);
        return;
    }
    // Allocate and retain the exact bundle before claiming anything. Keep even
    // an inactive entry through off-lock finish: no displaced public callable
    // can be finally destroyed beneath this SQLite/topology scope.
    deliveries.push_back({hooks, failed, false, {}});
    const auto index = deliveries.size() - 1;
    if (hooks->capture_owned) {
        struct capture_scope {
            attachment_operation& operation; database* previous;
            ~capture_scope() { operation.capturing_owner = previous; }
        } scope{*this, capturing_owner};
        capturing_owner = &owner;
        auto captured = hooks->capture_owned(owner, failed);
        // An existing active flush/reserved recovery batch retains its own
        // buffer and dirty flag. No empty substitute claims that obligation.
        if (!captured) return;
        deliveries[index].captured = std::move(captured);
    }
    deliveries[index].active = true;
    owner.txn_dirty_.store(false, std::memory_order_relaxed);
}
void database::attachment_operation::settle_locked(database& owner, bool failed, std::exception_ptr primary) {
    try { capture_settlement_locked(owner, failed); }
    catch (...) {
        if (primary) throw attachment_delivery_error(primary, std::current_exception(), 1);
        throw;
    }
    if (primary) std::rethrow_exception(primary);
}
void database::attachment_operation::finish(std::exception_ptr primary) {
    std::exception_ptr first_delivery_error;
    size_t delivery_failures = 0;
    for (const auto& item : deliveries) {
        try {
            if (!item.active) continue;
            if (item.captured) item.captured();
            else if (item.rollback) item.hooks->rolled_back();
            else item.hooks->settled();
        } catch (...) {
            if (!first_delivery_error) first_delivery_error = std::current_exception();
            ++delivery_failures;
        }
    }
    if (primary && first_delivery_error)
        throw attachment_delivery_error(std::move(primary), std::move(first_delivery_error), delivery_failures);
    if (primary) std::rethrow_exception(primary);
    if (first_delivery_error) std::rethrow_exception(first_delivery_error);
}

std::shared_ptr<const physical_store_identity> database::attachment_physical_identity(attachment_operation& operation) {
    attachment_sql_scope admitted(*this, operation);
    // An unsupported physical identity remains a legacy null identity; timeout
    // and logical close are separate errors and must not be swallowed. Legacy
    // single-thread/no-mutex SQL may run, but never gains projected provenance.
    if (!sqlite3_db_mutex(db_)) return {};
    return physical_identity_locked("main", {});
}

int database::resource_attachment_limit(attachment_operation& operation) {
    attachment_sql_scope admitted(*this,operation);
    return sqlite3_limit(db_,SQLITE_LIMIT_ATTACHED,-1); // actual private read-only metadata, same bounded Native admission
}

void database::execute_attachment_sql(const std::string& sql, attachment_operation& operation) {
    int rc = SQLITE_OK, extended_code = 0, system_errno = 0;
    std::string error;
    {
        attachment_sql_scope admitted(*this, operation);
        if (channel_reset_unsettled_.load(std::memory_order_acquire))
            throw db_error("channel reset unsettled; explicit rollback required");
        record_statement();
        rc = execute_observed_sql_(sql.c_str());
        if (rc != SQLITE_OK) {
            extended_code = sqlite3_extended_errcode(db_);
            system_errno = sqlite3_system_errno(db_);
            const auto* message=sqlite3_errmsg(db_);error = message ? message : "Unknown error";
        }
        // This tail uses the mutex already admitted above. If retaining the
        // tail fails, keep the original SQL failure as well as that failure.
        try { operation.capture_settlement_locked(*this, rc != SQLITE_OK); }
        catch (...) {
            if (rc != SQLITE_OK) throw attachment_delivery_error(
                std::make_exception_ptr(db_error("SQL execution failed: " + error + " (SQL: " + sql + ")")),
                std::current_exception(), 1);
            throw;
        }
    }
    if (rc != SQLITE_OK) {
        LOG_ERROR("db", "SQL execution failed: %s (SQL: %s) [rc=%d extended_code=%d system_errno=%d sqlite_version=%s sqlite_source_id=%s]",
            error.c_str(), sql.c_str(), rc, extended_code, system_errno, sqlite3_libversion(), sqlite3_sourceid());
        throw db_error("SQL execution failed: " + error + " (SQL: " + sql + ")");
    }
}

std::shared_ptr<const physical_store_identity> database::attach_and_capture_identity(
    const std::string& attach_sql, const std::string& schema, attachment_operation& operation) {
    attachment_sql_scope admitted(*this, operation);
    struct capture_context {
        database* owner;
        attachment_operation* operation;
        const std::string* schema;
        std::shared_ptr<const physical_store_identity> identity;
        std::exception_ptr error;
    } context{this, &operation, &schema, {}, {}};
    // ATTACH produces no rows. This single constant row provides an internal
    // capture point inside the same sqlite3_exec as the successful ATTACH.
    const std::string sql = attach_sql + "; SELECT 1";
    record_statement(); // ATTACH attempt; the executed SELECT is counted below.
    char* message = nullptr;
    const int rc = ::lattice::detail::store_physical_exec(db_, sql.c_str(),
        [](void* raw, int columns, char**, char**) noexcept -> int {
            auto& capture = *static_cast<capture_context*>(raw);
            try {
                // Also tolerate legacy PRAGMA empty_result_callbacks: the
                // rowless ATTACH itself must never act as the capture point.
                if (columns != 1) return 0;
                capture.operation->check();
                database::record_statement();
                auto* mutex = sqlite3_db_mutex(capture.owner->db_);
                // FULLMUTEX connections have a recursive mutex. Verify usable
                // ownership without polling; unsupported configurations retain
                // legacy attachment but cannot authorize projected provenance.
                if (!mutex || sqlite3_mutex_try(mutex) != SQLITE_OK) return 0;
                struct unlock {
                    sqlite3_mutex* mutex;
                    ~unlock() { sqlite3_mutex_leave(mutex); }
                } release{mutex};
                capture.identity = capture.owner->physical_identity_locked(*capture.schema, {});
                return 0;
            } catch (...) {
                capture.error = std::current_exception();
                return 1; // SQLite finalizes before C++ propagates this error.
            }
        }, &context, &message);
    const std::unique_ptr<char, decltype(&sqlite3_free)> free_message(message, &sqlite3_free);
    if (rc != SQLITE_OK) {
        auto primary = context.error ? context.error : std::make_exception_ptr(
            db_error("SQL execution failed: " + std::string(message ? message : "Unknown error") +
                     " (SQL: " + attach_sql + ")"));
        operation.settle_locked(*this, true, std::move(primary));
    }
    operation.capture_settlement_locked(*this, false);
    return context.identity;
}

std::vector<std::string> database::query_attachment_text_metadata(
    const std::string& sql, const std::string& column, attachment_operation& operation) {
    attachment_sql_scope admitted(*this, operation);
    struct metadata_context {
        database* owner;
        attachment_operation* operation;
        const std::string* sql;
        const std::string* column;
        std::vector<std::string> values;
        std::exception_ptr error;
        bool entered = false;
        bool statement_failed = false;
    } context{this, &operation, &sql, &column, {}, {}};
    // Do not use pragma_* table-valued functions: ordinary tables can shadow
    // those names. The nested original PRAGMA/SELECT also preserves real SQLite
    // types, which sqlite3_exec's string-only result callback would erase.
    record_statement(); // Constant rendezvous statement.
    char* message = nullptr;
    const int rc = ::lattice::detail::store_physical_exec(db_, "SELECT 1",
        [](void* raw, int columns, char** values, char**) noexcept -> int {
            auto& capture = *static_cast<metadata_context*>(raw);
            try {
                if (!values) return 0; // Legacy empty_result_callbacks.
                if (columns != 1 || capture.entered)
                    throw db_error("Unexpected attachment metadata rendezvous shape");
                capture.entered = true;
                capture.operation->check();
                auto* mutex = sqlite3_db_mutex(capture.owner->db_);
                if (mutex && sqlite3_mutex_try(mutex) != SQLITE_OK)
                    throw db_error("Attachment metadata execution scope unavailable");
                struct unlock {
                    sqlite3_mutex* mutex;
                    ~unlock() { if (mutex) sqlite3_mutex_leave(mutex); }
                } release{mutex};
                // NULL means the connection has no SQLite mutex (e.g. a
                // single-threaded WASM build); preserve its existing mode.
                database::record_statement(); // Actual metadata attempt.
                sqlite3_stmt* raw_statement = nullptr;
                const int prepare_rc = ::lattice::detail::store_physical_prepare_v2(capture.owner->db_,
                    capture.sql->c_str(), -1, &raw_statement, nullptr);
                const std::unique_ptr<sqlite3_stmt, decltype(&finalize_mutation_statement_)>
                    statement(raw_statement, &finalize_mutation_statement_);
                if (prepare_rc != SQLITE_OK)
                    throw db_error("Failed to prepare attachment metadata: " +
                        std::string(sqlite3_errmsg(capture.owner->db_)));
                if (!statement || !sqlite3_stmt_readonly(statement.get()))
                    throw db_error("Attachment metadata requires a read-only statement");
                int selected = -1;
                const int count = sqlite3_column_count(statement.get());
                for (int index = 0; index < count; ++index) {
                    const char* name = sqlite3_column_name(statement.get(), index);
                    if (!name) throw db_error("Attachment metadata column name allocation failed");
                    if (*capture.column == name) selected = index;
                }
                int step_rc = SQLITE_OK;
                for (;;) {
                    capture.operation->check();
                    step_rc = ::lattice::detail::store_physical_step(statement.get());
                    if (step_rc != SQLITE_ROW) break;
                    if (selected < 0 || sqlite3_column_type(statement.get(), selected) != SQLITE_TEXT)
                        continue; // Existing callers ignore missing/non-TEXT name values.
                    const auto* value = sqlite3_column_text(statement.get(), selected);
                    if (!value) throw db_error("Attachment metadata text allocation failed");
                    // Match extract_column's current C-string semantics. This
                    // is metadata only, not a new general binary/text codec.
                    capture.values.emplace_back(reinterpret_cast<const char*>(value));
                }
                if (step_rc != SQLITE_DONE) {
                    capture.statement_failed = true;
                    throw db_error("Attachment metadata query failed: " +
                        std::string(sqlite3_errmsg(capture.owner->db_)));
                }
                return 0; // Statement finalizes before releasing the recursive scope.
            } catch (...) {
                capture.error = std::current_exception();
                return 1;
            }
        }, &context, &message);
    const std::unique_ptr<char, decltype(&sqlite3_free)> free_message(message, &sqlite3_free);
    if (rc != SQLITE_OK || context.error) {
        auto primary = context.error ? context.error : std::make_exception_ptr(
            db_error("Attachment metadata execution failed: " + std::string(message ? message : "Unknown error")));
        if (context.statement_failed) operation.settle_locked(*this, true, primary);
        std::rethrow_exception(primary);
    }
    if (!context.entered) throw db_error("Attachment metadata rendezvous did not execute");
    operation.capture_settlement_locked(*this, false);
    return std::move(context.values);
}

database::database(const std::string& path, open_mode mode, int busy_timeout_ms,
                   std::shared_ptr<database_read_control> read_control, detail::ordinary_context context)
    : database(path, mode, busy_timeout_ms, std::move(read_control), initialization_key(false), std::move(context)) {}

std::shared_ptr<database> database::make_read_keeper(const std::string& path,
                                                   int busy_timeout_ms, detail::ordinary_context context,
                                                   std::shared_ptr<detail::database_open_ledger> ledger) {
    if(ledger)return make_owned_connection(path,open_mode::read_only,busy_timeout_ms,{},std::move(context),std::move(ledger),true);
    return std::make_shared<database>(path, open_mode::read_only, busy_timeout_ms,
                                     std::shared_ptr<database_read_control>{}, initialization_key(true), std::move(context));
}


namespace {
struct database_open_builder_tail {
    std::shared_ptr<detail::database_open_ledger> ledger;
    size_t slot; uint64_t serial; bool object_created=false;
    ~database_open_builder_tail(){
        if(!ledger->in_creator_process())return;
        if(!object_created)ledger->object_returned(slot,serial);
        ledger->builder_returned(slot,serial);
    }
};
}
void database::owned_connection_deleter::operator()(database* value) const noexcept {
    if(!ledger){delete value;return;}
    if(!ledger->in_creator_process())return; // Quarantine before inherited destructor/leaf.
    delete value; // Entire actual wrapper/native/callback/member destruction returned.
    if(!ledger->in_creator_process())return; // Arbitrary destruction may have forked.
    ledger->object_returned(slot,serial);
}
std::shared_ptr<database> database::make_owned_connection(const std::string& path,open_mode mode,int busy,
    std::shared_ptr<database_read_control> control,detail::ordinary_context context,
    std::shared_ptr<detail::database_open_ledger> ledger,bool keeper,std::optional<sqlite3_int64> wal_extent_limit,
    const std::shared_ptr<detail::observer_delivery_open_startup>& startup,
    detail::store_resource_initialization_original* normal_creation) {
    if(startup&&!startup->creator_current_())throw db_error("inherited delivery open before key capture");
    if(!ledger) {
        initialization_key key(keeper);key.delivery_startup_=&startup;key.delivery_startup_creator_=detail::database_native_process();
        key.normal_creation_=normal_creation;key.normal_creation_creator_=detail::database_native_process();
        return std::make_shared<database>(path,mode,busy,std::move(control),key,std::move(context),wal_extent_limit);
    }
    const auto admitted=ledger->reserve_open();
    database_open_builder_tail tail{ledger,admitted.slot,admitted.serial};
    auto fact=std::make_shared<detail::database_retirement_state>();
    ledger->bind_retirement(admitted.slot,admitted.serial,fact);
    initialization_key key(keeper,fact,ledger,admitted.slot,admitted.serial);
    key.delivery_startup_=&startup;key.delivery_startup_creator_=detail::database_native_process();
    key.normal_creation_=normal_creation;key.normal_creation_creator_=detail::database_native_process();
    auto* actual=new database(path,mode,busy,std::move(control),key,std::move(context),wal_extent_limit);
    tail.object_created=true;
    // Control-block failure calls this exact deleter; builder tail settles once.
    return std::shared_ptr<database>(actual,owned_connection_deleter{ledger,admitted.slot,admitted.serial});
}
database::private_connection database::make_private_connection(const std::string& path,open_mode mode,int busy,
    std::shared_ptr<database_read_control> control,detail::ordinary_context context,
    std::shared_ptr<detail::database_open_ledger> ledger) {
    if(!ledger)return private_connection(new database(path,mode,busy,std::move(control),std::move(context)),owned_connection_deleter{});
    const auto admitted=ledger->reserve_open();
    database_open_builder_tail tail{ledger,admitted.slot,admitted.serial};
    auto fact=std::make_shared<detail::database_retirement_state>();
    ledger->bind_retirement(admitted.slot,admitted.serial,fact);
    initialization_key key(false,fact,ledger,admitted.slot,admitted.serial);
    auto* actual=new database(path,mode,busy,std::move(control),key,std::move(context));
    tail.object_created=true;
    return private_connection(actual,owned_connection_deleter{ledger,admitted.slot,admitted.serial});
}

database::database(const std::string& requested_path, open_mode mode, int busy_timeout_ms,
                   std::shared_ptr<database_read_control> read_control, initialization_key key, detail::ordinary_context context,
                   std::optional<sqlite3_int64> wal_extent_limit)
    : administrative_connection_(key.administrative_), checked_existing_connection_(key.checked_existing_),
      retirement_(key.checked_retirement_?key.checked_retirement_:std::make_shared<detail::database_retirement_state>()),
      ordinary_context_(std::move(context)),
      path_(requested_path), mode_(mode), busy_timeout_ms_(busy_timeout_ms), read_control_(std::move(read_control)) {
    detail::ordinary_require_creator(ordinary_context_);
    if(key.delivery_startup_) {
        if(key.delivery_startup_creator_!=detail::database_native_process())
            throw db_error("inherited delivery constructor key before token access");
        if(*key.delivery_startup_&&!(*key.delivery_startup_)->creator_current_())
            throw db_error("inherited delivery constructor original");
        delivery_startup_.bind_(*key.delivery_startup_);
        require_creator_();
    }
    if(key.normal_creation_&&key.normal_creation_creator_!=detail::database_native_process())
        throw db_error("inherited normal factory key before original access");
    if(key.normal_creation_&&(!key.normal_creation_->transport_current_()||
       key.continuous_||key.administrative_||key.checked_existing_))
        throw db_error("normal factory original requires its exact ordinary writer builder");
    // One entry deadline covers resource selection and this actual open. The
    // independently enrolled VFS original outlives this constructor admission.
    const auto resource_open_end=delivery_startup_?delivery_startup_->original_deadline_():
        key.normal_creation_?key.normal_creation_->original_deadline_():std::chrono::steady_clock::now()+std::chrono::seconds(30);
    if(key.normal_creation_&&resource_open_end>key.normal_creation_->original_deadline_())
        throw db_error("normal writer renewed its actual factory deadline");
    const std::string path=detail::store_resource_factory_path(requested_path,resource_open_end);
    if(delivery_startup_)delivery_startup_->claim_open_(path); // one actual open, before all Native effects
    path_=path;
    detail::ordinary_before_open(path,ordinary_context_);
    ordinary_guarded_=detail::ordinary_requires_attachment_guard();
    suppress_destructor_optimize_=key.administrative_||key.checked_existing_;
    if(key.checked_existing_&&((key.checked_mounted_server_?
       static_cast<bool>(ordinary_context_):(!ordinary_context_||ordinary_context_!=key.checked_context_))||
       !key.checked_identity_||!retirement_||mode_!=open_mode::read_write))
        throw db_error("checked existing open requires its actual retained native owner");
    if(mode==open_mode::read_write && !key.continuous_)detail::require_continuous_path_unowned(path);
    // Determine SQLite open flags based on mode
    int flags = SQLITE_OPEN_FULLMUTEX;  // Always use serialized threading mode
    if(ordinary_context_){
#ifdef SQLITE_OPEN_NOFOLLOW
        flags |= SQLITE_OPEN_NOFOLLOW;
#else
        throw std::runtime_error("managed physical open requires SQLite no-follow support");
#endif
    }
    int rc;
    // Every actual managed file writer, including direct/public and checked
    // existing opens, selects from the retained context before VFS/SQLite effects.
    // Readonly opens retain their original route and reject explicit WAL policy
    // through the existing writable-only check below.
    if(mode==open_mode::read_write)
        wal_extent_limit=detail::ordinary_select_wal_extent_limit(path,ordinary_context_,wal_extent_limit);
    if(wal_extent_limit) {
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__)) && defined(SQLITE_OPEN_NOFOLLOW)
        if(mode!=open_mode::read_write)
            throw db_error("bounded WAL unsupported writer route");
        wal_extent_vfs_=detail::wal_extent_vfs::create(path,*wal_extent_limit);
#else
        throw db_error("bounded WAL native no-follow VFS unavailable");
#endif
    }

    if (mode == open_mode::read_only) {
        // WAL-aware read-only reader: a plain SQLITE_OPEN_READONLY connection
        // joins a concurrent writer's WAL and sees committed-but-not-yet-
        // checkpointed rows. Never immutable=1 — that ignores the -wal entirely,
        // which is wrong for any live, WAL-backed Lattice database. Modern SQLite
        // (>=3.22) reads a read-only WAL database even on read-only media by
        // falling back to a heap-memory wal-index, so this also covers bundled DBs.
        flags |= SQLITE_OPEN_READONLY;
        // URI handling is unconditional: SQLite only URI-parses names that
        // START with "file:" even when the flag is set, so plain paths are
        // unaffected — and ATTACH through this connection can then resolve
        // named-memory URIs ("file:<name>?mode=memory&cache=shared") instead
        // of creating a literal file of that name.
        flags |= SQLITE_OPEN_URI;
        const char* observed_vfs=nullptr;
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
        if(!key.administrative_&&!key.continuous_&&!key.keeper_cache_&&!read_control_)
            observed_vfs=detail::database_open_test_hooks::consume(path.c_str(),flags);
#endif
        if(key.open_ledger_)key.open_ledger_->native_open_entered(key.open_slot_,key.open_serial_);
        if(key.normal_creation_) {
            try {
                rc=detail::store_physical_api::open_initializing_(path.c_str(),&db_,flags,observed_vfs,resource_open_end,
                    physical_open_original_,normal_initialization_original_,key.normal_creation_);
            }catch(...) {retire_connection_(false);throw;}
        }else rc = detail::store_physical_open_v2_owned_at(path.c_str(), &db_, flags, observed_vfs,resource_open_end,physical_open_original_);
    } else {
        flags |= SQLITE_OPEN_READWRITE;
        if(key.administrative_||key.checked_existing_) {
#ifdef SQLITE_OPEN_NOFOLLOW
            flags |= SQLITE_OPEN_NOFOLLOW;
#else
            throw db_error("receipt administration requires no-follow SQLite open");
#endif
        } else if(ordinary_context_) flags |= SQLITE_OPEN_URI; // enrolled existing file, never CREATE
        else flags |= SQLITE_OPEN_CREATE | SQLITE_OPEN_URI; // ordinary open unchanged
        if(key.open_ledger_)key.open_ledger_->native_open_entered(key.open_slot_,key.open_serial_);
        const char* selected_vfs=nullptr;
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
        if(wal_extent_vfs_)selected_vfs=wal_extent_vfs_->name();
#endif
        if(key.normal_creation_||(delivery_startup_&&!key.continuous_&&!key.administrative_&&!key.checked_existing_)) {
            try {
                rc=detail::store_physical_api::open_initializing_(path.c_str(),&db_,flags,selected_vfs,resource_open_end,
                    physical_open_original_,normal_initialization_original_,key.normal_creation_);
            }catch(...) {retire_connection_(false);throw;} // retain actual first exception; no Native retry
        }else rc = detail::store_physical_open_v2_owned_at(path.c_str(), &db_, flags, selected_vfs,resource_open_end,physical_open_original_);
    }
    // THIS producer transported the preadmitted original through the actual
    // Native return, including partial/error handles. No registry lookup can
    // relabel a recycled address or turn failed publication into legacy.
    if(db_&&!physical_open_original_.matches_open_(db_)){
        retire_connection_(false);throw db_error("physical open original capture unavailable");
    }
    // Observe the actual first native return before any constructor SQL.
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    if(wal_extent_vfs_&&!wal_extent_vfs_->native_open_returned(db_,rc)&&rc==SQLITE_OK)rc=SQLITE_CANTOPEN;
#endif
    if(key.open_ledger_&&!db_)retirement_->record_close(SQLITE_OK,detail::database_retirement_state::not_attempted); // Actual open returned no native handle.
    if (rc != SQLITE_OK) {
        try {
            detail::store_physical_cleanup_scope failed_open(*this);
            if(!failed_open.entered())throw db_error("failed physical open cleanup original unavailable");
            std::string error = sqlite3_errmsg(db_);
            retire_connection_(false);
            LOG_ERROR("db", "Failed to open database: %s", error.c_str());
            throw db_error("Failed to open database: " + error);
        } catch (...) {
            // Preserve cleanup even if constructing the primary error fails.
            retire_connection_(false);
            throw;
        }
    }

    try {
    if(delivery_startup_)delivery_startup_->opened_(*this);
    if(normal_initialization_original_.transport_current_()) {
        if(delivery_startup_)normal_initialization_original_.bind_open_(*this,*delivery_startup_,physical_open_original_);
        else normal_initialization_original_.finish_unselected_open_(*this,physical_open_original_);
    }
    detail::store_physical_native_scope constructor_native(*this);
    if(!constructor_native.entered())throw db_error("constructor Native original unavailable");
    validate_ordinary_current_();
    if((ordinary_guarded_||wal_extent_vfs_||detail::store_physical_is_enrolled(db_))&&detail::restore_ordinary_attachment_guard(db_)!=SQLITE_OK)
        throw std::runtime_error("managed attachment gate installation failed");
    if (const auto* probe = detail::database_retirement_test_hooks::current; probe && probe->after_open)
        probe->after_open(*this, db_);
    if(key.administrative_||key.checked_existing_) {
#ifdef SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE
        if(sqlite3_db_config(db_,SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,1,nullptr)!=SQLITE_OK ||
           sqlite3_wal_autocheckpoint(db_,0)!=SQLITE_OK)
            throw db_error("receipt administration close/checkpoint policy unavailable");
#else
        throw db_error("receipt administration requires no-checkpoint-on-close support");
#endif
        const auto actual=physical_identity("main",{},true);
        const auto& expected=key.checked_existing_?key.checked_identity_:key.administrative_identity_;
        if(!expected || !actual || *actual!=*expected || actual->filename!=expected->filename)
            throw db_error("receipt administration file changed before SQLite admission");
    }
    // Statement-level busy timeout MUST be installed before ANY statement runs.
    // It used to be set after the open-time pragmas, so the very first
    // `PRAGMA journal_mode = WAL` had no busy handler — a concurrent open or
    // an in-flight writer on the same file made it throw
    // "database is locked" instantly instead of waiting. Instances now
    // genuinely close and reopen (weak instance cache), so open-time races
    // are common rather than exceptional.
    if (read_control_) {
        read_control_->publish(db_);
        if(set_sql_progress_(detail::database_handler_slot::checked_read,[](void* context)noexcept -> int {
            return static_cast<database_read_control*>(context)->stopped() ? 1 : 0;
        }, read_control_.get())!=SQLITE_OK)throw db_error("owned read progress policy installation failed");
        if(set_sql_busy_(detail::database_handler_slot::checked_read,[](void* context, int)noexcept -> int {
            auto* control = static_cast<database_read_control*>(context);
            if (control->stopped()) return 0;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            return control->stopped() ? 0 : 1;
        }, read_control_.get())!=SQLITE_OK)throw db_error("owned read busy policy installation failed");
        if (read_control_->stopped()) throw db_error("owned read cancelled before initialization");
    } else {
        sqlite3_busy_timeout(db_, busy_timeout_ms_);
    }

    // Register vec0 before any SQL can load an existing schema. SQLite marks
    // shadow tables while parsing that schema using the modules registered at
    // that moment; registering vec0 later does not reclassify those tables.
    // Busy/cancellation handlers above must still precede extension setup.
    if(delivery_startup_) {
        initialize_vec_registrations_();
        register_initialization_sync_disabled_();
    }else {
        char* vec_error=nullptr;const auto vec_creator=detail::database_native_process();
        const int vec_rc=sqlite3_vec_init(db_,&vec_error,nullptr);
        if(vec_creator!=detail::database_native_process())throw db_error("inherited vector registration return");
        sqlite3_free(vec_error);
        if(vec_rc!=SQLITE_OK) {
            LOG_ERROR("db", "Failed to initialize sqlite-vec extension");
            throw db_error("Failed to initialize sqlite-vec extension");
        }
    }

    if(delivery_startup_) {
        observer_delivery_sql_->register_startup_functions_(*this,*delivery_startup_);
        // Both vec0 setup and six-function registration have returned their
        // actual SQLite scopes. DATA-only observer, no initialization grant.
        const auto observed=detail::database_initialization_test_probe::after_functions;
        if(observed)observed(*this);
        require_creator_();delivery_startup_->require_initializing_(*this);
    }
    const auto initialization_control=[this](detail::observer_delivery_initialization_sql::recipe id) {
        auto statement=detail::observer_delivery_initialization_sql::fixed_(*delivery_startup_,*this,id);
        execute_initialization_(statement);
    };

    if(mode==open_mode::read_write && !key.checked_existing_)
        detail::recovery_continuous_producer::classify_open(*this,static_cast<bool>(key.continuous_),true);

    if(key.administrative_||key.checked_existing_) {
        const auto mode=query("PRAGMA main.journal_mode");
        if(mode.size()!=1 || mode[0].size()!=1 || !std::holds_alternative<std::string>(mode[0].begin()->second) ||
           std::get<std::string>(mode[0].begin()->second)!="wal")
            throw db_error("receipt administration requires existing WAL; no journal conversion");
        // synchronous is connection-local, not evidence of a persisted setting.
        // This private writer explicitly chooses FULL for its sole transaction.
        execute("PRAGMA synchronous=FULL");
        const auto durability=query("PRAGMA main.synchronous");
        if(durability.size()!=1 || durability[0].size()!=1 || !std::holds_alternative<int64_t>(durability[0].begin()->second) ||
           std::get<int64_t>(durability[0].begin()->second)!=2)
            throw db_error("receipt administration FULL durability unavailable");
        execute("PRAGMA foreign_keys=ON");
        return; // No WAL assignment, cache tuning, schema materialization or maintenance.
    }
    // Enable foreign keys
    if(delivery_startup_)initialization_control(detail::observer_delivery_initialization_sql::recipe::connection_foreign_keys);
    else execute("PRAGMA foreign_keys = ON");

#ifdef __EMSCRIPTEN__
    // WASM/OPFS mode: Use DELETE journal mode (WAL requires mmap/shm which OPFS doesn't support)
    // Also skip mmap since OPFS uses SyncAccessHandle instead
    if (mode == open_mode::read_write) {
        execute("PRAGMA journal_mode = DELETE");
    }
    if(delivery_startup_)initialization_control((read_control_||key.keeper_cache_)?
        detail::observer_delivery_initialization_sql::recipe::connection_cache_small:
        detail::observer_delivery_initialization_sql::recipe::connection_cache_large);
    else execute((read_control_ || key.keeper_cache_) ? "PRAGMA cache_size = 2000" : "PRAGMA cache_size = 50000");
    if(delivery_startup_)initialization_control(read_control_?
        detail::observer_delivery_initialization_sql::recipe::connection_temp_file:
        detail::observer_delivery_initialization_sql::recipe::connection_temp_memory);
    else execute(read_control_ ? "PRAGMA temp_store = FILE" : "PRAGMA temp_store = MEMORY");      // Temp tables in RAM
#else
    // Native mode: Enable WAL mode for better concurrency (only on read-write connection)
    if (mode == open_mode::read_write) {
        if(delivery_startup_)initialization_control(detail::observer_delivery_initialization_sql::recipe::connection_journal);
    else execute("PRAGMA journal_mode = WAL");
    }

    // Performance optimizations (matching Lattice.swift)
    if(delivery_startup_)initialization_control((read_control_||key.keeper_cache_)?
        detail::observer_delivery_initialization_sql::recipe::connection_cache_small:
        detail::observer_delivery_initialization_sql::recipe::connection_cache_large);
    else execute((read_control_ || key.keeper_cache_) ? "PRAGMA cache_size = 2000" : "PRAGMA cache_size = 50000");
    if(delivery_startup_)initialization_control(read_control_?
        detail::observer_delivery_initialization_sql::recipe::connection_mmap_off:
        detail::observer_delivery_initialization_sql::recipe::connection_mmap_on);
    else execute(read_control_ ? "PRAGMA mmap_size = 0" : "PRAGMA mmap_size = 300000000");    // Memory-mapped I/O (~300MB)
    if(delivery_startup_)initialization_control(read_control_?
        detail::observer_delivery_initialization_sql::recipe::connection_temp_file:
        detail::observer_delivery_initialization_sql::recipe::connection_temp_memory);
    else execute(read_control_ ? "PRAGMA temp_store = FILE" : "PRAGMA temp_store = MEMORY");      // Temp tables in RAM
#endif

    // (busy timeout installed immediately after open, above — before the
    // journal-mode/cache pragmas, which are themselves subject to locking.)

    if (mode == open_mode::read_write) {
        // No ANALYZE here: it scans every index (O(GB) on large databases) and
        // takes the write lock at the worst possible moment — open. Stats are
        // refreshed incrementally via "PRAGMA optimize" (dtor + maintenance
        // paths); analysis_limit bounds the cost of any future stats scan.
        if(delivery_startup_)initialization_control(detail::observer_delivery_initialization_sql::recipe::connection_analysis);
        else ::lattice::detail::store_physical_exec(db_, "PRAGMA analysis_limit=400", nullptr, nullptr, nullptr);
        // A post-checkpoint shrink policy, not prewrite extent admission. The
        // optional retained WAL VFS above enforces its explicit requested cap.
        if(delivery_startup_)initialization_control(detail::observer_delivery_initialization_sql::recipe::connection_journal_size);
        else ::lattice::detail::store_physical_exec(db_, "PRAGMA journal_size_limit=268435456", nullptr, nullptr, nullptr);
        // Materialize the WAL index (-shm/-wal) with a no-op read transaction.
        // Read-only connections CANNOT create the -shm file — on a fresh
        // database they fail with "unable to open database file" unless a
        // writable connection has started a transaction first. (ANALYZE used
        // to do this as a side effect.)
        if(delivery_startup_)initialization_control(detail::observer_delivery_initialization_sql::recipe::connection_schema_count);
        else ::lattice::detail::store_physical_exec(db_, "SELECT count(*) FROM sqlite_master", nullptr, nullptr, nullptr);
    }

    // Apple's libsqlite3 compiles SQLITE_ENABLE_STMT_SCANSTATUS and ships the
    // runtime toggle ON, which makes every WhereBegin opcode run the
    // sqlite3WhereAddExplainText/sqlite3_str_appendf explain-text pass — the
    // exact top frames of the Aug 2026 Engram SIGBUS — and measurably fattens
    // every prepare. We never read scanstatus, so turn it off per connection.
    // Stock amalgamations without the feature don't define the constant (and
    // the call would be a no-op there anyway).
#ifdef SQLITE_DBCONFIG_STMT_SCANSTATUS
    sqlite3_db_config(db_, SQLITE_DBCONFIG_STMT_SCANSTATUS, 0, nullptr);
#endif

    } catch (...) {
        retire_connection_();
        throw;
    }
}

database::~database() {
    if(target_staging_){retire_target_staging_();return;}
    if(!creator_current_()){retire_connection_(false);return;}
    detail::store_physical_cleanup_scope destruction_native(*this);
    if(!destruction_native.entered()){retire_connection_(false);return;}
    bool optional_current=!catalog_quarantined_();
    if(ordinary_context_){try{validate_ordinary_current_();}catch(...){optional_current=false;}}
    if (auto allowed = std::atomic_load(&local_producer_write_allowed_)) allowed->store(false, std::memory_order_release);
    if (canonical_write_allowed_) canonical_write_allowed_->store(false, std::memory_order_release);
    if (read_control_) read_control_->unpublish(db_);
    if (db_) {
        if (mode_ == open_mode::read_write) {
            // The connection is closing — silence change/commit hooks first.
            // PRAGMA optimize below may write sqlite_stat rows; firing hooks
            // into an owner that is mid-destruction locks destroyed mutexes.
            sqlite3_update_hook(db_, nullptr, nullptr);
            sqlite3_wal_hook(db_, nullptr, nullptr);
            sqlite3_commit_hook(db_, nullptr, nullptr);
            sqlite3_rollback_hook(db_, nullptr, nullptr);
            // Best-effort incremental stats refresh (bounded by analysis_limit).
            // Only re-analyzes tables this connection queried whose stats are
            // missing or stale. Never throw from a destructor.
            // An unresolved/failed producer bootstrap preserves its refused
            // schema. Hook detachment, passive checkpoint and close still run.
            // Logical close ends optional SQL maintenance as well as public
            // operations. Final destruction can happen later, after a checked
            // close returned; optimize may create sqlite_stat1 and invalidate
            // the already-published schema cookie. Physical retirement and its
            // original passive checkpoint below remain independent.
            if (optional_current && !closed_.load(std::memory_order_acquire) &&
                !suppress_destructor_optimize_ && !administrative_connection_) {
                int orc = ::lattice::detail::store_physical_exec(db_, "PRAGMA optimize", nullptr, nullptr, nullptr);
                if (orc != SQLITE_OK) {
                    LOG_DEBUG("db", "~database optimize skipped: rc=%d, path=%s", orc, path_.c_str());
                }
            }
            if(optional_current && !administrative_connection_ && !checked_existing_connection_) {
                int nLog = 0, nCkpt = 0;
                int rc = ::lattice::detail::store_physical_wal_checkpoint(db_, nullptr, SQLITE_CHECKPOINT_PASSIVE, &nLog, &nCkpt);
                LOG_DEBUG("db", "~database checkpoint: rc=%d, nLog=%d, nCkpt=%d, path=%s", rc, nLog, nCkpt, path_.c_str());
            }
        }
        retire_connection_();
    }
}

void database::retire_connection_(bool detach_callbacks) noexcept {
    if(target_staging_){retire_target_staging_();return;}
    if(creator_current_()) {
        auto* original=std::exchange(initialization_registrations_value_,nullptr);
        if(original){original->retired.store(true,std::memory_order_release);original->release();}
    }
    if (!db_) return;
    if(!creator_current_()){
        // No inherited SQLite, read-control mutex or callback teardown. The
        // genuine process slot retains the lease; no physical close is proved.
        db_=nullptr;
        if(retirement_)retirement_->record_close(detail::database_retirement_state::not_attempted,
                                               detail::database_retirement_state::not_attempted);
        return;
    }
    if(auto observed=std::atomic_exchange(&read_admission_original_,std::shared_ptr<read_admission_original>{}))
        observed->live.store(false,std::memory_order_release);
    detail::store_physical_cleanup_scope retirement_native(*this);
    if(!retirement_native.entered()){
        db_=nullptr; // Retain the physical original/lease; no Native ending was attempted.
        if(retirement_)retirement_->record_close(detail::database_retirement_state::not_attempted,detail::database_retirement_state::not_attempted);
        return;
    }
    if (auto allowed = std::atomic_load(&local_producer_write_allowed_)) allowed->store(false, std::memory_order_release);
    if (canonical_write_allowed_) canonical_write_allowed_->store(false, std::memory_order_release);
    if (read_control_) read_control_->unpublish(db_);
    // These callbacks borrow wrapper/read-control storage. Detach for every
    // mode and cleanup path before a deferred close can outlive that storage.
    // Connection-owned UDF contexts keep their existing SQLite-owned lifetime;
    // their producer/canonical admission was revoked above, not re-created.
    // Failed sqlite3_open can leave only a partial handle. No callbacks were
    // installed on that path; use only its supported error/close operations.
    if (detach_callbacks) {
        sqlite3_update_hook(db_, nullptr, nullptr);
        sqlite3_wal_hook(db_, nullptr, nullptr);
        sqlite3_commit_hook(db_, nullptr, nullptr);
        sqlite3_rollback_hook(db_, nullptr, nullptr);
        sqlite3_progress_handler(db_, 0, nullptr, nullptr);
        sqlite3_busy_handler(db_, nullptr, nullptr);
    }
    // Retained statements may invoke authorization during automatic reprepare
    // even after close_v2. Leave SQLite's DATA cookie alive, but revoke all
    // wrapper-borrowing callbacks before its context can be destroyed.
    if (auto* cookie=sql_dispatcher_.exchange(nullptr,std::memory_order_acq_rel)) (void)cookie->retire(*this);
    // No SQLite mutex guard may survive a successful close of that mutex.
    auto* retiring = std::exchange(db_, nullptr);
    #if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    if(wal_extent_vfs_)wal_extent_vfs_->native_close_entered(retiring);
    #endif
    const int checked = ::lattice::detail::store_physical_close_owned(physical_open_original_,retiring);
    if (checked == SQLITE_OK) target_terminal_shell_tail_.observer_close_proved_ = true;
    int fallback = detail::database_retirement_state::not_attempted;
    if (checked != SQLITE_OK) {
        // Preserve the existing zombie cleanup semantics. Never save/poll this
        // pointer: close_v2 may free it immediately or on final statement exit.
        fallback = ::lattice::detail::store_physical_close_v2_owned(physical_open_original_,retiring);
        if (fallback != SQLITE_OK)
            LOG_ERROR("db", "physical close cleanup failed: checked=%d fallback=%d, path=%s", checked, fallback, path_.c_str());
        else
            LOG_DEBUG("db", "physical close unproved: checked=%d fallback=%d, path=%s", checked, fallback, path_.c_str());
    } else LOG_DEBUG("db", "physical close proved: path=%s", path_.c_str());
    #if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    if(wal_extent_vfs_)wal_extent_vfs_->native_close_returned(retiring,checked,fallback);
    #endif
    if (retirement_) retirement_->record_close(checked, fallback);
}

void database::close() {
    // Logical close performs no Native admission. The monotone quarantine
    // read in require_creator_ linearizes refusal before these atomic effects;
    // a concurrent later quarantine cannot make close a cleanup/reuse grant.
    require_creator_();
    if (auto allowed = std::atomic_load(&local_producer_write_allowed_)) allowed->store(false, std::memory_order_release);
    // Logical close: ops short-circuit after this. The sqlite3* itself is freed in
    // ~database (single-threaded), so a concurrent reader holding this wrapper can
    // never deref a freed handle — it either sees closed_ and returns empty, or runs
    // a final query on the still-open connection. An already admitted private
    // maintenance scope likewise settles its complete transaction on its
    // owning thread; logical close never strands that transaction halfway.
    if (canonical_write_allowed_) canonical_write_allowed_->store(false, std::memory_order_release);
    closed_.store(true, std::memory_order_release);
}

sqlite3* database::handle() const {
    require_creator_();
    detail::ordinary_require_no_raw_escape(ordinary_context_);
    if(detail::store_physical_is_enrolled(db_))
        throw db_error("enrolled physical store forbids raw Native escape");
    // The connection stays allocated through logical close. As with every raw
    // access, callers must keep the database wrapper alive and not move it.
    if (!db_) return nullptr;
    auto* mutex = sqlite3_db_mutex(db_);
    sqlite3_mutex_enter(mutex);
    struct unlock {sqlite3_mutex* mutex;~unlock(){sqlite3_mutex_leave(mutex);}} release{mutex};
    require_catalog_clean_locked_();
    detail::require_continuous_raw_handle_absent(const_cast<database&>(*this));
    if(observer_delivery_sql_)throw db_error("bounded delivery writer cannot escape its raw handle");
    if (canonical_custody_bootstrap_)
        throw db_error("canonical attachment owns connection policy");
    if (canonical_callback_custody_) {
        if (!sqlite3_get_autocommit(db_) || update_hook_scope::active_for(db_))
            throw db_error("canonical work is active; raw handle refused");
        for (auto* statement=sqlite3_next_stmt(db_,nullptr);statement;statement=sqlite3_next_stmt(db_,statement))
            if (sqlite3_stmt_busy(statement))
                throw db_error("canonical statement is active; raw handle refused");
        if (canonical_write_allowed_) canonical_write_allowed_->store(false,std::memory_order_release);
    }
    raw_handle_escaped_.store(true, std::memory_order_release);
    if (retirement_) retirement_->raw_escaped_.store(true, std::memory_order_release);
    // A known raw escape ends future producer admission. There is no SQLite
    // getter that could establish which external authorizer a caller installs.
    if (auto allowed = std::atomic_load(&local_producer_write_allowed_)) allowed->store(false, std::memory_order_release);
    return db_;
}

void database::set_txn_hooks(std::function<void()> settled, std::function<void()> rolled_back) {
    require_creator_();
    if (!db_) throw db_error("transaction hooks require a physical connection");
    // Even std::function moves/swaps can copy or destroy inline targets. Finish
    // all callable construction before locking; replacement owns the displaced
    // bundle until after unlock, including on a refused registration.
    auto replacement = std::make_shared<txn_hook_callbacks>(std::move(settled), std::move(rolled_back));
    detail::store_physical_native_scope physical_call(*this);
    if(!physical_call.entered())throw db_error("resource Native caller original unavailable");
    {
    auto* mutex=sqlite3_db_mutex(db_);sqlite3_mutex_enter(mutex);
    struct unlock {sqlite3_mutex* mutex;~unlock(){sqlite3_mutex_leave(mutex);}} release{mutex};
    require_catalog_clean_locked_();
    if (canonical_custody_bootstrap_ || canonical_callback_custody_ ||
        std::atomic_load(&local_producer_callback_custody_))
        throw db_error("transaction hooks belong to attached recovery policy");
    // Clearing public hooks cannot recreate the original engine callbacks.
    txn_hooks_external_=true;
    txn_hooks_.swap(replacement);
    rebind_txn_hooks_owned_();
    }
}

void database::set_txn_hooks_owned_(std::function<void()> settled, std::function<void()> rolled_back,
        std::function<std::function<void()>(database&, bool)> capture_owned) {
    require_creator_();
    auto replacement = std::make_shared<txn_hook_callbacks>(
        std::move(settled), std::move(rolled_back), std::move(capture_owned));
    detail::store_physical_native_scope physical_call(*this);
    if(!physical_call.entered())throw db_error("resource Native caller original unavailable");
    {
        auto* mutex=sqlite3_db_mutex(db_);sqlite3_mutex_enter(mutex);
        struct unlock {sqlite3_mutex* mutex;~unlock(){sqlite3_mutex_leave(mutex);}} release{mutex};
        require_catalog_clean_locked_();
        txn_hooks_.swap(replacement);
        rebind_txn_hooks_owned_();
    }
}

void database::rebind_txn_hooks_owned_() noexcept {
    if(!creator_current_())return;
    detail::store_physical_native_scope physical_call(*this);
    if(!physical_call.entered())return;
    // Rollback hook: fires inside SQLite's C frames, so the trampoline only
    // clears a flag and (via rolled_back) a C++ vector — no SQLite calls,
    // nothing that can throw. Applies to every storage kind: without it a
    // rolled-back transaction's buffered rows linger and the NEXT flush
    // delivers them as phantoms (latent file-DB bug, see the design doc).
    sqlite3_rollback_hook(db_,
        [](void* self_ptr) {
            auto* self = static_cast<database*>(self_ptr);
            self->txn_dirty_.store(false, std::memory_order_relaxed);
            // SQLite holds its mutex here; retain only the stable bundle. The
            // rollback callback's existing no-SQL/no-throw contract applies.
            const auto hooks = self->txn_hooks_;
            if (hooks && hooks->rolled_back) hooks->rolled_back();
        },
        this);
}

void database::drain_if_settled() {
    require_creator_();
    detail::store_physical_native_scope physical_call(*this);
    if(!physical_call.entered())throw db_error("resource Native caller original unavailable");
    if (auto* operation = attachment_sql_scope::operation_for(this)) {
        operation->capture_settlement_locked(*this, false);
        return;
    }
    // A nested query from the update hook can finish before the outer
    // implicit statement does. Autocommit alone does not identify that
    // callback frame. Leave dirty state for the actual statement's tail.
    if (update_hook_scope::active_for(db_)) return;
    // The explicit maintenance tail delivers after releasing its outer
    // SQLite mutex and store gate. Keep dirty state pending until then.
    if (maintenance_scope::active_for(db_)) return;
    if (mutation_sql_scope::active_for(this) || core_settlement_scope::active_for(this)) return;
    if (lattice_update_hook_context_ && lattice_update_hook_context_->mutation_tail_pending) return;
    // Attached scalar wrappers release their writer (and optional vector
    // store gate) before delivering the outer successful statement's tail.
    if (detail::managed_route_scope::active_for(this)) return;
    // A compound read may retain the connection mutex and a real main cursor
    // across several queries. Its explicit tail drains after both are released.
    if (engine_query_scope::defers_delivery_for(*this)) return;
    // Post-statement drain point (docs/design-deferred-memory-delivery.md):
    // after a successful statement, autocommit != 0 means the top-level
    // transaction just closed (implicit, or the explicit COMMIT that funnels
    // through execute()) and every lock is released — the exact post-commit
    // point the WAL hook gives file DBs, but reached through plain C++
    // frames, so observer exceptions propagate to the writer instead of
    // unwinding through sqlite3_step. Clear the flag BEFORE draining so a
    // callback's own writes re-arm it rather than re-entering.
    if (!db_ || !txn_dirty_.load(std::memory_order_relaxed)) return;
    std::shared_ptr<txn_hook_callbacks> hooks;
    {
        auto* mutex = sqlite3_db_mutex(db_);
        sqlite3_mutex_enter(mutex);
        struct unlock {sqlite3_mutex* mutex;~unlock(){sqlite3_mutex_leave(mutex);}} release{mutex};
        require_catalog_clean_locked_();
        if (txn_hooks_ && txn_hooks_->settled && sqlite3_get_autocommit(db_) != 0 &&
            txn_dirty_.exchange(false, std::memory_order_relaxed)) hooks = txn_hooks_;
    }
    // Claim under SQLite, deliver after releasing it. Unrelated active read
    // cursors do not defer an already committed writer's notifications.
    // The callback may replace its own registration. Its current bundle stays
    // alive through return and is released here, outside SQLite.
    if (hooks) hooks->settled();
}

void database::discard_if_rolled_back() {
    require_creator_();
    detail::store_physical_native_scope physical_call(*this);
    if(!physical_call.entered())throw db_error("resource Native caller original unavailable");
    if (mutation_sql_scope::active_for(this) || core_settlement_scope::active_for(this)) return;
    if (lattice_update_hook_context_ && lattice_update_hook_context_->mutation_tail_pending) return;
    if (auto* operation = attachment_sql_scope::operation_for(this)) {
        operation->capture_settlement_locked(*this, true);
        return;
    }
    // Failed statement with autocommit restored: the implicit transaction
    // (if any) already rolled back. SQLite's rollback hook covers most of
    // these paths; clear defensively so a hook-buffered row from the failed
    // statement can't surface as a phantom in the next flush. Inside an
    // explicit transaction (autocommit == 0) nothing is cleared — the
    // transaction may still commit.
    std::shared_ptr<txn_hook_callbacks> hooks;
    {
        auto* mutex = sqlite3_db_mutex(db_);sqlite3_mutex_enter(mutex);
        struct unlock {sqlite3_mutex* mutex;~unlock(){sqlite3_mutex_leave(mutex);}} release{mutex};
        require_catalog_clean_locked_();
        if (sqlite3_get_autocommit(db_) != 0) {
            txn_dirty_.store(false, std::memory_order_relaxed);
            hooks = txn_hooks_;
        }
    }
    if (hooks && hooks->rolled_back) hooks->rolled_back();
}

sqlite3* database::note_owning_wrapper_move_(database& other) noexcept {
    if(other.creator_current_()&&other.initialization_registrations_value_)
        other.initialization_registrations_value_->retired.store(true,std::memory_order_release);
    // Pointer factories never move actual database objects. A public move
    // leaves their external owning-deleter envelope, so retain honest debt.
    if(other.retirement_)other.retirement_->owning_wrapper_moved_.store(true,std::memory_order_release);
    return other.db_;
}
database::database(database&& other) noexcept
    : physical_open_original_(std::move(other.physical_open_original_)),
      target_staging_(std::move(other.target_staging_)),
      db_(note_owning_wrapper_move_(other)), retirement_(std::move(other.retirement_)), ordinary_context_(std::move(other.ordinary_context_)),
      ordinary_guarded_(other.ordinary_guarded_), path_(std::move(other.path_)), mode_(other.mode_),
      busy_timeout_ms_(other.busy_timeout_ms_), read_control_(std::move(other.read_control_)),
      wal_extent_vfs_(std::move(other.wal_extent_vfs_)),
      main_physical_identity_(creator_current_()?std::atomic_load(&other.main_physical_identity_):nullptr) {
    delivery_startup_.take_(other.delivery_startup_);
    sql_dispatcher_.store(other.sql_dispatcher_.exchange(nullptr,std::memory_order_acq_rel),std::memory_order_release);
    canonical_trigger_only_ = std::exchange(other.canonical_trigger_only_, false);
    canonical_callback_custody_ = std::move(other.canonical_callback_custody_);
    canonical_write_allowed_ = std::move(other.canonical_write_allowed_);
    suppress_destructor_optimize_ = std::exchange(other.suppress_destructor_optimize_, false);
    administrative_connection_ = std::exchange(other.administrative_connection_, false);
    checked_existing_connection_ = std::exchange(other.checked_existing_connection_, false);
    continuous_file_.store(other.continuous_file_.exchange(continuous_classification::unknown));
    txn_hooks_external_ = std::exchange(other.txn_hooks_external_,false);
    local_producer_callback_custody_ = std::move(other.local_producer_callback_custody_);
    local_producer_write_allowed_ = std::move(other.local_producer_write_allowed_);
    channel_reset_unsettled_.store(other.channel_reset_unsettled_.exchange(false));
    core_transaction_serial_ = std::exchange(other.core_transaction_serial_, 0);
    mutation_operation_serial_ = std::exchange(other.mutation_operation_serial_, 0);
    core_transaction_original_ = std::move(other.core_transaction_original_);
    if(auto observed=std::atomic_exchange(&other.read_admission_original_,std::shared_ptr<read_admission_original>{}))
        observed->live.store(false,std::memory_order_release);
    sql_transaction_live_.store(other.sql_transaction_live_.exchange(false,std::memory_order_acq_rel),std::memory_order_release);
    sql_read_binding_ = std::move(other.sql_read_binding_);
    observer_delivery_sql_ = std::move(other.observer_delivery_sql_);
    if(observer_delivery_sql_)observer_delivery_sql_->moved();
    core_native_tail_owned_.store(other.core_native_tail_owned_.exchange(false));
    mutation_operation_unsettled_.store(other.mutation_operation_unsettled_.exchange(false) ||
        bool(core_transaction_original_) || core_native_tail_owned_.load());
    // Immutable originals keep their OLD owner. Moving a wrapper never issues
    // a new transaction grant or manufactures a completed old Native ending.
    lattice_update_hook_context_ = std::move(other.lattice_update_hook_context_);
    txn_dirty_.store(other.txn_dirty_.exchange(false));
    txn_hooks_ = std::move(other.txn_hooks_);
    if (txn_hooks_ && !catalog_quarantined_()) rebind_txn_hooks_owned_();
    raw_handle_escaped_.store(other.raw_handle_escaped_.load(std::memory_order_acquire));
    closed_.store(other.closed_.exchange(true, std::memory_order_acq_rel), std::memory_order_release);
    other.db_ = nullptr;
}

database& database::operator=(database&& other) noexcept {
    if (this != &other) {
        retire_connection_();
        // Retire displaced user captures after the complete move/rebind, with
        // no SQLite mutex held. Bare wrappers' public bundles move as well.
        auto retired_hooks = std::move(txn_hooks_);
        auto retired_target = std::move(target_staging_);
        target_staging_ = std::move(other.target_staging_);
        delivery_startup_.take_(other.delivery_startup_);
    sql_dispatcher_.store(other.sql_dispatcher_.exchange(nullptr,std::memory_order_acq_rel),std::memory_order_release);
    canonical_trigger_only_ = std::exchange(other.canonical_trigger_only_, false);
        canonical_callback_custody_ = std::move(other.canonical_callback_custody_);
        canonical_write_allowed_ = std::move(other.canonical_write_allowed_);
        suppress_destructor_optimize_ = std::exchange(other.suppress_destructor_optimize_, false);
        administrative_connection_ = std::exchange(other.administrative_connection_, false);
        checked_existing_connection_ = std::exchange(other.checked_existing_connection_, false);
        continuous_file_.store(other.continuous_file_.exchange(continuous_classification::unknown));
        txn_hooks_external_ = std::exchange(other.txn_hooks_external_,false);
        local_producer_callback_custody_ = std::move(other.local_producer_callback_custody_);
        local_producer_write_allowed_ = std::move(other.local_producer_write_allowed_);
        channel_reset_unsettled_.store(other.channel_reset_unsettled_.exchange(false));
        auto retired_core_original = std::move(core_transaction_original_);
        core_transaction_serial_ = std::exchange(other.core_transaction_serial_, 0);
        mutation_operation_serial_ = std::exchange(other.mutation_operation_serial_, 0);
        core_transaction_original_ = std::move(other.core_transaction_original_);
    if(auto observed=std::atomic_exchange(&other.read_admission_original_,std::shared_ptr<read_admission_original>{}))
        observed->live.store(false,std::memory_order_release);
    sql_transaction_live_.store(other.sql_transaction_live_.exchange(false,std::memory_order_acq_rel),std::memory_order_release);
    sql_read_binding_ = std::move(other.sql_read_binding_);
    observer_delivery_sql_ = std::move(other.observer_delivery_sql_);
    if(observer_delivery_sql_)observer_delivery_sql_->moved();
        core_native_tail_owned_.store(other.core_native_tail_owned_.exchange(false));
        mutation_operation_unsettled_.store(other.mutation_operation_unsettled_.exchange(false) ||
            bool(core_transaction_original_) || core_native_tail_owned_.load());
        lattice_update_hook_context_ = std::move(other.lattice_update_hook_context_);
        physical_open_original_ = std::move(other.physical_open_original_);
        db_ = note_owning_wrapper_move_(other);
        // Public wrapper moves cannot transfer an old Core transaction grant.
        core_transaction_thread_ = std::thread::id{};
        core_transaction_lifetime_.reset();
        retirement_ = std::move(other.retirement_);
        ordinary_context_ = std::move(other.ordinary_context_);
        ordinary_guarded_ = other.ordinary_guarded_;
        txn_dirty_.store(other.txn_dirty_.exchange(false));
        txn_hooks_ = std::move(other.txn_hooks_);
        // The SQLite update/WAL userdata address has not changed. The rollback
        // trampoline uses database*, including on a public-hook-only wrapper.
        if (txn_hooks_ && !catalog_quarantined_()) rebind_txn_hooks_owned_();
        raw_handle_escaped_.store(other.raw_handle_escaped_.load(std::memory_order_acquire));
        closed_.store(other.closed_.exchange(true, std::memory_order_acq_rel), std::memory_order_release);
        mode_ = other.mode_;
        busy_timeout_ms_ = other.busy_timeout_ms_;
        read_control_ = std::move(other.read_control_);
        wal_extent_vfs_ = std::move(other.wal_extent_vfs_);
        if(creator_current_())std::atomic_store(&main_physical_identity_, std::atomic_load(&other.main_physical_identity_));
        path_ = std::move(other.path_);
        other.db_ = nullptr;
    }
    return *this;
}

int64_t database::changes() const {
    require_creator_();
    catalog_native_admission_ admission(*this);
    if (closed_.load(std::memory_order_acquire) || !db_) return 0;
    return sqlite3_changes64(db_);
}

void database::note_history_mutation_(sqlite3_stmt* statement) const noexcept {
    // Called under this exact connection's SQLite mutex, immediately before
    // an actual step. SQLite's own program classification, not SQL text or a
    // caller flag, determines whether this program may write. The field moves
    // with retirement_ and cannot be reset through any public/native DTO API.
    if(!retirement_)return;
    auto& sequence=retirement_->ordinary_history_mutation_epoch_;
    if(!statement||sqlite3_db_handle(statement)!=db_){sequence.store(std::numeric_limits<uint64_t>::max(),std::memory_order_release);return;}
    if(sqlite3_stmt_readonly(statement))return;
    auto prior=sequence.load(std::memory_order_relaxed);
    while(prior!=std::numeric_limits<uint64_t>::max()&&!sequence.compare_exchange_weak(prior,prior+1,std::memory_order_release,std::memory_order_relaxed)){}
}
bool database::core_transaction_original::same_native_for(const database& value) const noexcept {
    return owner == &value && connection && creator == detail::database_native_process() &&
        value.creator_current_() && value.db_ == connection && native &&
        !native->owning_wrapper_moved_.load(std::memory_order_acquire) &&
        value.retirement_.get() == native.get() &&
        !value.retirement_.owner_before(native) && !native.owner_before(value.retirement_);
}
bool database::core_transaction_original::current_for(const database& value) const noexcept {
    if (!same_native_for(value) || ended.load(std::memory_order_acquire) ||
        thread != std::this_thread::get_id() || core_thread_lifetime_ended_ ||
        !lifetime || !lifetime->alive.load(std::memory_order_acquire)) return false;
    const auto& actual = core_thread_lifetime_().original;
    return actual == lifetime && !actual.owner_before(lifetime) && !lifetime.owner_before(actual);
}
bool database::core_transaction_origin_live_locked_() const noexcept {
    const auto original=core_transaction_lifetime_.lock();
    return original&&original->alive.load(std::memory_order_acquire);
}
bool database::core_transaction_origin_current_locked_() const noexcept {
    if(core_thread_lifetime_ended_)return false;
    const auto original=core_transaction_lifetime_.lock();
    const auto& current=core_thread_lifetime_().original;
    return original&&current&&original==current&&original->alive.load(std::memory_order_acquire);
}
void database::note_core_transaction_end_locked_(int native_result) const noexcept {
    const bool native_live=db_&&sqlite3_get_autocommit(db_)==0;
    sql_transaction_live_.store(native_live,std::memory_order_release);
    note_initialization_end_locked_();
    if(!native_live) {
        auto observed=std::atomic_load(&read_admission_original_);
        if(observed&&core_transaction_original_&&
           !observed->actual.owner_before(core_transaction_original_)&&
           !core_transaction_original_.owner_before(observed->actual)) {
            observed->live.store(false,std::memory_order_release);
            std::atomic_compare_exchange_strong(&read_admission_original_,&observed,
                std::shared_ptr<read_admission_original>{});
        }
    }
    // Only called with the actual physical SQLite mutex held. Do not infer an
    // origin for any successor; real native statement settlement can only
    // retire the Core BEGIN provenance already recorded on this wrapper.
    if(core_transaction_thread_!=std::thread::id{} && db_ && sqlite3_get_autocommit(db_)!=0) {
        if (auto* hook = lattice_update_hook_context_.get()) {
            if (hook->mutation_original && hook->mutation_original == core_transaction_original_ && hook->mutation_ended) {
                const bool successful = native_result == SQLITE_OK || native_result == SQLITE_DONE;
                const bool committed = successful && hook->mutation_commit_attempted && !hook->mutation_rollback_observed;
                hook->mutation_ended(*hook, committed, hook->mutation_rollback_observed);
            }
        }
        if (core_transaction_original_) {
            core_transaction_original_->ended.store(true, std::memory_order_release);
            if (core_transaction_original_->observer_delivery)
                core_transaction_original_->observer_delivery->ended();
        }
        core_transaction_original_.reset();
        core_transaction_thread_=std::thread::id{};
        core_transaction_lifetime_.reset();
    }
}

void database::latch_delivery_failure_(const std::exception_ptr& first)const noexcept {
    for(auto* active=mutation_sql_scope::current_;active;active=active->previous_) {
        if(active->creator_!=detail::database_native_process())return;
        if(active->connection_==db_&&!active->finalization_failure_) {
            active->finalization_failure_=first;return;
        }
    }
}
[[noreturn]] void database::refuse_delivery_statement_()const {
    // Match the actual operation's existing first-refusal latch. Catching a
    // closed-profile denial cannot commit an accepted prefix of THIS unit.
    // An unrelated genuine diagnostic read remains admitted above.
    try {throw db_error("bounded delivery requires its closed generated statement before bind/step");}
    catch(...) {latch_delivery_failure_(std::current_exception());throw;}
}
int database::execute_observed_sql_(const char* sql,bool core_control) {
    require_catalog_clean_locked_();
    detail::database_sql_request::binding no_inherited_request(*this,nullptr);
    // The no-callback branch of execute/attachment SQL already retains the
    // physical SQLite mutex. Follow SQLite's own exec prepare/tail/step/
    // finalize shape once; never double-prepare an authorizer/fault action.
    // Readonly transaction statements remain unchanged. An unknown failed
    // mutating step still advances the private sequence and forces auditing.
    // Preserve native exec's initial error-state/API validity reset without
    // preparing or stepping any program (and thus without fault callbacks).
    int rc=::lattice::detail::store_physical_exec(db_,"",nullptr,nullptr,nullptr);
    note_core_transaction_end_locked_();
    const char* at=sql?sql:"";
    while(rc==SQLITE_OK&&*at){
        sqlite3_stmt* statement=nullptr;const char* tail=nullptr;
        rc=::lattice::detail::store_physical_prepare_v2(db_,at,-1,&statement,&tail);
        if(rc!=SQLITE_OK){finalize_mutation_statement_(statement);break;}
        if(!statement){at=tail;continue;} // whitespace/comment, no actual step
        std::optional<detail::observer_delivery_sql_runtime::control_scope> delivery_control;
        try {
            if(observer_delivery_sql_&&core_control)delivery_control.emplace(*observer_delivery_sql_,*this,statement);
            if(observer_delivery_sql_&&!observer_delivery_sql_->admits(*this,statement)) {
                finalize_mutation_statement_(std::exchange(statement,nullptr));
                refuse_delivery_statement_();
            }
        }catch(...) {finalize_mutation_statement_(statement);throw;}
        do {note_history_mutation_(statement);rc=::lattice::detail::store_physical_step(statement);}while(rc==SQLITE_ROW);
        // Like native exec, finalize supplies the terminal result including
        // a failed virtual-table/commit settlement. No result callback/copy.
        rc=finalize_mutation_statement_(statement);note_core_transaction_end_locked_(rc);at=tail;
        // Match sqlite3_exec's ASCII tail advancement after an actual
        // finalized program. Do not prepare an over-limit whitespace tail
        // after a committed statement, or invent locale-sensitive spacing.
        while(static_cast<unsigned char>(*at)==0x20 ||
              (static_cast<unsigned char>(*at)>=0x09 &&
               static_cast<unsigned char>(*at)<=0x0d))++at;
    }
    return rc;
}

int database::step_statement_(sqlite3_stmt* statement) const {
    require_creator_();
    sql_entry_scope entry(*this);
    auto* mutex=sqlite3_db_mutex(db_);sqlite3_mutex_enter(mutex);
    struct unlock {sqlite3_mutex* mutex;~unlock(){sqlite3_mutex_leave(mutex);}} release{mutex};
    require_catalog_clean_locked_();
    if(observer_delivery_sql_&&!observer_delivery_sql_->admits(const_cast<database&>(*this),statement))
        refuse_delivery_statement_();
    // Transactions such as COMMIT are SQLite-readonly but have no result
    // columns. Only actual read diagnostics may step while fenced.
    const bool refused=(channel_reset_unsettled_.load(std::memory_order_acquire) ||
         mutation_operation_unsettled_.load(std::memory_order_acquire)) &&
        (!sqlite3_stmt_readonly(statement)||sqlite3_column_count(statement)==0);
    if(refused)return SQLITE_ABORT;
    detail::database_sql_request* request=nullptr;
    for(auto* read=engine_reads_;read;read=read->next_read)
        if(read->statement==statement){request=read->request;break;}
    detail::database_sql_request::binding requested(const_cast<database&>(*this),request,statement);
    note_history_mutation_(statement);
    const int rc=::lattice::detail::store_physical_step(statement);
    note_core_transaction_end_locked_(rc);
    return rc;
}

void database::execute(const std::string& sql, const std::vector<column_value_t>& params) {
    execute_impl_(sql, params, nullptr);
}

void database::capture_core_tail_locked_(core_settlement_result& result) {
    auto* context = lattice_update_hook_context_.get();
    if (!context || !context->mutation_tail_pending || !context->mutation_original ||
        !context->mutation_original->ended.load(std::memory_order_acquire)) return;
    result.ended = true;
    result.committed = context->mutation_tail_committed;
    result.publication = context->mutation_first_delivery_failure;
    const auto creator = detail::database_native_process();
    detail::native_retirement_storage<std::shared_ptr<txn_hook_callbacks>> custody;
    auto& hooks = custody.get<0>(); hooks = txn_hooks_;
    try {
        if (!hooks || !hooks->capture_owned)
            throw db_error("Core mutation tail has no owned Native capture");
        // Credit wrapping must allocate BEFORE capture detaches any Native
        // batch. The actual callable moves directly into preallocated heap
        // custody; a later wrapper allocation failure cannot lose that batch.
        const auto& delivery = context->mutation_original->observer_delivery;
        std::optional<detail::observer_delivery_original::carrier> carrier;
        if (delivery) {
            const auto& pending = target_terminal_shell_tail_.observer_delivery_;
            if (pending != delivery || pending.owner_before(delivery) || delivery.owner_before(pending))
                throw db_error("Core delivery capture lost its exact final-member original");
            carrier.emplace(detail::observer_delivery_original::prepare(delivery));
        }
        // Snapshot the actual committed/rolled-back Native tail before another
        // BEGIN can publish. The callable itself runs only after all leaves.
        result.delivery = hooks->capture_owned(*this, !result.committed);
        if (creator != detail::database_native_process())
            throw db_error("Core Native capture belongs to another process");
        if (!result.delivery) throw db_error("Core Native tail capture remains busy");
        if (carrier) {
            carrier->adopt(std::move(result.delivery));
            result.delivery = carrier->finish();
            delivery->captured(); // real ended original AND actual owned capture
            if(observer_delivery_sql_)observer_delivery_sql_->original_captured(delivery);
            target_terminal_shell_tail_.observer_delivery_.reset(); // carrier/context pin it
        }
        context->mutation_tail_pending = false;
        context->mutation_original.reset();
        core_native_tail_owned_.store(false, std::memory_order_release);
        context->mutation_first_delivery_failure = {};
        context->mutation_commit_attempted = false;
        context->mutation_rollback_observed = false;
        if (result.committed) txn_dirty_.store(false, std::memory_order_relaxed);
    } catch (...) {
        if (creator != detail::database_native_process()) throw;
        result.capture = std::current_exception();
        mutation_operation_unsettled_.store(true, std::memory_order_release);
        // Live originals/buffer remain pending; no SQL body is repeated.
    }
}
database::core_settlement_result database::end_transaction_owned_(bool rollback,
    std::atomic<std::thread::id>& owner_thread,
    const std::shared_ptr<const core_transaction_original>& expected) {
    core_settlement_result result; // legacy owning return contract
    end_transaction_owned_into_(rollback, owner_thread, expected, result);
    return result;
}
void database::end_transaction_owned_into_(bool rollback,
    std::atomic<std::thread::id>& owner_thread,
    const std::shared_ptr<const core_transaction_original>& expected, core_settlement_result& result, bool strict_receipt) {
    const auto creator = detail::database_native_process();
    require_creator_();
    if (!expected || !expected->same_native_for(*this))
        throw db_error("Core settlement requires its retained original transaction");
    const bool already_ended = expected->ended.load(std::memory_order_acquire);
    if (!already_ended && !expected->current_for(*this))
        throw db_error("Core settlement requires its original live thread");
    sql_entry_scope entry(*this, expected);
    core_settlement_scope settlement(*this, expected, already_ended);
    auto* mutex = sqlite3_db_mutex(db_); sqlite3_mutex_enter(mutex);
    struct unlock {
        sqlite3_mutex* value; const uint64_t creator;
        ~unlock() { if (creator == detail::database_native_process()) sqlite3_mutex_leave(value); }
    } hold{mutex, creator};
    require_catalog_clean_locked_();
    if (strict_receipt && raw_handle_escaped_.load(std::memory_order_acquire))
        throw db_error("Core receipt lost exclusive wrapper custody before settlement");
    if (!already_ended) {
        if (core_transaction_original_ != expected || !expected->current_for(*this) ||
            !core_transaction_origin_current_locked_() || sqlite3_get_autocommit(db_) != 0)
            throw db_error("Core settlement original transaction has ended");
        try { execute_impl_(rollback ? "ROLLBACK" : "COMMIT", {}, &owner_thread); }
        catch (...) {
            if (creator != detail::database_native_process()) throw;
            result.first = std::current_exception();
        }
    }
    if (creator != detail::database_native_process()) throw db_error("Core settlement belongs to another process");
    result.ended = expected->ended.load(std::memory_order_acquire);
    if (result.ended) {
        if (administrative_connection_) {
            // The existing closed-source administrative owner deliberately has
            // no ordinary model buffer/capture. Its actual returned control
            // result settles metadata only; never mint model delivery facts.
            result.committed = !rollback && !result.first;
            core_native_tail_owned_.store(false, std::memory_order_release);
        } else capture_core_tail_locked_(result);
    }

}

void database::execute_impl_(const std::string& sql, const std::vector<column_value_t>& params,
    std::atomic<std::thread::id>* owner_thread) {
    require_creator_();
    if (closed_.load(std::memory_order_acquire) && !maintenance_scope::active_for(db_) && !mutation_sql_scope::active_for(this) && !core_settlement_scope::active_for(this)) return;
    sql_entry_scope entry(*this);
    if (closed_.load(std::memory_order_acquire) && !maintenance_scope::active_for(db_) && !mutation_sql_scope::active_for(this) && !core_settlement_scope::active_for(this)) return;
    g_statement_count.fetch_add(1, std::memory_order_relaxed);
    ++t_statement_count;
    if (params.empty()) {
        // Fast path for parameterless queries
        std::string errmsg;
        int rc, failure_extended_code, failure_system_errno;
        bool core_ended = false;
        {
            auto* mutex=sqlite3_db_mutex(db_);sqlite3_mutex_enter(mutex);
            struct unlock {sqlite3_mutex* mutex;~unlock(){sqlite3_mutex_leave(mutex);}} release{mutex};
            require_catalog_clean_locked_();
            // Destroy before unlock, including allocation/error unwind. A
            // failed still-live COMMIT retains its genuine owner; a real end
            // clears before a raw successor or off-lock delivery can run.
            struct end_stamp {
                database& owner; sqlite3* db; std::atomic<std::thread::id>* thread; std::thread::id& origin;
                std::weak_ptr<const core_thread_lifetime>& lifetime;
                bool was_core; bool& ended;
                ~end_stamp() noexcept {
                    if((thread||was_core) && sqlite3_get_autocommit(db)!=0) {
                        owner.note_core_transaction_end_locked_(SQLITE_ERROR);
                        origin=std::thread::id{};lifetime.reset();ended=was_core||thread;
                        if(thread)thread->store(std::thread::id{},std::memory_order_release);
                    }
                }
            } stamp{*this,db_,owner_thread,core_transaction_thread_,core_transaction_lifetime_,
                core_transaction_thread_!=std::thread::id{},core_ended};
            const bool explicit_rollback=sql=="ROLLBACK";
            if(mutation_operation_unsettled_.load(std::memory_order_acquire)&&!explicit_rollback)
                throw db_error("mutation operation unsettled; explicit rollback required");
            if(channel_reset_unsettled_.load(std::memory_order_acquire)&&!explicit_rollback)
                throw db_error("channel reset unsettled; explicit rollback required");
            rc = execute_observed_sql_(sql.c_str(),owner_thread!=nullptr);
            if(rc!=SQLITE_OK) {
                // Preserve this failure while the existing connection mutex
                // still owns it, before cleanup or any metadata lookup.
                failure_extended_code=sqlite3_extended_errcode(db_);
                failure_system_errno=sqlite3_system_errno(db_);
                const auto* message=sqlite3_errmsg(db_);errmsg=message?message:"Unknown error";
            } else if(explicit_rollback&&sqlite3_get_autocommit(db_)!=0) {
                channel_reset_unsettled_.store(false,std::memory_order_release);
                mutation_operation_unsettled_.store(false,std::memory_order_release);
            }
        }
        entry.release(); // Native mutex/stamp ended; callbacks start a fresh public turn.
        // Private rendezvous observes an actual returned ending after the
        // physical mutex and stamp tail; production leaves the hook null.
        if(core_ended && detail::recovery_admission_test_hooks::after_core_transaction_end)
            detail::recovery_admission_test_hooks::after_core_transaction_end();
        if (rc != SQLITE_OK) {
            std::string error = errmsg;
            LOG_ERROR("db", "SQL execution failed: %s (SQL: %s) [rc=%d extended_code=%d system_errno=%d sqlite_version=%s sqlite_source_id=%s]",
                error.c_str(), sql.c_str(), rc, failure_extended_code, failure_system_errno, sqlite3_libversion(), sqlite3_sourceid());
            discard_if_rolled_back();
            throw db_error("SQL execution failed: " + error + " (SQL: " + sql + ")");
        }
        drain_if_settled();
    } else {
        // Prepared statement path for parameterized queries
        sqlite3_stmt* stmt = nullptr;
        int rc = prepare_catalog_checked_(sql.c_str(), -1, &stmt, nullptr);
        std::unique_ptr<sqlite3_stmt, decltype(&finalize_mutation_statement_)> issued(stmt, finalize_mutation_statement_);
        if (rc != SQLITE_OK) {
            LOG_ERROR("db", "Failed to prepare statement: %s (SQL: %s)", sqlite3_errmsg(db_), sql.c_str());
            throw db_error("Failed to prepare statement: " + std::string(sqlite3_errmsg(db_)));
        }

        int index = 1;
        for (const auto& param : params) {
            bind_value(stmt, index++, param);
        }

        // Step until SQLITE_DONE. Virtual tables (e.g. vec0) may return
        // SQLITE_ROW for DML statements (DELETE returns the deleted row).
        // Drain all rows before expecting SQLITE_DONE.
        do {
            rc = step_statement_(stmt);
        } while (rc == SQLITE_ROW);

        // Capture error message BEFORE finalize, which resets connection error state
        std::string errmsg_str;
        if (rc != SQLITE_DONE) {
            errmsg_str = sqlite3_errmsg(db_) ? sqlite3_errmsg(db_) : "Unknown error";
        }

        finalize_mutation_statement_(issued.release());

        if (rc != SQLITE_DONE) {
            LOG_ERROR("db", "Execution failed: %s (SQL: %s)", errmsg_str.c_str(), sql.c_str());
            entry.release();
            discard_if_rolled_back();
            throw db_error("Execution failed: " + errmsg_str);
        }
        entry.release(); // Off-lock delivery is a fresh reentrant public turn.
        drain_if_settled();
    }
}

bool database::table_exists(const std::string& name) const {
    require_creator_();
    const char* sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
    detail::database_sql_request request(const_cast<database&>(*this),detail::database_sql_read_kind::table_exists,sql,{});
    sql_entry_scope entry(const_cast<database&>(*this));
    engine_query_scope issued(const_cast<database&>(*this),false,&request);
    sqlite3_stmt* stmt = nullptr;

    int rc = prepare_catalog_checked_(sql, -1, &stmt, nullptr,&request);
    issued.adopt(stmt);
    if (rc != SQLITE_OK) {
        LOG_ERROR("db", "Failed to prepare table_exists statement: %s", sqlite3_errmsg(db_));
        throw db_error("Failed to prepare statement");
    }

    request.parameter_text_before_copy(name);
    sqlite3_bind_text(stmt, 1, name.c_str(), -1, SQLITE_TRANSIENT);
    if(stmt)request.columns(stmt);
    bool exists = (step_statement_(stmt) == SQLITE_ROW);
    if(exists)request.row(stmt);
    issued.reset();entry.release();

    return exists;
}

std::unordered_map<std::string, std::string> database::get_table_info(const std::string& table) const {
    require_creator_();
    std::unordered_map<std::string, std::string> columns;

    detail::database_sql_request request(const_cast<database&>(*this),detail::database_sql_read_kind::table_info,{},{});
    if(table.size()>UINT64_MAX-sizeof("PRAGMA table_info()"))throw db_error("table_info SQL byte overflow");
    request.sql_before_copy(uint64_t(table.size())+sizeof("PRAGMA table_info()")-1);
    std::string sql = "PRAGMA table_info(" + table + ")";
    sql_entry_scope entry(const_cast<database&>(*this));
    engine_query_scope issued(const_cast<database&>(*this),false,&request);
    sqlite3_stmt* stmt = nullptr;

    int rc = prepare_catalog_checked_(sql.c_str(), -1, &stmt, nullptr,&request);
    issued.adopt(stmt);
    if(stmt)request.columns(stmt);
    if (rc != SQLITE_OK) {
        LOG_ERROR("db", "Failed to prepare table_info statement: %s", sqlite3_errmsg(db_));
        throw db_error("Failed to prepare table_info statement");
    }

    // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
    while (step_statement_(stmt) == SQLITE_ROW) {
        request.row(stmt);
        const char* name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
        const char* type = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));

        if (name && type) {
            // Normalize type to uppercase for comparison
            std::string type_str(type);
            for (char& c : type_str) {
                c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
            }
            columns[name] = type_str;
        }
    }

    issued.reset();entry.release();
    return columns;
}

void database::create_table(const table_schema& schema) {
    std::ostringstream sql;
    sql << "CREATE TABLE " << schema.name << " (";

    bool first = true;
    if (!schema.is_link_table) {
        // Regular tables get id and globalId
        sql << "id INTEGER PRIMARY KEY AUTOINCREMENT, ";
        sql << "globalId TEXT UNIQUE NOT NULL";
        first = false;
    }

    for (const auto& col : schema.columns) {
        // Skip id and globalId - they're already added above for non-link tables
        if (!schema.is_link_table && (col.name == "id" || col.name == "globalId")) {
            continue;
        }
        if (!first) sql << ", ";
        sql << col.name << " ";
        first = false;

        switch (col.type) {
            case column_type::integer: sql << "INTEGER"; break;
            case column_type::real: sql << "REAL"; break;
            case column_type::text: sql << "TEXT"; break;
            case column_type::blob: sql << "BLOB"; break;
        }

        if (!col.nullable) {
            sql << " NOT NULL";
        }
        if (col.is_unique) {
            sql << " UNIQUE";
        }
        if (col.foreign_key_table) {
            sql << " REFERENCES " << *col.foreign_key_table
                << "(" << col.foreign_key_column.value_or("id") << ")";
        }
    }

    sql << ")";
    execute(sql.str());
}

void database::ensure_table(const table_schema& schema) {
    if (!table_exists(schema.name)) {
        create_table(schema);
    }
}

int database::finalize_mutation_statement_(sqlite3_stmt* statement) noexcept {
    return finalize_mutation_statement_impl_(statement, SQLITE_OK, {});
}

int database::finalize_failed_mutation_statement_(sqlite3_stmt* statement, int step_result,
                                                  const std::exception_ptr& first) noexcept {
    return finalize_mutation_statement_impl_(statement, step_result, first);
}

int database::finalize_mutation_statement_impl_(sqlite3_stmt* statement, int step_result,
                                                const std::exception_ptr& first) noexcept {
    auto* scope = mutation_sql_scope::current_;
    if (scope && scope->creator_ != detail::database_native_process()) return SQLITE_MISUSE;
    auto* connection = statement ? sqlite3_db_handle(statement) : nullptr;
    const int result = ::lattice::detail::store_physical_finalize(statement); // consumes even on error, exactly once
    if (scope && scope->creator_ != detail::database_native_process()) return result;
    if (result != SQLITE_OK) {
        for (auto* active = scope; active; active = active->previous_) {
            if (active->connection_ != connection || active->finalization_failure_) continue;
            // SQLite finalize returns the previous failed evaluation's code.
            // Reuse ONLY that exact statement's already retained primary for
            // an exact error-code match; never mask primary/extended codes.
            // The latch still forbids COMMIT if a body catches the failure.
            if (first && result == step_result && step_result != SQLITE_OK &&
                step_result != SQLITE_ROW && step_result != SQLITE_DONE)
                active->finalization_failure_ = first;
            else {
                try { throw db_error("mutation statement finalization returned an error"); }
                catch (...) { active->finalization_failure_ = std::current_exception(); }
            }
            break;
        }
    }
    return result;
}

void database::bind_value(sqlite3_stmt* stmt, int index, const column_value_t& value) {
    if (mutation_sql_scope::active_for(this)) {
        if (bind_value_checked_(stmt, index, value) != SQLITE_OK)
            throw db_error("mutation parameter binding failed");
        return;
    }
    require_creator_();
    catalog_native_admission_ admission(*this);
    std::visit([&](auto&& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::nullptr_t>) {
            sqlite3_bind_null(stmt, index);
        } else if constexpr (std::is_same_v<T, int64_t>) {
            sqlite3_bind_int64(stmt, index, v);
        } else if constexpr (std::is_same_v<T, double>) {
            sqlite3_bind_double(stmt, index, v);
        } else if constexpr (std::is_same_v<T, std::string>) {
            // Preserve every UTF-8 byte, including embedded NULs. Keep the
            // existing unchecked bind-return policy; error custody is separate.
            sqlite3_bind_text64(stmt, index, v.c_str(),
                                static_cast<sqlite3_uint64>(v.size()),
                                SQLITE_TRANSIENT, SQLITE_UTF8);
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            if (v.empty()) {
                sqlite3_bind_zeroblob(stmt, index, 0);
            } else {
                sqlite3_bind_blob(stmt, index, v.data(), static_cast<int>(v.size()), SQLITE_TRANSIENT);
            }
        }
    }, value);
}

int database::bind_value_checked_(sqlite3_stmt* stmt, int index, const column_value_t& value) {
    require_creator_();
    catalog_native_admission_ admission(*this);
    return std::visit([&](auto&& v) -> int {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::nullptr_t>) {
            return sqlite3_bind_null(stmt, index);
        } else if constexpr (std::is_same_v<T, int64_t>) {
            return sqlite3_bind_int64(stmt, index, v);
        } else if constexpr (std::is_same_v<T, double>) {
            return sqlite3_bind_double(stmt, index, v);
        } else if constexpr (std::is_same_v<T, std::string>) {
            // Preserve every UTF-8 byte, including embedded NULs, and retain
            // this actual checked bind result for the owning operation.
            return sqlite3_bind_text64(stmt, index, v.c_str(),
                                static_cast<sqlite3_uint64>(v.size()),
                                SQLITE_TRANSIENT, SQLITE_UTF8);
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            if (v.empty()) {
                return sqlite3_bind_zeroblob(stmt, index, 0);
            } else {
                return sqlite3_bind_blob(stmt, index, v.data(), static_cast<int>(v.size()), SQLITE_TRANSIENT);
            }
        }
        return SQLITE_MISMATCH;
    }, value);
}

column_value_t database::extract_column(sqlite3_stmt* stmt, int index) {
    require_creator_();
    catalog_native_admission_ admission(*this);
    int type = sqlite3_column_type(stmt, index);
    switch (type) {
        case SQLITE_INTEGER:
            return sqlite3_column_int64(stmt, index);
        case SQLITE_FLOAT:
            return sqlite3_column_double(stmt, index);
        case SQLITE_TEXT: {
            const char* text = reinterpret_cast<const char*>(sqlite3_column_text(stmt, index));
            if (!text) return std::string{};  // Preserve conversion-failure fallback.
            const int size = sqlite3_column_bytes(stmt, index);
            return std::string(text, static_cast<size_t>(size));
        }
        case SQLITE_BLOB: {
            const void* data = sqlite3_column_blob(stmt, index);
            int size = sqlite3_column_bytes(stmt, index);
            const uint8_t* bytes = static_cast<const uint8_t*>(data);
            return std::vector<uint8_t>(bytes, bytes + size);
        }
        case SQLITE_NULL:
        default:
            return nullptr;
    }
}

primary_key_t database::insert(const std::string& table,
                               const std::vector<std::pair<std::string, column_value_t>>& values,
                               const std::vector<std::string>& conflict_columns) {
    require_creator_();
    if ((closed_.load(std::memory_order_acquire) && !mutation_sql_scope::active_for(this))) return {};
    g_statement_count.fetch_add(1, std::memory_order_relaxed);
    ++t_statement_count;
    std::ostringstream sql;
    sql << "INSERT INTO main." << table << " (";

    bool first = true;
    for (const auto& [col, _] : values) {
        if (!first) sql << ", ";
        sql << col;
        first = false;
    }

    sql << ") VALUES (";
    first = true;
    for (size_t i = 0; i < values.size(); ++i) {
        if (!first) sql << ", ";
        sql << "?";
        first = false;
    }
    sql << ")";

    // Add ON CONFLICT clause for upsert if conflict_columns provided
    if (!conflict_columns.empty()) {
        sql << " ON CONFLICT (";
        first = true;
        for (const auto& col : conflict_columns) {
            if (!first) sql << ", ";
            sql << col;
            first = false;
        }
        sql << ")";
        std::ostringstream set_clause;
        first = true;
        for (const auto& [col, _] : values) {
            // Skip conflict columns and globalId in UPDATE
            if (col == "globalId") continue;
            bool is_conflict = false;
            for (const auto& cc : conflict_columns) {
                if (cc == col) { is_conflict = true; break; }
            }
            if (is_conflict) continue;
            if (!first) set_clause << ", ";
            set_clause << col << " = excluded." << col;
            first = false;
        }
        if (first) {
            // No columns to update — every non-globalId column is part of the conflict key.
            sql << " DO NOTHING";
        } else {
            sql << " DO UPDATE SET " << set_clause.str();
        }
        // Truthful row identity for upserts. last_insert_rowid() is NOT
        // updated when the DO UPDATE path runs — it keeps the rowid of the
        // last unrelated INSERT on this connection, silently binding the
        // caller's object to the wrong row. RETURNING reports the rowid of
        // the row actually inserted OR updated; DO NOTHING returns no row,
        // which we surface as 0 so the caller can look the row up by its
        // conflict key.
        sql << " RETURNING rowid";
    }

    sqlite3_stmt* stmt = nullptr;
    int rc = prepare_catalog_checked_(sql.str().c_str(), -1, &stmt, nullptr);
    std::unique_ptr<sqlite3_stmt, decltype(&finalize_mutation_statement_)> issued(stmt, finalize_mutation_statement_);
    if (rc != SQLITE_OK) {
        LOG_ERROR("db", "Failed to prepare insert: %s", sqlite3_errmsg(db_));
        throw db_error("Failed to prepare insert: " + std::string(sqlite3_errmsg(db_)));
    }

    int index = 1;
    for (const auto& [_, val] : values) {
        bind_value(stmt, index++, val);
    }

    rc = step_statement_(stmt);

    if (!conflict_columns.empty()) {
        primary_key_t affected_rowid = 0;
        if (rc == SQLITE_ROW) {
            affected_rowid = sqlite3_column_int64(stmt, 0);
            rc = step_statement_(stmt);  // drain RETURNING
        }
        if (rc != SQLITE_DONE) {
            int extended_rc = sqlite3_extended_errcode(db_);
            auto err = std::string(sqlite3_errmsg(db_));
            LOG_ERROR("db", "Upsert failed (rc=%d, ext=%d, db=%p, path=%s): %s",
                      rc, extended_rc, (void*)db_, path_.c_str(), err.c_str());
            detail::native_retirement_storage<std::exception_ptr> primary;
            require_creator_(); // allocation may have continued in a child
            try { throw db_error("Insert failed: " + err); }
            catch (const db_error&) { primary.get<0>() = std::current_exception(); }
            require_creator_();
            finalize_failed_mutation_statement_(issued.release(), rc, primary.get<0>());
            require_creator_(); // no inherited original access after finalizer callbacks
            discard_if_rolled_back();
            std::rethrow_exception(primary.get<0>());
        }
        finalize_mutation_statement_(issued.release());
        drain_if_settled();
        return affected_rowid;
    }

    if (rc != SQLITE_DONE) {
        int extended_rc = sqlite3_extended_errcode(db_);
        auto err = std::string(sqlite3_errmsg(db_));
        LOG_ERROR("db", "Insert failed (rc=%d, ext=%d, db=%p, path=%s): %s",
                  rc, extended_rc, (void*)db_, path_.c_str(), err.c_str());
        detail::native_retirement_storage<std::exception_ptr> primary;
        require_creator_(); // allocation may have continued in a child
        try { throw db_error("Insert failed: " + err); }
        catch (const db_error&) { primary.get<0>() = std::current_exception(); }
        require_creator_();
        finalize_failed_mutation_statement_(issued.release(), rc, primary.get<0>());
        require_creator_(); // no inherited original access after finalizer callbacks
        discard_if_rolled_back();
        std::rethrow_exception(primary.get<0>());
    }
    finalize_mutation_statement_(issued.release());

    // Capture the rowid BEFORE draining: the drain runs observer callbacks,
    // whose own writes would clobber last_insert_rowid on this connection.
    auto rowid = sqlite3_last_insert_rowid(db_);
    drain_if_settled();
    return rowid;
}

void database::update(const std::string& table,
                       primary_key_t id,
                       const std::vector<std::pair<std::string, column_value_t>>& values) {
    require_creator_();
    if ((closed_.load(std::memory_order_acquire) && !mutation_sql_scope::active_for(this))) return;
    if (values.empty()) return;
    g_statement_count.fetch_add(1, std::memory_order_relaxed);
    ++t_statement_count;

    std::ostringstream sql;
    sql << "UPDATE " << table << " SET ";

    bool first = true;
    for (const auto& [col, _] : values) {
        if (!first) sql << ", ";
        sql << col << " = ?";
        first = false;
    }

    sql << " WHERE id = ?";

    sqlite3_stmt* stmt = nullptr;
    int rc = prepare_catalog_checked_(sql.str().c_str(), -1, &stmt, nullptr);
    std::unique_ptr<sqlite3_stmt, decltype(&finalize_mutation_statement_)> issued(stmt, finalize_mutation_statement_);
    if (rc != SQLITE_OK) {
        auto error = std::string(sqlite3_errmsg(db_));
        LOG_ERROR("db", "Failed to prepare update: %s", error.c_str());
        throw db_error("Failed to prepare update: " + error);
    }

    int index = 1;
    for (const auto& [_, val] : values) {
        bind_value(stmt, index++, val);
    }
    sqlite3_bind_int64(stmt, index, id);

    rc = step_statement_(stmt);
    if (rc != SQLITE_DONE) {
        const auto errmsg = std::string(sqlite3_errmsg(db_));
        LOG_ERROR("db", "Update failed: %s", errmsg.c_str());
        detail::native_retirement_storage<std::exception_ptr> primary;
        require_creator_(); // allocation may have continued in a child
        try { throw db_error("Update failed: " + errmsg); }
        catch (const db_error&) { primary.get<0>() = std::current_exception(); }
        require_creator_();
        finalize_failed_mutation_statement_(issued.release(), rc, primary.get<0>());
        require_creator_(); // no inherited original access after finalizer callbacks
        discard_if_rolled_back();
        std::rethrow_exception(primary.get<0>());
    }
    finalize_mutation_statement_(issued.release());
    drain_if_settled();
}

void database::remove(const std::string& table, primary_key_t id) {
    require_creator_();
    if ((closed_.load(std::memory_order_acquire) && !mutation_sql_scope::active_for(this))) return;
    g_statement_count.fetch_add(1, std::memory_order_relaxed);
    ++t_statement_count;
    std::string sql = "DELETE FROM " + table + " WHERE id = ?";

    sqlite3_stmt* stmt = nullptr;
    int rc = prepare_catalog_checked_(sql.c_str(), -1, &stmt, nullptr);
    std::unique_ptr<sqlite3_stmt, decltype(&finalize_mutation_statement_)> issued(stmt, finalize_mutation_statement_);
    if (rc != SQLITE_OK) {
        LOG_ERROR("db", "Failed to prepare delete: %s", sqlite3_errmsg(db_));
        throw db_error("Failed to prepare delete: " + std::string(sqlite3_errmsg(db_)));
    }

    sqlite3_bind_int64(stmt, 1, id);
    rc = step_statement_(stmt);
    if (rc != SQLITE_DONE) {
        const auto errmsg = std::string(sqlite3_errmsg(db_));
        LOG_ERROR("db", "Delete failed: %s", errmsg.c_str());
        detail::native_retirement_storage<std::exception_ptr> primary;
        require_creator_(); // allocation may have continued in a child
        try { throw db_error("Delete failed: " + errmsg); }
        catch (const db_error&) { primary.get<0>() = std::current_exception(); }
        require_creator_();
        finalize_failed_mutation_statement_(issued.release(), rc, primary.get<0>());
        require_creator_(); // no inherited original access after finalizer callbacks
        discard_if_rolled_back();
        std::rethrow_exception(primary.get<0>());
    }
    finalize_mutation_statement_(issued.release());
    drain_if_settled();
}

database::engine_query_scope::engine_query_scope(database& db, bool defer_delivery, detail::database_sql_request* requested)
    : physical_scope_(db.physical_open_original_,(db.require_creator_(),db.db_)), owner(db), request(requested), previous(current), defers_delivery(defer_delivery) {
    if(!physical_scope_.entered())throw db_error("resource Native caller original unavailable");
    owner.require_creator_();current = this; }

database::engine_query_scope::~engine_query_scope() noexcept { reset(); }

bool database::engine_query_scope::active_for(const database& db) noexcept {
    for (auto* scope = current; scope; scope = scope->previous)
        if (&scope->owner == &db) return true;
    return false;
}

bool database::engine_query_scope::defers_delivery_for(const database& db) noexcept {
    for (auto* scope = current; scope; scope = scope->previous)
        if (&scope->owner == &db && scope->defers_delivery) return true;
    return false;
}

void database::engine_query_scope::adopt(sqlite3_stmt* value) {
    statement = value;
    if(!owner.creator_current_())return;
    if(request&&statement)request->adopt(statement);
    auto* mutex = sqlite3_db_mutex(owner.db_);
    sqlite3_mutex_enter(mutex);
    if (statement && sqlite3_stmt_readonly(statement) && sqlite3_column_count(statement) > 0) {
        next_read = owner.engine_reads_;
        owner.engine_reads_ = this;
        registered = true;
    }
    sqlite3_mutex_leave(mutex);
}

void database::engine_query_scope::reset() noexcept {
    if (!active) return;
    if((request&&!request->current())||!owner.creator_current_()){statement=nullptr;registered=false;current=previous;active=false;return;}
    auto* mutex = sqlite3_db_mutex(owner.db_);
    sqlite3_mutex_enter(mutex);
    if (registered) {
        auto** link = &owner.engine_reads_;
        while (*link && *link != this) link = &(*link)->next_read;
        if (*link == this) *link = next_read;
        registered = false;
    }
    try {detail::database_sql_request::binding requested(owner,request,statement,true);
        finalization_result=finalize_mutation_statement_(statement);
    }catch(...) {finalization_result=finalize_mutation_statement_(statement);}
    // Includes exceptional prepare/bind/row-copy unwind, before Request dies.
    // Non-initialization requests perform no additional action here.
    if(request)request->note_initialization_api_error_(finalization_result,true);
    owner.note_core_transaction_end_locked_();
    statement = nullptr;
    sqlite3_mutex_leave(mutex);
    current = previous;
    active = false;
}

std::vector<database::row_t> database::query(const std::string& sql,
                                             const std::vector<column_value_t>& params) {
    const auto kind=(sql=="PRAGMA busy_timeout"||sql=="PRAGMA busy_timeout;")?
        detail::database_sql_read_kind::busy_timeout_getter:detail::database_sql_read_kind::diagnostic;
    return query_requested_(sql,params,kind,nullptr);
}
std::vector<database::row_t> database::query_requested_(const std::string& sql,
    const std::vector<column_value_t>& params,detail::database_sql_read_kind kind,
    const detail::database_sql_read_limits* limits) {
    require_creator_();
    detail::database_sql_request request(*this,kind,sql,params,limits);
    g_statement_count.fetch_add(1, std::memory_order_relaxed);
    ++t_statement_count;
    if (closed_.load(std::memory_order_acquire) && !maintenance_scope::active_for(db_) && !mutation_sql_scope::active_for(this) && !core_settlement_scope::active_for(this)) return {};
    // Offer the fresh turn before this query publishes its own engine scope.
    // The later prepare/step entries then recognize real nested ownership.
    sql_entry_scope entry(*this);
    engine_query_scope statement(*this,false,&request);
    sqlite3_stmt* stmt = nullptr;
    int rc = prepare_catalog_checked_(sql.c_str(), -1, &stmt, nullptr,&request);
    statement.adopt(stmt);
    if (rc != SQLITE_OK) {
        auto errmsg = sqlite3_errmsg(db_);
        LOG_ERROR("db", "%s in %s", errmsg, sql.c_str());
        std::cerr<<"db: "<<errmsg<<" "<<sql.c_str()<<std::endl;
        throw db_error("Failed to prepare query: " + std::string(errmsg));
    }

    int index = 1;
    for (const auto& param : params) {
        request.parameter_before_copy(param);
        bind_value(stmt, index++, param);
    }

    std::vector<row_t> results;
    int col_count = sqlite3_column_count(stmt);

    // Capture column names ONCE, before stepping. sqlite3_column_name
    // materializes the name lazily on first access and returns NULL if that
    // allocation fails — under system memory pressure (Apple's purgeable
    // page cache purging mid-scan) this is a real, observed failure, and
    // constructing the row-map key from NULL is a segfault. Names are stable
    // for the life of the statement, so per-row capture was also wasted work.
    request.columns(stmt);
    std::vector<std::string> col_names;
    col_names.reserve(static_cast<size_t>(col_count));
    for (int i = 0; i < col_count; ++i) {
        const char* name = sqlite3_column_name(stmt, i);
        if (!name) {
            statement.reset();
            LOG_ERROR("db", "column_name OOM in %s", sql.c_str());
            throw db_error("Query failed: out of memory reading column name");
        }
        col_names.emplace_back(name);
    }

    while ((rc = step_statement_(stmt)) == SQLITE_ROW) {
        detail::recovery_admission_test_hooks::read_row(*this, stmt);
        request.row(stmt);
        row_t row;
        for (int i = 0; i < col_count; ++i) {
            row[col_names[static_cast<size_t>(i)]] = extract_column(stmt, i);
        }
        results.push_back(std::move(row));
    }

    statement.reset();
    entry.release(); // Finalized and off Native SQLite before callbacks.

    if (rc != SQLITE_DONE) {
        auto error = std::string(sqlite3_errmsg(db_));
        LOG_ERROR("db", "Query failed: %s", error.c_str());
        discard_if_rolled_back();
        throw db_error("Query failed: " + error);
    }

    // A plain SELECT can't close a transaction, but DML-via-RETURNING issued
    // through query() can — one relaxed load of insurance (see design doc).
    drain_if_settled();
    return results;
}

std::optional<column_value_t> database::query_managed_cell(
    const std::string& sql, const std::string& column, primary_key_t row_id) {
    require_creator_();
    g_statement_count.fetch_add(1, std::memory_order_relaxed);
    ++t_statement_count;
    if (closed_.load(std::memory_order_acquire)) return std::nullopt;

    detail::database_sql_request request(*this,detail::database_sql_read_kind::managed_cell,sql,{});
    sql_entry_scope entry(*this);
    engine_query_scope statement(*this,false,&request);
    sqlite3_stmt* raw = nullptr;
    int rc = prepare_catalog_checked_(sql.c_str(), -1, &raw, nullptr,&request);
    statement.adopt(raw);
    if (rc != SQLITE_OK) {
        auto errmsg = sqlite3_errmsg(db_);
        LOG_ERROR("db", "%s in %s", errmsg, sql.c_str());
        std::cerr << "db: " << errmsg << " " << sql.c_str() << std::endl;
        throw db_error("Failed to prepare query: " + std::string(errmsg));
    }
    request.parameter_before_copy(column_value_t{row_id});
    bind_value(raw, 1, row_id);

    // Normal primitive getters select one column. Retain the old name-match
    // behavior (including its last-duplicate-name rule) for manually assigned
    // field SQL instead of silently treating a different result as the field.
    request.columns(raw);
    int value_index = -1;
    const int column_count = sqlite3_column_count(raw);
    for (int i = 0; i < column_count; ++i) {
        const char* name = sqlite3_column_name(raw, i);
        if (!name) {
            LOG_ERROR("db", "column_name OOM in %s", sql.c_str());
            throw db_error("Query failed: out of memory reading column name");
        }
        if (column == name) value_index = i;
    }

    std::optional<column_value_t> value;
    bool first_row = true;
    while ((rc = step_statement_(raw)) == SQLITE_ROW) {
        detail::recovery_admission_test_hooks::read_row(*this, raw);
        request.row(raw,first_row&&value_index>=0,value_index);
        if (first_row && value_index >= 0) value = extract_column(raw, value_index);
        first_row = false;
    }
    // Release the read statement before invoking any settled callback. Keep
    // the same completion/error policy as query(); the RAII owner also covers
    // allocation or conversion exceptions before normal completion.
    statement.reset();
    entry.release(); // A settled callback starts a fresh public entry.
    if (rc != SQLITE_DONE) {
        auto error = std::string(sqlite3_errmsg(db_));
        LOG_ERROR("db", "Query failed: %s", error.c_str());
        discard_if_rolled_back();
        throw db_error("Query failed: " + error);
    }
    drain_if_settled();
    return value;
}

void database::refresh_wal_snapshot() {
    require_creator_();
    catalog_native_admission_ admission(*this);
    // A no-op SELECT forces SQLite to release the old WAL read snapshot
    // and acquire a fresh one on the next query.
    ::lattice::detail::store_physical_exec(db_, "SELECT 1", nullptr, nullptr, nullptr);
}

void database::interrupt() {
    require_creator_();
    detail::store_physical_native_scope physical_call(*this);
    if(!physical_call.entered())throw db_error("resource Native caller original unavailable");
    if (db_) sqlite3_interrupt(db_);
}

database::checkpoint_result database::wal_checkpoint(bool truncate, int busy_budget_ms) {
    require_creator_();
    detail::store_physical_native_scope physical_call(*this);
    if(!physical_call.entered())throw db_error("resource Native caller original unavailable");
    checkpoint_result result;
#ifdef __EMSCRIPTEN__
    // DELETE journal mode — there is no WAL to checkpoint.
    (void)truncate; (void)busy_budget_ms;
    return result;
#else
    if (closed_.load(std::memory_order_acquire) || mode_ != open_mode::read_write || !db_) {
        return result;
    }
    auto* connection = db_; // Owner lifetime/move contract is unchanged.
    {
        // FULLMUTEX alone serializes individual calls, not a PRAGMA's ROW →
        // next-step/finalize gap. A paused checkpoint PRAGMA is a writing VM
        // and makes a concurrent ACK COMMIT fail with "SQL statements in
        // progress". Use the native operation, and own the timeout mutation
        // through restoration too. db.cpp is compiled with SQLITE_CORE, so
        // sqlite3ext.h does not replace this API with an extension thunk.
        struct checkpoint_scope {
            sqlite3* connection;
            sqlite3_mutex* mutex;
            int restore_timeout;
            bool policy_changed = false;
            checkpoint_scope(sqlite3* db, int timeout) noexcept
                : connection(db), mutex(sqlite3_db_mutex(db)), restore_timeout(timeout) {
                sqlite3_mutex_enter(mutex);
            }
            ~checkpoint_scope() noexcept {
                if (policy_changed) sqlite3_busy_timeout(connection, restore_timeout);
                sqlite3_mutex_leave(mutex);
            }
        } scope(connection, busy_timeout_ms_);
        require_catalog_clean_locked_();
        scope.policy_changed = true;

        record_statement(); // Preserve the prior one-PRAGMA accounting.
        sqlite3_busy_timeout(connection, truncate ? busy_budget_ms : 0);
        int log_frames = -1, checkpointed = -1;
        // nullptr preserves the unqualified PRAGMA's all-attached-schema scope
        // and first-schema frame counters; do not silently narrow to "main".
        const int rc = ::lattice::detail::store_physical_wal_checkpoint(connection, nullptr,
            truncate ? SQLITE_CHECKPOINT_TRUNCATE : SQLITE_CHECKPOINT_PASSIVE,
            &log_frames, &checkpointed);
        if (rc == SQLITE_OK || rc == SQLITE_BUSY) {
            // PRAGMA represents BUSY as a successful result row with busy=1.
            result.rc = SQLITE_OK;
            result.busy = rc == SQLITE_BUSY ? 1 : 0;
            result.log_frames = log_frames;
            result.checkpointed = checkpointed;
        } else {
            // The old query path threw and left all frame values unavailable.
            result.rc = SQLITE_ERROR;
            LOG_DEBUG("db", "wal_checkpoint(%s) failed: rc=%d, path=%s",
                      truncate ? "TRUNCATE" : "PASSIVE", rc, path_.c_str());
        }
        detail::checkpoint_test_probe::fire(connection,
            detail::checkpoint_probe_stage::result_captured);
    }
    if (truncate && result.busy != 0) {
        // Preserve the existing retirement request, after releasing this method's
        // mutex ownership and restoring the timeout. A caller's pre-existing
        // recursive mutex level/callback contract is unchanged. This is not a
        // successful-checkpoint or authoritative history-epoch signal; a later
        // retry may truncate.
        detail::checkpoint_test_probe::fire(connection,
            detail::checkpoint_probe_stage::before_generation_retirement);
        try { retire_projection_store(physical_identity()); } catch (...) {}
    }
    return result;
#endif
}

bool database::maintenance_scope::idle(database& db) noexcept {
    if(!db.creator_current_())return false;
    if (!db.db_ || db.closed_.load(std::memory_order_acquire) ||
        sqlite3_get_autocommit(db.db_) == 0 ||
        sqlite3_txn_state(db.db_, nullptr) != SQLITE_TXN_NONE ||
        update_hook_scope::active_for(db.db_)) return false;
    for (auto* statement = sqlite3_next_stmt(db.db_, nullptr);
         statement; statement = sqlite3_next_stmt(db.db_, statement)) {
        if (sqlite3_stmt_busy(statement)) return false;
    }
    return true;
}

void database::maintenance_scope::probe_before_store_gate(database& db) {
    db.require_creator_();
    detail::recovery_admission_test_hooks::route_maintenance_entered();
    if (!try_probe_before_store_gate(db)) {
        detail::recovery_admission_test_hooks::route_maintenance(
            detail::recovery_admission_test_hooks::route_maintenance_point::mutex_busy);
        throw db_error("audit maintenance connection is busy");
    }
    detail::recovery_admission_test_hooks::route_maintenance(
        detail::recovery_admission_test_hooks::route_maintenance_point::passed);
}

bool database::maintenance_scope::try_probe_before_store_gate(database& db) {
    db.require_creator_();
    detail::store_physical_native_scope physical_call(db);
    if(!physical_call.entered())throw db_error("resource Native caller original unavailable");
    auto* mutex = db.db_ ? sqlite3_db_mutex(db.db_) : nullptr;
#ifndef __EMSCRIPTEN__
    if (!mutex) throw db_error("audit maintenance requires a serialized connection");
#endif
    // A SQLite callback may already own this recursive mutex while another
    // writer owns the shared-memory gate and is waiting for SQLite. Reject
    // callback/statement reentry BEFORE waiting for that gate. This probe
    // acquires no gate and never waits for another SQLite thread.
    if (sqlite3_mutex_try(mutex) != SQLITE_OK) return false;
    bool available;
    try { db.require_catalog_clean_locked_(); available = idle(db); }
    catch (...) { sqlite3_mutex_leave(mutex); throw; }
    sqlite3_mutex_leave(mutex);
    if (!available) throw db_error("audit maintenance requires an idle connection");
    return true;
}

database::maintenance_scope::admission database::maintenance_scope::controller_classify_locked(database& db,
    const std::atomic<std::thread::id>* core_transaction) {
    db.require_creator_();
    db.require_catalog_clean_locked_();
    if (engine_query_scope::active_for(db) || active_for(db.db_) || update_hook_scope::active_for(db.db_))
        throw db_error("controller maintenance cannot reenter a connection callback");
    if (idle(db)) return admission::ready;
    // The owning caller retains this exact writer/parent and revalidates them
    // before accepting a deferral. Only a real Core BEGIN publishes this stamp
    // while holding this same SQLite mutex. This is denial-only provenance:
    // never use it to read, join, commit, roll back or grant a foreign turn.
    if (core_transaction && db.db_ && !db.closed_.load(std::memory_order_acquire) &&
        !db.raw_handle_escaped_.load(std::memory_order_acquire) &&
        sqlite3_get_autocommit(db.db_) == 0 && sqlite3_txn_state(db.db_, "main") == SQLITE_TXN_WRITE) {
        const auto thread = core_transaction->load(std::memory_order_acquire);
        if (thread != std::thread::id{} && thread == db.core_transaction_thread_ &&
            db.core_transaction_origin_live_locked_() && thread != std::this_thread::get_id())
            return admission::core_write_busy;
    }
    if (!db.db_ || db.closed_.load(std::memory_order_acquire) ||
        sqlite3_get_autocommit(db.db_) == 0 || sqlite3_txn_state(db.db_, nullptr) == SQLITE_TXN_WRITE)
        throw db_error("audit maintenance requires an idle connection");
    bool found = false;
    for (auto* statement = sqlite3_next_stmt(db.db_, nullptr); statement;
         statement = sqlite3_next_stmt(db.db_, statement)) {
        if (!sqlite3_stmt_busy(statement)) continue;
        bool engine_read = false;
        for (auto* scope = db.engine_reads_; scope; scope = scope->next_read)
            if (scope->statement == statement && scope->thread != std::this_thread::get_id()) {
                engine_read = true; break;
            }
        if (!engine_read || !sqlite3_stmt_readonly(statement))
            throw db_error("audit maintenance requires an idle connection");
        found = true;
    }
    if (!found) throw db_error("audit maintenance requires an idle connection");
    return admission::engine_read_busy;
}

database::maintenance_scope::admission database::maintenance_scope::controller_probe(database& db,
    const std::atomic<std::thread::id>* core_transaction) {
    db.require_creator_();
    detail::store_physical_native_scope physical_call(db);
    if(!physical_call.entered())throw db_error("resource Native caller original unavailable");
    if (engine_query_scope::active_for(db) || active_for(db.db_) || update_hook_scope::active_for(db.db_))
        throw db_error("controller maintenance cannot reenter a connection callback");
    auto* mutex = db.db_ ? sqlite3_db_mutex(db.db_) : nullptr;
#ifndef __EMSCRIPTEN__
    if (!mutex) throw db_error("audit maintenance requires a serialized connection");
#endif
    if (sqlite3_mutex_try(mutex) != SQLITE_OK) return admission::mutex_busy;
    try {
        const auto result = controller_classify_locked(db, core_transaction);
        sqlite3_mutex_leave(mutex);
        return result;
    } catch (...) { sqlite3_mutex_leave(mutex); throw; }
}

database::maintenance_scope::maintenance_scope(database& db)
    : maintenance_scope(db, nullptr) {}

database::maintenance_scope::maintenance_scope(database& db, admission* controller_admission,
    const std::atomic<std::thread::id>* core_transaction)
    : physical_scope_(db.physical_open_original_,(db.require_creator_(),db.db_)), owner(db), mutex(physical_scope_.entered()?(db.require_creator_(), db.db_ ? sqlite3_db_mutex(db.db_) : nullptr):nullptr) {
    if(!physical_scope_.entered())throw db_error("resource Native caller original unavailable");
#ifndef __EMSCRIPTEN__
    if (!mutex) throw db_error("audit maintenance requires a serialized connection");
#endif
    if (controller_admission) {
        if (engine_query_scope::active_for(db) || active_for(db.db_) || update_hook_scope::active_for(db.db_))
            throw db_error("controller maintenance cannot reenter a connection callback");
        if (sqlite3_mutex_try(mutex) != SQLITE_OK) {
            *controller_admission = admission::mutex_busy;
            return;
        }
        try { *controller_admission = controller_classify_locked(db, core_transaction); }
        catch (...) { sqlite3_mutex_leave(mutex); throw; }
        if (*controller_admission != admission::ready) { sqlite3_mutex_leave(mutex); return; }
    } else sqlite3_mutex_enter(mutex);
    try { db.require_catalog_clean_locked_(); }
    catch (...) { sqlite3_mutex_leave(mutex); throw; }
    // Recheck after admission: the pre-gate probe is not an ownership lease.
    // Never wait for another transaction while retaining its owner's mutex.
    if (!idle(db)) {
        sqlite3_mutex_leave(mutex);
        throw db_error("audit maintenance requires an idle connection");
    }
    previous = current;
    current = this;
    entered = true;
}

database::maintenance_scope::~maintenance_scope() noexcept {
    if(!owner.creator_current_()){if(current==this)current=previous;return;}
    if (!entered) return;
    current = previous;
    sqlite3_mutex_leave(mutex);
}

namespace {
// Fixed copied native error: a diagnostic allocation cannot replace an actual
// failed control program before cleanup captures its first exception.
class mutation_control_error final : public std::exception {
    char message_[256]{};
public:
    mutation_control_error(sqlite3* connection, const char* fallback) noexcept {
        const char* source = connection ? sqlite3_errmsg(connection) : fallback;
        if (!source) source = fallback;
        size_t at = 0;
        for (; at + 1 < sizeof(message_) && source[at]; ++at) message_[at] = source[at];
        message_[at] = 0;
    }
    const char* what() const noexcept override { return message_; }
};
}
thread_local database::core_settlement_scope* database::core_settlement_scope::current_ = nullptr;
database::core_settlement_scope::core_settlement_scope(database& owner,
    std::shared_ptr<const core_transaction_original> original, bool capture)
    : physical_scope_(owner.physical_open_original_,(owner.require_creator_(),owner.db_)), owner_(owner), original_(std::move(original)), previous_(current_) {
    if(!physical_scope_.entered())throw db_error("resource Native caller original unavailable");
    owner_.require_creator_();
    const auto* hook = owner_.lattice_update_hook_context_.get();
    if (!original_ || !original_->same_native_for(owner_) ||
        (capture ? (!original_->ended.load(std::memory_order_acquire) || !hook ||
                    !hook->mutation_tail_pending || hook->mutation_original != original_)
                 : !original_->current_for(owner_)))
        throw db_error("Core settlement frame requires its exact original");
    current_ = this;
}
database::core_settlement_scope::~core_settlement_scope() noexcept { current_ = previous_; }
bool database::core_settlement_scope::active_for(const database* owner) noexcept {
    for (auto* frame = current_; frame; frame = frame->previous_)
        if (&frame->owner_ == owner) return true;
    return false;
}
thread_local database::mutation_sql_scope* database::mutation_sql_scope::current_ = nullptr;
bool database::mutation_sql_scope::active_for(const database* owner) noexcept {
    for (auto* frame = current_; frame; frame = frame->previous_)
        if (frame->creator_ == detail::database_native_process() && &frame->owner_ == owner && frame->installed_ && frame->issued_ &&
            (frame->phase_ == phase::active || frame->phase_ == phase::settling)) return true;
    return false;
}
bool database::mutation_sql_scope::same_native_() const noexcept {
    return creator_ == detail::database_native_process() && owner_.creator_current_() && owner_.db_ == connection_ && retirement_ &&
        !retirement_->owning_wrapper_moved_.load(std::memory_order_acquire) &&
        owner_.retirement_.get() == retirement_.get() &&
        !owner_.retirement_.owner_before(retirement_) && !retirement_.owner_before(owner_.retirement_);
}
bool database::mutation_sql_scope::same_transaction_() const noexcept {
    if (!same_native_() || !origin_ || !transaction_ || !transaction_->current_for(owner_) ||
        owner_.core_transaction_original_ != transaction_ ||
        sqlite3_get_autocommit(connection_) != 0 ||
        !core_serial_ || owner_.core_transaction_serial_ != core_serial_ ||
        route_.load(std::memory_order_acquire) != std::this_thread::get_id() ||
        owner_.core_transaction_thread_ != std::this_thread::get_id() ||
        !owner_.core_transaction_origin_current_locked_()) return false;
    const auto actual = owner_.core_transaction_lifetime_.lock();
    return actual == origin_ && !actual.owner_before(origin_) && !origin_.owner_before(actual) &&
        sqlite3_txn_state(connection_, "main") == SQLITE_TXN_WRITE;
}
void database::mutation_sql_scope::require_transaction_() const {
    if (!same_transaction_()) throw db_error("mutation operation lost its physical Core transaction");
}
void database::mutation_sql_scope::observe_end_() noexcept {
    if (!same_native_()) return;
    owner_.note_core_transaction_end_locked_();
    if (sqlite3_get_autocommit(connection_) != 0 &&
        owner_.core_transaction_serial_ == core_serial_) {
        auto actual = std::this_thread::get_id();
        route_.compare_exchange_strong(actual, std::thread::id{}, std::memory_order_acq_rel);
    }
}
void database::mutation_sql_scope::control_(const char* sql) {
    const int rc = owner_.execute_observed_sql_(sql,true);
    observe_end_();
    if (rc != SQLITE_OK) throw mutation_control_error(connection_, "mutation control program failed");
}
bool database::mutation_callback_active_() noexcept {
    return engine_query_scope::current || update_hook_scope::current || maintenance_scope::current ||
        core_settlement_scope::any_active() || attachment_sql_scope::any_active() ||
        detail::managed_route_scope::any_active() || detail::database_sql_dispatcher::callbacks_ ||
        (sql_entry_scope::any_active() && !mutation_sql_scope::current_);
}
bool database::mutation_wait_context_free_() noexcept {
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    if(detail::store_physical_api::managed_return_here_())return false;
#endif
    // Denial-only census of actual Core frames, including a different writer.
    // A raw external SQLite frame on some OTHER connection is not observable:
    // the caller's existing stable-wrapper/no-foreign-leaf contract still applies.
    return !sql_entry_scope::any_active() && !engine_query_scope::current &&
        !update_hook_scope::current && !maintenance_scope::current &&
        !mutation_sql_scope::current_ && !core_settlement_scope::any_active() &&
        !attachment_sql_scope::any_active() && !detail::managed_route_scope::any_active() &&
        !detail::database_sql_dispatcher::callbacks_;
}
namespace {
template<class T> bool mutation_same_owner_(const std::shared_ptr<T>& a,
                                             const std::shared_ptr<T>& b) noexcept {
    return a.get()==b.get() && !a.owner_before(b) && !b.owner_before(a);
}
}
database::mutation_admission_original_::mutation_admission_original_(database& owner,
    bool fresh_entry,std::chrono::steady_clock::time_point entered)
    : owner_(owner), connection_((owner.require_creator_(),owner.db_)),
      physical_cookie_(owner.physical_open_original_.original_),
      dispatcher_(owner.sql_dispatcher_.load(std::memory_order_acquire)),
      retirement_(owner.retirement_),context_(owner.ordinary_context_),reads_(owner.sql_read_binding_),
      startup_(owner.delivery_startup_.get()?owner.delivery_startup_.shared_():nullptr),
      runtime_(owner.observer_delivery_sql_),read_control_(owner.read_control_),
      offer_(detail::observer_delivery_original::observe_offer(owner)),
      deadline_(entered+std::chrono::milliseconds(owner.busy_timeout_ms_>0?std::min(owner.busy_timeout_ms_,2000):0)),
      budget_ms_(owner.busy_timeout_ms_>0?std::min(owner.busy_timeout_ms_,2000):0),
      may_wait_(fresh_entry && owner.mutation_wait_context_free_() &&
          !owner.raw_handle_escaped_.load(std::memory_order_acquire) && budget_ms_>0) {
    // The actual shared writer retained by with_mutation_operation supplies
    // the EXISTING stable-wrapper lifetime contract for memory and file opens.
    // physical_cookie_ is original comparison DATA/lease custody, NOT a pin
    // preventing SQLite close. Concurrent destructive public move/destruction
    // was never supported; neither this check nor physical_scope invents it.
    static_assert(sizeof(mutation_admission_original_)<=512,"fixed admission DATA workspace");
    require_current_();
}
void database::mutation_admission_original_::require_current_(bool native_locked) const {
    if(creator_!=detail::database_native_process())throw db_error("inherited mutation admission original");
    // The retained original's monotone move marker is inspected BEFORE any
    // wrapper metadata. This also refuses a completed same-thread transfer;
    // it does not make destructive concurrent move a supported operation.
    if(!retirement_ || retirement_->owning_wrapper_moved_.load(std::memory_order_acquire))
        throw db_error("mutation originating wrapper moved before admission");
    owner_.require_creator_();
    if(entry_ && !sql_entry_scope::current_is(entry_))
        throw db_error("mutation admission lost its original lexical SQL caller");
    if(!connection_ || owner_.db_!=connection_ || owner_.is_closed() ||
       !mutation_same_owner_(owner_.retirement_,retirement_) ||
       !mutation_same_owner_(owner_.ordinary_context_,context_) ||
       !mutation_same_owner_(owner_.sql_read_binding_,reads_) ||
       !mutation_same_owner_(owner_.observer_delivery_sql_,runtime_) ||
       !mutation_same_owner_(owner_.read_control_,read_control_) ||
       owner_.physical_open_original_.original_!=physical_cookie_ ||
       !owner_.physical_open_original_.matches_open_(connection_) ||
       owner_.sql_dispatcher_.load(std::memory_order_acquire)!=dispatcher_ ||
       owner_.mutation_operation_unsettled_.load(std::memory_order_acquire) ||
       owner_.channel_reset_unsettled_.load(std::memory_order_acquire))
        throw db_error("mutation admission lost its original writer policy");
    const auto* actual_startup=owner_.delivery_startup_.get();
    if(actual_startup!=startup_.get() ||
       (actual_startup && !mutation_same_owner_(owner_.delivery_startup_.shared_(),startup_)))
        throw db_error("mutation admission delivery startup original changed");
    const auto offered=detail::observer_delivery_original::observe_offer(owner_);
    if(!mutation_same_owner_(offered,offer_))
        throw db_error("mutation admission pre-admitted caller original changed");
    if(native_locked && runtime_ && runtime_->native(owner_)!=connection_)
        throw db_error("mutation admission runtime original changed");
    if(may_wait_ && owner_.raw_handle_escaped_.load(std::memory_order_acquire))
        throw db_error("mutation admission cannot wait after Native escape");
    if(native_locked) {
        owner_.require_catalog_clean_locked_();
        if(owner_.txn_hooks_external_)throw db_error("mutation original Native hooks replaced");
    }
}
database::mutation_admission_original_::state
 database::mutation_admission_original_::classify_locked_() const {
    require_current_(true);
    if(may_wait_ && sqlite3_get_autocommit(connection_)==0)
        throw db_error("mutation read wait requires fresh autocommit admission");
    // A complete bounded exact-pointer census is made ONLY while Native is
    // owned. No engine_query_scope/statement pointer escapes this classifier.
    // Limits are admission-only refusal, never permission to ignore a suffix.
    std::array<sqlite3_stmt*,256> owned{};
    std::array<bool,256> seen{};
    size_t count=0;
    for(auto* read=owner_.engine_reads_;read;read=read->next_read) {
        if(count==owned.size())throw db_error("mutation owned-read census capacity exhausted");
        if(!read->registered || !read->active || &read->owner!=&owner_ || !read->statement ||
           read->thread==std::this_thread::get_id())
            throw db_error("mutation operation refuses same-thread or incomplete engine read");
        for(size_t i=0;i<count;++i)if(owned[i]==read->statement)
            throw db_error("mutation owned-read identity is ambiguous");
        owned[count++]=read->statement;
    }
    size_t statements=0;
    for(auto* statement=sqlite3_next_stmt(connection_,nullptr);statement;
        statement=sqlite3_next_stmt(connection_,statement)) {
        if(++statements>4096)throw db_error("mutation Native statement census capacity exhausted");
        size_t at=0;while(at<count && owned[at]!=statement)++at;
        if(at<count) {
            if(seen[at] || !sqlite3_stmt_readonly(statement) || sqlite3_column_count(statement)<=0 ||
               sqlite3_db_handle(statement)!=connection_)
                throw db_error("mutation operation refuses non-readonly owned engine statement");
            seen[at]=true;
        } else if(sqlite3_stmt_busy(statement)) {
            detail::recovery_admission_test_hooks::observe_unregistered_busy(owner_,connection_,statement,count,statements);
            throw db_error("mutation operation refuses unregistered active engine statement");
        }
    }
    for(size_t i=0;i<count;++i)if(!seen[i])
        throw db_error("mutation owned-read statement is absent from actual Native corpus");
    // Include prepared/ROW/DONE-to-finalize owned materializations. A false
    // sqlite3_stmt_busy after DONE does not mean the query owner has ended.
    if(count) {
        if(!may_wait_)throw db_error("mutation operation refuses active engine reentry");
        return state::owned_read;
    }
    return state::ready;
}
database::mutation_admission_original_::state database::mutation_admission_original_::probe_() const {
    require_current_();
    auto* mutex=sqlite3_db_mutex(connection_);
#ifndef __EMSCRIPTEN__
    if(!mutex)throw db_error("mutation admission requires a physical SQLite mutex");
#endif
    if(sqlite3_mutex_try(mutex)!=SQLITE_OK)return state::native_busy;
    struct leave {sqlite3_mutex* mutex;~leave(){sqlite3_mutex_leave(mutex);}} release{mutex};
    return classify_locked_();
}
void database::mutation_admission_original_::check_deadline_() const {
    if(may_wait_ && std::chrono::steady_clock::now()>=deadline_)
        throw db_error("mutation admission busy budget exhausted");
}
void database::mutation_admission_original_::wait_(state actual) {
    require_current_();
    if(!may_wait_ || budget_ms_==0)throw db_error("mutation admission busy; no eligible wait budget");
    check_deadline_();
    // Caller has released every Core-owned/tracked Native/store/ownership hold.
    // Arbitrary unwrapped caller locks are neither enumerable nor released.
    // This passive test observation returns no permission and cannot change
    // lifetime. Native-try BUSY alone never emits an owned-reader observation.
    if(actual==state::owned_read)
        detail::recovery_admission_test_hooks::mutation_owned_read_wait_unlocked(owner_);
    require_current_();check_deadline_();
    const auto now=std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::min(deadline_-now,
        std::chrono::duration_cast<std::chrono::steady_clock::duration>(std::chrono::milliseconds(1))));
    require_current_();check_deadline_();
}
database::mutation_admission_original_::~mutation_admission_original_() noexcept {
    if(creator_==detail::database_native_process())return;
    // An inherited child does not decrement any captured DATA control block.
    new (&retirement_) std::shared_ptr<detail::database_retirement_state>();
    new (&context_) detail::ordinary_context();
    new (&reads_) std::shared_ptr<const detail::database_sql_read_binding>();
    new (&startup_) std::shared_ptr<detail::observer_delivery_open_startup>();
    new (&runtime_) std::shared_ptr<detail::observer_delivery_sql_runtime>();
    new (&read_control_) std::shared_ptr<database_read_control>();
    new (&offer_) std::shared_ptr<const detail::observer_delivery_original>();
}
database::mutation_sql_scope::mutation_sql_scope(database& owner, std::atomic<std::thread::id>& route,
    mutation_admission_original_* admission,mutation_admission_original_::state* actual)
    : physical_scope_(owner.physical_open_original_,(owner.require_creator_(),owner.db_)), owner_(owner), route_(route), retirement_(owner.retirement_) {
    if(!physical_scope_.entered())throw db_error("resource Native caller original unavailable");
    owner_.require_creator_();
    if (!sql_entry_scope::active_for(&owner_) || !owner_.db_ || !retirement_ || owner_.is_closed() ||
        owner_.mutation_operation_unsettled_.load(std::memory_order_acquire) ||
        owner_.channel_reset_unsettled_.load(std::memory_order_acquire) ||
        retirement_->owning_wrapper_moved_.load(std::memory_order_acquire) ||
        engine_query_scope::active_for(owner_) || update_hook_scope::active_for(owner_.db_) ||
        maintenance_scope::active_for(owner_.db_) || core_settlement_scope::active_for(&owner_))
        throw db_error("mutation operation requires a fresh usable SQL admission");
    connection_ = owner_.db_;
    mutex_ = sqlite3_db_mutex(connection_);
#ifndef __EMSCRIPTEN__
    if (!mutex_) throw db_error("mutation operation requires a physical SQLite mutex");
#endif
    if(bool(admission)!=bool(actual))throw db_error("mutation admission result requires its original");
    if(admission) {
        admission->require_current_();
        if(sqlite3_mutex_try(mutex_)!=SQLITE_OK) {
            *actual=mutation_admission_original_::state::native_busy;return;
        }
        locked_=true;
    } else {sqlite3_mutex_enter(mutex_);locked_=true;}
    try {
        owner_.require_catalog_clean_locked_();
        if(owner_.txn_hooks_external_)throw db_error("mutation operation requires a fresh usable SQL admission");
        if (!same_native_() || owner_.is_closed()) throw db_error("mutation operation closed before admission");
        if(admission) {
            *actual=admission->classify_locked_();
            if(*actual!=mutation_admission_original_::state::ready){unlock();return;}
            admission->check_deadline_();
        }
        // Authorizer/UDF callbacks must not recursively start a graph unit on
        // an actual busy engine statement. No callback, prepare or step retry.
        for (auto* statement = sqlite3_next_stmt(connection_, nullptr); statement;
             statement = sqlite3_next_stmt(connection_, statement))
            if (sqlite3_stmt_busy(statement)) throw db_error("mutation operation refuses active engine reentry");
        for (auto* frame = current_; frame; frame = frame->previous_)
            if (&frame->owner_ == &owner_ && frame->phase_ != phase::active)
                throw db_error("mutation operation refuses incomplete parent admission");
        if (owner_.mutation_operation_serial_ == UINT64_MAX)
            throw db_error("mutation operation savepoint identity exhausted");
        const auto serial = ++owner_.mutation_operation_serial_;
        // Fixed namespace, complete decimal serial, no body/user SQL in names.
        std::snprintf(savepoint_, sizeof(savepoint_), "SAVEPOINT __lattice_mutation_%llu", (unsigned long long)serial);
        std::snprintf(rollback_, sizeof(rollback_), "ROLLBACK TO __lattice_mutation_%llu", (unsigned long long)serial);
        std::snprintf(release_, sizeof(release_), "RELEASE __lattice_mutation_%llu", (unsigned long long)serial);
        owns_transaction_ = sqlite3_get_autocommit(connection_) != 0;
        if (!owns_transaction_) {
            // A raw escape prevents proving the caller's intervening history.
            // Even a matching old route/thread stamp cannot repair it.
            if (owner_.raw_handle_escaped_.load(std::memory_order_acquire))
                throw db_error("mutation savepoint refuses escaped caller transaction");
            origin_ = owner_.core_transaction_lifetime_.lock();
            transaction_ = owner_.core_transaction_original_;
            core_serial_ = owner_.core_transaction_serial_;
            require_transaction_();
        } else if (route_.load(std::memory_order_acquire) != std::thread::id{}) {
            throw db_error("mutation operation refuses unsettled caller routing stamp");
        }
        previous_ = current_; current_ = this; installed_ = true;
    } catch (...) { unlock(); throw; }
}
void database::mutation_sql_scope::start() {
    if (!installed_ || !locked_ || issued_ || phase_ != phase::admission)
        throw db_error("mutation operation admission already consumed");
        if (owns_transaction_) {
            // Genuine database Core BEGIN issues the private TLS lifetime and
            // physical origin under this retained native mutex, not raw SQL.
            try { owner_.begin_transaction_owned_(route_); }
            catch (...) {
                origin_ = owner_.core_transaction_lifetime_.lock();
                transaction_ = owner_.core_transaction_original_;
                core_serial_ = owner_.core_transaction_serial_;
                issued_ = same_transaction_(); // actual returned/throwing effect only
                throw;
            }
            origin_ = owner_.core_transaction_lifetime_.lock();
            transaction_ = owner_.core_transaction_original_;
            core_serial_ = owner_.core_transaction_serial_;
            issued_ = same_transaction_();
            if (!issued_) throw db_error("mutation operation BEGIN did not issue a Core transaction");
        } else {
            control_(savepoint_);
            issued_ = true; // before any later throwing check
            require_transaction_();
        }
        phase_ = phase::active;
}

void database::mutation_sql_scope::commit() {
    if (!issued_ || phase_ != phase::active) throw db_error("mutation operation already settled");
    require_transaction_(); phase_ = phase::settling;
    try {
        if (!owns_transaction_) {
            // Settle module-owned cached reads while THIS operation still owns
            // its original savepoint, Core transaction and Native mutex. In
            // particular SQLite's RTree xSavepoint closes its cached node BLOB.
            // RELEASE below removes this nested marker with our original one.
            // No busy-statement signature is exempted from the NEXT admission.
            control_("SAVEPOINT __lattice_mutation_module_settlement");
            require_transaction_();
        }
        control_(owns_transaction_ ? "COMMIT" : release_);
        if (owns_transaction_) {
            if (!same_native_() || sqlite3_get_autocommit(connection_) == 0)
                throw db_error("mutation COMMIT did not end its physical transaction");
        } else require_transaction_();
        phase_ = phase::committed; issued_ = false;
    } catch (...) {
        // Keep active ownership for the explicit failure path. A real native
        // auto-rollback must not authorize touching a successor transaction.
        phase_ = phase::active;
        throw;
    }
}
database::mutation_sql_scope::cleanup database::mutation_sql_scope::abort() noexcept {
    cleanup result;
    if (!issued_) return result;
    if (!same_transaction_()) {
        if (same_native_() && owner_.core_transaction_serial_ == core_serial_ &&
            sqlite3_get_autocommit(connection_) != 0) {
            // SQLite may have really auto-rolled back (e.g. RAISE(ROLLBACK)).
            // Absence is known under our continuously held physical mutex;
            // do not issue ROLLBACK/RELEASE or invent a successor transaction.
            observe_end_(); result.transaction_ended = true;
            phase_ = phase::rolled_back; issued_ = false; return result;
        }
        try { throw db_error("mutation rollback lost its physical transaction"); }
        catch (...) { result.rollback = std::current_exception(); }
        owner_.mutation_operation_unsettled_.store(true, std::memory_order_release);
        phase_ = phase::lost; issued_ = false; return result;
    }
    phase_ = phase::settling;
    try { control_(owns_transaction_ ? "ROLLBACK" : rollback_); }
    catch (...) { result.rollback = std::current_exception(); }
    if (!owns_transaction_ && !result.rollback) {
        try {
            require_transaction_();
            // A proved ROLLBACK TO can also leave a module's cached blob read.
            // Use the actual module callback before releasing this original;
            // any failure remains cleanup debt and keeps ordinary work fenced.
            control_("SAVEPOINT __lattice_mutation_module_settlement");
            require_transaction_(); control_(release_); require_transaction_();
        }
        catch (...) { result.release = std::current_exception(); }
    }
    if (result.rollback || result.release)
        owner_.mutation_operation_unsettled_.store(true, std::memory_order_release);
    result.transaction_ended = same_native_() && sqlite3_get_autocommit(connection_) != 0;
    phase_ = (result.rollback || result.release) ? phase::lost : phase::rolled_back;
    issued_ = false; return result;
}
void database::mutation_sql_scope::unlock() noexcept {
    if (installed_) { current_ = previous_; installed_ = false; }
    if (locked_) {
        // Child/inherited cleanup never touches a parent native mutex.
        if (creator_ == detail::database_native_process()) sqlite3_mutex_leave(mutex_);
        locked_ = false;
    }
}
database::mutation_sql_scope::~mutation_sql_scope() noexcept {
    if (creator_ != detail::database_native_process()) {
        // Do not decrement inherited control blocks or unlock parent SQLite.
        // Overwrite only the child copy; the OS reclaims quarantined ownership.
        issued_ = false; unlock();
        new (&retirement_) std::shared_ptr<detail::database_retirement_state>();
        new (&origin_) std::shared_ptr<const core_thread_lifetime>();
        new (&transaction_) std::shared_ptr<const core_transaction_original>();
        new (&finalization_failure_) std::exception_ptr();
        return;
    }
    if (issued_) (void)abort(); // explicit owner observes debt; fallback fences
    unlock();
}

void database::begin_transaction(bool exclusive) {
    begin_transaction_impl_(exclusive, nullptr);
}

void database::begin_transaction_owned_(std::atomic<std::thread::id>& owner_thread,
    std::shared_ptr<const core_transaction_original>* issued, bool strict_receipt,
    const std::shared_ptr<core_transaction_ending>& receipt_ending) {
    begin_transaction_impl_(false, &owner_thread, issued, strict_receipt, receipt_ending);
}

void database::begin_transaction_impl_(bool exclusive, std::atomic<std::thread::id>* owner_thread,
    std::shared_ptr<const core_transaction_original>* issued, bool strict_receipt,
    const std::shared_ptr<core_transaction_ending>& receipt_ending) {
    require_creator_();
    if(mutation_operation_unsettled_.load(std::memory_order_acquire))
        throw db_error("mutation operation unsettled; explicit rollback required");
    if(channel_reset_unsettled_.load(std::memory_order_acquire))throw db_error("channel reset unsettled; explicit rollback required");
    if (closed_.load(std::memory_order_acquire) && !maintenance_scope::active_for(db_) && !mutation_sql_scope::active_for(this) && !core_settlement_scope::active_for(this)) return;
    // IMMEDIATE: acquires write lock, readers still allowed (WAL mode).
    // EXCLUSIVE: acquires write lock AND blocks all readers.
    // Use exclusive for migrations so stale connections can't read mid-migration.
    const char* sql = exclusive ? "BEGIN EXCLUSIVE" : "BEGIN IMMEDIATE";
    // Allocate before the first native attempt/effect. Raw/database BEGIN
    // never issues an original. A failed BEGIN publishes no physical origin.
    std::shared_ptr<core_thread_lifetime> lifetime;
    if(owner_thread) {
        if(core_thread_lifetime_ended_)throw db_error("Core transaction thread lifetime has ended");
        auto& thread=core_thread_lifetime_();
        if(!thread.original)thread.original=std::make_shared<core_thread_lifetime>();
        lifetime=thread.original;
    }
    std::shared_ptr<core_transaction_original> original;
    if (owner_thread) original = std::make_shared<core_transaction_original>(
        this, db_, lifetime, retirement_, receipt_ending,
        detail::observer_delivery_original::offered(*this));
    if(observer_delivery_sql_&&(!owner_thread||!original||!original->observer_delivery))
        throw db_error("bounded delivery BEGIN requires its genuine pre-admitted Core original");
    using read_custody=detail::native_retirement_storage<std::shared_ptr<read_admission_original>>;
    std::optional<read_custody> read_admission;
    if(original&&original->observer_delivery) {
        const auto issuer=original->creator;
        original->observer_delivery->claim_begin_workspace(sizeof(read_admission_original)+read_custody::allocation_bytes());
        read_admission.emplace();
        if(issuer!=detail::database_native_process()||!creator_current_())
            throw db_error("inherited bounded original read custody allocation");
        read_admission->get<0>()=std::shared_ptr<read_admission_original>(new read_admission_original(issuer,original),read_admission_original::destroy);
        if(issuer!=detail::database_native_process()||!creator_current_())
            throw db_error("inherited bounded original read DATA allocation");
    }
    const auto attempt = [&] {
        // Public database/raw BEGIN keeps its existing behavior and creates no
        // Core ownership. For a genuine Core BEGIN, cover only this attempt:
        // failed attempts release before the original retry/sleep so another
        // thread can finish its actual transaction on this same connection.
        auto* mutex = sqlite3_db_mutex(db_);
        sqlite3_mutex_enter(mutex);
        struct unlock { sqlite3_mutex* mutex; ~unlock() { sqlite3_mutex_leave(mutex); } } release{mutex};
        require_catalog_clean_locked_();
        if (strict_receipt && raw_handle_escaped_.load(std::memory_order_acquire))
            throw db_error("Core receipt requires unescaped Native custody at BEGIN");
        if (owner_thread && lattice_update_hook_context_ && lattice_update_hook_context_->mutation_tail_pending)
            throw db_error("Core transaction delivery remains unsettled");
        if (owner_thread && core_transaction_serial_ == UINT64_MAX)
            throw db_error("Core transaction identity exhausted");
        const bool preparing = owner_thread && sqlite3_get_autocommit(db_) != 0;
        if (preparing) {
            if (original->observer_delivery) {
                if (target_terminal_shell_tail_.observer_delivery_)
                    throw db_error("Core delivery original still owns its final-member tail");
                // A wrapper may have been reassigned after an earlier proved
                // close. That old physical fact cannot settle a new original.
                target_terminal_shell_tail_.observer_close_proved_ = false;
                target_terminal_shell_tail_.observer_delivery_ = original->observer_delivery;
            }
            // Deny generic drains BEFORE taking the hook's prefix leaf. Its
            // under-leaf recheck observes this reservation even if it saw idle
            // just before the actual BEGIN attempt. A failed attempt owns none.
            core_native_tail_owned_.store(true, std::memory_order_release);
            try {
                auto* hook = lattice_update_hook_context_.get();
                if (hook && hook->mutation_preparing) hook->mutation_preparing(*hook, *original);
                if(observer_delivery_sql_)observer_delivery_sql_->original_preparing(original->observer_delivery);
            } catch (...) {
                core_native_tail_owned_.store(false, std::memory_order_release);
                if (original->observer_delivery) target_terminal_shell_tail_.observer_delivery_.reset();
                throw;
            }
        }
        if (preparing && original->observer_delivery) original->observer_delivery->attempting();
        const int rc = ::lattice::detail::store_physical_exec(db_, sql, nullptr, nullptr, nullptr);
        require_creator_(); // Before the added post-return Native/DATA observation.
        sql_transaction_live_.store(sqlite3_get_autocommit(db_)==0,std::memory_order_release);
        if (preparing && rc != SQLITE_OK) {
            const bool known_unissued=sqlite3_get_autocommit(db_)!=0;
            core_native_tail_owned_.store(!known_unissued&&bool(original->observer_delivery),std::memory_order_release);
            if (original->observer_delivery && known_unissued) {
                original->observer_delivery->unissued();
                if(observer_delivery_sql_)observer_delivery_sql_->original_unissued(original->observer_delivery);
                target_terminal_shell_tail_.observer_delivery_.reset();
            }else if(original->observer_delivery) {
                // No actual successful BEGIN was issued, but Native is not in
                // proved autocommit. Do not grant/rollback a guessed original,
                // detach its charged tail, or admit a fresh offer over it.
                // The final shell keeps this DATA through actual close/ending.
                mutation_operation_unsettled_.store(true,std::memory_order_release);
            }
        }
        if (rc == SQLITE_OK && owner_thread) {
            if (original->observer_delivery) original->observer_delivery->began();
            ++core_transaction_serial_; // same actual BEGIN, before mutex release
            original->serial = core_transaction_serial_;
            original->ended.store(false, std::memory_order_release);
            core_transaction_original_ = original;
            core_native_tail_owned_.store(true, std::memory_order_release);
            if (auto* hook = lattice_update_hook_context_.get())
                if (hook->mutation_started) hook->mutation_started(*hook, original);
            if (issued) *issued = original;
            core_transaction_thread_=std::this_thread::get_id();
            core_transaction_lifetime_=lifetime;
            owner_thread->store(std::this_thread::get_id(), std::memory_order_release);
            if(read_admission) {
                require_creator_(); // The preexisting started hook may call arbitrary code.
                auto& read_original=read_admission->get<0>();
                read_original->live.store(true,std::memory_order_release);
                std::atomic_store(&read_admission_original_,read_original);
            }
        }
        return rc;
    };

    // Wall-clock budget for acquiring the transaction. Each attempt blocks
    // INSIDE SQLite's busy handler for the remaining budget — the handler
    // re-polls the lock with sub-millisecond cadence, so the lock is acquired
    // the moment it frees. (A previous version zeroed the statement timeout
    // and slept between attempts; under heavy writer traffic that sparse
    // polling starves — long write transactions hand the lock to whoever is
    // inside the busy handler, never to a sleeper.) The deadline, not the
    // per-attempt timeout, bounds the total wait: attempts repeat only for
    // same-connection transaction races, which resolve quickly.
    //
    // The budget is the configured busy timeout, NOT a constant: an
    // interactive process that opened with busy_timeout_ms=2000 must not
    // discover its explicit BEGINs still park for 30s (Aug 2026 hook-wedge
    // incident — two such waits ate a 60s harness deadline whole).
    // The actual unpublished checked preparation privately owns this whole
    // archive turn. Do not replace its original busy control with a renewed
    // ordinary timeout; the BEGIN budget may only shorten the original end.
    if(checked_existing_connection_&&read_control_) {
        const int budget_ms=busy_timeout_ms_>0?busy_timeout_ms_:kDefaultBusyTimeoutMs;
        const auto deadline=std::min(read_control_->deadline,
            std::chrono::steady_clock::now()+std::chrono::milliseconds(budget_ms));
        struct busy_context {database_read_control* control;std::chrono::steady_clock::time_point end;};
        busy_context context{read_control_.get(),deadline};
        struct restore_busy {
            database* owner;void* expected;std::shared_ptr<database_read_control> control;
            ~restore_busy() noexcept {
                // Remove only THIS stack context. The original checked-read
                // role remains installed, with its original deadline/custody.
                if(control->creator_process!=detail::database_native_process()||
                   !detail::ordinary_creator_current({}))return;
                (void)owner->clear_sql_busy_(detail::database_handler_slot::checkpoint,
                    expected,owner->busy_timeout_ms_);
            }
        } restore{this,&context,read_control_};
        if(set_sql_busy_(detail::database_handler_slot::checkpoint,[](void* p,int)noexcept ->int {
            auto& c=*static_cast<busy_context*>(p);
            if(c.control->stopped()||std::chrono::steady_clock::now()>=c.end)return 0;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            return c.control->stopped()||std::chrono::steady_clock::now()>=c.end?0:1;
        },&context)!=SQLITE_OK)throw db_error("checked preparation BEGIN busy control unavailable");
        int rc;
        for(;;) {
            if(read_control_->stopped()||std::chrono::steady_clock::now()>=deadline){rc=SQLITE_BUSY;break;}
            rc=attempt();
            // Return successful BEGIN unchanged. Its real owning caller must
            // record active before any later original-end refusal/rollback.
            if(rc==SQLITE_OK)return;
            const bool expired=read_control_->stopped()||std::chrono::steady_clock::now()>=deadline;
            if((rc==SQLITE_BUSY||rc==SQLITE_LOCKED)&&!expired)continue;
            if(rc==SQLITE_ERROR&&is_in_transaction()&&!expired){
                std::this_thread::sleep_for(std::chrono::milliseconds(1));continue;
            }
            break;
        }
        auto error=std::string(sqlite3_errmsg(db_));
        throw db_error("Failed to begin checked preparation transaction: "+error);
    }

    const int budget_ms = busy_timeout_ms_ > 0 ? busy_timeout_ms_ : kDefaultBusyTimeoutMs;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(budget_ms);
    int rc;
    for (;;) {
        auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now()).count();
        if (remaining < 1) remaining = 1;
        { catalog_native_admission_ admission(*this);
          sqlite3_busy_timeout(db_, static_cast<int>(remaining)); }

        rc = attempt();
        if (rc == SQLITE_OK) break;

        const bool past_deadline = std::chrono::steady_clock::now() >= deadline;
        if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
            // The busy handler already waited out `remaining` — only retry if
            // wall clock says budget is left (e.g. spurious early return).
            if (past_deadline) break;
            continue;
        }
        if (rc == SQLITE_ERROR && is_in_transaction()) {
            // "cannot start a transaction within a transaction" — another
            // thread on this serialized connection (SQLITE_OPEN_FULLMUTEX)
            // holds a transaction. Brief sleep; it finishes shortly.
            if (past_deadline) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        break;  // non-retryable error
    }

    { catalog_native_admission_ admission(*this);
      sqlite3_busy_timeout(db_, busy_timeout_ms_); } // Restore, never mutate after UNKNOWN.

    if (rc != SQLITE_OK) {
        auto error = std::string(sqlite3_errmsg(db_));
        int ext = sqlite3_extended_errcode(db_);
        LOG_ERROR("db", "Failed to begin transaction (rc=%d, ext=%d, db=%p, path=%s): %s",
                  rc, ext, (void*)db_, path_.c_str(), error.c_str());
        throw db_error("Failed to begin transaction: " + error);
    }
}

bool database::try_begin_immediate(int /*timeout_ms*/) {
    require_creator_();
    if(mutation_operation_unsettled_.load(std::memory_order_acquire))return false;
    if(channel_reset_unsettled_.load(std::memory_order_acquire))return false;
    if (closed_.load(std::memory_order_acquire)) return false;
    catalog_native_admission_ admission(*this);
    // Non-blocking: temporarily set busy timeout to 0, try BEGIN IMMEDIATE once,
    // then restore the original timeout. This never sleeps.
    sqlite3_busy_timeout(db_, 0);
    int rc = ::lattice::detail::store_physical_exec(db_, "BEGIN IMMEDIATE", nullptr, nullptr, nullptr);
    sqlite3_busy_timeout(db_, busy_timeout_ms_);  // Restore configured timeout
    return rc == SQLITE_OK;
}

void database::commit() {
    execute("COMMIT");
}

void database::rollback() {
    execute("ROLLBACK");
}

bool database::is_in_transaction() const {
    require_creator_();
    catalog_native_admission_ admission(*this);
    // sqlite3_get_autocommit returns 0 if a transaction is active, non-zero otherwise
    return sqlite3_get_autocommit(db_) == 0;
}

// Transaction RAII guard
transaction::transaction(database& db, bool exclusive) : db_(db) {
    db_.begin_transaction(exclusive);
}

transaction::~transaction() {
    if (!completed_) {
        try {
            db_.rollback();
        } catch (...) {
            // Suppress exceptions in destructor
        }
    }
}

void transaction::commit() {
    db_.commit();
    completed_ = true;
}

void transaction::rollback() {
    db_.rollback();
    completed_ = true;
}

#include "database_initialization_channel.inc"

} // namespace lattice

// Closed target-only construction; no ordinary constructor body is replaced.
#include "ordinary_target_staging_database.inc"
