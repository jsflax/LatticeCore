#include "database_sql_dispatcher.hpp"
#include <lattice/detail/observer_delivery_startup.hpp>
#include "ordinary_cohort_attachment.hpp"
#include <lattice/db.hpp>
#include <utility>
#include <algorithm>
#include <climits>

namespace lattice::detail {
namespace {
constexpr const char* cookie_name="lattice_owned_sql_dispatch_cookie_v1";
struct native_unlock {sqlite3_mutex* mutex;uint64_t creator;~native_unlock(){
    if(mutex&&creator==database_native_process())sqlite3_mutex_leave(mutex);
}};
// Actual SQLite statement state only; no SQL spelling, public handle or owner
// classification is inferred. Caller holds this exact connection's mutex.
bool has_busy_statement(sqlite3* native)noexcept {
    for(auto* statement=sqlite3_next_stmt(native,nullptr);statement;
        statement=sqlite3_next_stmt(native,statement))
        if(sqlite3_stmt_busy(statement))return true;
    return false;
}
}
thread_local database_sql_dispatcher::callback_frame* database_sql_dispatcher::callbacks_=nullptr;
database_sql_dispatcher::callback_frame::callback_frame(const database_sql_dispatcher& value)noexcept
    :creator(value.creator_),cookie(&value),previous(callbacks_){callbacks_=this;}
database_sql_dispatcher::callback_frame::~callback_frame()noexcept {
    // POD TLS shell only, including a returned fork child. No inherited
    // cookie/Native reference, callback target or mutex is released here.
    callbacks_=previous;
}
bool database_sql_dispatcher::callback_active_for_(const database& owner)noexcept {
    if(!owner.creator_current_())return true;
    const auto issuer=database_native_process();
    const auto* actual=owner.sql_dispatcher_.load(std::memory_order_acquire);
    for(auto* frame=callbacks_;frame;frame=frame->previous) {
        if(frame->creator!=issuer)return true; // inherited callback custody denies
        if(frame->cookie==actual)return true;
    }
    return false;
}
bool database_sql_dispatcher::initialization_admits_(database& owner,sqlite3_stmt* statement)noexcept {
    if(!owner.creator_current_())return false;
    auto* cookie=owner.sql_dispatcher_.load(std::memory_order_acquire);
    return cookie&&cookie->creator_==database_native_process()&&cookie->active_.load(std::memory_order_acquire)&&
        cookie->native_==owner.db_&&cookie->request_&&cookie->request_->owner_==&owner&&
        cookie->request_->initialization_admits_(statement);
}
database_sql_dispatcher::database_sql_dispatcher(uint64_t issuer,sqlite3* native,
    store_physical_policy_original&& original)noexcept
    :creator_(issuer),native_(native),original_(std::move(original)){}
void database_sql_dispatcher::destroy(void* raw)noexcept {
    auto* value=static_cast<database_sql_dispatcher*>(raw);
    // No SQLite/Native callback/owner destructor, including in a fork child.
    if(value&&value->creator_==database_native_process()) {
        const auto issuer=value->creator_;auto* observer=value->destroyed_observer_;
        delete value;
        if(observer&&issuer==database_native_process())observer->fetch_add(1,std::memory_order_release);
    }
}
void database_sql_dispatcher::release_()noexcept {
    if(creator_==database_native_process()&&references_.fetch_sub(1,std::memory_order_acq_rel)==1)destroy(this);
}
void database_sql_dispatcher::sqlite_destroy(void* raw)noexcept {
    auto* value=static_cast<database_sql_dispatcher*>(raw);
    if(value&&value->creator_==database_native_process()&&
       value->sqlite_reference_.exchange(false,std::memory_order_acq_rel))value->release_();
}
int database_sql_dispatcher::expired_authorize(void*,int,const char*,const char*,const char*,const char*)noexcept {
    return SQLITE_DENY;
}
void database_sql_dispatcher::inaccessible(sqlite3_context* context,int,sqlite3_value**)noexcept {
    sqlite3_result_error(context,"internal SQL policy cookie is not callable",-1);
}
int database_sql_dispatcher::authorize(void* raw,int action,const char* first,
    const char* second,const char* schema,const char* origin)noexcept {
    auto* value=static_cast<database_sql_dispatcher*>(raw);
    if(!value||value->creator_!=database_native_process()||
       !value->active_.load(std::memory_order_acquire)||value->in_callback_)return SQLITE_DENY;
    // The fixed cookie is only SQLite lifetime custody, never an executable grant.
    if(action==SQLITE_FUNCTION&&second&&sqlite3_stricmp(second,cookie_name)==0)return SQLITE_DENY;
    if(value->request_&&value->request_->authorize(action,first,second,schema,origin)!=SQLITE_OK)return SQLITE_DENY;
    value->references_.fetch_add(1,std::memory_order_relaxed);
    struct callback_pin {database_sql_dispatcher* value;~callback_pin(){value->release_();}} pin{value};
    value->in_callback_=true;
    callback_frame provenance(*value);
    struct reset {database_sql_dispatcher* value;~reset(){
        if(value->creator_==database_native_process())value->in_callback_=false;
    }} executing{value};
    int result=ordinary_composed_attachment_authorize(value->native_,action,first,second);
    if(result!=SQLITE_OK&&result!=SQLITE_IGNORE)return SQLITE_DENY;
    for(const auto& slot:value->slots_)if(slot.call) {
        const int answer=slot.call(slot.context,action,first,second,schema,origin);
        if(value->creator_!=database_native_process()||
           !value->active_.load(std::memory_order_acquire))return SQLITE_DENY;
        if(answer!=SQLITE_OK&&answer!=SQLITE_IGNORE)return SQLITE_DENY;
        if(answer==SQLITE_IGNORE)result=SQLITE_IGNORE;
    }
    if(action==SQLITE_PRAGMA&&first&&second&&sqlite3_stricmp(first,"busy_timeout")==0)
        value->numeric_known_=false; // a permitted setter may change the actual policy
    return result;
}
int database_sql_dispatcher::install(database& owner)noexcept {
    database_sql_dispatcher* pending=nullptr;
    uint64_t issuer=0;
    try {
        owner.require_creator_();
        store_physical_native_scope physical_call(owner);
        if(!physical_call.entered())return SQLITE_MISUSE;
        if(!owner.db_||owner.raw_handle_escaped_.load(std::memory_order_acquire))return SQLITE_AUTH;
        if(owner.sql_dispatcher_.load(std::memory_order_acquire))return SQLITE_OK;
        issuer=database_native_process();
        auto* const native=owner.db_;
        // Capture from this wrapper's once-bound actual open before allocation.
        // Missing/legacy provenance never becomes a healthy bounded-mode token.
        auto original=owner.observer_policy_original_();
        auto* cookie=pending=new database_sql_dispatcher(issuer,native,std::move(original));
        if(issuer!=database_native_process())return SQLITE_MISUSE; // abandon inherited custody
        owner.require_creator_();
        auto* mutex=sqlite3_db_mutex(native);
        if(!mutex){std::exchange(pending,nullptr)->release_();return SQLITE_MISUSE;}
        sqlite3_mutex_enter(mutex);native_unlock held{mutex,issuer};
        if(issuer!=database_native_process())return SQLITE_MISUSE;
        if(owner.db_!=native||owner.sql_dispatcher_.load(std::memory_order_acquire)||owner.raw_handle_escaped_.load(std::memory_order_acquire)) {
            std::exchange(pending,nullptr)->release_();return owner.sql_dispatcher_.load(std::memory_order_acquire)?SQLITE_OK:SQLITE_AUTH;
        }
        // First installation is a policy publication too. Do not disturb a
        // live VM or publish any new callback custody while it is busy.
        if(has_busy_statement(native)){
            std::exchange(pending,nullptr)->release_();return SQLITE_BUSY;
        }
        // Keep our distinct DATA reference through the return. The actual
        // xDestroy consumes SQLite's reference; a wrapper refusal before the
        // Native call cannot be mistaken for that callback or consume ours.
        cookie->references_.fetch_add(1,std::memory_order_relaxed);
        cookie->sqlite_reference_.store(true,std::memory_order_release);
        const int registered=store_physical_create_function_v2(native,cookie_name,0,SQLITE_UTF8,
            cookie,inaccessible,nullptr,nullptr,sqlite_destroy);
        if(issuer!=database_native_process())return SQLITE_MISUSE;
        if(registered!=SQLITE_OK) {
            sqlite_destroy(cookie); // no-op if real Native failure already called it
            std::exchange(pending,nullptr)->release_();return registered;
        }
        if(!cookie->sqlite_reference_.load(std::memory_order_acquire)) {
            std::exchange(pending,nullptr)->release_();return SQLITE_MISUSE;
        }
        owner.sql_dispatcher_.store(cookie,std::memory_order_release);pending=nullptr;
        const int installed=sqlite3_set_authorizer(native,authorize,cookie);
        if(issuer!=database_native_process())return SQLITE_MISUSE;
        if(installed!=SQLITE_OK){
            if(cookie->publication_failure_==SQLITE_OK)cookie->publication_failure_=installed;
            if(cookie->expiration_failure_==SQLITE_OK)cookie->expiration_failure_=installed;
            cookie->original_.note_observer_policy_unsettled();return installed;
        }
        cookie->active_.store(true,std::memory_order_release);
        return SQLITE_OK;
    }catch(...) {
        // Only unregistered DATA remains ours. No callback/Native owner tail
        // is released by this cleanup, and foreign custody is abandoned.
        if(pending&&issuer==database_native_process())pending->release_();
        return SQLITE_MISUSE;
    }
}
int database_sql_dispatcher::bind(database& owner,database_authorizer_slot kind,
    callback call,void* context)noexcept {
    const std::size_t index=std::size_t(kind);
    if(index>=count||!call||!context)return SQLITE_MISUSE;
    const int created=install(owner);if(created!=SQLITE_OK)return created;
    try {
        owner.require_creator_();
        store_physical_native_scope physical_call(owner);
        if(!physical_call.entered())return SQLITE_MISUSE;
        auto* cookie=owner.sql_dispatcher_.load(std::memory_order_acquire);
        if(!cookie||cookie->creator_!=database_native_process()||owner.db_!=cookie->native_)return SQLITE_MISUSE;
        auto* mutex=sqlite3_db_mutex(cookie->native_);if(!mutex)return SQLITE_MISUSE;
        sqlite3_mutex_enter(mutex);native_unlock held{mutex,cookie->creator_};
        if(cookie->creator_!=database_native_process())return SQLITE_MISUSE;
        if(!cookie->active_.load(std::memory_order_acquire)||cookie->in_callback_)return SQLITE_MISUSE;
        owner.require_catalog_clean_locked_();
        if(has_busy_statement(cookie->native_))return SQLITE_BUSY;
        const auto previous=cookie->slots_[index];
        cookie->slots_[index]={call,context};
        // Role changes behind a stable callback pointer do not invalidate
        // already-compiled programs. Reissue the same owning callback under
        // this same serialization so actual v2/v3 execution must reauthorize.
        const int published=sqlite3_set_authorizer(cookie->native_,authorize,cookie);
        if(cookie->creator_!=database_native_process())return SQLITE_MISUSE;
        if(published!=SQLITE_OK){
            cookie->slots_[index]=previous;
            if(cookie->publication_failure_==SQLITE_OK)cookie->publication_failure_=published;
            if(cookie->expiration_failure_==SQLITE_OK)cookie->expiration_failure_=published;
            cookie->active_.store(false,std::memory_order_release);
            cookie->original_.note_observer_policy_unsettled();
            return published;
        }
        return SQLITE_OK;
    }catch(...){return SQLITE_MISUSE;}
}
int database_sql_dispatcher::clear(database& owner,database_authorizer_slot kind,void* expected)noexcept {
    const std::size_t index=std::size_t(kind);if(index>=count)return SQLITE_MISUSE;
    // Exact cleanup remains possible after catalog/VM admission debt. No new
    // callback is installed, and no Native owner is copied or released here.
    if(!owner.creator_current_())return SQLITE_MISUSE;
    auto* cookie=owner.sql_dispatcher_.load(std::memory_order_acquire);
    if(!cookie||!owner.db_)return SQLITE_OK;
    if(cookie->creator_!=database_native_process()||owner.db_!=cookie->native_)return SQLITE_MISUSE;
    store_physical_cleanup_scope physical_call(owner);
    if(!physical_call.entered())return SQLITE_MISUSE;
    auto* mutex=sqlite3_db_mutex(cookie->native_);if(!mutex)return SQLITE_MISUSE;
    sqlite3_mutex_enter(mutex);native_unlock held{mutex,cookie->creator_};
    if(cookie->creator_!=database_native_process())return SQLITE_MISUSE;
    if(cookie->in_callback_)return SQLITE_MISUSE;
    auto& current=cookie->slots_[index];
    if(!current.call||current.context!=expected)return SQLITE_OK; // empty/old cleanup cannot erase a successor
    // A retiring producer cannot leave its raw context installed, even when
    // actual busy/Native debt prevents a successful new policy publication.
    const bool busy=has_busy_statement(cookie->native_);
    current={};
    if(busy){
        cookie->active_.store(false,std::memory_order_release);
        if(cookie->publication_failure_==SQLITE_OK)cookie->publication_failure_=SQLITE_BUSY;
        cookie->original_.note_observer_policy_unsettled();
    }
    // Even a failed busy cleanup expires OTHER idle programs. Inactive is a
    // deny-only callback, not proof the already-running VM was cancelled.
    const int published=sqlite3_set_authorizer(cookie->native_,authorize,cookie);
    if(cookie->creator_!=database_native_process())return SQLITE_MISUSE;
    if(published!=SQLITE_OK){
        if(cookie->publication_failure_==SQLITE_OK)cookie->publication_failure_=published;
        if(cookie->expiration_failure_==SQLITE_OK)cookie->expiration_failure_=published;
        cookie->active_.store(false,std::memory_order_release);
        cookie->original_.note_observer_policy_unsettled();
    }
    return busy?SQLITE_BUSY:published;
}
// All progress producers use the existing1000 VM-instruction cadence. Each
// constraint is checked; no owner gets replaced by another owner or request.
int database_sql_dispatcher::progress(void* raw)noexcept {
    auto* value=static_cast<database_sql_dispatcher*>(raw);
    if(!value||value->creator_!=database_native_process()||!value->active_.load(std::memory_order_acquire)||value->in_callback_)return 1;
    value->references_.fetch_add(1,std::memory_order_relaxed);
    struct callback_pin {database_sql_dispatcher* value;~callback_pin(){value->release_();}} pin{value};
    // These are real SQLite callbacks too: role mutation/handler restoration
    // must refuse reentry before changing this connection's Native policy.
    value->in_callback_=true;
    callback_frame provenance(*value);
    struct callback_reset {database_sql_dispatcher* value;~callback_reset(){
        if(value->creator_==database_native_process())value->in_callback_=false;
    }} executing{value};
    if(!value->request_cleanup_&&value->request_&&value->request_->stopped())return 1;
    for(const auto& slot:value->progress_)if(slot.call) {
        const int stop=slot.call(slot.context);
        if(value->creator_!=database_native_process()||!value->active_.load(std::memory_order_acquire)||stop)return 1;
    }
    return 0;
}
// Busy policies are genuine nested ownership, not independent sleepers: the
// innermost actual catalog/capture/checkpoint producer owns its old policy.
// Request deadline is an additional deny-only constraint, never a new sleep.
int database_sql_dispatcher::busy(void* raw,int attempts)noexcept {
    auto* value=static_cast<database_sql_dispatcher*>(raw);
    if(!value||value->creator_!=database_native_process()||!value->active_.load(std::memory_order_acquire)||value->in_callback_)return 0;
    value->references_.fetch_add(1,std::memory_order_relaxed);
    struct callback_pin {database_sql_dispatcher* value;~callback_pin(){value->release_();}} pin{value};
    // These are real SQLite callbacks too: role mutation/handler restoration
    // must refuse reentry before changing this connection's Native policy.
    value->in_callback_=true;
    callback_frame provenance(*value);
    struct callback_reset {database_sql_dispatcher* value;~callback_reset(){
        if(value->creator_==database_native_process())value->in_callback_=false;
    }} executing{value};
    if(!value->request_cleanup_&&value->request_&&value->request_->stopped())return 0;
    for(std::size_t i=handler_count;i>0;--i)if(value->busy_[i-1].call) {
        const auto slot=value->busy_[i-1];const int again=slot.call(slot.context,attempts);
        if(value->creator_!=database_native_process()||!value->active_.load(std::memory_order_acquire)||
           (!value->request_cleanup_&&value->request_&&value->request_->stopped()))return 0;
        return again;
    }
    return 0;
}
void database_sql_dispatcher::refresh_progress_()noexcept {
    bool any=request_&&request_->bounded_&&
        request_->limits_.deadline!=std::chrono::steady_clock::time_point::max();
    for(const auto& slot:progress_)any=any||slot.call;
    if(any||progress_override_)sqlite3_progress_handler(native_,any?1000:0,any?progress:nullptr,any?this:nullptr);
    progress_override_=any;
}
bool database_sql_dispatcher::capture_numeric_()noexcept {
    if(!request_||numeric_sampling_||request_->kind_==database_sql_read_kind::busy_timeout_getter)return false;
    const auto issuer=creator_;
    numeric_sampling_=true;
    struct sampling_guard {database_sql_dispatcher* value;uint64_t creator;~sampling_guard(){
        if(creator==database_native_process())value->numeric_sampling_=false;
    }} sampling{this,issuer};
    sqlite3_stmt* statement=nullptr;
    try {
        database_sql_read_limits declared;
        declared.sql_bytes=128;declared.parameter_bytes=0;declared.columns=4;declared.rows=1;declared.result_bytes=128;
        declared.deadline=request_->limits_.deadline;
        const auto read=[&](database_sql_request& actual) {
            database_sql_request::binding selected(&actual);
            const int prepared=store_physical_prepare_v3(native_,"PRAGMA busy_timeout",-1,SQLITE_PREPARE_NO_VTAB,&statement,nullptr);
            if(issuer!=database_native_process())return false;
            actual.note_initialization_api_error_(prepared);
            if(prepared!=SQLITE_OK)throw db_error("actual busy policy getter unavailable");
            actual.adopt(statement);actual.columns(statement);
            const int row=store_physical_step(statement);
            if(issuer!=database_native_process())return false;
            actual.note_initialization_api_error_(row);
            if(row!=SQLITE_ROW)throw db_error("actual busy policy row unavailable");
            actual.row(statement);
            const int ended=store_physical_step(statement);
            if(issuer!=database_native_process())return false;
            actual.note_initialization_api_error_(ended);
            if(ended!=SQLITE_DONE)throw db_error("actual busy policy getter did not end");
            const int rc=store_physical_finalize(std::exchange(statement,nullptr));
            if(issuer!=database_native_process())return false;
            actual.note_initialization_api_error_(rc,true);
            if(rc!=SQLITE_OK)throw db_error("actual busy policy getter finalization failed");
            return numeric_known_;
        };
        // This child gets ONLY the fixed getter entry and the SAME preadmitted
        // setup budget. Public/UDF SQL never inherits initialization authority.
        if(request_->initialization_) {
            database_sql_request actual(*request_->owner_,*request_);
            return read(actual);
        }
        database_sql_request actual(*request_->owner_,database_sql_read_kind::busy_timeout_getter,
            "PRAGMA busy_timeout",{},&declared);
        return read(actual);
    }catch(...) {
        if(issuer==database_native_process()) {
            if(statement) {
                const int ended=store_physical_finalize(statement);
                if(issuer==database_native_process()&&request_)request_->note_initialization_api_error_(ended,true);
            }
            if(issuer==database_native_process())numeric_known_=false;
        }
        return false;
    }
}
int database_sql_dispatcher::refresh_busy_(bool restoring)noexcept {
    if(numeric_sampling_) {
        // A failed getter's binding unwinds BEFORE capture_numeric_'s catch.
        // Restoration is DATA-only here: no recursive sample or renewed read.
        // A different newly entered finite request is refused, not admitted.
        if(restoring||(request_&&request_->kind_==database_sql_read_kind::busy_timeout_getter))return SQLITE_OK;
        return SQLITE_MISUSE;
    }
    bool any=false;for(const auto& slot:busy_)any=any||slot.call;
    if(any)return sqlite3_busy_handler(native_,busy,this);
    if(request_&&request_->initialization_cleanup_()) {
        numeric_override_=true;
        return sqlite3_busy_handler(native_,nullptr,nullptr);
    }
    const bool finite=request_&&request_->bounded_&&!request_cleanup_;
    if(finite) {
        // The getter itself never substitutes a numeric policy before it has
        // actually read one. All other declared reads must have actual policy
        // evidence; a configured field/path or last wrapper result is not it.
        if(request_->kind_==database_sql_read_kind::busy_timeout_getter)return SQLITE_OK;
        if(!numeric_known_&&!capture_numeric_())return SQLITE_MISUSE;
        const auto remaining=request_->limits_.deadline-std::chrono::steady_clock::now();
        const auto millis=std::chrono::duration_cast<std::chrono::milliseconds>(remaining).count();
        const int bounded=millis<=0?0:int(std::min<int64_t>(numeric_timeout_,std::min<int64_t>(millis,INT_MAX)));
        const int rc=sqlite3_busy_timeout(native_,bounded);
        if(creator_!=database_native_process())return SQLITE_MISUSE;
        if(rc==SQLITE_OK)numeric_override_=true;else original_.note_observer_policy_unsettled();
        return rc;
    }
    if(numeric_override_) {
        if(!numeric_known_){original_.note_observer_policy_unsettled();return SQLITE_MISUSE;}
        const int rc=sqlite3_busy_timeout(native_,numeric_timeout_);
        if(creator_!=database_native_process())return SQLITE_MISUSE;
        if(rc==SQLITE_OK)numeric_override_=false;else original_.note_observer_policy_unsettled();
        return rc;
    }
    return SQLITE_OK; // do not replace an untouched legacy numeric/raw policy
}
int database_sql_dispatcher::set_progress(database& owner,database_handler_slot role,progress_callback call,void* context)noexcept {
    const auto index=std::size_t(role);if(index>=handler_count||!call||!context)return SQLITE_MISUSE;
    const int installed=install(owner);if(installed!=SQLITE_OK)return installed;
    auto* value=owner.sql_dispatcher_.load(std::memory_order_acquire);
    if(!owner.creator_current_()||!value||value->creator_!=database_native_process()||owner.db_!=value->native_)return SQLITE_MISUSE;
    store_physical_native_scope physical_call(owner);
    if(!physical_call.entered())return SQLITE_MISUSE;
    auto* mutex=sqlite3_db_mutex(value->native_);sqlite3_mutex_enter(mutex);native_unlock held{mutex,value->creator_};
    if(value->creator_!=database_native_process())return SQLITE_MISUSE;
    if(!value->active_.load(std::memory_order_acquire)||value->in_callback_)return SQLITE_MISUSE;
    value->progress_[index]={call,context};value->refresh_progress_();return SQLITE_OK;
}
int database_sql_dispatcher::clear_progress(database& owner,database_handler_slot role,void* expected)noexcept {
    const auto index=std::size_t(role);if(index>=handler_count||!owner.creator_current_())return SQLITE_MISUSE;
    auto* value=owner.sql_dispatcher_.load(std::memory_order_acquire);if(!value)return SQLITE_OK;
    if(value->creator_!=database_native_process()||owner.db_!=value->native_)return SQLITE_MISUSE;
    store_physical_cleanup_scope physical_call(owner);
    if(!physical_call.entered())return SQLITE_MISUSE;
    auto* mutex=sqlite3_db_mutex(value->native_);sqlite3_mutex_enter(mutex);native_unlock held{mutex,value->creator_};
    if(value->creator_!=database_native_process())return SQLITE_MISUSE;
    if(value->in_callback_)return SQLITE_MISUSE;
    if(value->progress_[index].context==expected)value->progress_[index]={};
    value->refresh_progress_();return SQLITE_OK;
}
int database_sql_dispatcher::set_busy(database& owner,database_handler_slot role,busy_callback call,void* context)noexcept {
    const auto index=std::size_t(role);if(index>=handler_count||!call||!context)return SQLITE_MISUSE;
    const int installed=install(owner);if(installed!=SQLITE_OK)return installed;
    auto* value=owner.sql_dispatcher_.load(std::memory_order_acquire);
    if(!owner.creator_current_()||!value||value->creator_!=database_native_process()||owner.db_!=value->native_)return SQLITE_MISUSE;
    store_physical_native_scope physical_call(owner);
    if(!physical_call.entered())return SQLITE_MISUSE;
    auto* mutex=sqlite3_db_mutex(value->native_);sqlite3_mutex_enter(mutex);native_unlock held{mutex,value->creator_};
    if(value->creator_!=database_native_process())return SQLITE_MISUSE;
    if(!value->active_.load(std::memory_order_acquire)||value->in_callback_)return SQLITE_MISUSE;
    const auto prior=value->busy_[index];value->busy_[index]={call,context};
    const int rc=sqlite3_busy_handler(value->native_,busy,value);
    if(rc!=SQLITE_OK){value->busy_[index]=prior;value->original_.note_observer_policy_unsettled();}
    return rc;
}
int database_sql_dispatcher::clear_busy(database& owner,database_handler_slot role,void* expected,int timeout)noexcept {
    const auto index=std::size_t(role);if(index>=handler_count||!owner.creator_current_())return SQLITE_MISUSE;
    auto* value=owner.sql_dispatcher_.load(std::memory_order_acquire);if(!value)return SQLITE_OK;
    if(value->creator_!=database_native_process()||owner.db_!=value->native_)return SQLITE_MISUSE;
    store_physical_cleanup_scope physical_call(owner);
    if(!physical_call.entered())return SQLITE_MISUSE;
    auto* mutex=sqlite3_db_mutex(value->native_);sqlite3_mutex_enter(mutex);native_unlock held{mutex,value->creator_};
    if(value->creator_!=database_native_process())return SQLITE_MISUSE;
    if(value->in_callback_)return SQLITE_MISUSE;
    // Cleanup of an older context cannot overwrite its replacement's policy.
    if(value->busy_[index].context!=expected)return SQLITE_OK;
    value->busy_[index]={};bool any=false;for(const auto& slot:value->busy_)any=any||slot.call;
    const int rc=any?sqlite3_busy_handler(value->native_,busy,value):sqlite3_busy_timeout(value->native_,timeout);
    if(rc!=SQLITE_OK)value->original_.note_observer_policy_unsettled();
    else if(!any){value->numeric_timeout_=timeout;value->numeric_known_=true;value->numeric_override_=false;}
    return rc;
}
int database_sql_dispatcher::retire(database& owner)noexcept {
    if(creator_!=database_native_process())return SQLITE_MISUSE;
    if(!owner.creator_current_()||owner.db_!=native_){
        active_.store(false,std::memory_order_release);original_.note_observer_policy_unsettled();return SQLITE_MISUSE;
    }
    store_physical_cleanup_scope physical_call(owner);
    if(!physical_call.entered())return SQLITE_MISUSE;
    active_.store(false,std::memory_order_release);
    // Legacy raw callers may replace even the private UDF after raw escape.
    // Our wrapper reference kept authorizer userdata valid. If SQLite already
    // released its reference, detach that userdata before dropping ours. The
    // stateless denied callback remains safe for zombie automatic reprepare.
    if(!sqlite_reference_.load(std::memory_order_acquire)) {
        const int rc=sqlite3_set_authorizer(native_,expired_authorize,nullptr);
        if(rc!=SQLITE_OK) {
            original_.note_observer_policy_unsettled();
            // Explicit fixed DATA quarantine: freeing this still-installed
            // userdata would turn unknown cleanup into a dangling callback.
            return rc;
        }
    }
    release_();return SQLITE_OK;
}
}
