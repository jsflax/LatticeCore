#include "ordinary_target_staging_io.hpp"
#ifdef LATTICE_MANAGED_TARGET_RECLAMATION_FIXTURE
#include "ordinary_target_reclamation_fixture.hpp"
#endif
#include "ordinary_target_staging_connection.hpp"
#include "lattice/file_growth_layout.hpp"
#include <sqlite3.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstring>
#include <exception>
#include <limits>
#include <mutex>
#include <new>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>
#if defined(__APPLE__) || defined(__linux__)
#include <unistd.h>
#endif

namespace lattice::detail {
namespace {
constexpr std::int64_t target_logical_bytes = 1024LL * 1024 * 1024;
constexpr std::size_t maximum_target_policies = 64;
managed_fixed_file_ceiling target_layout() {
    constexpr auto page = managed_fixed_file_ceiling::page_size;
    constexpr auto padding = 2 * managed_fixed_file_ceiling::maximum_sector_size;
    constexpr auto pair = page + (page + 8);
    constexpr auto pages = (target_logical_bytes - padding) / pair;
    static_assert(pages > 0 && pages <= 0x3fffffff);
    constexpr auto main = pages * page;
    constexpr auto journal = padding + pages * (page + 8);
    static_assert(main > 0 && journal > 0 && main <= target_logical_bytes - journal);
    return {main, journal, pages};
}
}
#if defined(__APPLE__) || defined(__linux__)
struct ordinary_target_staging_io::environmental_phase {
    ordinary_target_scope_root_tail terminal_scope_root_{this,4}; // First: real scalar Native environment bank member ending.
    const pid_t creator=::getpid();
    const std::thread::id thread=std::this_thread::get_id();
    const ordinary_target_staging_connection* const connection;
    const std::uint64_t generation;
    struct environmental_call {int kind=0,argument=0,result=0;std::uint64_t callback_generation=0;bool attempted=false,entered=false,returned=false;};
    std::array<environmental_call,128> calls{};std::size_t count=0;
    bool settled=false;std::uint64_t settlement_generation=0;
    struct terminal_ending_record {
        bool attempted=false,evidence_returned=false,evidence_known=false;
        bool detachment_attempted=false,detachment_returned=false,detached=false,returned=false,joined=false;
        std::uint64_t cleanup_generation=0;
    } terminal_ending; // Separate fixed first receipt; ordinary settlement stays historical.
    environmental_phase(const ordinary_target_staging_connection& original,std::uint64_t phase)
        :connection(&original),generation(phase) {terminal_scope_root_.construction_returned_=true;}
    bool creator_current()const noexcept {return creator==::getpid()&&thread==std::this_thread::get_id();}
};
struct ordinary_target_staging_io::state {
    const pid_t creator = ::getpid();
    const std::thread::id thread = std::this_thread::get_id();
    const ordinary_launch::deadline end;
    const std::shared_ptr<const ordinary_unpublished_group_target> target;
    std::size_t policy_ordinal = 0;
    bool published = false, preparation_attempted = false, preparation_joined = false;
    bool guard_prepared = false, registration_attempted = false, registration_observed = false;
    int registration_rc = SQLITE_MISUSE;
    std::exception_ptr first_failure;
    std::weak_ptr<ordinary_target_staging_connection> connection;
    const ordinary_target_staging_connection* connection_identity=nullptr;
    bool connection_bound=false;
    state(std::shared_ptr<const ordinary_unpublished_group_target> original,ordinary_launch::deadline limit)
        :end(limit),target(std::move(original)) {}
    bool creator_current()const noexcept {
        return creator == ::getpid() && thread == std::this_thread::get_id();
    }
    void require_route(ordinary_launch::deadline limit)const {
        if(!creator_current() || limit > end || std::chrono::steady_clock::now() >= limit)
            throw std::runtime_error("Original target I/O creator or deadline unavailable");
        ordinary_target_staging_io::require_bound_original(target,limit);
    }
    struct registry {
        const pid_t creator = ::getpid();
        std::mutex mutex;
        std::size_t attempts = 0;
        std::int64_t charged_logical_bytes = 0;
        std::array<std::shared_ptr<ordinary_target_staging_io>,maximum_target_policies> originals;
    };
    static registry& originals() {
        // Intentionally process-lifetime custody, not facade/deinit cleanup.
        static registry* held = new registry;
        return *held;
    }
    struct budget {
        const pid_t creator = ::getpid();
        const std::thread::id thread = std::this_thread::get_id();
        const managed_fixed_file_ceiling ceiling = target_layout();
        const std::string path;
        const std::weak_ptr<state> original;
        std::mutex mutex;
        std::int64_t main_high = 0, journal_high = 0;
        bool growth_refused = false;
        std::atomic<bool> uncertain{false};
        budget(std::string main,std::weak_ptr<state> owner):path(std::move(main)),original(std::move(owner)) {}
        bool creator_is_current()const noexcept {
            return creator == ::getpid() && thread == std::this_thread::get_id();
        }
        bool route()const noexcept {
            if(!creator_is_current())return false;
            try {auto held=original.lock();if(!held)return false;held->require_route(held->end);return true;}
            catch(...){return false;}
        }
        managed_fixed_file_ceiling fixed_file_ceiling()const {return ceiling;}
        const std::string& main_path()const noexcept {return path;}
        void mark_uncertain()noexcept {if(creator_is_current())uncertain.store(true,std::memory_order_release);}
        bool charge(bool journal,std::int64_t end) {
            if(!route())return false;
            std::lock_guard<std::mutex> leaf(mutex);
            if(uncertain.load(std::memory_order_acquire)||growth_refused)return false;
            auto& high=journal?journal_high:main_high;const auto other=journal?main_high:journal_high;
            const auto next=std::max(high,end);const auto cap=journal?ceiling.journal_bytes:ceiling.main_bytes;
            if(end<0||high<0||other<0||next>cap||other>target_logical_bytes||next>target_logical_bytes-other){
                growth_refused=true;return false;}
            high=next;return true; // monotonic through later failure/rollback
        }

    };
    struct guard_state {
        using state = guard_state; // target-only callbacks name their including state
    struct file_wrapper {
        sqlite3_file base{};state* owner=nullptr;std::uint64_t resource=0;
    };
    static_assert(offsetof(file_wrapper,base)==0,"SQLite file must be first");
    static_assert(alignof(file_wrapper)<=alignof(sqlite3_file),"SQLite wrapper alignment");
    static_assert(sizeof(file_wrapper)<=static_cast<std::size_t>(std::numeric_limits<int>::max()),"SQLite wrapper size");
        const pid_t creator_pid = ::getpid();
        const std::thread::id creator_thread = std::this_thread::get_id();
        const std::weak_ptr<ordinary_target_staging_io::state> original;
        std::shared_ptr<budget> accounting;
        std::uint64_t registration_identity = 0;
        sqlite3_vfs* parent = nullptr;
        sqlite3_vfs vfs{};
        std::string name,parent_name,source_id;
        int sqlite_version = 0;
        std::mutex mutex;
        std::atomic<bool> uncertain{false};
        bool unsupported_refused=false;
        bool registration_attempted=false,registering=false,registered=false,retiring=false,retired=false;
        bool unregister_attempted=false,unregister_returned=false;int unregister_rc=SQLITE_MISUSE;
        std::uint64_t callbacks=0;
        using environmental_call=environmental_phase::environmental_call;
        std::shared_ptr<environmental_phase> environment; // Actual active original; historical banks stay with scope.
        std::shared_ptr<environmental_phase> terminal_environment; // Actual terminal original and callback-ingress seal.
        std::shared_ptr<state> registration_custody;
        struct terminal_registration_record {
            bool attempted=false,evidence_attempted=false,evidence_returned=false,evidence_known=false;
            bool native_entered=false,native_returned=false;int native_result=SQLITE_MISUSE;
            bool publication_attempted=false,publication_returned=false;
            bool custody_detachment_attempted=false,custody_detachment_returned=false;
            const void* detached_custody_original=nullptr;
            bool custody_release_attempted=false,custody_release_returned=false,returned=false;
            const ordinary_target_staging_connection* connection_original=nullptr;
            const void* guard_original=nullptr;const sqlite3_vfs* vfs_original=nullptr;
            std::uint64_t registration_identity=0;
            const void* scope_original=nullptr;std::uint64_t generation=0;
            std::shared_ptr<environmental_phase> environment_original;
            std::exception_ptr first_failure;
        } terminal_registration; // Fixed original guard before native registration.
        guard_state(std::shared_ptr<budget> ledger,std::weak_ptr<ordinary_target_staging_io::state> owner)
            :original(std::move(owner)),accounting(std::move(ledger)) {}
        bool current()const noexcept {
            // Native return/debt bookkeeping uses immutable creator identity,
            // not a renewed target/deadline permission for another operation.
            return creator_pid==::getpid()&&creator_thread==std::this_thread::get_id();
        }
        bool route()const noexcept {
            if(!current())return false;
            try {auto held=original.lock();if(!held)return false;auto native=held->connection.lock();
                return native&&held->connection_identity==native.get()&&native->descriptor_callback_admissible();}
            catch(...){return false;}
        }
        static state* from(sqlite3_vfs* v)noexcept {return v?static_cast<state*>(v->pAppData):nullptr;}
        static file_wrapper* wrapper(sqlite3_file* v)noexcept {return reinterpret_cast<file_wrapper*>(v);}
        using callback_kind=ordinary_target_staging_connection::descriptor_callback_kind;
        struct callback_scope {
            state* owner;bool admitted=false;
            std::shared_ptr<ordinary_target_staging_connection> connection;
            ordinary_target_staging_connection::descriptor_callback_scope original;
            callback_scope(state* value,callback_kind kind,std::uint64_t cookie=0,sqlite3_file* borrower=nullptr)noexcept
                :owner(value) {
                if(!owner||!owner->current()||!owner->route())return;
                bool counted=false;
                try {
                    connection=owner->actual_connection();
                    {std::lock_guard<std::mutex> leaf(owner->mutex);
                        if(!owner->registered||owner->retiring||owner->retired||owner->terminal_environment||
                           owner->callbacks==std::numeric_limits<std::uint64_t>::max())return;
                        ++owner->callbacks;counted=true;}
                    // Same actual original held before inline generation admission.
                    // No registration leaf spans target/native body or record destruction.
                    connection->begin_descriptor_callback(kind,cookie,borrower,original);admitted=true;
                }catch(...) {
                    owner->mark_uncertain();
                    if(counted&&owner->current()) {
                        try{std::lock_guard<std::mutex> leaf(owner->mutex);
                            if(owner->callbacks)--owner->callbacks;else owner->mark_uncertain();}
                        catch(...){owner->mark_uncertain();}
                    }
                }
            }
            void record_body_return(int rc)noexcept {original.record_body_return(rc);}
            ~callback_scope()noexcept {
                if(!admitted||!owner->current())return;
                bool returned=false;
                try{{std::lock_guard<std::mutex> leaf(owner->mutex);
                        if(!owner->callbacks){owner->mark_uncertain();return;}
                        --owner->callbacks;returned=true;}}
                catch(...){owner->mark_uncertain();}
                // This real count return is the issuer. It follows body/local
                // destruction; nested returns cannot recycle the outer bank.
                if(returned&&!original.leave())owner->mark_uncertain();
            }
            explicit operator bool()const noexcept {return admitted;}
        };
        void mark_uncertain()noexcept {
            if(current()){uncertain.store(true,std::memory_order_release);accounting->mark_uncertain();}
        }
#include "ordinary_target_descriptor_callbacks.inc"
    };
    std::shared_ptr<budget> accounting;
    std::shared_ptr<guard_state> guard;
};

const sqlite3_io_methods ordinary_target_staging_io::state::guard_state::io_methods = {
    3, close, read, write, truncate, sync, file_size, lock, unlock, check_reserved,
    file_control, sector_size, device_characteristics,
    shm_map, shm_lock, shm_barrier, shm_unmap, fetch, unfetch
};

ordinary_target_staging_io::ordinary_target_staging_io(std::shared_ptr<state> held):state_(std::move(held)) {}
ordinary_target_staging_io::~ordinary_target_staging_io() = default;
void ordinary_target_staging_io::require_bound_original(
    const std::shared_ptr<const ordinary_unpublished_group_target>& target,ordinary_launch::deadline end) {
    if(!target)throw std::runtime_error("Original target I/O role unavailable");
    target->require_staging_original(end);
}
void ordinary_target_staging_io::prepare(const std::shared_ptr<const ordinary_unpublished_group_target>& target,
    std::shared_ptr<ordinary_target_staging_io>& retained,ordinary_launch::deadline end) {
    require_bound_original(target,end);
    if(retained)throw std::runtime_error("Original target I/O preparation already retained");
    auto value=std::shared_ptr<state>(new state(target,end),[](state* original)noexcept {
        if(original->creator_current())delete original; // inherited mutex/native custody is not torn down
    });
    retained=std::shared_ptr<ordinary_target_staging_io>(new ordinary_target_staging_io(value));
    try {
        value->require_route(end);
        auto& all=state::originals();
        if(all.creator!=::getpid())throw std::runtime_error("Inherited original target I/O registry");
        {
            std::lock_guard<std::mutex> leaf(all.mutex);
            for(std::size_t n=0;n<all.attempts;++n)if(all.originals[n]->state_->target==target) {
                retained=all.originals[n];
                throw std::runtime_error("Original target I/O policy already attempted");
            }
            if(all.attempts==all.originals.size()||all.charged_logical_bytes<0||
                all.charged_logical_bytes>static_cast<std::int64_t>(maximum_target_policies)*target_logical_bytes-target_logical_bytes)
                throw std::runtime_error("Original target I/O policy census exhausted");
            value->policy_ordinal=all.attempts;
            all.originals[all.attempts++]=retained; // strong original BEFORE budget/guard/native effects
            all.charged_logical_bytes+=target_logical_bytes; // no refund, including failed preparation
            value->published=true;value->preparation_attempted=true;
        }
        auto path=target->staging_main_path(end);
        if(path.empty()||path.front()!='/'||path.find('\0')!=std::string::npos)
            throw std::runtime_error("Original target I/O path invalid");
        value->accounting=std::make_shared<state::budget>(std::move(path),value);
        value->guard=std::shared_ptr<state::guard_state>(new state::guard_state(value->accounting,value),
            [](state::guard_state* original)noexcept {
                if(original->creator_pid==::getpid())delete original;
            });
        auto held=value->guard;
        value->require_route(end);
        held->parent=sqlite3_vfs_find(nullptr);
        value->require_route(end);
        auto* parent=held->parent;
        if(!parent||parent->iVersion<1||parent->szOsFile<static_cast<int>(sizeof(sqlite3_file))||
            parent->mxPathname<=0||!parent->zName||!*parent->zName||!parent->xRandomness||!parent->xSleep||!parent->xCurrentTime)
            throw std::runtime_error("Parent VFS does not support original target staging");
        held->parent_name=parent->zName;
        value->require_route(end);held->source_id=sqlite3_sourceid();value->require_route(end);
        held->sqlite_version=sqlite3_libversion_number();value->require_route(end);
        held->name="lattice-target-staging-"+std::to_string(value->creator)+"-"+std::to_string(value->policy_ordinal)+"-"+std::to_string(reinterpret_cast<std::uintptr_t>(held.get()));
        held->registration_identity=value->policy_ordinal+1;
        auto& vfs=held->vfs;
        vfs.iVersion=3;vfs.szOsFile=static_cast<int>(sizeof(state::guard_state::file_wrapper));
        vfs.mxPathname=parent->mxPathname;vfs.zName=held->name.c_str();vfs.pAppData=held.get();
        vfs.xOpen=state::guard_state::open;vfs.xDelete=state::guard_state::remove;
        vfs.xAccess=state::guard_state::access;vfs.xFullPathname=state::guard_state::full_path;
        vfs.xDlOpen=state::guard_state::dl_open;vfs.xDlError=state::guard_state::dl_error;
        vfs.xDlSym=state::guard_state::dl_symbol;vfs.xDlClose=state::guard_state::dl_close;
        vfs.xRandomness=state::guard_state::randomness;vfs.xSleep=state::guard_state::sleep;
        vfs.xCurrentTime=state::guard_state::current_time;vfs.xGetLastError=state::guard_state::last_error;
        vfs.xCurrentTimeInt64=state::guard_state::current_time64;
        vfs.xSetSystemCall=state::guard_state::set_system_call;vfs.xGetSystemCall=state::guard_state::get_system_call;
        vfs.xNextSystemCall=state::guard_state::next_system_call;
        value->guard_prepared=true; // first prepared guard held before native registration
        value->require_route(end);
        held->registration_custody=held;held->registration_attempted=true;held->registering=true;
        value->registration_attempted=true;
        const int rc=sqlite3_vfs_register(&held->vfs,0);
        value->registration_rc=rc;value->registration_observed=true; // BEFORE current/delivery/diagnostic
        held->registered=rc==SQLITE_OK;held->registering=false;
        value->require_route(end);
        if(rc!=SQLITE_OK){held->uncertain.store(true,std::memory_order_release);held->accounting->mark_uncertain();
            throw std::runtime_error("Original target VFS registration failed");}
        value->preparation_joined=true;
    } catch(...) {
        if(value->creator_current()&&!value->first_failure)value->first_failure=std::current_exception();
        // Actual original registry and any registration custody remain. No
        // retry, slot refund, VFS replacement/unregister or diagnostic success.
        throw;
    }
}
void ordinary_target_staging_io::require_original(ordinary_launch::deadline end)const {
    auto held=state_;
    if(!held)throw std::runtime_error("Original target I/O custody missing");
    held->require_route(end);
    if(!held->published||!held->preparation_joined||held->first_failure||!held->guard||
        !held->registration_observed||held->registration_rc!=SQLITE_OK||!held->guard->registered||
        held->guard->uncertain.load(std::memory_order_acquire))
        throw std::runtime_error("Original target I/O preparation not joined");
}
void ordinary_target_staging_io::require_same_target(
    const std::shared_ptr<const ordinary_unpublished_group_target>& target,ordinary_launch::deadline end)const {
    if(!state_||state_->target!=target)throw std::runtime_error("Different original target I/O role");
    require_original(end);
}
const char* ordinary_target_staging_io::registered_vfs_name(ordinary_launch::deadline end)const {
    require_original(end);return state_->guard->name.c_str();
}
std::int64_t ordinary_target_staging_io::maximum_page_count(ordinary_launch::deadline end)const {
    require_original(end);return state_->accounting->ceiling.maximum_page_count;
}
void ordinary_target_staging_io::require_preopen_constructor(ordinary_launch::deadline end)const {
    require_original(end);
    const auto native=state_->connection.lock();
    if(!native||state_->connection_identity!=native.get())
        throw std::runtime_error("Original target descriptor connection unavailable");
    native->require_preopen_constructor(end);
}
void ordinary_target_staging_io::bind_descriptor_connection(
    const std::shared_ptr<ordinary_target_staging_connection>& actual,ordinary_launch::deadline end) {
    require_original(end);
    if(!actual||state_->connection_bound)throw std::runtime_error("Original target descriptor binding already attempted");
    actual->require_target_identity(state_->target.get(),end);
    state_->connection=actual;state_->connection_identity=actual.get();state_->connection_bound=true;
    require_original(end);
}
bool ordinary_target_staging_io::charge_descriptor(const ordinary_target_staging_connection& actual,
    bool journal,std::int64_t extent) {
    if(!state_||!state_->creator_current()||state_->connection_identity!=&actual||
       state_->connection.lock().get()!=&actual)return false;
    require_original(state_->end);return state_->accounting->charge(journal,extent);
}
void ordinary_target_staging_io::begin_descriptor_phase(const ordinary_target_staging_connection& actual,
    std::shared_ptr<environmental_phase>& retained) {
    if(!state_||!state_->creator_current()||!state_->guard||state_->connection_identity!=&actual||
       state_->connection.lock().get()!=&actual)throw std::runtime_error("Target environmental phase original unavailable");
    const auto generation=actual.active_descriptor_phase_generation();
    const auto guard=state_->guard;
    if(!guard->current())throw std::runtime_error("Target environmental creator unavailable");
    {
        std::lock_guard<std::mutex> leaf(guard->mutex);
        if(guard->callbacks||!guard->registered||guard->retiring||guard->retired)
            throw std::runtime_error("Target environmental phase overlaps native callbacks");
        if(retained&&!retained->settled) {
            // Cleanup retains SAME failed/unknown environmental bank and its finite remaining capacity.
            if(guard->environment!=retained||!retained->creator_current()||retained->connection!=&actual||
               !actual.descriptor_environment_phase_admissible(retained->generation))
                throw std::runtime_error("Target cleanup environmental predecessor unavailable");
            return;
        }
        if(guard->environment||guard->uncertain.load(std::memory_order_acquire))
            throw std::runtime_error("Target environmental previous ending unavailable");
    }
    auto next=std::make_shared<environmental_phase>(actual,generation);
    retained=next; // Actual scope custody BEFORE publication, current diagnostics or provider entry.
    {
        std::lock_guard<std::mutex> leaf(guard->mutex);
        if(guard->callbacks||guard->environment||guard->uncertain.load(std::memory_order_acquire))
            throw std::runtime_error("Target environmental publication unavailable");
        guard->environment=next;
    }
    if(!actual.descriptor_environment_phase_admissible(generation))
        throw std::runtime_error("Target environmental published original changed");
}
void ordinary_target_staging_io::settle_descriptor_phase(const ordinary_target_staging_connection& actual,
    const std::shared_ptr<environmental_phase>& retained) {
    if(!state_||!state_->creator_current()||!state_->guard||state_->connection_identity!=&actual||
       state_->connection.lock().get()!=&actual||!retained||!retained->creator_current()||retained->connection!=&actual)
        throw std::runtime_error("Target environmental settlement original unavailable");
    // Only the actual same-scope database leave or explicit checked-close
    // constructor abandonment installs this private environmental ending.
    const auto ending=actual.settling_descriptor_phase_generation();
    if(!actual.descriptor_environment_phase_admissible(retained->generation))
        throw std::runtime_error("Target environmental phase identity changed");
    const auto guard=state_->guard;
    if(!guard->current())throw std::runtime_error("Target environmental ending creator unavailable");
    std::lock_guard<std::mutex> leaf(guard->mutex);
    if(guard->callbacks||guard->environment!=retained||retained->settled||
       guard->uncertain.load(std::memory_order_acquire)||state_->accounting->uncertain.load(std::memory_order_acquire))
        throw std::runtime_error("Target environmental callbacks or debt remain");
    for(std::size_t n=0;n<retained->count;++n) {
        const auto& call=retained->calls[n];
        if(!call.attempted||!call.entered||!call.returned)
            throw std::runtime_error("Target environmental first provider ending unknown");
    }
    retained->settlement_generation=ending;retained->settled=true;
    guard->environment.reset(); // No historical result reset, VFS rebind, unregister or budget refund.
}
void ordinary_target_staging_io::settle_descriptor_callback(const ordinary_target_staging_connection& actual) {
    if(!state_||!state_->creator_current()||!state_->guard||state_->connection_identity!=&actual||
       state_->connection.lock().get()!=&actual)
        throw std::runtime_error("Target callback environmental original unavailable");
    const auto ending=actual.settling_descriptor_callback_generation();
    const auto issued=actual.settling_descriptor_callback_high_generation();
    const auto guard=state_->guard;if(!guard->current())
        throw std::runtime_error("Target callback environmental creator unavailable");
    std::shared_ptr<environmental_phase> retained;
    {std::lock_guard<std::mutex> leaf(guard->mutex);
        retained=guard->environment;
        if(!ending||guard->callbacks||!retained||!retained->creator_current()||retained->connection!=&actual||
           retained->settled||guard->uncertain.load(std::memory_order_acquire)||
           state_->accounting->uncertain.load(std::memory_order_acquire))
            throw std::runtime_error("Target callback/environmental debt remains");
    }
    if(!actual.descriptor_environment_phase_admissible(retained->generation))
        throw std::runtime_error("Target callback environmental phase changed");
    for(std::size_t n=0;n<retained->count;++n) {
        const auto& call=retained->calls[n];
        const bool result=call.kind==1?call.result>=0&&call.result<=call.argument:
            call.kind==2?call.result>=0:(call.kind==3||call.kind==4)&&call.result==SQLITE_OK;
        if(!call.attempted||!call.entered||!call.returned||!call.callback_generation||
           call.callback_generation<ending||call.callback_generation>issued||!result)
            throw std::runtime_error("Target callback first environmental ending unknown/failed");
    }
    // Same creator thread, actual zero callback count and actual outer return.
    // Only successful temporary facts are released; failed banks never reset.
    for(std::size_t n=0;n<retained->count;++n)retained->calls[n]=environmental_phase::environmental_call{};
    retained->count=0;
}
bool ordinary_target_staging_io::callbacks_joined(const ordinary_target_staging_connection& actual)const noexcept {
    try {
        if(!state_||!state_->creator_current()||!state_->guard||state_->connection_identity!=&actual||
           state_->connection.lock().get()!=&actual)return false;
        const auto guard=state_->guard;if(!guard->current())return false;
        std::lock_guard<std::mutex> leaf(guard->mutex);return guard->callbacks==0;
    }catch(...){return false;}
}
bool ordinary_target_staging_io::terminal_environment_known(const ordinary_target_staging_connection& actual,
    const std::shared_ptr<environmental_phase>& retained)const noexcept {
    try {
        if(!state_||!state_->creator_current()||!state_->preparation_joined||!state_->registration_observed||
           state_->registration_rc!=SQLITE_OK||!state_->guard||!state_->accounting||
           state_->connection_identity!=&actual||state_->connection.lock().get()!=&actual||
           !retained||!retained->creator_current()||retained->connection!=&actual)return false;
        const auto guard=state_->guard;
        if(!guard->current()||guard->original.lock()!=state_||
           !actual.terminal_descriptor_calls_known(retained->generation))return false;
        std::lock_guard<std::mutex> leaf(guard->mutex);
        if(guard->callbacks||guard->environment!=retained||retained->settled||
           !guard->registered||guard->retiring||guard->retired||guard->unregister_attempted||
           retained->count>retained->calls.size())return false;
        for(std::size_t at=0;at<retained->count;++at) {
            const auto& call=retained->calls[at];
            if(!call.attempted||call.kind<1||call.kind>4||
               ((call.kind==1||call.kind==2)&&call.argument<0)||
               ((call.kind==3||call.kind==4)&&call.argument!=0)||
               (call.entered&&(!call.returned||!call.callback_generation))||
               (!call.entered&&call.returned))return false;
        }
        // A known returned provider error stays an error. No ordinary settled
        // bit, failed bank, guard/accounting uncertainty, reference or credit
        // is cleared. Actual terminal settlement/unregister remains separate.
        return true;
    }catch(...){return false;}
}
bool ordinary_target_staging_io::terminal_environment_ended(const ordinary_target_staging_connection& actual,
    const std::shared_ptr<environmental_phase>& retained)const noexcept {
    try {
        if(!state_||!state_->creator_current()||!state_->guard||state_->connection_identity!=&actual||
           state_->connection.lock().get()!=&actual||!retained||!retained->creator_current()||
           retained->connection!=&actual)return false;
        const auto guard=state_->guard;
        if(!guard->current()||guard->original.lock()!=state_)return false;
        const auto ending=actual.terminal_environment_ending_generation(*this,retained.get(),retained->generation);
        if(!ending)return false;
        std::lock_guard<std::mutex> leaf(guard->mutex);
        const auto& receipt=retained->terminal_ending;
        return receipt.attempted&&receipt.evidence_returned&&receipt.evidence_known&&
            receipt.cleanup_generation==ending&&receipt.detachment_attempted&&receipt.detachment_returned&&
            receipt.detached&&receipt.returned&&receipt.joined&&!retained->settled&&
            !guard->callbacks&&!guard->environment&&guard->terminal_environment==retained&&
            guard->registered&&!guard->retiring&&!guard->retired&&!guard->unregister_attempted;
    }catch(...){return false;}
}
bool ordinary_target_staging_io::end_terminal_environment(const ordinary_target_staging_connection& actual,
    const std::shared_ptr<environmental_phase>& retained)noexcept {
    try {
        if(!state_||!state_->creator_current()||!state_->guard||state_->connection_identity!=&actual||
           state_->connection.lock().get()!=&actual||!retained||!retained->creator_current()||
           retained->connection!=&actual)return false;
        const auto guard=state_->guard;
        if(!guard->current()||guard->original.lock()!=state_)return false;
        auto& receipt=retained->terminal_ending;
        if(receipt.attempted)return terminal_environment_ended(actual,retained); // Observe only; no retry.
        {
            std::lock_guard<std::mutex> leaf(guard->mutex);
            if(guard->environment!=retained||guard->terminal_environment||retained->settled||
               !guard->registered||guard->retiring||guard->retired||guard->unregister_attempted)return false;
            receipt.attempted=true; // Actual same bound phase before evidence/effect.
        }
        receipt.evidence_known=terminal_environment_known(actual,retained);
        receipt.evidence_returned=true;
        if(!receipt.evidence_known){receipt.returned=true;return false;}
        const auto ending=actual.terminal_environment_ending_generation(*this,retained.get(),retained->generation);
        receipt.cleanup_generation=ending;
        if(!ending){receipt.returned=true;return false;}
        // Recheck the actual same native/call/provider proofs before detaching.
        // Both connection calls precede the IO leaf; no nested leaf lock is held.
        if(!terminal_environment_known(actual,retained)||
           actual.terminal_environment_ending_generation(*this,retained.get(),retained->generation)!=ending) {
            receipt.returned=true;return false;
        }
        {
            std::lock_guard<std::mutex> leaf(guard->mutex);
            if(guard->callbacks||guard->environment!=retained||guard->terminal_environment||retained->settled||
               !guard->registered||guard->retiring||guard->retired||guard->unregister_attempted) {
                receipt.returned=true;return false;
            }
            // Actual checked SQL/VM/call/provider ending and zero callbacks
            // precede this strong retention and irreversible ingress seal.
            guard->terminal_environment=retained;
            receipt.detachment_attempted=true;
            guard->environment.reset(); // Scope/prior/terminal guard retain THIS historical bank.
            receipt.detachment_returned=true;
            receipt.detached=!guard->environment&&guard->terminal_environment==retained;
            receipt.returned=true;receipt.joined=receipt.detached;
            // No ordinary settled bit, result bank, uncertainty, failed leave,
            // VFS registration, payload/lease or admission credit is changed.
        }
        return terminal_environment_ended(actual,retained);
    }catch(...){return false;}
}

bool ordinary_target_staging_io::terminal_environment_historical_known(const ordinary_target_staging_connection& actual,
    const std::shared_ptr<environmental_phase>& retained)const noexcept {
    try {
        if(!state_||!state_->creator_current()||!state_->preparation_joined||!state_->registration_observed||
           state_->registration_rc!=SQLITE_OK||!state_->guard||!state_->accounting||!state_->connection_bound||
           state_->connection_identity!=&actual||state_->connection.lock().get()!=&actual||
           !retained||!retained->creator_current()||retained->connection!=&actual)return false;
        const auto guard=state_->guard;const auto accounting=state_->accounting;
        if(!guard->current()||guard->original.lock()!=state_||guard->accounting!=accounting||
           !accounting->creator_is_current()||accounting->original.lock()!=state_)return false;
        // Both real Native/VM/callback observers run before the IO leaf. They
        // do not require live MAIN and still require the actual active cleanup.
        if(!actual.terminal_descriptor_calls_known(retained->generation))return false;
        const auto ending=actual.terminal_environment_ending_generation(*this,retained.get(),retained->generation);
        if(!ending)return false;
        std::lock_guard<std::mutex> leaf(guard->mutex);
        const auto& receipt=retained->terminal_ending;
        if(!receipt.attempted||!receipt.evidence_returned||!receipt.evidence_known||
           receipt.cleanup_generation!=ending||!receipt.detachment_attempted||!receipt.detachment_returned||
           !receipt.detached||!receipt.returned||!receipt.joined||retained->settled||guard->callbacks||guard->environment||
           guard->terminal_environment!=retained||!guard->registered||guard->retiring||guard->retired||
           guard->unregister_attempted||retained->count>retained->calls.size())return false;
        for(std::size_t at=0;at<retained->count;++at) {
            const auto& call=retained->calls[at];
            if(!call.attempted||call.kind<1||call.kind>4||
               ((call.kind==1||call.kind==2)&&call.argument<0)||((call.kind==3||call.kind==4)&&call.argument!=0)||
               (call.entered&&(!call.returned||!call.callback_generation))||(!call.entered&&call.returned))return false;
        }
        return true; // Strong historical bank/first detachment, never file/VFS/leave authority.
    }catch(...){return false;}
}
std::int64_t ordinary_target_staging_io::terminal_descriptor_extent_limit(
    const ordinary_target_staging_connection& actual,const void* resource_original)const noexcept {
    try {
        if(!state_||!state_->creator_current()||!state_->accounting||!state_->connection_bound||
           state_->connection_identity!=&actual||state_->connection.lock().get()!=&actual)return -1;
        const auto accounting=state_->accounting;
        if(!accounting->creator_is_current()||accounting->original.lock()!=state_)return -1;
        // Exact private collected-original class DATA, derived outside any IO
        // leaf; no caller Boolean/path chooses or manufactures a ceiling.
        const int kind=actual.terminal_general_file_kind(*this,resource_original);
        const auto cap=kind==0?accounting->ceiling.main_bytes:kind==1?accounting->ceiling.journal_bytes:-1;
        return cap>0?cap:-1; // Const existing accounting, no growth permission or uncertainty reset.
    }catch(...){return -1;}
}

bool ordinary_target_staging_io::terminal_environment_after_vfs_known(const ordinary_target_staging_connection& actual,
    const std::shared_ptr<environmental_phase>& retained)const noexcept {
    try {
        if(!state_||!state_->creator_current()||!state_->preparation_joined||!state_->registration_observed||
           state_->registration_rc!=SQLITE_OK||!state_->guard||!state_->accounting||!state_->connection_bound||
           state_->connection_identity!=&actual||state_->connection.lock().get()!=&actual||
           !retained||!retained->creator_current()||retained->connection!=&actual)return false;
        const auto held=state_->guard;const auto accounting=state_->accounting;
        if(!held->current()||held->original.lock()!=state_||held->accounting!=accounting||
           !accounting->creator_is_current()||accounting->original.lock()!=state_)return false;
        // These real observers require zero callbacks and same active cleanup,
        // but do not require registered=true. No General/IO recursive grant.
        if(!actual.terminal_descriptor_calls_known(retained->generation))return false;
        const auto ending=actual.terminal_environment_ending_generation(*this,retained.get(),retained->generation);
        const auto& first=held->terminal_registration;
        if(!ending||!actual.terminal_vfs_scope_matches(*this,first.scope_original,retained.get(),ending))return false;
        std::lock_guard<std::mutex> leaf(held->mutex);
        const auto& environment=retained->terminal_ending;
        if(!first.attempted||!first.evidence_attempted||!first.evidence_returned||!first.evidence_known||
           first.connection_original!=&actual||first.environment_original!=retained||first.generation!=ending||
           first.guard_original!=held.get()||first.vfs_original!=&held->vfs||!first.registration_identity||
           first.registration_identity!=held->registration_identity||first.registration_identity!=state_->policy_ordinal+1||
           !first.native_entered||!first.native_returned||first.native_result!=SQLITE_OK||
           !first.publication_attempted||!first.publication_returned||
           !first.custody_detachment_attempted||!first.custody_detachment_returned||first.detached_custody_original!=held.get()||
           !first.custody_release_attempted||!first.custody_release_returned||!first.returned||
           !held->unregister_attempted||!held->unregister_returned||held->unregister_rc!=first.native_result||
           !held->registration_attempted||held->registering||held->registered||held->retiring||!held->retired||
           held->registration_custody||held->callbacks||held->environment||
           held->terminal_environment!=retained||retained->settled||retained->count>retained->calls.size()||
           !environment.attempted||!environment.evidence_returned||!environment.evidence_known||
           environment.cleanup_generation!=ending||!environment.detachment_attempted||!environment.detachment_returned||
           !environment.detached||!environment.returned||!environment.joined)return false;
        for(std::size_t at=0;at<retained->count;++at) {
            const auto& call=retained->calls[at];
            if(!call.attempted||call.kind<1||call.kind>4||((call.kind==1||call.kind==2)&&call.argument<0)||
               ((call.kind==3||call.kind==4)&&call.argument!=0)||
               (call.entered&&(!call.returned||!call.callback_generation))||(!call.entered&&call.returned))return false;
        }
        return true; // Actual unregister/guard/custody/environment history only; files still independently observed.
    }catch(...){return false;}
}
std::int64_t ordinary_target_staging_io::terminal_descriptor_extent_limit_after_vfs(
    const ordinary_target_staging_connection& actual,const void* resource_original)const noexcept {
    try {
        if(!state_||!state_->creator_current()||!state_->accounting||!state_->connection_bound||
           state_->connection_identity!=&actual||state_->connection.lock().get()!=&actual)return -1;
        const auto accounting=state_->accounting;
        if(!accounting->creator_is_current()||accounting->original.lock()!=state_)return -1;
        // Exact private collected-original class DATA, derived outside any IO
        // leaf; no caller Boolean/path chooses or manufactures a ceiling.
        const int kind=actual.terminal_general_file_kind_after_vfs(*this,resource_original);
        const auto cap=kind==0?accounting->ceiling.main_bytes:kind==1?accounting->ceiling.journal_bytes:-1;
        return cap>0?cap:-1; // Const existing accounting, no growth permission or uncertainty reset.
    }catch(...){return -1;}
}

bool ordinary_target_staging_io::terminal_registration_ended(const ordinary_target_staging_connection& actual,
    const std::shared_ptr<environmental_phase>& retained,const void* scope_original,std::uint64_t generation)const noexcept {
    try {
        if(!state_||!state_->creator_current()||!state_->guard||state_->connection_identity!=&actual||
           state_->connection.lock().get()!=&actual||!retained||!retained->creator_current()||retained->connection!=&actual||
           !actual.terminal_vfs_scope_matches(*this,scope_original,retained.get(),generation)||
           !terminal_environment_after_vfs_known(actual,retained))return false;
        // Full actual General/RAW/physical receipts are re-observed outside
        // the IO leaf, through the distinct valid-after-unregister family.
        if(!actual.terminal_vfs_files_after_known(*this,scope_original,retained.get(),generation))return false;
        const auto held=state_->guard;
        std::lock_guard<std::mutex> leaf(held->mutex);const auto& first=held->terminal_registration;
        return first.scope_original==scope_original&&first.generation==generation&&first.environment_original==retained&&
            first.connection_original==&actual&&first.guard_original==held.get()&&first.vfs_original==&held->vfs&&
            first.registration_identity&&first.registration_identity==held->registration_identity&&
            first.registration_identity==state_->policy_ordinal+1&&
            first.attempted&&first.evidence_attempted&&first.evidence_returned&&first.evidence_known&&
            first.native_entered&&first.native_returned&&first.native_result==SQLITE_OK&&
            first.publication_attempted&&first.publication_returned&&first.custody_detachment_attempted&&first.custody_detachment_returned&&
            first.detached_custody_original==held.get()&&first.custody_release_attempted&&first.custody_release_returned&&
            first.returned&&!held->callbacks&&!held->environment&&held->terminal_environment==retained&&
            held->unregister_attempted&&held->unregister_returned&&held->unregister_rc==SQLITE_OK&&
            held->registration_attempted&&!held->registering&&!held->registered&&!held->retiring&&held->retired&&!held->registration_custody;
    }catch(...){return false;}
}
bool ordinary_target_staging_io::terminal_environment_after_scope_known(const ordinary_target_staging_connection& actual,
    const std::shared_ptr<environmental_phase>& retained)const noexcept {
    try {
        if(!state_||!state_->creator_current()||!state_->preparation_joined||!state_->registration_observed||
           state_->registration_rc!=SQLITE_OK||!state_->guard||!state_->accounting||!state_->connection_bound||
           state_->connection_identity!=&actual||state_->connection.lock().get()!=&actual||
           !retained||!retained->creator_current()||retained->connection!=&actual)return false;
        const auto held=state_->guard;const auto accounting=state_->accounting;
        if(!held->current()||held->original.lock()!=state_||held->accounting!=accounting||
           !accounting->creator_is_current()||accounting->original.lock()!=state_)return false;
        // These real observers require zero callbacks and same retained cleanup history,
        // but do not require registered=true. No General/IO recursive grant.
        if(!actual.terminal_descriptor_calls_after_scope_known(retained->generation))return false;
        const auto ending=actual.terminal_environment_ending_generation_after_scope(*this,retained.get(),retained->generation);
        const auto& first=held->terminal_registration;
        if(!ending||!actual.terminal_vfs_scope_after_scope_matches(*this,first.scope_original,retained.get(),ending))return false;
        std::lock_guard<std::mutex> leaf(held->mutex);
        const auto& environment=retained->terminal_ending;
        if(!first.attempted||!first.evidence_attempted||!first.evidence_returned||!first.evidence_known||
           first.connection_original!=&actual||first.environment_original!=retained||first.generation!=ending||
           first.guard_original!=held.get()||first.vfs_original!=&held->vfs||!first.registration_identity||
           first.registration_identity!=held->registration_identity||first.registration_identity!=state_->policy_ordinal+1||
           !first.native_entered||!first.native_returned||first.native_result!=SQLITE_OK||
           !first.publication_attempted||!first.publication_returned||
           !first.custody_detachment_attempted||!first.custody_detachment_returned||first.detached_custody_original!=held.get()||
           !first.custody_release_attempted||!first.custody_release_returned||!first.returned||
           !held->unregister_attempted||!held->unregister_returned||held->unregister_rc!=first.native_result||
           !held->registration_attempted||held->registering||held->registered||held->retiring||!held->retired||
           held->registration_custody||held->callbacks||held->environment||
           held->terminal_environment!=retained||retained->settled||retained->count>retained->calls.size()||
           !environment.attempted||!environment.evidence_returned||!environment.evidence_known||
           environment.cleanup_generation!=ending||!environment.detachment_attempted||!environment.detachment_returned||
           !environment.detached||!environment.returned||!environment.joined)return false;
        for(std::size_t at=0;at<retained->count;++at) {
            const auto& call=retained->calls[at];
            if(!call.attempted||call.kind<1||call.kind>4||((call.kind==1||call.kind==2)&&call.argument<0)||
               ((call.kind==3||call.kind==4)&&call.argument!=0)||
               (call.entered&&(!call.returned||!call.callback_generation))||(!call.entered&&call.returned))return false;
        }
        return true; // Actual unregister/guard/custody/environment history only; files still independently observed.
    }catch(...){return false;}
}

std::int64_t ordinary_target_staging_io::terminal_descriptor_extent_limit_after_scope(
    const ordinary_target_staging_connection& actual,const void* resource_original)const noexcept {
    try {
        if(!state_||!state_->creator_current()||!state_->accounting||!state_->connection_bound||
           state_->connection_identity!=&actual||state_->connection.lock().get()!=&actual)return -1;
        const auto accounting=state_->accounting;
        if(!accounting->creator_is_current()||accounting->original.lock()!=state_)return -1;
        // Exact private collected-original class DATA, derived outside any IO
        // leaf; no caller Boolean/path chooses or manufactures a ceiling.
        const int kind=actual.terminal_general_file_kind_after_scope(*this,resource_original);
        const auto cap=kind==0?accounting->ceiling.main_bytes:kind==1?accounting->ceiling.journal_bytes:-1;
        return cap>0?cap:-1; // Const existing accounting, no growth permission or uncertainty reset.
    }catch(...){return -1;}
}

bool ordinary_target_staging_io::terminal_registration_after_scope_known(const ordinary_target_staging_connection& actual,
    const std::shared_ptr<environmental_phase>& retained,const void* scope_original,std::uint64_t generation)const noexcept {
    try {
        if(!state_||!state_->creator_current()||!state_->guard||state_->connection_identity!=&actual||
           state_->connection.lock().get()!=&actual||!retained||!retained->creator_current()||retained->connection!=&actual||
           !actual.terminal_vfs_scope_after_scope_matches(*this,scope_original,retained.get(),generation)||
           !terminal_environment_after_scope_known(actual,retained))return false;
        // Full actual General/RAW/physical receipts are re-observed outside
        // the IO leaf, through the distinct valid-after-unregister family.
        if(!actual.terminal_vfs_files_after_scope_known(*this,scope_original,retained.get(),generation))return false;
        const auto held=state_->guard;
        std::lock_guard<std::mutex> leaf(held->mutex);const auto& first=held->terminal_registration;
        return first.scope_original==scope_original&&first.generation==generation&&first.environment_original==retained&&
            first.connection_original==&actual&&first.guard_original==held.get()&&first.vfs_original==&held->vfs&&
            first.registration_identity&&first.registration_identity==held->registration_identity&&
            first.registration_identity==state_->policy_ordinal+1&&
            first.attempted&&first.evidence_attempted&&first.evidence_returned&&first.evidence_known&&
            first.native_entered&&first.native_returned&&first.native_result==SQLITE_OK&&
            first.publication_attempted&&first.publication_returned&&first.custody_detachment_attempted&&first.custody_detachment_returned&&
            first.detached_custody_original==held.get()&&first.custody_release_attempted&&first.custody_release_returned&&
            first.returned&&!held->callbacks&&!held->environment&&held->terminal_environment==retained&&
            held->unregister_attempted&&held->unregister_returned&&held->unregister_rc==SQLITE_OK&&
            held->registration_attempted&&!held->registering&&!held->registered&&!held->retiring&&held->retired&&!held->registration_custody;
    }catch(...){return false;}
}

bool ordinary_target_staging_io::retire_terminal_registration(const ordinary_target_staging_connection& actual,
    const std::shared_ptr<environmental_phase>& retained,const void* scope_original,std::uint64_t generation)noexcept {
    const auto native=state_;
    if(!native||!native->creator_current()||!native->guard||native->connection_identity!=&actual||
       native->connection.lock().get()!=&actual)return false;
    const auto held=native->guard;auto& first=held->terminal_registration;
    if(!held->current()||held->original.lock()!=native||!native->preparation_joined||!native->connection_bound||
       !native->registration_observed||native->registration_rc!=SQLITE_OK||!native->accounting||
       !native->accounting->creator_is_current()||native->accounting->original.lock()!=native||
       held->accounting!=native->accounting||!held->registration_identity||held->registration_identity!=native->policy_ordinal+1)return false;
    if(first.attempted)return terminal_registration_ended(actual,retained,scope_original,generation); // Never retry any first effect.
    try {
        {
            std::lock_guard<std::mutex> leaf(held->mutex);
            if(!held->registration_attempted||held->registering||!held->registered||held->retiring||held->retired||
               held->unregister_attempted||held->callbacks||held->environment||
               held->terminal_environment!=retained||held->registration_custody.get()!=held.get())return false;
            first.connection_original=&actual;first.guard_original=held.get();first.vfs_original=&held->vfs;
            first.registration_identity=held->registration_identity;first.scope_original=scope_original;first.generation=generation;
            first.environment_original=retained;first.attempted=true;
        }
        first.evidence_attempted=true;
        first.evidence_known=retained&&actual.terminal_vfs_files_before_known(*this,scope_original,retained.get(),generation);
        first.evidence_returned=true;
        if(!first.evidence_known){first.returned=true;return false;}
        // Reobserve the actual first SQL/VM/UDF/RAW/files/provider/zero-callback
        // evidence before irreversible unregister, outside the IO leaf.
        if(!actual.terminal_vfs_files_before_known(*this,scope_original,retained.get(),generation)) {first.returned=true;return false;}
        {
            std::lock_guard<std::mutex> leaf(held->mutex);
            if(!held->registration_attempted||held->registering||!held->registered||held->retiring||held->retired||
               held->unregister_attempted||held->callbacks||held->environment||
               held->terminal_environment!=retained||held->registration_custody.get()!=held.get()){first.returned=true;return false;}
            held->retiring=true;held->unregister_attempted=true;
        }
        first.native_entered=true;
        const int rc=sqlite3_vfs_unregister(&held->vfs); // Actual first Native call; no provider leaf held.
        held->unregister_rc=rc;held->unregister_returned=true;first.native_result=rc;first.native_returned=true;
        if(!held->current()||rc!=SQLITE_OK){held->mark_uncertain();first.returned=true;return false;}
        std::shared_ptr<state::guard_state> released;
        first.publication_attempted=true;
        {
            std::lock_guard<std::mutex> leaf(held->mutex);
            if(held->callbacks||held->environment||held->terminal_environment!=retained||
               held->registration_custody.get()!=held.get()){held->mark_uncertain();first.returned=true;return false;}
            held->registered=false;held->retired=true;held->retiring=false;
            first.custody_detachment_attempted=true;
            released=std::move(held->registration_custody); // Move only; no last-reference destruction under the leaf.
            first.detached_custody_original=released.get();first.custody_detachment_returned=true;
        }
        first.publication_returned=true;
        first.custody_release_attempted=true;released.reset();first.custody_release_returned=true;
        first.returned=true;
        return terminal_registration_ended(actual,retained,scope_original,generation);
    }catch(...){if(!first.first_failure)first.first_failure=std::current_exception();held->mark_uncertain();first.returned=true;return false;}
    // VFS only. Provider/native/RAW/files/error originals remain retained;
    // no payload/input/lease/scope/owner/FIFO join or admission/refund occurs.
}

bool ordinary_target_staging_io::retire_descriptor_registration(const ordinary_target_staging_connection& actual)noexcept {
    try {
        if(!callbacks_joined(actual)||!actual.descriptor_retirement_admissible())return false;
        const auto held=state_->guard;
        {
            std::lock_guard<std::mutex> leaf(held->mutex);
            if(!held->registered||held->retiring||held->retired||held->unregister_attempted)return false;
            held->retiring=true;held->unregister_attempted=true;
        }
        // Actual creator-only native ending: no provider mutex is held and no
        // expired target authority is renewed to settle this registration.
        const int rc=sqlite3_vfs_unregister(&held->vfs);
        held->unregister_rc=rc;held->unregister_returned=true;
        if(!held->current()||rc!=SQLITE_OK){held->mark_uncertain();return false;}
        {
            std::lock_guard<std::mutex> leaf(held->mutex);
            if(held->callbacks){held->mark_uncertain();return false;}
            held->registered=false;held->retired=true;held->retiring=false;
        }
        held->registration_custody.reset();return true; // No registry/byte refund.
    }catch(...){return false;}
}

#ifdef LATTICE_MANAGED_TARGET_RECLAMATION_FIXTURE
nlohmann::json ordinary_target_reclamation_fixture::registration_ending(const ordinary_target_staging_io& io) {
    const auto state=io.state_;
    if(!state||!state->creator_current()||!state->guard)
        throw std::runtime_error("constructor fixture actual IO ending unavailable");
    const auto guard=state->guard;
    std::lock_guard<std::mutex> lock(guard->mutex);
    return {{"attempted",guard->unregister_attempted},{"returned",guard->unregister_returned},
        {"rc",guard->unregister_rc},{"callbacks",guard->callbacks},{"registered",guard->registered},
        {"retired",guard->retired},{"retiring",guard->retiring},
        {"uncertain",guard->uncertain.load(std::memory_order_acquire)}};
}
#endif
#include "ordinary_target_scope_environment_history.inc"
#else
struct ordinary_target_staging_io::state {};
struct ordinary_target_staging_io::environmental_phase {};
bool ordinary_target_staging_io::prepare_terminal_scope_environment_history(const ordinary_target_staging_connection&,
    const std::shared_ptr<environmental_phase>&,const std::shared_ptr<ordinary_resource_reservation<true>>&,
    std::shared_ptr<ordinary_target_scope_environment_record>&)const noexcept{return false;}
bool ordinary_target_staging_io::bind_terminal_scope_environment_history(const ordinary_target_staging_connection&,
    const std::shared_ptr<environmental_phase>&,const std::shared_ptr<ordinary_target_scope_environment_record>&)noexcept{return false;}

ordinary_target_staging_io::ordinary_target_staging_io(std::shared_ptr<state> held):state_(std::move(held)) {}
ordinary_target_staging_io::~ordinary_target_staging_io() = default;
void ordinary_target_staging_io::require_bound_original(const std::shared_ptr<const ordinary_unpublished_group_target>&,ordinary_launch::deadline) {
    throw std::runtime_error("Original target staging unavailable on this platform");
}
void ordinary_target_staging_io::prepare(const std::shared_ptr<const ordinary_unpublished_group_target>&,
    std::shared_ptr<ordinary_target_staging_io>&,ordinary_launch::deadline) {
    throw std::runtime_error("Original target staging unavailable on this platform");
}
void ordinary_target_staging_io::require_original(ordinary_launch::deadline)const {require_bound_original({},{});}
void ordinary_target_staging_io::require_same_target(const std::shared_ptr<const ordinary_unpublished_group_target>&,ordinary_launch::deadline end)const {require_original(end);}
const char* ordinary_target_staging_io::registered_vfs_name(ordinary_launch::deadline end)const {require_original(end);return nullptr;}
std::int64_t ordinary_target_staging_io::maximum_page_count(ordinary_launch::deadline end)const {require_original(end);return 0;}
void ordinary_target_staging_io::require_preopen_constructor(ordinary_launch::deadline end)const {require_original(end);}
void ordinary_target_staging_io::bind_descriptor_connection(const std::shared_ptr<ordinary_target_staging_connection>&,ordinary_launch::deadline end){require_original(end);}
bool ordinary_target_staging_io::charge_descriptor(const ordinary_target_staging_connection&,bool,std::int64_t){return false;}
void ordinary_target_staging_io::begin_descriptor_phase(const ordinary_target_staging_connection&,std::shared_ptr<environmental_phase>&){require_original({});}
void ordinary_target_staging_io::settle_descriptor_phase(const ordinary_target_staging_connection&,const std::shared_ptr<environmental_phase>&){require_original({});}
void ordinary_target_staging_io::settle_descriptor_callback(const ordinary_target_staging_connection&){require_original({});}
bool ordinary_target_staging_io::callbacks_joined(const ordinary_target_staging_connection&)const noexcept{return false;}
bool ordinary_target_staging_io::terminal_environment_known(const ordinary_target_staging_connection&,const std::shared_ptr<environmental_phase>&)const noexcept{return false;}
bool ordinary_target_staging_io::end_terminal_environment(const ordinary_target_staging_connection&,const std::shared_ptr<environmental_phase>&)noexcept{return false;}
bool ordinary_target_staging_io::terminal_environment_ended(const ordinary_target_staging_connection&,const std::shared_ptr<environmental_phase>&)const noexcept{return false;}
bool ordinary_target_staging_io::terminal_environment_historical_known(const ordinary_target_staging_connection&,const std::shared_ptr<environmental_phase>&)const noexcept{return false;}
std::int64_t ordinary_target_staging_io::terminal_descriptor_extent_limit(const ordinary_target_staging_connection&,const void*)const noexcept{return -1;}
bool ordinary_target_staging_io::terminal_environment_after_scope_known(const ordinary_target_staging_connection&,const std::shared_ptr<environmental_phase>&)const noexcept{return false;}
std::int64_t ordinary_target_staging_io::terminal_descriptor_extent_limit_after_scope(const ordinary_target_staging_connection&,const void*)const noexcept{return -1;}
bool ordinary_target_staging_io::terminal_registration_after_scope_known(const ordinary_target_staging_connection&,const std::shared_ptr<environmental_phase>&,const void*,std::uint64_t)const noexcept{return false;}
bool ordinary_target_staging_io::terminal_environment_after_vfs_known(const ordinary_target_staging_connection&,const std::shared_ptr<environmental_phase>&)const noexcept{return false;}
std::int64_t ordinary_target_staging_io::terminal_descriptor_extent_limit_after_vfs(const ordinary_target_staging_connection&,const void*)const noexcept{return -1;}
bool ordinary_target_staging_io::retire_terminal_registration(const ordinary_target_staging_connection&,const std::shared_ptr<environmental_phase>&,const void*,std::uint64_t)noexcept{return false;}
bool ordinary_target_staging_io::terminal_registration_ended(const ordinary_target_staging_connection&,const std::shared_ptr<environmental_phase>&,const void*,std::uint64_t)const noexcept{return false;}
bool ordinary_target_staging_io::retire_descriptor_registration(const ordinary_target_staging_connection&)noexcept{return false;}

#endif
} // namespace lattice::detail
