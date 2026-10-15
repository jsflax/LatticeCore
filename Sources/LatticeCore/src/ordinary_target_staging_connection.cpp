#include "ordinary_target_staging_connection.hpp"
#include "ordinary_target_history_spool.hpp"
#include "ordinary_managed_history.hpp"
#include "ordinary_target_staging_io.hpp"
#ifdef LATTICE_MANAGED_TARGET_RECLAMATION_FIXTURE
#include "ordinary_target_reclamation_fixture.hpp"
#endif
#include "ordinary_group_target_schema.hpp"
#include "ordinary_group_target_receiving_owner.hpp"
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
#include "ordinary_receiver_reclamation_fixture.hpp"
#endif
#include "ordinary_history_receive.hpp"
#include "../include/lattice/db.hpp"
#include <array>
#include <initializer_list>
#include <cstdint>
#include <cstring>
#include <exception>
#include <mutex>
#include <limits>
#include <algorithm>
#include <optional>
#include "database_retirement.hpp"
#include <stdexcept>
#include <thread>
#include <utility>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <cerrno>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>
#define LATTICE_TARGET_MAIN_POSIX 1
#endif

namespace lattice::detail {
namespace {
[[noreturn]] void birth_fail(const char* reason) { throw std::runtime_error(reason); }
constexpr const char* main_leaf="store.sqlite";
struct birth_registry {
    const ordinary_launch::process_identity process=ordinary_launch::current_process();
    std::mutex mutex;
    std::size_t charged=0;
    std::array<std::shared_ptr<const ordinary_unpublished_group_target>,256> targets;
    std::array<std::shared_ptr<ordinary_target_staging_connection>,256> originals;
};
birth_registry& registry() {
    // No refund or static teardown: failed allocations and every returned
    // original remain charged until this actual process ends.
    static auto* retained=new birth_registry;
    if(ordinary_launch::current_process()!=retained->process)
        birth_fail("target MAIN registry inherited or replaced");
    return *retained;
}
}
struct ordinary_target_staging_connection::state {
    const std::shared_ptr<const ordinary_unpublished_group_target> target;
    const std::shared_ptr<ordinary_target_staging_io> io;
    const ordinary_launch::process_identity process=ordinary_launch::current_process();
    const std::thread::id thread=std::this_thread::get_id();
    const ordinary_launch::deadline end;
    const std::size_t registry_slot;
    bool row_import_attempted=false;std::uint64_t row_import_scopes=0;
    bool import_suspension_attempted=false;
    bool history_chain_attempted=false;std::uint64_t history_chain_scopes=0;
    bool history_spool_attempted=false;
    struct raw_allocation_receipt {
        bool attempted=false,returned=false,exception_returned=false;
        std::exception_ptr exception_original;
    } history_spool_allocation; // Actual allocation-only return before any RAW Native birth.
    bool raw_ending_attempted=false,raw_ending_returned=false;
    terminal_raw_retirement_result raw_ending_result=terminal_raw_retirement_result::retained_debt;
    std::shared_ptr<ordinary_target_history_spool> history_spool;
    struct terminal_vfs_disposition {
        bool attempted=false,returned=false;
        terminal_vfs_retirement_result result=terminal_vfs_retirement_result::retained_debt;
        const void* scope_original=nullptr;std::uint64_t generation=0;
        std::shared_ptr<ordinary_target_staging_io::environmental_phase> environment_original;
        std::exception_ptr first_failure;
    } terminal_vfs; // Separate first custody before any MAIN/registration effect.
    struct terminal_scope_disposition {
        bool attempted=false,evidence_attempted=false,evidence_returned=false,evidence_known=false;
        bool active_detachment_attempted=false,active_detachment_returned=false;
        bool identity_detachment_attempted=false,identity_detachment_returned=false,returned=false;
        terminal_scope_retirement_result result=terminal_scope_retirement_result::retained_debt;
        const ::lattice::database* shell_original=nullptr;
        std::shared_ptr<database_scope::scope_state> scope_original;
        std::uint64_t generation=0;
        std::shared_ptr<ordinary_target_staging_io::environmental_phase> environment_original;
        std::shared_ptr<database_retirement_state> retirement_original;
        std::exception_ptr first_failure;
    } terminal_scope; // Fixed before MAIN birth, strong exact history before real binding reset.
    bool bound=false;
    std::string fixed_main_path; // Derived once from actual original before MAIN birth.
    int member_fd=-1;
    bool member_fstat_attempted=false;
    int member_fstat_rc=-1,member_fstat_errno=0;
    std::uint64_t member_device=0,member_inode=0;
    bool open_attempted=false;
    int main_fd=-1,open_rc=-1,open_errno=0;
    bool fstat_attempted=false;
    int fstat_rc=-1,fstat_errno=0;
    std::uint64_t device=0,inode=0;
    bool birth_verified=false;
    bool shell_claimed=false;
    std::shared_ptr<::lattice::database> shell;
    std::exception_ptr first_failure;
    enum class action { open_journal, inspect, read, write, truncate, sync, lock, unlock, reserved, unlink, directory_sync, close };
    struct resource;
    struct native_call {
        action kind=action::inspect;std::size_t resource_index=0;int fd=-1;
        std::int64_t requested=0,result=-1;int error=0;
        bool attempted=false,entered=false,returned=false;
        std::size_t callback_slot=64;std::uint64_t callback_generation=0;
        std::shared_ptr<resource> original; // SAME issued resource survives first return/phase retirement.
    };
    struct resource {
        const std::uint64_t cookie;
        explicit resource(std::uint64_t original):cookie(original) {}
        int fd=-1;std::uint64_t device=0,inode=0;std::int64_t size=0;
        bool journal=false,birth=false,named=false,borrowed=false,borrow_attempted=false;
        bool birth_attempted=false,birth_entered=false,birth_returned=false;
        bool write_pending=false,unlink_attempted=false,unlinked=false,close_attempted=false,close_returned=false;
        bool sync_attempted=false,sync_returned=false;int sync_rc=SQLITE_MISUSE;
        bool unlink_sync_required=false,unlink_sync_returned=false;int unlink_sync_rc=SQLITE_MISUSE;
        int close_rc=-1,close_errno=0,lock_level=SQLITE_LOCK_NONE;
        sqlite3_file* borrower=nullptr;
        bool pending_byte=false,pending_write=false,reserved_byte=false,shared_range=false,exclusive_range=false;
    };
    struct callback_original {
        const descriptor_callback_kind kind;
        const std::uint64_t generation,cookie;
        sqlite3_file* const borrower;
        const std::size_t previous_slot;
        const std::uint64_t previous_generation;
        bool body_returned=false,body_exception_returned=false,leave_attempted=false,leave_returned=false,leave_joined=false,retained_semantic=false;
        int body_result=SQLITE_MISUSE;
        std::shared_ptr<resource> first_resource;
        std::optional<native_call> first_negative,first_short_read;
        callback_original(descriptor_callback_kind method,std::uint64_t issued,std::uint64_t loan,sqlite3_file* input,
            std::size_t prior,std::uint64_t previous)
            :kind(method),generation(issued),cookie(loan),borrower(input),previous_slot(prior),previous_generation(previous) {}
    };
    // Fixed first-result capacity belongs to the connection before MAIN birth.
    // No callback, diagnostic or post-unlink allocation can replace its facts.
    struct disposition_call {
        bool attempted=false,entered=false,returned=false;
        int result=-1,error=0;
#ifdef LATTICE_TARGET_MAIN_POSIX
        struct stat value{};
#endif
    };
    struct disposition_scan {
        disposition_call open,inspect,convert,close;
        int descriptor=-1;
#ifdef LATTICE_TARGET_MAIN_POSIX
        DIR* directory=nullptr;
#endif
        bool read_attempted=false,read_returned=false,terminal=false,main_seen=false;
        int read_error=0;
        std::size_t entries=0;
    };
    struct main_disposition {
        bool selected=false,attempted=false,claim_returned=false,claim_joined=false,joined=false;
        ordinary_unpublished_group_target::abandonment_binding binding;
        std::shared_ptr<resource> original;
        disposition_call stage,root,root_named,member,member_named,main,main_named;
        disposition_scan before,after;
        disposition_call unlink,unlinked,absent,root_named_after,member_named_after,directory_sync,close;
    } abandoned_main;
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_EXTRA_ENTRY)
    disposition_call qualification_extra_open,qualification_extra_close;
    int qualification_extra_fd=-1;
#endif
    enum class terminal_reason {none,database_failure,receiving_owner_stop};
    struct terminal_disposition {
        bool attempted=false,seal_attempted=false,seal_returned=false,sealed=false;
        terminal_reason reason=terminal_reason::none;
        const ::lattice::database* shell_original=nullptr;
        std::exception_ptr failure_original;
        std::shared_ptr<const ordinary_group_target_receiving_owner> owner_original;
    } terminal_abandonment; // Fixed before MAIN birth; no retry or joined fiction.
    struct terminal_namespace_disposition {
        bool attempted=false,claim_returned=false,claimed=false,failed=false;
        const void* scope_original=nullptr;std::uint64_t generation=0;
        ordinary_unpublished_group_target::terminal_namespace_binding binding;
        struct namespace_check {
            bool attempted=false,returned=false,joined=false;
            disposition_call stage,root,root_named,member,member_named;
        };
        std::array<namespace_check,8> checks{};std::size_t check_count=0;
    } terminal_namespace; // Fixed before MAIN birth; no path/identity copies from caller.
    static constexpr std::size_t maximum_calls=4096,normal_calls=3840,maximum_journals=64;

    // Dedicated general first-result custody is allocated before MAIN birth.
    // Only the actual current/predecessor union is collected: at most 64
    // distinct journals plus MAIN. This does not widen the ordinary call bank.
    struct general_member_scan {
        disposition_call open,inspect,convert,close;
        int descriptor=-1;
#ifdef LATTICE_TARGET_MAIN_POSIX
        DIR* directory=nullptr;
#endif
        struct read_record {disposition_call call;std::array<char,256> name{};std::size_t name_bytes=0;};
        // Actual namespace has at most MAIN and one named journal, plus
        // dot entries and EOF. A sixth first read permits bounded refusal.
        std::array<read_record,6> reads{};std::size_t read_count=0,entries=0;
        bool terminal=false,main_seen=false,journal_seen=false,joined=false;
    };
    struct general_file_disposition {
        bool attempted=false,birth_known=false,not_issued=false,prior_closed=false,ended=false;
        std::shared_ptr<resource> original;
        const native_call* prior_open=nullptr;const native_call* prior_unlink=nullptr;
        disposition_call held,named,unlink,unlinked,absent,directory_sync,close;
        std::array<terminal_namespace_disposition::namespace_check,5> namespace_checks{};
        general_member_scan before,after;
    };
    struct general_files_disposition {
        bool attempted=false,selected=false,failed=false,returned=false,disposed=false;
        const void* scope_original=nullptr;std::uint64_t generation=0;
        // Actual lexical producer/observation identity, not a caller grant.
        const void* producer_frame=nullptr;
        const terminal_namespace_disposition::namespace_check* observing_namespace=nullptr;
        std::array<general_file_disposition,maximum_journals+1> files{};std::size_t count=0;
        terminal_namespace_disposition::namespace_check initial_namespace,final_namespace;
        general_member_scan initial_census,journals_census,empty_census;
        disposition_call prior_directory_sync;
        std::exception_ptr first_failure;
    } general_files;
    struct call_bank {
        const database_scope::scope_state* const issuer;
        std::array<native_call,maximum_calls> calls{};
        std::size_t count=0;
        explicit call_bank(const database_scope::scope_state* original):issuer(original) {}
    };
    std::size_t journal_count=0; // Highest live-slot index, never a lifetime birth quota.
    std::uint64_t journal_generation=1,scope_generation=0,callback_generation=0;
    std::array<std::shared_ptr<resource>,1+maximum_journals> resources{};
    std::shared_ptr<database_scope::scope_state> last_scope; // Actual prior first results; failed custody is not reset.
    std::shared_ptr<database_scope::scope_state> settling_scope;
    std::shared_ptr<database_scope::scope_state> settling_callback_phase;
    std::int64_t main_high=0,journal_high=0;
    std::optional<std::size_t> named_journal;
    bool descriptor_uncertain=false,constructor_attempted=false,schema_attempted=false,catalog_attempted=false,history_header_attempted=false,projection_attempted=false,cleanup_attempted=false;
    std::uint64_t history_page_scopes=0;
    std::weak_ptr<ordinary_target_staging_connection> facade;
    std::weak_ptr<database_scope::scope_state> active_scope;
    const database_scope::scope_state* scope_identity=nullptr;
    void publish_scope(const std::shared_ptr<database_scope::scope_state>&);
    void require_role(bool)const;
    static std::size_t resource_slot(std::uint64_t);
    native_call& reserve(action,std::size_t,int,std::int64_t=0);
    void returned(native_call&,std::int64_t,int)noexcept;
    resource& loan(std::uint64_t,sqlite3_file*);
    void inspect(resource&,std::size_t,bool);
    int file_lock(resource&,std::size_t,int,short,std::int64_t,std::int64_t);
    bool creator_current()const noexcept {return ordinary_launch::current_process()==process&&std::this_thread::get_id()==thread;}
#ifdef LATTICE_TARGET_MAIN_POSIX
    struct stat member_stat{};
    struct stat first_stat{};
#endif
    state(std::shared_ptr<const ordinary_unpublished_group_target> target_,
        std::shared_ptr<ordinary_target_staging_io> io_,ordinary_launch::deadline end_,std::size_t slot)
        :target(std::move(target_)),io(std::move(io_)),end(end_),registry_slot(slot) {
        resources[0]=std::make_shared<resource>((std::uint64_t(1)<<7)|1); // MAIN original before any Native birth.
    }
    ~state() {
#ifdef LATTICE_TARGET_MAIN_POSIX
        if(main_fd>=0)::close(main_fd);
#endif
    }
    void require_local(ordinary_launch::deadline requested)const {
        // Before any inherited lock, descriptor or mutable ledger access.
        if(ordinary_launch::current_process()!=process||std::this_thread::get_id()!=thread)
            birth_fail("target MAIN original creator unavailable");
        if(requested>end||std::chrono::steady_clock::now()>=requested)
            birth_fail("target MAIN original deadline unavailable");
    }
};
#include "ordinary_target_descriptor_ledger.inc"
#include "ordinary_target_terminal_call_evidence.inc"
#include "ordinary_target_history_spool.inc"
#include "ordinary_target_abandoned_raw.inc"
#include "ordinary_target_general_files.inc"
#include "ordinary_target_terminal_vfs.inc"
#include "ordinary_target_terminal_scope.inc"
ordinary_target_staging_connection::ordinary_target_staging_connection(std::shared_ptr<state> value)
    :state_(std::move(value)){}
ordinary_target_staging_connection::~ordinary_target_staging_connection()=default;

void ordinary_target_staging_connection::prepare(
    const std::shared_ptr<const ordinary_unpublished_group_target>& target,
    const std::shared_ptr<ordinary_target_staging_io>& io,
    std::shared_ptr<ordinary_target_staging_connection>& retained,ordinary_launch::deadline end) {
    if(retained)birth_fail("target MAIN output already holds an original");
    if(!target)birth_fail("target MAIN actual target missing");
    auto& owners=registry();std::size_t slot=0;bool already_reserved=false;
    {std::lock_guard<std::mutex> lock(owners.mutex);
        for(std::size_t n=0;n<owners.charged;++n)if(owners.targets[n].get()==target.get()) {
            retained=owners.originals[n];already_reserved=true;break;}
        if(!already_reserved) {
            if(owners.charged==owners.originals.size())birth_fail("target MAIN original registry exhausted");
            slot=owners.charged;owners.targets[slot]=target;++owners.charged;
        }}
    // The actual target reservation survives allocation/current failures too.
    // A later call cannot replace it with a fresh connection original.
    if(already_reserved)birth_fail("target MAIN original already reserved");
    auto native=std::make_shared<state>(target,io,end,slot);
    auto original=std::shared_ptr<ordinary_target_staging_connection>(new ordinary_target_staging_connection(native));
    {std::lock_guard<std::mutex> lock(owners.mutex);owners.originals[slot]=original;}
    retained=original; // Strong custody precedes claim, native birth and errors.
    native->facade=original;
    try {
        native->require_local(end);
        if(!target||!io)birth_fail("target MAIN requires its actual target and IO originals");
        io->require_same_target(target,end);
        target->bind_staging_connection(original,end);native->bound=true;
        native->fixed_main_path=target->staging_main_path(end);
        const int member=target->staging_member_descriptor(original.get(),end);
#ifdef LATTICE_TARGET_MAIN_POSIX
        native->member_fd=member;native->member_fstat_attempted=true;
        errno=0;
        const int member_result=::fstat(member,&native->member_stat);
        const int member_error=member_result?errno:0;
        native->member_fstat_rc=member_result;native->member_fstat_errno=member_error;
        if(!member_result) {native->member_device=std::uint64_t(native->member_stat.st_dev);
            native->member_inode=std::uint64_t(native->member_stat.st_ino);}
        if(member_result||!S_ISDIR(native->member_stat.st_mode)||native->member_stat.st_uid!=::geteuid()||
            (native->member_stat.st_mode&07777)!=0700)
            birth_fail("target MAIN held member identity unavailable");
        // A returned fstat can arrive after the original cutoff. Its first
        // result stays retained, but cannot authorize a later creating call.
        native->require_local(end);
        io->require_same_target(target,end);
        if(target->staging_member_descriptor(original.get(),end)!=member)
            birth_fail("target MAIN creating member association changed");
        native->require_local(end);
        native->open_attempted=true;
        errno=0;
        const int opened=::openat(member,main_leaf,O_RDWR|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600);
        const int open_error=opened<0?errno:0;
        native->main_fd=opened;native->open_rc=opened;native->open_errno=open_error;
        if(opened<0)birth_fail("exclusive target MAIN birth failed");
        native->fstat_attempted=true;
        errno=0;
        const int inspected=::fstat(opened,&native->first_stat);
        const int stat_error=inspected?errno:0;
        native->fstat_rc=inspected;native->fstat_errno=stat_error;
        if(!inspected) {native->device=std::uint64_t(native->first_stat.st_dev);
            native->inode=std::uint64_t(native->first_stat.st_ino);}
        // Actual native results/identity are held before policy/current joins.
        if(inspected||!S_ISREG(native->first_stat.st_mode)||native->first_stat.st_uid!=::geteuid()||
            (native->first_stat.st_mode&07777)!=0600||native->first_stat.st_nlink!=1||native->first_stat.st_size!=0)
            birth_fail("target MAIN native birth identity invalid");
        native->birth_verified=true;
        auto& main=*native->resources[0];main.fd=opened;main.device=native->device;main.inode=native->inode;
        main.birth=main.named=true;main.size=0;
        original->require_original(end);
        io->bind_descriptor_connection(original,end);
        original->construct_database_shell(original,end);
#else
        (void)member;birth_fail("exclusive target MAIN birth unsupported");
#endif
    } catch(...) {
        if(!native->first_failure)native->first_failure=std::current_exception();
        throw;
    }
}
void ordinary_target_staging_connection::require_original(ordinary_launch::deadline end)const {
    if(!state_)birth_fail("target MAIN original missing");
    const auto& native=*state_;native.require_local(end);
    require_target_identity(native.target.get(),end);
    if(!native.bound||!native.target||!native.io||native.first_failure||native.terminal_abandonment.attempted)
        birth_fail("target MAIN original is held failure debt");
    if(!native.open_attempted||!native.birth_verified||!native.fstat_attempted||native.fstat_rc||native.main_fd<0)
        birth_fail("target MAIN original birth has not joined");
    native.io->require_same_target(native.target,end);
    (void)native.target->staging_member_descriptor(this,end);
}
void ordinary_target_staging_connection::require_target_identity(
    const ordinary_unpublished_group_target* target,ordinary_launch::deadline end)const {
    if(!state_)birth_fail("target MAIN original missing");
    const auto& native=*state_;native.require_local(end);
    auto& owners=registry();
    {std::lock_guard<std::mutex> lock(owners.mutex);
        if(native.registry_slot>=owners.charged||owners.originals[native.registry_slot].get()!=this||
            owners.targets[native.registry_slot].get()!=target)
            birth_fail("target MAIN registry association differs");}
    if(!target||native.target.get()!=target||!native.io)
        birth_fail("target MAIN exact native target differs");
}
void ordinary_target_staging_connection::verify_member_census(int member,ordinary_launch::deadline end)const {
    if(!state_)birth_fail("target MAIN census original missing");
    const auto& native=*state_;native.require_local(end);
    require_target_identity(native.target.get(),end);
    // Deliberately no IO/target current recursion. The caller already joined
    // the admitted root/member and exact weak original association.
    if(!native.bound||native.first_failure)birth_fail("target MAIN census retains failed original");
#ifdef LATTICE_TARGET_MAIN_POSIX
    if(native.member_fd>=0) {
        struct stat directory{};
        if(member!=native.member_fd||!native.member_fstat_attempted||native.member_fstat_rc||
            ::fstat(member,&directory)||!S_ISDIR(directory.st_mode)||directory.st_uid!=::geteuid()||
            (directory.st_mode&07777)!=0700||std::uint64_t(directory.st_dev)!=native.member_device||
            std::uint64_t(directory.st_ino)!=native.member_inode)
            birth_fail("target MAIN census member differs from actual birth");
    }
    const int scan=::openat(member,".",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC);
    if(scan<0)birth_fail("target MAIN census unavailable");
    DIR* raw=::fdopendir(scan);
    if(!raw) {const int error=errno;::close(scan);(void)error;birth_fail("target MAIN census unavailable");}
    std::unique_ptr<DIR,decltype(&::closedir)> held(raw,&::closedir);
    bool main_seen=false,journal_seen=false,spool_seen=false;
    for(;;) {
        if(std::chrono::steady_clock::now()>=end)birth_fail("target MAIN census expired");
        errno=0;const auto* entry=::readdir(raw);
        if(!entry) {if(errno)birth_fail("target MAIN census failed");break;}
        if(std::strcmp(entry->d_name,".")==0||std::strcmp(entry->d_name,"..")==0)continue;
        if(std::strcmp(entry->d_name,main_leaf)==0&&!main_seen&&native.birth_verified)main_seen=true;
        else if(std::strcmp(entry->d_name,"store.sqlite-journal")==0&&!journal_seen&&native.named_journal)journal_seen=true;
        else if(!spool_seen&&native.history_spool&&native.history_spool->owns_leaf(entry->d_name))spool_seen=true;
        else birth_fail("target census contains an unowned resource");
    }
    if(native.history_spool)native.history_spool->verify_member_census(member,spool_seen,end);
    else if(native.history_spool_attempted||spool_seen)birth_fail("target spool construction custody unresolved");
    if(native.abandoned_main.attempted) {
        const auto& ending=native.abandoned_main;
        // A terminal receipt permits only an empty member in the shared
        // admission census. It never rearms this target for another writer.
        if(!ending.joined||!ending.claim_returned||!ending.claim_joined||main_seen||journal_seen||spool_seen||
           native.main_fd>=0||native.descriptor_uncertain||!ending.original||
           ending.original!=native.resources[0]||ending.original->fd>=0||!ending.original->unlinked||
           ending.original->named||!ending.close.returned||ending.close.result||
           !ending.directory_sync.returned||ending.directory_sync.result)
            birth_fail("target MAIN abandonment retains original namespace debt");
        return;
    }
    if(!native.open_attempted) {if(main_seen||journal_seen||spool_seen)birth_fail("target resource exists without actual birth");return;}
    if(!native.birth_verified||!native.fstat_attempted||native.fstat_rc||!main_seen||native.descriptor_uncertain)
        birth_fail("target MAIN birth or descriptor ending is unresolved");
    const auto verify=[&](const state::resource& resource,const char* name,std::int64_t high) {
        if(!resource.birth||!resource.named||resource.unlinked||high<0)birth_fail("target resource has no owned birth");
        struct stat named{},held{};
        if(::fstatat(member,name,&named,AT_SYMLINK_NOFOLLOW))birth_fail("target named resource unavailable");
        const auto same=[&](const struct stat& actual) {
            return S_ISREG(actual.st_mode)&&actual.st_uid==::geteuid()&&(actual.st_mode&07777)==0600&&
                actual.st_nlink==1&&actual.st_size>=0&&actual.st_size<=high&&
                std::uint64_t(actual.st_dev)==resource.device&&std::uint64_t(actual.st_ino)==resource.inode;
        };
        if(!same(named))birth_fail("target resource identity or charged extent changed");
        if(resource.fd>=0) {if(::fstat(resource.fd,&held)||!same(held)||held.st_size!=named.st_size)
            birth_fail("target held resource differs from named original");}
        else if(!resource.close_returned||resource.close_rc!=0||resource.borrowed||resource.lock_level!=SQLITE_LOCK_NONE)
            birth_fail("target resource lacks a known native close");
        if(!resource.write_pending&&named.st_size!=resource.size)birth_fail("target resource length has no native join");
    };
    for(std::size_t i=1;i<=native.journal_count;++i) {
        const auto retained=native.resources[i];if(!retained)continue;
        const auto& old=*retained;if(native.named_journal&&*native.named_journal==i)continue;
        // A real monotonic reservation precedes the final pre-open current
        // join. It is not an entered/failed birth or an owned journal yet.
        if(old.journal&&old.birth_attempted&&!old.birth_entered&&!old.birth_returned&&old.fd<0&&
           !old.birth&&!old.named&&!old.borrowed&&!old.borrower)continue;
        if(!old.birth||!old.unlinked||old.named||old.borrowed||old.borrower)
            birth_fail("target superseded journal ending unavailable");
        if(old.fd>=0) {
            struct stat held{};
            if(::fstat(old.fd,&held)||!S_ISREG(held.st_mode)||held.st_uid!=::geteuid()||
               (held.st_mode&07777)!=0600||held.st_nlink!=0||held.st_size!=old.size||
               held.st_size<0||held.st_size>native.journal_high||
               std::uint64_t(held.st_dev)!=old.device||std::uint64_t(held.st_ino)!=old.inode)
                birth_fail("target unlinked journal original changed");
        } else if(!old.close_returned||old.close_rc!=0)birth_fail("target journal native close unknown");
    }
    verify(*native.resources[0],main_leaf,native.main_high);
    if(native.named_journal) {
        if(!journal_seen)birth_fail("owned target journal disappeared");
        verify(*native.resources[*native.named_journal],"store.sqlite-journal",native.journal_high);
    } else if(journal_seen)birth_fail("target journal has no original");
#else
    (void)member;birth_fail("target MAIN census unsupported");
#endif
}
ordinary_launch::deadline ordinary_target_staging_connection::original_deadline()const noexcept {
    return state_?state_->end:ordinary_launch::deadline{};
}
std::string ordinary_target_staging_connection::main_path(ordinary_launch::deadline end)const {
    require_original(end);return state_->target->staging_main_path(end);
}
int ordinary_target_staging_connection::main_descriptor(ordinary_launch::deadline end)const {
    require_original(end);
    if(!state_->birth_verified||state_->main_fd<0)birth_fail("target MAIN native descriptor unavailable");
    return state_->main_fd;
}
std::shared_ptr<ordinary_target_staging_io> ordinary_target_staging_connection::io_original(ordinary_launch::deadline end)const {
    require_original(end);return state_->io;
}
void ordinary_target_staging_connection::claim_database_shell(ordinary_launch::deadline end) {
    require_original(end);
    if(state_->shell_claimed)birth_fail("target database shell already attempted");
    state_->shell_claimed=true; // No reset after allocation/open failure.
}
void ordinary_target_staging_connection::construct_database_shell(
    const std::shared_ptr<ordinary_target_staging_connection>& original,ordinary_launch::deadline end) {
    if(original.get()!=this)birth_fail("target database shell requires the same connection original");
    require_original(end);
    if(state_->shell)birth_fail("target database shell already retained");
    auto shell=::lattice::database::make_target_staging_shell(original);
    state_->shell=std::move(shell); // Hold the actual returned shell before any postcheck.
    if(!state_->shell)birth_fail("target database factory returned no original shell");
    require_original(end);
    // The only mounted consumer is this actual retained shell. Its private
    // constructor scope proves same object/retirement before any native open.
    state_->shell->initialize_target_staging_();
}
void ordinary_target_staging_connection::require_preopen_constructor(ordinary_launch::deadline end)const {
    require_original(end);
    const auto scope=state_->active_scope.lock();
    if(!scope||scope.get()!=state_->scope_identity||scope->cleanup||scope->schema||scope->ending||scope->left||
       scope->shell!=state_->shell.get()||scope->retirement!=state_->shell->retirement_)
        birth_fail("target constructor requires its actual retained database scope");
    scope->require(*state_->shell);
}
} // namespace lattice::detail

namespace lattice::detail {
void ordinary_target_staging_connection::bootstrap_metadata(
    const std::shared_ptr<ordinary_group_target_schema_original>& original,ordinary_launch::deadline end) {
    require_original(end);
    if(!original||!state_->shell)birth_fail("target metadata actual schema/shell missing");
    original->require_connection(*this,end);
    state_->shell->bootstrap_target_metadata_(original);
    original->require_connection(*this,end);
}
} // namespace lattice::detail

namespace lattice::detail {
void ordinary_target_staging_connection::install_catalog(
    const std::shared_ptr<ordinary_group_target_schema_original>& original,ordinary_launch::deadline end) {
    require_original(end);if(!original||!state_->shell)birth_fail("target catalog actual original/shell missing");
    original->require_catalog(*this,end);state_->shell->install_target_catalog_(original);
    original->require_connection(*this,end);
}
bool ordinary_target_staging_connection::retire_receiving_shell()noexcept {
    const auto native=state_;
    if(!native||!native->creator_current()||!native->shell)return false;
    if(native->terminal_abandonment.attempted)return false;
    native->shell->retire_target_staging_();
    if(!native->shell->target_staging_cleanup_joined_(*this))return false;
    if(!native->history_spool_attempted)return !native->history_spool;
    return native->history_spool&&native->history_spool->retire_unused(*this);
}
ordinary_target_staging_connection::abandoned_retirement_result
ordinary_target_staging_connection::retire_abandoned_receiving_shell()noexcept {
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||!native->shell)return abandoned_retirement_result::retained_debt;
        if(native->terminal_abandonment.attempted)return abandoned_retirement_result::retained_debt;
        if(native->abandoned_main.selected)return native->abandoned_main.joined&&
            native->shell->target_staging_cleanup_joined_(*this)?abandoned_retirement_result::joined:
            abandoned_retirement_result::retained_debt; // Observe only; no second close/unlink.
        // Select only the actual successful fixed constructor, before all
        // schema/catalog/history/import work. Uncertain/failed constructor
        // endings and an already begun ordinary close keep their original debt.
        const auto prior=native->last_scope;
        if(native->first_failure||!native->constructor_attempted||native->cleanup_attempted||
           native->schema_attempted||native->catalog_attempted||native->history_header_attempted||
           native->history_page_scopes||native->history_spool_attempted||native->history_spool||
           native->row_import_attempted||native->import_suspension_attempted||native->history_chain_attempted||
           native->projection_attempted||!native->active_scope.expired()||native->scope_identity||
           !prior||prior->schema||prior->cleanup||prior->ending||prior->failed||!prior->left||
           !prior->leave_returned||!prior->leave_joined||!prior->open_observed||prior->open_rc!=SQLITE_OK||
           prior->configuration_count!=18||native->descriptor_uncertain)return abandoned_retirement_result::not_applicable;
        native->abandoned_main.selected=true; // SAME fixed receipt precedes actual SQLite close.
        native->shell->retire_target_staging_();
        return native->abandoned_main.joined&&native->shell->target_staging_cleanup_joined_(*this)?
            abandoned_retirement_result::joined:abandoned_retirement_result::retained_debt;
    }catch(...){return abandoned_retirement_result::retained_debt;}
}

bool ordinary_target_staging_connection::terminal_cleanup_pending(const ::lattice::database& actual)const noexcept {
    const auto native=state_;
    return native&&native->creator_current()&&native->shell.get()==&actual&&
        native->terminal_abandonment.attempted&&native->terminal_abandonment.shell_original==&actual;
    // Election only; no VM/callback/provider/namespace or cleanup-ending proof.
}
bool ordinary_target_staging_connection::elect_terminal_database_failure(const ::lattice::database& actual)noexcept {
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||native->shell.get()!=&actual)return false;
        auto& receipt=native->terminal_abandonment;
        if(receipt.attempted)return receipt.shell_original==&actual&&receipt.seal_returned&&receipt.sealed;
        if(native->abandoned_main.selected||native->cleanup_attempted||!native->target||!native->io)return false;
        const auto failure=actual.target_staging_failure_original_(*this);
        if(!failure)return false; // Actual DB producer, not a supplied exception.
        receipt.reason=state::terminal_reason::database_failure;
        receipt.shell_original=&actual;receipt.failure_original=failure;
        receipt.attempted=true; // First disposition precedes the only seal/close.
        receipt.seal_attempted=true;
        receipt.sealed=native->target->seal_terminal_staging(this);
        receipt.seal_returned=true;
        return receipt.sealed; // Failed seal is immutable selected debt.
    }catch(...){return false;}
}
ordinary_target_staging_connection::abandoned_retirement_result
ordinary_target_staging_connection::retire_terminal_receiving_shell(const ordinary_group_target_receiving_owner& owner)noexcept {
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||!native->shell)return abandoned_retirement_result::retained_debt;
        auto& receipt=native->terminal_abandonment;
        if(receipt.attempted)return abandoned_retirement_result::retained_debt; // Never restart a partial election.
        if(native->abandoned_main.selected||native->cleanup_attempted||!native->target||!native->io)
            return abandoned_retirement_result::retained_debt;
        const auto original=owner.terminal_abandonment_original(*this,native->target,native->io);
        receipt.reason=state::terminal_reason::receiving_owner_stop;
        receipt.shell_original=native->shell.get();receipt.owner_original=original;
        receipt.attempted=true;
        receipt.seal_attempted=true;
        receipt.sealed=native->target->seal_terminal_staging(this);
        receipt.seal_returned=true;
        if(receipt.sealed)native->shell->retire_target_staging_();
        // Same SQLite first result/scope are retained. General raw/journal,
        // provider, VFS and terminal input endings are explicit remaining debt.
        return abandoned_retirement_result::retained_debt;
    }catch(...){return abandoned_retirement_result::retained_debt;}
}
// Input ending keeps the real shell/elected owner/native phases retained.
// Capture the actual post-scope identity before destroying receiver payloads.
bool ordinary_target_staging_connection::terminal_receiver_input_context_known(
    const ordinary_group_target_receiving_owner& owner,const void*& scope_original,std::uint64_t& generation)const noexcept {
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||native->facade.lock().get()!=this||!native->shell||
           !native->target||!native->io||native->abandoned_main.selected)return false;
        const auto original=owner.terminal_abandonment_original(*this,native->target,native->io);
        if(original.get()!=&owner||!terminal_scope_ended(*native->shell))return false;
        const auto& first=native->terminal_scope;
        if(!first.scope_original||first.shell_original!=native->shell.get()||!first.generation)return false;
        scope_original=first.scope_original.get();generation=first.generation;
        return true; // Actual private identity DATA under all pre-input endings.
    }catch(...){return false;}
}
bool ordinary_target_staging_connection::terminal_receiver_input_history_matches(
    const ordinary_group_target_receiving_owner& owner,const void* scope_original,std::uint64_t generation)const noexcept {
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||native->facade.lock().get()!=this||!native->shell||
           !native->target||!native->io||native->abandoned_main.selected)return false;
        const auto original=owner.terminal_abandonment_original(*this,native->target,native->io);
        const auto& elected=native->terminal_abandonment;const auto& first=native->terminal_scope;
        const auto scope=first.scope_original;
        if(original.get()!=&owner||!elected.attempted||!elected.seal_attempted||!elected.seal_returned||!elected.sealed||
           elected.shell_original!=native->shell.get()||!first.attempted||!first.returned||
           first.result!=terminal_scope_retirement_result::detached||first.shell_original!=native->shell.get()||
           !scope||scope.get()!=scope_original||!generation||generation!=first.generation||generation!=native->scope_generation||
           scope->original.lock()!=native||scope->shell!=native->shell.get()||scope->generation!=generation||
           scope!=native->last_scope||!scope->cleanup||scope->left||native->scope_identity||
           !native->active_scope.expired()||native->settling_scope||native->settling_callback_phase)return false;
        if(elected.reason==state::terminal_reason::receiving_owner_stop)
            return elected.owner_original.get()==&owner&&!elected.failure_original;
        if(elected.reason==state::terminal_reason::database_failure)
            return bool(elected.failure_original)&&!elected.owner_original;
        return false;
        // Historical association only. Never call pre-input Native/payload
        // predicates after those receiver captures have actually died.
    }catch(...){return false;}
}
// A distinct pre-Native-payload boundary after actual receiving input ending.
// Old Native objects/call/error/VM/static banks still exist at this point.
bool ordinary_target_staging_connection::terminal_native_decoder_context_known(
    const ordinary_group_target_receiving_owner& owner,const void*& scope_original,std::uint64_t& generation)const noexcept {
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||native->facade.lock().get()!=this||!native->shell||
           native->abandoned_main.selected||!owner.terminal_inputs_ended(*this)||
           !terminal_general_native_after_scope_known(*native->shell))return false;
        const auto& first=native->terminal_scope;
        if(!first.scope_original||!first.generation||
           !terminal_receiver_input_history_matches(owner,first.scope_original.get(),first.generation))return false;
        scope_original=first.scope_original.get();generation=first.generation;return true;
    }catch(...){return false;}
}
bool ordinary_target_staging_connection::retire_terminal_native_decoder_payloads(
    const ordinary_group_target_receiving_owner& owner)noexcept {
    const auto native=state_;
    if(!native||!native->creator_current()||native->facade.lock().get()!=this||!native->shell)return false;
    return native->shell->retire_target_terminal_decoder_payloads_(*this,owner)==
        ::lattice::database::terminal_decoder_payload_result::captures_ended;
}
bool ordinary_target_staging_connection::terminal_native_decoder_payloads_ended(
    const ordinary_group_target_receiving_owner& owner)const noexcept {
    const auto native=state_;
    return native&&native->creator_current()&&native->facade.lock().get()==this&&native->shell&&
        native->shell->target_terminal_decoder_payloads_ended_(*this,owner);
}
bool ordinary_target_staging_connection::retire_terminal_native_phases(
    const ordinary_group_target_receiving_owner& owner)noexcept {
    const auto native=state_;
    if(!native||!native->creator_current()||native->facade.lock().get()!=this||!native->shell)return false;
    return native->shell->retire_target_terminal_native_phases_(*this,owner)==
        ::lattice::database::terminal_native_phase_result::phases_ended;
}
bool ordinary_target_staging_connection::terminal_native_phases_ended(
    const ordinary_group_target_receiving_owner& owner)const noexcept {
    const auto native=state_;
    return native&&native->creator_current()&&native->facade.lock().get()==this&&native->shell&&
        native->shell->target_terminal_native_phases_ended_(*this,owner);
}
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
std::uint64_t ordinary_target_staging_connection::qualification_disposition_observation()const noexcept {
    const auto native=state_;if(!native||!native->creator_current())return 0;
    using fixture=ordinary_receiver_reclamation_fixture;
    const auto& receipt=native->abandoned_main;std::uint64_t bits=0;
    const auto cleanup=native->last_scope;
    if(cleanup&&cleanup->cleanup&&cleanup->close_observed&&cleanup->close_rc==SQLITE_OK)bits|=fixture::first_sqlite_close_ok;
    if(receipt.unlink.entered&&receipt.unlink.returned&&receipt.unlink.result==0)bits|=fixture::first_unlink_ok;
    if(receipt.unlinked.entered&&receipt.unlinked.returned&&receipt.unlinked.result==0&&
       receipt.unlinked.value.st_nlink==0)bits|=fixture::held_zero_links;
    if(receipt.absent.entered&&receipt.absent.returned&&receipt.absent.result==-1&&receipt.absent.error==ENOENT)bits|=fixture::name_absent;
    if(receipt.directory_sync.entered&&receipt.directory_sync.returned&&receipt.directory_sync.result==0)bits|=fixture::first_directory_sync_ok;
    if(receipt.close.entered&&receipt.close.returned&&receipt.close.result==0)bits|=fixture::first_main_close_ok;
    if(cleanup&&cleanup->native_files_joined)bits|=fixture::native_files_joined;
    if(receipt.selected&&!receipt.unlink.entered)bits|=fixture::unlink_never_entered;
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_EXTRA_ENTRY)
    if(native->qualification_extra_open.entered&&native->qualification_extra_open.returned&&native->qualification_extra_open.result>=0&&
       native->qualification_extra_close.entered&&native->qualification_extra_close.returned&&native->qualification_extra_close.result==0&&
       native->qualification_extra_fd==-1)bits|=fixture::extra_entry_created_and_closed;
#endif
    return bits; // Creator-worker measurement only; never cleanup or admission input.
}
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_EXTRA_ENTRY)
bool ordinary_target_staging_connection::qualification_create_extra_entry()noexcept {
    try {
#ifdef LATTICE_TARGET_MAIN_POSIX
        const auto native=state_;if(!native||!native->creator_current()||native->qualification_extra_open.attempted)return false;
        require_original(native->end); // Actual admitted member/MAIN/current fences before this real fault.
        if(native->schema_attempted||native->history_spool_attempted||native->cleanup_attempted||!native->shell)return false;
        auto& opened=native->qualification_extra_open;opened.attempted=true;errno=0;opened.entered=true;
        const int fd=::openat(native->member_fd,"qualification.extra",O_RDWR|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600);
        const int error=fd<0?errno:0;opened.result=fd;opened.error=error;opened.returned=true;native->qualification_extra_fd=fd;
        if(fd<0)return false;
        auto& closed=native->qualification_extra_close;closed.attempted=true;errno=0;closed.entered=true;
        const int rc=::close(fd);const int close_error=rc?errno:0;closed.result=rc;closed.error=close_error;closed.returned=true;
        if(rc)return false; // First unknown close is retained, never retried.
        native->qualification_extra_fd=-1;return true; // Real unowned entry; no fake native rc or closed DTO.
#else
        return false;
#endif
    }catch(...){return false;}
}
#endif
#endif

} // namespace lattice::detail

namespace lattice::detail {
void ordinary_target_staging_connection::stage_history_header(const std::shared_ptr<ordinary_group_target_receiving_owner>& owner,ordinary_launch::deadline end) {
    require_original(end);if(!owner||!state_->shell)birth_fail("GROUP history header real shell unavailable");
    owner->require_header_original(*this);state_->shell->stage_target_history_header_(owner);
    require_history_header_ending(*owner,end);
    auto native=state_;
    if(native->history_spool_attempted||native->history_spool)birth_fail("GROUP history spool already attempted");
    native->history_spool_attempted=true; // Charge before allocation or Native effects; never reset.
    ordinary_target_history_spool::prepare(native->facade.lock(),owner,native->history_spool,end);
    native->history_spool->persist_header(end); // SAME exact wire bytes; no progress before its ending.
}
void ordinary_target_staging_connection::stage_raw_history_header(const std::shared_ptr<ordinary_group_target_receiving_owner>& owner,ordinary_launch::deadline end) {
    require_original(end);if(!owner||!state_->shell)birth_fail("GROUP raw header actual shell unavailable");
    owner->require_header_original(*this);state_->shell->stage_target_raw_history_header_(owner);
    require_history_header_ending(*owner,end);
    const auto native=state_;
    if(native->history_spool_attempted||native->history_spool)birth_fail("GROUP raw history spool already attempted");
    native->history_spool_attempted=true;
    ordinary_target_history_spool::prepare(native->facade.lock(),owner,native->history_spool,end);
    native->history_spool->persist_header(end);
}
void ordinary_target_staging_connection::require_history_spool_current(const ordinary_target_history_spool& spool,
    const ordinary_group_target_receiving_owner& owner,ordinary_launch::deadline end)const {
    require_history_header_ending(owner,end);
    const auto native=state_;
    if(!native->history_spool_attempted||native->history_spool.get()!=&spool||native->member_fd<0||
       native->cleanup_attempted||!native->active_scope.expired()||native->scope_identity)
        birth_fail("GROUP history spool lacks same Conn/header/member ending");
}
const ordinary_history_descriptor& ordinary_target_staging_connection::history_spool_header_original(
    const ordinary_target_history_spool& spool,const ordinary_group_target_receiving_owner& owner,ordinary_launch::deadline end)const {
    require_history_spool_current(spool,owner,end);
    return owner.require_header_original(*this); // DATA borrowed only from the SAME original retained ingress.
}
const ordinary_history_decoded_page& ordinary_target_staging_connection::history_spool_page_original(
    const ordinary_target_history_spool& spool,const ordinary_group_target_receiving_owner& owner,ordinary_launch::deadline end)const {
    require_history_spool_current(spool,owner,end);
    return owner.require_page_original(*this);
}
std::shared_ptr<const ordinary_history_decoded_page> ordinary_target_staging_connection::history_spool_page_input_pin(
    const ordinary_target_history_spool& spool,const ordinary_group_target_receiving_owner& owner,ordinary_launch::deadline end)const {
    require_history_spool_current(spool,owner,end);return owner.require_page_input_pin(*this);
}
const std::shared_ptr<ordinary_resource_reservation<true>>& ordinary_target_staging_connection::history_spool_decoder_charge(
    const ordinary_target_history_spool& spool,const ordinary_group_target_receiving_owner& owner,ordinary_launch::deadline end)const {
    require_history_spool_current(spool,owner,end);return owner.require_decoder_charge(*this);
}
std::uint64_t ordinary_target_staging_connection::history_spool_page_before_bytes(
    const ordinary_target_history_spool& spool,const ordinary_group_target_receiving_owner& owner,
    ordinary_history_progress& before,ordinary_launch::deadline end)const {
    require_history_spool_current(spool,owner,end);
    if(!state_->shell)birth_fail("GROUP raw page prior actual shell missing");
    return state_->shell->target_history_page_append_before_bytes_(*this,owner,before);
}
std::shared_ptr<const ordinary_target_history_page_append> ordinary_target_staging_connection::append_history_page(
    const std::shared_ptr<ordinary_group_target_receiving_owner>& owner,ordinary_launch::deadline end) {
    require_original(end);
    if(!owner||!state_->history_spool)birth_fail("GROUP raw page actual owner/spool unavailable");
    require_history_spool_current(*state_->history_spool,*owner,end);
    return state_->history_spool->append_page(end); // Private durable IO original, not metadata/progress authority.
}
void ordinary_target_staging_connection::require_history_raw_locator_ending(const ordinary_group_target_receiving_owner& owner,
    const ordinary_target_history_page_append& append,ordinary_launch::deadline end)const {
    require_original(end);if(!state_->shell||!state_->active_scope.expired()||state_->scope_identity)
        birth_fail("GROUP raw locator join requires actual scope leave");
    state_->shell->require_target_raw_history_page_ending_(*this,owner,append);
}
void ordinary_target_staging_connection::end_history_raw_input(const ordinary_group_target_receiving_owner& owner,
    std::shared_ptr<const ordinary_history_decoded_page>& pin,ordinary_launch::deadline end)const {
    require_original(end);owner.end_page_input_pin(*this,pin); // Genuine SAME receive/send/input ending, no refund.
}
void ordinary_target_staging_connection::stage_raw_history_page(const std::shared_ptr<ordinary_group_target_receiving_owner>& owner,ordinary_launch::deadline end) {
    require_original(end);if(!owner||!state_->shell||!state_->history_spool)birth_fail("GROUP raw page actual owner/shell/spool missing");
    const auto append=append_history_page(owner,end); // Exact IO ends before entering a SQL scope.
    state_->shell->stage_target_raw_history_page_(owner,append);
    require_history_raw_locator_ending(*owner,*append,end);
    state_->history_spool->join_page_locator(*this,*owner,append,end);
    (void)history_page_progress(*owner,end); // Only after SAME SQL locator+raw original joined.
}
void ordinary_target_staging_connection::end_raw_history_page_input(const ordinary_group_target_receiving_owner& owner,ordinary_launch::deadline end) {
    require_original(end);if(!state_->history_spool)birth_fail("GROUP raw input actual spool missing");
    state_->history_spool->end_page_input(*this,owner,end);
}
bool ordinary_target_staging_connection::history_spool_retirement_admissible(const ordinary_target_history_spool* spool)const noexcept {
    try {
        const auto native=state_;
        return native&&native->creator_current()&&native->history_spool.get()==spool&&native->history_spool_attempted&&
            native->cleanup_attempted&&native->shell&&native->shell->target_staging_cleanup_joined_(*this);
    }catch(...) {return false;}
}
void ordinary_target_staging_connection::require_history_header_ending(const ordinary_group_target_receiving_owner& owner,ordinary_launch::deadline end)const {
    require_original(end);owner.require_header_original(*this);
    if(!state_->shell)birth_fail("GROUP history header ending shell unavailable");
    state_->shell->require_target_history_header_ending_(*this,owner);
}
std::pair<std::uint64_t,std::uint64_t> ordinary_target_staging_connection::history_main_identity(ordinary_launch::deadline end)const {
    require_original(end);const auto& main=*state_->resources[0];
    if(!main.birth||main.fd<0||main.close_attempted||main.device!=state_->device||main.inode!=state_->inode)
        birth_fail("GROUP history MAIN identity unavailable");
    return {main.device,main.inode};
}
}

namespace lattice::detail {
void ordinary_target_staging_connection::stage_history_page(const std::shared_ptr<ordinary_group_target_receiving_owner>& owner,ordinary_launch::deadline end) {
    require_original(end);if(!owner||!state_->shell)birth_fail("GROUP page actual owner/shell unavailable");
    if(!state_->history_spool)birth_fail("GROUP page lacks actual header spool original");
    state_->history_spool->require_header_ending(end);
    state_->history_spool->require_no_page_append(end); // Legacy BLOB route cannot advertise an unjoined raw append.
    (void)owner->require_page_original(*this);state_->shell->stage_target_history_page_(owner);
    (void)history_page_progress(*owner,end);
}
ordinary_history_progress ordinary_target_staging_connection::history_page_progress(const ordinary_group_target_receiving_owner& owner,ordinary_launch::deadline end)const {
    require_original(end);(void)owner.require_header_original(*this);
    if(!state_->shell)birth_fail("GROUP page actual ending shell unavailable");
    return state_->shell->target_history_page_progress_(*this,owner);
}
}

namespace lattice::detail {
void ordinary_target_staging_connection::validate_history_projection(
    const std::shared_ptr<ordinary_group_target_receiving_owner>& receiving,ordinary_launch::deadline end) {
    require_original(end);if(!receiving||!state_->shell)birth_fail("GROUP projection actual owner/shell unavailable");
    state_->shell->require_target_history_complete_(*this,*receiving);
    state_->shell->validate_target_history_projection_(receiving);
    require_history_projection_ending(*receiving,end);
}
void ordinary_target_staging_connection::require_history_projection_ending(
    const ordinary_group_target_receiving_owner& receiving,ordinary_launch::deadline end)const {
    require_original(end);if(!state_->shell)birth_fail("GROUP projection actual ending shell unavailable");
    state_->shell->require_target_history_projection_ending_(*this,receiving);
}
}

namespace lattice::detail {
void ordinary_target_staging_connection::verify_history_chain(
    const std::shared_ptr<ordinary_group_target_receiving_owner>& receiving,ordinary_launch::deadline end) {
    require_original(end);if(!receiving||!state_->shell||state_->history_chain_attempted)
        birth_fail("GROUP durable chain actual one-shot original unavailable");
    state_->shell->require_target_history_projection_ending_(*this,*receiving);
    state_->history_chain_attempted=true; // Same original; never replay or replacement after failure.
    state_->shell->verify_target_history_chain_(receiving);require_history_chain_ending(*receiving,end);
}
void ordinary_target_staging_connection::require_history_chain_ending(
    const ordinary_group_target_receiving_owner& receiving,ordinary_launch::deadline end)const {
    require_original(end);if(!state_->shell)birth_fail("GROUP durable chain original shell missing");
    state_->shell->require_target_history_chain_ending_(*this,receiving);
}
}

namespace lattice::detail {
void ordinary_target_staging_connection::suspend_import(
    const std::shared_ptr<ordinary_group_target_receiving_owner>& receiving,ordinary_launch::deadline end) {
    require_original(end);if(!receiving||!state_->shell||state_->import_suspension_attempted)
        birth_fail("GROUP import suspension one-shot original unavailable");
    require_history_chain_ending(*receiving,end);
    state_->import_suspension_attempted=true; // Actual entered producer; never reset after failure.
    state_->shell->suspend_target_import_(receiving);require_import_suspension_ending(*receiving,end);
}
void ordinary_target_staging_connection::require_import_suspension_ending(
    const ordinary_group_target_receiving_owner& receiving,ordinary_launch::deadline end)const {
    require_original(end);if(!state_->shell)birth_fail("GROUP import suspension actual shell missing");
    state_->shell->require_target_import_suspension_ending_(*this,receiving);
}
} // namespace lattice::detail

namespace lattice::detail {
void ordinary_target_staging_connection::import_history_rows(
    const std::shared_ptr<ordinary_group_target_receiving_owner>& receiving,ordinary_launch::deadline end) {
    require_original(end);if(!receiving||!state_->shell||state_->row_import_attempted)
        birth_fail("GROUP bounded row import original unavailable");
    require_import_suspension_ending(*receiving,end);state_->row_import_attempted=true;
    state_->shell->import_target_history_rows_(receiving);require_row_import_ending(*receiving,end);
}
void ordinary_target_staging_connection::require_row_import_ending(
    const ordinary_group_target_receiving_owner& receiving,ordinary_launch::deadline end)const {
    require_original(end);if(!state_->shell)birth_fail("GROUP row import actual shell absent");
    state_->shell->require_target_row_import_ending_(*this,receiving);
}
} // namespace lattice::detail

#ifdef LATTICE_MANAGED_TARGET_RECLAMATION_FIXTURE
namespace lattice::detail {
nlohmann::json ordinary_target_reclamation_fixture::construct_and_abandon(
    const std::shared_ptr<const ordinary_unpublished_group_target>& target,ordinary_launch::deadline end) {
    std::shared_ptr<ordinary_target_staging_io> io;
    std::shared_ptr<ordinary_target_staging_connection> connection;
    ordinary_target_staging_io::prepare(target,io,end);
    ordinary_target_staging_connection::prepare(target,io,connection,end);
    const auto native=connection->state_;
    if(!native||!native->creator_current()||native->target!=target||native->io!=io||native->end!=end||
       std::chrono::steady_clock::now()>=end||native->first_failure)
        birth_fail("constructor fixture actual originals/deadline unavailable");
    const auto constructor=native->last_scope; // Own the genuine first constructor ending.
    if(!constructor||constructor->cleanup||constructor->failed||!constructor->leave_returned||
       !constructor->leave_joined||constructor->open_rc!=SQLITE_OK||constructor->configuration_count!=18)
        birth_fail("constructor fixture lacks first successful constructor ending");
    const auto ending=connection->retire_abandoned_receiving_shell();
    if(ending!=ordinary_target_staging_connection::abandoned_retirement_result::joined)
        birth_fail("constructor fixture retains abandonment debt"); // Never fall back/retry as success.
    const auto cleanup=native->last_scope;
    const auto& receipt=native->abandoned_main;
    if(!cleanup||!cleanup->cleanup||!cleanup->leave_returned||!cleanup->leave_joined||cleanup->failed||
       !cleanup->close_observed||cleanup->close_rc!=SQLITE_OK||!cleanup->native_files_joined||
       !receipt.joined||!receipt.original||receipt.original!=native->resources[0])
        birth_fail("constructor fixture actual cleanup ending unavailable");
    auto call=[](const auto& value){return nlohmann::json{{"attempted",value.attempted},
        {"entered",value.entered},{"returned",value.returned},{"rc",value.result},{"errno",value.error}};};
#ifdef LATTICE_TARGET_MAIN_POSIX
    return {{"format","lattice-target-constructor-reclamation-observation-v1"},
        {"mainDevice",receipt.original->device},{"mainInode",receipt.original->inode},
        {"memberDevice",native->member_device},{"memberInode",native->member_inode},
        {"linksBefore",receipt.main.value.st_nlink},{"linksAfter",receipt.unlinked.value.st_nlink},
        {"entriesBefore",receipt.before.entries},{"entriesAfter",receipt.after.entries},
        {"unlink",call(receipt.unlink)},{"absent",call(receipt.absent)},
        {"directorySync",call(receipt.directory_sync)},{"descriptorClose",call(receipt.close)},
        {"sqliteOpenRC",constructor->open_rc},{"sqliteCloseRC",cleanup->close_rc},
        {"configurations",constructor->configuration_count},{"nativeFilesJoined",cleanup->native_files_joined},
        {"constructorLeaveJoined",constructor->leave_joined},{"cleanupLeaveJoined",cleanup->leave_joined},
        {"registration",registration_ending(*io)},
        {"schemaAttempted",native->schema_attempted},{"catalogAttempted",native->catalog_attempted},
        {"historyAttempted",native->history_header_attempted||native->history_page_scopes||native->history_spool_attempted},
        {"importAttempted",native->row_import_attempted||native->import_suspension_attempted},
        {"projectionAttempted",native->projection_attempted}};
#else
    birth_fail("constructor fixture physical receipt unsupported");
#endif
}
}
#endif
