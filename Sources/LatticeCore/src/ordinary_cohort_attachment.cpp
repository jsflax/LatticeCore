#include "ordinary_cohort_attachment.hpp"
#include <lattice/lattice.hpp>
#include <stdexcept>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <unistd.h>
#include "wal_extent_vfs.hpp"
#endif

namespace lattice::detail {
namespace {
std::int64_t process_id() {
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    return ::getpid();
#else
    throw db_error("managed cohort attachment platform unavailable");
#endif
}
std::string sql_literal(const std::string& value) {
    if(value.empty()||value.find('\0')!=std::string::npos)throw db_error("managed attachment empty or invalid filename");
    std::string result="'";
    for(char c:value){result+=c;if(c=='\'')result+='\'';}
    return result+"'";
}
std::string sql_alias(const std::string& value) {
    if(value.empty()||value.find('\0')!=std::string::npos)throw db_error("managed attachment empty or invalid alias");
    std::string result="\"";
    for(char c:value){result+=c;if(c=='\"')result+='\"';}
    return result+"\"";
}
std::string read_only_filename(const std::string& path) {
    static constexpr char hex[]="0123456789ABCDEF";
    std::string result="file:";
    for(unsigned char c:path){
        if((c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='/'||c=='-'||c=='_'||c=='.'||c=='~')result+=char(c);
        else{result+='%';result+=hex[c>>4];result+=hex[c&15];}
    }
    return result+"?mode=ro";
}
class one_action final {
    sqlite3* const connection_;
    const int action_;
    const std::int64_t creator_=process_id();
    const std::string& value_;
    bool consumed_=false;
    static thread_local one_action* current_;
public:
    one_action(sqlite3* connection,int action,const std::string& value)
        :connection_(connection),action_(action),value_(value) {
        if(!connection||current_)throw db_error("nested managed attachment permit refused");
        current_=this;
    }
    ~one_action(){current_=nullptr;}
    one_action(const one_action&)=delete;
    one_action& operator=(const one_action&)=delete;
    static int authorize(sqlite3* connection,int action,const char* first,const char* second)noexcept {
        if(action!=SQLITE_ATTACH&&action!=SQLITE_DETACH)return SQLITE_OK;
        auto* permit=current_;
        // NULL/parameterized filename is never a wildcard. The ATTACH alias
        // is not exposed here; only our exact statement binds that alias.
        if(!permit||permit->creator_!=static_cast<std::int64_t>(database_native_process())||
            permit->connection_!=connection||permit->action_!=action||permit->consumed_||
            !first||second||permit->value_!=first)return SQLITE_DENY;
        permit->consumed_=true;return SQLITE_OK;
    }
    void finish()const{if(!consumed_)throw db_error("managed attachment authorizer did not consume its exact action");}
};
thread_local one_action* one_action::current_=nullptr;
}
int ordinary_composed_attachment_authorize(sqlite3* connection,int action,const char* first,const char* second) noexcept {
    try {
        if(!ordinary_creator_current({}))return SQLITE_DENY;
        if((action==SQLITE_ATTACH||action==SQLITE_DETACH)&&wal_extent_attachment_guard_required(connection))
            return ordinary_cohort_attachment_access::authorize(connection,action,first,second);
        if(!ordinary_requires_attachment_guard())return SQLITE_OK;
        return ordinary_cohort_attachment_access::authorize(connection,action,first,second);
    } catch(...) {return SQLITE_DENY;}
}
int restore_ordinary_attachment_guard(sqlite3* connection) noexcept {
    try {
        if(!ordinary_creator_current({}))return SQLITE_MISUSE;
        if(!ordinary_requires_attachment_guard()&&!wal_extent_attachment_guard_required(connection))return sqlite3_set_authorizer(connection,nullptr,nullptr);
        return sqlite3_set_authorizer(connection,
            [](void* raw,int action,const char* one,const char* two,const char*,const char*) noexcept {
                return ordinary_composed_attachment_authorize(static_cast<sqlite3*>(raw),action,one,two);
            },connection);
    }catch(...){return SQLITE_MISUSE;}
}
void ordinary_cohort_attachment_binding::validate_wal_policy()const {
    if(!recipient_wal_limit_&&!target_wal_limit_)return;
    if(!recipient_wal_limit_||!target_wal_limit_||*recipient_wal_limit_<=0||*target_wal_limit_<=0)
        throw db_error("typed attachment requires both immutable WAL policies");
    if(recipient_&&recipient_->wal_extent_limit_bytes(recipient_path_)!=*recipient_wal_limit_)
        throw db_error("typed recipient WAL policy differs from actual retained issuer");
    if(target_&&target_->wal_extent_limit_bytes(path_)!=*target_wal_limit_)
        throw db_error("typed target WAL policy differs from actual retained issuer");
}
void ordinary_cohort_attachment_binding::validate(std::chrono::steady_clock::time_point end)const {
    if(native_wal_only_){
        ordinary_require_no_attachment({});
        if(creator_!=process_id()||recipient_||target_||!recipient_guard_||!target_guard_||
           !recipient_guard_->alive.load(std::memory_order_acquire)||!target_guard_->alive.load(std::memory_order_acquire)||
           !recipient_identity_||!target_identity_||token_<=0||!recipient_wal_limit_||!target_wal_limit_)
            throw db_error("native typed WAL attachment original unavailable");
        validate_wal_policy();
        if(std::chrono::steady_clock::now()>=end)throw db_error("managed attachment validation expired");
        return;
    }

    if(creator_!=process_id()||!recipient_guard_||!target_guard_||
        !recipient_guard_->alive.load(std::memory_order_acquire)||!target_guard_->alive.load(std::memory_order_acquire)||
        !recipient_profile_||!target_profile_||!recipient_identity_||!target_identity_)
        throw db_error("managed attachment retained owner unavailable");
    const auto a=recipient_profile_->attachment_admission(recipient_,recipient_->primary_path(),
        recipient_identity_->device,recipient_identity_->inode,end);
    const auto b=target_profile_->attachment_admission(target_,path_,target_identity_->device,target_identity_->inode,end);
    if(a!=b)throw db_error("managed attachment crossed actual child cohorts");
    validate_wal_policy();
    if(std::chrono::steady_clock::now()>=end)throw db_error("managed attachment validation expired");
}
ordinary_cohort_attachment ordinary_cohort_attachment_access::bind(lattice_db& recipient,lattice_db& target,
    const std::string& alias,std::int64_t token,const std::shared_ptr<const physical_store_identity>& target_identity,
    const std::shared_ptr<const physical_store_identity>& recipient_identity,database::attachment_operation& operation) {
    const bool native_only=!recipient.config_.ordinary_context&&!target.config_.ordinary_context;
    const bool bounded=recipient.config_.wal_extent_limit_bytes.has_value()||target.config_.wal_extent_limit_bytes.has_value();
    if(native_only){
        ordinary_require_no_attachment(recipient.config_.ordinary_context);
        if(!bounded)return {};
        if(token<=0||!target_identity||!recipient_identity)
            throw db_error("native typed WAL attachment needs two actual physical views");
    } else if(!recipient.config_.ordinary_context||!target.config_.ordinary_context||token<=0||!target_identity||!recipient_identity)
        throw db_error("managed attachment needs two enrolled physical views");
    auto value=std::shared_ptr<ordinary_cohort_attachment_binding>(new ordinary_cohort_attachment_binding);
    value->recipient_=recipient.config_.ordinary_context;value->target_=target.config_.ordinary_context;
    value->recipient_profile_=recipient.ordinary_managed_profile_.load(std::memory_order_acquire);
    value->target_profile_=target.ordinary_managed_profile_.load(std::memory_order_acquire);
    value->recipient_guard_=recipient.guard_;value->target_guard_=target.guard_;
    value->recipient_identity_=recipient_identity;value->target_identity_=target_identity;
    value->path_=target.config_.path;value->alias_=alias;value->token_=token;value->creator_=process_id();
    value->recipient_path_=recipient.config_.path;value->recipient_wal_limit_=recipient.config_.wal_extent_limit_bytes;
    value->target_wal_limit_=target.config_.wal_extent_limit_bytes;value->native_wal_only_=native_only;
    value->validate(operation.deadline);return value;
}
void ordinary_cohort_attachment_access::validate(const ordinary_cohort_attachment& value,std::chrono::steady_clock::time_point end) {
    if(!value)throw db_error("managed attachment binding unavailable");value->validate(end);
}
void ordinary_cohort_attachment_access::validate_target(const ordinary_cohort_attachment& value,
    const std::shared_ptr<const physical_store_identity>& actual,std::chrono::steady_clock::time_point end) {
    validate(value,end);
    if(!actual||*actual!=*value->target_identity_)throw db_error("managed attachment target changed before reuse");
}
void ordinary_cohort_attachment_access::validate_target(const ordinary_cohort_attachment& value,
    const std::shared_ptr<const physical_store_identity>& actual,std::optional<sqlite3_int64> actual_wal_limit,
    std::chrono::steady_clock::time_point end) {
    validate_target(value,actual,end);
    if(value->target_wal_limit_!=actual_wal_limit)
        throw db_error("typed attachment requested target WAL policy changed before reuse");
}
int ordinary_cohort_attachment_access::authorize(sqlite3* connection,int action,const char* first,const char* second)noexcept {
    return one_action::authorize(connection,action,first,second);
}
std::shared_ptr<const physical_store_identity> ordinary_cohort_attachment_access::attach(database& connection,
    const ordinary_cohort_attachment& value,database::attachment_operation& operation,bool read_only) {
    validate(value,operation.deadline);operation.check();
    const auto recipient=connection.attachment_physical_identity(operation);
    if(connection.ordinary_context_!=value->recipient_||!recipient||*recipient!=*value->recipient_identity_)
        throw db_error("managed attachment used a different actual recipient handle");
    for(const auto& alias:connection.query_attachment_text_metadata("PRAGMA database_list","name",operation))
        if(alias==value->alias_)throw db_error("managed attachment alias was already present");
    const bool bounded=value->target_wal_limit_.has_value();
    const bool selected_read_only=read_only||(bounded&&connection.mode_==database::open_mode::read_only);
    const auto filename=selected_read_only?read_only_filename(value->path_):value->path_;
    const auto statement="ATTACH DATABASE "+sql_literal(filename)+" AS "+sql_alias(value->alias_);
    auto* native=connection.internal_handle();if(!native)throw db_error("typed attachment native original unavailable");
    const bool guarded=ordinary_requires_attachment_guard()||wal_extent_attachment_guard_required(native);
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    std::optional<wal_extent_vfs::member_operation> enrollment;
    if(bounded){
        if(connection.wal_extent_vfs_){
            if(!value->recipient_wal_limit_||!connection.wal_extent_vfs_->matches_primary_policy(native,
                value->recipient_path_,*value->recipient_wal_limit_))throw db_error("typed recipient actual WAL owner changed");
            enrollment.emplace(connection.wal_extent_vfs_->enroll_attachment(native,value->path_,value->alias_,
                *value->target_identity_,*value->target_wal_limit_,value->token_));
        } else if(connection.mode_!=database::open_mode::read_only)
            throw db_error("typed writable attachment requires its actual bounded VFS owner");
    }
#else
    if(bounded)throw db_error("typed bounded attachment platform unavailable");
#endif
    operation.check();
    std::shared_ptr<const physical_store_identity> actual;
    {
        one_action permit(native,SQLITE_ATTACH,filename);
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
        if(enrollment)enrollment->enter();
#endif
        actual=connection.attach_and_capture_identity(statement,value->alias_,operation);
        // The default-VFS null-context legacy read view has no installed
        // authorizer. Its owned literal + generatedRO + actual identity are
        // preserved, without fabricating consumed-permit authority.
        if(guarded)permit.finish();
    } // Permit ends before subsequent verification, view rebuild or delivery.
    if(!actual||*actual!=*value->target_identity_)throw db_error("managed attachment opened a different physical file");
    if(selected_read_only&&sqlite3_db_readonly(native,value->alias_.c_str())!=1)
        throw db_error("managed projection attachment was not opened read-only");
    validate(value,operation.deadline);
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    if(enrollment)enrollment->complete();
#endif
    return actual;
}
void ordinary_cohort_attachment_access::detach(database& connection,const ordinary_cohort_attachment& value,
    std::int64_t expected_token,database::attachment_operation& operation) {
    if(!value||value->creator_!=process_id()||value->token_!=expected_token||expected_token<=0)
        throw db_error("managed attachment cleanup binding changed");
    if(connection.ordinary_context_!=value->recipient_)
        throw db_error("managed attachment cleanup used a different recipient view");
    // Cleanup removes only the actual binding; an expired source/cohort does
    // not grant new access and must not prohibit retiring an existing alias.
    operation.check();
    bool present=false;
    for(const auto& alias:connection.query_attachment_text_metadata("PRAGMA database_list","name",operation))
        if(alias==value->alias_)present=true;
    if(!present)return; // Actual owned-handle observation, never an error-string inference.
    const auto statement="DETACH DATABASE "+sql_alias(value->alias_);
    auto* native=connection.internal_handle();if(!native)throw db_error("typed detach native original unavailable");
    const bool guarded=ordinary_requires_attachment_guard()||wal_extent_attachment_guard_required(native);
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    std::optional<wal_extent_vfs::member_operation> retirement;
    if(value->target_wal_limit_&&connection.wal_extent_vfs_)
        retirement.emplace(connection.wal_extent_vfs_->detach_attachment(native,value->path_,value->alias_,expected_token));
#endif
    {
        one_action permit(native,SQLITE_DETACH,value->alias_);
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
        if(retirement)retirement->enter();
#endif
        connection.execute_attachment_sql(statement,operation);
        if(guarded)permit.finish();
    }
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    if(retirement)retirement->complete();
#endif
}
}
