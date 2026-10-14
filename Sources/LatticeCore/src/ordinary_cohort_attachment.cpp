#include "ordinary_cohort_attachment.hpp"
#include <lattice/lattice.hpp>
#include <stdexcept>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <unistd.h>
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
        if(!ordinary_requires_attachment_guard())return SQLITE_OK;
        return ordinary_cohort_attachment_access::authorize(connection,action,first,second);
    } catch(...) {return SQLITE_DENY;}
}
int restore_ordinary_attachment_guard(sqlite3* connection) noexcept {
    try {
        if(!ordinary_creator_current({}))return SQLITE_MISUSE;
        if(!ordinary_requires_attachment_guard())return sqlite3_set_authorizer(connection,nullptr,nullptr);
        return sqlite3_set_authorizer(connection,
            [](void* raw,int action,const char* one,const char* two,const char*,const char*) noexcept {
                return ordinary_composed_attachment_authorize(static_cast<sqlite3*>(raw),action,one,two);
            },connection);
    }catch(...){return SQLITE_MISUSE;}
}
void ordinary_cohort_attachment_binding::validate(std::chrono::steady_clock::time_point end)const {
    if(creator_!=process_id()||!recipient_guard_||!target_guard_||
        !recipient_guard_->alive.load(std::memory_order_acquire)||!target_guard_->alive.load(std::memory_order_acquire)||
        !recipient_profile_||!target_profile_||!recipient_identity_||!target_identity_)
        throw db_error("managed attachment retained owner unavailable");
    const auto a=recipient_profile_->attachment_admission(recipient_,recipient_->primary_path(),
        recipient_identity_->device,recipient_identity_->inode,end);
    const auto b=target_profile_->attachment_admission(target_,path_,target_identity_->device,target_identity_->inode,end);
    if(a!=b)throw db_error("managed attachment crossed actual child cohorts");
    if(std::chrono::steady_clock::now()>=end)throw db_error("managed attachment validation expired");
}
ordinary_cohort_attachment ordinary_cohort_attachment_access::bind(lattice_db& recipient,lattice_db& target,
    const std::string& alias,std::int64_t token,const std::shared_ptr<const physical_store_identity>& target_identity,
    const std::shared_ptr<const physical_store_identity>& recipient_identity,database::attachment_operation& operation) {
    if(!recipient.config_.ordinary_context&&!target.config_.ordinary_context){
        ordinary_require_no_attachment(recipient.config_.ordinary_context);return {};
    }
    if(!recipient.config_.ordinary_context||!target.config_.ordinary_context||token<=0||!target_identity||!recipient_identity)
        throw db_error("managed attachment needs two enrolled physical views");
    auto value=std::shared_ptr<ordinary_cohort_attachment_binding>(new ordinary_cohort_attachment_binding);
    value->recipient_=recipient.config_.ordinary_context;value->target_=target.config_.ordinary_context;
    value->recipient_profile_=recipient.ordinary_managed_profile_.load(std::memory_order_acquire);
    value->target_profile_=target.ordinary_managed_profile_.load(std::memory_order_acquire);
    value->recipient_guard_=recipient.guard_;value->target_guard_=target.guard_;
    value->recipient_identity_=recipient_identity;value->target_identity_=target_identity;
    value->path_=target.config_.path;value->alias_=alias;value->token_=token;value->creator_=process_id();
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
    const auto filename=read_only?read_only_filename(value->path_):value->path_;
    const auto statement="ATTACH DATABASE "+sql_literal(filename)+" AS "+sql_alias(value->alias_);
    std::shared_ptr<const physical_store_identity> actual;
    {
        one_action permit(connection.internal_handle(),SQLITE_ATTACH,filename);
        actual=connection.attach_and_capture_identity(statement,value->alias_,operation);
        permit.finish();
    } // Permit ends before subsequent verification, view rebuild or delivery.
    if(!actual||*actual!=*value->target_identity_)throw db_error("managed attachment opened a different physical file");
    if(read_only&&sqlite3_db_readonly(connection.internal_handle(),value->alias_.c_str())!=1)
        throw db_error("managed projection attachment was not opened read-only");
    validate(value,operation.deadline);return actual;
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
    {
        one_action permit(connection.internal_handle(),SQLITE_DETACH,value->alias_);
        connection.execute_attachment_sql(statement,operation);
        permit.finish();
    }
}
}
