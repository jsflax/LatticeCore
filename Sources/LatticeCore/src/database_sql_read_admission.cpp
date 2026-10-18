#include "database_sql_read_admission.hpp"
#include <lattice/db.hpp>
#include <sqlite3.h>
#include <utility>
namespace lattice::detail {
database_sql_read_profile::database_sql_read_profile(uint64_t issuer,const void* publisher,
    observer_delivery_store store,observer_delivery_store::client client,database_sql_read_policy policy,
    std::chrono::milliseconds maximum_wait)
    :creator_(issuer),publisher_(publisher),store_(std::move(store)),client_(std::move(client)),
     policy_(policy),maximum_admission_wait_(maximum_wait){}
void database_sql_read_profile::destroy(const database_sql_read_profile* value)noexcept {
    if(value&&value->creator_==database_native_process())delete value;
}
std::shared_ptr<const database_sql_read_profile> database_sql_read_profile::create(uint64_t issuer,
    const void* publisher,observer_delivery_store store,observer_delivery_store::client client,
    database_sql_read_policy policy,std::chrono::milliseconds maximum_wait) {
    if(issuer!=database_native_process()||!publisher||!policy.workspace_bytes||
       policy.workspace_bytes==UINT64_MAX||!policy.columns||!policy.rows||
       policy.columns==UINT64_MAX||policy.rows==UINT64_MAX||policy.maximum_duration.count()<=0||
       maximum_wait.count()<=0||policy.maximum_duration>std::chrono::minutes(1)||maximum_wait>std::chrono::minutes(1))
        throw db_error("read profile requires finite actual count/byte/deadline declarations");
    auto result=std::shared_ptr<const database_sql_read_profile>(new database_sql_read_profile(issuer,publisher,
        std::move(store),std::move(client),policy,maximum_wait),destroy);
    if(issuer!=database_native_process())throw db_error("inherited read profile allocation");
    return result;
}
void database_sql_read_profile::publish()const noexcept {
    if(creator_==database_native_process())published_.store(true,std::memory_order_release);
}
database_sql_read_binding::database_sql_read_binding(uint64_t issuer,const database* owner,
    std::shared_ptr<const database_sql_read_profile> profile)
    :creator_(issuer),owner_(owner),profile_(std::move(profile)){}
void database_sql_read_binding::destroy(const database_sql_read_binding* value)noexcept {
    if(value&&value->creator_==database_native_process())delete value;
}
bool database_sql_read_binding::current(const database& owner)const noexcept {
    return creator_==database_native_process()&&owner_==&owner&&owner.creator_current_()&&
        profile_&&profile_->creator_==creator_&&profile_->published_.load(std::memory_order_acquire)&&
        std::atomic_load(&owner.sql_read_binding_).get()==this;
}
std::shared_ptr<const database_sql_read_binding> database_sql_read_binding::install(database& owner,
    const std::shared_ptr<const database_sql_read_profile>& profile) {
    owner.require_creator_();const auto issuer=database_native_process();
    if(!profile||profile->creator_!=issuer||!profile->store_.current_for(owner))
        throw db_error("reader original differs from its actual profile physical store");
    auto result=std::shared_ptr<const database_sql_read_binding>(new database_sql_read_binding(issuer,&owner,profile),destroy);
    if(issuer!=database_native_process())throw db_error("inherited reader binding allocation");
    // Existing exact-original lexical Native custody; no domain admission or
    // waiting for capacity occurs here. No caller-owned statement is retired.
    database::catalog_native_admission_ admitted(owner);
    if(!owner.db_||owner.closed_.load(std::memory_order_acquire)||
       owner.raw_handle_escaped_.load(std::memory_order_acquire)||owner.engine_reads_||
       sqlite3_get_autocommit(owner.db_)==0||sqlite3_next_stmt(owner.db_,nullptr)||
       std::atomic_load(&owner.sql_read_binding_))
        throw db_error("read binding requires its idle unescaped actual original");
    std::atomic_store(&owner.sql_read_binding_,result);
    return result;
}
void database_sql_read_binding::remove(database& owner,
    const std::shared_ptr<const database_sql_read_binding>& expected)noexcept {
    if(!expected||expected->creator_!=database_native_process()||expected->owner_!=&owner)return;
    auto actual=expected;
    std::atomic_compare_exchange_strong(&owner.sql_read_binding_,&actual,
        std::shared_ptr<const database_sql_read_binding>{});
    // Pure DATA only. The caller's heap custody still pins expected/profile;
    // this is neither a Native ending nor any original/delivery refund.
}
}
