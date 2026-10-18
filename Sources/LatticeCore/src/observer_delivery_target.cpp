#include <lattice/detail/observer_delivery_target.hpp>
#include <lattice/db.hpp>
#include <lattice/lattice.hpp>
#include <atomic>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>

namespace lattice::detail {
struct observer_delivery_identity::payload final {
    const uint64_t creator;
    std::atomic<size_t> references{1};
    const lattice_db* owner=nullptr;
    std::string table,global_id;
    int64_t attachment=0;
    std::atomic<int64_t> rowid{0};
    std::weak_ptr<database> writer;
    std::weak_ptr<instance_guard> lifetime;
    std::weak_ptr<const observer_delivery_connection> profile;
    explicit payload(uint64_t process):creator(process){}
    bool current()const noexcept{return creator==database_native_process();}
};
observer_delivery_identity::observer_delivery_identity(payload* value)noexcept:value_(value){}
uint64_t observer_delivery_identity::allocation_bytes()noexcept{return sizeof(payload);}
void observer_delivery_identity::retain(payload* value) {
    if(!value||!value->current())return;
    auto count=value->references.load(std::memory_order_relaxed);
    do {
        if(count==std::numeric_limits<size_t>::max())
            throw std::length_error("delivery binding reference count exhausted");
    }while(!value->references.compare_exchange_weak(count,count+1,
        std::memory_order_relaxed,std::memory_order_relaxed));
}
void observer_delivery_identity::release(payload* value)noexcept {
    if(!value||!value->current())return;
    if(value->references.fetch_sub(1,std::memory_order_acq_rel)!=1)return;
    value->profile.reset();if(!value->current())return;
    value->writer.reset();if(!value->current())return;
    value->lifetime.reset();if(!value->current())return;
    delete value; // all inherited Native/owner weak ownership is already empty
}
const observer_delivery_identity::payload& observer_delivery_identity::checked(payload* value,bool published) {
    if(!value||!value->current()||(published&&value->rowid.load(std::memory_order_acquire)<=0))
        throw db_error("delivery binding is unavailable, inherited or unpublished");
    return *value;
}
observer_delivery_identity observer_delivery_identity::prepare(lattice_db& owner,
    const std::shared_ptr<database>& writer,const std::shared_ptr<instance_guard>& lifetime,
    const std::shared_ptr<const observer_delivery_connection>& profile,
    std::string_view table,std::string_view global_id,int64_t attachment) {
    owner.require_creator_process(); // before inherited originals or input access
    const auto creator=database_native_process();
    auto* value=new payload(creator);
    try {
        checked(value,false);
        owner.require_creator_process(); // allocation may have continued in a child
        if(!writer||!lifetime||!profile||table.empty()||global_id.empty())
            throw db_error("delivery binding requires its actual prepared originals");
        value->owner=&owner;value->attachment=attachment;
        value->writer=writer;value->lifetime=lifetime;value->profile=profile;
        value->table.assign(table);checked(value,false);
        value->global_id.assign(global_id);checked(value,false);
        return observer_delivery_identity(value);
    }catch(...){release(value);throw;}
}
void observer_delivery_identity::publish(int64_t actual_rowid)const noexcept {
    if(!value_||!value_->current()||actual_rowid<=0)return;
    int64_t expected=0;
    (void)value_->rowid.compare_exchange_strong(expected,actual_rowid,
        std::memory_order_release,std::memory_order_relaxed);
    // Only the actual binding/adoption code calls this once. A different
    // second rowid cannot rewrite this original or mint a successor identity.
}
observer_delivery_identity::observer_delivery_identity(const observer_delivery_identity& other) {
    if(other.value_&&other.value_->current()){retain(other.value_);value_=other.value_;}
}
observer_delivery_identity::observer_delivery_identity(observer_delivery_identity&& other)noexcept
    :value_(std::exchange(other.value_,nullptr)){}
observer_delivery_identity& observer_delivery_identity::operator=(const observer_delivery_identity& other) {
    if(this==&other)return *this;
    auto* next=other.value_;
    if(next&&!next->current())next=nullptr;
    retain(next);auto* previous=std::exchange(value_,next);release(previous);return *this;
}
observer_delivery_identity& observer_delivery_identity::operator=(observer_delivery_identity&& other)noexcept {
    if(this==&other)return *this;
    auto* previous=std::exchange(value_,std::exchange(other.value_,nullptr));release(previous);return *this;
}
observer_delivery_identity::~observer_delivery_identity()noexcept {release(value_);}
bool observer_delivery_identity::valid()const noexcept {
    return value_&&value_->current()&&value_->rowid.load(std::memory_order_acquire)>0;
}
std::string_view observer_delivery_identity::table()const{return checked(value_).table;}
std::string_view observer_delivery_identity::global_id()const{return checked(value_).global_id;}
int64_t observer_delivery_identity::rowid()const{return checked(value_).rowid.load(std::memory_order_acquire);}
int64_t observer_delivery_identity::attachment_generation()const{return checked(value_).attachment;}
const lattice_db* observer_delivery_identity::owner_address()const{return checked(value_).owner;}
const std::weak_ptr<database>& observer_delivery_identity::writer_identity()const{return checked(value_).writer;}
const std::weak_ptr<instance_guard>& observer_delivery_identity::owner_lifetime()const{return checked(value_).lifetime;}
const std::weak_ptr<const observer_delivery_connection>& observer_delivery_identity::profile_identity()const{return checked(value_).profile;}

observer_delivery_target::observer_delivery_target(const observer_delivery_identity& identity,std::string_view column)
    :identity_(identity),column_(column) {
    if(!identity_.valid()||column.empty()||column.size()>255||column.find('\0')!=std::string_view::npos)
        throw db_error("delivery UPDATE target requires its actual published field binding");
}
bool observer_delivery_target::same_binding(const observer_delivery_target& other)const noexcept {
    return valid()&&other.valid()&&identity_.value_==other.identity_.value_&&column_==other.column_;
}
bool observer_delivery_target::valid()const noexcept{return identity_.valid()&&!column_.empty();}
std::string_view observer_delivery_target::table()const{return identity_.table();}
std::string_view observer_delivery_target::global_id()const{return identity_.global_id();}
std::string_view observer_delivery_target::column()const{(void)identity_.rowid();return column_;}
int64_t observer_delivery_target::rowid()const{return identity_.rowid();}
int64_t observer_delivery_target::attachment_generation()const{return identity_.attachment_generation();}
}
