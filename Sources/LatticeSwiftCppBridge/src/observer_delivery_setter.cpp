#include <observer_delivery_setter.hpp>
#include <dynamic_object.hpp>
#include <lattice.hpp>
#include <lattice/detail/observer_delivery_update.hpp>
#include <atomic>
#include <utility>
#include <limits>

namespace lattice {
namespace {
using Values=detail::observer_delivery_values;
void add_bytes(uint64_t& out,uint64_t bytes){detail::observer_delivery_value_detail::add_structured_bytes(out,bytes);}
uint64_t fresh_capacity(uint64_t extent) {
    // Pinned libc++ builtin constructors use a size-derived capacity; no old
    // reserve is imported. Charge a conservative size-derived envelope before
    // allocation, then check the actual capacity before any Native effects.
    uint64_t result=0;add_bytes(result,extent);add_bytes(result,extent);add_bytes(result,32);return result;
}
uint64_t capacity_bytes(const column_value_t& input) {
    return std::visit([](const auto& value)->uint64_t {
        using T=std::decay_t<decltype(value)>;
        if constexpr(std::is_same_v<T,std::string>) {uint64_t bytes=value.capacity();add_bytes(bytes,1);return bytes;}
        else if constexpr(std::is_same_v<T,std::vector<uint8_t>>)return value.capacity();
        else return 0;
    },input);
}
uint64_t fresh_value_bytes(const column_value_t& input) {
    return std::visit([](const auto& value)->uint64_t {
        using T=std::decay_t<decltype(value)>;
        if constexpr(std::is_same_v<T,std::string>||std::is_same_v<T,std::vector<uint8_t>>)return fresh_capacity(value.size());
        else return 0;
    },input);
}
Values::value_view borrow_value(const column_value_t& input) {
    return std::visit([](const auto& actual)->Values::value_view {
        using T=std::decay_t<decltype(actual)>;
        if constexpr(std::is_same_v<T,std::string>)return std::string_view(actual);
        else if constexpr(std::is_same_v<T,std::vector<uint8_t>>)return std::span<const uint8_t>(actual);
        else return actual;
    },input);
}
column_value_t own_value(const Values::value_view& input) {
    return std::visit([](const auto& value)->column_value_t {
        using T=std::decay_t<decltype(value)>;
        if constexpr(std::is_same_v<T,std::string_view>)return std::string(value);
        else if constexpr(std::is_same_v<T,std::span<const uint8_t>>)return std::vector<uint8_t>(value.begin(),value.end());
        else return value;
    },input);
}
}
struct swift_delivery_setter::payload {
    const uint64_t creator;
    std::atomic<uint64_t> references{1};
    std::atomic<bool> attempted{false};
    bool text=false;
    std::weak_ptr<dynamic_object> object;
    std::shared_ptr<dynamic_object> running_object;
    detail::observer_delivery_target binding;
    detail::observer_delivery_update update;
    std::string column;
    std::unordered_map<std::string,column_value_t> cache;
    payload(uint64_t issuer,bool is_text,detail::observer_delivery_update&& original)
        :creator(issuer),text(is_text),update(std::move(original)){}
    bool current()const noexcept{return creator==detail::database_native_process();}
    static payload& checked(payload* original) {
        if(!original||!original->current())throw db_error("delivery setter original is inherited or unavailable");
        return *original;
    }
};
swift_delivery_setter::swift_delivery_setter(payload* original)noexcept:value_(original){}
bool swift_delivery_setter::retain(payload* original)noexcept {
    if(!original||!original->current())return false;
    auto count=original->references.load(std::memory_order_relaxed);
    for(;;) {
        // An unavailable/overflow copy is empty, never an inherited weak/Native
        // access or a C++ exception crossing an implicit Swift value copy.
        if(!count||count==std::numeric_limits<uint64_t>::max())return false;
        if(original->references.compare_exchange_weak(count,count+1,std::memory_order_relaxed))return true;
    }
}
void swift_delivery_setter::release(payload* original)noexcept {
    if(!original||!original->current())return;
    if(original->references.fetch_sub(1,std::memory_order_acq_rel)!=1)return;
    // The map has builtin values only. The actual object's final release can
    // run arbitrary owner cleanup, so check again before deleting the token.
    original->cache.clear();if(!original->current())return;
    original->column.clear();if(!original->current())return;
    original->running_object.reset();if(!original->current())return;
    original->object.reset();if(!original->current())return;
    delete original; // Core update's own destructor is creator-fenced.
}
swift_delivery_setter::swift_delivery_setter(const swift_delivery_setter& other)noexcept {
    if(retain(other.value_))value_=other.value_;
}
swift_delivery_setter::swift_delivery_setter(swift_delivery_setter&& other)noexcept
    :value_(std::exchange(other.value_,nullptr)){}
swift_delivery_setter& swift_delivery_setter::operator=(const swift_delivery_setter& other)noexcept {
    if(this!=&other){auto* next=retain(other.value_)?other.value_:nullptr;auto* prior=std::exchange(value_,next);release(prior);}return *this;
}
swift_delivery_setter& swift_delivery_setter::operator=(swift_delivery_setter&& other)noexcept {
    if(this!=&other){auto* prior=std::exchange(value_,std::exchange(other.value_,nullptr));release(prior);}return *this;
}
swift_delivery_setter::~swift_delivery_setter()noexcept{release(value_);}
bool swift_delivery_setter::prepared()const noexcept{return value_&&value_->current();}
swift_delivery_setter swift_delivery_setter::prepare(const dynamic_object_ref& ref,
    std::string_view column,bool text,uint64_t maximum) {
    if(!ref.impl_||!ref.impl_->lattice)return {};
    auto& owner=*ref.impl_->lattice;
    if(owner.is_closed())throw db_error("delivery setter owner is closed or inherited");
    if(!detail::observer_delivery_enabled(owner))return {};
    if(column.empty()||column.size()>255||column.find('\0')!=std::string_view::npos)
        throw db_error("delivery setter column is not bounded metadata");
    const auto issuer=detail::database_native_process();
    const auto target=ref.impl_->managed_.observer_delivery_target_for(column);
    auto update=detail::observer_delivery_update::prepare(owner,target,text?
        detail::observer_delivery_update::value_kind::text:detail::observer_delivery_update::value_kind::blob,maximum);
    uint64_t bytes=sizeof(payload);add_bytes(bytes,column.size()+1);
    // The existing SDK annotation obtains a Bridge table-name value and then
    // its Swift String. Both bounded metadata copies remain charged alongside
    // the actual holder; this counts structured UTF8, not allocator overhead.
    add_bytes(bytes,target.table().size()+1);add_bytes(bytes,target.table().size()+1);
    update.reserve_output_workspace(bytes);
    auto* result=new payload(issuer,text,std::move(update));
    try {
        if(!result->current()||owner.is_closed())throw db_error("delivery setter preparation changed owner/process");
        result->column.assign(column);
        if(!result->current())throw db_error("delivery setter metadata changed process");
        result->binding=ref.impl_->managed_.observer_delivery_target_for(result->column);
        if(!result->binding.same_binding(target))throw db_error("delivery setter binding changed during preparation");
        result->object=ref.impl_; // weak: no model/query image/graph retained between calls
        return swift_delivery_setter(result);
    }catch(...){release(result);throw;}
}
void swift_delivery_setter::apply(const uint8_t* bytes,int64_t signed_count,bool text)const {
    if(signed_count<0)throw db_error("delivery setter extent is negative");
    const auto count=static_cast<std::size_t>(signed_count);
    // Pin the ACTUAL original across arbitrary synchronous observer delivery.
    // A callback may drop every external Swift/C++ copy of this handle.
    const swift_delivery_setter retained(*this);
    auto& value=payload::checked(retained.value_);
    if(text!=value.text||value.attempted.exchange(true,std::memory_order_acq_rel))
        throw db_error("delivery setter accepts one matching supply/execute caller");
    // The private SDK entry keeps its object/ref alive during synchronous
    // apply. A copied prepared token alone promises no model lifetime.
    struct ending {
        payload* value;
        ~ending()noexcept {if(value->current())value->running_object.reset();}
    } retire_object{&value};
    value.running_object=value.object.lock(); // creator-fenced heap promotion
    if(!value.current())throw db_error("delivery setter object promotion changed process");
    if(!value.running_object||!value.running_object->lattice)
        throw db_error("delivery setter object expired or is no longer managed");
    const auto current=value.running_object->managed_.observer_delivery_target_for(value.column);
    if(!value.binding.same_binding(current))throw db_error("delivery setter actual binding changed before apply");
    if(!bytes&&count)throw db_error("delivery setter bytes unavailable");
    if(text)value.update.supply(std::string_view(reinterpret_cast<const char*>(bytes?bytes:reinterpret_cast<const uint8_t*>("")),count));
    else value.update.supply(std::span<const uint8_t>(bytes,count));
    if(!value.current())throw db_error("delivery setter supply changed process");
    const auto immutable=value.update.value();
    const auto& old=value.running_object->managed_.source.values;
    if(old.size()>Values::column_ceiling)throw db_error("delivery cache field bound exceeded");
    static constexpr uint64_t bucket_ceiling=1024;
    uint64_t workspace=0;
    // The old map is transferred into this same retained original at adoption.
    // Account its ACTUAL reserve/capacity, even when logical rows are tiny.
    const auto old_buckets=uint64_t(old.bucket_count());
    if(old_buckets>std::numeric_limits<uint64_t>::max()/sizeof(void*))throw db_error("delivery cache bucket extent overflow");
    add_bytes(workspace,old_buckets*sizeof(void*));
    add_bytes(workspace,uint64_t(old.size())*sizeof(decltype(value.cache)::value_type));
    for(const auto& [name,contents]:old) {
        if(name.size()>255)throw db_error("delivery cache key bound exceeded");
        add_bytes(workspace,name.capacity());add_bytes(workspace,1);add_bytes(workspace,capacity_bytes(contents));
    }
    // Build a FRESH bounded map. Copy assignment would import the caller's
    // arbitrary old bucket_count under libc++, independently of old.size().
    add_bytes(workspace,bucket_ceiling*sizeof(void*));
    add_bytes(workspace,uint64_t(old.size()+1)*sizeof(decltype(value.cache)::value_type));
    for(const auto& [name,contents]:old) {
        add_bytes(workspace,fresh_capacity(name.size()));add_bytes(workspace,fresh_value_bytes(contents));
    }
    add_bytes(workspace,fresh_capacity(value.column.size()));
    std::visit([&](const auto& actual){
        using T=std::decay_t<decltype(actual)>;
        if constexpr(std::is_same_v<T,std::string_view>||std::is_same_v<T,std::span<const uint8_t>>)
            add_bytes(workspace,fresh_capacity(actual.size()));
    },immutable);
    value.update.reserve_output_workspace(workspace);
    if(!value.current())throw db_error("delivery setter cache admission changed process");
    value.cache.max_load_factor(1.0f);value.cache.rehash(Values::column_ceiling*2+1);
    if(!value.current())throw db_error("delivery setter bucket allocation changed process");
    if(value.cache.bucket_count()>bucket_ceiling)throw db_error("delivery fresh cache bucket bound exceeded");
    for(const auto& [name,contents]:old) {
        value.cache.emplace(std::string(name.data(),name.size()),own_value(borrow_value(contents)));
        if(!value.current())throw db_error("delivery setter cache copy changed process");
        const auto& copied=*value.cache.find(name);
        if(copied.first.capacity()+1>fresh_capacity(name.size())||capacity_bytes(copied.second)>fresh_value_bytes(contents))
            throw db_error("delivery fresh cache value capacity exceeded");
    }
    value.cache.erase(value.column);
    value.cache.emplace(std::string(value.column.data(),value.column.size()),own_value(immutable));
    if(!value.current())throw db_error("delivery setter cache replacement changed process");
    const auto& replaced=*value.cache.find(value.column);
    uint64_t replacement_bytes=0;
    std::visit([&](const auto& actual){using T=std::decay_t<decltype(actual)>;
        if constexpr(std::is_same_v<T,std::string_view>||std::is_same_v<T,std::span<const uint8_t>>)
            replacement_bytes=fresh_capacity(actual.size());
    },immutable);
    if(value.cache.bucket_count()>bucket_ceiling||replaced.first.capacity()+1>fresh_capacity(value.column.size())||
       capacity_bytes(replaced.second)>replacement_bytes)
        throw db_error("delivery fresh replacement capacity exceeded");
    value.update.execute(*value.running_object->lattice,{&value,[](void* raw,bool committed,bool)noexcept {
        auto& actual=*static_cast<payload*>(raw);
        if(!actual.current()||!committed)return;
        // Real missing-row success preserves legacy cache adoption too.
        actual.running_object->managed_.source.values.swap(actual.cache);
    }});
    if(!value.current())throw db_error("delivery setter completion changed process");
}
void swift_delivery_setter::apply_text(const uint8_t* bytes,int64_t signed_count)const {
    sealed([&]{apply(bytes,signed_count,true);});
}
void swift_delivery_setter::apply_bytes(const uint8_t* bytes,int64_t signed_count)const {
    sealed([&]{apply(bytes,signed_count,false);});
}
bool dynamic_object_ref::delivery_profile_enabled()const {
    return sealed([&]{return impl_&&impl_->lattice&&detail::observer_delivery_enabled(*impl_->lattice);});
}
swift_delivery_setter dynamic_object_ref::prepare_delivery_text(const uint8_t* column,int64_t signed_count,uint64_t maximum)const {
    return sealed([&]{if(signed_count<0)throw db_error("delivery column extent is negative");
        const auto count=static_cast<std::size_t>(signed_count);if(!column&&count)throw db_error("delivery column bytes unavailable");
        return swift_delivery_setter::prepare(*this,std::string_view(reinterpret_cast<const char*>(column?column:reinterpret_cast<const uint8_t*>("")),count),true,maximum);});
}
swift_delivery_setter dynamic_object_ref::prepare_delivery_bytes(const uint8_t* column,int64_t signed_count,uint64_t maximum)const {
    return sealed([&]{if(signed_count<0)throw db_error("delivery column extent is negative");
        const auto count=static_cast<std::size_t>(signed_count);if(!column&&count)throw db_error("delivery column bytes unavailable");
        return swift_delivery_setter::prepare(*this,std::string_view(reinterpret_cast<const char*>(column?column:reinterpret_cast<const uint8_t*>("")),count),false,maximum);});
}
}
