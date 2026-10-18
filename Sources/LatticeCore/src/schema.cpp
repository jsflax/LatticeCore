#include "lattice/schema.hpp"
#include "lattice/recovery_schema.hpp"
#include <limits>
#include <stdexcept>
#if defined(_WIN32)
#include <process.h>
#else
#include <unistd.h>
#endif

namespace lattice::detail {
std::uint64_t schema_registry_process_v2()noexcept {
#if defined(_WIN32)
    return static_cast<std::uint64_t>(::_getpid());
#else
    return static_cast<std::uint64_t>(::getpid());
#endif
}
struct schema_generation_v2 {
    std::uint64_t number=0;
    schema_model_map_v2 models;
    std::map<std::string,std::string,std::less<>> types;
};
namespace {
// No automatic inherited shared payload is destroyed after a callback/allocator
// changes processes. Pure retained DATA has no Native/SQL or callback owner.
template<class T> class local_schema_shared {
    using shared=std::shared_ptr<const T>;
    const std::uint64_t creator_=schema_registry_process_v2();
    alignas(shared) unsigned char storage_[sizeof(shared)];
public:
    local_schema_shared()noexcept{new(storage_)shared;}
    ~local_schema_shared()noexcept{if(creator_==schema_registry_process_v2())value().~shared();}
    local_schema_shared(const local_schema_shared&)=delete;
    shared& value()noexcept{return *std::launder(reinterpret_cast<shared*>(storage_));}
    const shared& value()const noexcept{return *std::launder(reinterpret_cast<const shared*>(storage_));}
    void adopt(shared&& actual){if(creator_!=schema_registry_process_v2())throw std::runtime_error("inherited registry model allocation");value()=std::move(actual);}
};
template<class T> struct schema_delete {
    std::uint64_t creator;
    void operator()(const T* value)const noexcept{if(creator==schema_registry_process_v2())delete value;}
};
template<class T> struct raw_schema_owner {
    std::uint64_t creator;T* value=nullptr;
    ~raw_schema_owner()noexcept{if(creator==schema_registry_process_v2())delete value;}
    T* release()noexcept{auto* held=value;value=nullptr;return held;}
};
}
}
namespace lattice {
using detail::schema_registry_process_v2;
namespace {
// Constant initialization precedes every cross-TU static registrar. Claim the
// actual PID before a function-local singleton guard can be inherited live.
constinit std::atomic<std::uint64_t> schema_singleton_creator{0};
constinit std::atomic<schema_registry*> schema_singleton_ready{nullptr};
void require_schema_singleton_creator() {
    const auto actual=schema_registry_process_v2();
    const auto recorded=schema_singleton_creator.load(std::memory_order_acquire);
    if(recorded==actual)return; // hot reads never perform an RMW
    if(recorded)throw std::runtime_error("inherited schema registry belongs to another process");
    std::uint64_t empty=0;
    if(!schema_singleton_creator.compare_exchange_strong(empty,actual,std::memory_order_acq_rel)&&empty!=actual)
        throw std::runtime_error("inherited schema registry belongs to another process");
}
}
schema_snapshot_v2::shared& schema_snapshot_v2::shared_()noexcept{return *std::launder(reinterpret_cast<shared*>(shared_storage_));}
const schema_snapshot_v2::shared& schema_snapshot_v2::shared_()const noexcept{return *std::launder(reinterpret_cast<const shared*>(shared_storage_));}
schema_snapshot_v2::schema_snapshot_v2()noexcept:creator_(schema_registry_process_v2()){new(shared_storage_)shared;}
schema_snapshot_v2::schema_snapshot_v2(const schema_snapshot_v2& other)noexcept:schema_snapshot_v2(){
    if(other.creator_==creator_)shared_()=other.shared_();
}
schema_snapshot_v2::schema_snapshot_v2(schema_snapshot_v2&& other)noexcept:schema_snapshot_v2(){
    if(other.creator_==creator_)shared_()=std::move(other.shared_());
}
schema_snapshot_v2& schema_snapshot_v2::operator=(const schema_snapshot_v2& other){
    require_creator_();other.require_creator_();if(this!=&other)shared_()=other.shared_();return *this;
}
schema_snapshot_v2& schema_snapshot_v2::operator=(schema_snapshot_v2&& other){
    require_creator_();other.require_creator_();if(this!=&other)shared_()=std::move(other.shared_());return *this;
}
schema_snapshot_v2::~schema_snapshot_v2()noexcept{if(creator_==schema_registry_process_v2())shared_().~shared();}
void schema_snapshot_v2::require_creator_()const{if(creator_!=schema_registry_process_v2())throw std::runtime_error("inherited schema snapshot belongs to another process");}
void schema_snapshot_v2::adopt_(shared&& value){require_creator_();shared_()=std::move(value);}
bool schema_snapshot_v2::valid()const noexcept{return creator_==schema_registry_process_v2()&&bool(shared_());}
std::size_t schema_snapshot_v2::size()const{require_creator_();return shared_()?shared_()->models.size():0;}
std::uint64_t schema_snapshot_v2::generation()const{require_creator_();return shared_()?shared_()->number:0;}
schema_snapshot_v2::iterator::iterator(const detail::schema_generation_v2* owner,detail::schema_model_map_v2::const_iterator at)
    :generation_(owner),at_(at){}
const model_schema* schema_snapshot_v2::iterator::operator*()const{
    if(creator_!=schema_registry_process_v2()||!generation_)throw std::runtime_error("inherited or empty schema iterator");return at_->second.get();
}
schema_snapshot_v2::iterator& schema_snapshot_v2::iterator::operator++(){
    if(creator_!=schema_registry_process_v2()||!generation_)throw std::runtime_error("inherited or empty schema iterator");++at_;return *this;
}
bool schema_snapshot_v2::iterator::operator==(const iterator& other)const{
    if(creator_!=schema_registry_process_v2()||other.creator_!=schema_registry_process_v2())throw std::runtime_error("inherited schema iterator comparison");
    if(generation_!=other.generation_)return false;return !generation_||at_==other.at_;
}
schema_snapshot_v2::iterator schema_snapshot_v2::begin()const{require_creator_();return shared_()?iterator(shared_().get(),shared_()->models.begin()):iterator{};}
schema_snapshot_v2::iterator schema_snapshot_v2::end()const{require_creator_();return shared_()?iterator(shared_().get(),shared_()->models.end()):iterator{};}
schema_handle_v2 schema_snapshot_v2::get_schema(std::string_view name)const{
    require_creator_();if(!shared_())return {};
    const auto found=shared_()->models.find(name);return found==shared_()->models.end()?schema_handle_v2{}:schema_handle_v2(*this,found->second.get());
}
schema_handle_v2 schema_snapshot_v2::get_schema(const std::type_info& type)const{
    require_creator_();if(!shared_())return {};
    const auto found=shared_()->types.find(std::string_view(type.name()));return found==shared_()->types.end()?schema_handle_v2{}:get_schema(found->second);
}
const model_schema* schema_handle_v2::operator->()const{if(!valid())throw std::runtime_error("empty or inherited schema handle");return model_;}
schema_registry::fields& schema_registry::fields_()const noexcept{return *std::launder(reinterpret_cast<fields*>(const_cast<unsigned char*>(fields_storage_)));}
schema_registry::schema_registry()noexcept:creator_(schema_registry_process_v2()){new(fields_storage_)fields;}
schema_registry::~schema_registry()noexcept{if(creator_==schema_registry_process_v2())fields_().~fields();}
void schema_registry::require_creator_()const{if(creator_!=schema_registry_process_v2())throw std::runtime_error("inherited schema registry belongs to another process");}
schema_registry::publication_hold::publication_hold(const schema_registry& owner,bool try_only):creator_(schema_registry_process_v2()){
    owner.require_creator_();auto* actual=&owner.fields_();
    if(try_only){if(!actual->gate.try_lock())return;}else actual->gate.lock();
    if(creator_!=schema_registry_process_v2())throw std::runtime_error("inherited registry publication return");actual_=actual;
}
schema_registry::publication_hold::~publication_hold()noexcept{if(creator_==schema_registry_process_v2()&&actual_)actual_->gate.unlock();}
schema_registry& schema_registry::instance(){
    require_schema_singleton_creator();static schema_registry registry;registry.require_creator_();
    if(schema_singleton_ready.load(std::memory_order_acquire)!=&registry)
        schema_singleton_ready.store(&registry,std::memory_order_release);
    return registry;
}
std::optional<schema_snapshot_v2> schema_registry::try_entry_snapshot_(){
    require_schema_singleton_creator(); // BEFORE observing a parent's address
    auto* actual=schema_singleton_ready.load(std::memory_order_acquire);
    if(!actual)return std::nullopt; // no waiting on a singleton initialization guard
    schema_snapshot_v2 result;
    {publication_hold held(*actual,true);if(!held.entered())return std::nullopt;
     result.shared_()=actual->fields_().current;}
    return result;
}
schema_snapshot_v2 schema_registry::all_schemas()const{
    require_creator_();schema_snapshot_v2 result;
    {publication_hold held(*this);result.shared_()=fields_().current;}
    return result; // Only a real retained immutable generation; no map/SQL copy.
}
std::optional<schema_snapshot_v2> schema_registry::bounded_schemas(std::size_t maximum)const{
    auto actual=all_schemas();if(actual.size()>maximum)return std::nullopt;return actual;
}
schema_handle_v2 schema_registry::get_schema(const std::type_info& type)const{return all_schemas().get_schema(type);}
schema_handle_v2 schema_registry::get_schema(std::string_view name)const{return all_schemas().get_schema(name);}
void schema_registry::register_model(const std::type_info& type,model_schema schema){
    require_creator_();const auto creator=schema_registry_process_v2();
    detail::raw_schema_owner<model_schema> incoming_raw{creator,new model_schema(std::move(schema))};
    require_creator_();detail::local_schema_shared<model_schema> incoming;
    incoming.adopt(std::shared_ptr<const model_schema>(incoming_raw.release(),detail::schema_delete<model_schema>{creator}));
    require_creator_();const std::string type_name(type.name());require_creator_();
    for(;;){
        auto prior=all_schemas();
        detail::raw_schema_owner<detail::schema_generation_v2> next{creator,
            prior.shared_()?new detail::schema_generation_v2(*prior.shared_()):new detail::schema_generation_v2};
        require_creator_();
        if(next.value->number==std::numeric_limits<std::uint64_t>::max())throw std::overflow_error("schema generation exhausted");
        ++next.value->number;
        next.value->types[type_name]=incoming.value()->table_name;require_creator_();
        next.value->models[incoming.value()->table_name]=incoming.value();require_creator_();
        schema_snapshot_v2 candidate;
        candidate.adopt_(std::shared_ptr<const detail::schema_generation_v2>(next.release(),detail::schema_delete<detail::schema_generation_v2>{creator}));
        require_creator_();
        // Actual test-only observation is off the publication gate. It may
        // block/reenter/throw; the same real compare below resolves a successor.
        if(const auto* observed=fields_().observation.load(std::memory_order_acquire))
            if(observed->after_candidate)observed->after_candidate(observed->context,*this);
        require_creator_();bool published=false;schema_snapshot_v2 retired;
        {
            publication_hold held(*this);
            if(fields_().current==prior.shared_()){
                retired.shared_()=std::move(fields_().current);
                fields_().current=std::move(candidate.shared_());published=true;
            }
        }
        // Retired/candidate/prior/incoming ownership releases only off-gate.
        // A reentrant or racing publication causes DATA rebase, never SQL/body retry.
        if(published)return;
    }
}
}

namespace lattice::detail {
thread_local schema_read_scope_v2* schema_read_scope_v2::current_=nullptr;
schema_read_scope_v2::schema_read_scope_v2(bool may_capture,bool try_only) {
    // The singleton creator fence precedes inherited TLS/lease inspection.
    require_schema_singleton_creator();
    if(current_) {
        view_=current_->view_;previous_=current_;entered_=true;current_=this;
    } else if(may_capture) {
        if(try_only) {
            auto actual=schema_registry::try_entry_snapshot_();
            if(!actual){publication_busy_=true;return;}
            view_=std::move(*actual);
        }else view_=schema_registry::instance().all_schemas();
        entered_=true;current_=this;
    }
}
schema_read_scope_v2::~schema_read_scope_v2()noexcept {
    if(creator_!=schema_registry_process_v2()){current_=nullptr;return;}
    if(entered_)current_=previous_;
}
schema_handle_v2 schema_read_scope_v2::lookup(std::string_view name) {
    require_schema_singleton_creator(); // process fence before TLS; no singleton guard under Native
    if(!current_)throw std::runtime_error("schema lookup lacks its pre-Native declaration lease");
    return current_->view_.get_schema(name);
}
bool recovery_owner_schema::charge(size_t size) {
    if(!valid_||size>max_bytes-bytes_){valid_=false;return false;}
    bytes_+=size;return true;
}
bool recovery_owner_schema::field(const std::string& value) {
    if(value.size()>65536){valid_=false;return false;}
    return charge(value.size()+16);
}
bool recovery_owner_schema::property(const property_descriptor& p) {
    if(properties_==max_properties){valid_=false;return false;}
    ++properties_;
    if(!charge(128)||!field(p.name)||!field(p.target_table)||!field(p.link_table)||
       !field(p.column_name)||!field(p.union_desc.union_table_name)||
       !admit_count(p.union_desc.cases.size(),64,32))return false;
    for(const auto& c:p.union_desc.cases) {
        if(!field(c.case_name)||!admit_count(c.values.size(),32,32))return false;
        for(const auto& v:c.values)if(!field(v.param_name)||!field(v.link_target))return false;
    }
    return true;
}
bool recovery_owner_schema::admit_count(size_t count,size_t cap,size_t overhead) {
    if(count>cap||overhead>max_bytes||(overhead&&count>max_bytes/overhead)){valid_=false;return false;}
    return charge(count*overhead);
}
bool recovery_owner_schema::admit_field(const std::string& value){return field(value);}
bool recovery_owner_schema::admit_property(const property_descriptor& value){return property(value);}
bool recovery_owner_schema::admit_model(const model_schema& value) {
    if(models.size()>=max_models||models.count(value.table_name)||value.properties.size()>256){valid_=false;return false;}
    if(!field(value.table_name))return false;
    for(const auto& p:value.properties)if(!property(p))return false;
    models.emplace(value.table_name,value);return true;
}
recovery_owner_schema recovery_owner_schema::capture_native() {
    recovery_owner_schema result;
    // One genuine immutable lease survives every bounded model/property copy.
    // Allocator reentry may publish a successor, but cannot change this view.
    result.native_fallback_=schema_registry::instance().all_schemas();
    result.native_fallback_ready_=true;
    if(result.native_fallback_.size()>max_models){result.refuse();return result;}
    for(const auto* model:result.native_fallback_)if(!result.admit_model(*model))break;
    // A valid bounded catalog owns every declaration by deep copy. Its unused
    // fallback must not anchor a retired whole global generation indefinitely.
    // A later explicit refuse obtains its actual pre-open fallback anew.
    if(result.valid_){result.native_fallback_={};result.native_fallback_ready_=false;}
    return result;
}
const model_schema* recovery_owner_schema::find(const std::string& name)const noexcept {
    const auto found=models.find(name);return found==models.end()?nullptr:&found->second;
}
}

namespace lattice::detail {
recovery_owner_schema&& recovery_owner_schema::prepare_native_fallback_()&& {
    if(!valid_&&!native_fallback_ready_) {
        native_fallback_=schema_registry::instance().all_schemas();native_fallback_ready_=true;
    }
    return std::move(*this);
}
const schema_snapshot_v2& recovery_owner_schema::native_fallback_lease_()const {
    if(!native_fallback_ready_)throw std::runtime_error("uncaptured ordinary schema fallback");
    return native_fallback_;
}
}
