#include "ordinary_managed_ipc_route.hpp"
#include "ordinary_managed_history.hpp"
#include "ordinary_managed_product_route.hpp"
#include <lattice/lattice.hpp>
#include <lattice/ordinary_sync.hpp>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <utility>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <unistd.h>
#endif

namespace lattice::detail {
namespace {
void require_route(bool condition,const char* message) {
    if(!condition)throw std::runtime_error(message);
}
bool identifier(std::string_view value) noexcept {
    if(value.empty()||value.size()>256)return false;
    for(std::size_t i=0;i<value.size();++i){const unsigned char c=value[i];
        if(!((c>='a'&&c<='z')||(c>='A'&&c<='Z')||c=='_'||(i&&c>='0'&&c<='9')))return false;
    }
    return true;
}
bool equal_identifier(std::string_view a,std::string_view b) noexcept {
    if(a.size()!=b.size())return false;
    const auto fold=[](unsigned char c){return c>='A'&&c<='Z'?c+('a'-'A'):c;};
    for(std::size_t i=0;i<a.size();++i)if(fold(a[i])!=fold(b[i]))return false;
    return true;
}
std::uint64_t current_process() noexcept {
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    return static_cast<std::uint64_t>(::getpid());
#else
    return 0;
#endif
}
}
std::size_t ordinary_managed_ipc_route_description::copy_charge() const {
    std::size_t charge=sizeof(*this);
    const auto add=[&](std::size_t amount){
        require_route(amount<=std::numeric_limits<std::size_t>::max()-charge,"managed route charge overflow");
        charge+=amount;
    };
    for(const auto* value:{&route_key,&generation,&source_member,&recipient_member,&channel}){
        require_route(!value->empty()&&value->size()<=4096&&value->find('\0')==std::string::npos,"managed route identity bound");
        add(value->size()+1);
    }
    require_route(source_member!=recipient_member,"managed route repeats its store member");
    require_route(policy_revision>0,"managed route policy revision missing");
    require_route(channel.size()>4&&channel.compare(0,4,"ipc:")==0,"managed route is not an IPC channel");
    require_route(!models.empty()&&models.size()<=ordinary_sync_limits::models,"managed route model bound");
    add(models.size()*sizeof(std::string));
    for(std::size_t i=0;i<models.size();++i){
        require_route(identifier(models[i]),"managed route model is not a generated identifier");
        for(std::size_t j=0;j<i;++j)require_route(!equal_identifier(models[i],models[j]),"managed route repeats a model");
        add(models[i].size()+1);
    }
    if(filter){
        require_route(filter->size()<=models.size(),"managed route filter count bound");
        add(sizeof(*filter)+filter->size()*sizeof(sync_filter_entry));
        for(std::size_t i=0;i<filter->size();++i){const auto& entry=(*filter)[i];
            require_route(identifier(entry.table_name),"managed route filter identifier");
            bool declared=false;for(const auto& model:models)if(model==entry.table_name)declared=true;
            require_route(declared,"managed route filter lies outside exact declared scope");
            for(std::size_t j=0;j<i;++j)require_route(!equal_identifier(entry.table_name,(*filter)[j].table_name),"managed route repeats a filter");
            add(entry.table_name.size()+1);
            if(entry.where_clause){
                require_route(!entry.where_clause->empty()&&entry.where_clause->size()<=64*1024&&entry.where_clause->find('\0')==std::string::npos,"managed route predicate bound");
                add(entry.where_clause->size()+1);
            }
        }
    }
    return charge;
}
struct ordinary_managed_ipc_local_route::implementation {
    struct retained {
        const std::uint64_t process=current_process();
        std::shared_ptr<lattice_db> source,recipient;
        std::shared_ptr<const ordinary_managed_profile> source_profile,recipient_profile;
        std::shared_ptr<const ordinary_managed_product_state> product;
    };
    struct dispose_retained {
        void operator()(retained* capsule)const noexcept {
            if(!capsule||capsule->process!=current_process())return;
            capsule->product.reset();if(capsule->process!=current_process())return;
            capsule->recipient_profile.reset();if(capsule->process!=current_process())return;
            capsule->source_profile.reset();if(capsule->process!=current_process())return;
            capsule->recipient.reset();if(capsule->process!=current_process())return;
            capsule->source.reset();if(capsule->process!=current_process())return;
            delete capsule;
        }
    };
    using custody=std::unique_ptr<retained,dispose_retained>;
    static custody retain(std::shared_ptr<lattice_db>&& source,
        std::shared_ptr<const ordinary_managed_profile>&& source_profile,
        std::shared_ptr<lattice_db>&& recipient,
        std::shared_ptr<const ordinary_managed_profile>&& recipient_profile,
        std::shared_ptr<const ordinary_managed_product_state>&& product) {
        custody result(new retained);
        // Moves to empty shared_ptr members are nonthrowing. Any later
        // description/validation failure uses this already installed guard.
        result->source=std::move(source);result->recipient=std::move(recipient);
        result->source_profile=std::move(source_profile);result->recipient_profile=std::move(recipient_profile);
        result->product=std::move(product);
        return result;
    }
    custody held;
    const std::uint64_t creator_process=current_process();
    const std::shared_ptr<lattice_db>& source;
    const std::shared_ptr<lattice_db>& recipient;
    const std::shared_ptr<const ordinary_managed_profile>& source_profile;
    const std::shared_ptr<const ordinary_managed_profile>& recipient_profile;
    const ordinary_managed_ipc_route_description description;
    const std::shared_ptr<const ordinary_managed_product_state>& product;
    implementation(std::shared_ptr<lattice_db>&& s,std::shared_ptr<const ordinary_managed_profile>&& sp,
        std::shared_ptr<lattice_db>&& r,std::shared_ptr<const ordinary_managed_profile>&& rp,
        ordinary_managed_ipc_route_description&& d,std::shared_ptr<const ordinary_managed_product_state>&& p)
        :held(retain(std::move(s),std::move(sp),std::move(r),std::move(rp),std::move(p))),
         source(held->source),recipient(held->recipient),source_profile(held->source_profile),recipient_profile(held->recipient_profile),
         description(std::move(d)),product(held->product){}
    ~implementation()=default;
};
ordinary_managed_ipc_local_route::ordinary_managed_ipc_local_route(
    std::shared_ptr<lattice_db>&& source,std::shared_ptr<const ordinary_managed_profile>&& source_profile,
    std::shared_ptr<lattice_db>&& recipient,std::shared_ptr<const ordinary_managed_profile>&& recipient_profile,
    ordinary_managed_ipc_route_description&& description,std::shared_ptr<const ordinary_managed_product_state>&& product)
    :impl_(std::make_unique<implementation>(std::move(source),std::move(source_profile),std::move(recipient),std::move(recipient_profile),std::move(description),std::move(product))) {
    (void)impl_->description.copy_charge();
    require_route(impl_->source&&impl_->recipient&&impl_->source.get()!=impl_->recipient.get()&&impl_->source_profile&&impl_->recipient_profile,"managed route retained owners missing");
    validate_lifetime();
}
ordinary_managed_ipc_local_route::~ordinary_managed_ipc_local_route() {
    // A fork inherits C++ mutex/owner state without ownership of the parent's
    // native settlement. Never enter a database/profile destructor there.
    // Quarantine the inherited allocation until that process exits instead.
    if(impl_&&impl_->creator_process!=current_process())(void)impl_.release();
}
void ordinary_managed_ipc_local_route::validate_lifetime() const {
    require_route(impl_&&impl_->creator_process!=0&&impl_->creator_process==current_process(),"managed route belongs to another process");
    require_managed_product_live(impl_->product);
    require_route(!impl_->source->is_closed()&&!impl_->recipient->is_closed(),"managed route actual owner closed");
}
const std::shared_ptr<const ordinary_managed_product_state>& ordinary_managed_ipc_local_route::product_authority()const {validate_lifetime();return impl_->product;}
const managed_engram_policy& ordinary_managed_ipc_local_route::product_policy()const {validate_lifetime();return managed_product_policy(impl_->product);}
lattice_db& ordinary_managed_ipc_local_route::source_owner() const {validate_lifetime();return *impl_->source;}
lattice_db& ordinary_managed_ipc_local_route::recipient_owner() const {validate_lifetime();return *impl_->recipient;}
const ordinary_managed_profile& ordinary_managed_ipc_local_route::source_profile() const {validate_lifetime();return *impl_->source_profile;}
const ordinary_managed_profile& ordinary_managed_ipc_local_route::recipient_profile() const {validate_lifetime();return *impl_->recipient_profile;}
const ordinary_managed_ipc_route_description& ordinary_managed_ipc_local_route::description() const {validate_lifetime();return impl_->description;}
}
