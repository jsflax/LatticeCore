#include <lattice.hpp>
#include "../../LatticeCore/src/recovery_producer_continuity.hpp"
#include <algorithm>
#include <cstring>

namespace lattice {
namespace {
void diagnostic(std::array<char,769>& out,std::exception_ptr error)noexcept {
    out.fill(0);if(!error)return;
    const char* text="Unknown C++ continuity error";
    try{std::rethrow_exception(error);}catch(const std::exception& e){
        text=e.what();size_t size=0;while(size<768&&text[size])++size;
        std::memcpy(out.data(),text,size);return;
    }catch(...){}
    std::memcpy(out.data(),text,std::strlen(text));
}
void require(bool valid,const char* message){if(!valid)throw db_error(message);}
detail::recovery_continuous_policy native_policy(const continuous_policy& value) {
    // Bound all nested caller storage before the first copy. Native validation
    // remains authoritative for identity, limits, duplicate names and topology.
    require(!value.contributions.empty()&&value.contributions.size()<=16&&
        !value.routes.empty()&&value.routes.size()<=32,"continuous bridge inventory outside caps");
    for(const auto& c:value.contributions){
        for(const auto* s:{&c.channel,&c.authority,&c.source,&c.epoch,&c.scope,&c.schema,&c.profile_digest,&c.receipt_namespace})
            require(!s->empty()&&s->size()<=4096,"continuous bridge binding outside caps");
        require(!c.models.empty()&&c.models.size()<=16&&!c.incoming_grant_claim.empty()&&c.incoming_grant_claim.size()<=65536,"continuous bridge contribution outside caps");
        for(const auto& model:c.models)require(!model.empty()&&model.size()<=128,"continuous bridge model outside caps");
    }
    for(const auto& route:value.routes)require(!route.sync_id.empty()&&route.sync_id.size()<=4096&&route.endpoint.size()<=4096,"continuous bridge route outside caps");
    require(value.owners>0&&value.owners<=64&&value.physical_routes>0&&value.physical_routes<=128&&
        value.operations>0&&value.operations<=256&&value.frozen_entries>0&&value.frozen_entries<=100000&&
        value.frozen_bytes>0&&value.frozen_bytes<=67108864,"continuous bridge admission limits outside caps");
    detail::recovery_continuous_policy out;
    out.limits={{value.scopes,value.records,value.field_bytes,value.journal_bytes},
        {value.channels,value.binding_field_bytes,value.binding_bytes},
        {value.profiles,value.stamps,value.producer_field_bytes,value.manifest_bytes,value.producer_bytes}};
    out.owners=static_cast<size_t>(value.owners);out.physical_routes=static_cast<size_t>(value.physical_routes);
    out.operations=static_cast<size_t>(value.operations);out.frozen_entries=static_cast<size_t>(value.frozen_entries);out.frozen_bytes=static_cast<uint64_t>(value.frozen_bytes);
    for(const auto& c:value.contributions)out.contributions.push_back({{{c.channel,c.authority,c.source,c.epoch,c.scope,c.schema},c.profile_digest,c.receipt_namespace},c.models,c.incoming_grant_claim});
    for(const auto& route:value.routes)out.routes.push_back({route.sync_id,route.endpoint});
    return out;
}
}
void continuous_result::assign(const detail::recovery_install_result& value)noexcept {
    phase_=static_cast<int32_t>(value.state);unexpected_=value.unexpected_commit_observed;
    errors_=(value.primary_error?1:0)|(value.cleanup_error?2:0)|(value.postcommit_error?4:0)|(value.notification_error?8:0);
    diagnostic(primary_,value.primary_error);diagnostic(cleanup_,value.cleanup_error);
    diagnostic(postcommit_,value.postcommit_error);diagnostic(notification_,value.notification_error);
}
void continuous_result::failure(std::exception_ptr error)noexcept {
    const uint8_t bit=phase_==2?4:1;
    if(!(errors_&bit))diagnostic(phase_==2?postcommit_:primary_,error);
    if(error)errors_|=bit;
}
void continuous_result::assign(const detail::recovery_continuous_quiescence& value)noexcept {
    assign(value.settlement);waiting_=value.waiting;
    try {
        if(value.barrier)barrier_.value_=std::make_shared<detail::recovery_continuous_barrier>(*value.barrier);
        frozen_=value.unsent.has_value();
        if(value.unsent)unsent_count_=static_cast<int64_t>(value.unsent->canonical_originals().size());
        // The verified proof stays native/internal. Only the diagnostic count
        // crosses this boundary; caller labels cannot reconstruct that proof.
    }catch(...){failure(std::current_exception());}
}
bool continuous_result::has_error()const noexcept{return errors_!=0;}
std::string continuous_result::primary_error()const noexcept{return sealed([&]{return std::string(primary_.data());});}
std::string continuous_result::cleanup_error()const noexcept{return sealed([&]{return std::string(cleanup_.data());});}
std::string continuous_result::postcommit_error()const noexcept{return sealed([&]{return std::string(postcommit_.data());});}
std::string continuous_result::notification_error()const noexcept{return sealed([&]{return std::string(notification_.data());});}
continuous_result continuous_barrier::finish()const noexcept {
    continuous_result out;try{require(static_cast<bool>(value_),"continuous barrier unavailable");out.assign(detail::recovery_continuous_producer::finish(*value_));}catch(...){out.failure(std::current_exception());}return out;
}
continuous_result continuous_barrier::cancel()const noexcept {
    continuous_result out;try{require(static_cast<bool>(value_),"continuous barrier unavailable");out.assign(detail::recovery_continuous_producer::cancel(*value_));}catch(...){out.failure(std::current_exception());}return out;
}
continuous_result swift_lattice_ref::begin_continuous(int64_t attempt)const noexcept {
    continuous_result out;try{require(static_cast<bool>(impl_),"continuous owner unavailable");out.assign(detail::recovery_continuous_producer::begin(impl_,attempt));}catch(...){out.failure(std::current_exception());}return out;
}
continuous_result swift_lattice_ref::inspect_continuous()const noexcept {
    continuous_result out;try{require(static_cast<bool>(impl_),"continuous owner unavailable");out.assign(detail::recovery_continuous_producer::inspect(impl_));}catch(...){out.failure(std::current_exception());}return out;
}
#if LATTICE_HAS_FRT
swift_lattice_ref* swift_lattice_ref::create_continuous(const swift_configuration& config,const SchemaVector& schemas,const continuous_policy& policy,continuous_result& result) {
    std::unique_ptr<swift_lattice_ref> ref;
#else
swift_lattice_ref swift_lattice_ref::create_continuous(const swift_configuration& config,const SchemaVector& schemas,const continuous_policy& policy,continuous_result& result) {
    swift_lattice_ref value;auto* ref=&value;
#endif
    result=continuous_result{};
    try {
#if LATTICE_HAS_FRT
        ref.reset(new swift_lattice_ref()); // Allocate before any durable effects.
#endif
        require(!config.row_migration_fn_&&!config.get_schema_pair_fn_&&config.migration_schemas_.empty(),"continuous Swift migration is not an admitted adoption path");
        require(swift_lattice::recovery_catalog(config,schemas).valid(),"continuous Swift catalog exceeds immutable bounds");
        auto native=native_policy(policy);
        auto declarations=std::make_shared<const SchemaVector>(schemas);
        auto recipe=std::make_shared<detail::recovery_continuous_producer::owner_recipe>();
        recipe->construct=[declarations](const configuration& base,const std::shared_ptr<detail::recovery_continuous_admission>& admission){
            // Same full declarations on the app facade and dedicated WSS facade.
            return std::shared_ptr<lattice_db>(new swift_lattice(swift_configuration(base),*declarations,admission));
        };
        recipe->publish_pointer=[](const std::shared_ptr<lattice_db>& owner){
            detail::LatticeCache::instance().register_pointer(std::static_pointer_cast<swift_lattice>(owner));
        };
        auto opened=detail::recovery_continuous_producer::open_owned(config,native,std::move(recipe));
        result.assign(opened.settlement);
        if(opened.owner&&!result.has_error())ref->impl_=std::static_pointer_cast<swift_lattice>(std::move(opened.owner));
    }catch(...){result.failure(std::current_exception());}
#if LATTICE_HAS_FRT
    return ref.release();
#else
    return value;
#endif
}
} // namespace lattice
