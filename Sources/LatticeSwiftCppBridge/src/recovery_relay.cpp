#include <lattice.hpp>
#include <cstring>
#include "../../LatticeCore/src/vendor/picosha2/picosha2.h"
#include "../../LatticeCore/src/recovery_authenticated_session.hpp"
namespace lattice {
namespace {
void relay_failure()noexcept {
    try{throw;}catch(const std::exception& e){record_bridge_error(e.what());}
    catch(...){record_bridge_error("Unknown authenticated relay bridge exception");}
}
}
relay_recovery_setup swift_lattice_ref::open_relay_recovery_setup(const std::string& policy,const std::string& connection,
    void* context,int32_t(*current)(void*),void(*destroy)(void*))const noexcept {
    last_bridge_error().clear();
    try{relay_recovery_setup result;
        result.value_=detail::authenticated_relay_setup::open(impl_,policy,connection,context,current,destroy);return result;
    }catch(...){relay_failure();return {};}
}
bool relay_recovery_setup::valid()const noexcept{return bool(value_);}
int32_t swift_lattice_ref::migrate_relay_receipt_coverage_file(const std::string& path,const SchemaVector& schemas,
    int64_t schema_version,int32_t busy_timeout_ms,const std::string& prior,const std::string& next)noexcept {
    last_bridge_error().clear();
    try {
        if(schema_version<1||schema_version>INT32_MAX)throw db_error("receipt administration declared schema version outside bounds");
        swift_configuration declared;declared.target_schema_version=static_cast<int>(schema_version);
        return detail::authenticated_relay_setup::migrate_receipt_coverage_file(path,swift_lattice::recovery_catalog(declared,schemas),
            schema_version,busy_timeout_ms,prior,next)?1:2;
    }catch(...){relay_failure();return 4;}
}
namespace {
void adoption_diagnostic(std::array<char,769>& out,std::exception_ptr error)noexcept {
    out.fill(0);if(!error)return;
    const char* text="Unknown lifecycle administration error";
    try{std::rethrow_exception(error);}catch(const std::exception& e){
        text=e.what();size_t n=0;while(n<768&&text[n])++n;std::memcpy(out.data(),text,n);return;
    }catch(...){}
    std::memcpy(out.data(),text,std::strlen(text));
}
}
void relay_lifecycle_adoption_result::assign(detail::authenticated_lifecycle_adoption_result&& value)noexcept {
    // Save known transaction truth before diagnostics or record conversion.
    const auto& settlement=value.adoption.settlement;
    phase_=static_cast<int32_t>(settlement.state);unexpected_=settlement.unexpected_commit_observed;
    pending_=value.pending_quiescence;
    errors_=(settlement.primary_error?1:0)|(settlement.cleanup_error?2:0)|(settlement.postcommit_error?4:0)|(settlement.notification_error?8:0);
    adoption_diagnostic(primary_,settlement.primary_error);adoption_diagnostic(cleanup_,settlement.cleanup_error);
    adoption_diagnostic(postcommit_,settlement.postcommit_error);adoption_diagnostic(notification_,settlement.notification_error);
    if(phase_==2&&value.adoption.record&&value.adoption.disposition)try {
        const auto& record=*value.adoption.record;
        constexpr std::string_view prefix="canonical-ready-adoption-v1;36:";
        if(record.size()>16384||record.size()<prefix.size()+36||!record.starts_with(prefix))
            throw db_error("audited lifecycle record shape unavailable");
        const auto digest=picosha2::hash256_hex_string(record);
        std::memcpy(transition_id_.data(),record.data()+prefix.size(),36);
        std::memcpy(record_digest_.data(),digest.data(),64);
        record_=std::move(*value.adoption.record);
        disposition_=*value.adoption.disposition==detail::canonical_ready_adoption_disposition::applied?1:2;
    }catch(...){failure(std::current_exception());}
}
void relay_lifecycle_adoption_result::failure(std::exception_ptr error)noexcept {
    const uint8_t bit=phase_==2?4:1;
    if(!(errors_&bit))adoption_diagnostic(phase_==2?postcommit_:primary_,error);
    if(error)errors_|=bit;record_.clear();disposition_=0;transition_id_.fill(0);record_digest_.fill(0);
}
std::string relay_lifecycle_adoption_result::primary_error()const noexcept{return sealed([&]{return std::string(primary_.data());});}
std::string relay_lifecycle_adoption_result::cleanup_error()const noexcept{return sealed([&]{return std::string(cleanup_.data());});}
std::string relay_lifecycle_adoption_result::postcommit_error()const noexcept{return sealed([&]{return std::string(postcommit_.data());});}
std::string relay_lifecycle_adoption_result::notification_error()const noexcept{return sealed([&]{return std::string(notification_.data());});}
std::string relay_lifecycle_adoption_result::transition_id()const noexcept{return sealed([&]{return std::string(transition_id_.data());});}
std::string relay_lifecycle_adoption_result::record_digest()const noexcept{return sealed([&]{return std::string(record_digest_.data());});}
std::string relay_lifecycle_adoption_result::take_record()noexcept{std::string out;out.swap(record_);return out;}
relay_lifecycle_adoption_result swift_lattice_ref::adopt_relay_lifecycle_file(const std::string& path,const SchemaVector& schemas,
    int64_t schema_version,int32_t busy_timeout_ms,const std::string& prior,const std::string& next)noexcept {
    relay_lifecycle_adoption_result out;
    try {
        if(schema_version<1||schema_version>INT32_MAX)throw db_error("lifecycle administration declared schema version outside bounds");
        swift_configuration declared;declared.target_schema_version=static_cast<int>(schema_version);
        out.assign(detail::authenticated_relay_setup::adopt_lifecycle_file(path,swift_lattice::recovery_catalog(declared,schemas),
            schema_version,busy_timeout_ms,prior,next));
    }catch(...){out.failure(std::current_exception());}
    return out;
}
int32_t swift_lattice_ref::migrate_relay_receipt_coverage(const std::string& prior,const std::string& next)const noexcept {
    last_bridge_error().clear();
    try{return detail::authenticated_relay_setup::migrate_receipt_coverage(impl_,prior,next)?1:2;}
    catch(...){relay_failure();return 4;}
}
std::string relay_recovery_setup::descriptor()const noexcept {
    last_bridge_error().clear();try{return value_?value_->descriptor():std::string{};}catch(...){relay_failure();return {};}
}
relay_recovery_stop relay_recovery_setup::stop_token()const noexcept {relay_recovery_stop r;if(value_)r.value_=value_->stop_token();return r;}
bool relay_recovery_setup::finish_authorization(const std::string& outcome)const noexcept {
    last_bridge_error().clear();try{return value_&&value_->finish_authorization(outcome);}catch(...){relay_failure();return false;}
}
relay_recovery_result relay_recovery_setup::receive(const std::string& frame)const noexcept {
    last_bridge_error().clear();relay_recovery_result result;
    try{if(!value_)return result;auto actual=value_->receive(frame);result.status_=actual.status;
        result.ids_=std::move(actual.applied);result.operation_=std::move(actual.operation);return result;
    }catch(...){relay_failure();result.status_=4;return result;}
}
void relay_recovery_setup::close_on_io()const noexcept{if(value_)value_->close();}
void relay_recovery_stop::stop()const noexcept{if(value_)value_->stop();}
bool relay_recovery_stop::live()const noexcept{return value_&&value_->live();}
bool relay_recovery_stop::drained()const noexcept{return !value_||value_->drained();}
relay_ready_charge relay_recovery_stop::reserve_ready(uint64_t bytes)const noexcept {
    last_bridge_error().clear();relay_ready_charge result;
    try{if(value_)result.value_=value_->reserve_ready(bytes);}catch(...){relay_failure();}return result;
}
int32_t relay_recovery_result::status_code()const noexcept{return status_;}
const std::vector<std::string>& relay_recovery_result::ids()const noexcept{return ids_;}
std::vector<std::string> relay_recovery_result::take_ids()noexcept {
    std::vector<std::string> result;result.swap(ids_);return result;
}
bool relay_recovery_result::publishable()const noexcept{return operation_&&operation_->publishable();}
relay_ready_result relay_recovery_setup::ready(const std::string& wire,const relay_ready_charge& charge)const noexcept {
    last_bridge_error().clear();relay_ready_result result;
    try{if(!value_)return result;auto actual=value_->ready(wire,charge.value_);result.status_=actual.status;
        result.wire_=std::move(actual.wire);result.request_id_=std::move(actual.request_id);result.operation_=std::move(actual.operation);return result;
    }catch(...){relay_failure();result.status_=4;return result;}
}
int32_t relay_ready_result::status_code()const noexcept{return status_;}
const std::string& relay_ready_result::wire()const noexcept{return wire_;}
const std::string& relay_ready_result::request_id()const noexcept{return request_id_;}
std::string relay_ready_result::take_wire()noexcept{std::string owned;owned.swap(wire_);return owned;}
std::string relay_ready_result::take_request_id()noexcept{std::string owned;owned.swap(request_id_);return owned;}
bool relay_ready_result::publishable()const noexcept{return operation_&&operation_->publishable();}
}
