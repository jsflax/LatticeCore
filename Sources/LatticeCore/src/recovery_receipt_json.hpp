#pragma once
#include "recovery_receipt_coverage.hpp"
#include <nlohmann/json.hpp>

namespace lattice::detail::receipt_json {
using json=nlohmann::json;
inline void shape(const json& j,std::initializer_list<const char*> fields){if(!j.is_object()||j.size()!=fields.size())throw db_error("receipt coverage object shape");for(const auto* f:fields)if(!j.contains(f))throw db_error("receipt coverage object member");}
inline std::string text(const json& j,const char* name,size_t cap){const auto& v=j.at(name);if(!v.is_string())throw db_error("receipt coverage string expected");const auto& s=v.get_ref<const std::string&>();if(s.empty()||s.size()>cap||s.find('\0')!=std::string::npos)throw db_error("receipt coverage string bound");return s;}
inline int64_t number(const json& j,const char* name){const auto& v=j.at(name);if(!v.is_number_integer()||v.is_number_unsigned()&&v.get<uint64_t>()>INT64_MAX)throw db_error("receipt coverage integer expected");const auto n=v.get<int64_t>();if(n<=0)throw db_error("receipt coverage positive revision required");return n;}
inline json encode(const recovery_producer_registration& p){p.validate();return {{"registrationID",p.registration_id},{"incarnation",p.incarnation}};}
inline recovery_producer_registration producer(const json& j){shape(j,{"registrationID","incarnation"});recovery_producer_registration p{text(j,"registrationID",256),text(j,"incarnation",36)};p.validate();return p;}
inline json encode(const recovery_receipt_binding& b){b.validate();return {{"producer",encode(b.producer)},{"cohortID",b.cohort_id},{"cohortRevision",b.cohort_revision},{"operationCodec",b.operation_codec}};}
inline recovery_receipt_binding binding(const json& j){shape(j,{"producer","cohortID","cohortRevision","operationCodec"});recovery_receipt_binding b{producer(j.at("producer")),text(j,"cohortID",36),number(j,"cohortRevision"),number(j,"operationCodec")};b.validate();return b;}
inline json encode(const canonical_coverage_profile& p){p.validate();return {{"kind","registeredProducerV3"},{"cohortID",p.cohort_id},{"cohortRevision",p.revision},{"operationCodec",1},{"namespaces",p.namespaces}};}
inline canonical_coverage_profile profile(const json& j){
    shape(j,{"kind","cohortID","cohortRevision","operationCodec","namespaces"});
    if(j.at("kind")!="registeredProducerV3"||number(j,"operationCodec")!=1)throw db_error("unsupported receipt coverage policy");
    const auto& members=j.at("namespaces");if(!members.is_array()||members.empty()||members.size()>64)throw db_error("receipt coverage member bound");
    canonical_coverage_profile p;p.cohort_id=text(j,"cohortID",36);p.revision=number(j,"cohortRevision");
    for(const auto& v:members){if(!v.is_string())throw db_error("receipt cohort member string expected");const auto& s=v.get_ref<const std::string&>();if(s.empty()||s.size()>256||s.find('\0')!=std::string::npos)throw db_error("receipt cohort member bound");if(!p.namespaces.empty()&&p.namespaces.back()>=s)throw db_error("receipt cohort members must be sorted and unique");p.namespaces.push_back(s);}
    p.validate();return p;
}
inline recovery_receipt_binding authorization(const json& j,const canonical_coverage_profile& p){
    shape(j,{"kind","registrationID","incarnation","cohortID","cohortRevision"});if(j.at("kind")!="registeredProducer")throw db_error("registered producer authorization required");
    recovery_receipt_binding b{{text(j,"registrationID",256),text(j,"incarnation",36)},text(j,"cohortID",36),number(j,"cohortRevision"),1};b.validate();
    if(b.cohort_id!=p.cohort_id||b.cohort_revision!=p.revision)throw db_error("registered producer authorization belongs to another cohort");return b;
}
}
