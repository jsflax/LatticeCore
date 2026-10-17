#pragma once
#include "canonical_ready_named_profile.hpp"
#include "vendor/picosha2/picosha2.h"

namespace lattice::detail::predecessor_wire {
using json=nlohmann::json;
constexpr size_t profile_bytes=8192,body_bytes=8192;
inline void require(bool ok,const char* why){if(!ok)throw db_error(why);}
inline std::string hash(const char* domain,const std::string& raw) {
    return picosha2::hash256_hex_string(std::string(domain)+std::to_string(raw.size())+":"+raw);
}
inline bool lifecycle_name(const json& value) {
    return value.is_object()&&value.contains("name")&&value.at("name").is_string()&&
        (value.at("name")=="boundedV1OrphanV1"||value.at("name")=="bounded48MiBOrphanV1");
}
inline std::string canonical_profile(const json& value,bool registered) {
    require(value.is_object()&&value.contains("name")&&value.at("name").is_string(),"predecessor profile name missing");
    const auto name=value.at("name").get<std::string>();
    std::optional<int64_t> grace;
    if(lifecycle_name(value)) {
        require(value.contains("orphanResumeGraceMilliseconds")&&value.at("orphanResumeGraceMilliseconds").is_number_integer(),"predecessor grace integer required");
        const auto& n=value.at("orphanResumeGraceMilliseconds");
        require(!n.is_number_unsigned()||n.get<uint64_t>()<=3600000,"predecessor grace overflow");
        grace=n.get<int64_t>();require(*grace>0&&*grace<=3600000,"predecessor grace bound");
    }
    // Exact canonical bytes also distinguish real/boolean values from integers,
    // and reject unknown members. This constructs data, never source authority.
    const auto expected=canonical_ready_profile_description(canonical_named_ready_profile("",canonical_store_limits{},registered,name,grace),name).dump();
    const auto raw=value.dump();require(raw.size()<=profile_bytes&&raw==expected,"predecessor named profile bytes differ");return raw;
}
inline void pair(const json& before,const json& after,bool registered) {
    (void)canonical_profile(before,registered);(void)canonical_profile(after,registered);
    const auto name=before.at("name").get<std::string>();
    require((name=="boundedV1"&&after.at("name")=="boundedV1OrphanV1")||
        (name=="bounded48MiBV1"&&after.at("name")=="bounded48MiBOrphanV1"),"predecessor profile transition differs");
    auto a=before,b=after;a.erase("name");b.erase("name");b.erase("orphanResumeGraceMilliseconds");
    require(a.dump()==b.dump(),"predecessor numeric envelope differs");
}
inline std::string profile_digest(const json& value){return hash("canonical-ready-wire-profile-v1;",value.dump());}
inline std::string transition_digest(const std::string& raw){return hash("canonical-ready-predecessor-proof-v1;",raw);}
}
