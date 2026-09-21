#pragma once
#include "schema.hpp"
#include <map>
#include <set>
#include <memory>

namespace lattice::detail {
// Engine-owned declaration snapshot, not a caller's recovery grant. An ordinary
// store with declarations outside these bounds may still open; only recovery
// admission refuses its unavailable catalog. The owner retains an immutable
// copy before protected bootstrap can inspect any durable programs.
class recovery_owner_schema {
    size_t bytes_=0, properties_=0;
    bool valid_=true;
    bool charge(size_t);
    bool field(const std::string&);
    bool property(const property_descriptor&);
public:
    static constexpr size_t max_models=256, max_properties=8192, max_bytes=1048576;
    std::map<std::string,model_schema> models;
    std::set<std::string> swift_models;
    std::string swift_digest, swift_fingerprint;

    static recovery_owner_schema capture_native();
    bool valid()const noexcept{return valid_;}
    void refuse()noexcept{valid_=false;}
    // Preflight before copying the model or its nested property descriptors.
    bool admit_model(const model_schema&);
    bool admit_property(const property_descriptor&);
    bool admit_field(const std::string&);
    bool admit_count(size_t count,size_t cap,size_t overhead);
    const model_schema* find(const std::string& name)const noexcept;
};
}
