#pragma once
#include "schema.hpp"
#include <map>
#include <set>
#include <memory>

namespace lattice::detail {
// Retained declaration inputs for the same unique-index/link-shadow generators
// used by ordinary Swift setup. A digest alone cannot reconstruct those rules.
struct recovery_constraint {
    std::vector<std::string> columns;
    bool allows_upsert = false;
};
// Engine-owned declaration snapshot, not a caller's recovery grant. An ordinary
// store with declarations outside these bounds may still open; only recovery
// admission refuses its unavailable catalog. The owner retains an immutable
// copy before protected bootstrap can inspect any durable programs.
class recovery_owner_schema {
    size_t bytes_=0, properties_=0;
    bool valid_=true;
    bool native_fallback_ready_=false;
    schema_snapshot_v2 native_fallback_; // captured before every actual Native open

    bool charge(size_t);
    bool field(const std::string&);
    bool property(const property_descriptor&);
public:
    static constexpr size_t max_models=256, max_properties=8192, max_bytes=1048576;
    std::map<std::string,model_schema> models;
    std::set<std::string> swift_models;
    std::vector<std::string> swift_order; // Actual producing SchemaVector order, not a selector grant.
    std::map<std::string,std::vector<recovery_constraint>> swift_constraints;
    std::string swift_digest, swift_fingerprint;
    std::string swift_snapshot; // Actual normal serializer retained on SAME owner catalog.

    static recovery_owner_schema capture_native();
    recovery_owner_schema&& prepare_native_fallback_()&&;
    const schema_snapshot_v2& native_fallback_lease_()const;
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
