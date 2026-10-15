#pragma once
#include <lattice/db.hpp>
#include <lattice/ordinary_context.hpp>
#include "ordinary_managed_history.hpp"
#include <map>
#include <memory>

namespace lattice { struct instance_guard; }
namespace lattice::detail {
class ordinary_cohort_attachment_access;
int ordinary_composed_attachment_authorize(sqlite3*,int,const char*,const char*) noexcept;
int restore_ordinary_attachment_guard(sqlite3*) noexcept;
// Native lifetime and topology permission only. No wire/source/profile issuer.
class ordinary_cohort_attachment_binding final {
    friend class ordinary_cohort_attachment_access;
    ordinary_context recipient_, target_;
    std::shared_ptr<const ordinary_managed_profile> recipient_profile_, target_profile_;
    std::shared_ptr<instance_guard> recipient_guard_, target_guard_;
    std::shared_ptr<const physical_store_identity> recipient_identity_, target_identity_;
    std::string path_, alias_;
    std::int64_t token_=0, creator_=0;
    ordinary_cohort_attachment_binding()=default;
    void validate(std::chrono::steady_clock::time_point)const;
public:
    ordinary_cohort_attachment_binding(const ordinary_cohort_attachment_binding&)=delete;
    ordinary_cohort_attachment_binding& operator=(const ordinary_cohort_attachment_binding&)=delete;
};
using ordinary_cohort_attachment = std::shared_ptr<const ordinary_cohort_attachment_binding>;

class ordinary_cohort_attachment_access final {
    friend class ::lattice::lattice_db;
    // Only native topology paths create a binding after retaining actual
    // parent/target handles and checking their immutable enrolled profiles.
    static ordinary_cohort_attachment bind(lattice_db&,lattice_db&,const std::string& alias,
        std::int64_t token,const std::shared_ptr<const physical_store_identity>& target,
        const std::shared_ptr<const physical_store_identity>& recipient,database::attachment_operation&);
public:
    static void require_creator(const ordinary_context& context){ordinary_require_creator(context);}
    // Only called with a privately issued binding. Every path constructs a
    // literal synchronous statement itself; no parameterized filename permit.
    static std::shared_ptr<const physical_store_identity> attach(database&,const ordinary_cohort_attachment&,
        database::attachment_operation&,bool read_only=false);
    static void detach(database&,const ordinary_cohort_attachment&,std::int64_t expected_token,
        database::attachment_operation&);
    static void validate(const ordinary_cohort_attachment&,std::chrono::steady_clock::time_point);
    static void validate_target(const ordinary_cohort_attachment&,
        const std::shared_ptr<const physical_store_identity>&,std::chrono::steady_clock::time_point);
    static std::int64_t token(const ordinary_cohort_attachment& value){if(!value)throw db_error("managed attachment binding missing");return value->token_;}
    // SQLite authorizer sees filename for ATTACH and alias for DETACH. Its
    // second argument is NULL; it does not prove the ATTACH alias. The latter
    // is bound by our exact constructed statement and nonescaping execution.
    static int authorize(sqlite3*,int,const char*,const char*) noexcept;
};
}
