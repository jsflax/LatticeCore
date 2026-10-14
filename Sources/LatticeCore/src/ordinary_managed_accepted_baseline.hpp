#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace lattice::detail {
class ordinary_export_access;
struct ordinary_export_table;
// Exact last accepted server state, not the current application's rows or a
// replay of an older original. Only the private actual view reader constructs
// this observation. It does not confer partition completeness, a visible-value
// decision, model mutation, ACK, privacy removal or source-install authority.
class ordinary_managed_accepted_baseline final {
    friend class ordinary_export_access;
    struct implementation;
    std::unique_ptr<implementation> impl_;
    explicit ordinary_managed_accepted_baseline(std::unique_ptr<implementation>);
    const implementation& checked()const;
public:
    ~ordinary_managed_accepted_baseline();
    ordinary_managed_accepted_baseline(ordinary_managed_accepted_baseline&&)noexcept;
    ordinary_managed_accepted_baseline& operator=(ordinary_managed_accepted_baseline&&)noexcept;
    ordinary_managed_accepted_baseline(const ordinary_managed_accepted_baseline&)=delete;
    const std::string& source_id()const;
    const std::string& scope()const;
    const std::string& schema()const;
    const std::string& cut_digest()const;
    const std::string& baseline_digest()const;
    std::int64_t epoch()const;
    std::int64_t cut()const;
    // Full accepted source scope. Route eligibility is a separate operation;
    // a filter miss or omitted route model never authorizes a row deletion.
    const std::vector<ordinary_export_table>& tables()const;
};
}
