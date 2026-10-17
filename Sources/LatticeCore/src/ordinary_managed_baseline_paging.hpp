#pragma once
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>

namespace lattice::detail {
class ordinary_export_access;
// Passive durable custody only. Neither this result nor a complete staged cut
// chooses a visible value, replaces a partition, retracts a contribution, or
// acknowledges an original. Currentness is separately revalidated by the real
// source owner; stored rows and scalar seals cannot reissue that capability.
struct ordinary_managed_baseline_staging_result final {
    std::string staged_cut_seal;
    bool other_lineage_observed=false;
};
class ordinary_managed_baseline_cursor final {
    friend class ordinary_export_access;
    struct implementation;
    std::unique_ptr<implementation> impl_;
    explicit ordinary_managed_baseline_cursor(std::unique_ptr<implementation>);
    const implementation& checked()const;
public:
    ~ordinary_managed_baseline_cursor();
    ordinary_managed_baseline_cursor(ordinary_managed_baseline_cursor&&)noexcept;
    ordinary_managed_baseline_cursor& operator=(ordinary_managed_baseline_cursor&&)noexcept;
    ordinary_managed_baseline_cursor(const ordinary_managed_baseline_cursor&)=delete;
    std::size_t total_bytes()const;
    std::size_t produced_bytes()const;
    const std::string& baseline_digest()const;
};
class ordinary_managed_baseline_page final {
    friend class ordinary_export_access;
    struct implementation;
    std::unique_ptr<implementation> impl_;
    explicit ordinary_managed_baseline_page(std::unique_ptr<implementation>);
    const implementation& checked()const;
public:
    static constexpr std::size_t maximum_bytes=256*1024;
    ~ordinary_managed_baseline_page();
    ordinary_managed_baseline_page(ordinary_managed_baseline_page&&)noexcept;
    ordinary_managed_baseline_page& operator=(ordinary_managed_baseline_page&&)noexcept;
    ordinary_managed_baseline_page(const ordinary_managed_baseline_page&)=delete;
    std::size_t offset()const;
    std::span<const std::uint8_t> bytes()const;
};
class ordinary_managed_baseline_stage final {
    friend class ordinary_export_access;
    struct implementation;
    std::unique_ptr<implementation> impl_;
    explicit ordinary_managed_baseline_stage(std::unique_ptr<implementation>);
    const implementation& checked()const;
public:
    ~ordinary_managed_baseline_stage();
    ordinary_managed_baseline_stage(ordinary_managed_baseline_stage&&)noexcept;
    ordinary_managed_baseline_stage& operator=(ordinary_managed_baseline_stage&&)noexcept;
    ordinary_managed_baseline_stage(const ordinary_managed_baseline_stage&)=delete;
    const std::string& identity()const;
};
}
