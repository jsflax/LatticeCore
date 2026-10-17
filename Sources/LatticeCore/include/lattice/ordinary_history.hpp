#pragma once
#include "ordinary_sync.hpp"
#include <span>
#include <optional>

namespace lattice {
namespace detail {struct ordinary_history_export_state;struct ordinary_history_page_state;struct ordinary_mounted_history_source_state;class ordinary_export_access;}
class ordinary_mounted_history_source_ref;

// Explicit local resource policy for negotiated paged-history-v1. Remote
// declarations never enlarge these limits. The durable byte cap is shared by
// every export and receive stage in the same physical store, including stages
// recovered after restart. No unresolved user history is evicted for capacity.
struct ordinary_history_limits final {
    uint64_t durable_stage_bytes=uint64_t{2}*1024*1024*1024;
    uint64_t records=uint64_t{4}*1024*1024;
    uint64_t pages=65536;
    uint32_t records_per_page=64;
    uint32_t page_bytes=4*1024*1024;
    // Disjoint simultaneously live logical reservations. The row allowance
    // includes its parser workspace; the stage SQL allowance includes copied
    // bind/postimage values. Allocator/SQLite/WAL overhead is not an RSS bound.
    static constexpr size_t metadata_bytes=32*1024*1024;
    static constexpr size_t row_scratch_bytes=56*1024*1024;
    static constexpr size_t stage_scratch_bytes=24*1024*1024;
    static constexpr size_t page_buffer_bytes=4*1024*1024;
    static constexpr size_t record_buffer_bytes=4*1024*1024;
    static constexpr size_t owner_bytes=8*1024*1024;
    static constexpr size_t scratch_bytes=metadata_bytes+row_scratch_bytes+stage_scratch_bytes+page_buffer_bytes+record_buffer_bytes+owner_bytes;
    static_assert(scratch_bytes==ordinary_sync_limits::logical_export_reservation);
    static constexpr size_t simultaneous_exports=4,simultaneous_receives=8;
};

// A native owning page lease, not a mutable DTO. Each export admits only one
// distinct page buffer at a time; copies retain that same immutable buffer.
// Consumers must retain this lease through their actual send/copy completion.
class ordinary_history_page final {
    std::shared_ptr<const detail::ordinary_history_page_state> state_;
    explicit ordinary_history_page(std::shared_ptr<const detail::ordinary_history_page_state>);
    friend class ordinary_history_export;
public:
    ordinary_history_page(const ordinary_history_page&)=default;
    ordinary_history_page(ordinary_history_page&&) noexcept=default;
    ordinary_history_page& operator=(const ordinary_history_page&)=default;
    ordinary_history_page& operator=(ordinary_history_page&&) noexcept=default;
    std::span<const uint8_t> bytes()const;
};

// Passive coordinates of a genuinely existing full-mounted source observed in
// the SAME native WRITE as this selected cut. They are not an old receipt, a
// source grant, a rebind permission, or a synthetic full-mounted accepted cut.
struct ordinary_history_prior_source_observation final {
    std::string scope,source,schema_digest;
    int64_t epoch=0,issued_frontier=0;
};

// Source-issued, durably staged complete cut. It owns an actual same-image
// native child and stage lease; there is no raw-pointer alias or URL/JSON
// constructor. Page zero is its sealed header. The source snapshot is complete
// before return, and no source SQL transaction remains open during transport.
class ordinary_history_export final {
    std::shared_ptr<detail::ordinary_history_export_state> state_;
    explicit ordinary_history_export(std::shared_ptr<detail::ordinary_history_export_state>);
    friend class detail::ordinary_export_access;
public:
    ordinary_history_export(const ordinary_history_export&)=default;
    ordinary_history_export(ordinary_history_export&&) noexcept=default;
    ordinary_history_export& operator=(const ordinary_history_export&)=default;
    ordinary_history_export& operator=(ordinary_history_export&&) noexcept=default;
    const std::string& stage_id()const;
    const std::string& source_id()const;
    int64_t epoch()const;
    int64_t cut()const;
    uint64_t frame_count()const;
    uint64_t durable_bytes()const;
    ordinary_history_page page(uint64_t index)const;
    std::optional<ordinary_history_prior_source_observation> prior_mounted_source()const;
    // Passive facts from this fresh mounted capture only; no caller input or
    // historical receipt/continuity/ACK/result/debt authority. Resume has none.
    const std::string& mounted_source_relation_observation()const;

};

// Trusted server mount issuer. Only the owning Swift bridge can create it,
// using its actual shared native owner and that owner's exact Swift catalog.
// A wire selector, URL, pointer alias or copied source coordinate cannot issue
// this capability. It retains ONE common native export slot through its cut
// and every page alias; only one cut may be outstanding on a registration.
class ordinary_mounted_history_source final {
    std::shared_ptr<detail::ordinary_mounted_history_source_state> state_;
    explicit ordinary_mounted_history_source(std::shared_ptr<detail::ordinary_mounted_history_source_state>);
    static ordinary_mounted_history_source issue(std::shared_ptr<lattice_db>,
        const std::vector<std::string>& mounted,const ordinary_history_limits& = {});
    friend class ordinary_mounted_history_source_ref;
    friend class detail::ordinary_export_access;
public:
    ordinary_mounted_history_source(const ordinary_mounted_history_source&)=default;
    ordinary_mounted_history_source(ordinary_mounted_history_source&&)noexcept=default;
    ordinary_mounted_history_source& operator=(const ordinary_mounted_history_source&)=default;
    ordinary_mounted_history_source& operator=(ordinary_mounted_history_source&&)noexcept=default;
    ordinary_history_export capture(const std::vector<std::string>& selected)const;
    // Explicit observation opt-in, still issued only by this actual mounted owner.
    ordinary_history_export capture_with_lineage_observation(const std::vector<std::string>& selected)const;
};

// Trusted local issued endpoints. Existing historical origin promotion still
// requires actual managed cohort/source authority; staged bytes are never it.
ordinary_history_export capture_ordinary_history_export(lattice_db&,
    const std::vector<std::string>& models,const ordinary_history_limits& = {});
// Resumes only an already-sealed stage belonging to this actual physical
// source/epoch/schema. The caller-supplied stage ID is selection, not authority.
ordinary_history_export resume_ordinary_history_export(lattice_db&,const std::string& stage,
    const ordinary_history_limits& = {});
}
