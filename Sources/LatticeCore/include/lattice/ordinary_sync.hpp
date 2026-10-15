#pragma once
#include "owned_server_export.hpp"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <functional>
#include <string>
#include <vector>

namespace lattice {
namespace detail {struct ordinary_wire_export_state;class ordinary_wire_access;class ordinary_export_access;}

// Configuration policy, not source or transport authority. Existing callers
// keep provisional semantics until they deliberately require negotiated v2.

// Local bounds are never enlarged by a remote declaration. The retained wire
// budget keeps the existing 40MiB/8000-row workload feasible. These are logical
// byte/count limits; allocator and platform overhead are not an RSS promise.
struct ordinary_sync_limits final {
    static constexpr size_t frame_bytes=4*1024*1024;
    static constexpr size_t retained_wire_bytes=40*1024*1024;
    static constexpr size_t frames=770, records_per_page=64;
    static constexpr size_t schema_bytes=1024*1024, models=256, properties=8192;
    static constexpr size_t projection_tables=8448, columns_per_table=2050, projection_columns=65536;
    static constexpr size_t rows=8192, originals=8192;
    static constexpr size_t scalar_bytes=1024*1024, parser_nodes=262144, parser_depth=32;
    static constexpr size_t simultaneous_exports=4;
    static constexpr size_t logical_export_reservation=128*1024*1024;
    static constexpr size_t simultaneous_receives=8;
    static constexpr size_t logical_receive_reservation=128*1024*1024;
    static constexpr size_t decoded_receive_bytes=96*1024*1024;
    static constexpr size_t parser_workspace_bytes=32*1024*1024;
    static constexpr size_t decoded_cells=2097152;
    // Conservative logical workspace units for the SAX/DOM and one original
    // round trip; these do not describe the allocator's actual RSS.
    static constexpr size_t parser_node_logical_bytes=384, parser_wire_copies=4;
    static_assert(decoded_receive_bytes+parser_workspace_bytes==logical_receive_reservation);
};

// Only the actual four-slot admission failure has this type. Content, schema,
// physical-owner, SQL and authority failures are never classified as capacity.
class ordinary_export_capacity_exhausted final : public db_error {
public:
    ordinary_export_capacity_exhausted():db_error("ordinary v2: export capacity exhausted"){}
};
namespace detail { struct ordinary_export_capacity_observation; }
// Payload-free availability hint, never a reservation or source capability.
// A copied callback may finish after cancel; consumers must recheck lifetime.
class ordinary_export_capacity_observer final {
    std::shared_ptr<detail::ordinary_export_capacity_observation> state_;
    explicit ordinary_export_capacity_observer(std::shared_ptr<detail::ordinary_export_capacity_observation>);
    friend ordinary_export_capacity_observer observe_ordinary_export_capacity(std::function<void()>);
public:
    ordinary_export_capacity_observer()=default;
    ~ordinary_export_capacity_observer();
    ordinary_export_capacity_observer(const ordinary_export_capacity_observer&);
    ordinary_export_capacity_observer(ordinary_export_capacity_observer&&)noexcept;
    ordinary_export_capacity_observer& operator=(const ordinary_export_capacity_observer&);
    ordinary_export_capacity_observer& operator=(ordinary_export_capacity_observer&&)noexcept;
    bool valid()const noexcept;
    void cancel()noexcept;
};
ordinary_export_capacity_observer observe_ordinary_export_capacity(std::function<void()>);

// An engine-issued immutable wire export. Only the actual capture endpoint can
// create it. Copying a frame into JSON/bytes does not create a receiving grant.
// The object retains its complete cut and frame reservation after source close.
class ordinary_sync_export final {
    std::shared_ptr<const detail::ordinary_wire_export_state> state_;
    explicit ordinary_sync_export(std::shared_ptr<const detail::ordinary_wire_export_state>);
    friend class detail::ordinary_wire_access;
public:
    ordinary_sync_export(const ordinary_sync_export&)=default;
    ordinary_sync_export(ordinary_sync_export&&) noexcept=default;
    ordinary_sync_export& operator=(const ordinary_sync_export&)=default;
    ordinary_sync_export& operator=(ordinary_sync_export&&) noexcept=default;
    size_t frame_count()const;
    size_t retained_wire_bytes()const;
    const std::string& frame(size_t index)const;
    const std::string& source_id()const;
    int64_t epoch()const;
    int64_t cut()const;
};

// Trusted local application endpoint: named models are requested scope, not a
// metadata-derived grant. Actual owner/catalog/source validation still occurs
// inside the owned capture transaction. No caller transaction is borrowed.
ordinary_sync_export capture_ordinary_sync_export(lattice_db&,
    const std::vector<std::string>& models);
// C-bridge variant checks counts/lengths and reserves native export capacity
// before copying the borrowed model strings. Pointers last only this call.
ordinary_sync_export capture_ordinary_sync_export(lattice_db&,
    const char* const* models,size_t model_count);
// Local delivery consumes the actual issued handle; there is deliberately no
// byte/JSON constructor. Remote delivery requires the separate authenticated
// physical-session path and cannot invoke this overload using decoded fields.
std::vector<std::string> apply_ordinary_sync_export(lattice_db&,
    const ordinary_sync_export&);
// Validated logical projection closure for payload-free observation. This
// reads schema only; returned names carry no export or source authority.
std::vector<std::string> ordinary_sync_observation_tables(lattice_db&,
    const std::vector<std::string>& models);
// Explicit authoritative-source intake. The trusted local caller chooses
// required ordinary-v2 policy; remote bytes supply only provisional edits and
// passive original membership. Returns actual-applied IDs, never cut receipts.
// Caller keeps this exact owner alive through this synchronous call.
std::vector<std::string> receive_ordinary_source_upload(lattice_db&,
    const uint8_t* bytes,size_t count,const std::vector<std::string>& models);
// Actual source intake result. The acknowledgment correlates the exact handled
// upload version with committed immutable-original custody only. It does not
// assert that a deduplicated NoHistory projection was replayed or accepted.
class ordinary_upload_acknowledgment_result final {
    friend class detail::ordinary_export_access;
    struct payload;
    // Value copies, including imported Swift values, share one immutable
    // payload and its actual reservation. Copying cannot duplicate storage
    // while retaining only one charge.
    std::shared_ptr<const payload> value_;
    ordinary_upload_acknowledgment_result(std::shared_ptr<void>,
        std::vector<std::string>,std::vector<uint8_t>);
public:
    ordinary_upload_acknowledgment_result()=default;
    bool isValid()const noexcept;
    ordinary_upload_acknowledgment_result merged(const ordinary_upload_acknowledgment_result&)const noexcept;
    const std::vector<std::string>& ids()const noexcept;
    // Passive owned-ID access for Swift; the immutable issued payload and its
    // reservation remain retained by this value. No interior reference escapes.
    size_t idCount()const noexcept;
    std::string idAt(size_t)const noexcept;
    const std::vector<uint8_t>& acknowledgment()const noexcept;
    size_t acknowledgmentByteCount()const noexcept;
    // Exact-sized, caller-owned destination. An invalid result never copies;
    // a valid empty result accepts a zero-sized destination, including null.
    bool copyAcknowledgmentTo(void*,size_t)const noexcept;
};
ordinary_upload_acknowledgment_result receive_ordinary_source_upload_with_acknowledgment(
    lattice_db&,const uint8_t*,size_t,const std::vector<std::string>&);

// Namespace discrimination only. This never authenticates, validates or issues
// a source; it prevents reserved/mixed frames falling through legacy parsers.
bool ordinary_sync_reserved_frame(const uint8_t* bytes,size_t count)noexcept;
}
