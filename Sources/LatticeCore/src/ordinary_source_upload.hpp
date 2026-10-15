#pragma once
#include <lattice/sync.hpp>
#include "ordinary_v2_audit_json.hpp"
#include <memory>
#include <string_view>

namespace lattice::detail {
// Synchronous passive value decoding. Reservation is declared first, so all
// decoded entries and their immutable observations die before its pool slot.
struct ordinary_source_upload_batch final {
    std::shared_ptr<void> reservation;
    std::vector<audit_log_entry> entries;
    size_t logical_bytes=0;
};
ordinary_source_upload_batch decode_ordinary_source_upload(std::string_view,bool require_versions=false);
struct ordinary_upload_ack final {std::string gid,version;};
bool ordinary_delivery_version_valid(std::string_view)noexcept;
std::shared_ptr<void> reserve_ordinary_upload_result();
std::vector<ordinary_upload_ack> decode_ordinary_upload_ack(std::string_view);
std::string encode_ordinary_upload_ack(const std::vector<audit_log_entry>&,
    const std::vector<std::string>&);
// The exact envelope keeps the original spelling. Actual AuditLog REAL storage
// uses the same established conversion as ordinary source intake.
double ordinary_upload_timestamp_to_epoch(const std::string&);
std::vector<database::row_t> read_ordinary_upload_rows(database&,const std::string&,
    const std::vector<column_value_t>&,size_t,const ordinary_v2_retained_charge&);
std::vector<audit_log_entry> query_ordinary_upload_entries(database&,const std::string&,
    const std::optional<std::vector<sync_filter_entry>>&,int64_t,size_t);
std::string encode_ordinary_source_upload(const std::vector<audit_log_entry>&);
void capture_ordinary_upload_original(audit_log_entry&,std::string_view,std::string_view,
    const ordinary_v2_retained_charge&,const ordinary_v2_workspace_acquire&);
std::vector<uint8_t> encode_ordinary_upload_observation(const ordinary_upload_original&,
    const ordinary_v2_retained_charge&);
std::shared_ptr<const ordinary_upload_original> decode_ordinary_upload_observation(
    std::span<const uint8_t>,const ordinary_v2_retained_charge&);
}
