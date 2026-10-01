#pragma once
#include "canonical_durable_ready.hpp"
#include <nlohmann/json.hpp>

namespace lattice::detail {
// Data construction only; these helpers confer no source/owner authority.
// Shared by the authenticated recipe and the audited administrative record.
struct canonical_ready_transport_limits {
    static constexpr uint64_t max_requests=64,max_bytes=67108864,max_workspace=268435456,input_limit=8388608,reply_limit=4194304;
};
inline canonical_ready_profile canonical_named_ready_profile(const std::string& authority,
    const canonical_store_limits& limits,bool registered,const std::string& name,
    std::optional<int64_t> grace=std::nullopt) {
    const bool small=name=="boundedV1"||name=="boundedV1OrphanV1";
    const bool large=name=="bounded48MiBV1"||name=="bounded48MiBOrphanV1";
    const bool lifecycle=name=="boundedV1OrphanV1"||name=="bounded48MiBOrphanV1";
    if((!small&&!large)||registered&&!large||lifecycle!=bool(grace)||grace&&(*grace<=0||*grace>3600000))
        throw db_error("canonical named READY profile/grace differs");
    canonical_ready_profile p;
    p.authority=authority;p.transfers=16;p.bindings=1024;p.charged_bytes=67108864;p.transfer_bytes=2097152;
    p.package={{{16384,4096,2,256,4096,1048576,256,256,262144},16,4096,4096,256,256,65536,131072,3600000,{4096,32,256,2048,4096}},1572864,514};
    p.capture={{{65536,16,4096,8192,2,2048,4096,1048576},16,32,32},limits,256,256,32};
    if(large) {
        p.transfers=8;p.charged_bytes=536870912;p.transfer_bytes=50331648;
        p.package={{{4194304,16384,64,512,16384,33554432,256,8192,8388608},
            16,262144,32768,8192,8192,2097152,4194304,3600000,{16384,64,256,4096,16384}},41943040,770};
        p.capture={{{262144,16,16384,16384,64,256,16384,33554432},16,32,32},limits,8192,8192,256};
    }
    if(registered){p.transfers=16;p.charged_bytes=1073741824;}
    p.orphan_resume_grace_ms=grace;return p;
}
inline nlohmann::json canonical_ready_profile_description(const canonical_ready_profile& p,const std::string& name) {
    using json=nlohmann::json;const auto& c=p.package.codec;const auto& v=c.maximum;
    const json wire={{"frame_bytes",std::to_string(v.frame_bytes)},{"payload_bytes",std::to_string(v.payload_bytes)},
        {"items_per_page",std::to_string(v.items_per_page)},{"content_pages",std::to_string(v.content_pages)},
        {"content_identities",std::to_string(v.content_identities)},{"content_bytes",std::to_string(v.content_bytes)},
        {"receipt_pages",std::to_string(v.receipt_pages)},{"receipts",std::to_string(v.receipts)},{"receipt_bytes",std::to_string(v.receipt_bytes)}};
    json result={{"name",name},{"wire",wire},
        {"requestEntries",c.request_entries},{"requestTargets",c.request_targets},{"requestTargetBytes",c.request_target_bytes},
        {"parserDepth",c.depth},{"parserNodes",c.nodes},{"scalarBytes",c.string_bytes},{"restartBytes",c.restart_bytes},
        {"valueLimits",{{"rawBytes",c.values.raw_bytes},{"fields",c.values.fields},{"nameBytes",c.values.name_bytes},
            {"valueBytes",c.values.value_bytes},{"decodedBytes",c.values.decoded_bytes}}},
        {"leaseMilliseconds",c.lease_ms},{"packageBytes",p.package.retained_wire_bytes},{"frames",p.package.frames},
        {"transfers",p.transfers},{"bindings",p.bindings},{"durableBytes",p.charged_bytes},{"transferBytes",p.transfer_bytes},
        {"captureRows",p.capture.rows.wire.total_rows},{"captureBytes",p.capture.rows.wire.total_bytes},
        {"requestBytes",canonical_ready_transport_limits::input_limit},{"pendingRequests",canonical_ready_transport_limits::max_requests},
        {"pendingInputAndReplyBytes",canonical_ready_transport_limits::max_bytes},{"pendingWorkspaceBytes",canonical_ready_transport_limits::max_workspace}};
    if(p.orphan_resume_grace_ms)result["orphanResumeGraceMilliseconds"]=*p.orphan_resume_grace_ms;
    return result;
}
}
