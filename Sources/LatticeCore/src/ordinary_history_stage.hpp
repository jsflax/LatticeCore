#pragma once
#include <lattice/ordinary_history.hpp>
#include <lattice/db.hpp>
#include <functional>
#include <optional>

namespace lattice::detail {
// Passive stage identity. Only the owning exporter or authenticated receiver
// can construct a writer; this record never supplies an admission capability.
struct ordinary_history_stage_identity {
    std::string stage,source,scope,schema,device,inode;
    int64_t epoch=0,cut=0;
    bool receive=false;
};
struct ordinary_history_stage_totals {
    uint64_t rows=0,known=0,unknown=0,pages=0,bytes=0;
};
class ordinary_history_stage_writer final {
    friend class ordinary_export_access;
    friend class ordinary_receiver_session;
    struct state;
    std::unique_ptr<state> state_;
    ordinary_history_stage_writer(database&,const ordinary_history_stage_identity&,
        const ordinary_history_limits&);
public:
    ~ordinary_history_stage_writer();
    ordinary_history_stage_writer(const ordinary_history_stage_writer&)=delete;
    ordinary_history_stage_writer& operator=(const ordinary_history_stage_writer&)=delete;
    // Actual source capture supplies complete pages synchronously while its
    // owned WRITE pins the image. No network/user callback may run here.
    void append(std::string_view page,std::string_view previous_digest,
        std::string_view digest,uint64_t rows,uint64_t known,uint64_t unknown);
    // Seal the actual page manifest/header within this same owned WRITE.
    // Caller must still prove own COMMIT before publishing an export handle.
    ordinary_history_stage_totals finish(std::string_view header,
        std::string_view content_digest);
};
// Reads are passive resource checks. The caller proves real source or session
// authority and retains the actual physical store/serialized view separately.
struct ordinary_history_stage_record {
    ordinary_history_stage_identity identity;
    ordinary_history_stage_totals totals;
    std::string header,content_digest,seal;
    bool sealed=false;
};
}
