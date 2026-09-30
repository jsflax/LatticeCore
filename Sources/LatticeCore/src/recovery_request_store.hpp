#pragma once
#include "recovery_obligation_store.hpp"
#include <map>

namespace lattice::detail {
class recovery_receiver_controller;
class recovery_continuous_producer;

// Exact framing retained before network handoff. No frontier, installation
// success, source authentication, or receipt disposition is stored here.
struct recovery_request_row {
    recovery_obligation_address journal;
    int64_t barrier=0, sequence=0, journal_revision=0, route=0;
    std::string domain, source_context, request_frame, manifest_frame;
    bool operator==(const recovery_request_row&) const = default;
};

// Owned by the opt-in continuous profile. Every method borrows the caller's
// actual owned main WRITE; its outputs are provisional until known COMMIT.
// Only the controller/enrollment boundary can construct this storage helper.
class recovery_request_store {
    friend class recovery_receiver_controller;
    friend class recovery_continuous_producer;
    friend class recovery_unknown_reconciliation;
    std::shared_ptr<lattice_db> owner_;
    explicit recovery_request_store(std::shared_ptr<lattice_db>);
    database& writer()const;
    void initialize();
    void audit()const;
    std::vector<std::string> channels()const;
    std::optional<recovery_request_row> read(const std::string&)const;
    void insert(const recovery_request_row&);
    void add_manifest(const recovery_request_row&,const std::string&);
    void erase(const recovery_request_row&);
    void rebind(const recovery_request_row&,int64_t replacement_route);
    std::map<std::string,std::string> fingerprints()const;
public:
    static constexpr int64_t version=1;
    static constexpr size_t maximum_rows=16;
    static constexpr size_t frame_bytes=4*1024*1024;
    static constexpr size_t context_bytes=65536;
    static constexpr size_t stored_bytes=128*1024*1024;
};
} // namespace lattice::detail
