#pragma once
#include "recovery_obligation_store.hpp"
#include <map>

namespace lattice::detail {
class recovery_receiver_controller;
class recovery_continuous_producer;

// Exact framing retained before network handoff. No frontier, installation
// success or source authentication is granted here. Version 2 retains the
// verified receipt disposition/resend projection through cancellation.
struct recovery_request_row {
    recovery_obligation_address journal;
    int64_t barrier=0, sequence=0, journal_revision=0, route=0;
    std::string domain, source_context, request_frame, manifest_frame;
    // Empty BLOB with format2: new Q, full future decision charge reserved.
    // "0" is DTO-only: actual format1 + SQL NULL from v1 migration,
    // unavailable knowledge, no payload charge and no restricted send authority.
    // "1" + 64-byte exact parent+decision digest + (C/N/U,0/1) pairs in exact Q receipt order. C/N require bit0.
    // UNKNOWN+0 grants neither UNSENT nor legacy ACK authority.
    std::string receipt_decisions;
    bool operator==(const recovery_request_row&) const = default;
};

// Owned by the opt-in continuous profile. Every method borrows the caller's
// actual owned main WRITE; its outputs are provisional until known COMMIT.
// Only the controller/enrollment boundary can construct this storage helper.
class recovery_request_store {
    friend class recovery_receiver_controller;
    friend class recovery_continuous_producer;
    friend class recovery_unknown_reconciliation;
    friend struct recovery_request_store_test_access;
    std::shared_ptr<lattice_db> owner_;
    explicit recovery_request_store(std::shared_ptr<lattice_db>);
    database& writer()const;
    void initialize();
    void migrate_existing();
    void audit()const;
    std::vector<std::string> channels()const;
    std::optional<recovery_request_row> read(const std::string&)const;
    void insert(const recovery_request_row&);
    void add_manifest(const recovery_request_row&,const std::string&);
    void erase(const recovery_request_row&);
    void rebind(const recovery_request_row&,int64_t replacement_route);
    void retain_receipt_decisions(const recovery_request_row&,const std::string&);
    static std::string fingerprint(const recovery_request_row&);
    static std::string seal_receipt_decisions(const recovery_request_row&,const std::string& pairs);
    std::map<std::string,std::string> fingerprints()const;
    size_t charged_bytes()const;
public:
    static constexpr int64_t version=2;
    static constexpr size_t decision_entries=8192;
    static constexpr size_t decision_bytes=65+2*decision_entries;
    static constexpr size_t maximum_rows=16;
    static constexpr size_t frame_bytes=4*1024*1024;
    static constexpr size_t context_bytes=65536;
    static constexpr size_t stored_bytes=128*1024*1024;
};
} // namespace lattice::detail
