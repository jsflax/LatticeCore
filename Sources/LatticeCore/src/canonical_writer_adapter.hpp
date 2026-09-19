#pragma once
#include "canonical_change_store.hpp"
#include <memory>
#include <string>
#include <vector>

namespace lattice::detail {
// Private Slice A qualification profile. No serving/epoch authority. The owner
// must outlive this attachment; it pins only the exact physical writer wrapper.
// Call before exposing that writer, with exclusive callback/DDL custody. Raw
// callback replacement, external DDL, attached writes and implicit adoption of a
// replacement writer are unsupported. Slice B must supply those lifecycle seams.
// Derived vec0/FTS indexes of ordinary models remain required qualification;
// their current refusal is distinct from unsupported virtual source relations.
struct canonical_writer_profile {
    canonical_store_binding binding;
    canonical_store_limits limits;
    std::vector<std::string> models; // complete registered scalar/regular-link closure
    bool upstream_requested = false; // always refused by Slice A
};
class canonical_writer_adapter {
    friend void require_canonical_relation(database&, const std::string&);
    struct context;
    std::shared_ptr<database> writer_;
    std::shared_ptr<context> context_;
    explicit canonical_writer_adapter(lattice_db&, const canonical_writer_profile&);
public:
    static std::unique_ptr<canonical_writer_adapter> attach(lattice_db&, const canonical_writer_profile&);
    ~canonical_writer_adapter();
    canonical_writer_adapter(const canonical_writer_adapter&) = delete;
    canonical_writer_adapter& operator=(const canonical_writer_adapter&) = delete;
    static constexpr bool serving_capability = false;
    // UUID equality key only. Never rewrites persisted model/reference/AuditLog bytes.
    static std::string uuid_key(const std::string&);
};
} // namespace lattice::detail
