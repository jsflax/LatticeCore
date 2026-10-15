#pragma once
#include "sync.hpp"
#include <memory>

namespace lattice {
namespace detail {
class ordinary_export_access;
class ordinary_wire_access;
class ordinary_receiver_session;
struct ordinary_export_frame;
}

/// A local engine-issued server capture. Its source and complete cut belong to
/// the retained immutable capture, not to mutable audit entries or JSON fields.
/// A receiving channel may establish one source from this type; changing that
/// source requires explicit rebind/bootstrap. Copying entries() into a vector
/// drops the ordered guarantee and retains the legacy provisional semantics.
/// This initial Core endpoint is not a wire decoder or historical-store grant.
class owned_server_export final {
    std::shared_ptr<const detail::ordinary_export_frame> frame_;
    explicit owned_server_export(std::shared_ptr<const detail::ordinary_export_frame>);
    friend class detail::ordinary_export_access;
    friend class detail::ordinary_wire_access;
    friend class detail::ordinary_receiver_session;
public:
    owned_server_export(const owned_server_export&) = default;
    owned_server_export(owned_server_export&&) noexcept = default;
    owned_server_export& operator=(const owned_server_export&) = default;
    owned_server_export& operator=(owned_server_export&&) noexcept = default;
    const std::string& source_id() const;
    int64_t epoch() const;
    int64_t cut() const;
    size_t row_count() const;
    const std::vector<audit_log_entry>& entries() const;
};

/// Trusted local server endpoint. Captures the named complete model scope in
/// the actual owner's own WRITE, commits its durable cut, then publishes the
/// immutable result. This first bounded component admits native scalar model tables with exact
/// generated programs and no foreign-key/relationship closure;
/// unsupported shapes and bounds refuse before issuing a cut. A caller-owned
/// transaction, unowned database handle or populated unenrolled origin cannot
/// acquire source authority. C/Swift/wire issuance is a separate integration.
owned_server_export capture_owned_server_export(lattice_db&,
    const std::vector<std::string>& models);
/// Installs a local engine-issued complete cut and its durable frontier in one
/// owned WRITE. Unknown local audit values and divergent preimages are retained
/// as conflicts; replay of a retained cut never executes model SQL again.
/// Receivers first enrolled through this explicit endpoint refuse subsequent
/// legacy vector export/receive. An already helper-enrolled scope retains its
/// separately guarded provisional delivery route and unresolved-payload ledger. This
/// does not retire a payload already captured by an earlier transport. Active
/// transport cutover must be integrated before normal product activation.
/// This bounded foundation does not yet supply conflict resolution, historical
/// admission, incremental pages, C/Swift issuance or a negotiated wire route.
std::vector<std::string> apply_owned_server_export(lattice_db&,
    const owned_server_export&);
}
