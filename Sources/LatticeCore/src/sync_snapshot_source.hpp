#pragma once
#include "sync_recovery_protocol.hpp"
#include <functional>

namespace lattice { class lattice_db; }
namespace lattice::detail::sync_recovery {

enum class relation_kind { model, link };
struct source_relation {
    std::string table;
    relation_kind kind;
    // Caller explicitly declares authority over every row of this relation.
    // Filtered scopes need a separate validated selector; never silently widen.
    bool complete_table_scope = false;
};
struct source_limits {
    limits wire;
    size_t tables;
    size_t columns_per_table;
    size_t indexes_per_table;
};
struct source_column {
    std::string name;
    std::string sql_type;
    bool not_null;
    int64_t primary_key_order;
};
struct source_layout {
    std::string table;
    relation_kind kind;
    std::vector<source_column> columns;
    // A proven UNIQUE index's collation, limited to BINARY or NOCASE.
    std::string identity_collation;
};
struct unsealed_materialization {
    // These are observed at the same SQLite generation as every row. Neither
    // is an authoritative stream epoch, validated receipt or resume token.
    uint64_t audit_head_candidate = 0;
    std::optional<std::string> local_history_revision;
    int64_t schema_cookie = 0;
    std::vector<source_layout> layouts;
    // Content pages only. Final wire encoding adds binding/digest/envelope
    // bytes and must repartition or refuse if its raw-frame cap is exceeded.
    std::vector<std::vector<row>> pages;
    uint64_t rows = 0;
    uint64_t content_bytes = 0;
};

// Synchronous, private, read-only and file-WAL-only. The caller retains the
// actual owner for the complete call; this is not an async owner-lifetime API.
// Uses one read generation and refuses retirement instead of resolving a new
// view. Input batches use indexed keysets. The capped materialization is then
// sorted once in byte order and moved into bounded content pages.
// No source spool, cryptographic digest, receipt or installation is produced.
// Payload bytes use audit_log_entry's AnyProperty JSON map: ordered property
// names, its existing numeric spelling and lowercase hexadecimal BLOB values.
// id/globalId are omitted from that map; globalId is the separate row identity.
// Byte preflight is conservative and may refuse a row whose final JSON fits.
unsealed_materialization materialize_source(
    lattice_db& owner, const std::vector<source_relation>& scope, const source_limits& budget);

namespace source_test_hooks {
// Invoked after a copied input batch, before final byte sorting/page packing,
// with no active SQL statement/pool lock. This per-invocation seam is never
// installed on an owner or used by production. Count is not output page count.
unsealed_materialization materialize(
    lattice_db& owner, const std::vector<source_relation>& scope, const source_limits& budget,
    const std::function<void(size_t, uint64_t)>& after_capture_batch);
}
} // namespace lattice::detail::sync_recovery
