#pragma once
#include <string>

namespace lattice::detail {
// The exact existing normal setup SQL, shared with read-only metadata validation.
// Returning a template does not create, repair, inspect or grant authority.
inline std::string ordinary_audit_table_program() { return R"(
            CREATE TABLE IF NOT EXISTS AuditLog(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                globalId TEXT UNIQUE COLLATE NOCASE DEFAULT (
                    lower(hex(randomblob(4)))   || '-' ||
                    lower(hex(randomblob(2)))   || '-' ||
                    '4' || substr(lower(hex(randomblob(2))),2) || '-' ||
                    substr('89AB', 1 + (abs(random()) % 4), 1) ||
                      substr(lower(hex(randomblob(2))),2)     || '-' ||
                    lower(hex(randomblob(6)))
                ),
                tableName TEXT,
                operation TEXT,
                rowId INTEGER,
                globalRowId TEXT,
                changedFields TEXT,
                changedFieldsNames TEXT,
                isFromRemote INTEGER DEFAULT 0,
                isSynchronized INTEGER DEFAULT 0,
                timestamp REAL DEFAULT (unixepoch('subsec')),
                synthesized INTEGER DEFAULT 0
            )
        )"; }

inline std::string ordinary_audit_pending_index_program() { return R"(
            CREATE INDEX IF NOT EXISTS idx_audit_log_pending_sync
                ON AuditLog(isSynchronized)
                WHERE isSynchronized = 0
        )"; }

inline std::string ordinary_audit_lookup_index_program() { return R"(
            CREATE INDEX IF NOT EXISTS idx_audit_log_change_lookup
                ON AuditLog(tableName, rowId, operation)
        )"; }

inline std::string ordinary_sync_control_table_program() { return R"(
            CREATE TABLE IF NOT EXISTS _SyncControl (
                id INTEGER PRIMARY KEY CHECK(id=1),
                disabled INTEGER NOT NULL DEFAULT 0
            )
        )"; }

inline std::string ordinary_sync_state_table_program() { return R"(
            CREATE TABLE IF NOT EXISTS _lattice_sync_state (
                audit_entry_id INTEGER NOT NULL,
                sync_id TEXT NOT NULL,
                is_synchronized INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (audit_entry_id, sync_id)
            )
        )"; }

inline std::string ordinary_sync_state_pending_index_program() { return R"(
            CREATE INDEX IF NOT EXISTS idx_sync_state_pending
                ON _lattice_sync_state(sync_id, is_synchronized)
                WHERE is_synchronized = 0
        )"; }

inline std::string ordinary_meta_table_program() { return R"(
            CREATE TABLE IF NOT EXISTS _lattice_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        )"; }

} // namespace lattice::detail
