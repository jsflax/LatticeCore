// lattice.cpp - Implementation moved to header (templates)
// This file kept for potential non-template implementations

#include "lattice/lattice.hpp"

namespace lattice {

// Singleton instance - defined here to ensure single copy across all translation units
instance_registry& instance_registry::instance() {
    static instance_registry reg;
    return reg;
}
void lattice_db::setup_change_hook() {
    printf("[setup_change_hook] Setting up hooks for path: %s\n", config_.path.c_str());

    // Update hook - buffers changes (called for each row change)
    sqlite3_update_hook(db_->handle(),
        [](void* user_data, int operation, const char* db_name, const char* table_name, sqlite3_int64 rowid) {
            auto* self = static_cast<lattice_db*>(user_data);

            std::string op;
            switch (operation) {
                case SQLITE_INSERT: op = "INSERT"; break;
                case SQLITE_UPDATE: op = "UPDATE"; break;
                case SQLITE_DELETE: op = "DELETE"; break;
                default: return;
            }

            printf("[update_hook] table=%s op=%s rowid=%lld\n", table_name, op.c_str(), (long long)rowid);

            // Handle internal tables specially
            std::string table(table_name);
            if (table == "_SyncControl") {
                printf("[update_hook] Skipping _SyncControl table\n");
                return;
            }

            // For AuditLog changes:
            // - In-memory DBs: notify directly (WAL hook won't fire)
            // - File DBs: skip (let flush_changes handle it via WAL hook to avoid double notification)
            if (table == "AuditLog") {
                if (self->config_.path == ":memory:" || self->config_.path.empty()) {
                    printf("[update_hook] AuditLog change (in-memory), notifying directly: op=%s rowid=%lld\n", op.c_str(), (long long)rowid);
                    // Get globalId for the AuditLog entry
                    std::string global_id;
                    if (operation != SQLITE_DELETE) {
                        std::string sql = "SELECT globalId FROM AuditLog WHERE id = ?";
                        auto rows = self->db_->query(sql, {static_cast<int64_t>(rowid)});
                        if (!rows.empty()) {
                            auto it = rows[0].find("globalId");
                            if (it != rows[0].end() && std::holds_alternative<std::string>(it->second)) {
                                global_id = std::get<std::string>(it->second);
                            }
                        }
                    }
                    self->notify_change("AuditLog", op, static_cast<int64_t>(rowid), global_id);
                } else {
                    printf("[update_hook] AuditLog change (file DB), skipping - WAL hook will handle\n");
                }
                return;
            }

            // Skip link tables (they start with underscore and have no 'id' column)
            if (!table.empty() && table[0] == '_') {
                printf("[update_hook] Skipping link table: %s\n", table_name);
                return;
            }

            // Get the globalId for this row (only for model tables with 'id' column)
            std::string global_id;
            if (operation != SQLITE_DELETE) {
                std::string sql = "SELECT globalId FROM " + table + " WHERE id = ?";
                auto rows = self->db_->query(sql, {static_cast<int64_t>(rowid)});
                if (!rows.empty()) {
                    auto it = rows[0].find("globalId");
                    if (it != rows[0].end() && std::holds_alternative<std::string>(it->second)) {
                        global_id = std::get<std::string>(it->second);
                    }
                }
            }

            // Buffer the change instead of notifying immediately
            printf("[update_hook] Buffering change: table=%s op=%s rowid=%lld globalId=%s\n",
                   table.c_str(), op.c_str(), (long long)rowid, global_id.c_str());
            self->append_to_change_buffer(table, op, static_cast<int64_t>(rowid), global_id);

            // For in-memory databases, flush immediately since WAL hook won't fire
            if (self->config_.path == ":memory:" || self->config_.path.empty()) {
                printf("[update_hook] In-memory DB, calling flush_changes()\n");
                self->flush_changes();
            } else {
                printf("[update_hook] File DB (path=%s), waiting for WAL hook\n", self->config_.path.c_str());
            }
        },
        this
    );

    // WAL hook - flushes buffered changes on transaction commit (file-based DBs only)
    sqlite3_wal_hook(db_->handle(),
        [](void* user_data, sqlite3*, const char*, int) -> int {
            auto* self = static_cast<lattice_db*>(user_data);
            self->flush_changes();
            return SQLITE_OK;
        },
        this
    );
}
// Future: non-template implementations can go here
void lattice_db::setup_sync_if_configured() {
    if (!config_.is_sync_enabled()) {
        return;
    }

    // Create sync config from our configuration
    sync_config sync_cfg;
    sync_cfg.websocket_url = config_.websocket_url;
    sync_cfg.authorization_token = config_.authorization_token;

    // Create synchronizer
    synchronizer_ = std::make_unique<synchronizer>(*this, sync_cfg, scheduler_);

    // Wire up callbacks if set
    if (on_sync_state_change_) {
        synchronizer_->set_on_state_change(on_sync_state_change_);
    }
    if (on_sync_error_) {
        synchronizer_->set_on_error(on_sync_error_);
    }

    // Auto-connect (like Swift's Lattice.init)
    synchronizer_->connect();
}

void lattice_db::attach(lattice_db &lattice) {
    std::reference_wrapper<std::unique_ptr<database>> dbs[2] = {this->db_, this->read_db_};
    for (auto& db : dbs) {
        std::stringstream ss;
        ss << "ATTACH DATABASE ";
        ss << "'" << lattice.config_.path << "'";
        ss << " AS ";
        std::filesystem::path p = lattice.config_.path;
        auto alias = p.filename().replace_extension();
        ss << alias;
        
        db.get()->execute(ss.str());
        
        auto tables = lattice.db_->query(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' "
            "AND name NOT IN ('AuditLog', '_SyncControl') "
            "AND name NOT LIKE '%_vec0'");

        int64_t total_entries = 0;

        for (const auto& table_row : tables) {
            auto it = table_row.find("name");
            if (it == table_row.end() || !std::holds_alternative<std::string>(it->second))
                continue;

            std::string table_name = std::get<std::string>(it->second);
            db.get()->execute("CREATE TEMP VIEW IF NOT EXISTS " + table_name +
                    " AS SELECT * FROM \"" + alias.string() + "\"." + table_name);
        }
    }
}

} // namespace lattice
