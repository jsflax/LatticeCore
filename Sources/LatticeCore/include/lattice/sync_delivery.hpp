#pragma once

// Internal durable filtered-delivery storage. Not a second transport or sync engine.
// Included after lattice.hpp by sync.cpp; tests may exercise its exact transactional seam.
#include "sync.hpp"

namespace lattice::detail {
inline constexpr size_t sync_outbox_bytes = 2 * sync_max_encoded_message_bytes;
inline constexpr size_t sync_outbox_rows = 4096;

// The released sync owner already used this transaction boundary. Keep its
// idle-connection admission and post-unlock notification settlement for every
// durable delivery mutation, including Q2's admission/cancellation paths.
inline bool sync_delivery_transaction(lattice_db& lattice, const std::function<bool()>& body) {
    auto& db = lattice.db();
    if (lattice.store_write_gate()) database::maintenance_scope::probe_before_store_gate(db);
    bool committed = false;
    try {
        {
            lattice_db::store_write_gate_hold gate(lattice);
            database::maintenance_scope ownership(db);
            db.begin_transaction();
            try {
                committed = body();
                if (committed) db.commit(); else db.rollback();
                if (db.is_in_transaction()) throw db_error("Sync delivery transaction did not settle");
            } catch (...) {
                try {
                    if (db.is_in_transaction()) db.rollback();
                    if (db.is_in_transaction()) throw db_error("Sync delivery rollback did not settle");
                } catch (...) { db.closed_.store(true, std::memory_order_release); }
                throw;
            }
        }
    } catch (...) {
        try { db.drain_if_settled(); } catch (...) {}
        throw;
    }
    db.drain_if_settled();
    return committed;
}
inline int64_t read_sync_reset_cutoff(database& db,const std::string& sync_id) {
    if(db.is_closed()) throw db_error("cannot read sync reset cutoff on closed database");
    const auto rows=db.query("SELECT CASE WHEN typeof(value)='text' AND length(CAST(value AS BLOB))<=19 "
        "THEN value END AS value FROM main._lattice_meta WHERE key=?",{sync_reset_meta_key(sync_id)});
    if(db.is_closed()) throw db_error("sync reset cutoff database closed during read");
    if(rows.empty()) return 0;
    if(rows.size()!=1 || !std::holds_alternative<std::string>(rows[0].at("value")))
        throw db_error("invalid sync reset cutoff storage");
    return parse_sync_reset_cutoff(std::get<std::string>(rows[0].at("value")));
}


inline std::vector<sync_delivery> read_sync_outbox(database& db, const std::string& sync_id,
        const std::vector<sync_delivery>& previously_validated = {}) {
    if (db.is_closed()) throw db_error("cannot read delivery outbox on closed database");
    // One statement bounds every retained string before materialization. The first row carries a
    // failure sentinel if the complete channel exceeds its logical retained-byte/row allowance.
    const auto cap = std::to_string(sync_outbox_bytes);
    const auto rows = db.query(
        "WITH selected AS (SELECT receipt_id,source_audit_id,source_global_id,wire_global_id,table_name,global_row_id,membership,wire_json,routing_key,encoded_charge,legacy_receipt FROM main._lattice_sync_outbox WHERE sync_id=? "
        "ORDER BY legacy_receipt DESC, CASE WHEN legacy_receipt=1 THEN receipt_id ELSE source_audit_id END LIMIT 4097), budget AS (SELECT COUNT(*) AS n, "
        "COALESCE(SUM(length(CAST(wire_json AS BLOB))+COALESCE(length(CAST(source_global_id AS BLOB)),0)+"
        "length(CAST(wire_global_id AS BLOB))+length(CAST(table_name AS BLOB))+"
        "length(CAST(global_row_id AS BLOB))+length(CAST(routing_key AS BLOB))+128),0) AS bytes FROM selected) "
        "SELECT a.receipt_id, typeof(a.source_audit_id) IN ('integer','null') AS source_type_valid, "
        "CASE WHEN typeof(a.source_audit_id)='integer' THEN a.source_audit_id END AS source_audit_id, "
        "CASE WHEN typeof(a.membership)='integer' THEN a.membership END AS membership, "
        "CASE WHEN typeof(a.encoded_charge)='integer' THEN a.encoded_charge END AS encoded_charge, "
        "CASE WHEN typeof(a.legacy_receipt)='integer' THEN a.legacy_receipt END AS legacy_receipt, b.n, b.bytes, "
        "EXISTS(SELECT 1 FROM main.AuditLog source WHERE source.id=a.source_audit_id "
        "AND source.globalId=a.source_global_id AND source.tableName=a.table_name AND source.globalRowId=a.global_row_id) AS source_valid, "
        "EXISTS(SELECT 1 FROM main.AuditLog source WHERE source.id=a.source_audit_id) AS source_present, "
        "CASE WHEN b.n<=4096 AND b.bytes<=" + cap + " THEN a.source_global_id END AS source_global_id, "
        "CASE WHEN b.n<=4096 AND b.bytes<=" + cap + " THEN a.wire_global_id END AS wire_global_id, "
        "CASE WHEN b.n<=4096 AND b.bytes<=" + cap + " THEN a.table_name END AS table_name, "
        "CASE WHEN b.n<=4096 AND b.bytes<=" + cap + " THEN a.global_row_id END AS global_row_id, "
        "CASE WHEN b.n<=4096 AND b.bytes<=" + cap + " THEN a.wire_json END AS wire_json, "
        "CASE WHEN b.n<=4096 AND b.bytes<=" + cap + " THEN a.routing_key END AS routing_key "
        "FROM selected a CROSS JOIN budget b ORDER BY a.legacy_receipt DESC, CASE WHEN a.legacy_receipt=1 THEN a.receipt_id ELSE a.source_audit_id END", {sync_id});
    if (db.is_closed()) throw db_error("delivery outbox closed during read");
    // Reuse only the decoded wire value from this owner's prior validated read/admission.
    // The current SQL still proves the complete byte/row budget and every source/identity field.
    // Comparing exact wire bytes prevents stale payload reuse after a sibling edit or reset.
    std::unordered_map<int64_t, const sync_delivery*> previous;
    for (const auto& delivery : previously_validated) previous.emplace(delivery.receipt_id, &delivery);
    std::vector<sync_delivery> result;
    result.reserve(rows.size());
    for (const auto& row : rows) {
        const auto number = [&](const char* name) -> int64_t {
            const auto& value = row.at(name);
            if (!std::holds_alternative<int64_t>(value)) throw db_error("invalid delivery integer");
            return std::get<int64_t>(value);
        };
        if (number("n") > static_cast<int64_t>(sync_outbox_rows) || number("bytes") > static_cast<int64_t>(sync_outbox_bytes))
            throw db_error("delivery outbox exceeds retained budget");
        const auto string = [&](const char* name) -> const std::string& {
            const auto& value = row.at(name);
            if (!std::holds_alternative<std::string>(value)) throw db_error("invalid delivery text");
            return std::get<std::string>(value);
        };
        const auto& wire_json = string("wire_json");
        sync_delivery delivery;
        delivery.receipt_id = number("receipt_id");
        if (number("source_type_valid")!=1) throw db_error("invalid delivery source type");
        const auto legacy = number("legacy_receipt");
        if (legacy != 0 && legacy != 1) throw db_error("invalid delivery legacy disposition");
        delivery.legacy_receipt = legacy == 1;
        const bool absent_source = std::holds_alternative<std::nullptr_t>(row.at("source_audit_id"));
        const bool absent_global = std::holds_alternative<std::nullptr_t>(row.at("source_global_id"));
        if ((!delivery.legacy_receipt && (absent_source || absent_global)))
            throw db_error("modern delivery requires source provenance");
        delivery.source_id = absent_source ? 0 : number("source_audit_id");
        delivery.source_global_id = absent_global ? "" : string("source_global_id");
        const auto old = previous.find(delivery.receipt_id);
        if (old != previous.end() && old->second->wire_json == wire_json) {
            delivery.wire = old->second->wire;
        } else {
            auto event = server_sent_event::from_json("{\"kind\":\"auditLog\",\"auditLog\":[" + wire_json + "]}");
            if (!event || event->audit_logs.size() != 1) throw db_error("invalid durable delivery wire row");
            delivery.wire = std::move(event->audit_logs.front());
        }
        delivery.wire_json = wire_json;
        delivery.routing_key = string("routing_key");
        if (delivery.routing_key.empty() || delivery.routing_key.size()>65536) throw db_error("invalid delivery routing key");
        const auto membership = number("membership");
        if (membership < -1 || membership > 1) throw db_error("invalid delivery membership");
        delivery.membership = static_cast<int>(membership);
        const auto charge = number("encoded_charge");
        // Released source columns were independently nullable and never gated
        // retry. They remain provenance only, even if a live audit row reuses
        // that number; only modern receipts require current source identity.
        const bool source_valid = delivery.legacy_receipt || number("source_valid") == 1;
        if (!source_valid || (!absent_source && delivery.source_id <= 0) ||
            (!absent_global && delivery.source_global_id.empty()) || charge <= 0 ||
            charge > static_cast<int64_t>(sync_max_encoded_message_bytes) ||
            delivery.membership < -1 || delivery.membership > 1 ||
            delivery.wire.id != (delivery.legacy_receipt ? 0 : delivery.source_id) || delivery.wire.global_id != string("wire_global_id") ||
            delivery.wire.table_name != string("table_name") ||
            delivery.wire.global_row_id != string("global_row_id"))
            throw db_error("durable delivery identity mismatch");
        constexpr size_t envelope = sizeof("{\"kind\":\"auditLog\",\"auditLog\":[") - 1 + 2;
        if (wire_json.size() + envelope != static_cast<size_t>(charge))
            throw db_error("durable delivery encoded charge mismatch");
        delivery.charge = static_cast<size_t>(charge);
        result.push_back(std::move(delivery));
    }
    return result;
}

inline size_t sync_delivery_retained_bytes(const sync_delivery& delivery, const std::string& wire_json) {
    return wire_json.size() + delivery.source_global_id.size() + delivery.wire.global_id.size() +
           delivery.wire.table_name.size() + delivery.wire.global_row_id.size() + delivery.routing_key.size() + 128;
}

// False means retention credit is currently occupied or this entity already has a predecessor.
// An existing receipt is never replaced. The caller reloads that exact receipt on the next pass.
inline bool admit_sync_delivery_in_transaction(lattice_db& lattice, const std::string& sync_id, const sync_delivery& delivery) {
    auto& db = lattice.db();
    if (db.is_closed()) throw db_error("cannot admit delivery on closed database");
    auto frame = encode_audit_log_prefix({delivery.wire}, 0, 1);
    const auto wire_json = single_encoded_audit_log_json(frame);
    if (delivery.legacy_receipt || delivery.source_id <= 0 || delivery.source_global_id.empty() || delivery.routing_key.empty() || delivery.routing_key.size()>65536 ||
        delivery.wire.id != delivery.source_id || delivery.membership < -1 || delivery.membership > 1 || frame.count != 1)
        throw db_error("invalid delivery admission");
    const auto retained = sync_delivery_retained_bytes(delivery, wire_json);
    if (retained > sync_outbox_bytes) throw std::length_error("delivery exceeds retained byte limit");
    if (!db.is_in_transaction()) throw db_error("delivery admission requires an owned write transaction");
    if(delivery.source_id<=read_sync_reset_cutoff(db,sync_id)) throw db_error("delivery source belongs to a retired snapshot cohort");
    {
        const auto source = db.query("SELECT id FROM main.AuditLog WHERE id=? AND globalId=? AND tableName=? AND globalRowId=?", 
            {delivery.source_id, delivery.source_global_id,delivery.wire.table_name,delivery.wire.global_row_id});
        if (source.size() != 1) throw db_error("delivery source changed before admission");
        const auto conflict = db.query("SELECT 1 FROM main._lattice_sync_outbox WHERE sync_id=? "
            "AND (source_audit_id=? OR (table_name=? AND global_row_id=?)) LIMIT 1",
            {sync_id, delivery.source_id, delivery.wire.table_name, delivery.wire.global_row_id});
        const auto budget = db.query("SELECT COUNT(*) AS n, COALESCE(SUM(length(CAST(wire_json AS BLOB))+"
            "COALESCE(length(CAST(source_global_id AS BLOB)),0)+length(CAST(wire_global_id AS BLOB))+"
            "length(CAST(table_name AS BLOB))+length(CAST(global_row_id AS BLOB))+length(CAST(routing_key AS BLOB))+128),0) AS bytes "
            "FROM main._lattice_sync_outbox WHERE sync_id=?", {sync_id});
        if (budget.size() != 1) throw db_error("cannot read delivery admission budget");
        const auto n = std::get<int64_t>(budget[0].at("n")), bytes = std::get<int64_t>(budget[0].at("bytes"));
        if (n < 0 || bytes < 0 || static_cast<uint64_t>(bytes) > sync_outbox_bytes)
            throw db_error("invalid delivery admission budget");
        if (!conflict.empty() || n >= static_cast<int64_t>(sync_outbox_rows) ||
            retained > sync_outbox_bytes - static_cast<size_t>(bytes)) return false;
        db.execute("INSERT INTO main._lattice_sync_outbox(sync_id,source_audit_id,source_global_id,"
            "wire_global_id,table_name,global_row_id,membership,wire_json,encoded_charge,routing_key) VALUES(?,?,?,?,?,?,?,?,?,?)",
            {sync_id, delivery.source_id, delivery.source_global_id, delivery.wire.global_id,
             delivery.wire.table_name, delivery.wire.global_row_id, static_cast<int64_t>(delivery.membership),
             wire_json, static_cast<int64_t>(frame.entry_charges[0]),delivery.routing_key});
        if (test_hooks::sync_outbox_after_insert) test_hooks::sync_outbox_after_insert();
        // Explicit pending disposition preserves this original even if a previous floor passed it.
        db.execute("INSERT INTO main._lattice_sync_state(audit_entry_id,sync_id,is_synchronized) VALUES(?,?,0) "
                   "ON CONFLICT(audit_entry_id,sync_id) DO UPDATE SET is_synchronized=0", {delivery.source_id,sync_id});
        db.execute("UPDATE main._lattice_replication_slots SET upload_floor=MIN(upload_floor,?) WHERE sync_id=?",
                   {delivery.source_id - 1,sync_id});
        return true;
    }
}

inline bool admit_sync_delivery(lattice_db& lattice, const std::string& sync_id, const sync_delivery& delivery) {
    return sync_delivery_transaction(lattice, [&] {
        return admit_sync_delivery_in_transaction(lattice, sync_id, delivery);
    });
}
} // namespace lattice::detail
