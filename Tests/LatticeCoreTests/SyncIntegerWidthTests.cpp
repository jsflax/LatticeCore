#include "TestHelpers.hpp"
#include <nlohmann/json.hpp>
#include <limits>

namespace {
using json = nlohmann::json;

std::vector<int64_t> swift_integer_boundaries() {
    return {std::numeric_limits<int64_t>::min(), -4294967297LL,
        -2147483649LL, -2147483648LL, -1, 0, 1, 2147483647LL,
        2147483648LL, 4294967296LL, 9007199254740993LL,
        std::numeric_limits<int64_t>::max()};
}

void expect_wire_integer(const lattice::audit_log_entry& entry, int kind, int64_t expected) {
    const auto found = entry.changed_fields.find("int64_val");
    ASSERT_NE(found, entry.changed_fields.end());
    EXPECT_EQ(found->second.kind, static_cast<lattice::any_property_kind>(kind));
    ASSERT_TRUE(std::holds_alternative<int64_t>(found->second.value));
    EXPECT_EQ(std::get<int64_t>(found->second.value), expected);
    EXPECT_EQ(found->second.to_column_value(), lattice::column_value_t(expected));
    const auto emitted = json::parse(entry.changed_fields_to_json()).at("int64_val");
    EXPECT_EQ(emitted.at("kind"), kind);
    ASSERT_TRUE(emitted.at("value").is_number_integer());
    EXPECT_EQ(emitted.at("value").get<int64_t>(), expected);
}
}

TEST(SyncIntegerWidth, SwiftIntAndInt64KeepValueAndKindAcrossEveryAuditDecodeEntry) {
    for (const int kind : {0, 1}) for (const int64_t value : swift_integer_boundaries()) {
        SCOPED_TRACE(kind);
        SCOPED_TRACE(value);
        const json fields = {{"int64_val", {{"kind", kind}, {"value", value}}}};
        lattice::audit_log_entry standalone;
        standalone.changed_fields = lattice::audit_log_entry::parse_changed_fields(fields.dump());
        expect_wire_integer(standalone, kind, value);

        for (const bool string_fields : {false, true}) {
            SCOPED_TRACE(string_fields);
            json original = {{"globalId", "wide-audit"}, {"tableName", "TestAllTypes"},
                {"operation", "UPDATE"}, {"globalRowId", "wide-row"},
                {"changedFields", fields}, {"changedFieldsNames", {"int64_val"}},
                {"timestamp", "2026-09-29T00:00:00Z"}};
            if (string_fields) original["changedFields"] = fields.dump();
            const auto entry = lattice::audit_log_entry::from_json(original.dump());
            ASSERT_TRUE(entry); expect_wire_integer(*entry, kind, value);
            const json wire = {{"kind", "auditLog"}, {"auditLog", json::array({original})}};
            const auto event = lattice::server_sent_event::from_json(wire.dump());
            ASSERT_TRUE(event); ASSERT_EQ(event->audit_logs.size(), 1u);
            expect_wire_integer(event->audit_logs.front(), kind, value);
            const auto relayed = lattice::server_sent_event::from_json(event->to_json());
            ASSERT_TRUE(relayed); ASSERT_EQ(relayed->audit_logs.size(), 1u);
            expect_wire_integer(relayed->audit_logs.front(), kind, value);
        }
    }
}

TEST(SyncIntegerWidth, SwiftIntRemoteApplyAndDuplicateReplayKeepExactStoredIntegers) {
    TempDB file{"sync-swift-int-width"};
    lattice::lattice_db db(lattice::configuration(file.str()));
    unsigned index = 0;
    for (const int64_t value : swift_integer_boundaries()) {
        SCOPED_TRACE(value);
        lattice::audit_log_entry original;
        original.global_id = "wide-audit-" + std::to_string(index);
        original.global_row_id = "wide-row-" + std::to_string(index++);
        original.table_name = "TestAllTypes"; original.operation = "INSERT";
        original.timestamp = "2026-09-29T00:00:00Z";
        original.changed_fields = {{"int_val", lattice::any_property(0)},
            {"int64_val", lattice::any_property(value)},
            {"double_val", lattice::any_property(0.0)},
            {"bool_val", lattice::any_property(0)},
            {"string_val", lattice::any_property("wide integer")},
            {"optional_int", lattice::any_property(nullptr)},
            {"optional_string", lattice::any_property(nullptr)}};
        original.changed_fields_names = {"int_val", "int64_val", "double_val", "bool_val",
            "string_val", "optional_int", "optional_string"};
        // The exact kind0 payload emitted by Swift Int, including values that
        // cannot pass through a 32-bit C++ int. The Int64 constructor above is
        // only used to construct the JSON number, not to decode the wire.
        auto wire = json::parse(lattice::server_sent_event::make_audit_log({original}).to_json());
        wire.at("auditLog").at(0).at("changedFields").at("int64_val")["kind"] = 0;
        const auto received = lattice::server_sent_event::from_json(wire.dump());
        ASSERT_TRUE(received); ASSERT_EQ(received->audit_logs.size(), 1u);
        expect_wire_integer(received->audit_logs.front(), 0, value);
        EXPECT_EQ(lattice::apply_remote_changes(db, received->audit_logs),
            std::vector<std::string>{original.global_id});
        const auto row = db.db().query("SELECT int64_val,typeof(int64_val) AS storage FROM TestAllTypes WHERE globalId=?",
            {original.global_row_id});
        ASSERT_EQ(row.size(), 1u);
        EXPECT_EQ(row.front().at("int64_val"), lattice::column_value_t(value));
        EXPECT_EQ(row.front().at("storage"), lattice::column_value_t(std::string("integer")));
        const auto audit = db.db().query("SELECT * FROM AuditLog WHERE globalId=?", {original.global_id});
        ASSERT_EQ(audit.size(), 1u);
        const auto recorded = lattice::audit_log_entry::parse_changed_fields(
            std::get<std::string>(audit.front().at("changedFields")));
        ASSERT_EQ(recorded.count("int64_val"), 1u);
        EXPECT_EQ(recorded.at("int64_val").to_column_value(), lattice::column_value_t(value));
        EXPECT_EQ(lattice::apply_remote_changes(db, received->audit_logs),
            std::vector<std::string>{original.global_id});
        EXPECT_EQ(db.db().query("SELECT int64_val,typeof(int64_val) AS storage FROM TestAllTypes WHERE globalId=?",
            {original.global_row_id}), row);
        EXPECT_EQ(db.db().query("SELECT * FROM AuditLog WHERE globalId=?", {original.global_id}), audit);
    }
}
