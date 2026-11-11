#include "TestHelpers.hpp"
#include <lattice/sync.hpp>

namespace {
constexpr size_t hard_byte_limit = 2 * lattice::sync_max_encoded_message_bytes;
constexpr size_t frame_envelope = sizeof("{\"kind\":\"auditLog\",\"auditLog\":[") - 1 + 2;

lattice::audit_log_entry text_entry(const std::string& identity, size_t payload_size = 0) {
    lattice::audit_log_entry entry;
    entry.id = 1;
    entry.global_id = identity;
    entry.table_name = "LargeText";
    entry.global_row_id = "large-row";
    entry.operation = "INSERT";
    entry.changed_fields["text"] = lattice::any_property(std::string(payload_size, 'x'));
    entry.changed_fields_names = {"text"};
    return entry;
}

// Match the production query's materialization accounting rather than assuming
// the stored JSON and the transmitted JSON have identical overhead.
size_t materialized_charge(lattice::database& db, int64_t id) {
    const auto rows = db.query(
        "SELECT 64+COALESCE(length(CAST(globalId AS BLOB)),0)"
        "+COALESCE(length(CAST(tableName AS BLOB)),0)"
        "+COALESCE(length(CAST(operation AS BLOB)),0)"
        "+COALESCE(length(CAST(globalRowId AS BLOB)),0)"
        "+COALESCE(length(CAST(changedFields AS BLOB)),0)"
        "+COALESCE(length(CAST(changedFieldsNames AS BLOB)),0)"
        "+COALESCE(length(CAST(timestamp AS BLOB)),0) AS bytes FROM AuditLog WHERE id=?", {id});
    return static_cast<size_t>(std::get<int64_t>(rows.at(0).at("bytes")));
}

void set_materialized_charge(lattice::database& db, lattice::audit_log_entry entry, size_t target) {
    entry.changed_fields["name"] = lattice::any_property(std::string{});
    db.execute("UPDATE AuditLog SET changedFields=? WHERE id=?", {entry.changed_fields_to_json(), entry.id});
    const auto overhead = materialized_charge(db, entry.id);
    if (overhead > target) throw std::invalid_argument("Fixture budget is smaller than its row metadata");
    entry.changed_fields["name"] = lattice::any_property(std::string(target - overhead, 'x'));
    db.execute("UPDATE AuditLog SET changedFields=? WHERE id=?", {entry.changed_fields_to_json(), entry.id});
}

template <typename Operation>
void expect_row_refusal(Operation operation, const lattice::audit_log_entry& row) {
    try {
        operation();
        FAIL() << "Expected a byte-limit refusal for " << row.global_id;
    } catch (const std::length_error& error) {
        const std::string message = error.what();
        EXPECT_NE(message.find(row.global_id), std::string::npos) << message;
        EXPECT_NE(message.find(row.table_name), std::string::npos) << message;
    }
}
}

// A batch may exceed its ordinary target only for one row; the full envelope
// still counts toward the hard window limit, even with a larger caller target.
TEST(SyncOversizedRow, ExactHardEncodedLimitPassesAloneAndOneByteMoreNamesTheRow) {
    auto row = text_entry("encoded-boundary");
    const size_t overhead = row.to_json().size() + frame_envelope;
    ASSERT_LT(overhead, hard_byte_limit);
    row.changed_fields["text"] = lattice::any_property(std::string(hard_byte_limit - overhead, 'x'));
    const auto frame = lattice::encode_audit_log_prefix({row, text_entry("following")}, 0, 2);
    ASSERT_EQ(frame.count, 1u);
    ASSERT_EQ(frame.entry_charges.size(), 1u);
    EXPECT_EQ(frame.json.size(), hard_byte_limit);
    EXPECT_EQ(frame.entry_charges[0], hard_byte_limit);

    row.changed_fields["text"] = lattice::any_property(std::string(hard_byte_limit - overhead + 1, 'x'));
    expect_row_refusal([&] { (void)lattice::encode_audit_log_prefix({row}, 0, 1); }, row);
    expect_row_refusal([&] {
        (void)lattice::encode_audit_log_prefix({row}, 0, 1, hard_byte_limit + 1024, hard_byte_limit + 1024);
    }, row);
}

// Large valid rows wait for sufficient byte credit without being mistaken for
// a permanently unsendable row, and they never absorb their next neighbour.
TEST(SyncOversizedRow, LargeRowWaitsForCreditThenSendsAlone) {
    const auto row = text_entry("credit-boundary", 5 * 1024 * 1024);
    const auto charge = row.to_json().size() + frame_envelope;
    ASSERT_GT(charge, lattice::sync_max_encoded_message_bytes);
    ASSERT_LT(charge, hard_byte_limit);
    const auto held = lattice::encode_audit_log_prefix({row}, 0, 1,
        lattice::sync_max_encoded_message_bytes, charge - 1);
    EXPECT_EQ(held.count, 0u);
    EXPECT_TRUE(held.json.empty());
    const auto released = lattice::encode_audit_log_prefix({row, text_entry("next")}, 0, 2,
        lattice::sync_max_encoded_message_bytes, charge);
    ASSERT_EQ(released.count, 1u);
    EXPECT_EQ(released.json.size(), charge);
}

// The existing hydration cap stays in place and now identifies its refused
// source row, without changing any synchronization disposition.
TEST(SyncOversizedRow, ExactHydrationLimitPassesAndOneByteMoreNamesTheRow) {
    TempDB path{"hydration-boundary"};
    lattice::lattice_db db{lattice::configuration(path.str())};
    db.add(TestPerson{"", 1, std::nullopt});
    const auto source = lattice::query_audit_log_for_sync(db.db(), "hydration").at(0);
    set_materialized_charge(db.db(), source, hard_byte_limit);
    ASSERT_EQ(materialized_charge(db.db(), source.id), hard_byte_limit);
    bool truncated = false;
    const auto accepted = lattice::query_audit_log_for_sync(db.db(), "hydration", std::nullopt,
        0, 10, std::nullopt, hard_byte_limit, &truncated);
    ASSERT_EQ(accepted.size(), 1u);
    EXPECT_EQ(accepted[0].global_id, source.global_id);
    EXPECT_FALSE(truncated);

    set_materialized_charge(db.db(), source, hard_byte_limit + 1);
    ASSERT_EQ(materialized_charge(db.db(), source.id), hard_byte_limit + 1);
    expect_row_refusal([&] {
        (void)lattice::query_audit_log_for_sync(db.db(), "hydration", std::nullopt,
            0, 10, std::nullopt, hard_byte_limit);
    }, source);
    const auto state = db.db().query("SELECT isSynchronized FROM AuditLog WHERE id=?", {source.id});
    EXPECT_EQ(std::get<int64_t>(state.at(0).at("isSynchronized")), 0);
    const auto dispositions = db.db().query("SELECT COUNT(*) AS n FROM _lattice_sync_state");
    EXPECT_EQ(std::get<int64_t>(dispositions.at(0).at("n")), 0);
}

// Green guard: the established cumulative hydration budget returns complete
// ordered prefixes; a row filling the cap is neither skipped nor combined.
TEST(SyncOversizedRow, HydrationPrefixesNeverSkipTheLargeMiddleRow) {
    TempDB path{"hydration-prefix"};
    lattice::lattice_db db{lattice::configuration(path.str())};
    db.add(TestPerson{"before", 0, std::nullopt});
    db.add(TestPerson{"", 1, std::nullopt});
    db.add(TestPerson{"after", 2, std::nullopt});
    const auto sources = lattice::query_audit_log_for_sync(db.db(), "prefix");
    ASSERT_EQ(sources.size(), 3u);
    set_materialized_charge(db.db(), sources[1], hard_byte_limit);
    int64_t floor = 0;
    for (size_t index = 0; index < sources.size(); ++index) {
        bool truncated = false;
        const auto prefix = lattice::query_audit_log_for_sync(db.db(), "prefix", std::nullopt,
            floor, 10, std::nullopt, hard_byte_limit, &truncated);
        ASSERT_EQ(prefix.size(), 1u) << "prefix " << index;
        EXPECT_EQ(prefix[0].global_id, sources[index].global_id);
        EXPECT_EQ(truncated, index + 1 < sources.size());
        floor = prefix[0].id;
    }
    EXPECT_TRUE(lattice::query_audit_log_for_sync(db.db(), "prefix", std::nullopt,
        floor, 10, std::nullopt, hard_byte_limit).empty());
}
