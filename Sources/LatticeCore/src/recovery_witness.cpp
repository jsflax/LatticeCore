#include "recovery_witness.hpp"
#include "recovery_writer_access.hpp"
#include <algorithm>
#include <cstring>
#include <limits>
#include <string_view>

namespace lattice::detail {
namespace {
constexpr const char* schema =
    "CREATE TABLE _lattice_recovery_witness(id INTEGER PRIMARY KEY CHECK(id=1),"
    "version INTEGER NOT NULL,incarnation BLOB NOT NULL,generation INTEGER NOT NULL) WITHOUT ROWID";
[[noreturn]] void refuse(const char* message) { throw db_error(message); }
void require(bool yes, const char* message) { if (!yes) refuse(message); }
struct statement {
    sqlite3_stmt* p = nullptr;
    statement(sqlite3* db, const char* sql) {
        database::record_statement();
        const auto rc = sqlite3_prepare_v2(db, sql, -1, &p, nullptr);
        if (rc != SQLITE_OK || !p) { sqlite3_finalize(p); p = nullptr; refuse("recovery witness prepare failed"); }
    }
    ~statement() { sqlite3_finalize(p); }
    statement(const statement&) = delete;
    bool next() {
        const auto rc = sqlite3_step(p);
        if (rc == SQLITE_ROW) return true;
        require(rc == SQLITE_DONE, "recovery witness step failed"); return false;
    }
    void done() { require(!next(), "recovery witness write returned rows"); }
    int64_t integer(int i) {
        require(sqlite3_column_type(p, i) == SQLITE_INTEGER, "recovery witness integer type mismatch");
        return sqlite3_column_int64(p, i);
    }
    bool text_is(int i, std::string_view expected) {
        if (sqlite3_column_type(p, i) != SQLITE_TEXT ||
            sqlite3_column_bytes(p, i) != static_cast<int>(expected.size())) return false;
        const auto* bytes = sqlite3_column_text(p, i);
        return bytes && std::memcmp(bytes, expected.data(), expected.size()) == 0;
    }
};
bool existing(sqlite3* db) {
    statement s(db, "SELECT type,tbl_name,sql FROM main.sqlite_schema "
                    "WHERE name='_lattice_recovery_witness' COLLATE NOCASE LIMIT 2");
    if (!s.next()) return false;
    require(s.text_is(0, "table") && s.text_is(1, "_lattice_recovery_witness") && s.text_is(2, schema),
        "recovery witness schema mismatch");
    require(!s.next(), "recovery witness schema alias ambiguity");
    // No unknown trigger/index may suppress or amplify a witness update. The
    // WITHOUT ROWID primary key creates no sqlite_schema autoindex entry.
    statement attached(db, "SELECT 1 FROM main.sqlite_schema "
        "WHERE tbl_name='_lattice_recovery_witness' COLLATE NOCASE AND type!='table' LIMIT 1");
    require(!attached.next(), "recovery witness unexpected trigger/index");
    return true;
}
recovery_witness value(sqlite3* db) {
    // Reject corrupt wide scalars inside SQLite before returning a value to the
    // client. typeof/length inspect type/byte length without copying a huge
    // corrupt BLOB/TEXT value into the result register.
    statement s(db, "SELECT CASE WHEN typeof(id)='integer' THEN id END,"
        "CASE WHEN typeof(version)='integer' THEN version END,"
        "CASE WHEN typeof(incarnation)='blob' AND length(incarnation)=16 THEN incarnation END,"
        "CASE WHEN typeof(generation)='integer' THEN generation END "
        "FROM main._lattice_recovery_witness LIMIT 2");
    require(s.next() && s.integer(0) == 1 && s.integer(1) == 1, "recovery witness missing/version mismatch");
    require(sqlite3_column_type(s.p, 2) == SQLITE_BLOB && sqlite3_column_bytes(s.p, 2) == 16,
        "recovery witness incarnation type/length mismatch");
    const auto* bytes = sqlite3_column_blob(s.p, 2);
    require(bytes != nullptr, "recovery witness incarnation unavailable");
    recovery_witness result;
    std::memcpy(result.incarnation.data(), bytes, result.incarnation.size());
    result.generation = s.integer(3);
    require(result.generation > 0 && !s.next(), "recovery witness generation/singleton mismatch");
    return result;
}
} // namespace

std::optional<recovery_witness> recovery_witness_access::read(database& reader) {
    require(!reader.is_closed(), "recovery witness reader is closed");
    auto* db = reader.internal_handle();
    if (!existing(db)) return std::nullopt;
    return value(db);
}

recovery_witness recovery_witness_access::bump(lattice_db& owner) {
    auto* writer = recovery_writer_access::active_writer(owner);
    require(writer != nullptr, "recovery witness requires owned writer");
    auto* db = writer->internal_handle();
    if (!existing(db)) {
        statement create(db, schema); create.done();
        // randomblob is a store-local collision-resistant incarnation, not an
        // authenticated epoch. A restore may move generation backwards; readers
        // compare the entire tuple for inequality, never only greater-than.
        statement insert(db, "INSERT INTO main._lattice_recovery_witness VALUES(1,1,randomblob(16),1)");
        insert.done();
        require(sqlite3_changes(db) == 1, "recovery witness insertion ignored");
        require(existing(db), "recovery witness creation missing");
        return value(db);
    }
    const auto before = value(db);
    require(before.generation < std::numeric_limits<int64_t>::max(), "recovery witness generation exhausted");
    statement update(db, "UPDATE main._lattice_recovery_witness SET generation=generation+1 WHERE id=1 AND generation=?");
    require(sqlite3_bind_int64(update.p, 1, before.generation) == SQLITE_OK, "recovery witness bind failed");
    update.done();
    require(sqlite3_changes(db) == 1, "recovery witness update ignored");
    const auto after = value(db);
    require(after.incarnation == before.incarnation && after.generation == before.generation + 1,
        "recovery witness update changed unexpectedly");
    return after;
}
std::optional<recovery_witness> read_recovery_witness(database& reader) { return recovery_witness_access::read(reader); }
recovery_witness bump_recovery_witness(lattice_db& owner) { return recovery_witness_access::bump(owner); }
} // namespace lattice::detail
