#include "receive_delivery_guard.hpp"
#include "recovery_writer_access.hpp"
#include <lattice/lattice.hpp>
#include <cstring>
#include <limits>
#include <memory>

namespace lattice::detail {
namespace receive_guard_test_hooks { thread_local std::function<void()> after_intake_commit; }
namespace {
constexpr receive_guard_limits caps{};
constexpr const char* store_sql = "CREATE TABLE _lattice_receive_guard_store (id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,legacy_origin INTEGER NOT NULL,last_incarnation INTEGER NOT NULL,channels INTEGER NOT NULL,channel_bytes INTEGER NOT NULL,capacity_refused INTEGER NOT NULL) WITHOUT ROWID";
constexpr const char* channel_sql = "CREATE TABLE _lattice_receive_guard (channel BLOB PRIMARY KEY NOT NULL CHECK(typeof(channel)='blob' AND length(channel)>0),incarnation INTEGER UNIQUE NOT NULL,generation INTEGER NOT NULL,state INTEGER NOT NULL,reason INTEGER NOT NULL,checkpoint BLOB) WITHOUT ROWID";
constexpr const char* index_sql = "CREATE INDEX _lattice_receive_guard_state ON _lattice_receive_guard(state)";
[[noreturn]] void refuse(const char* message) { throw db_error(std::string("receive guard: ") + message); }
void key(const std::string& s, int64_t bound) {
    if (s.empty() || s.size() > static_cast<uint64_t>(bound)) refuse("opaque key exceeds qualification bound");
}
struct statement {
    sqlite3* db; std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> p;
    statement(sqlite3* h, const char* sql) : db(h), p(nullptr, &sqlite3_finalize) {
        database::record_statement(); sqlite3_stmt* raw = nullptr;
        const int rc = sqlite3_prepare_v2(db, sql, -1, &raw, nullptr); p.reset(raw);
        if (rc != SQLITE_OK || !raw) refuse("prepare failed");
    }
    void integer(int i, int64_t v) { if (sqlite3_bind_int64(p.get(), i, v) != SQLITE_OK) refuse("integer bind failed"); }
    void bytes(int i, const std::string& s) {
        if (s.size() > static_cast<size_t>(std::numeric_limits<int>::max()) ||
            sqlite3_bind_blob(p.get(), i, s.data(), static_cast<int>(s.size()), SQLITE_TRANSIENT) != SQLITE_OK) refuse("BLOB bind failed");
    }
    void text(int i, const std::string& s) {
        if (s.size() > static_cast<size_t>(std::numeric_limits<int>::max()) ||
            sqlite3_bind_text(p.get(), i, s.data(), static_cast<int>(s.size()), SQLITE_TRANSIENT) != SQLITE_OK) refuse("TEXT bind failed");
    }
    void optional(int i, const std::optional<std::string>& s) {
        if (s) bytes(i, *s); else if (sqlite3_bind_null(p.get(), i) != SQLITE_OK) refuse("NULL bind failed");
    }
    int step() { const int rc = sqlite3_step(p.get()); if (rc != SQLITE_ROW && rc != SQLITE_DONE) refuse("step failed"); return rc; }
    void done() { if (step() != SQLITE_DONE) refuse("unexpected result row"); }
    int64_t number(int c) {
        if (sqlite3_column_type(p.get(), c) != SQLITE_INTEGER) refuse("expected integer state");
        return sqlite3_column_int64(p.get(), c);
    }
    std::optional<std::string> optional_bytes(int c, int64_t limit) {
        const int type = sqlite3_column_type(p.get(), c);
        if (type == SQLITE_NULL) return {};
        if (type != SQLITE_BLOB) refuse("expected opaque BLOB checkpoint");
        const int n = sqlite3_column_bytes(p.get(), c);
        if (n <= 0 || n > limit) refuse("checkpoint length out of bounds");
        const auto* value = static_cast<const char*>(sqlite3_column_blob(p.get(), c));
        if (!value) refuse("checkpoint read failed");
        return std::string(value, static_cast<size_t>(n));
    }
};
struct store_state {
    bool legacy = false, overflow = false;
    int64_t sequence = 0, count = 0, bytes = 0, version = 1;
};
bool schema(sqlite3* h, bool allow_absent = false) {
    statement q(h, "SELECT name,sql FROM main.sqlite_master WHERE name IN ('_lattice_receive_guard_store','_lattice_receive_guard','_lattice_receive_guard_state') ORDER BY name LIMIT 4");
    const char* names[] = {"_lattice_receive_guard", "_lattice_receive_guard_state", "_lattice_receive_guard_store"};
    const char* definitions[] = {channel_sql, index_sql, store_sql};
    int i = 0;
    while (q.step() == SQLITE_ROW) {
        if (i == 3 || sqlite3_column_type(q.p.get(), 0) != SQLITE_TEXT || sqlite3_column_type(q.p.get(), 1) != SQLITE_TEXT)
            refuse("malformed schema inventory");
        for (int c = 0; c < 2; ++c) {
            const char* expected = c == 0 ? names[i] : definitions[i];
            const int n = sqlite3_column_bytes(q.p.get(), c);
            const auto* value = sqlite3_column_text(q.p.get(), c);
            if (n != static_cast<int>(std::strlen(expected)) || !value || std::memcmp(value, expected, n)) refuse("unsupported schema or index");
        }
        ++i;
    }
    if (i == 0 && allow_absent) return false;
    if (i != 3) refuse("missing or partial schema");
    return true;
}
store_state store(sqlite3* h) {
    statement q(h, "SELECT id,version,legacy_origin,last_incarnation,channels,channel_bytes,capacity_refused FROM main._lattice_receive_guard_store LIMIT 2");
    if (q.step() != SQLITE_ROW || q.number(0) != 1 || (q.number(1) != 1 && q.number(1) != 2)) refuse("missing or unsupported store version");
    const auto legacy = q.number(2), overflow = q.number(6);
    store_state s{legacy == 1, overflow == 1, q.number(3), q.number(4), q.number(5), q.number(1)};
    if ((legacy != 0 && legacy != 1) || (overflow != 0 && overflow != 1) || s.sequence < 0 ||
        s.count < 0 || s.count > caps.channels || s.bytes < 0 || s.bytes > caps.channel_bytes || s.sequence < s.count)
        refuse("invalid bounded store counters");
    q.done(); return s;
}
receive_guard_snapshot read_row(sqlite3* h, const std::string& channel) {
    key(channel, caps.key_bytes); schema(h); const auto s = store(h);
    receive_guard_snapshot out; out.channel = channel; out.legacy_origin = s.legacy; out.capacity_refused = s.overflow;
    out.store_version = s.version; out.store_incarnation = s.sequence; out.store_channels = s.count; out.store_channel_bytes = s.bytes;
    if (s.legacy) { out.state = receive_guard_state::recovery_required; out.reason = receive_guard_reason::legacy_unverified; }
    statement q(h, "SELECT incarnation,generation,state,reason,checkpoint FROM main._lattice_receive_guard WHERE channel=?"); q.bytes(1, channel);
    if (q.step() == SQLITE_DONE) return out;
    out.present = true; out.incarnation = q.number(0); out.generation = q.number(1);
    const auto state = q.number(2), reason = q.number(3);
    if (out.incarnation <= 0 || out.incarnation > s.sequence || out.generation <= 0 || state < 0 || state > (s.version == 2 ? 4 : 3) || reason < 0 || reason > 4)
        refuse("malformed channel state");
    out.state = static_cast<receive_guard_state>(state); out.reason = static_cast<receive_guard_reason>(reason);
    out.checkpoint = q.optional_bytes(4, caps.checkpoint_bytes); q.done();
    if ((state <= 1 && reason != 0) || (state == 2 && (reason == 0 || reason == 4)) || (state == 3 && reason == 0)) refuse("inconsistent state/reason");
    if (state == 4 && (s.legacy || reason != 0 || out.checkpoint)) refuse("inconsistent canonical receive state");
    return out;
}
bool same_store(const store_state& a, const store_state& b) {
    return a.legacy == b.legacy && a.overflow == b.overflow && a.sequence == b.sequence && a.count == b.count && a.bytes == b.bytes && a.version == b.version;
}
void save_store(sqlite3* h, const store_state& before, const store_state& after) {
    statement q(h, "UPDATE main._lattice_receive_guard_store SET last_incarnation=?,channels=?,channel_bytes=?,capacity_refused=? WHERE id=1 AND last_incarnation=? AND channels=? AND channel_bytes=? AND capacity_refused=?");
    q.integer(1, after.sequence); q.integer(2, after.count); q.integer(3, after.bytes); q.integer(4, after.overflow);
    q.integer(5, before.sequence); q.integer(6, before.count); q.integer(7, before.bytes); q.integer(8, before.overflow); q.done();
    if (sqlite3_changes64(h) != 1 || !same_store(store(h), after)) refuse("store write postimage mismatch");
}
void save_row(sqlite3* h, const receive_guard_snapshot& before, const receive_guard_snapshot& after) {
    statement q(h, before.present ?
        "UPDATE main._lattice_receive_guard SET generation=?,state=?,reason=?,checkpoint=? WHERE channel=? AND incarnation=? AND generation=?" :
        "INSERT INTO main._lattice_receive_guard(generation,state,reason,checkpoint,channel,incarnation) VALUES(?,?,?,?,?,?)");
    q.integer(1, after.generation); q.integer(2, static_cast<int64_t>(after.state)); q.integer(3, static_cast<int64_t>(after.reason)); q.optional(4, after.checkpoint); q.bytes(5, after.channel); q.integer(6, after.incarnation);
    if (before.present) q.integer(7, before.generation);
    q.done();
    if (sqlite3_changes64(h) != 1 || !(read_row(h, after.channel) == after)) refuse("channel write postimage mismatch");
}
bool allocate(sqlite3* h, receive_guard_snapshot& row) {
    if (row.present) return true;
    const auto s = store(h); auto next = s;
    if (s.overflow || s.count == caps.channels || static_cast<int64_t>(row.channel.size()) > caps.channel_bytes - s.bytes) {
        next.overflow = true; save_store(h, s, next); row.capacity_refused = true; return false;
    }
    if (s.sequence == std::numeric_limits<int64_t>::max()) refuse("incarnation exhausted");
    next.sequence++; next.count++; next.bytes += static_cast<int64_t>(row.channel.size()); save_store(h, s, next);
    row.incarnation = next.sequence; row.store_incarnation = next.sequence;
    row.store_channels = next.count; row.store_channel_bytes = next.bytes; return true;
}
void mirror(sqlite3* h, const receive_guard_snapshot& row) {
    // A slot may have expired. Recreate only its neutral upload defaults; the
    // guard owns the nullable receive checkpoint. This does not revive retired
    // channels (admission rejects those before reaching this function).
    statement insert(h, "INSERT INTO main._lattice_replication_slots(sync_id) VALUES(?) ON CONFLICT(sync_id) DO NOTHING"); insert.text(1, row.channel); insert.done();
    // A committed intake is real channel activity even if its later effects
    // fail. Activity only prevents stale-slot eviction; it is not an ACK or
    // proof that the nullable checkpoint advanced. Failed intake rolls it back.
    statement update(h, "UPDATE main._lattice_replication_slots SET last_received_event_id=?,last_active_at=datetime('now') WHERE sync_id=?");
    if (row.checkpoint) update.text(1, *row.checkpoint); else update.optional(1, {});
    update.text(2, row.channel); update.done();
    if (sqlite3_changes64(h) != 1) refuse("slot update refused");
}
void verify_mirror(sqlite3* h, const receive_guard_snapshot& row) {
    statement q(h, "SELECT last_received_event_id FROM main._lattice_replication_slots WHERE sync_id=? LIMIT 2"); q.text(1, row.channel);
    if (q.step() != SQLITE_ROW) refuse("missing receive slot mirror");
    if (!row.checkpoint) { if (sqlite3_column_type(q.p.get(), 0) != SQLITE_NULL) refuse("NULL checkpoint mirror mismatch"); }
    else {
        if (sqlite3_column_type(q.p.get(), 0) != SQLITE_TEXT) refuse("checkpoint mirror is not TEXT");
        const auto* value = sqlite3_column_text(q.p.get(), 0); const int n = sqlite3_column_bytes(q.p.get(), 0);
        if (n != static_cast<int>(row.checkpoint->size()) || !value || std::memcmp(value, row.checkpoint->data(), n)) refuse("checkpoint mirror mismatch");
    }
    q.done();
}
}
bool receive_guard_snapshot::operator==(const receive_guard_snapshot& o) const {
    return present == o.present && legacy_origin == o.legacy_origin && capacity_refused == o.capacity_refused && channel == o.channel && incarnation == o.incarnation && generation == o.generation && store_version == o.store_version && store_incarnation == o.store_incarnation && store_channels == o.store_channels && store_channel_bytes == o.store_channel_bytes && state == o.state && reason == o.reason && checkpoint == o.checkpoint;
}
sqlite3* receive_delivery_guard_access::owned(lattice_db& owner, database& writer) {
    auto* h = writer.internal_handle(); auto* hook = writer.lattice_update_hook_context_.get();
    if (!h || writer.channel_reset_unsettled_.load() || !hook || hook->owner != &owner || hook->connection != h ||
        !hook->sync_chunk || hook->sync_chunk->state != database::sync_apply_chunk_state::phase::active ||
        sqlite3_get_autocommit(h) != 0 || sqlite3_txn_state(h, "main") != SQLITE_TXN_WRITE ||
        (!database::maintenance_scope::active_for(h) &&
         !recovery_writer_access::active_channel_reset_for(owner,writer) &&
         recovery_writer_access::active_writer(owner) != &writer))
        refuse("requires the actual owned sync/reset transaction");
    return h;
}
void receive_delivery_guard_access::initialize_schema(database& writer, bool legacy) {
    auto* h = writer.internal_handle();
    if (sqlite3_get_autocommit(h) || sqlite3_txn_state(h, "main") != SQLITE_TXN_WRITE) refuse("schema initialization requires owned bootstrap WRITE");
    if (schema(h, true)) {
        const auto s = store(h);
        statement count(h, "SELECT COUNT(*),COALESCE(SUM(length(channel)),0) FROM (SELECT channel FROM main._lattice_receive_guard LIMIT 257)");
        if (count.step() != SQLITE_ROW || count.number(0) != s.count || count.number(1) != s.bytes) refuse("guard count/byte audit mismatch");
        count.done();
        statement rows(h, "SELECT channel FROM main._lattice_receive_guard LIMIT 257");
        while (rows.step() == SQLITE_ROW) {
            const auto channel = rows.optional_bytes(0, caps.key_bytes);
            if (!channel) refuse("NULL channel key");
            (void)read_row(h, *channel);
        }
        return;
    }
    statement(h, store_sql).done(); statement(h, channel_sql).done(); statement(h, index_sql).done();
    statement q(h, "INSERT INTO main._lattice_receive_guard_store VALUES(1,1,?,0,0,0,0)"); q.integer(1, legacy); q.done();
    schema(h); const auto s = store(h);
    if (s.legacy != legacy || s.sequence || s.count || s.bytes || s.overflow) refuse("bootstrap postimage mismatch");
}
receive_guard_token receive_delivery_guard_access::begin(lattice_db& owner, database& writer, const std::string& channel) {
    auto* h = owned(owner, writer); auto before = read_row(h, channel); auto next = before;
    if (before.state == receive_guard_state::retired) refuse("channel is retired; explicit recovery binding required");
    if (before.state == receive_guard_state::canonical_installed) refuse("canonical channel refuses legacy receive admission");
    if (!allocate(h, next)) return {next, false, true};
    if (before.generation == std::numeric_limits<int64_t>::max()) refuse("delivery generation exhausted");
    const bool advance = before.state == receive_guard_state::idle;
    next.present = true; next.generation++;
    if (advance) { next.state = receive_guard_state::in_progress; next.reason = receive_guard_reason::none; }
    else if (before.state == receive_guard_state::in_progress) { next.state = receive_guard_state::recovery_required; next.reason = receive_guard_reason::interrupted; }
    save_row(h, before, next); mirror(h, next); verify_owned(owner, writer, next); verify_mirror(h, next);
    return {next, advance, false};
}
receive_guard_snapshot receive_delivery_guard_access::require_current(lattice_db& owner, database& writer, const receive_guard_token& token) {
    const auto row = read_row(owned(owner, writer), token.admitted.channel);
    if (!row.present || row.incarnation != token.admitted.incarnation || row.generation != token.admitted.generation || row.state == receive_guard_state::retired || row.state == receive_guard_state::canonical_installed || token.capacity_refused)
        refuse("stale delivery token");
    return row;
}
receive_guard_snapshot receive_delivery_guard_access::finish(lattice_db& owner, database& writer, const receive_guard_token& token,
    const receive_guard_snapshot& before, const std::optional<std::string>& prefix, bool failed, bool final_chunk) {
    auto* h = owned(owner, writer);
    if (!(require_current(owner, writer, token) == before)) refuse("chunk effects changed the admitted receive state");
    auto next = before;
    if (prefix) key(*prefix, caps.checkpoint_bytes);
    if (token.may_advance && before.state == receive_guard_state::in_progress) {
        if (prefix) next.checkpoint = prefix;
        if (failed) { next.state = receive_guard_state::recovery_required; next.reason = receive_guard_reason::entry_failed; }
        else if (final_chunk) next.state = receive_guard_state::idle;
    }
    save_row(h, before, next); mirror(h, next); return next;
}
receive_guard_snapshot receive_delivery_guard_access::read_owned(lattice_db& owner, database& writer, const std::string& channel) { return read_row(owned(owner, writer), channel); }
void receive_delivery_guard_access::verify_owned(lattice_db& owner, database& writer, const receive_guard_snapshot& expected) {
    const auto actual = read_row(owned(owner, writer), expected.channel);
    if (!(actual == expected)) refuse("final receive state postimage mismatch");
    if (expected.present && expected.state != receive_guard_state::retired) verify_mirror(writer.internal_handle(), expected);
}
receive_guard_snapshot receive_delivery_guard_access::retire(lattice_db& owner, database& writer, const receive_guard_snapshot& before) {
    auto* h = owned(owner, writer);
    // Outbound removal may fire triggers. It cannot establish a new baseline
    // that downgrades receive ambiguity, clears its cursor, or changes charges.
    // The slot is intentionally gone here; only compare guard/store custody.
    if (!(read_row(h, before.channel) == before)) refuse("channel removal changed its captured receive state");
    auto next = before;
    // No receive admission ever committed for an absent guard, so there is no
    // guard token to invalidate. Do not turn outbound-only channel retirement
    // into invented receive uncertainty or a permanent history pin.
    if (!before.present) return before;
    if (before.generation == std::numeric_limits<int64_t>::max()) refuse("retirement generation exhausted");
    next.generation++; next.state = receive_guard_state::retired;
    if (before.state == receive_guard_state::idle || before.state == receive_guard_state::canonical_installed) next.reason = receive_guard_reason::retired;
    else if (before.state == receive_guard_state::in_progress) next.reason = receive_guard_reason::interrupted;
    save_row(h, before, next); return next;
}
receive_guard_snapshot receive_delivery_guard_access::read(lattice_db& owner, const std::string& channel) {
    key(channel, caps.key_bytes); std::shared_ptr<database> writer;
    { std::lock_guard<std::mutex> lock(owner.connection_ownership_mutex_); if (owner.closed_.load()) refuse("controller owner closed"); writer = owner.db_; }
    if (!writer) refuse("controller writer unavailable");
    database::maintenance_scope::probe_before_store_gate(*writer);
    database::maintenance_scope scope(*writer); auto* h = writer->internal_handle();
    statement(h, "BEGIN").done();
    try { auto value = read_row(h, channel); statement(h, "COMMIT").done(); return value; }
    catch (...) { const auto error = std::current_exception(); try { statement(h, "ROLLBACK").done(); } catch (...) { writer->closed_.store(true); } std::rethrow_exception(error); }
}
namespace {
receive_guard_snapshot canonical_postimage(const receive_guard_snapshot& before) {
    if (!before.present || before.legacy_origin || before.capacity_refused ||
        (before.store_version != 1 && before.store_version != 2) ||
        before.state == receive_guard_state::retired || before.incarnation <= 0 || before.generation <= 0 ||
        before.generation == std::numeric_limits<int64_t>::max())
        refuse("canonical completion requires a current modern nonretired guard");
    auto next = before; next.store_version = 2; next.generation++;
    next.state = receive_guard_state::canonical_installed; next.reason = receive_guard_reason::none;
    next.checkpoint.reset(); return next;
}
void require_canonical_metadata_without_triggers(sqlite3* h) {
    // TEMP triggers may target main tables; inspect both namespaces before
    // any completion write. Table-name comparison follows SQLite casing.
    statement q(h, "SELECT 1 FROM main.sqlite_schema WHERE type='trigger' AND tbl_name COLLATE NOCASE IN ('_lattice_receive_guard_store','_lattice_receive_guard','_lattice_replication_slots') "
        "UNION ALL SELECT 1 FROM temp.sqlite_schema WHERE type='trigger' AND tbl_name COLLATE NOCASE IN ('_lattice_receive_guard_store','_lattice_receive_guard','_lattice_replication_slots') LIMIT 1");
    if (q.step() != SQLITE_DONE) refuse("canonical completion metadata triggers are unsupported");
}
}
std::optional<std::string> receive_delivery_guard_access::legacy_checkpoint(lattice_db& owner, const std::string& channel) {
    const auto current = read(owner, channel);
    if (current.state == receive_guard_state::canonical_installed) refuse("canonical channel has no legacy checkpoint");
    return current.checkpoint;
}
receive_guard_snapshot receive_delivery_guard_access::complete_canonical(lattice_db& owner, database& writer,
    const receive_guard_snapshot& before) {
    auto* h = owned(owner, writer); const auto next = canonical_postimage(before);
    require_canonical_metadata_without_triggers(h);
    verify_owned(owner, writer, before);
    if (before.store_version == 1) {
        const auto prior = store(h); auto expected = prior; expected.version = 2;
        statement q(h, "UPDATE main._lattice_receive_guard_store SET version=2 WHERE id=1 AND version=1"); q.done();
        if (sqlite3_changes64(h) != 1 || !same_store(store(h), expected)) refuse("canonical store upgrade postimage mismatch");
    }
    save_row(h, before, next); mirror(h, next); verify_owned(owner, writer, next); return next;
}
receive_guard_snapshot receive_delivery_guard_access::verify_canonical_completed(lattice_db& owner, database& writer,
    const receive_guard_snapshot& before) {
    auto* h = owned(owner, writer); auto expected = canonical_postimage(before);
    const auto current = read_row(h, before.channel);
    // Other channels may legitimately be admitted between COMMIT and an exact
    // retry. Their counters are not this channel's installation identity.
    expected.store_incarnation = current.store_incarnation;
    expected.store_channels = current.store_channels;
    expected.store_channel_bytes = current.store_channel_bytes;
    if (!(current == expected)) refuse("canonical installed guard differs from exact retry");
    verify_owned(owner, writer, expected); return current;
}
bool receive_delivery_guard_access::manages_cursor(database& writer) { auto* h = writer.internal_handle(); if (!schema(h, true)) return false; (void)store(h); return true; }
void receive_delivery_guard_access::require_history_unblocked(database& writer) {
    auto* h = writer.internal_handle(); if (!schema(h, true)) return;
    if (store(h).overflow) refuse("history evidence pinned by receive capacity refusal");
    statement q(h, "SELECT 1 FROM main._lattice_receive_guard WHERE state IN (1,2) OR (state=3 AND reason!=4) LIMIT 1");
    if (q.step() == SQLITE_ROW) refuse("history evidence pinned by unresolved or retired receive state");
}
void initialize_receive_guard_schema(database& writer, bool legacy) { receive_delivery_guard_access::initialize_schema(writer, legacy); }
bool receive_guard_manages_cursor(database& writer) { return receive_delivery_guard_access::manages_cursor(writer); }
void require_receive_guard_history_unblocked(database& writer) { receive_delivery_guard_access::require_history_unblocked(writer); }
} // namespace lattice::detail
