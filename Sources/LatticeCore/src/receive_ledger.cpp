#include "receive_ledger.hpp"
#include <lattice/lattice.hpp>
#include <exception>
#include <limits>
#include <map>

namespace lattice::detail {
namespace {
using code = receive_ledger_error_code;
using row = database::row_t;
using blob = std::vector<uint8_t>;
[[noreturn]] void fail(code c, const char* message) { throw receive_ledger_error(c, message); }
int64_t integer(const row& r, const char* key) {
    auto i = r.find(key);
    if (i == r.end() || !std::holds_alternative<int64_t>(i->second))
        fail(code::corrupt_state, "receive ledger expected an integer");
    return std::get<int64_t>(i->second);
}
int64_t positive_sequence(const row& r, const char* key, bool zero_allowed = false) {
    const auto n = integer(r, key);
    if (n < (zero_allowed ? 0 : 1)) fail(code::corrupt_state, "receive ledger invalid sequence");
    return n;
}
blob bytes(const std::string& value) { return blob(value.begin(), value.end()); }
std::string string_bytes(const column_value_t& value) {
    if (!std::holds_alternative<blob>(value)) fail(code::corrupt_state, "receive ledger expected encoded ID bytes");
    const auto& b = std::get<blob>(value);
    if (b.empty()) fail(code::corrupt_state, "receive ledger empty encoded ID");
    return std::string(b.begin(), b.end());
}
int64_t size(const std::string& id) {
    if (id.empty() || id.size() > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))
        fail(code::invalid_argument, "receive ledger requires a nonempty representable ID");
    return static_cast<int64_t>(id.size());
}
bool fits(int64_t existing, int64_t added, int64_t limit) {
    return existing >= 0 && added >= 0 && existing <= limit && added <= limit - existing;
}
bool valid_disposition(int64_t d) { return d >= 0 && d <= 2; }
bool valid_acceptance(int64_t a) { return a >= 0 && a <= 3; }
void changed(database& db, int64_t expected = 1) {
    if (db.changes() != expected) throw db_error("receive ledger write did not change its expected rows");
}
receive_identity_state parse_identity(const row& r) {
    auto d = integer(r, "disposition"), a = integer(r, "acceptance");
    if (!valid_disposition(d) || !valid_acceptance(a) || (a == 3 && d == 0) ||
        ((a == 1 || a == 2) && d != 0)) fail(code::corrupt_state, "receive ledger invalid acceptance/disposition");
    return {static_cast<receive_intake_disposition>(d), static_cast<receive_acceptance>(a)};
}
template<typename Function> auto atomic(database& db, Function&& function) {
    db.execute("SAVEPOINT lattice_receive_primitive");
    try {
        auto result = function();
        db.execute("RELEASE lattice_receive_primitive");
        return result;
    } catch (...) {
        const auto original = std::current_exception();
        if (!db.is_in_transaction()) std::rethrow_exception(original);
        try {
            db.execute("ROLLBACK TO lattice_receive_primitive");
            db.execute("RELEASE lattice_receive_primitive");
        } catch (...) {
            // The caller must abort its owned unit; never call this a safe
            // partial admission or swallow inability to restore the boundary.
            throw receive_ledger_error(code::cleanup_failed,
                "receive ledger operation failed and savepoint cleanup was unavailable; abort owning transaction",
                original,std::current_exception());
        }
        std::rethrow_exception(original);
    }
}
const char* count_ids = "SELECT COUNT(*) AS n, COALESCE(SUM(length(event_id)),0) AS bytes, "
    "COALESCE(SUM(acceptance=0),0) AS pending FROM main._lattice_receive_identity";
}

receive_ledger::receive_ledger(lattice_db& owner, receive_ledger_limits limits)
    : owner_(owner), limits_(limits) {
    const int64_t values[] = {limits.channels, limits.channel_id_bytes, limits.identities,
        limits.identity_bytes, limits.identities_per_channel, limits.identity_bytes_per_channel,
        limits.checkpoint_bytes_per_channel};
    for (auto n : values) if (n < 0) fail(code::invalid_argument, "receive ledger limits must be nonnegative");
}
database& receive_ledger::connection() const {
    if (owner_.is_closed() || !owner_.owns_write_transaction())
        fail(code::transaction_required, "receive ledger requires this thread's owned active write transaction");
    auto& db = owner_.db();
    if (db.is_closed() || sqlite3_get_autocommit(db.handle()) != 0 ||
        sqlite3_txn_state(db.handle(), "main") != SQLITE_TXN_WRITE)
        fail(code::transaction_required, "receive ledger requires an active main write transaction");
    return db;
}
int64_t receive_ledger::check_schema() const {
    auto& db = connection();
    const auto rows = db.query("SELECT * FROM main._lattice_receive_store");
    if (rows.size() != 1 || integer(rows[0], "id") != 1 || integer(rows[0], "version") != 1)
        fail(code::corrupt_state, "receive ledger missing or unsupported store metadata");
    const auto& r = rows[0];
    const auto last_incarnation = positive_sequence(r, "last_incarnation", true);
    const auto overflow = integer(r, "channel_overflow");
    if (overflow != 0 && overflow != 1) fail(code::corrupt_state, "receive ledger invalid store overflow");
    const receive_ledger_limits recorded{integer(r,"max_channels"), integer(r,"max_channel_bytes"),
        integer(r,"max_ids"), integer(r,"max_id_bytes"), integer(r,"max_channel_ids"),
        integer(r,"max_channel_id_bytes"), integer(r,"max_checkpoint_bytes")};
    if (!(recorded == limits_)) fail(code::limits_mismatch, "receive ledger explicit limits differ from durable configuration");
    return last_incarnation;
}
void receive_ledger::audit() const {
    check_schema();
    auto& db = connection();
    // Do not count malformed text IDs as fewer bytes or accept orphan evidence.
    if (!db.query("SELECT 1 FROM main._lattice_receive_channel WHERE typeof(channel)!='blob' OR length(channel)=0 "
        "OR typeof(incarnation)!='integer' OR incarnation<=0 OR typeof(generation)!='integer' OR generation<0 "
        "OR incarnation>(SELECT last_incarnation FROM main._lattice_receive_store WHERE id=1) "
        "OR initialized!=1 OR typeof(initialized)!='integer' OR overflow NOT IN(0,1) OR typeof(overflow)!='integer' "
        "OR (checkpoint IS NOT NULL AND (typeof(checkpoint)!='blob' OR length(checkpoint)=0 OR length(checkpoint)>?)) LIMIT 1",
        {limits_.checkpoint_bytes_per_channel}).empty()) fail(code::corrupt_state, "receive ledger malformed channel state");
    if (!db.query("SELECT 1 FROM main._lattice_receive_identity i LEFT JOIN main._lattice_receive_channel c USING(incarnation) "
        "WHERE c.incarnation IS NULL OR typeof(event_id)!='blob' OR length(event_id)=0 "
        "OR typeof(disposition)!='integer' OR disposition NOT IN(0,1,2) "
        "OR typeof(acceptance)!='integer' OR acceptance NOT IN(0,1,2,3) "
        "OR (acceptance=3 AND disposition=0) OR (acceptance IN(1,2) AND disposition!=0) LIMIT 1").empty())
        fail(code::corrupt_state, "receive ledger malformed identity state");
    (void)usage();
}
void receive_ledger::initialize() {
    auto& db = connection();
    const auto present = db.query("SELECT name FROM main.sqlite_master WHERE name IN "
        "('_lattice_receive_store','_lattice_receive_channel','_lattice_receive_identity')");
    if (!present.empty()) {
        if (present.size() != 3) fail(code::corrupt_state, "receive ledger partial schema; migration refused");
        audit();
        return;
    }
    atomic(db, [&] {
        connection().execute("CREATE TABLE main._lattice_receive_store (id INTEGER PRIMARY KEY CHECK(id=1), "
            "version INTEGER NOT NULL, last_incarnation INTEGER NOT NULL CHECK(last_incarnation>=0), "
            "channel_overflow INTEGER NOT NULL CHECK(channel_overflow IN(0,1)), "
            "max_channels INTEGER NOT NULL, max_channel_bytes INTEGER NOT NULL, max_ids INTEGER NOT NULL, "
            "max_id_bytes INTEGER NOT NULL, max_channel_ids INTEGER NOT NULL, max_channel_id_bytes INTEGER NOT NULL, "
            "max_checkpoint_bytes INTEGER NOT NULL) WITHOUT ROWID");
        connection().execute("CREATE TABLE main._lattice_receive_channel (channel BLOB PRIMARY KEY NOT NULL CHECK(typeof(channel)='blob' AND length(channel)>0), "
            "incarnation INTEGER UNIQUE NOT NULL CHECK(incarnation>0), generation INTEGER NOT NULL CHECK(generation>=0), "
            "initialized INTEGER NOT NULL CHECK(initialized=1), checkpoint BLOB, overflow INTEGER NOT NULL CHECK(overflow IN(0,1))) WITHOUT ROWID");
        connection().execute("CREATE TABLE main._lattice_receive_identity (incarnation INTEGER NOT NULL REFERENCES _lattice_receive_channel(incarnation), "
            "event_id BLOB NOT NULL CHECK(typeof(event_id)='blob' AND length(event_id)>0), "
            "disposition INTEGER NOT NULL CHECK(disposition IN(0,1,2)), acceptance INTEGER NOT NULL CHECK(acceptance IN(0,1,2,3)), "
            "PRIMARY KEY(incarnation,event_id)) WITHOUT ROWID");
        connection().execute("INSERT INTO main._lattice_receive_store VALUES(1,1,0,0,?,?,?,?,?,?,?)",
            {limits_.channels, limits_.channel_id_bytes, limits_.identities, limits_.identity_bytes,
             limits_.identities_per_channel, limits_.identity_bytes_per_channel, limits_.checkpoint_bytes_per_channel});
        return true;
    });
}
receive_store_usage receive_ledger::usage() const {
    check_schema();
    auto& db = connection();
    auto channels = db.query("SELECT COUNT(*) AS n, COALESCE(SUM(length(channel)),0) AS bytes FROM main._lattice_receive_channel").at(0);
    auto ids = db.query(count_ids).at(0);
    receive_store_usage u{integer(channels,"n"),integer(channels,"bytes"),integer(ids,"n"),integer(ids,"bytes"),
        integer(db.query("SELECT channel_overflow FROM main._lattice_receive_store WHERE id=1").at(0),"channel_overflow") == 1};
    if (!fits(u.channels,0,limits_.channels) || !fits(u.channel_id_bytes,0,limits_.channel_id_bytes) ||
        !fits(u.identities,0,limits_.identities) || !fits(u.identity_bytes,0,limits_.identity_bytes))
        fail(code::corrupt_state, "receive ledger durable usage exceeds configured budget");
    if (!db.query("SELECT incarnation FROM main._lattice_receive_identity GROUP BY incarnation "
        "HAVING COUNT(*)>? OR SUM(length(event_id))>? LIMIT 1",
        {limits_.identities_per_channel,limits_.identity_bytes_per_channel}).empty())
        fail(code::corrupt_state, "receive ledger durable channel usage exceeds configured budget");
    return u;
}
receive_ledger_snapshot receive_ledger::read(const std::string& channel) const {
    const auto n = size(channel);
    if (n > limits_.channel_id_bytes) fail(code::invalid_argument,"receive ledger channel ID exceeds configured store byte budget");
    auto& db = connection();
    const auto tables = db.query("SELECT name FROM main.sqlite_master WHERE name IN "
        "('_lattice_receive_store','_lattice_receive_channel','_lattice_receive_identity')");
    if (tables.empty()) return {};
    if (tables.size() != 3) fail(code::corrupt_state, "receive ledger partial schema");
    auto result = channel_row(channel);
    if (result.kind == receive_checkpoint_kind::absent) return result;
    auto totals = db.query(std::string(count_ids) + " WHERE incarnation=?", {result.token.incarnation}).at(0);
    result.identities = integer(totals,"n"); result.identity_bytes = integer(totals,"bytes"); result.pending = integer(totals,"pending");
    if (!fits(result.identities,0,limits_.identities_per_channel) || !fits(result.identity_bytes,0,limits_.identity_bytes_per_channel))
        fail(code::corrupt_state, "receive ledger channel exceeds configured budget");
    return result;
}
receive_ledger_snapshot receive_ledger::channel_row(const std::string& channel) const {
    const auto n = size(channel);
    if (n > limits_.channel_id_bytes) fail(code::invalid_argument,"receive ledger channel ID exceeds configured store byte budget");
    const auto last_incarnation = check_schema();
    auto& db = connection();
    auto rows = db.query("SELECT * FROM main._lattice_receive_channel WHERE channel=?", {bytes(channel)});
    if (rows.empty()) return {};
    if (rows.size() != 1) fail(code::corrupt_state, "receive ledger duplicate channel");
    const auto& r = rows[0];
    receive_ledger_snapshot result;
    result.token = {channel,positive_sequence(r,"incarnation"),positive_sequence(r,"generation",true)};
    if (result.token.incarnation > last_incarnation || integer(r,"initialized") != 1 ||
        (integer(r,"overflow") != 0 && integer(r,"overflow") != 1) || string_bytes(r.at("channel")) != channel)
        fail(code::corrupt_state,"receive ledger malformed addressed channel state");
    result.overflow = integer(r,"overflow") == 1;
    if (std::holds_alternative<std::nullptr_t>(r.at("checkpoint"))) result.kind = receive_checkpoint_kind::initialized_null;
    else { result.kind = receive_checkpoint_kind::value; result.checkpoint = string_bytes(r.at("checkpoint")); }
    if (result.checkpoint && size(*result.checkpoint) > limits_.checkpoint_bytes_per_channel)
        fail(code::corrupt_state,"receive ledger addressed checkpoint exceeds configured budget");
    return result;
}
std::optional<receive_ledger_token> receive_ledger::create(const std::string& channel) {
    const auto channel_bytes = size(channel);
    check_schema();
    if (channel_bytes <= limits_.channel_id_bytes && read(channel).kind != receive_checkpoint_kind::absent)
        fail(code::invalid_argument, "receive ledger channel already exists");
    const auto u = usage();
    auto& db = connection();
    return atomic(db, [&]() -> std::optional<receive_ledger_token> {
        if (u.channel_overflow || !fits(u.channels,1,limits_.channels) || !fits(u.channel_id_bytes,channel_bytes,limits_.channel_id_bytes)) {
            connection().execute("UPDATE main._lattice_receive_store SET channel_overflow=1 WHERE id=1");
            changed(db);
            return std::nullopt;
        }
        auto last = integer(db.query("SELECT last_incarnation FROM main._lattice_receive_store WHERE id=1").at(0),"last_incarnation");
        if (last == std::numeric_limits<int64_t>::max()) fail(code::sequence_exhausted, "receive ledger incarnation exhausted");
        connection().execute("UPDATE main._lattice_receive_store SET last_incarnation=? WHERE id=1", {last+1});
        changed(db);
        connection().execute("INSERT INTO main._lattice_receive_channel VALUES(?,?,0,1,NULL,0)", {bytes(channel),last+1});
        changed(db);
        return receive_ledger_token{channel,last+1,0};
    });
}
void receive_ledger::assert_current(const receive_ledger_token& token) const {
    if (token.incarnation <= 0 || token.generation < 0) fail(code::stale_token, "receive ledger invalid token");
    const auto state = channel_row(token.channel);
    if (state.kind == receive_checkpoint_kind::absent || !(state.token == token))
        fail(code::stale_token, "receive ledger stale or retired token");
}
std::optional<receive_identity_state> receive_ledger::identity(const receive_ledger_token& token, const std::string& id) const {
    assert_current(token);
    const auto n = size(id);
    if (n > limits_.identity_bytes || n > limits_.identity_bytes_per_channel)
        fail(code::invalid_argument,"receive ledger identity exceeds configured byte budget");
    auto rows = connection().query("SELECT disposition,acceptance FROM main._lattice_receive_identity WHERE incarnation=? AND event_id=?",
        {token.incarnation,bytes(id)});
    if (rows.empty()) return std::nullopt;
    if (rows.size()!=1) fail(code::corrupt_state,"receive ledger duplicate identity");
    return parse_identity(rows[0]);
}
receive_reservation receive_ledger::reserve(const receive_ledger_token& token,
                                           const std::vector<receive_identity_request>& request) {
    assert_current(token);
    const auto state = read(token.channel);
    const auto u = usage();
    std::map<std::string,receive_intake_disposition> fresh;
    int64_t added_bytes = 0;
    bool capacity_refused = false;
    for (const auto& item : request) {
        const auto n = size(item.id);
        if (!valid_disposition(static_cast<int64_t>(item.disposition))) fail(code::invalid_argument,"receive ledger invalid intake disposition");
        // Such an ID cannot already be charged under this durable budget.
        // Refuse before constructing an oversized encoded binding/copy.
        if (n > limits_.identity_bytes || n > limits_.identity_bytes_per_channel) { capacity_refused = true; break; }
        const auto existing = identity(token,item.id);
        if (existing) {
            if (existing->disposition != item.disposition) fail(code::disposition_conflict,"receive ledger identity disposition is immutable");
            continue;
        }
        const auto seen = fresh.find(item.id);
        if (seen != fresh.end()) {
            if (seen->second != item.disposition) fail(code::disposition_conflict,"receive ledger conflicting duplicate disposition");
            continue;
        }
        const auto count = static_cast<int64_t>(fresh.size());
        if (state.overflow || !fits(count,1,limits_.identities-u.identities) ||
            !fits(count,1,limits_.identities_per_channel-state.identities) ||
            !fits(added_bytes,n,limits_.identity_bytes-u.identity_bytes) ||
            !fits(added_bytes,n,limits_.identity_bytes_per_channel-state.identity_bytes)) { capacity_refused = true; break; }
        fresh.emplace(item.id,item.disposition);
        added_bytes += n;
    }
    const auto added = static_cast<int64_t>(fresh.size());
    const bool admitted = !capacity_refused;
    if (token.generation == std::numeric_limits<int64_t>::max()) fail(code::sequence_exhausted,"receive ledger generation exhausted");
    auto& db = connection();
    return atomic(db,[&] {
        assert_current(token);
        connection().execute("UPDATE main._lattice_receive_channel SET generation=?,overflow=? WHERE incarnation=?",
            {token.generation+1,static_cast<int64_t>(state.overflow || !admitted),token.incarnation});
        changed(db);
        const receive_ledger_token next{token.channel,token.incarnation,token.generation+1};
        if (admitted) for (const auto& [id,disposition] : fresh) {
            assert_current(next);
            connection().execute("INSERT INTO main._lattice_receive_identity VALUES(?,?,?,0)",
                {next.incarnation,bytes(id),static_cast<int64_t>(disposition)});
            changed(db);
        }
        assert_current(next);
        return receive_reservation{next,admitted,admitted ? added : 0,admitted ? added_bytes : 0};
    });
}
void receive_ledger::complete(const receive_ledger_token& token, const std::string& id, receive_acceptance accepted) {
    assert_current(token);
    auto prior = identity(token,id);
    if (!prior) fail(code::identity_missing,"receive ledger cannot settle an unreserved identity");
    const auto a = static_cast<int64_t>(accepted);
    if (a < 1 || a > 3 || ((prior->disposition == receive_intake_disposition::known_schema) == (accepted == receive_acceptance::policy)))
        fail(code::invalid_acceptance,"receive ledger acceptance contradicts durable intake disposition");
    if (prior->acceptance != receive_acceptance::pending) {
        if (prior->acceptance != accepted) fail(code::invalid_acceptance,"receive ledger cannot rewrite complete acceptance");
        return;
    }
    atomic(connection(),[&] {
        assert_current(token);
        connection().execute("UPDATE main._lattice_receive_identity SET acceptance=? WHERE incarnation=? AND event_id=?",
            {a,token.incarnation,bytes(id)});
        changed(connection());
        assert_current(token);
        return true;
    });
}
void receive_ledger::retire(const receive_ledger_token& token) {
    assert_current(token);
    const auto state = read(token.channel);
    atomic(connection(),[&] {
        assert_current(token);
        connection().execute("DELETE FROM main._lattice_receive_identity WHERE incarnation=?", {token.incarnation});
        changed(connection(),state.identities);
        assert_current(token);
        connection().execute("DELETE FROM main._lattice_receive_channel WHERE incarnation=?", {token.incarnation});
        changed(connection());
        return true;
    });
}
} // namespace lattice::detail
