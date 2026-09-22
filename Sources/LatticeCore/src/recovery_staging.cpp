#include "recovery_staging.hpp"
#include "recovery_writer_access.hpp"
#include "vendor/picosha2/picosha2.h"
#include <nlohmann/json.hpp>
#include <algorithm>
#include <array>
#include <limits>

namespace lattice::detail {
namespace {
namespace protocol = sync_recovery;
using code = recovery_staging_code;
using blob = std::vector<uint8_t>;
using sql_row = database::row_t;
constexpr uint64_t hash_input_max = std::numeric_limits<uint64_t>::max() / 8;
[[noreturn]] void fail(code c, const char* reason) { throw recovery_staging_error(c,reason); }
blob bytes(const std::string& s) { return blob(s.begin(),s.end()); }
int64_t number(const sql_row& r, const char* key) {
    const auto i = r.find(key);
    if (i == r.end() || !std::holds_alternative<int64_t>(i->second))
        fail(code::corrupt_state,"snapshot staging expected integer metadata");
    return std::get<int64_t>(i->second);
}
std::string text(const sql_row& r, const char* key) {
    const auto i = r.find(key);
    if (i == r.end() || !std::holds_alternative<blob>(i->second))
        fail(code::corrupt_state,"snapshot staging expected encoded bytes");
    const auto& v = std::get<blob>(i->second);
    return std::string(v.begin(),v.end());
}
int64_t size(const std::string& s) {
    if (s.size() > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))
        fail(code::invalid_argument,"snapshot encoded size is not representable");
    return static_cast<int64_t>(s.size());
}
bool fits(int64_t current, int64_t add, int64_t limit) {
    return current >= 0 && add >= 0 && current <= limit && add <= limit-current;
}
void changed(database& db) {
    if (db.changes() != 1) throw db_error("snapshot staging write did not change its expected row");
}
template<class F> auto atomic(database& db, F&& body) {
    db.execute("SAVEPOINT lattice_recovery_staging");
    try {
        auto result = body();
        db.execute("RELEASE lattice_recovery_staging");
        return result;
    } catch (...) {
        const auto original = std::current_exception();
        if (!db.is_in_transaction()) std::rethrow_exception(original);
        try {
            db.execute("ROLLBACK TO lattice_recovery_staging");
            db.execute("RELEASE lattice_recovery_staging");
        } catch (...) {
            throw recovery_staging_error(code::cleanup_failed,
                "snapshot staging cleanup failed; abort the owning transaction",original,std::current_exception());
        }
        std::rethrow_exception(original);
    }
}
// PicoSHA2's process() first copies its input range. Feed small bounded chunks,
// never a full payload. The canonical stream has no page boundary delimiters.
class canonical_hash {
    picosha2::hash256_one_by_one hash_;
    uint64_t count_ = 0;
    void feed(const uint8_t* p, size_t n) {
        while (n) {
            const auto part = std::min<size_t>(n,4096);
            hash_.process(p,p+part); p += part; n -= part;
        }
    }
    void string(const std::string& s) {
        std::array<uint8_t,8> prefix{};
        uint64_t n = s.size();
        for (size_t i=0;i<8;++i) prefix[7-i] = static_cast<uint8_t>(n >> (i*8));
        feed(prefix.data(),prefix.size());
        feed(reinterpret_cast<const uint8_t*>(s.data()),s.size());
    }
public:
    void add(const protocol::row& r) {
        const auto n = protocol::canonical_row_bytes(r);
        if (count_ > hash_input_max || n > hash_input_max-count_)
            fail(code::invalid_argument,"canonical SHA-256 input exceeds its bit-length representation");
        count_ += n;
        string(r.table); string(r.global_id); string(r.payload);
    }
    std::string finish() { hash_.finish(); return picosha2::get_hash_hex_string(hash_); }
};
bool same_usage(const recovery_staging_usage& a,const recovery_staging_usage& b) {
    return a.channels==b.channels && a.pages==b.pages && a.rows==b.rows &&
        a.canonical_bytes==b.canonical_bytes && a.stored_bytes==b.stored_bytes;
}
recovery_staging_usage read_usage(database& db) {
    const auto r=db.query("SELECT used_channels,used_pages,used_rows,used_canonical_bytes,used_stored_bytes "
        "FROM main._lattice_recovery_store WHERE id=1");
    if (r.size()!=1) fail(code::corrupt_state,"snapshot usage record is absent");
    return {number(r[0],"used_channels"),number(r[0],"used_pages"),number(r[0],"used_rows"),
        number(r[0],"used_canonical_bytes"),number(r[0],"used_stored_bytes")};
}
void write_usage(database& db,const recovery_staging_usage& before,const recovery_staging_usage& after) {
    db.execute("UPDATE main._lattice_recovery_store SET used_channels=?,used_pages=?,used_rows=?,used_canonical_bytes=?,used_stored_bytes=? "
        "WHERE id=1 AND used_channels=? AND used_pages=? AND used_rows=? AND used_canonical_bytes=? AND used_stored_bytes=?",
        {after.channels,after.pages,after.rows,after.canonical_bytes,after.stored_bytes,
         before.channels,before.pages,before.rows,before.canonical_bytes,before.stored_bytes});
    changed(db);
    if (!same_usage(read_usage(db),after)) fail(code::corrupt_state,"snapshot usage update did not persist exact counters");
}
protocol::end ending(const protocol::manifest& m) {
    return {m.identity,m.frontier,m.page_count,m.row_count,m.content_bytes,m.content_digest};
}
protocol::manifest stored_manifest(const std::string& wire, const protocol::limits& budget) {
    try {
        auto decoded = protocol::decode(wire,budget);
        if (auto* value = std::get_if<protocol::manifest>(&decoded)) return *value;
    } catch (const protocol::protocol_error&) {}
    fail(code::corrupt_state,"snapshot stored manifest is invalid");
}
protocol::page stored_page(const std::string& wire, const protocol::limits& budget) {
    try {
        auto decoded = protocol::decode(wire,budget);
        if (auto* value = std::get_if<protocol::page>(&decoded)) return *value;
    } catch (const protocol::protocol_error&) {}
    fail(code::corrupt_state,"snapshot stored page is invalid");
}
}

std::string canonical_rows_sha256(const std::vector<sync_recovery::row>& rows) {
    canonical_hash hash;
    for (const auto& row : rows) hash.add(row);
    return hash.finish();
}

recovery_staging::recovery_staging(lattice_db& owner, receive_ledger_limits ledger_limits,
        sync_recovery::limits codec, recovery_staging_limits limits)
    : owner_(owner), ledger_(owner,ledger_limits), codec_(codec), limits_(limits),
      channel_bytes_(ledger_limits.channel_id_bytes) {
    const int64_t values[] = {limits.channels,limits.pages,limits.rows,limits.canonical_bytes,limits.stored_bytes};
    for (const auto n : values) if (n < 0) fail(code::invalid_argument,"snapshot staging limits must be nonnegative");
    // Match the codec's finite hard ceilings before encoding any state image.
    const auto position_max = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
    if (!codec.frame_bytes || codec.frame_bytes > 16*1024*1024 || !codec.depth || codec.depth > 64 ||
        !codec.nodes || codec.nodes > 65536 || !codec.string_bytes || codec.string_bytes > codec.frame_bytes ||
        !codec.rows_per_page || codec.rows_per_page > 4096 || !codec.pages || codec.pages > position_max ||
        !codec.total_rows || codec.total_rows > position_max || !codec.total_bytes || codec.total_bytes > hash_input_max ||
        static_cast<uint64_t>(limits.canonical_bytes) > hash_input_max)
        fail(code::invalid_argument,"snapshot staging codec or SHA-256 limits are invalid");
}
database& recovery_staging::connection() const {
    auto* writer = recovery_writer_access::active_writer(owner_);
    if (!writer) fail(code::transaction_required,"snapshot staging requires this thread's owned main write transaction");
    return *writer;
}
std::string recovery_staging::configuration() const {
    return nlohmann::json{{"frame",codec_.frame_bytes},{"depth",codec_.depth},{"nodes",codec_.nodes},
        {"string",codec_.string_bytes},{"pageRows",codec_.rows_per_page},{"pages",codec_.pages},
        {"rows",codec_.total_rows},{"bytes",codec_.total_bytes},{"channels",limits_.channels},
        {"reservedPages",limits_.pages},{"reservedRows",limits_.rows},{"canonicalBytes",limits_.canonical_bytes},
        {"storedBytes",limits_.stored_bytes},{"channelBytes",channel_bytes_}}.dump();
}
void recovery_staging::check_schema() const {
    auto& db = connection();
    const auto tables = db.query("SELECT wr FROM pragma_table_list WHERE schema='main' AND name IN "
        "('_lattice_recovery_store','_lattice_recovery_attempt','_lattice_recovery_page')");
    if (tables.size()!=3 || std::any_of(tables.begin(),tables.end(),[](const auto& r){return number(r,"wr")!=1;}))
        fail(code::corrupt_state,"snapshot staging requires all three WITHOUT ROWID metadata tables");
    const auto config = configuration();
    const auto shape = db.query("SELECT id,version,typeof(configuration)='blob' AS encoded,length(configuration) AS bytes "
        "FROM main._lattice_recovery_store LIMIT 2");
    if (shape.size()!=1 || number(shape[0],"id")!=1 || number(shape[0],"version")!=2 ||
        number(shape[0],"encoded")!=1)
        fail(code::corrupt_state,"snapshot staging store metadata is invalid");
    if (number(shape[0],"bytes") != size(config)) fail(code::limits_mismatch,"snapshot staging durable limits differ");
    if (text(db.query("SELECT configuration FROM main._lattice_recovery_store WHERE id=1").at(0),"configuration")!=config)
        fail(code::limits_mismatch,"snapshot staging durable limits differ");
}
void recovery_staging::initialize() {
    auto& db=connection();
    ledger_.audit(); // No implicit creation/migration of the authority ledger.
    const auto present=db.query("SELECT name FROM main.sqlite_master WHERE name IN "
        "('_lattice_recovery_store','_lattice_recovery_attempt','_lattice_recovery_page')");
    if (!present.empty()) { audit(); return; }
    const auto config=configuration();
    if (size(config)>limits_.stored_bytes) fail(code::capacity,"snapshot store configuration exceeds stored-byte budget");
    atomic(db,[&] {
        db.execute("CREATE TABLE main._lattice_recovery_store (id INTEGER PRIMARY KEY CHECK(id=1), "
            "version INTEGER NOT NULL, configuration BLOB NOT NULL, used_channels INTEGER NOT NULL, used_pages INTEGER NOT NULL, "
            "used_rows INTEGER NOT NULL, used_canonical_bytes INTEGER NOT NULL, used_stored_bytes INTEGER NOT NULL) WITHOUT ROWID");
        db.execute("CREATE TABLE main._lattice_recovery_attempt (channel BLOB PRIMARY KEY NOT NULL, "
            "incarnation INTEGER NOT NULL, generation INTEGER NOT NULL, manifest BLOB NOT NULL, state BLOB NOT NULL, "
            "verified INTEGER NOT NULL CHECK(verified IN(0,1)), pages INTEGER NOT NULL, rows INTEGER NOT NULL, "
            "canonical_bytes INTEGER NOT NULL, page_bytes INTEGER NOT NULL) WITHOUT ROWID");
        db.execute("CREATE TABLE main._lattice_recovery_page (channel BLOB NOT NULL REFERENCES _lattice_recovery_attempt(channel), "
            "page_index INTEGER NOT NULL, wire BLOB NOT NULL, PRIMARY KEY(channel,page_index)) WITHOUT ROWID");
        db.execute("INSERT INTO main._lattice_recovery_store VALUES(1,2,?,0,0,0,0,?)",{bytes(config),size(config)}); changed(db);
        (void)usage();
        return true;
    });
}
recovery_staging_usage recovery_staging::usage() const {
    check_schema();
    const auto u=read_usage(connection());
    if (!fits(0,u.channels,limits_.channels) || !fits(0,u.pages,limits_.pages) || !fits(0,u.rows,limits_.rows) ||
        !fits(0,u.canonical_bytes,limits_.canonical_bytes) || u.stored_bytes<size(configuration()) || !fits(0,u.stored_bytes,limits_.stored_bytes))
        fail(code::corrupt_state,"snapshot usage counters exceed their configured bounds");
    return u;
}
void recovery_staging::audit_usage() const {
    const auto recorded=usage();
    auto& db=connection();
    // Full storage validation is deliberately outside hot point access. Each
    // stored BLOB is type/length checked before an audit copies its content.
    if (!db.query("SELECT 1 FROM main._lattice_recovery_attempt WHERE typeof(channel)!='blob' OR length(channel)=0 "
        "OR length(channel)>? OR typeof(incarnation)!='integer' OR incarnation<=0 OR typeof(generation)!='integer' OR generation<0 "
        "OR typeof(manifest)!='blob' OR length(manifest)=0 OR length(manifest)>? "
        "OR typeof(state)!='blob' OR length(state)=0 OR length(state)>? OR typeof(verified)!='integer' OR verified NOT IN(0,1) "
        "OR typeof(pages)!='integer' OR pages<0 OR typeof(rows)!='integer' OR rows<0 "
        "OR typeof(canonical_bytes)!='integer' OR canonical_bytes<0 OR typeof(page_bytes)!='integer' OR page_bytes<0 LIMIT 1",
        {channel_bytes_,static_cast<int64_t>(codec_.frame_bytes),static_cast<int64_t>(codec_.frame_bytes)}).empty())
        fail(code::corrupt_state,"snapshot staging malformed or oversized attempt metadata");
    if (!db.query("SELECT 1 FROM main._lattice_recovery_page p LEFT JOIN main._lattice_recovery_attempt a USING(channel) "
        "WHERE a.channel IS NULL OR typeof(p.page_index)!='integer' OR p.page_index<0 "
        "OR typeof(p.wire)!='blob' OR length(p.wire)=0 OR length(p.wire)>? LIMIT 1",
        {static_cast<int64_t>(codec_.frame_bytes)}).empty())
        fail(code::corrupt_state,"snapshot staging orphan or oversized page");
    if (!db.query("SELECT 1 FROM main._lattice_recovery_attempt a LEFT JOIN "
        "(SELECT channel,SUM(length(channel)+length(wire)) AS bytes FROM main._lattice_recovery_page GROUP BY channel) p USING(channel) "
        "WHERE a.page_bytes!=COALESCE(p.bytes,0) LIMIT 1").empty())
        fail(code::corrupt_state,"snapshot attempt page-byte counter differs from actual storage");
    const auto a=db.query("SELECT COUNT(*) AS channels,COALESCE(SUM(pages),0) AS pages,COALESCE(SUM(rows),0) AS rows, "
        "COALESCE(SUM(canonical_bytes),0) AS canonical,COALESCE(SUM(length(channel)+length(manifest)+length(state)),0) AS bytes "
        "FROM main._lattice_recovery_attempt").at(0);
    const auto p=db.query("SELECT COUNT(*) AS pages,COALESCE(SUM(length(channel)+length(wire)),0) AS bytes "
        "FROM main._lattice_recovery_page").at(0);
    recovery_staging_usage actual{number(a,"channels"),number(a,"pages"),number(a,"rows"),number(a,"canonical"),size(configuration())};
    if (!fits(0,actual.channels,limits_.channels) || !fits(0,actual.pages,limits_.pages) || !fits(0,actual.rows,limits_.rows) ||
        !fits(0,actual.canonical_bytes,limits_.canonical_bytes) || !fits(0,number(p,"pages"),actual.pages) ||
        !fits(actual.stored_bytes,number(a,"bytes"),limits_.stored_bytes))
        fail(code::corrupt_state,"snapshot actual storage exceeds aggregate reservations");
    actual.stored_bytes+=number(a,"bytes");
    if (!fits(actual.stored_bytes,number(p,"bytes"),limits_.stored_bytes)) fail(code::corrupt_state,"snapshot actual stored-byte budget exceeded");
    actual.stored_bytes+=number(p,"bytes");
    if (!same_usage(actual,recorded)) fail(code::corrupt_state,"snapshot usage counters differ from actual storage");
}
recovery_staging_snapshot recovery_staging::addressed(const receive_ledger_token& token,
        const sync_recovery::binding& expected) const {
    const auto account=usage();
    auto& db=connection();
    // Indexed preflight: never copy a stored manifest/state until SQL type and
    // actual byte lengths are bounded. No retained-page COUNT/SUM is on this path.
    const auto shape=db.query("SELECT typeof(channel)='blob' AND length(channel)>0 AND length(channel)<=? "
        "AND typeof(incarnation)='integer' AND incarnation>0 AND typeof(generation)='integer' AND generation>=0 "
        "AND typeof(manifest)='blob' AND length(manifest)>0 AND length(manifest)<=? "
        "AND typeof(state)='blob' AND length(state)>0 AND length(state)<=? "
        "AND typeof(verified)='integer' AND verified IN(0,1) AND typeof(pages)='integer' AND pages>=0 "
        "AND typeof(rows)='integer' AND rows>=0 AND typeof(canonical_bytes)='integer' AND canonical_bytes>=0 "
        "AND typeof(page_bytes)='integer' AND page_bytes>=0 AS valid, "
        "length(channel) AS key_bytes,length(manifest) AS manifest_bytes,length(state) AS state_bytes,page_bytes "
        "FROM main._lattice_recovery_attempt WHERE channel=?",
        {channel_bytes_,static_cast<int64_t>(codec_.frame_bytes),static_cast<int64_t>(codec_.frame_bytes),bytes(token.channel)});
    if (shape.size()!=1) fail(code::stale_attempt,"snapshot attempt is absent");
    if (number(shape[0],"valid")!=1) fail(code::corrupt_state,"snapshot addressed attempt has invalid types or lengths");
    int64_t remaining=account.stored_bytes-size(configuration());
    for (const auto* key:{"key_bytes","manifest_bytes","state_bytes","page_bytes"}) {
        const auto n=number(shape[0],key);
        if (n<0 || n>remaining) fail(code::corrupt_state,"snapshot attempt byte charges exceed store counters");
        remaining-=n;
    }
    const auto rows=db.query("SELECT * FROM main._lattice_recovery_attempt WHERE channel=?",{bytes(token.channel)});
    if (rows.size()!=1) fail(code::stale_attempt,"snapshot attempt is absent");
    const auto& r=rows[0];
    if (number(r,"incarnation")!=token.incarnation || number(r,"generation")!=token.generation)
        fail(code::stale_attempt,"snapshot attempt belongs to another channel generation");
    const auto offer=stored_manifest(text(r,"manifest"),codec_);
    if (!(offer.identity==expected)) fail(code::stale_attempt,"snapshot attempt binding differs");
    protocol::staging_state state;
    try { state=protocol::decode_staging(text(r,"state"),expected,codec_); }
    catch (const protocol::protocol_error&) { fail(code::corrupt_state,"snapshot staging state image is invalid"); }
    const bool verified=number(r,"verified")==1;
    if (!(state.offer==offer) || number(r,"pages")!=static_cast<int64_t>(offer.page_count) ||
        number(r,"rows")!=static_cast<int64_t>(offer.row_count) || number(r,"canonical_bytes")!=static_cast<int64_t>(offer.content_bytes) ||
        verified!=(state.status==protocol::phase::sequence_complete_unverified))
        fail(code::corrupt_state,"snapshot staging state and reservation disagree");
    const auto page_bytes=number(r,"page_bytes");
    if ((state.next_page==0 && page_bytes!=0) || (state.next_page>0 && page_bytes/2<static_cast<int64_t>(state.next_page)))
        fail(code::corrupt_state,"snapshot page-byte counter cannot represent its stored progress");
    return {std::move(state),verified};
}
recovery_staging_snapshot recovery_staging::verify_storage(const receive_ledger_token& token,
        const sync_recovery::binding& expected, bool whole) const {
    const auto stored=addressed(token,expected);
    auto rebuilt=protocol::begin(stored.state.offer,expected,codec_);
    canonical_hash hash;
    auto& db=connection();
    // One bounded page at a time. Require every index, including absence of an
    // extra tail; the mutable state image is not evidence that rows exist.
    for (uint64_t index=0;index<stored.state.next_page;++index) {
        const auto pages=db.query("SELECT wire FROM main._lattice_recovery_page WHERE channel=? AND page_index=?",
            {bytes(token.channel),static_cast<int64_t>(index)});
        if (pages.size()!=1) fail(code::corrupt_state,"snapshot staging is missing a page");
        const auto p=stored_page(text(pages[0],"wire"),codec_);
        if (p.index!=index || canonical_rows_sha256(p.rows)!=p.content_digest)
            fail(code::digest_mismatch,"snapshot staging page digest/index differs");
        for (const auto& r:p.rows) hash.add(r);
        try { rebuilt=protocol::propose(rebuilt,p,codec_); }
        catch (const protocol::protocol_error&) { fail(code::corrupt_state,"snapshot stored page sequence is invalid"); }
    }
    const auto count=db.query("SELECT COUNT(*) AS n,COALESCE(MIN(page_index),0) AS first,COALESCE(MAX(page_index),-1) AS last, "
        "COALESCE(SUM(length(channel)+length(wire)),0) AS bytes FROM main._lattice_recovery_page WHERE channel=?",{bytes(token.channel)}).at(0);
    const auto charge=number(db.query("SELECT page_bytes FROM main._lattice_recovery_attempt WHERE channel=?",{bytes(token.channel)}).at(0),"page_bytes");
    if (number(count,"n")!=static_cast<int64_t>(stored.state.next_page) || number(count,"first")!=0 ||
        number(count,"last")!=static_cast<int64_t>(stored.state.next_page)-1 || number(count,"bytes")!=charge)
        fail(code::corrupt_state,"snapshot actual page set or byte charge differs from recorded progress");
    if (whole || stored.content_verified) {
        if (hash.finish()!=stored.state.offer.content_digest)
            fail(code::digest_mismatch,"snapshot whole-content digest differs");
        try { rebuilt=protocol::propose(rebuilt,ending(stored.state.offer),codec_); }
        catch (const protocol::protocol_error&) { fail(code::corrupt_state,"snapshot staging is incomplete"); }
    }
    auto comparison=rebuilt;
    if (whole && !stored.content_verified) comparison.status=protocol::phase::receiving;
    if (!(comparison==stored.state)) fail(code::corrupt_state,"snapshot stored progress does not match actual pages");
    return {std::move(rebuilt),whole || stored.content_verified};
}
void recovery_staging::audit() const {
    ledger_.audit();
    audit_usage();
    auto& db=connection();
    std::optional<std::string> previous;
    // Fenced attempts stay stored and charged. Audit validates their bytes but
    // does not grant a stale token permission to resume or replace them.
    while (true) {
        const auto rows=previous
            ? db.query("SELECT channel,incarnation,generation,manifest FROM main._lattice_recovery_attempt WHERE channel>? ORDER BY channel LIMIT 1",{bytes(*previous)})
            : db.query("SELECT channel,incarnation,generation,manifest FROM main._lattice_recovery_attempt ORDER BY channel LIMIT 1");
        if (rows.empty()) break;
        const auto& r=rows[0];
        const receive_ledger_token token{text(r,"channel"),number(r,"incarnation"),number(r,"generation")};
        const auto offer=stored_manifest(text(r,"manifest"),codec_);
        (void)verify_storage(token,offer.identity,false);
        previous=token.channel;
    }
}
recovery_staging_snapshot recovery_staging::begin(const receive_ledger_token& token,
        const sync_recovery::manifest& offer, const sync_recovery::binding& expected) {
    connection(); ledger_.assert_current(token);
    const auto state=protocol::begin(offer,expected,codec_);
    const auto manifest_wire=protocol::encode(offer,codec_);
    const auto state_wire=protocol::encode_staging(state,codec_); // May exceed a valid manifest's frame budget.
    auto& db=connection();
    const auto u=usage();
    if (!db.query("SELECT 1 FROM main._lattice_recovery_attempt WHERE channel=?",{bytes(token.channel)}).empty()) {
        const auto old=resume(token,expected);
        if (!(old.state.offer==offer)) fail(code::stale_attempt,"snapshot attempt replacement requires an explicit fenced operation");
        return old;
    }
    if (!fits(u.channels,1,limits_.channels) || !fits(u.pages,static_cast<int64_t>(offer.page_count),limits_.pages) ||
        !fits(u.rows,static_cast<int64_t>(offer.row_count),limits_.rows) ||
        !fits(u.canonical_bytes,static_cast<int64_t>(offer.content_bytes),limits_.canonical_bytes))
        fail(code::capacity,"snapshot declaration exceeds aggregate reservations");
    int64_t remaining=limits_.stored_bytes-u.stored_bytes;
    for (const auto n : {size(token.channel),size(manifest_wire),size(state_wire)}) {
        if (n>remaining) fail(code::capacity,"snapshot attempt exceeds stored-byte budget");
        remaining-=n;
    }
    auto after=u;
    ++after.channels; after.pages+=static_cast<int64_t>(offer.page_count); after.rows+=static_cast<int64_t>(offer.row_count);
    after.canonical_bytes+=static_cast<int64_t>(offer.content_bytes); after.stored_bytes=limits_.stored_bytes-remaining;
    return atomic(db,[&] {
        db.execute("INSERT INTO main._lattice_recovery_attempt VALUES(?,?,?,?,?,0,?,?,?,0)",
            {bytes(token.channel),token.incarnation,token.generation,bytes(manifest_wire),bytes(state_wire),
             static_cast<int64_t>(offer.page_count),static_cast<int64_t>(offer.row_count),static_cast<int64_t>(offer.content_bytes)});
        changed(db); write_usage(db,u,after);
        return recovery_staging_snapshot{state,false};
    });
}
recovery_staging_snapshot recovery_staging::resume(const receive_ledger_token& token,
        const sync_recovery::binding& expected) const {
    connection(); ledger_.assert_current(token);
    audit_usage();
    return verify_storage(token,expected,false);
}
recovery_staging_snapshot recovery_staging::append(const receive_ledger_token& token, const sync_recovery::page& page) {
    connection(); ledger_.assert_current(token);
    const auto wire=protocol::encode(page,codec_);
    const auto old=addressed(token,page.identity);
    auto& db=connection();
    const auto existing=db.query("SELECT typeof(wire)='blob' AS encoded,length(wire) AS n FROM main._lattice_recovery_page WHERE channel=? AND page_index=?",
        {bytes(token.channel),static_cast<int64_t>(page.index)});
    if (!existing.empty()) {
        if (number(existing[0],"encoded")!=1 || number(existing[0],"n")<=0 || number(existing[0],"n")>static_cast<int64_t>(codec_.frame_bytes))
            fail(code::corrupt_state,"snapshot existing page has invalid encoded size");
        const auto duplicate=db.query("SELECT wire FROM main._lattice_recovery_page WHERE channel=? AND page_index=?",
            {bytes(token.channel),static_cast<int64_t>(page.index)});
        if (text(duplicate[0],"wire")!=wire) fail(code::conflicting_page,"snapshot page index already has different bytes");
        if (canonical_rows_sha256(page.rows)!=page.content_digest)
            fail(code::digest_mismatch,"snapshot duplicate page digest differs");
        // A returned true flag always follows verification of actual content.
        // Ordinary receiving-page duplicates do not rehash the whole prefix.
        if (old.content_verified) { audit_usage(); return verify_storage(token,page.identity,true); }
        return old;
    }
    if (canonical_rows_sha256(page.rows)!=page.content_digest) fail(code::digest_mismatch,"snapshot page digest differs");
    const auto next=protocol::propose(old.state,page,codec_);
    const auto next_wire=protocol::encode_staging(next,codec_);
    const auto u=usage();
    // Stored images are canonical: subtract the actual old state, not an
    // assumed payload count. The query also handles externally recoded JSON.
    const auto old_bytes=db.query("SELECT length(state) AS n,page_bytes FROM main._lattice_recovery_attempt WHERE channel=?",{bytes(token.channel)}).at(0);
    const auto actual=number(old_bytes,"n"),page_bytes=number(old_bytes,"page_bytes");
    int64_t remaining=limits_.stored_bytes-(u.stored_bytes-actual);
    for (const auto n : {size(token.channel),size(wire),size(next_wire)}) {
        if (n>remaining) fail(code::capacity,"snapshot page/state exceeds stored-byte budget");
        remaining-=n;
    }
    const auto added_page=size(token.channel)+size(wire); // Both individually bounded above.
    if (!fits(page_bytes,added_page,limits_.stored_bytes)) fail(code::capacity,"snapshot page-byte counter exceeds budget");
    auto after=u; after.stored_bytes=limits_.stored_bytes-remaining;
    return atomic(db,[&] {
        db.execute("INSERT INTO main._lattice_recovery_page VALUES(?,?,?)",{bytes(token.channel),static_cast<int64_t>(page.index),bytes(wire)}); changed(db);
        db.execute("UPDATE main._lattice_recovery_attempt SET state=?,page_bytes=? WHERE channel=?",{bytes(next_wire),page_bytes+added_page,bytes(token.channel)}); changed(db);
        write_usage(db,u,after);
        return recovery_staging_snapshot{next,false};
    });
}
recovery_staging_snapshot recovery_staging::verify_end(const receive_ledger_token& token, const sync_recovery::end& end) {
    connection(); ledger_.assert_current(token);
    (void)protocol::encode(end,codec_);
    audit_usage();
    const auto old=addressed(token,end.identity);
    if (!(ending(old.state.offer)==end)) fail(code::stale_attempt,"snapshot end does not match the bound manifest");
    const auto verified=verify_storage(token,end.identity,true);
    if (old.content_verified) return verified;
    const auto image=protocol::encode_staging(verified.state,codec_);
    auto& db=connection();
    const auto actual=number(db.query("SELECT length(state) AS n FROM main._lattice_recovery_attempt WHERE channel=?",{bytes(token.channel)}).at(0),"n");
    const auto u=usage();
    if (!fits(u.stored_bytes-actual,size(image),limits_.stored_bytes)) fail(code::capacity,"verified state exceeds stored-byte budget");
    auto after=u; after.stored_bytes=u.stored_bytes-actual+size(image);
    return atomic(db,[&] {
        db.execute("UPDATE main._lattice_recovery_attempt SET state=?,verified=1 WHERE channel=?",{bytes(image),bytes(token.channel)}); changed(db);
        write_usage(db,u,after);
        return verified;
    });
}
sync_recovery::page recovery_staging::read_verified_page(const receive_ledger_token& token,
        const sync_recovery::binding& expected, uint64_t index) const {
    connection(); ledger_.assert_current(token);
    const auto stored=addressed(token,expected);
    if (!stored.content_verified) fail(code::not_verified,"snapshot content has not been verified");
    if (index>=stored.state.offer.page_count) fail(code::invalid_argument,"snapshot page index is outside the verified manifest");
    const auto shape=connection().query("SELECT typeof(wire)='blob' AS encoded,length(wire) AS n FROM main._lattice_recovery_page WHERE channel=? AND page_index=?",
        {bytes(token.channel),static_cast<int64_t>(index)});
    if (shape.size()!=1 || number(shape[0],"encoded")!=1 || number(shape[0],"n")<=0 ||
        number(shape[0],"n")>static_cast<int64_t>(codec_.frame_bytes))
        fail(code::corrupt_state,"snapshot verified page is missing or oversized");
    const auto pages=connection().query("SELECT wire FROM main._lattice_recovery_page WHERE channel=? AND page_index=?",
        {bytes(token.channel),static_cast<int64_t>(index)});
    const auto wire=text(pages[0],"wire");
    auto page=stored_page(wire,codec_);
    if (!(page.identity==expected) || page.index!=index || protocol::encode(page,codec_)!=wire)
        fail(code::corrupt_state,"snapshot verified page encoding or binding differs");
    if (canonical_rows_sha256(page.rows)!=page.content_digest)
        fail(code::digest_mismatch,"snapshot verified page digest differs");
    return page;
}
} // namespace lattice::detail
