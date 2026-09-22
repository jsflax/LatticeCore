#include "receive_install_state.hpp"
#include "recovery_writer_access.hpp"
#include <limits>
#include <algorithm>
#include <vector>

namespace lattice::detail {
namespace {
using code = receive_install_error_code;
using blob = std::vector<uint8_t>;
using row_t = database::row_t;
constexpr int64_t maximum = std::numeric_limits<int64_t>::max();
[[noreturn]] void fail(code c, const char* message) { throw receive_install_error(c, message); }
int64_t integer(const row_t& r, const std::string& name) {
    auto it = r.find(name);
    if (it == r.end() || !std::holds_alternative<int64_t>(it->second))
        fail(code::corrupt_state, "receiver installation expected integer metadata");
    return std::get<int64_t>(it->second);
}
bool fits(int64_t n, int64_t extra, int64_t limit) {
    return n >= 0 && extra >= 0 && n <= limit && extra <= limit - n;
}
int64_t sum(int64_t a, int64_t b) {
    if (!fits(a,b,maximum)) fail(code::invalid_argument,"receiver installation size overflow");
    return a+b;
}
blob bytes(const std::string& s) { return {s.begin(),s.end()}; }
void field(const std::string& s, const receive_install_limits& limits, code c = code::invalid_argument) {
    if (s.empty() || s.size() > static_cast<uint64_t>(limits.field_bytes))
        fail(c,"receiver installation identifier/digest exceeds explicit field limit");
}
int64_t binding_size(const receive_install_binding& b, const receive_install_limits& limits,
                     code c = code::invalid_argument) {
    int64_t n=0;
    for (auto* s : {&b.channel,&b.authority,&b.source,&b.epoch,&b.scope,&b.schema}) {
        field(*s,limits,c); n=sum(n,static_cast<int64_t>(s->size()));
    }
    if (n>limits.encoded_bytes) fail(c==code::corrupt_state ? c : code::capacity,
        "receiver installation binding exceeds total byte limit");
    return n;
}
void frontier(const receive_install_frontier& f, code c) {
    if (f.kind == receive_frontier_kind::position) {
        if (!f.position || *f.position < 0) fail(c,"receiver installation requires a numeric frontier");
    } else if ((f.kind != receive_frontier_kind::uninitialized && f.kind != receive_frontier_kind::beginning_null) || f.position)
        fail(c,"receiver installation has contradictory frontier initialization");
}
int64_t encoded_size(const receive_install_identity& i) {
    return sum(88,sum(static_cast<int64_t>(i.request_digest.size()),
        sum(static_cast<int64_t>(i.receipt_digest.size()),sum(static_cast<int64_t>(i.content_digest.size()),
            static_cast<int64_t>(i.manifest_digest.size())))));
}
void identity(const receive_install_identity& i, const receive_install_limits& limits,
              code c = code::invalid_argument) {
    frontier(i.base,c);
    if (i.sequence <= 0 || i.expected_revision < 0 || i.expected_revision == maximum || i.head < 0 ||
        (i.mode != receive_install_mode::full && i.mode != receive_install_mode::delta) ||
        (i.mode == receive_install_mode::delta && i.base.kind != receive_frontier_kind::position) ||
        (i.base.position && i.head < *i.base.position)) fail(c,"receiver installation invalid identity or regressing head");
    if (i.sequence<=i.expected_revision ||
        ((i.base.kind==receive_frontier_kind::position)!=(i.expected_revision>0)))
        fail(c,"receiver installation identity contradicts its expected initialization/revision");
    field(i.request_digest,limits,c); field(i.receipt_digest,limits,c); field(i.content_digest,limits,c); field(i.manifest_digest,limits,c);
    const auto n=encoded_size(i);
    if (n>limits.encoded_bytes) fail(c==code::corrupt_state ? c : code::capacity,
        "receiver installation encoded identity exceeds total byte limit");
}
void append(blob& b, int64_t n) {
    for (int shift=56; shift>=0; shift-=8) b.push_back(static_cast<uint8_t>(static_cast<uint64_t>(n)>>shift));
}
void append(blob& b, const std::string& s) {
    append(b,static_cast<int64_t>(s.size())); b.insert(b.end(),s.begin(),s.end());
}
blob encode(const receive_install_identity& i, const receive_install_limits& limits) {
    identity(i,limits);
    blob b;
    const auto n=encoded_size(i);
    b.reserve(static_cast<size_t>(n));
    append(b,1); append(b,i.sequence); append(b,i.expected_revision);
    append(b,static_cast<int64_t>(i.base.kind)); append(b,i.base.position.value_or(0));
    append(b,i.head); append(b,static_cast<int64_t>(i.mode));
    append(b,i.request_digest); append(b,i.receipt_digest); append(b,i.content_digest); append(b,i.manifest_digest);
    return b;
}
struct decoder {
    const blob& b; size_t offset=0;
    int64_t number() {
        if (b.size()-offset < 8) fail(code::corrupt_state,"receiver installation truncated identity");
        uint64_t n=0;
        for (int j=0;j<8;++j) n=(n<<8)|b[offset++];
        if (n>static_cast<uint64_t>(maximum)) fail(code::corrupt_state,"receiver installation unrepresentable identity");
        return static_cast<int64_t>(n);
    }
    std::string string(const receive_install_limits& limits) {
        const auto n=number();
        if (n<=0 || n>limits.field_bytes || static_cast<uint64_t>(n)>b.size()-offset)
            fail(code::corrupt_state,"receiver installation invalid encoded digest length");
        std::string s(b.begin()+offset,b.begin()+offset+static_cast<size_t>(n));
        offset+=static_cast<size_t>(n); return s;
    }
};
receive_install_identity decode(const blob& b, const receive_install_limits& limits) {
    decoder d{b};
    if (d.number()!=1) fail(code::corrupt_state,"receiver installation unsupported identity encoding");
    receive_install_identity i;
    i.sequence=d.number(); i.expected_revision=d.number();
    i.base.kind=static_cast<receive_frontier_kind>(d.number());
    const auto position=d.number();
    if (i.base.kind==receive_frontier_kind::position) i.base.position=position;
    else if (position!=0) fail(code::corrupt_state,"receiver installation noncanonical NULL frontier");
    i.head=d.number(); i.mode=static_cast<receive_install_mode>(d.number());
    i.request_digest=d.string(limits); i.receipt_digest=d.string(limits); i.content_digest=d.string(limits); i.manifest_digest=d.string(limits);
    if (d.offset!=b.size()) fail(code::corrupt_state,"receiver installation trailing identity bytes");
    identity(i,limits,code::corrupt_state); return i;
}
const blob& bounded_blob(const row_t& r, const std::string& name, int64_t limit) {
    auto it=r.find(name);
    if (it==r.end() || !std::holds_alternative<blob>(it->second))
        fail(code::corrupt_state,"receiver installation wrong type or oversized encoded field");
    const auto& b=std::get<blob>(it->second);
    if (b.empty() || b.size()>static_cast<uint64_t>(limit))
        fail(code::corrupt_state,"receiver installation empty or oversized encoded field");
    return b;
}
std::string bounded_string(const row_t& r, const std::string& name, int64_t limit) {
    const auto& b=bounded_blob(r,name,limit); return {b.begin(),b.end()};
}
std::string integer_projection(const std::string& name) {
    return "CASE WHEN typeof("+name+")='integer' THEN "+name+" ELSE NULL END AS "+name;
}
std::string bounded_projection(const std::string& name, int64_t limit) {
    // CASE bounds SQLite's result before database::query can copy TEXT/BLOB.
    return "CASE WHEN typeof("+name+")='blob' AND length("+name+") BETWEEN 1 AND "+
        std::to_string(limit)+" THEN "+name+" ELSE NULL END AS "+name;
}
int64_t row_size(const receive_install_snapshot& s, const receive_install_limits& limits) {
    auto n=binding_size(s.binding,limits);
    if (s.active) n=sum(n,encoded_size(*s.active));
    if (s.last_installed) n=sum(n,encoded_size(*s.last_installed));
    return n;
}
void covered(const std::optional<receive_install_snapshot>& s,const receive_install_usage& u,const receive_install_limits& limits) {
    if (s && (u.channels<1 || u.encoded_bytes<row_size(*s,limits)))
        fail(code::corrupt_state,"receiver installation counters do not cover addressed row");
}
void valid_row(const receive_install_snapshot& s, const receive_install_limits& limits) {
    binding_size(s.binding,limits,code::corrupt_state); frontier(s.frontier,code::corrupt_state);
    if (s.revision<0 || s.last_sequence<0 ||
        ((s.frontier.kind==receive_frontier_kind::position)!=(s.revision>0)) ||
        ((s.revision>0)!=s.last_installed.has_value()))
        fail(code::corrupt_state,"receiver installation contradictory durable frontier/revision");
    if (s.last_installed) {
        const auto& i=*s.last_installed; identity(i,limits,code::corrupt_state);
        if (i.expected_revision+1!=s.revision || !s.frontier.position || i.head!=*s.frontier.position ||
            i.sequence>s.last_sequence) fail(code::corrupt_state,"receiver installation invalid retained result");
    }
    if (s.active) {
        const auto& i=*s.active; identity(i,limits,code::corrupt_state);
        if (i.sequence!=s.last_sequence || i.expected_revision!=s.revision || i.base!=s.frontier ||
            (s.last_installed && i.sequence<=s.last_installed->sequence))
            fail(code::corrupt_state,"receiver installation invalid active attempt");
    }
    // Explicitly abandoned committed attempts leave no active record but do
    // consume sequence numbers. High water is never an installed-result proof;
    // only exact last_installed equality can support the retained retry path.
}
void changed(database& db) {
    if (db.changes()!=1) throw db_error("receiver installation write did not change its expected row");
}
template<class F> auto atomic(database& db,F&& f) {
    db.execute("SAVEPOINT lattice_receive_install");
    try { auto result=f(); db.execute("RELEASE lattice_receive_install"); return result; }
    catch (...) {
        auto primary=std::current_exception();
        if (!db.is_in_transaction()) std::rethrow_exception(primary);
        try { db.execute("ROLLBACK TO lattice_receive_install"); db.execute("RELEASE lattice_receive_install"); }
        catch (...) { throw receive_install_error(code::cleanup_failed,
            "receiver installation cleanup failed; abort owning transaction",primary,std::current_exception()); }
        std::rethrow_exception(primary);
    }
}
void bound(const receive_install_snapshot& s,const receive_install_binding& b) {
    if (s.binding!=b) fail(code::binding_mismatch,"receiver installation immutable binding differs");
}
receive_install_receipt retry(const receive_install_snapshot& s) {
    return {receive_install_disposition::already_installed,s.revision,*s.frontier.position};
}
}

receive_install_store::receive_install_store(std::shared_ptr<lattice_db> owner,receive_install_limits limits)
    : owner_(std::move(owner)),limits_(limits) {
    if (!owner_ || limits.channels<0 || limits.field_bytes<=0 || limits.field_bytes>(maximum-176)/16 || limits.encoded_bytes<0)
        fail(code::invalid_argument,"receiver installation requires owner and representable explicit limits");
}
database& receive_install_store::connection() const {
    auto* db=recovery_writer_access::active_writer(*owner_);
    if (!db) fail(code::transaction_required,"receiver installation requires this thread's owned active main WRITE transaction");
    return *db;
}
receive_install_usage receive_install_store::configuration() const {
    std::string sql="SELECT ";
    for (auto name : {"id","version","max_channels","max_field_bytes","max_bytes","channels","bytes"}) {
        if (sql!="SELECT ") sql+=",";
        sql+=integer_projection(name);
    }
    auto rows=connection().query(sql+" FROM main._lattice_install_store LIMIT 2");
    if (rows.size()!=1 || integer(rows[0],"id")!=1 || integer(rows[0],"version")!=1)
        fail(code::corrupt_state,"receiver installation missing/unsupported fixed metadata");
    const auto& r=rows[0];
    if (receive_install_limits{integer(r,"max_channels"),integer(r,"max_field_bytes"),integer(r,"max_bytes")}!=limits_)
        fail(code::limits_mismatch,"receiver installation explicit limits differ from durable configuration");
    receive_install_usage u{integer(r,"channels"),integer(r,"bytes")};
    if (!fits(u.channels,0,limits_.channels) || !fits(u.encoded_bytes,0,limits_.encoded_bytes))
        fail(code::corrupt_state,"receiver installation invalid durable usage");
    return u;
}
std::optional<receive_install_snapshot> receive_install_store::row(const std::string& channel) const {
    field(channel,limits_);
    const auto identity_limit=std::min(88+4*limits_.field_bytes,limits_.encoded_bytes);
    // Indexed scalar preflight caps the total before copying any stored BLOB,
    // including corrupt rows whose individual fields fit but combined do not.
    std::string preflight="SELECT ";
    for (auto name : {"channel","authority","source","epoch","scope","schema_digest","active","last_install"}) {
        if (preflight!="SELECT ") preflight+=",";
        const std::string column=name;
        const bool optional=column=="active" || column=="last_install";
        preflight+="CASE ";
        if (optional) preflight+="WHEN "+column+" IS NULL THEN 0 ";
        preflight+="WHEN typeof("+column+")='blob' THEN length("+column+") ELSE -1 END AS "+column;
    }
    const auto sizes=connection().query(preflight+" FROM main._lattice_install_channel WHERE channel=? LIMIT 2",{bytes(channel)});
    if (sizes.empty()) return std::nullopt;
    if (sizes.size()!=1) fail(code::corrupt_state,"receiver installation duplicate channel");
    int64_t total=0;
    for (auto name : {"channel","authority","source","epoch","scope","schema_digest","active","last_install"}) {
        const std::string column=name;
        const bool optional=column=="active" || column=="last_install";
        const auto n=integer(sizes[0],column);
        if (n<(optional ? 0 : 1) || n>(optional ? identity_limit : limits_.field_bytes) || !fits(total,n,limits_.encoded_bytes))
            fail(code::corrupt_state,"receiver installation stored fields exceed type/byte limits");
        total+=n;
    }
    std::string sql="SELECT active IS NULL AS active_null,last_install IS NULL AS last_null,typeof(frontier) AS frontier_type";
    for (auto name : {"frontier_kind","frontier","revision","last_sequence","bytes"}) sql+=","+integer_projection(name);
    for (auto name : {"channel","authority","source","epoch","scope","schema_digest"})
        sql+=","+bounded_projection(name,limits_.field_bytes);
    sql+=","+bounded_projection("active",identity_limit)+","+bounded_projection("last_install",identity_limit)+
        " FROM main._lattice_install_channel WHERE channel=? LIMIT 2";
    const auto rows=connection().query(sql,{bytes(channel)});
    if (rows.empty()) return std::nullopt;
    if (rows.size()!=1) fail(code::corrupt_state,"receiver installation duplicate channel");
    const auto& r=rows[0]; receive_install_snapshot s;
    s.binding={bounded_string(r,"channel",limits_.field_bytes),bounded_string(r,"authority",limits_.field_bytes),
        bounded_string(r,"source",limits_.field_bytes),bounded_string(r,"epoch",limits_.field_bytes),
        bounded_string(r,"scope",limits_.field_bytes),bounded_string(r,"schema_digest",limits_.field_bytes)};
    s.frontier.kind=static_cast<receive_frontier_kind>(integer(r,"frontier_kind"));
    const auto type=std::get<std::string>(r.at("frontier_type"));
    if (type=="integer") s.frontier.position=integer(r,"frontier");
    else if (type!="null") fail(code::corrupt_state,"receiver installation frontier has wrong storage type");
    s.revision=integer(r,"revision"); s.last_sequence=integer(r,"last_sequence");
    if (!integer(r,"active_null")) s.active=decode(bounded_blob(r,"active",identity_limit),limits_);
    if (!integer(r,"last_null")) s.last_installed=decode(bounded_blob(r,"last_install",identity_limit),limits_);
    valid_row(s,limits_);
    if (s.binding.channel!=channel || integer(r,"bytes")!=row_size(s,limits_) || integer(r,"bytes")>limits_.encoded_bytes)
        fail(code::corrupt_state,"receiver installation row identity/usage mismatch");
    return s;
}
void receive_install_store::initialize() {
    auto& db=connection();
    auto tables=db.query("SELECT name FROM main.sqlite_master WHERE name IN ('_lattice_install_store','_lattice_install_channel')");
    if (!tables.empty()) {
        if (tables.size()!=2) fail(code::corrupt_state,"receiver installation partial schema; migration refused");
        audit(); return;
    }
    atomic(db,[&] {
        db.execute("CREATE TABLE main._lattice_install_store(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,"
            "max_channels INTEGER NOT NULL,max_field_bytes INTEGER NOT NULL,max_bytes INTEGER NOT NULL,"
            "channels INTEGER NOT NULL,bytes INTEGER NOT NULL) WITHOUT ROWID");
        db.execute("CREATE TABLE main._lattice_install_channel(channel BLOB PRIMARY KEY NOT NULL,authority BLOB NOT NULL,"
            "source BLOB NOT NULL,epoch BLOB NOT NULL,scope BLOB NOT NULL,schema_digest BLOB NOT NULL,"
            "frontier_kind INTEGER NOT NULL,frontier INTEGER,revision INTEGER NOT NULL,last_sequence INTEGER NOT NULL,"
            "active BLOB,last_install BLOB,bytes INTEGER NOT NULL,UNIQUE(authority,scope)) WITHOUT ROWID");
        db.execute("INSERT INTO main._lattice_install_store VALUES(1,1,?,?,?,0,0)",
            {limits_.channels,limits_.field_bytes,limits_.encoded_bytes}); changed(db);
        if (configuration()!=receive_install_usage{}) fail(code::corrupt_state,"receiver installation initialization was altered");
        return true;
    });
}
void receive_install_store::audit() const {
    const auto expected=configuration();
    receive_install_usage actual;
    std::optional<std::string> previous;
    for (;;) {
        const auto sql="SELECT "+bounded_projection("channel",limits_.field_bytes)+" FROM main._lattice_install_channel"+
            (previous ? " WHERE channel>?" : "")+" ORDER BY channel LIMIT 1";
        auto keys=previous ? connection().query(sql,{bytes(*previous)}) : connection().query(sql);
        if (keys.empty()) break;
        auto key=bounded_string(keys[0],"channel",limits_.field_bytes);
        const auto s=row(key);
        if (!s) fail(code::corrupt_state,"receiver installation channel disappeared during owned audit");
        if (connection().query("SELECT 1 FROM main._lattice_install_channel WHERE authority=? AND scope=? LIMIT 2",
                {bytes(s->binding.authority),bytes(s->binding.scope)}).size()!=1)
            fail(code::corrupt_state,"receiver installation duplicate authority/scope alias");
        const auto n=row_size(*s,limits_);
        if (!fits(actual.channels,1,limits_.channels) || !fits(actual.encoded_bytes,n,limits_.encoded_bytes))
            fail(code::corrupt_state,"receiver installation actual usage exceeds limits");
        ++actual.channels; actual.encoded_bytes+=n; previous=std::move(key);
    }
    if (actual!=expected) fail(code::corrupt_state,"receiver installation durable usage differs from full audit");
}
receive_install_usage receive_install_store::usage() const { return configuration(); }
std::optional<receive_install_snapshot> receive_install_store::read(const std::string& channel) const {
    const auto u=configuration(); auto s=row(channel); covered(s,u,limits_); return s;
}
void receive_install_store::write_row(const receive_install_snapshot& next,const receive_install_snapshot* prior) {
    valid_row(next,limits_);
    auto& db=connection(); const auto before=configuration();
    const auto old_bytes=prior ? row_size(*prior,limits_) : 0;
    const auto next_bytes=row_size(next,limits_);
    if (before.encoded_bytes<old_bytes || (prior && before.channels==0))
        fail(code::corrupt_state,"receiver installation counters do not cover addressed row");
    const auto count=prior ? before.channels : sum(before.channels,1);
    if (count>limits_.channels || !fits(before.encoded_bytes-old_bytes,next_bytes,limits_.encoded_bytes))
        fail(code::capacity,"receiver installation explicit storage budget exhausted");
    const auto total=before.encoded_bytes-old_bytes+next_bytes;
    const column_value_t head=next.frontier.position ? column_value_t{*next.frontier.position} : column_value_t{nullptr};
    const column_value_t active=next.active ? column_value_t{encode(*next.active,limits_)} : column_value_t{nullptr};
    const column_value_t last=next.last_installed ? column_value_t{encode(*next.last_installed,limits_)} : column_value_t{nullptr};
    const auto& b=next.binding;
    if (prior) {
        // Called only under owned WRITE admission, without callback/release
        // between addressed read and update. Readback rejects trigger rewrites.
        db.execute("UPDATE main._lattice_install_channel SET frontier_kind=?,frontier=?,revision=?,last_sequence=?,active=?,last_install=?,bytes=? WHERE channel=?",
            {static_cast<int64_t>(next.frontier.kind),head,next.revision,next.last_sequence,active,last,next_bytes,bytes(b.channel)});
    } else db.execute("INSERT INTO main._lattice_install_channel VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        {bytes(b.channel),bytes(b.authority),bytes(b.source),bytes(b.epoch),bytes(b.scope),bytes(b.schema),
         static_cast<int64_t>(next.frontier.kind),head,next.revision,next.last_sequence,active,last,next_bytes});
    changed(db);
    if (row(b.channel)!=std::optional<receive_install_snapshot>{next}) fail(code::corrupt_state,"receiver installation row write was altered");
    db.execute("UPDATE main._lattice_install_store SET channels=?,bytes=? WHERE id=1 AND channels=? AND bytes=?",
        {count,total,before.channels,before.encoded_bytes}); changed(db);
    if (configuration()!=receive_install_usage{count,total}) fail(code::corrupt_state,"receiver installation counter write was altered");
}
void receive_install_store::bind(const receive_install_binding& b) {
    binding_size(b,limits_); const auto u=configuration();
    auto& db=connection();
    atomic(db,[&] {
        const auto existing=row(b.channel); covered(existing,u,limits_);
        if (existing) { bound(*existing,b); return true; }
        if (!db.query("SELECT 1 FROM main._lattice_install_channel WHERE authority=? AND scope=? LIMIT 1",{bytes(b.authority),bytes(b.scope)}).empty())
            fail(code::alias,"receiver installation authority/scope already belongs to another channel");
        receive_install_snapshot s; s.binding=b; write_row(s,nullptr); return true;
    });
}
receive_install_receipt receive_install_store::begin(const receive_install_binding& b,const receive_install_identity& i) {
    binding_size(b,limits_); identity(i,limits_); const auto u=configuration();
    auto& db=connection();
    return atomic(db,[&] {
        const auto current=row(b.channel); covered(current,u,limits_);
        if (!current) fail(code::binding_mismatch,"receiver installation channel is not bound");
        bound(*current,b);
        if (current->last_installed==std::optional<receive_install_identity>{i}) return retry(*current);
        if (current->active==std::optional<receive_install_identity>{i})
            return receive_install_receipt{receive_install_disposition::active,current->revision,i.head};
        if (current->last_sequence==maximum) fail(code::sequence_exhausted,"receiver installation sequence exhausted");
        if (i.sequence<=current->last_sequence || i.expected_revision!=current->revision || i.base!=current->frontier)
            fail(code::stale,"receiver installation sequence, revision or base is stale");
        if (current->active) fail(code::active_conflict,"receiver installation active attempt requires explicit resolution");
        if (i.sequence!=current->last_sequence+1) fail(code::stale,"receiver installation requires the next committed sequence");
        auto next=*current; next.active=i; next.last_sequence=i.sequence; write_row(next,&*current);
        return receive_install_receipt{receive_install_disposition::active,current->revision,i.head};
    });
}
receive_install_receipt receive_install_store::complete(const receive_install_binding& b,const receive_install_identity& i,
    const std::optional<receive_install_identity>& supersede) {
    binding_size(b,limits_); identity(i,limits_); const auto u=configuration();
    auto& db=connection();
    return atomic(db,[&] {
        const auto current=row(b.channel); covered(current,u,limits_);
        if (!current) fail(code::binding_mismatch,"receiver installation channel is not bound");
        bound(*current,b);
        if (current->last_installed==std::optional<receive_install_identity>{i}) return retry(*current);
        if (current->active!=std::optional<receive_install_identity>{i} || current->revision!=i.expected_revision || current->frontier!=i.base)
            fail(code::stale,"receiver installation completion does not own the active revision/base");
        if (current->last_installed!=supersede)
            fail(code::supersession_required,"receiver installation requires exact explicit prior-result supersession");
        if (current->revision==maximum) fail(code::sequence_exhausted,"receiver installation revision exhausted");
        auto next=*current; next.frontier={receive_frontier_kind::position,i.head};
        ++next.revision; next.last_installed=i; next.active.reset(); write_row(next,&*current);
        return receive_install_receipt{receive_install_disposition::installed,next.revision,i.head};
    });
}
receive_install_store::journal_snapshot receive_install_store::snapshot_for_journal() const {
    audit();
    journal_snapshot result{configuration(),{}};
    receive_install_usage observed;
    std::optional<std::string> after;
    for(;;){
        const auto sql="SELECT "+bounded_projection("channel",limits_.field_bytes)+" FROM main._lattice_install_channel"+
            (after?" WHERE channel>?":"")+" ORDER BY channel LIMIT 1";
        const auto keys=after?connection().query(sql,{bytes(*after)}):connection().query(sql);
        if(keys.empty())break;
        if(observed.channels>=limits_.channels)fail(code::corrupt_state,"receiver cancellation snapshot channel cap exceeded");
        auto key=bounded_string(keys[0],"channel",limits_.field_bytes);
        auto current=row(key);
        if(!current)fail(code::corrupt_state,"receiver cancellation snapshot channel disappeared");
        const auto charge=row_size(*current,limits_);
        if(!fits(observed.encoded_bytes,charge,limits_.encoded_bytes))
            fail(code::corrupt_state,"receiver cancellation snapshot byte cap exceeded");
        ++observed.channels;observed.encoded_bytes+=charge;
        result.channels.push_back(std::move(*current));after=std::move(key);
    }
    if(observed!=result.usage||configuration()!=result.usage)
        fail(code::corrupt_state,"receiver cancellation snapshot usage changed");
    return result;
}
receive_install_snapshot receive_install_store::retire_unstarted_for_journal(
    const receive_install_snapshot& expected,int64_t sequence) {
    auto& db=connection();
    return atomic(db,[&] {
        const auto current=read(expected.binding.channel);
        if (!current || *current!=expected || current->active || sequence<=0 ||
            (current->last_installed && current->last_installed->sequence>=sequence))
            fail(code::stale,"receiver journal cancellation baseline differs or attempt installed");
        // An explicitly abandoned active identity already consumed this number.
        if (sequence==current->last_sequence) return *current;
        if (current->last_sequence==maximum)
            fail(code::sequence_exhausted,"receiver journal cancellation sequence exhausted");
        if (sequence!=current->last_sequence+1)
            fail(code::stale,"receiver journal cancellation must retire the exact next sequence");
        auto next=*current;
        next.last_sequence=sequence;
        write_row(next,&*current);
        return next;
    });
}
void receive_install_store::abandon_active(const receive_install_binding& b,const receive_install_identity& i) {
    binding_size(b,limits_);identity(i,limits_);const auto u=configuration();auto& db=connection();
    atomic(db,[&] {
        const auto current=row(b.channel);covered(current,u,limits_);
        if(!current)fail(code::binding_mismatch,"receiver installation channel is not bound");
        bound(*current,b);
        if(current->active!=std::optional<receive_install_identity>{i} || current->revision!=i.expected_revision || current->frontier!=i.base)
            fail(code::stale,"receiver abandonment requires exact active identity");
        auto next=*current;next.active.reset();write_row(next,&*current);return true;
    });
}
receive_install_receipt receive_install_store::apply_if_new(const receive_install_binding& b,const receive_install_identity& i,
    const std::optional<receive_install_identity>& supersede,const std::function<void(database&)>& effects) {
    if (!effects) fail(code::invalid_argument,"receiver installation requires explicit trusted effects");
    auto& db=connection();
    return atomic(db,[&] {
        auto admission=begin(b,i);
        if (admission.disposition==receive_install_disposition::already_installed) return admission;
        // Refuse missing/stale explicit supersession before any effects.
        const auto current=row(b.channel);
        if (!current || current->last_installed!=supersede)
            fail(code::supersession_required,"receiver installation requires exact prior-result supersession before effects");
        effects(db);
        if (&connection()!=&db) fail(code::transaction_required,"receiver installation writer changed during effects");
        auto result=complete(b,i,supersede);
        if (result.disposition!=receive_install_disposition::installed)
            fail(code::stale,"receiver installation effects changed their own bookkeeping");
        return result;
    });
}
} // namespace lattice::detail
