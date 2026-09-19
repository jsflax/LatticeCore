#include "recovery_obligation_store.hpp"
#include "recovery_writer_access.hpp"
#include "canonical_writer_adapter.hpp"
#include <algorithm>
#include <limits>
#include <set>

namespace lattice::detail {
namespace {
using code=recovery_obligation_error_code;
using scope_t=recovery_obligation_scope;
using entry_t=recovery_obligation_entry;
using mode=recovery_obligation_mode;
using stage=recovery_obligation_stage;
using origin=recovery_obligation_origin;
using row=database::row_t;
using blob=std::vector<uint8_t>;
constexpr int64_t maximum=std::numeric_limits<int64_t>::max();
[[noreturn]] void fail(code c,const char* m) { throw recovery_obligation_error(c,m); }
bool fits(int64_t n,int64_t extra,int64_t limit) { return n>=0 && extra>=0 && n<=limit && extra<=limit-n; }
int64_t add(int64_t a,int64_t b,code c=code::corrupt_state) {
    if (!fits(a,b,maximum)) fail(c,"obligation integer/byte budget overflow");
    return a+b;
}
int64_t next(int64_t n) {
    if (n<0 || n==maximum) fail(code::exhausted,"obligation committed sequence exhausted");
    return n+1;
}
blob bytes(const std::string& s) { return {s.begin(),s.end()}; }
void field(const std::string& s,const recovery_obligation_limits& l,code c=code::invalid_argument) {
    if (s.empty() || s.size()>static_cast<uint64_t>(l.field_bytes)) fail(c,"obligation field exceeds explicit limit");
}
std::string uuid(const std::string& s,code c=code::invalid_argument) {
    try { return canonical_writer_adapter::uuid_key(s); }
    catch (const db_error&) { fail(c,"obligation requires a strict UUID identity"); }
}
int64_t number(const row& r,const std::string& n) {
    auto it=r.find(n);
    if (it==r.end() || !std::holds_alternative<int64_t>(it->second)) fail(code::corrupt_state,"obligation expected integer metadata");
    return std::get<int64_t>(it->second);
}
std::optional<int64_t> optional_number(const row& r,const std::string& n) {
    if (number(r,n+"_null")==1) return {};
    return number(r,n);
}
std::string string(const row& r,const std::string& n,bool empty=false) {
    auto it=r.find(n);
    if (it==r.end() || !std::holds_alternative<blob>(it->second)) fail(code::corrupt_state,"obligation invalid or oversized byte field");
    const auto& b=std::get<blob>(it->second);
    if (!empty && b.empty()) fail(code::corrupt_state,"obligation empty required field");
    return {b.begin(),b.end()};
}
std::string ints(std::initializer_list<const char*> names) {
    std::string result;
    for (auto n:names) {
        if (!result.empty()) result+=",";
        result+="CASE WHEN typeof("+std::string(n)+")='integer' THEN "+n+" ELSE NULL END AS "+n;
    }
    return result;
}
std::string blobs(std::initializer_list<const char*> names,const recovery_obligation_limits& l) {
    // Bound the combined stored row before database::query copies any bytes.
    std::string total="0";
    for (auto n:names) total+="+length("+std::string(n)+")";
    std::string result;
    for (auto n:names) result+=",CASE WHEN typeof("+std::string(n)+")='blob' AND length("+n+")<="+
        std::to_string(l.field_bytes)+" AND ("+total+")<="+std::to_string(l.encoded_bytes)+" THEN "+n+" ELSE NULL END AS "+n;
    return result;
}
void changed(database& db) { if (db.changes()!=1) throw db_error("obligation write did not change its expected row"); }
template<class F> auto atomic(database& db,F&& f) {
    db.execute("SAVEPOINT lattice_recovery_obligation");
    try { auto result=f(); db.execute("RELEASE lattice_recovery_obligation"); return result; }
    catch (...) {
        auto primary=std::current_exception();
        if (!db.is_in_transaction()) std::rethrow_exception(primary);
        try { db.execute("ROLLBACK TO lattice_recovery_obligation"); db.execute("RELEASE lattice_recovery_obligation"); }
        catch (...) { throw recovery_obligation_error(code::cleanup_failed,"obligation cleanup failed; abort outer transaction",primary,std::current_exception()); }
        std::rethrow_exception(primary);
    }
}
int64_t scope_size(const scope_t& s,const recovery_obligation_limits& l,code c=code::invalid_argument) {
    int64_t n=13*8;
    const auto& b=s.profile.binding;
    for (auto* f:{&b.channel,&b.authority,&b.source,&b.epoch,&b.scope,&b.schema,&s.profile.profile_digest,&s.profile.receipt_namespace}) {
        field(*f,l,c); n=add(n,static_cast<int64_t>(f->size()),c);
    }
    if (!s.installed_manifest.empty()) field(s.installed_manifest,l,c);
    return add(n,static_cast<int64_t>(s.installed_manifest.size()),c);
}
int64_t entry_size(const entry_t& e,const recovery_obligation_limits& l,const std::string& channel,code c=code::invalid_argument) {
    field(channel,l,c); int64_t n=add(8*8,static_cast<int64_t>(channel.size()),c);
    for (auto* f:{&e.record.original_id,&e.record.table,&e.record.target_id,&e.canonical_original_id,&e.canonical_target_id}) {
        field(*f,l,c); n=add(n,static_cast<int64_t>(f->size()),c);
    }
    return n; // ACK namespace is the immutable scope namespace, not another copy.
}
struct global {
    recovery_obligation_usage usage;
    int64_t incarnation=0,record=0,export_claim=0;
    bool operator==(const global&) const=default;
};
struct schema_definition { const char* name; const char* sql; };
constexpr schema_definition definitions[]={
    {"_lattice_obligation_store","CREATE TABLE main._lattice_obligation_store(id INTEGER PRIMARY KEY,version INTEGER NOT NULL,max_scopes INTEGER NOT NULL,max_records INTEGER NOT NULL,max_field INTEGER NOT NULL,max_bytes INTEGER NOT NULL,scopes INTEGER NOT NULL,records INTEGER NOT NULL,bytes INTEGER NOT NULL,incarnation INTEGER NOT NULL,record_sequence INTEGER NOT NULL,export_sequence INTEGER NOT NULL) WITHOUT ROWID"},
    {"_lattice_obligation_scope","CREATE TABLE main._lattice_obligation_scope(channel BLOB PRIMARY KEY,authority BLOB NOT NULL,source BLOB NOT NULL,epoch BLOB NOT NULL,scope BLOB NOT NULL,schema_digest BLOB NOT NULL,profile_digest BLOB NOT NULL,receipt_namespace BLOB NOT NULL,incarnation INTEGER NOT NULL UNIQUE,generation INTEGER NOT NULL,revision INTEGER NOT NULL,last_attempt INTEGER NOT NULL,freeze_revision INTEGER NOT NULL,freeze_record INTEGER NOT NULL,freeze_export INTEGER NOT NULL,mode INTEGER NOT NULL,installed_sequence INTEGER NOT NULL,installed_revision INTEGER NOT NULL,installed_head INTEGER NOT NULL,installed_manifest BLOB NOT NULL,bytes INTEGER NOT NULL,UNIQUE(authority,scope)) WITHOUT ROWID"},
    {"_lattice_obligation_entry","CREATE TABLE main._lattice_obligation_entry(channel BLOB NOT NULL,original BLOB NOT NULL,audit_id INTEGER NOT NULL,actual_original BLOB NOT NULL,table_name BLOB NOT NULL,target BLOB NOT NULL,actual_target BLOB NOT NULL,origin INTEGER NOT NULL,record_sequence INTEGER NOT NULL UNIQUE,first_export INTEGER,stage INTEGER NOT NULL,ack_position INTEGER,ack_outcome INTEGER,settled_sequence INTEGER NOT NULL,bytes INTEGER NOT NULL,PRIMARY KEY(channel,original),UNIQUE(channel,audit_id)) WITHOUT ROWID"},
    {"_lattice_obligation_audit","CREATE INDEX main._lattice_obligation_audit ON _lattice_obligation_entry(audit_id,channel)"},
    {"_lattice_obligation_original","CREATE INDEX main._lattice_obligation_original ON _lattice_obligation_entry(original,audit_id)"},
};
struct backend {
    database& db;
    const recovery_obligation_limits& l;
    global config() const {
        auto rows=db.query("SELECT "+ints({"id","version","max_scopes","max_records","max_field","max_bytes","scopes","records","bytes","incarnation","record_sequence","export_sequence"})+
            " FROM main._lattice_obligation_store LIMIT 2");
        if (rows.size()!=1 || number(rows[0],"id")!=1 || number(rows[0],"version")!=1) fail(code::corrupt_state,"obligation missing/unsupported fixed metadata");
        const auto& r=rows[0];
        if (recovery_obligation_limits{number(r,"max_scopes"),number(r,"max_records"),number(r,"max_field"),number(r,"max_bytes")}!=l)
            fail(code::limits_mismatch,"obligation limits differ from durable configuration");
        global g{{number(r,"scopes"),number(r,"records"),number(r,"bytes")},number(r,"incarnation"),number(r,"record_sequence"),number(r,"export_sequence")};
        if (!fits(g.usage.scopes,0,l.scopes) || !fits(g.usage.records,0,l.records) || !fits(g.usage.encoded_bytes,0,l.encoded_bytes) ||
            g.incarnation<g.usage.scopes || g.record<g.usage.records || g.export_claim<0)
            fail(code::corrupt_state,"obligation invalid fixed counters");
        return g;
    }
    void put_global(const global& old,const global& g) {
        if (!fits(g.usage.scopes,0,l.scopes) || !fits(g.usage.records,0,l.records) || !fits(g.usage.encoded_bytes,0,l.encoded_bytes))
            fail(code::capacity,"obligation count or byte capacity exhausted; no evidence was evicted");
        db.execute("UPDATE main._lattice_obligation_store SET scopes=?,records=?,bytes=?,incarnation=?,record_sequence=?,export_sequence=? "
            "WHERE id=1 AND scopes=? AND records=? AND bytes=? AND incarnation=? AND record_sequence=? AND export_sequence=?",
            {g.usage.scopes,g.usage.records,g.usage.encoded_bytes,g.incarnation,g.record,g.export_claim,
             old.usage.scopes,old.usage.records,old.usage.encoded_bytes,old.incarnation,old.record,old.export_claim}); changed(db);
        if (config()!=g) fail(code::corrupt_state,"obligation counter write postimage differs");
    }
    std::optional<scope_t> scope(const std::string& channel) const {
        field(channel,l);
        auto rows=db.query("SELECT "+ints({"incarnation","generation","revision","last_attempt","freeze_revision","freeze_record","freeze_export","mode","installed_sequence","installed_revision","installed_head","bytes"})+
            blobs({"channel","authority","source","epoch","scope","schema_digest","profile_digest","receipt_namespace","installed_manifest"},l)+
            " FROM main._lattice_obligation_scope WHERE channel=? LIMIT 2",{bytes(channel)});
        if (rows.empty()) return {};
        if (rows.size()!=1) fail(code::corrupt_state,"obligation duplicate channel");
        const auto& r=rows[0]; scope_t s;
        s.profile={{string(r,"channel"),string(r,"authority"),string(r,"source"),string(r,"epoch"),string(r,"scope"),string(r,"schema_digest")},string(r,"profile_digest"),string(r,"receipt_namespace")};
        s.address={s.profile.binding.channel,number(r,"incarnation"),number(r,"generation")};
        s.revision=number(r,"revision"); s.last_attempt=number(r,"last_attempt"); s.freeze_revision=number(r,"freeze_revision");
        s.freeze_record_high_water=number(r,"freeze_record"); s.freeze_export_high_water=number(r,"freeze_export");
        s.mode=static_cast<mode>(number(r,"mode")); s.installed_sequence=number(r,"installed_sequence");
        s.installed_revision=number(r,"installed_revision"); s.installed_head=number(r,"installed_head"); s.installed_manifest=string(r,"installed_manifest",true);
        if (s.address.incarnation<=0 || s.address.generation<=0 || s.revision<=0 || s.last_attempt<0 ||
            s.freeze_revision<0 || s.freeze_revision>s.revision || s.freeze_record_high_water<0 || s.freeze_export_high_water<0 ||
            s.mode<mode::recording || s.mode>mode::installed || s.installed_sequence<0 || s.installed_revision<0 || s.installed_head<0 ||
            s.installed_sequence>s.last_attempt || ((s.installed_sequence>0)!=(s.installed_revision>0)) ||
            ((s.installed_sequence>0)!=(!s.installed_manifest.empty())) || (!s.installed_sequence && s.installed_head!=0) ||
            (s.mode!=mode::recording && (!s.last_attempt || !s.freeze_revision)) ||
            (s.mode==mode::installed && s.installed_sequence!=s.last_attempt) ||
            scope_size(s,l,code::corrupt_state)!=number(r,"bytes")) fail(code::corrupt_state,"obligation contradictory scope state");
        return s;
    }
    scope_t current(const recovery_obligation_address& a) const {
        const auto g=config(); auto s=scope(a.channel);
        if (!s || s->address!=a) fail(code::stale,"obligation stale channel incarnation/generation");
        if (g.usage.scopes<1 || g.usage.encoded_bytes<scope_size(*s,l) || g.incarnation<s->address.incarnation ||
            g.record<s->freeze_record_high_water || g.export_claim<s->freeze_export_high_water)
            fail(code::corrupt_state,"obligation counters do not cover addressed scope");
        return *s;
    }
    void put_scope(const std::optional<scope_t>& old,const scope_t& s) {
        const auto expected_global=config();
        const auto& b=s.profile.binding;
        std::vector<column_value_t> values={bytes(b.authority),bytes(b.source),bytes(b.epoch),bytes(b.scope),bytes(b.schema),
            bytes(s.profile.profile_digest),bytes(s.profile.receipt_namespace),s.address.incarnation,s.address.generation,s.revision,
            s.last_attempt,s.freeze_revision,s.freeze_record_high_water,s.freeze_export_high_water,static_cast<int64_t>(s.mode),
            s.installed_sequence,s.installed_revision,s.installed_head,bytes(s.installed_manifest),scope_size(s,l),bytes(b.channel)};
        if (old) {
            values.push_back(old->address.incarnation); values.push_back(old->address.generation); values.push_back(old->revision);
            db.execute("UPDATE main._lattice_obligation_scope SET authority=?,source=?,epoch=?,scope=?,schema_digest=?,profile_digest=?,receipt_namespace=?,"
                "incarnation=?,generation=?,revision=?,last_attempt=?,freeze_revision=?,freeze_record=?,freeze_export=?,mode=?,"
                "installed_sequence=?,installed_revision=?,installed_head=?,installed_manifest=?,bytes=? WHERE channel=? AND incarnation=? AND generation=? AND revision=?",values);
        } else db.execute("INSERT INTO main._lattice_obligation_scope(authority,source,epoch,scope,schema_digest,profile_digest,receipt_namespace,"
            "incarnation,generation,revision,last_attempt,freeze_revision,freeze_record,freeze_export,mode,installed_sequence,installed_revision,installed_head,installed_manifest,bytes,channel) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",values);
        changed(db);
        if (scope(b.channel)!=s || config()!=expected_global) fail(code::corrupt_state,"obligation scope/counter write postimage differs");
    }
    std::optional<entry_t> entry(const scope_t& s,const std::string& key) const {
        auto rows=db.query("SELECT "+ints({"audit_id","origin","record_sequence","first_export","stage","ack_position","ack_outcome","settled_sequence","bytes"})+
            ",first_export IS NULL AS first_export_null,ack_position IS NULL AS ack_position_null,ack_outcome IS NULL AS ack_outcome_null"+
            blobs({"original","actual_original","table_name","target","actual_target"},l)+
            " FROM main._lattice_obligation_entry WHERE channel=? AND original=? LIMIT 2",{bytes(s.address.channel),bytes(key)});
        if (rows.empty()) return {};
        if (rows.size()!=1) fail(code::corrupt_state,"obligation duplicate original identity");
        const auto& r=rows[0]; entry_t e;
        e.record={number(r,"audit_id"),string(r,"actual_original"),string(r,"table_name"),string(r,"actual_target"),static_cast<origin>(number(r,"origin"))};
        e.canonical_original_id=string(r,"original"); e.canonical_target_id=string(r,"target");
        e.sequence=number(r,"record_sequence"); e.first_export_claim=optional_number(r,"first_export");
        e.stage=static_cast<stage>(number(r,"stage")); e.settled_install_sequence=number(r,"settled_sequence");
        const auto position=optional_number(r,"ack_position"),outcome=optional_number(r,"ack_outcome");
        if (position.has_value()!=outcome.has_value()) fail(code::corrupt_state,"obligation partial ACK metadata");
        if (position) e.acknowledged=recovery_obligation_receipt_claim{e.canonical_original_id,s.profile.receipt_namespace,*position,static_cast<recovery_obligation_outcome>(*outcome)};
        if (e.record.audit_id<=0 || e.sequence<=0 || e.record.origin<origin::local_candidate || e.record.origin>origin::legacy_unknown ||
            e.record.table.find('\0')!=std::string::npos || e.stage<stage::open || e.stage>stage::settled ||
            (e.first_export_claim && *e.first_export_claim<=0) || (position && (*position<0 || *outcome<0 || *outcome>1)) ||
            ((e.stage==stage::open)==e.acknowledged.has_value()) ||
            ((e.stage==stage::settled)!=(e.settled_install_sequence>0)) || e.settled_install_sequence<0 ||
            e.settled_install_sequence>s.installed_sequence || uuid(e.record.original_id,code::corrupt_state)!=e.canonical_original_id ||
            uuid(e.record.target_id,code::corrupt_state)!=e.canonical_target_id || e.canonical_original_id!=key ||
            entry_size(e,l,s.address.channel,code::corrupt_state)!=number(r,"bytes")) fail(code::corrupt_state,"obligation contradictory identity state");
        return e;
    }
    entry_t required(const scope_t& s,const std::string& id) const {
        auto e=entry(s,uuid(id));
        if (!e) fail(code::conflict,"obligation original is not recorded for this scope");
        const auto g=config();
        if (g.usage.records<1 || g.usage.encoded_bytes<entry_size(*e,l,s.address.channel) || g.record<e->sequence ||
            (e->first_export_claim && g.export_claim<*e->first_export_claim)) fail(code::corrupt_state,"obligation counters do not cover addressed identity");
        return *e;
    }
    recovery_obligation_record actual(const recovery_obligation_record& input) const {
        if (input.audit_id<=0 || input.origin<origin::local_candidate || input.origin>origin::legacy_unknown)
            fail(code::invalid_argument,"obligation invalid audit id/origin claim");
        field(input.table,l); field(input.original_id,l); field(input.target_id,l);
        if (input.table.find('\0')!=std::string::npos) fail(code::invalid_argument,"obligation invalid table name");
        const auto original=uuid(input.original_id),target=uuid(input.target_id);
        const std::string total="length(CAST(globalId AS BLOB))+length(CAST(tableName AS BLOB))+length(CAST(globalRowId AS BLOB))";
        const auto project=[&](const char* n) { return "CASE WHEN typeof("+std::string(n)+")='text' AND length(CAST("+n+
            " AS BLOB)) BETWEEN 1 AND "+std::to_string(l.field_bytes)+" AND ("+total+")<="+std::to_string(l.encoded_bytes)+
            " THEN CAST("+n+" AS BLOB) ELSE NULL END AS "+n; };
        auto rows=db.query("SELECT "+project("globalId")+","+project("tableName")+","+project("globalRowId")+","+
            ints({"isFromRemote","synthesized"})+" FROM main.AuditLog WHERE id=? LIMIT 2",{input.audit_id});
        if (rows.size()!=1) fail(code::audit_mismatch,"obligation required original AuditLog row is absent");
        const auto& r=rows[0]; const auto remote=number(r,"isFromRemote"),synthetic=number(r,"synthesized");
        recovery_obligation_record a{input.audit_id,string(r,"globalId"),string(r,"tableName"),string(r,"globalRowId"),input.origin};
        if (uuid(a.original_id,code::audit_mismatch)!=original || a.table!=input.table || uuid(a.target_id,code::audit_mismatch)!=target ||
            remote<0 || remote>1 || synthetic<0 || synthetic>1 || (input.origin==origin::local_candidate && (remote || synthetic)))
            fail(code::audit_mismatch,"obligation audit identity/target/local-origin contradicts original row");
        return a; // unchanged original spellings; no AuditLog body or flags written.
    }
    void check_actual(const entry_t& e) const {
        if (actual(e.record)!=e.record) fail(code::audit_mismatch,"obligation original spelling changed");
    }
    void put_entry(const scope_t& s,const entry_t& e,bool insert) {
        const auto expected_global=config();
        const auto nullable=[](std::optional<int64_t> n)->column_value_t { if (n) return *n; return nullptr; };
        std::vector<column_value_t> values={e.record.audit_id,bytes(e.record.original_id),bytes(e.record.table),bytes(e.canonical_target_id),
            bytes(e.record.target_id),static_cast<int64_t>(e.record.origin),e.sequence,nullable(e.first_export_claim),static_cast<int64_t>(e.stage),
            nullable(e.acknowledged ? std::optional<int64_t>{e.acknowledged->position}:std::nullopt),
            nullable(e.acknowledged ? std::optional<int64_t>{static_cast<int64_t>(e.acknowledged->outcome)}:std::nullopt),
            e.settled_install_sequence,entry_size(e,l,s.address.channel),bytes(s.address.channel),bytes(e.canonical_original_id)};
        if (insert) db.execute("INSERT INTO main._lattice_obligation_entry(audit_id,actual_original,table_name,target,actual_target,origin,record_sequence,first_export,stage,ack_position,ack_outcome,settled_sequence,bytes,channel,original) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",values);
        else db.execute("UPDATE main._lattice_obligation_entry SET audit_id=?,actual_original=?,table_name=?,target=?,actual_target=?,origin=?,record_sequence=?,first_export=?,stage=?,ack_position=?,ack_outcome=?,settled_sequence=?,bytes=? WHERE channel=? AND original=?",values);
        changed(db);
        if (entry(s,e.canonical_original_id)!=e || scope(s.address.channel)!=s || config()!=expected_global)
            fail(code::corrupt_state,"obligation identity/scope/counter write postimage differs");
    }
    std::vector<entry_t> entries(const scope_t& s,bool active_only,bool check_bodies) const {
        std::vector<entry_t> result; int64_t after=0,total=0;
        for (;;) {
            auto rows=db.query("SELECT "+ints({"audit_id"})+blobs({"original"},l)+
                " FROM main._lattice_obligation_entry WHERE channel=? AND audit_id>? ORDER BY audit_id LIMIT 1",{bytes(s.address.channel),after});
            if (rows.empty()) break;
            if (result.size()>=static_cast<uint64_t>(l.records)) fail(code::corrupt_state,"obligation record count exceeds explicit cap");
            auto e=required(s,string(rows[0],"original"));
            if (e.record.audit_id!=number(rows[0],"audit_id") || e.record.audit_id<=after) fail(code::corrupt_state,"obligation invalid ordered identity");
            after=e.record.audit_id; total=add(total,entry_size(e,l,s.address.channel));
            if (total>l.encoded_bytes) fail(code::corrupt_state,"obligation stored records exceed byte cap");
            if (check_bodies && e.stage!=stage::settled) check_actual(e);
            // Count all rows even if the returned active snapshot omits tombstones.
            result.push_back(std::move(e));
        }
        if (active_only) result.erase(std::remove_if(result.begin(),result.end(),[](const auto& e){return e.stage==stage::settled;}),result.end());
        return result;
    }
    void full_audit() const {
        // Private versioned schema: no adoption of another PK/index layout.
        // SQLite stores these exact CREATE statements with only the schema
        // qualifier removed. Copy at most each compiled definition's size.
        for (const auto& definition:definitions) {
            std::string expected=definition.sql;
            expected.erase(expected.find("main."),5);
            const auto rows=db.query("SELECT CASE WHEN typeof(sql)='text' AND length(CAST(sql AS BLOB))=? THEN CAST(sql AS BLOB) ELSE NULL END AS definition "
                "FROM main.sqlite_schema WHERE name=? LIMIT 2",{static_cast<int64_t>(expected.size()),std::string(definition.name)});
            if (rows.size()!=1 || string(rows[0],"definition")!=expected) fail(code::corrupt_state,"obligation required table/index definition differs");
        }
        // Fixed-shape checks are explicit integrity work, never per-identity
        // settlement scans. WITHOUT ROWID also avoids generic row-hook queues.
        for (const auto& [name,columns]:std::initializer_list<std::pair<const char*,int64_t>>{
            {"_lattice_obligation_store",12},{"_lattice_obligation_scope",21},{"_lattice_obligation_entry",15}}) {
            const auto shape=db.query("SELECT ncol,wr FROM pragma_table_list WHERE schema='main' AND type='table' AND name=? LIMIT 2",{std::string(name)});
            if (shape.size()!=1 || number(shape[0],"wr")!=1 || number(shape[0],"ncol")!=columns)
                fail(code::corrupt_state,"obligation internal table shape differs");
        }
        const auto g=config(); recovery_obligation_usage observed;
        std::string after; bool first=true;
        for (;;) {
            auto rows=db.query("SELECT "+blobs({"channel"},l).substr(1)+" FROM main._lattice_obligation_scope "+
                (first ? "" : "WHERE channel>? ")+"ORDER BY channel LIMIT 1",first ? std::vector<column_value_t>{}:std::vector<column_value_t>{bytes(after)});
            if (rows.empty()) break;
            if (observed.scopes>=l.scopes) fail(code::corrupt_state,"obligation scope count exceeds explicit cap");
            after=string(rows[0],"channel"); first=false;
            auto s=scope(after); if (!s) fail(code::corrupt_state,"obligation scope disappeared");
            current(s->address); ++observed.scopes; observed.encoded_bytes=add(observed.encoded_bytes,scope_size(*s,l));
            for (const auto& e:entries(*s,false,true)) {
                observed.records=add(observed.records,1); observed.encoded_bytes=add(observed.encoded_bytes,entry_size(e,l,s->address.channel));
                if (observed.records>l.records || observed.encoded_bytes>l.encoded_bytes) fail(code::corrupt_state,"obligation total exceeds explicit cap");
            }
        }
        // Orphan/corrupt rows also obey the audit work cap. Refuse a saturated
        // INT64_MAX boundary rather than overflow cap+1 or silently undercount.
        const auto record_limit=l.records==maximum ? maximum:l.records+1;
        const auto scope_limit=l.scopes==maximum ? maximum:l.scopes+1;
        const auto count=db.query("SELECT COUNT(*) AS n FROM (SELECT 1 FROM main._lattice_obligation_entry LIMIT ?)",{record_limit});
        const auto scopes=db.query("SELECT COUNT(*) AS n FROM (SELECT 1 FROM main._lattice_obligation_scope LIMIT ?)",{scope_limit});
        const auto actual_records=number(count.at(0),"n"),actual_scopes=number(scopes.at(0),"n");
        if (actual_records==record_limit || actual_scopes==scope_limit || observed!=g.usage || actual_records!=observed.records || actual_scopes!=observed.scopes)
            fail(code::corrupt_state,"obligation durable counters disagree or audit work cap reached");
        // Overflow has already refused; DISTINCT cannot sort unbounded orphan
        // input even if a future damaged file somehow violates uniqueness.
        const auto distinct_scopes=db.query("SELECT COUNT(DISTINCT incarnation) AS n FROM (SELECT incarnation FROM main._lattice_obligation_scope LIMIT ?)",{scope_limit});
        const auto distinct_entries=db.query("SELECT COUNT(DISTINCT record_sequence) AS n FROM (SELECT record_sequence FROM main._lattice_obligation_entry LIMIT ?)",{record_limit});
        if (number(distinct_scopes.at(0),"n")!=observed.scopes || number(distinct_entries.at(0),"n")!=observed.records)
            fail(code::corrupt_state,"obligation committed identities were reused");
    }
    recovery_obligation_receipt_claim receipt(const scope_t& s,const recovery_obligation_receipt_claim& r) const {
        field(r.original_id,l); field(r.receipt_namespace,l);
        if (r.receipt_namespace!=s.profile.receipt_namespace || r.position<0 || r.outcome<recovery_obligation_outcome::applied || r.outcome>recovery_obligation_outcome::no_op)
            fail(code::invalid_argument,"obligation receipt namespace/position/outcome differs");
        auto result=r; result.original_id=uuid(r.original_id); return result;
    }
};
void installed(receive_install_store& installs,const scope_t& s,const receive_install_identity& i) {
    if (i.sequence<=0 || i.expected_revision<0 || i.expected_revision==maximum || i.head<0)
        fail(code::invalid_argument,"obligation invalid installation identity integers");
    const auto actual=installs.read(s.address.channel);
    if (!actual || actual->binding!=s.profile.binding || actual->active || actual->last_installed!=i ||
        actual->revision!=i.expected_revision+1 || actual->frontier!=receive_install_frontier{receive_frontier_kind::position,i.head})
        fail(code::stale,"obligation settlement requires exact actual completed receiver installation");
}
}

recovery_obligation_store::recovery_obligation_store(std::shared_ptr<lattice_db> owner,recovery_obligation_limits limits,receive_install_limits installs)
    :owner_(std::move(owner)),limits_(limits),install_limits_(installs) {
    if (!owner_ || limits.scopes<0 || limits.records<0 || limits.field_bytes<=0 || limits.field_bytes>std::numeric_limits<int>::max() || limits.encoded_bytes<0)
        fail(code::invalid_argument,"obligation requires retained owner and finite representable limits");
}
database& recovery_obligation_store::writer() const {
    auto* db=recovery_writer_access::active_writer(*owner_);
    if (!db) fail(code::transaction_required,"obligation requires this thread's actual owned main WRITE transaction");
    return *db;
}
void recovery_obligation_store::initialize() {
    auto& db=writer(); backend b{db,limits_};
    atomic(db,[&] {
        auto existing=db.query("SELECT 1 AS present FROM main.sqlite_schema WHERE name='_lattice_obligation_store' LIMIT 1");
        if (existing.empty()) {
            // Partial preexisting tables refuse through CREATE, never IF NOT EXISTS adoption.
            for (const auto& definition:definitions) db.execute(definition.sql);
            db.execute("INSERT INTO main._lattice_obligation_store VALUES(1,1,?,?,?,?,0,0,0,0,0,0)",{limits_.scopes,limits_.records,limits_.field_bytes,limits_.encoded_bytes}); changed(db);
        }
        b.full_audit(); return 0;
    });
}
void recovery_obligation_store::audit() const { backend{writer(),limits_}.full_audit(); }
recovery_obligation_usage recovery_obligation_store::usage() const { return backend{writer(),limits_}.config().usage; }
std::optional<scope_t> recovery_obligation_store::read(const std::string& channel) const {
    backend b{writer(),limits_}; b.config(); auto s=b.scope(channel); if (s) b.current(s->address); return s;
}
scope_t recovery_obligation_store::bind(const recovery_obligation_profile& profile) {
    backend b{writer(),limits_}; scope_t s; s.profile=profile; s.address.channel=profile.binding.channel;
    scope_size(s,limits_);
    return atomic(b.db,[&] {
        auto g=b.config(); auto old=b.scope(s.address.channel);
        if (old) { b.current(old->address); if (old->profile!=profile) fail(code::binding_mismatch,"obligation binding differs"); return *old; }
        if (!b.db.query("SELECT 1 AS present FROM main._lattice_obligation_scope WHERE authority=? AND scope=? LIMIT 1",{bytes(profile.binding.authority),bytes(profile.binding.scope)}).empty())
            fail(code::alias,"obligation authority/scope already belongs to another channel");
        receive_install_store installs(owner_,install_limits_);
        const auto baseline=installs.read(profile.binding.channel);
        if (!baseline || baseline->binding!=profile.binding) fail(code::binding_mismatch,"obligation requires an existing exact receiver binding");
        if (baseline->active) fail(code::wrong_mode,"obligation cannot adopt a baseline with active installation");
        if (baseline->last_sequence==maximum || baseline->revision==maximum) fail(code::exhausted,"obligation receiver baseline cannot admit a fresh installation");
        s.last_attempt=baseline->last_sequence;
        if (baseline->last_installed) {
            s.installed_sequence=baseline->last_installed->sequence; s.installed_revision=baseline->revision;
            s.installed_head=baseline->last_installed->head; s.installed_manifest=baseline->last_installed->manifest_digest;
        }
        const auto charge=scope_size(s,limits_);
        auto updated=g; updated.incarnation=next(g.incarnation); s.address.incarnation=updated.incarnation; s.address.generation=1; s.revision=1;
        updated.usage.scopes=add(g.usage.scopes,1); updated.usage.encoded_bytes=add(g.usage.encoded_bytes,charge);
        b.put_global(g,updated); b.put_scope({},s); return s;
    });
}
entry_t recovery_obligation_store::record(const recovery_obligation_address& a,const recovery_obligation_record& input) {
    backend b{writer(),limits_};
    return atomic(b.db,[&] {
        auto s=b.current(a); auto actual=b.actual(input); const auto key=uuid(actual.original_id);
        if (auto old=b.entry(s,key)) {
            if (old->record!=actual) fail(code::conflict,"obligation original already has a different claim/target");
            return b.required(s,key);
        }
        if (s.mode!=mode::recording && actual.origin!=origin::local_candidate) fail(code::wrong_mode,"obligation frozen scope refuses imported/legacy intake");
        auto g=b.config(),updated=g; entry_t e; e.record=std::move(actual); e.canonical_original_id=key; e.canonical_target_id=uuid(e.record.target_id);
        updated.record=next(g.record); e.sequence=updated.record; updated.usage.records=add(g.usage.records,1);
        updated.usage.encoded_bytes=add(g.usage.encoded_bytes,entry_size(e,limits_,s.address.channel));
        auto prior=s; s.revision=next(s.revision); b.put_global(g,updated); b.put_scope(prior,s); b.put_entry(s,e,true); return e;
    });
}
std::optional<entry_t> recovery_obligation_store::find(const recovery_obligation_address& a,const std::string& id) const {
    backend b{writer(),limits_}; auto s=b.current(a); auto result=b.entry(s,uuid(id));
    if (result) b.required(s,id); return result;
}
recovery_obligation_export_ticket recovery_obligation_store::claim_export(const recovery_obligation_address& a,const std::vector<std::string>& ids) {
    backend b{writer(),limits_};
    if (ids.empty() || ids.size()>static_cast<uint64_t>(limits_.records)) fail(code::invalid_argument,"obligation export requires a bounded nonempty identity list");
    return atomic(b.db,[&] {
        auto s=b.current(a); if (s.mode!=mode::recording) fail(code::wrong_mode,"obligation frozen/installed scope cannot claim export");
        std::set<std::string> unique; std::vector<entry_t> entries; int64_t total=0;
        for (const auto& id:ids) {
            field(id,limits_); auto e=b.required(s,id);
            if (!unique.insert(e.canonical_original_id).second) fail(code::invalid_argument,"obligation duplicate export identity");
            if (e.stage!=stage::open) fail(code::wrong_mode,"obligation ACKed/settled original cannot be newly exported");
            b.check_actual(e); total=add(total,entry_size(e,limits_,s.address.channel));
            if (total>limits_.encoded_bytes) fail(code::corrupt_state,"obligation export entries exceed stored byte cap");
            entries.push_back(std::move(e));
        }
        auto g=b.config(),updated=g; updated.export_claim=next(g.export_claim);
        auto prior=s; s.revision=next(s.revision); b.put_global(g,updated); b.put_scope(prior,s);
        recovery_obligation_export_ticket ticket{a,updated.export_claim,s.revision,{}};
        for (auto& e:entries) { if (!e.first_export_claim) e.first_export_claim=ticket.sequence; b.put_entry(s,e,false); ticket.canonical_original_ids.push_back(e.canonical_original_id); }
        for (const auto& e:entries) if (b.entry(s,e.canonical_original_id)!=e) fail(code::corrupt_state,"obligation export final identity postimage differs");
        return ticket;
    });
}
scope_t recovery_obligation_store::freeze(const recovery_obligation_address& a,int64_t attempt) {
    backend b{writer(),limits_};
    return atomic(b.db,[&] {
        auto s=b.current(a); if (attempt<=s.last_attempt || attempt<=0) fail(code::stale,"obligation recovery attempt must advance committed high water");
        const auto g=b.config(); auto prior=s; s.address.generation=next(s.address.generation); s.revision=next(s.revision);
        s.last_attempt=attempt; s.freeze_revision=s.revision; s.freeze_record_high_water=g.record; s.freeze_export_high_water=g.export_claim; s.mode=mode::frozen;
        b.put_scope(prior,s); return s;
    });
}
recovery_obligation_snapshot recovery_obligation_store::snapshot_for_install(const recovery_obligation_address& a,int64_t attempt) const {
    backend b{writer(),limits_}; auto s=b.current(a);
    if (s.mode!=mode::frozen || s.last_attempt!=attempt) fail(code::stale,"obligation final snapshot requires current frozen attempt");
    b.full_audit(); return {s,b.entries(s,true,true)};
}
scope_t recovery_obligation_store::acknowledge(const recovery_obligation_address& a,const recovery_obligation_receipt_claim& input) {
    backend b{writer(),limits_};
    return atomic(b.db,[&] {
        auto s=b.current(a); if (s.mode!=mode::recording) fail(code::wrong_mode,"obligation ACK callback belongs to a fenced dispatch generation");
        const auto receipt=b.receipt(s,input); auto e=b.required(s,receipt.original_id); b.check_actual(e);
        if (e.acknowledged) { if (e.acknowledged!=receipt) fail(code::conflict,"obligation retained first ACK differs"); return s; }
        e.acknowledged=receipt; e.stage=stage::acknowledged_awaiting_install;
        auto prior=s; s.revision=next(s.revision); b.put_scope(prior,s); b.put_entry(s,e,false); return s;
    });
}
scope_t recovery_obligation_store::settle_install(const recovery_obligation_address& a,int64_t revision,const receive_install_identity& i,
    const std::vector<recovery_obligation_receipt_claim>& positives) {
    backend b{writer(),limits_};
    if (positives.size()>static_cast<uint64_t>(limits_.records)) fail(code::capacity,"obligation settlement list exceeds explicit cap");
    return atomic(b.db,[&] {
        auto s=b.current(a);
        if (s.mode!=mode::frozen || s.last_attempt!=i.sequence || s.revision!=revision) fail(code::stale,"obligation settlement journal postimage changed");
        receive_install_store installs(owner_,install_limits_); installed(installs,s,i);
        std::set<std::string> unique; std::vector<entry_t> entries; int64_t total=0;
        for (const auto& input:positives) {
            auto receipt=b.receipt(s,input); if (!unique.insert(receipt.original_id).second) fail(code::invalid_argument,"obligation duplicate settlement identity");
            auto e=b.required(s,receipt.original_id); b.check_actual(e);
            if (e.stage==stage::settled || receipt.position>i.head || (e.acknowledged && e.acknowledged!=receipt))
                fail(code::conflict,"obligation positive receipt is inconsistent with covered head/retained ACK");
            total=add(total,entry_size(e,limits_,s.address.channel));
            if (total>limits_.encoded_bytes) fail(code::corrupt_state,"obligation settlement entries exceed stored byte cap");
            e.acknowledged=receipt; e.stage=stage::settled; e.settled_install_sequence=i.sequence; entries.push_back(std::move(e));
        }
        auto prior=s; s.mode=mode::installed; s.revision=next(s.revision); s.installed_sequence=i.sequence;
        s.installed_revision=i.expected_revision+1; s.installed_head=i.head; s.installed_manifest=i.manifest_digest;
        auto g=b.config(),updated=g;
        const auto old_charge=scope_size(prior,limits_),new_charge=scope_size(s,limits_);
        if (g.usage.encoded_bytes<old_charge) fail(code::corrupt_state,"obligation byte counter underflow");
        updated.usage.encoded_bytes=add(g.usage.encoded_bytes-old_charge,new_charge);
        b.put_global(g,updated); b.put_scope(prior,s);
        for (const auto& e:entries) b.put_entry(s,e,false);
        for (const auto& e:entries) if (b.entry(s,e.canonical_original_id)!=e) fail(code::corrupt_state,"obligation settlement final identity postimage differs");
        installed(installs,s,i); return s;
    });
}
scope_t recovery_obligation_store::resume(const recovery_obligation_address& a,const receive_install_identity& i) {
    backend b{writer(),limits_};
    return atomic(b.db,[&] {
        auto s=b.current(a);
        if (i.expected_revision<0 || i.expected_revision==maximum) fail(code::invalid_argument,"obligation invalid resume revision");
        if (s.mode!=mode::installed || s.installed_sequence!=i.sequence || s.installed_revision!=i.expected_revision+1 ||
            s.installed_head!=i.head || s.installed_manifest!=i.manifest_digest) fail(code::stale,"obligation resume requires this exact completed installation");
        receive_install_store installs(owner_,install_limits_); installed(installs,s,i); b.full_audit();
        auto prior=s; s.mode=mode::recording; s.address.generation=next(s.address.generation); s.revision=next(s.revision); b.put_scope(prior,s); return s;
    });
}
void recovery_obligation_store::retire(const recovery_obligation_address& a) {
    backend b{writer(),limits_};
    atomic(b.db,[&] {
        const auto s=b.current(a); if (s.mode==mode::frozen) fail(code::wrong_mode,"obligation frozen recovery cannot retire");
        const auto entries=b.entries(s,false,false); int64_t charge=scope_size(s,limits_);
        for (const auto& e:entries) { if (e.stage!=stage::settled) fail(code::wrong_mode,"obligation unresolved original still pins recovery evidence"); charge=add(charge,entry_size(e,limits_,s.address.channel)); }
        auto g=b.config(),updated=g;
        if (g.usage.records<static_cast<int64_t>(entries.size()) || g.usage.scopes<1 || g.usage.encoded_bytes<charge) fail(code::corrupt_state,"obligation retirement counter underflow");
        updated.usage.records-=static_cast<int64_t>(entries.size()); --updated.usage.scopes; updated.usage.encoded_bytes-=charge;
        b.db.execute("DELETE FROM main._lattice_obligation_entry WHERE channel=?",{bytes(a.channel)});
        if (b.db.changes()!=static_cast<int64_t>(entries.size())) throw db_error("obligation retirement did not delete expected identity count");
        b.db.execute("DELETE FROM main._lattice_obligation_scope WHERE channel=? AND incarnation=? AND generation=? AND revision=?",{bytes(a.channel),a.incarnation,a.generation,s.revision}); changed(b.db);
        if (b.scope(a.channel) || !b.db.query("SELECT 1 AS present FROM main._lattice_obligation_entry WHERE channel=? LIMIT 1",{bytes(a.channel)}).empty()) fail(code::corrupt_state,"obligation retirement postimage differs");
        b.put_global(g,updated); return 0;
    });
}
bool recovery_obligation_store::pins_audit(int64_t audit_id,const std::string& original_id) const {
    backend b{writer(),limits_}; b.config(); const auto key=uuid(original_id);
    if (audit_id<=0) fail(code::invalid_argument,"obligation pin requires positive numeric audit identity");
    // Both branches are indexed. No absence/error is translated into permission
    // to prune a different numeric/UUID identity; settled rows remain readable
    // even after a future retention integration legitimately removes the body.
    if (!b.db.query("SELECT 1 AS present FROM main._lattice_obligation_entry WHERE original=? AND audit_id!=? LIMIT 1",{bytes(key),audit_id}).empty())
        fail(code::audit_mismatch,"obligation retention original has a different numeric identity");
    bool pinned=false,first=true; std::string after; int64_t count=0,total=0;
    for (;;) {
        auto rows=b.db.query("SELECT "+blobs({"channel","original"},limits_).substr(1)+
            " FROM main._lattice_obligation_entry WHERE audit_id=? "+(first ? "" : "AND channel>? ")+"ORDER BY channel LIMIT 1",
            first ? std::vector<column_value_t>{audit_id}:std::vector<column_value_t>{audit_id,bytes(after)});
        if (rows.empty()) break;
        if (count>=limits_.records) fail(code::corrupt_state,"obligation pin count exceeds durable cap");
        ++count; after=string(rows[0],"channel"); first=false;
        if (string(rows[0],"original")!=key) fail(code::audit_mismatch,"obligation retention numeric identity has a different original");
        auto s=b.scope(after); if (!s) fail(code::corrupt_state,"obligation retention record has no scope");
        b.current(s->address); const auto e=b.required(*s,key);
        total=add(total,entry_size(e,limits_,s->address.channel)); if (total>limits_.encoded_bytes) fail(code::corrupt_state,"obligation pin bytes exceed durable cap");
        pinned|=e.stage!=stage::settled;
    }
    return pinned;
}
} // namespace lattice::detail
