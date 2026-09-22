#include "recovery_obligation_store.hpp"
#include "recovery_obligation_producer.hpp"
#include "recovery_writer_access.hpp"
#include "canonical_writer_adapter.hpp"
#include <algorithm>
#include <limits>
#include <set>
#include <utility>

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
// Exact cancellation preservation, not a completeness/dispatch capability.
// All retained entries include settled tombstones; aggregate budgets are the
// same limits enforced by the full audit that constructs each snapshot.
struct cancellation_journal_state {
    global store;
    std::vector<std::pair<scope_t,std::vector<entry_t>>> scopes;
    bool operator==(const cancellation_journal_state&) const=default;
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
    void full_audit(cancellation_journal_state* preserved=nullptr) const {
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
        if(preserved){preserved->store=g;preserved->scopes.clear();}
        std::string after; bool first=true;
        for (;;) {
            auto rows=db.query("SELECT "+blobs({"channel"},l).substr(1)+" FROM main._lattice_obligation_scope "+
                (first ? "" : "WHERE channel>? ")+"ORDER BY channel LIMIT 1",first ? std::vector<column_value_t>{}:std::vector<column_value_t>{bytes(after)});
            if (rows.empty()) break;
            if (observed.scopes>=l.scopes) fail(code::corrupt_state,"obligation scope count exceeds explicit cap");
            after=string(rows[0],"channel"); first=false;
            auto s=scope(after); if (!s) fail(code::corrupt_state,"obligation scope disappeared");
            current(s->address); ++observed.scopes; observed.encoded_bytes=add(observed.encoded_bytes,scope_size(*s,l));
            if(observed.encoded_bytes>l.encoded_bytes)fail(code::corrupt_state,"obligation total exceeds explicit cap");
            auto retained=entries(*s,false,true);
            for (const auto& e:retained) {
                observed.records=add(observed.records,1); observed.encoded_bytes=add(observed.encoded_bytes,entry_size(e,l,s->address.channel));
                if (observed.records>l.records || observed.encoded_bytes>l.encoded_bytes) fail(code::corrupt_state,"obligation total exceeds explicit cap");
            }
            if(preserved)preserved->scopes.emplace_back(*s,std::move(retained));
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
void refuse_enrolled_producer(database&,const std::string&);
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
scope_t recovery_obligation_store::cancel_frozen_for_retry(const recovery_obligation_address& a,
    int64_t attempt,int64_t revision) {
    backend b{writer(),limits_};
    if (attempt<=0 || revision<=0) fail(code::invalid_argument,"obligation cancellation requires positive attempt and revision");
    return atomic(b.db,[&] {
        auto s=b.current(a);
        if (s.mode!=mode::frozen || s.last_attempt!=attempt || s.revision!=revision)
            fail(code::stale,"obligation cancellation requires the exact frozen journal revision");
        receive_install_store installs(owner_,install_limits_);
        const auto receiver=installs.read(s.address.channel);
        if (!receiver || receiver->binding!=s.profile.binding || receiver->active ||
            receiver->last_sequence>s.last_attempt || receiver->revision!=s.installed_revision)
            fail(code::stale,"obligation cancellation receiver is active, changed or unavailable");
        if (s.installed_sequence==0) {
            if (receiver->last_installed || receiver->frontier!=receive_install_frontier{})
                fail(code::stale,"obligation cancellation receiver no longer has its initial baseline");
        } else if (!receiver->last_installed ||
                   receiver->last_installed->sequence!=s.installed_sequence ||
                   receiver->last_installed->head!=s.installed_head ||
                   receiver->last_installed->manifest_digest!=s.installed_manifest ||
                   receiver->frontier!=receive_install_frontier{receive_frontier_kind::position,s.installed_head}) {
            fail(code::stale,"obligation cancellation receiver differs from the retained installed baseline");
        }
        // Capture before EITHER write: receiver retirement can invoke a
        // metadata trigger before put_scope samples its own expected_global.
        cancellation_journal_state expected_journal;
        b.full_audit(&expected_journal);
        auto expected_receivers=installs.snapshot_for_journal();
        // Freeze can precede receipt of a manifest, so begin() may never have
        // consumed this sequence. Retire its number without inventing an I;
        // otherwise next-attempt monotonicity and receiver next-sequence
        // admission would disagree forever after a pre-manifest timeout.
        const auto retired=installs.retire_unstarted_for_journal(*receiver,attempt);
        auto prior=s;
        s.mode=mode::recording;
        s.address.generation=next(s.address.generation);
        s.revision=next(s.revision);
        b.put_scope(prior,s);
        // A metadata trigger/error cannot turn cancellation into adoption of a
        // changed receiver. Any failure rolls this savepoint back, retaining Q.
        if (installs.read(s.address.channel)!=std::optional<receive_install_snapshot>{retired})
            fail(code::stale,"obligation cancellation receiver changed during settlement");
        bool journal_found=false,receiver_found=false;
        for(auto& retained:expected_journal.scopes)if(retained.first.address.channel==prior.address.channel){
            if(retained.first!=prior)fail(code::stale,"obligation cancellation initial scope changed");
            retained.first=s;journal_found=true;
        }
        for(auto& channel:expected_receivers.channels)if(channel.binding.channel==receiver->binding.channel){
            if(channel!=*receiver)fail(code::stale,"obligation cancellation initial receiver changed");
            channel=retired;receiver_found=true;
        }
        if(!journal_found||!receiver_found)fail(code::corrupt_state,"obligation cancellation missing preservation baseline");
        cancellation_journal_state actual_journal;
        b.full_audit(&actual_journal);
        if(actual_journal!=expected_journal)
            fail(code::corrupt_state,"obligation cancellation changed retained journal evidence");
        if(installs.snapshot_for_journal()!=expected_receivers)
            fail(code::stale,"obligation cancellation changed retained receiver evidence");
        return s;
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
        const auto s=b.current(a); refuse_enrolled_producer(b.db,a.channel); if (s.mode==mode::frozen) fail(code::wrong_mode,"obligation frozen recovery cannot retire");
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

namespace lattice::detail {
namespace {
using producer_limits=recovery_obligation_producer_limits;
using producer_profile=recovery_obligation_producer_profile;
constexpr schema_definition producer_definitions[]={
 {"_lattice_obligation_producer_store","CREATE TABLE main._lattice_obligation_producer_store(id INTEGER PRIMARY KEY,version INTEGER NOT NULL,max_profiles INTEGER NOT NULL,max_stamps INTEGER NOT NULL,max_field INTEGER NOT NULL,max_manifest INTEGER NOT NULL,max_bytes INTEGER NOT NULL,profiles INTEGER NOT NULL,stamps INTEGER NOT NULL,bytes INTEGER NOT NULL) WITHOUT ROWID"},
 {"_lattice_obligation_producer_profile","CREATE TABLE main._lattice_obligation_producer_profile(channel BLOB PRIMARY KEY,incarnation INTEGER NOT NULL UNIQUE,program_revision INTEGER NOT NULL,program_digest BLOB NOT NULL,manifest BLOB NOT NULL,bytes INTEGER NOT NULL) WITHOUT ROWID"},
 {"_lattice_obligation_producer_stamp","CREATE TABLE main._lattice_obligation_producer_stamp(channel BLOB NOT NULL,original BLOB NOT NULL,incarnation INTEGER NOT NULL,program_revision INTEGER NOT NULL,audit_id INTEGER NOT NULL,record_sequence INTEGER NOT NULL UNIQUE,generation INTEGER NOT NULL,scope_revision INTEGER NOT NULL,base_scopes INTEGER NOT NULL,base_records INTEGER NOT NULL,base_bytes INTEGER NOT NULL,base_incarnation INTEGER NOT NULL,base_export INTEGER NOT NULL,producer_profiles INTEGER NOT NULL,producer_stamps INTEGER NOT NULL,producer_bytes INTEGER NOT NULL,bytes INTEGER NOT NULL,PRIMARY KEY(channel,original),UNIQUE(channel,audit_id)) WITHOUT ROWID"}
};
constexpr schema_definition install_definitions[]={
 {"_lattice_install_store","CREATE TABLE main._lattice_install_store(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,max_channels INTEGER NOT NULL,max_field_bytes INTEGER NOT NULL,max_bytes INTEGER NOT NULL,channels INTEGER NOT NULL,bytes INTEGER NOT NULL) WITHOUT ROWID"},
 {"_lattice_install_channel","CREATE TABLE main._lattice_install_channel(channel BLOB PRIMARY KEY NOT NULL,authority BLOB NOT NULL,source BLOB NOT NULL,epoch BLOB NOT NULL,scope BLOB NOT NULL,schema_digest BLOB NOT NULL,frontier_kind INTEGER NOT NULL,frontier INTEGER,revision INTEGER NOT NULL,last_sequence INTEGER NOT NULL,active BLOB,last_install BLOB,bytes INTEGER NOT NULL,UNIQUE(authority,scope)) WITHOUT ROWID"}
};
void exact_definition(database& db,const schema_definition& d) {
    std::string expected=d.sql; expected.erase(expected.find("main."),5);
    auto rows=db.query("SELECT CASE WHEN typeof(sql)='text' AND length(CAST(sql AS BLOB))=? THEN CAST(sql AS BLOB) END AS definition FROM main.sqlite_schema WHERE name=? LIMIT 2",
        {static_cast<int64_t>(expected.size()),std::string(d.name)});
    if (rows.size()!=1 || string(rows[0],"definition")!=expected) fail(code::corrupt_state,"producer required schema/index differs");
}
void producer_policy(const producer_limits& l) {
    if(l.profiles<0 || l.stamps<0 || l.field_bytes<=0 || l.field_bytes>std::numeric_limits<int>::max() ||
       l.manifest_bytes<0 || l.manifest_bytes>std::numeric_limits<int>::max() || l.encoded_bytes<0)
        fail(code::invalid_argument,"producer requires finite representable explicit budgets");
}
void obligation_policy(const recovery_obligation_limits& l) {
    if(l.scopes<0 || l.records<0 || l.field_bytes<=0 || l.field_bytes>std::numeric_limits<int>::max() || l.encoded_bytes<0)
        fail(code::invalid_argument,"producer obligation budgets are invalid");
}
void producer_field(const std::string& s,const producer_limits& l,code c=code::invalid_argument) {
    if(s.empty() || s.size()>static_cast<uint64_t>(l.field_bytes)) fail(c,"producer field exceeds explicit budget");
}
int64_t profile_charge(const producer_profile& p,const recovery_obligation_limits& ol,const producer_limits& l,code c=code::invalid_argument) {
    scope_t s;s.profile=p.contribution;scope_size(s,ol,c);
    producer_field(p.contribution.binding.channel,l,c);producer_field(p.program_digest,l,c);
    if(p.contribution_incarnation<=0 || p.program_revision<=0 || p.grant_manifest.empty() || p.grant_manifest.size()>static_cast<uint64_t>(l.manifest_bytes))
        fail(c,"producer invalid incarnation/program/manifest");
    return add(24,add(static_cast<int64_t>(p.contribution.binding.channel.size()),
        add(static_cast<int64_t>(p.program_digest.size()),static_cast<int64_t>(p.grant_manifest.size()),c),c),c);
}
int64_t stamp_charge(const std::string& channel) { return add(15*8,add(static_cast<int64_t>(channel.size()),36)); }
struct producer_usage { int64_t profiles=0,stamps=0,bytes=0;bool operator==(const producer_usage&)const=default; };
struct producer_backend {
    backend base;
    const producer_limits& l;
    producer_usage config()const {
        auto rows=base.db.query("SELECT "+ints({"id","version","max_profiles","max_stamps","max_field","max_manifest","max_bytes","profiles","stamps","bytes"})+" FROM main._lattice_obligation_producer_store LIMIT 2");
        if(rows.size()!=1 || number(rows[0],"id")!=1 || number(rows[0],"version")!=1)fail(code::corrupt_state,"producer missing/unsupported fixed metadata");
        const auto& r=rows[0];
        if(producer_limits{number(r,"max_profiles"),number(r,"max_stamps"),number(r,"max_field"),number(r,"max_manifest"),number(r,"max_bytes")}!=l)
            fail(code::limits_mismatch,"producer explicit budgets differ from durable configuration");
        producer_usage u{number(r,"profiles"),number(r,"stamps"),number(r,"bytes")};
        if(!fits(u.profiles,0,l.profiles)||!fits(u.stamps,0,l.stamps)||!fits(u.bytes,0,l.encoded_bytes))fail(code::corrupt_state,"producer invalid counters");return u;
    }
    void put(const producer_usage& old,const producer_usage& u) {
        if(!fits(u.profiles,0,l.profiles)||!fits(u.stamps,0,l.stamps)||!fits(u.bytes,0,l.encoded_bytes))fail(code::capacity,"producer retained provenance capacity exhausted");
        base.db.execute("UPDATE main._lattice_obligation_producer_store SET profiles=?,stamps=?,bytes=? WHERE id=1 AND profiles=? AND stamps=? AND bytes=?",
            {u.profiles,u.stamps,u.bytes,old.profiles,old.stamps,old.bytes});changed(base.db);
        if(config()!=u)fail(code::corrupt_state,"producer counter postimage differs");
    }
    std::optional<producer_profile> profile(const std::string& channel)const {
        field(channel,base.l);producer_field(channel,l);
        const auto rows=base.db.query("SELECT "+ints({"incarnation","program_revision","bytes"})+
            ",CASE WHEN typeof(program_digest)='blob' AND length(program_digest) BETWEEN 1 AND ? AND length(program_digest)+length(manifest)+length(channel)<=? THEN program_digest END AS program_digest,"
            "CASE WHEN typeof(manifest)='blob' AND length(manifest) BETWEEN 1 AND ? AND length(program_digest)+length(manifest)+length(channel)<=? THEN manifest END AS manifest "
            "FROM main._lattice_obligation_producer_profile WHERE channel=? LIMIT 2",
            {l.field_bytes,l.encoded_bytes,l.manifest_bytes,l.encoded_bytes,bytes(channel)});
        if(rows.empty())return {};if(rows.size()!=1)fail(code::corrupt_state,"producer duplicate profile");
        auto s=base.scope(channel);if(!s)fail(code::corrupt_state,"producer profile has no contribution");base.current(s->address);
        producer_profile p{s->profile,number(rows[0],"incarnation"),number(rows[0],"program_revision"),string(rows[0],"program_digest"),bytes(string(rows[0],"manifest"))};
        const auto charge=profile_charge(p,base.l,l,code::corrupt_state);const auto u=config();
        if(p.contribution_incarnation!=s->address.incarnation || charge!=number(rows[0],"bytes") || u.profiles<1 || u.bytes<charge)
            fail(code::corrupt_state,"producer profile identity/charge differs");return p;
    }
    std::optional<recovery_obligation_producer_stamp> stamp(const scope_t& s,const std::string& key)const {
        const auto rows=base.db.query("SELECT "+ints({"incarnation","program_revision","audit_id","record_sequence","generation","scope_revision","base_scopes","base_records","base_bytes","base_incarnation","base_export","producer_profiles","producer_stamps","producer_bytes","bytes"})+
            " FROM main._lattice_obligation_producer_stamp WHERE channel=? AND original=? LIMIT 2",{bytes(s.address.channel),bytes(key)});
        if(rows.empty())return {};if(rows.size()!=1)fail(code::corrupt_state,"producer duplicate original stamp");
        const auto p=profile(s.address.channel);if(!p)fail(code::corrupt_state,"producer stamp has no profile");
        const auto e=base.required(s,key);const auto& r=rows[0];
        recovery_obligation_producer_stamp result{number(r,"incarnation"),number(r,"program_revision"),number(r,"audit_id"),number(r,"record_sequence"),p->program_digest};
        const auto g=base.config();const auto u=config();
        if(result.contribution_incarnation!=s.address.incarnation || result.program_revision!=p->program_revision || result.audit_id!=e.record.audit_id || result.record_sequence!=e.sequence || e.record.origin!=origin::local_candidate ||
            number(r,"generation")<=0 || number(r,"generation")>s.address.generation || number(r,"scope_revision")<=0 || number(r,"scope_revision")>s.revision ||
            number(r,"base_scopes")<=0 || number(r,"base_scopes")>base.l.scopes || number(r,"base_records")<=0 || number(r,"base_records")>base.l.records ||
            number(r,"base_bytes")<entry_size(e,base.l,s.address.channel) || number(r,"base_bytes")>base.l.encoded_bytes || number(r,"base_incarnation")<s.address.incarnation || number(r,"base_incarnation")>g.incarnation ||
            number(r,"base_export")<0 || number(r,"base_export")>g.export_claim || number(r,"producer_profiles")<=0 || number(r,"producer_profiles")>l.profiles ||
            number(r,"producer_stamps")<=0 || number(r,"producer_stamps")>l.stamps || number(r,"producer_bytes")<stamp_charge(s.address.channel) || number(r,"producer_bytes")>l.encoded_bytes ||
            number(r,"bytes")!=stamp_charge(s.address.channel) || u.stamps<1 || u.bytes<stamp_charge(s.address.channel))fail(code::corrupt_state,"producer stamp contradicts addressed obligation");
        if(e.stage!=stage::settled)base.check_actual(e);return result;
    }
    std::vector<producer_profile> audit()const {
        for(const auto& d:producer_definitions)exact_definition(base.db,d);
        const auto expected=config();producer_usage observed;std::vector<producer_profile> result;
        bool first=true;std::string after;
        for(;;) {
            auto rows=base.db.query("SELECT CASE WHEN typeof(channel)='blob' AND length(channel) BETWEEN 1 AND ? THEN channel END AS channel FROM main._lattice_obligation_producer_profile "+
                std::string(first?"":"WHERE channel>? ")+"ORDER BY channel LIMIT 1",first?std::vector<column_value_t>{l.field_bytes}:std::vector<column_value_t>{l.field_bytes,bytes(after)});
            if(rows.empty())break;if(observed.profiles>=l.profiles)fail(code::corrupt_state,"producer profile audit exceeds count cap");
            after=string(rows[0],"channel");first=false;auto p=profile(after);if(!p)fail(code::corrupt_state,"producer profile disappeared");
            ++observed.profiles;observed.bytes=add(observed.bytes,profile_charge(*p,base.l,l));
            if(observed.bytes>l.encoded_bytes)fail(code::corrupt_state,"producer profile audit exceeds byte cap");result.push_back(std::move(*p));
        }
        first=true;std::string after_channel,after_original;
        for(;;) {
            auto rows=base.db.query("SELECT CASE WHEN typeof(channel)='blob' AND length(channel) BETWEEN 1 AND ? THEN channel END AS channel,"
                "CASE WHEN typeof(original)='blob' AND length(original)=36 THEN original END AS original FROM main._lattice_obligation_producer_stamp "+
                std::string(first?"":"WHERE (channel,original)>(?,?) ")+"ORDER BY channel,original LIMIT 1",first?std::vector<column_value_t>{l.field_bytes}:std::vector<column_value_t>{l.field_bytes,bytes(after_channel),bytes(after_original)});
            if(rows.empty())break;if(observed.stamps>=l.stamps)fail(code::corrupt_state,"producer stamp audit exceeds count cap");
            after_channel=string(rows[0],"channel");after_original=string(rows[0],"original");first=false;
            if(uuid(after_original,code::corrupt_state)!=after_original)fail(code::corrupt_state,"producer stamp key is not canonical UUID");
            auto s=base.scope(after_channel);if(!s)fail(code::corrupt_state,"producer orphan stamp");base.current(s->address);
            if(!stamp(*s,after_original))fail(code::corrupt_state,"producer stamp disappeared");
            ++observed.stamps;observed.bytes=add(observed.bytes,stamp_charge(after_channel));
            if(observed.bytes>l.encoded_bytes)fail(code::corrupt_state,"producer stamp audit exceeds byte cap");
        }
        if(observed!=expected)fail(code::corrupt_state,"producer retained usage differs from bounded audit");return result;
    }
};
void refuse_enrolled_producer(database& db,const std::string& channel) {
    const auto family=db.query("SELECT name FROM main.sqlite_schema WHERE name IN ('_lattice_obligation_producer_store','_lattice_obligation_producer_profile','_lattice_obligation_producer_stamp') LIMIT 4");
    if(family.empty())return;if(family.size()!=3)fail(code::corrupt_state,"producer partial family blocks contribution retirement");
    for(const auto& d:producer_definitions)exact_definition(db,d);
    if(!db.query("SELECT 1 AS present FROM main._lattice_obligation_producer_profile WHERE channel=? LIMIT 1",{bytes(channel)}).empty() ||
       !db.query("SELECT 1 AS present FROM main._lattice_obligation_producer_stamp WHERE channel=? LIMIT 1",{bytes(channel)}).empty())
        fail(code::wrong_mode,"producer contribution requires atomic adapter retirement");
}
database& producer_writer(const std::shared_ptr<lattice_db>& owner) {
    auto* db=owner?recovery_writer_access::active_writer(*owner):nullptr;
    if(!db)fail(code::transaction_required,"producer metadata requires actual owned main WRITE transaction");return *db;
}
std::string hex_blob(const std::string& value) {
    static constexpr char digits[]="0123456789abcdef";std::string result="X'";
    for(unsigned char c:value){result+=digits[c>>4];result+=digits[c&15];}return result+"'";
}
std::string demand(const std::string& condition) {return "SELECT CASE WHEN ("+condition+") THEN 1 ELSE RAISE(ABORT,'lattice recovery producer refused') END;";}
class bounded_producer_sql {
    std::string value_;
    size_t limit_;
public:
    explicit bounded_producer_sql(int64_t limit):limit_(static_cast<size_t>(limit)){}
    void operator+=(const std::string& part){
        if(part.size()>limit_-value_.size())fail(code::capacity,"producer generated SQL exceeds explicit program cap");
        value_+=part;
    }
    std::string finish(){return std::move(value_);}
};
}

recovery_obligation_producer_program::recovery_obligation_producer_program(producer_profile p,recovery_obligation_limits ol,producer_limits pl)
 :profile_(std::move(p)),obligations_(ol),producers_(pl){}
recovery_obligation_producer_store::recovery_obligation_producer_store(std::shared_ptr<lattice_db> owner,recovery_obligation_limits ol,receive_install_limits il,producer_limits pl)
 :owner_(std::move(owner)),obligations_(ol),installations_(il),limits_(pl){
    if(!owner_)fail(code::invalid_argument,"producer requires retained owner");obligation_policy(ol);producer_policy(pl);
}
void recovery_obligation_producer_store::initialize(){
    auto& db=producer_writer(owner_);backend b{db,obligations_};b.full_audit();receive_install_store(owner_,installations_).audit();
    atomic(db,[&]{
        if(db.query("SELECT 1 FROM main.sqlite_schema WHERE name='_lattice_obligation_producer_store' LIMIT 1").empty()){
            for(const auto& d:producer_definitions)db.execute(d.sql);
            db.execute("INSERT INTO main._lattice_obligation_producer_store VALUES(1,1,?,?,?,?,?,0,0,0)",
                {limits_.profiles,limits_.stamps,limits_.field_bytes,limits_.manifest_bytes,limits_.encoded_bytes});changed(db);
        }
        producer_backend{b,limits_}.audit();return 0;
    });
}
recovery_obligation_producer_program recovery_obligation_producer_store::compile(const producer_profile& p,recovery_obligation_limits ol,producer_limits pl){
    obligation_policy(ol);producer_policy(pl);if(profile_charge(p,ol,pl)>pl.encoded_bytes)fail(code::capacity,"producer profile exceeds byte budget");return {p,ol,pl};
}
recovery_obligation_producer_program recovery_obligation_producer_store::enroll(const producer_profile& p){
    auto result=compile(p,obligations_,limits_);producer_backend b{{producer_writer(owner_),obligations_},limits_};
    return atomic(b.base.db,[&]{
        b.audit();auto s=b.base.scope(p.contribution.binding.channel);
        if(!s || s->profile!=p.contribution || s->address.incarnation!=p.contribution_incarnation)fail(code::binding_mismatch,"producer enrollment requires exact current contribution");
        b.base.current(s->address);
        if(auto old=b.profile(s->address.channel)){if(*old!=p)fail(code::conflict,"producer immutable profile differs");return result;}
        // Adoption cannot stamp old records. Existing records remain unproven.
        auto old=b.config(),updated=old;updated.profiles=add(old.profiles,1);updated.bytes=add(old.bytes,profile_charge(p,obligations_,limits_));b.put(old,updated);
        b.base.db.execute("INSERT INTO main._lattice_obligation_producer_profile VALUES(?,?,?,?,?,?)",{bytes(s->address.channel),p.contribution_incarnation,p.program_revision,bytes(p.program_digest),p.grant_manifest,profile_charge(p,obligations_,limits_)});changed(b.base.db);
        if(b.profile(s->address.channel)!=p || b.config()!=updated)fail(code::corrupt_state,"producer enrollment postimage differs");return result;
    });
}
std::vector<producer_profile> recovery_obligation_producer_store::profiles()const{
    producer_backend b{{producer_writer(owner_),obligations_},limits_};b.base.full_audit();return b.audit();
}
std::optional<recovery_obligation_producer_stamp> recovery_obligation_producer_store::read_stamp(std::shared_ptr<lattice_db> owner,recovery_obligation_limits ol,receive_install_limits il,producer_limits pl,const recovery_obligation_address& address,const std::string& id){
    recovery_obligation_producer_store retained(std::move(owner),ol,il,pl);producer_backend b{{producer_writer(retained.owner_),ol},pl};
    b.config();const auto s=b.base.current(address);return b.stamp(s,uuid(id));
}
void recovery_obligation_producer_store::retire_contribution(const recovery_obligation_address& address,const producer_profile& expected){
    producer_backend b{{producer_writer(owner_),obligations_},limits_};
    atomic(b.base.db,[&]{
        const auto s=b.base.current(address);b.base.full_audit();b.audit();
        const auto p=b.profile(address.channel);if(!p || *p!=expected)fail(code::stale,"producer retirement immutable profile differs");
        if(s.mode==mode::frozen)fail(code::wrong_mode,"producer frozen contribution cannot retire");
        const auto entries=b.base.entries(s,false,false);int64_t count=0,charge=profile_charge(*p,obligations_,limits_);
        for(const auto& e:entries){if(e.stage!=stage::settled)fail(code::wrong_mode,"producer unresolved identity still pins evidence");if(b.stamp(s,e.canonical_original_id)){++count;charge=add(charge,stamp_charge(address.channel));}}
        auto old=b.config(),updated=old;if(old.profiles<1||old.stamps<count||old.bytes<charge)fail(code::corrupt_state,"producer retirement usage underflow");
        --updated.profiles;updated.stamps-=count;updated.bytes-=charge;
        b.base.db.execute("DELETE FROM main._lattice_obligation_producer_stamp WHERE channel=?",{bytes(address.channel)});if(b.base.db.changes()!=count)fail(code::corrupt_state,"producer stamp retirement count differs");
        b.base.db.execute("DELETE FROM main._lattice_obligation_producer_profile WHERE channel=? AND incarnation=?",{bytes(address.channel),address.incarnation});changed(b.base.db);b.put(old,updated);
        recovery_obligation_store(owner_,obligations_,installations_).retire(address);
        if(b.profile(address.channel) || !b.base.db.query("SELECT 1 FROM main._lattice_obligation_producer_stamp WHERE channel=? LIMIT 1",{bytes(address.channel)}).empty())fail(code::corrupt_state,"producer retirement postimage differs");
        b.audit();return 0;
    });
}
std::string recovery_obligation_producer_program::emit_tail(const std::string& relation,bool link,int64_t maximum_sql_bytes)const{
    field(relation,obligations_);if(relation.find('\0')!=std::string::npos)fail(code::invalid_argument,"producer relation contains NUL");
    const auto& p=profile_;const auto& binding=p.contribution.binding;
    if(obligations_.field_bytes<36)fail(code::capacity,"producer UUID exceeds ordinary field budget");
    const int64_t sql_cap=maximum_sql_bytes<0?producers_.encoded_bytes:maximum_sql_bytes;
    // Bound the largest temporary SQL fragment before copying any literals.
    // This fixed template has at most eight channel hex-literal occurrences
    // in one fragment (16 emitted bytes per input byte); all other input
    // fields have fewer occurrences. The fixed 32 KiB allowance covers its
    // keywords, identifiers, integer literals and repeated point subqueries.
    // Manifest content is never a hot SQL literal. Total output is counted
    // exactly, before each append, rather than multiplying manifest bytes or
    // rejecting a whole program from a loose aggregate expansion estimate.
    int64_t fragment_bound=32768;
    for(const auto* value:{&binding.channel,&binding.authority,&binding.source,&binding.epoch,&binding.scope,&binding.schema,&p.contribution.profile_digest,&p.contribution.receipt_namespace,&p.program_digest,&relation})
        for(int repeat=0;repeat<16;++repeat)fragment_bound=add(fragment_bound,static_cast<int64_t>(value->size()),code::capacity);
    if(sql_cap<0 || static_cast<uint64_t>(sql_cap)>std::numeric_limits<size_t>::max() || fragment_bound>sql_cap)
        fail(code::capacity,"producer generated SQL fragment exceeds explicit program cap");
    const auto ch=hex_blob(binding.channel),rel=hex_blob(relation),digest=hex_blob(p.program_digest);
    const auto n=[](int64_t value){return std::to_string(value);};
    const std::string original="lattice_recovery_producer_uuid_v1((SELECT globalId FROM AuditLog WHERE id=last_insert_rowid()))";
    const std::string target="lattice_recovery_producer_uuid_v1((SELECT globalRowId FROM AuditLog WHERE id=last_insert_rowid()))";
    const std::string stamped=" FROM _lattice_obligation_producer_stamp WHERE channel="+ch+" AND original="+original;
    const auto stamp=[&](const char* column){return "(SELECT "+std::string(column)+stamped+")";};
    const auto entry_charge=add(64+4*36,add(static_cast<int64_t>(binding.channel.size()),static_cast<int64_t>(relation.size())));
    const auto provenance_charge=stamp_charge(binding.channel);
    const auto typed=[](const std::string& alias,std::initializer_list<const char*> columns){std::string sql="1";for(auto c:columns)sql+=" AND typeof("+alias+c+")='integer'";return sql;};
    bounded_producer_sql result(sql_cap);
    result+=demand("changes()=1 AND lattice_recovery_producer_guard_v1("+ch+","+n(p.contribution_incarnation)+","+n(p.program_revision)+","+digest+","+rel+",1)=1");
    result+=demand("EXISTS(SELECT 1 FROM AuditLog a WHERE a.id=last_insert_rowid() AND "+typed("a.",{"id","rowId","isFromRemote","synthesized","isSynchronized"})+
        " AND a.id>0 AND a.rowId"+(link?std::string("=0"):std::string(">0"))+" AND a.isFromRemote=0 AND a.synthesized=0 AND a.isSynchronized=0 "
        "AND typeof(a.globalId)='text' AND length(CAST(a.globalId AS BLOB))=36 AND typeof(a.globalRowId)='text' AND length(CAST(a.globalRowId AS BLOB))=36 "
        "AND typeof(a.tableName)='text' AND CAST(a.tableName AS BLOB)="+rel+" AND typeof(a.operation)='text' AND a.operation IN ("+(link?std::string("'INSERT','DELETE'"):std::string("'INSERT','UPDATE','DELETE'"))+")) AND "+original+" IS NOT NULL AND "+target+" IS NOT NULL");
    result+=demand("NOT EXISTS(SELECT 1 FROM _lattice_obligation_entry WHERE channel="+ch+" AND original="+original+") AND NOT EXISTS(SELECT 1 FROM _lattice_obligation_entry WHERE channel="+ch+" AND audit_id=last_insert_rowid()) AND NOT EXISTS(SELECT 1"+stamped+")");
    // Bind exact fixed policy and immutable program before arithmetic/copying.
    std::string scope_check=typed("s.",{"incarnation","generation","revision","last_attempt","freeze_revision","freeze_record","freeze_export","mode","installed_sequence","installed_revision","installed_head","bytes"});
    for(const auto& pair:std::initializer_list<std::pair<const char*,std::string>>{{"channel",binding.channel},{"authority",binding.authority},{"source",binding.source},{"epoch",binding.epoch},{"scope",binding.scope},{"schema_digest",binding.schema},{"profile_digest",p.contribution.profile_digest},{"receipt_namespace",p.contribution.receipt_namespace}})
        scope_check+=" AND s."+std::string(pair.first)+"="+hex_blob(pair.second);
    scope_t profile_scope;profile_scope.profile=p.contribution;
    const auto base_scope_charge=scope_size(profile_scope,obligations_);
    scope_check+=" AND s.incarnation="+n(p.contribution_incarnation)+" AND s.generation>0 AND s.revision>0 AND s.revision<="+n(maximum)+
        " AND s.mode BETWEEN 0 AND 2 AND s.last_attempt>=0 AND s.freeze_revision BETWEEN 0 AND s.revision AND s.freeze_record>=0 AND s.freeze_export>=0 "
        "AND s.installed_sequence BETWEEN 0 AND s.last_attempt AND s.installed_revision>=0 AND s.installed_head>=0 "
        "AND typeof(s.installed_manifest)='blob' AND length(s.installed_manifest)<="+n(obligations_.field_bytes)+
        " AND s.bytes="+n(base_scope_charge)+"+length(s.installed_manifest) AND ((s.installed_sequence=0 AND s.installed_revision=0 AND s.installed_head=0 AND length(s.installed_manifest)=0) "
        "OR (s.installed_sequence>0 AND s.installed_revision>0 AND length(s.installed_manifest)>0)) AND (s.mode=0 OR (s.last_attempt>0 AND s.freeze_revision>0)) AND (s.mode!=2 OR s.installed_sequence=s.last_attempt)";
    const std::string global_check=typed("g.",{"id","version","max_scopes","max_records","max_field","max_bytes","scopes","records","bytes","incarnation","record_sequence","export_sequence"})+
        " AND g.id=1 AND g.version=1 AND g.max_scopes="+n(obligations_.scopes)+" AND g.max_records="+n(obligations_.records)+" AND g.max_field="+n(obligations_.field_bytes)+" AND g.max_bytes="+n(obligations_.encoded_bytes)+
        " AND g.scopes BETWEEN 1 AND g.max_scopes AND g.records>=0 AND g.records<g.max_records AND g.bytes>=s.bytes AND g.bytes<=g.max_bytes AND "+n(entry_charge)+"<=g.max_bytes-g.bytes AND g.incarnation>=s.incarnation "
        "AND s.revision<"+n(maximum)+" AND g.record_sequence>=g.records AND g.record_sequence<"+n(maximum)+" AND g.record_sequence>=s.freeze_record AND g.export_sequence>=s.freeze_export";
    const std::string producer_check=typed("u.",{"id","version","max_profiles","max_stamps","max_field","max_manifest","max_bytes","profiles","stamps","bytes"})+
        " AND u.id=1 AND u.version=1 AND u.max_profiles="+n(producers_.profiles)+" AND u.max_stamps="+n(producers_.stamps)+" AND u.max_field="+n(producers_.field_bytes)+" AND u.max_manifest="+n(producers_.manifest_bytes)+" AND u.max_bytes="+n(producers_.encoded_bytes)+
        " AND u.profiles BETWEEN 1 AND u.max_profiles AND u.stamps>=0 AND u.stamps<u.max_stamps AND u.bytes>=p.bytes AND u.bytes<=u.max_bytes AND "+n(provenance_charge)+"<=u.max_bytes-u.bytes";
    const std::string profile_check=typed("p.",{"incarnation","program_revision","bytes"})+" AND p.channel="+ch+" AND p.incarnation="+n(p.contribution_incarnation)+" AND p.program_revision="+n(p.program_revision)+" AND p.program_digest="+digest+
        " AND typeof(p.manifest)='blob' AND length(p.manifest)="+n(static_cast<int64_t>(p.grant_manifest.size()))+" AND p.bytes="+n(profile_charge(p,obligations_,producers_));
    const std::string intake=" FROM _lattice_obligation_scope s JOIN _lattice_obligation_producer_profile p ON p.channel=s.channel CROSS JOIN _lattice_obligation_store g CROSS JOIN _lattice_obligation_producer_store u WHERE s.channel="+ch+" AND "+scope_check+" AND "+global_check+" AND "+producer_check+" AND "+profile_check;
    result+=demand("EXISTS(SELECT 1"+intake+")");
    // Fixed integer snapshots permit exact postimage checks without a writing
    // UDF, per-identity full scans, temporary payloads or a reusable scratch slot.
    result+="INSERT INTO _lattice_obligation_producer_stamp SELECT "+ch+","+original+",p.incarnation,p.program_revision,last_insert_rowid(),g.record_sequence+1,s.generation,s.revision+1,g.scopes,g.records+1,g.bytes+"+n(entry_charge)+",g.incarnation,g.export_sequence,u.profiles,u.stamps+1,u.bytes+"+n(provenance_charge)+","+n(provenance_charge)+intake+";";
    result+=demand("changes()=1");
    result+="UPDATE _lattice_obligation_store SET records="+stamp("base_records")+",bytes="+stamp("base_bytes")+",record_sequence="+stamp("record_sequence")+
        " WHERE id=1 AND records="+stamp("base_records")+"-1 AND bytes="+stamp("base_bytes")+"-"+n(entry_charge)+" AND record_sequence="+stamp("record_sequence")+"-1;";
    result+=demand("changes()=1");
    result+="UPDATE _lattice_obligation_scope SET revision="+stamp("scope_revision")+" WHERE channel="+ch+" AND incarnation="+n(p.contribution_incarnation)+" AND generation="+stamp("generation")+" AND revision="+stamp("scope_revision")+"-1;";
    result+=demand("changes()=1");
    result+="UPDATE _lattice_obligation_producer_store SET stamps="+stamp("producer_stamps")+",bytes="+stamp("producer_bytes")+" WHERE id=1 AND stamps="+stamp("producer_stamps")+"-1 AND bytes="+stamp("producer_bytes")+"-"+n(provenance_charge)+";";
    result+=demand("changes()=1");
    result+="INSERT INTO _lattice_obligation_entry SELECT "+ch+","+original+",a.id,CAST(a.globalId AS BLOB),CAST(a.tableName AS BLOB),"+target+",CAST(a.globalRowId AS BLOB),0,"+stamp("record_sequence")+",NULL,0,NULL,NULL,0,"+n(entry_charge)+" FROM AuditLog a WHERE a.id=last_insert_rowid();";
    result+=demand("changes()=1");
    result+=demand("EXISTS(SELECT 1 FROM _lattice_obligation_store g WHERE "+typed("g.",{"id","version","max_scopes","max_records","max_field","max_bytes","scopes","records","bytes","incarnation","record_sequence","export_sequence"})+" AND g.id=1 AND g.version=1 AND g.max_scopes="+n(obligations_.scopes)+" AND g.max_records="+n(obligations_.records)+" AND g.max_field="+n(obligations_.field_bytes)+" AND g.max_bytes="+n(obligations_.encoded_bytes)+" AND g.scopes="+stamp("base_scopes")+" AND g.records="+stamp("base_records")+" AND g.bytes="+stamp("base_bytes")+" AND g.incarnation="+stamp("base_incarnation")+" AND g.record_sequence="+stamp("record_sequence")+" AND g.export_sequence="+stamp("base_export")+")");
    result+=demand("EXISTS(SELECT 1 FROM _lattice_obligation_producer_store u WHERE "+typed("u.",{"id","version","max_profiles","max_stamps","max_field","max_manifest","max_bytes","profiles","stamps","bytes"})+" AND u.id=1 AND u.version=1 AND u.max_profiles="+n(producers_.profiles)+" AND u.max_stamps="+n(producers_.stamps)+" AND u.max_field="+n(producers_.field_bytes)+" AND u.max_manifest="+n(producers_.manifest_bytes)+" AND u.max_bytes="+n(producers_.encoded_bytes)+" AND u.profiles="+stamp("producer_profiles")+" AND u.stamps="+stamp("producer_stamps")+" AND u.bytes="+stamp("producer_bytes")+")");
    result+=demand("EXISTS(SELECT 1 FROM _lattice_obligation_scope s JOIN _lattice_obligation_producer_profile p ON p.channel=s.channel WHERE "+scope_check+" AND "+profile_check+" AND s.generation="+stamp("generation")+" AND s.revision="+stamp("scope_revision")+")");
    result+=demand("EXISTS(SELECT 1 FROM _lattice_obligation_entry e JOIN AuditLog a ON a.id=e.audit_id WHERE e.channel="+ch+" AND e.original="+original+" AND e.audit_id=last_insert_rowid() AND e.actual_original=CAST(a.globalId AS BLOB) AND e.table_name="+rel+" AND e.target="+target+" AND e.actual_target=CAST(a.globalRowId AS BLOB) AND e.origin=0 AND e.record_sequence="+stamp("record_sequence")+" AND e.first_export IS NULL AND e.stage=0 AND e.ack_position IS NULL AND e.ack_outcome IS NULL AND e.settled_sequence=0 AND e.bytes="+n(entry_charge)+")");
    result+=demand("EXISTS(SELECT 1 FROM _lattice_obligation_producer_stamp t WHERE t.channel="+ch+" AND t.original="+original+" AND "+typed("t.",{"incarnation","program_revision","audit_id","record_sequence","generation","scope_revision","base_scopes","base_records","base_bytes","base_incarnation","base_export","producer_profiles","producer_stamps","producer_bytes","bytes"})+" AND t.incarnation="+n(p.contribution_incarnation)+" AND t.program_revision="+n(p.program_revision)+" AND t.audit_id=last_insert_rowid() AND t.bytes="+n(provenance_charge)+")");
    result+=demand("lattice_recovery_producer_guard_v1("+ch+","+n(p.contribution_incarnation)+","+n(p.program_revision)+","+digest+","+rel+",1)=1");
    return result.finish();
}
recovery_obligation_producer_inventory recovery_obligation_producer_store::bootstrap_profiles(std::shared_ptr<database> physical,const recovery_obligation_producer_discovery_limits& caps,
    const std::function<void(database&,const recovery_obligation_producer_inventory&)>& validate){
    obligation_policy(caps.obligations);producer_policy(caps.producers);
    if(caps.installations.channels<0 || caps.installations.field_bytes<=0 || caps.installations.field_bytes>std::numeric_limits<int>::max() || caps.installations.encoded_bytes<0)
        fail(code::invalid_argument,"producer bootstrap invalid independent receiver caps");
    if(!physical)fail(code::invalid_argument,"producer bootstrap requires retained physical connection");
    auto* handle=physical->internal_handle();auto* mutex=handle?sqlite3_db_mutex(handle):nullptr;
#ifndef __EMSCRIPTEN__
    if(!mutex)fail(code::transaction_required,"producer bootstrap requires serialized physical connection");
#endif
    if(!handle || sqlite3_mutex_try(mutex)!=SQLITE_OK)fail(code::transaction_required,"producer bootstrap physical connection is busy");
    struct release_mutex{sqlite3_mutex* mutex;~release_mutex(){sqlite3_mutex_leave(mutex);}} release{mutex};
    if(!database::maintenance_scope::idle(*physical))fail(code::transaction_required,"producer bootstrap requires idle physical connection");
    database::maintenance_scope admission(*physical); // recursive level; first level was try-only
    physical->execute("BEGIN");
    try {
        if(sqlite3_get_autocommit(handle)!=0)fail(code::transaction_required,"producer bootstrap did not own read snapshot");
        recovery_obligation_producer_inventory result;
        auto family=physical->query("SELECT name FROM main.sqlite_schema WHERE name IN ('_lattice_obligation_producer_store','_lattice_obligation_producer_profile','_lattice_obligation_producer_stamp') LIMIT 4");
        if(!family.empty()){
            if(family.size()!=3)fail(code::corrupt_state,"producer bootstrap partial family; migration refused");
            for(const auto& d:producer_definitions)exact_definition(*physical,d);
            for(const auto& d:definitions)exact_definition(*physical,d);
            for(const auto& d:install_definitions)exact_definition(*physical,d);
            // Read fixed scalar policy before any stored byte field. Every stored
            // limit must fit the independent adapter's discovery policy.
            auto read_fixed=[&](const std::string& query){auto rows=physical->query(query);if(rows.size()!=1 || number(rows[0],"id")!=1 || number(rows[0],"version")!=1)fail(code::corrupt_state,"producer bootstrap fixed policy is missing/unknown");return rows[0];};
            auto o=read_fixed("SELECT "+ints({"id","version","max_scopes","max_records","max_field","max_bytes"})+" FROM main._lattice_obligation_store LIMIT 2");
            result.stored_obligation_limits={number(o,"max_scopes"),number(o,"max_records"),number(o,"max_field"),number(o,"max_bytes")};
            auto i=read_fixed("SELECT "+ints({"id","version","max_channels","max_field_bytes","max_bytes","channels","bytes"})+" FROM main._lattice_install_store LIMIT 2");
            result.stored_installation_limits={number(i,"max_channels"),number(i,"max_field_bytes"),number(i,"max_bytes")};
            auto p=read_fixed("SELECT "+ints({"id","version","max_profiles","max_stamps","max_field","max_manifest","max_bytes"})+" FROM main._lattice_obligation_producer_store LIMIT 2");
            result.stored_producer_limits={number(p,"max_profiles"),number(p,"max_stamps"),number(p,"max_field"),number(p,"max_manifest"),number(p,"max_bytes")};
            const auto& ol=result.stored_obligation_limits;const auto& il=result.stored_installation_limits;const auto& pl=result.stored_producer_limits;
            obligation_policy(ol);producer_policy(pl);
            if(ol.scopes>caps.obligations.scopes || ol.records>caps.obligations.records || ol.field_bytes>caps.obligations.field_bytes || ol.encoded_bytes>caps.obligations.encoded_bytes ||
                il.channels<0 || il.field_bytes<=0 || il.encoded_bytes<0 || il.channels>caps.installations.channels || il.field_bytes>caps.installations.field_bytes || il.encoded_bytes>caps.installations.encoded_bytes ||
                pl.profiles>caps.producers.profiles || pl.stamps>caps.producers.stamps || pl.field_bytes>caps.producers.field_bytes || pl.manifest_bytes>caps.producers.manifest_bytes || pl.encoded_bytes>caps.producers.encoded_bytes ||
                !fits(number(i,"channels"),0,il.channels) || !fits(number(i,"bytes"),0,il.encoded_bytes))
                fail(code::capacity,"producer stored policy exceeds independent bootstrap admission");
            producer_backend b{{*physical,ol},pl};b.base.full_audit();result.profiles=b.audit();result.initialized=true;
        }
        const auto before_validation=sqlite3_total_changes64(handle);
        if(validate)validate(*physical,result);
        if(sqlite3_get_autocommit(handle)!=0 || sqlite3_txn_state(handle,nullptr)==SQLITE_TXN_WRITE || sqlite3_total_changes64(handle)!=before_validation)
            fail(code::transaction_required,"producer bootstrap validator changed its read-only snapshot");
        for(auto* statement=sqlite3_next_stmt(handle,nullptr);statement;statement=sqlite3_next_stmt(handle,statement))
            if(sqlite3_stmt_busy(statement))fail(code::transaction_required,"producer bootstrap validator left a live statement");
        physical->execute("ROLLBACK");if(sqlite3_get_autocommit(handle)==0)fail(code::cleanup_failed,"producer bootstrap read snapshot did not settle");return result;
    }catch(...){
        auto primary=std::current_exception();
        if(sqlite3_get_autocommit(handle)==0){try{physical->execute("ROLLBACK");if(sqlite3_get_autocommit(handle)==0)throw db_error("producer bootstrap remains in transaction");}
            catch(...){throw recovery_obligation_error(code::cleanup_failed,"producer bootstrap rollback failed; retained physical connection must be discarded",primary,std::current_exception());}}
        std::rethrow_exception(primary);
    }
}
} // namespace lattice::detail
