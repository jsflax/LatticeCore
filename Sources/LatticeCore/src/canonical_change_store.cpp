#include "canonical_change_store.hpp"
#include "recovery_writer_access.hpp"
#include <limits>
#include <map>
#include <set>
#include <utility>

namespace lattice::detail {
namespace {
using code = canonical_store_error_code;
using row = database::row_t;
using blob = std::vector<uint8_t>;
constexpr int64_t max_int = std::numeric_limits<int64_t>::max();
[[noreturn]] void fail(code c, const char* why) { throw canonical_store_error(c, why); }
int64_t integer(const row& r, const char* k) {
    auto i = r.find(k);
    if (i == r.end() || !std::holds_alternative<int64_t>(i->second))
        fail(code::corrupt_state, "canonical store expected integer");
    return std::get<int64_t>(i->second);
}
blob encoded(const std::string& s) { return blob(s.begin(), s.end()); }
std::string decoded(const row& r, const char* k) {
    auto i = r.find(k);
    if (i == r.end() || !std::holds_alternative<blob>(i->second))
        fail(code::corrupt_state, "canonical store expected bounded encoded identity");
    const auto& b = std::get<blob>(i->second);
    if (b.empty()) fail(code::corrupt_state, "canonical store empty identity");
    return std::string(b.begin(), b.end());
}
void valid_bytes(const std::string& s, int64_t limit) {
    if (s.empty() || s.size() > static_cast<uint64_t>(limit))
        fail(code::invalid_argument, "canonical store identity exceeds explicit limit");
}
void valid_identity(const canonical_identity& i, const canonical_store_limits& b) {
    valid_bytes(i.table, b.identity_bytes); valid_bytes(i.global_id, b.identity_bytes);
}
bool valid_outcome(int64_t n) { return n >= 1 && n <= 3; }
int64_t marker_charge(const canonical_identity& i) {
    return 24 + static_cast<int64_t>(i.table.size() + i.global_id.size());
}
int64_t receipt_charge(const canonical_receipt_request& r) {
    return 32 + static_cast<int64_t>(r.original_id.size()) +
        (r.target ? static_cast<int64_t>(r.target->table.size() + r.target->global_id.size()) : 0) +
        (r.namespace_id ? static_cast<int64_t>(r.namespace_id->size()) : 0);
}
bool fits(int64_t old, int64_t add, int64_t limit) {
    return old >= 0 && add >= 0 && old <= limit && add <= limit - old;
}
void changed(database& db, int64_t n = 1) {
    if (db.changes() != n) fail(code::corrupt_state, "canonical store write ignored or changed unexpected rows");
}
template<class F> auto atomic(database& db, F&& body) {
    db.execute("SAVEPOINT lattice_canonical_primitive");
    try {
        auto result = body(); db.execute("RELEASE lattice_canonical_primitive"); return result;
    } catch (...) {
        auto primary = std::current_exception();
        if (!db.is_in_transaction()) std::rethrow_exception(primary);
        try {
            db.execute("ROLLBACK TO lattice_canonical_primitive");
            db.execute("RELEASE lattice_canonical_primitive");
        } catch (...) {
            throw canonical_store_error(code::cleanup_failed,
                "canonical savepoint cleanup failed; abort owning transaction", primary, std::current_exception());
        }
        std::rethrow_exception(primary);
    }
}
}

void canonical_namespace_profile::validate() const {
    valid_bytes(local_namespace,256);
    if(entries.empty() || entries.size()>64)fail(code::invalid_argument,"canonical namespace catalog outside fixed v2 bounds");
    std::set<std::string> unique;
    for(const auto& entry:entries) {
        valid_bytes(entry.namespace_id,256);valid_bytes(entry.coverage_id,256);
        if(entry.revision<=0 || !unique.insert(entry.namespace_id).second)
            fail(code::invalid_argument,"canonical namespace revision or duplicate identity");
    }
    if(!unique.count(local_namespace))fail(code::invalid_argument,"canonical source-local namespace not enrolled");
}
canonical_change_store::canonical_change_store(lattice_db& owner, const canonical_store_binding& binding,
                                               canonical_store_limits limits,const canonical_namespace_profile* namespaces)
    : owner_(owner), limits_(limits) {
    for (const auto* s : {&binding.source, &binding.epoch, &binding.scope, &binding.schema}) valid_bytes(*s, 256);
    if (limits.markers < 0 || limits.marker_bytes < 0 || limits.receipts < 0 || limits.receipt_bytes < 0 ||
        limits.batch_identities <= 0 || limits.batch_identities > 4096 ||
        limits.identity_bytes <= 0 || limits.identity_bytes > 256 ||
        limits.operation_bytes <= 0 || limits.operation_bytes > 256)
        fail(code::invalid_argument, "invalid canonical store limits");
    if(namespaces){namespaces->validate();namespaces_=*namespaces;}
    binding_=binding; // copy only after every component has passed its hard cap
}
database& canonical_change_store::connection() const {
    auto* db = recovery_writer_access::active_writer(owner_);
    if (!db) fail(code::transaction_required, "canonical store requires this thread's owned main WRITE transaction");
    return *db;
}
canonical_store_state canonical_change_store::state() const {
    auto r = connection().query("SELECT "
        "CASE WHEN typeof(id)='integer' THEN id END AS id,"
        "CASE WHEN typeof(version)='integer' THEN version END AS version,"
        "CASE WHEN typeof(head)='integer' THEN head END AS head,"
        "CASE WHEN typeof(floor)='integer' THEN floor END AS floor,"
        "CASE WHEN typeof(markers)='integer' THEN markers END AS markers,"
        "CASE WHEN typeof(marker_bytes)='integer' THEN marker_bytes END AS marker_bytes,"
        "CASE WHEN typeof(receipts)='integer' THEN receipts END AS receipts,"
        "CASE WHEN typeof(receipt_bytes)='integer' THEN receipt_bytes END AS receipt_bytes,"
        "CASE WHEN typeof(max_markers)='integer' THEN max_markers END AS max_markers,"
        "CASE WHEN typeof(max_marker_bytes)='integer' THEN max_marker_bytes END AS max_marker_bytes,"
        "CASE WHEN typeof(max_receipts)='integer' THEN max_receipts END AS max_receipts,"
        "CASE WHEN typeof(max_receipt_bytes)='integer' THEN max_receipt_bytes END AS max_receipt_bytes,"
        "CASE WHEN typeof(max_batch)='integer' THEN max_batch END AS max_batch,"
        "CASE WHEN typeof(max_identity)='integer' THEN max_identity END AS max_identity,"
        "CASE WHEN typeof(max_operation)='integer' THEN max_operation END AS max_operation,"
        "CASE WHEN typeof(source)='blob' AND length(source) BETWEEN 1 AND 256 THEN source END AS source,"
        "CASE WHEN typeof(epoch)='blob' AND length(epoch) BETWEEN 1 AND 256 THEN epoch END AS epoch,"
        "CASE WHEN typeof(scope)='blob' AND length(scope) BETWEEN 1 AND 256 THEN scope END AS scope,"
        "CASE WHEN typeof(schema_id)='blob' AND length(schema_id) BETWEEN 1 AND 256 THEN schema_id END AS schema_id "
        "FROM main._lattice_canonical_store LIMIT 2");
    if (r.size() != 1 || integer(r[0],"id") != 1 || integer(r[0],"version") != (namespaces_?2:1))
        fail(code::corrupt_state, "missing or unsupported canonical store");
    const auto& v = r[0];
    if (canonical_store_binding{decoded(v,"source"),decoded(v,"epoch"),decoded(v,"scope"),decoded(v,"schema_id")} != binding_)
        fail(code::binding_mismatch, "canonical source binding differs; no implicit reset");
    if (canonical_store_limits{integer(v,"max_markers"),integer(v,"max_marker_bytes"),integer(v,"max_receipts"),
        integer(v,"max_receipt_bytes"),integer(v,"max_batch"),integer(v,"max_identity"),integer(v,"max_operation")} != limits_)
        fail(code::limits_mismatch, "canonical limits differ from durable configuration");
    canonical_store_state s{integer(v,"head"),integer(v,"floor"),integer(v,"markers"),integer(v,"marker_bytes"),
        integer(v,"receipts"),integer(v,"receipt_bytes")};
    if (s.floor < 0 || s.head < s.floor || !fits(s.markers,0,limits_.markers) ||
        !fits(s.marker_bytes,0,limits_.marker_bytes) || !fits(s.receipts,0,limits_.receipts) ||
        !fits(s.receipt_bytes,0,limits_.receipt_bytes)) fail(code::corrupt_state, "canonical counters outside bounds");
    return s;
}
void canonical_change_store::write_state(const canonical_store_state& old, const canonical_store_state& next) {
    auto& db = connection();
    db.execute("UPDATE main._lattice_canonical_store SET head=?,floor=?,markers=?,marker_bytes=?,receipts=?,receipt_bytes=? "
        "WHERE id=1 AND head=? AND floor=? AND markers=? AND marker_bytes=? AND receipts=? AND receipt_bytes=?",
        {next.head,next.floor,next.markers,next.marker_bytes,next.receipts,next.receipt_bytes,
         old.head,old.floor,old.markers,old.marker_bytes,old.receipts,old.receipt_bytes});
    changed(db);
    if (state() != next) fail(code::corrupt_state, "canonical counter write did not persist exact state");
}
void canonical_change_store::initialize() {
    auto& db = connection();
    const auto namespace_found=db.query("SELECT name FROM main.sqlite_master WHERE name='_lattice_canonical_namespace'");
    const auto found = db.query("SELECT name FROM main.sqlite_master WHERE name IN "
        "('_lattice_canonical_store','_lattice_canonical_touch','_lattice_canonical_receipt','_lattice_canonical_touch_position')");
    if (!found.empty()) {
        if (found.size() != 4 || namespace_found.size()!=(namespaces_?1u:0u)) fail(code::corrupt_state, "partial canonical schema; migration refused");
        audit(); return;
    }
    if(!namespace_found.empty())fail(code::corrupt_state,"orphan canonical namespace catalog");
    atomic(db, [&] {
        db.execute("CREATE TABLE main._lattice_canonical_store (id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,"
            "source BLOB NOT NULL,epoch BLOB NOT NULL,scope BLOB NOT NULL,schema_id BLOB NOT NULL,"
            "head INTEGER NOT NULL,floor INTEGER NOT NULL,markers INTEGER NOT NULL,marker_bytes INTEGER NOT NULL,"
            "receipts INTEGER NOT NULL,receipt_bytes INTEGER NOT NULL,max_markers INTEGER NOT NULL,max_marker_bytes INTEGER NOT NULL,"
            "max_receipts INTEGER NOT NULL,max_receipt_bytes INTEGER NOT NULL,max_batch INTEGER NOT NULL,"
            "max_identity INTEGER NOT NULL,max_operation INTEGER NOT NULL) WITHOUT ROWID");
        db.execute("CREATE TABLE main._lattice_canonical_touch (relation BLOB NOT NULL,identity BLOB NOT NULL,"
            "position INTEGER NOT NULL,charge INTEGER NOT NULL,PRIMARY KEY(relation,identity)) WITHOUT ROWID");
        db.execute("CREATE INDEX main._lattice_canonical_touch_position ON _lattice_canonical_touch(position,relation,identity)");
        db.execute("CREATE TABLE main._lattice_canonical_receipt (original_id BLOB PRIMARY KEY NOT NULL,"
            "position INTEGER NOT NULL,outcome INTEGER NOT NULL,relation BLOB,identity BLOB,charge INTEGER NOT NULL"+
            std::string(namespaces_?",namespace_id BLOB NOT NULL":"")+") WITHOUT ROWID");
        if(namespaces_) {
            db.execute("CREATE TABLE main._lattice_canonical_namespace(namespace_id BLOB PRIMARY KEY NOT NULL,coverage_id BLOB NOT NULL,revision INTEGER NOT NULL,status INTEGER NOT NULL,is_local INTEGER NOT NULL) WITHOUT ROWID");
            for(const auto& entry:namespaces_->entries) {
                db.execute("INSERT INTO main._lattice_canonical_namespace VALUES(?,?,?,1,?)",
                    {encoded(entry.namespace_id),encoded(entry.coverage_id),entry.revision,int64_t(entry.namespace_id==namespaces_->local_namespace)});
                changed(db);
            }
        }
        db.execute("INSERT INTO main._lattice_canonical_store VALUES(1,"+std::to_string(namespaces_?2:1)+",?,?,?,?,0,0,0,0,0,0,?,?,?,?,?,?,?)",
            {encoded(binding_.source),encoded(binding_.epoch),encoded(binding_.scope),encoded(binding_.schema),
             limits_.markers,limits_.marker_bytes,limits_.receipts,limits_.receipt_bytes,
             limits_.batch_identities,limits_.identity_bytes,limits_.operation_bytes});
        changed(db); audit(); return true;
    });
}
void canonical_change_store::audit() const {
    const auto s = state(); auto& db = connection();
    if(namespaces_) {
        const auto rows=db.query("SELECT CASE WHEN typeof(namespace_id)='blob' AND length(namespace_id) BETWEEN 1 AND 256 THEN namespace_id END AS namespace_id,"
            "CASE WHEN typeof(coverage_id)='blob' AND length(coverage_id) BETWEEN 1 AND 256 THEN coverage_id END AS coverage_id,"
            "CASE WHEN typeof(revision)='integer' THEN revision END AS revision,CASE WHEN typeof(status)='integer' THEN status END AS status,"
            "CASE WHEN typeof(is_local)='integer' THEN is_local END AS is_local FROM main._lattice_canonical_namespace LIMIT 65");
        if(rows.size()!=namespaces_->entries.size())fail(code::corrupt_state,"canonical namespace inventory differs");
        std::set<std::string> seen;
        for(const auto& row:rows) {
            const auto id=decoded(row,"namespace_id");bool matched=false;
            for(const auto& entry:namespaces_->entries)if(entry.namespace_id==id)
                matched=decoded(row,"coverage_id")==entry.coverage_id && integer(row,"revision")==entry.revision;
            if(!matched || !seen.insert(id).second || integer(row,"status")!=1 ||
                integer(row,"is_local")!=int64_t(id==namespaces_->local_namespace))
                fail(code::corrupt_state,"canonical namespace provenance differs");
        }
    }
    const auto tables = db.query("SELECT name,wr FROM pragma_table_list WHERE schema='main' AND name IN "
        "('_lattice_canonical_store','_lattice_canonical_touch','_lattice_canonical_receipt')");
    if (tables.size()!=3) fail(code::corrupt_state,"canonical metadata table missing");
    for (const auto& t : tables) if (integer(t,"wr")!=1) fail(code::corrupt_state,"canonical metadata must be WITHOUT ROWID");
    if (!db.query("SELECT 1 FROM main._lattice_canonical_touch WHERE typeof(relation)!='blob' OR length(relation) NOT BETWEEN 1 AND ? "
        "OR typeof(identity)!='blob' OR length(identity) NOT BETWEEN 1 AND ? OR typeof(position)!='integer' OR position<=? OR position>? "
        "OR typeof(charge)!='integer' OR charge!=24+length(relation)+length(identity) LIMIT 1",
        {limits_.identity_bytes,limits_.identity_bytes,s.floor,s.head}).empty()) fail(code::corrupt_state,"malformed canonical marker");
    if (!db.query("SELECT 1 FROM main._lattice_canonical_receipt WHERE typeof(original_id)!='blob' OR length(original_id) NOT BETWEEN 1 AND ? "
        "OR typeof(position)!='integer' OR position<=0 OR position>? OR typeof(outcome)!='integer' OR outcome NOT IN(1,2,3) "
        "OR (relation IS NULL)!=(identity IS NULL) OR (relation IS NOT NULL AND (typeof(relation)!='blob' OR length(relation) NOT BETWEEN 1 AND ? "
        "OR typeof(identity)!='blob' OR length(identity) NOT BETWEEN 1 AND ?)) OR typeof(charge)!='integer' "
        "OR charge!=32+length(original_id)+COALESCE(length(relation),0)+COALESCE(length(identity),0)"+
        std::string(namespaces_?"+length(namespace_id) OR typeof(namespace_id)!='blob' OR length(namespace_id) NOT BETWEEN 1 AND 256 OR NOT EXISTS(SELECT 1 FROM main._lattice_canonical_namespace n WHERE n.namespace_id=_lattice_canonical_receipt.namespace_id AND n.status=1)":"")+" LIMIT 1",
        {limits_.operation_bytes,s.head,limits_.identity_bytes,limits_.identity_bytes}).empty()) fail(code::corrupt_state,"malformed canonical receipt");
    const auto m = db.query("SELECT COUNT(*) AS n,COALESCE(SUM(charge),0) AS bytes FROM main._lattice_canonical_touch").at(0);
    const auto r = db.query("SELECT COUNT(*) AS n,COALESCE(SUM(charge),0) AS bytes FROM main._lattice_canonical_receipt").at(0);
    if (integer(m,"n")!=s.markers || integer(m,"bytes")!=s.marker_bytes || integer(r,"n")!=s.receipts || integer(r,"bytes")!=s.receipt_bytes)
        fail(code::corrupt_state,"canonical counters differ from actual storage");
}
std::optional<int64_t> canonical_change_store::touch(const canonical_identity& id) const {
    valid_identity(id,limits_); const auto s=state();
    const auto rows=connection().query("SELECT CASE WHEN typeof(position)='integer' THEN position END AS position,"
        "CASE WHEN typeof(charge)='integer' THEN charge END AS charge FROM main._lattice_canonical_touch WHERE relation=? AND identity=?",
        {encoded(id.table),encoded(id.global_id)});
    if (rows.empty()) return std::nullopt;
    if (rows.size()!=1 || integer(rows[0],"charge")!=marker_charge(id)) fail(code::corrupt_state,"invalid addressed marker");
    const auto p=integer(rows[0],"position");
    if (p<=s.floor || p>s.head) fail(code::corrupt_state,"addressed marker outside retained range");
    return p;
}
std::optional<canonical_receipt> canonical_change_store::receipt(const std::string& id) const {
    valid_bytes(id,limits_.operation_bytes); const auto s=state();
    const auto rows=connection().query("SELECT CASE WHEN typeof(position)='integer' THEN position END AS position,"
        "CASE WHEN typeof(outcome)='integer' THEN outcome END AS outcome,"
        "CASE WHEN typeof(charge)='integer' THEN charge END AS charge,(relation IS NULL AND identity IS NULL) AS no_target,"
        "CASE WHEN typeof(relation)='blob' AND length(relation) BETWEEN 1 AND ? THEN relation END AS relation,"
        "CASE WHEN typeof(identity)='blob' AND length(identity) BETWEEN 1 AND ? THEN identity END AS identity "
        +std::string(namespaces_?",CASE WHEN typeof(namespace_id)='blob' AND length(namespace_id) BETWEEN 1 AND 256 THEN namespace_id END AS namespace_id ":"")+
        "FROM main._lattice_canonical_receipt WHERE original_id=?", {limits_.identity_bytes,limits_.identity_bytes,encoded(id)});
    if (rows.empty()) return std::nullopt;
    if (rows.size()!=1) fail(code::corrupt_state,"duplicate canonical receipt");
    const auto& v=rows[0]; const auto p=integer(v,"position"), outcome=integer(v,"outcome");
    if (p<=0 || p>s.head || !valid_outcome(outcome)) fail(code::corrupt_state,"invalid canonical receipt outcome");
    canonical_receipt result{{id,static_cast<canonical_receipt_outcome>(outcome),std::nullopt},p};
    if (!integer(v,"no_target")) result.original.target=canonical_identity{decoded(v,"relation"),decoded(v,"identity")};
    if(namespaces_) {
        result.original.namespace_id=decoded(v,"namespace_id");
        bool known=false;for(const auto& entry:namespaces_->entries)if(entry.namespace_id==*result.original.namespace_id)known=true;
        if(!known)fail(code::corrupt_state,"canonical addressed receipt namespace is not enrolled");
    }
    if (integer(v,"charge")!=receipt_charge(result.original)) fail(code::corrupt_state,"invalid canonical receipt charge");
    return result;
}
canonical_record_result canonical_change_store::record(const std::vector<canonical_identity>& identities,
    const std::optional<canonical_receipt_request>& request) {
    const auto old=state();
    if (request) {
        if(bool(request->namespace_id)!=bool(namespaces_))fail(code::invalid_argument,"canonical receipt profile mismatch");
        if(namespaces_) {
            bool admitted=false;for(const auto& n:namespaces_->entries)if(n.namespace_id==*request->namespace_id)admitted=true;
            if(!admitted)fail(code::invalid_argument,"canonical receipt namespace not enrolled");
        }
        if (const auto prior=receipt(request->original_id)) {
            if(prior->original.namespace_id!=request->namespace_id)fail(code::binding_mismatch,"original ID already belongs to another namespace");
            return {prior->position,false,prior};
        }
        if (!valid_outcome(static_cast<int64_t>(request->outcome))) fail(code::invalid_argument,"invalid receipt outcome");
        if (request->target) valid_identity(*request->target,limits_);
    }
    if (identities.size()>static_cast<uint64_t>(limits_.batch_identities)) fail(code::capacity,"canonical batch identity limit");
    std::map<std::pair<std::string,std::string>,canonical_identity> distinct;
    for (const auto& id: identities) { valid_identity(id,limits_); distinct.emplace(std::make_pair(id.table,id.global_id),id); }
    if (distinct.empty() && !request) return {old.head,false,std::nullopt};
    if (old.head==max_int) fail(code::sequence_exhausted,"canonical head exhausted");
    auto next=old; ++next.head;
    for (const auto& [key,id]:distinct) if (!touch(id)) {
        const auto charge=marker_charge(id);
        if (!fits(next.markers,1,limits_.markers) || !fits(next.marker_bytes,charge,limits_.marker_bytes))
            fail(code::capacity,"canonical marker capacity exhausted");
        ++next.markers; next.marker_bytes+=charge;
    }
    if (request) {
        const auto charge=receipt_charge(*request);
        if (!fits(next.receipts,1,limits_.receipts) || !fits(next.receipt_bytes,charge,limits_.receipt_bytes))
            fail(code::capacity,"canonical receipt capacity exhausted; no eviction");
        ++next.receipts; next.receipt_bytes+=charge;
    }
    auto& db=connection();
    return atomic(db,[&] {
        write_state(old,next);
        for (const auto& [key,id]:distinct) {
            db.execute("INSERT INTO main._lattice_canonical_touch VALUES(?,?,?,?) "
                "ON CONFLICT(relation,identity) DO UPDATE SET position=excluded.position",
                {encoded(id.table),encoded(id.global_id),next.head,marker_charge(id)});
            changed(db);
            if (touch(id)!=std::optional<int64_t>(next.head)) fail(code::corrupt_state,"canonical marker update lost");
        }
        std::optional<canonical_receipt> result;
        if (request) {
            column_value_t table=nullptr, identity=nullptr;
            if (request->target) { table=encoded(request->target->table); identity=encoded(request->target->global_id); }
            std::vector<column_value_t> values{encoded(request->original_id),next.head,static_cast<int64_t>(request->outcome),table,identity,receipt_charge(*request)};
            if(namespaces_)values.push_back(encoded(*request->namespace_id));
            db.execute("INSERT INTO main._lattice_canonical_receipt VALUES(?,?,?,?,?,?"+std::string(namespaces_?",?":"")+")",values);
            changed(db); result=receipt(request->original_id);
            if (result!=std::optional<canonical_receipt>(canonical_receipt{*request,next.head}))
                fail(code::corrupt_state,"canonical receipt insertion lost");
        }
        return canonical_record_result{next.head,true,result};
    });
}
void canonical_change_store::require_base(int64_t base) const {
    const auto s=state();
    if (base<0) fail(code::invalid_argument,"negative canonical base");
    if (base<s.floor) fail(code::base_retired,"canonical base retired; full recovery required");
    if (base>s.head) fail(code::base_ahead,"canonical base ahead of head");
}
void canonical_change_store::advance_floor(int64_t floor, int64_t protected_base) {
    audit(); const auto old=state();
    if (floor<old.floor || floor>old.head || protected_base<old.floor || protected_base>old.head)
        fail(code::invalid_argument,"invalid canonical floor or protected base");
    if (floor>protected_base) fail(code::protected_floor,"canonical floor would pass protected base");
    const auto removed=connection().query("SELECT COUNT(*) AS n,COALESCE(SUM(charge),0) AS bytes "
        "FROM main._lattice_canonical_touch WHERE position<=?",{floor}).at(0);
    auto next=old; next.floor=floor; next.markers-=integer(removed,"n"); next.marker_bytes-=integer(removed,"bytes");
    auto& db=connection();
    atomic(db,[&] {
        write_state(old,next);
        db.execute("DELETE FROM main._lattice_canonical_touch WHERE position<=?",{floor}); changed(db,integer(removed,"n"));
        audit(); return true;
    });
}
} // namespace lattice::detail
