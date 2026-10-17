#include "canonical_receipt_coverage.hpp"
#include <limits>
#include <algorithm>
#include <tuple>

namespace lattice::detail {
namespace {
using blob=std::vector<uint8_t>;
[[noreturn]] void refuse(const char* s){throw db_error(s);}
blob bytes(const std::string& s){return {s.begin(),s.end()};}
int64_t integer(const database::row_t& r,const char* key){auto it=r.find(key);if(it==r.end()||!std::holds_alternative<int64_t>(it->second))refuse("receipt coverage expected integer");return std::get<int64_t>(it->second);}
std::string binary(const database::row_t& r,const char* key,size_t cap){auto it=r.find(key);if(it==r.end()||!std::holds_alternative<blob>(it->second))refuse("receipt coverage expected bounded identity");const auto& b=std::get<blob>(it->second);if(b.empty()||b.size()>cap)refuse("receipt coverage identity bound");return {b.begin(),b.end()};}
bool admitted(const canonical_coverage_profile& p,const std::string& ns){return std::find(p.namespaces.begin(),p.namespaces.end(),ns)!=p.namespaces.end();}
void binding(const canonical_coverage_profile& p,const recovery_receipt_binding& b,const std::string& ns){p.validate();b.validate();if(b.cohort_id!=p.cohort_id||b.cohort_revision!=p.revision||!admitted(p,ns))refuse("receipt coverage is outside actual enrolled cohort");}
void changed(database& db){if(db.changes()!=1)refuse("receipt coverage schema enrollment was ignored");}
}
std::map<std::string,std::string> canonical_coverage_schema(){return {
    {"_lattice_canonical_receipt_profile","CREATE TABLE _lattice_canonical_receipt_profile(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,cohort BLOB NOT NULL,revision INTEGER NOT NULL,codec INTEGER NOT NULL,mutation INTEGER NOT NULL,origins INTEGER NOT NULL,origin_bytes INTEGER NOT NULL,cells INTEGER NOT NULL,cell_bytes INTEGER NOT NULL,max_origins INTEGER NOT NULL,max_origin_bytes INTEGER NOT NULL,max_cells INTEGER NOT NULL,max_cell_bytes INTEGER NOT NULL) WITHOUT ROWID"},
    {"_lattice_canonical_receipt_member","CREATE TABLE _lattice_canonical_receipt_member(namespace_id BLOB PRIMARY KEY NOT NULL) WITHOUT ROWID"},
    {"_lattice_canonical_receipt_origin","CREATE TABLE _lattice_canonical_receipt_origin(original_id BLOB PRIMARY KEY NOT NULL,producer BLOB NOT NULL,incarnation BLOB NOT NULL,digest BLOB NOT NULL,operation BLOB NOT NULL,charge INTEGER NOT NULL) WITHOUT ROWID"},
    {"_lattice_canonical_receipt_coverage","CREATE TABLE _lattice_canonical_receipt_coverage(original_id BLOB NOT NULL,namespace_id BLOB NOT NULL,revision INTEGER NOT NULL,charge INTEGER NOT NULL,PRIMARY KEY(original_id,namespace_id)) WITHOUT ROWID"}
};}
void initialize_canonical_coverage(database& db,const canonical_coverage_profile& p){
    p.validate();for(const auto& [name,ddl]:canonical_coverage_schema())db.execute(ddl);
    db.execute("INSERT INTO main._lattice_canonical_receipt_profile VALUES(1,3,?,?,1,0,0,0,0,0,?,?,?,?)",
        {bytes(p.cohort_id),p.revision,p.maximum_origins,p.maximum_origin_bytes,p.maximum_cells,p.maximum_cell_bytes});changed(db);
    for(const auto& ns:p.namespaces){db.execute("INSERT INTO main._lattice_canonical_receipt_member VALUES(?)",{bytes(ns)});changed(db);}
}
canonical_coverage_state read_canonical_coverage(const canonical_coverage_query& q,const canonical_coverage_profile& p){
    p.validate();const auto rows=q("SELECT CASE WHEN typeof(id)='integer' THEN id END AS id,CASE WHEN typeof(version)='integer' THEN version END AS version,CASE WHEN typeof(cohort)='blob' AND length(cohort)=36 THEN cohort END AS cohort,CASE WHEN typeof(revision)='integer' THEN revision END AS revision,CASE WHEN typeof(codec)='integer' THEN codec END AS codec,CASE WHEN typeof(mutation)='integer' THEN mutation END AS mutation,CASE WHEN typeof(origins)='integer' THEN origins END AS origins,CASE WHEN typeof(origin_bytes)='integer' THEN origin_bytes END AS origin_bytes,CASE WHEN typeof(cells)='integer' THEN cells END AS cells,CASE WHEN typeof(cell_bytes)='integer' THEN cell_bytes END AS cell_bytes,CASE WHEN typeof(max_origins)='integer' THEN max_origins END AS max_origins,CASE WHEN typeof(max_origin_bytes)='integer' THEN max_origin_bytes END AS max_origin_bytes,CASE WHEN typeof(max_cells)='integer' THEN max_cells END AS max_cells,CASE WHEN typeof(max_cell_bytes)='integer' THEN max_cell_bytes END AS max_cell_bytes FROM main._lattice_canonical_receipt_profile LIMIT 2",{});
    if(rows.size()!=1)refuse("receipt coverage singleton missing");const auto& r=rows[0];
    if(integer(r,"id")!=1||integer(r,"version")!=3||binary(r,"cohort",36)!=p.cohort_id||integer(r,"revision")!=p.revision||integer(r,"codec")!=1||
       integer(r,"max_origins")!=p.maximum_origins||integer(r,"max_origin_bytes")!=p.maximum_origin_bytes||integer(r,"max_cells")!=p.maximum_cells||integer(r,"max_cell_bytes")!=p.maximum_cell_bytes)refuse("receipt coverage persistent policy differs");
    canonical_coverage_state s{integer(r,"mutation"),integer(r,"origins"),integer(r,"origin_bytes"),integer(r,"cells"),integer(r,"cell_bytes")};
    if(s.mutation<0||s.origins<0||s.origins>p.maximum_origins||s.origin_bytes<0||s.origin_bytes>p.maximum_origin_bytes||s.cells<0||s.cells>p.maximum_cells||s.cell_bytes<0||s.cell_bytes>p.maximum_cell_bytes||s.mutation!=s.cells)refuse("receipt coverage counters outside durable bounds");
    const auto members=q("SELECT CASE WHEN typeof(namespace_id)='blob' AND length(namespace_id) BETWEEN 1 AND 256 THEN namespace_id END AS namespace_id FROM main._lattice_canonical_receipt_member LIMIT 65",{});
    if(members.size()!=p.namespaces.size())refuse("receipt cohort member inventory differs");std::set<std::string> seen;
    for(const auto& m:members){auto ns=binary(m,"namespace_id",256);if(!admitted(p,ns)||!seen.insert(ns).second)refuse("receipt cohort member differs");}
    return s;
}
void audit_canonical_coverage(const canonical_coverage_query& q,const canonical_coverage_profile& p){
    const auto state=read_canonical_coverage(q,p);
    const auto schema=q("SELECT type,name,CASE WHEN length(CAST(sql AS BLOB))<=4096 THEN sql END AS sql FROM main.sqlite_schema WHERE name IN ('_lattice_canonical_receipt_profile','_lattice_canonical_receipt_member','_lattice_canonical_receipt_origin','_lattice_canonical_receipt_coverage') LIMIT 5",{});
    const auto expected=canonical_coverage_schema();if(schema.size()!=expected.size())refuse("receipt coverage schema incomplete");
    for(const auto& r:schema){const auto name=std::get_if<std::string>(&r.at("name")),sql=std::get_if<std::string>(&r.at("sql")),type=std::get_if<std::string>(&r.at("type"));if(!name||!sql||!type||*type!="table"||!expected.count(*name)||expected.at(*name)!=*sql)refuse("receipt coverage schema differs");}
    // Bound even a corrupt oversized file before the relationship audits.
    // This admits at most the actual policy ceiling plus one sentinel row;
    // after exact counts match, every full audit below has that finite input.
    for(const auto& x:std::vector<std::tuple<std::string,int64_t,int64_t,int64_t>>{
            {"origin",state.origins,state.origin_bytes,p.maximum_origins},
            {"coverage",state.cells,state.cell_bytes,p.maximum_cells}}){
        const auto r=q("SELECT count(*) AS n,coalesce(sum(charge),0) AS bytes FROM (SELECT charge FROM main._lattice_canonical_receipt_"+
            std::get<0>(x)+" LIMIT "+std::to_string(std::get<3>(x)+1)+")",{});
        if(r.size()!=1||integer(r[0],"n")!=std::get<1>(x)||integer(r[0],"bytes")!=std::get<2>(x))refuse("receipt coverage counters differ from actual storage");
    }
    if(!q("SELECT 1 FROM main._lattice_canonical_receipt_origin o LEFT JOIN main._lattice_canonical_receipt r USING(original_id) WHERE typeof(o.original_id)!='blob' OR length(o.original_id)!=36 OR typeof(producer)!='blob' OR length(producer) NOT BETWEEN 1 AND 256 OR typeof(incarnation)!='blob' OR length(incarnation)!=36 OR typeof(digest)!='blob' OR length(digest)!=64 OR typeof(operation)!='blob' OR operation NOT IN(CAST('INSERT' AS BLOB),CAST('UPDATE' AS BLOB),CAST('DELETE' AS BLOB)) OR typeof(o.charge)!='integer' OR o.charge!=160+length(producer)+length(incarnation) OR r.original_id IS NULL OR r.outcome NOT IN(1,2) OR NOT EXISTS(SELECT 1 FROM main._lattice_canonical_receipt_coverage c WHERE c.original_id=o.original_id AND c.namespace_id=r.namespace_id) LIMIT 1",{}).empty())refuse("receipt origin has invalid identity or first coverage");
    if(!q("SELECT 1 FROM main._lattice_canonical_receipt_coverage c LEFT JOIN main._lattice_canonical_receipt_origin o USING(original_id) LEFT JOIN main._lattice_canonical_receipt_member m USING(namespace_id) WHERE typeof(c.original_id)!='blob' OR length(c.original_id)!=36 OR typeof(c.namespace_id)!='blob' OR length(c.namespace_id) NOT BETWEEN 1 AND 256 OR typeof(c.revision)!='integer' OR c.revision<=0 OR c.revision>? OR typeof(c.charge)!='integer' OR c.charge!=80+length(c.namespace_id) OR o.original_id IS NULL OR m.namespace_id IS NULL LIMIT 1",{state.mutation}).empty())refuse("receipt coverage cell is malformed or unbound");
}
int64_t canonical_origin_charge(const recovery_producer_registration& p){p.validate();return 160+static_cast<int64_t>(p.registration_id.size()+p.incarnation.size());}
int64_t canonical_coverage_charge(const std::string& ns){if(ns.empty()||ns.size()>256)refuse("receipt coverage namespace bound");return 80+static_cast<int64_t>(ns.size());}
namespace {
canonical_coverage_lookup lookup_coverage(const canonical_coverage_query& q,const canonical_coverage_profile& p,
    const std::string& original,const std::string& ns,const recovery_receipt_binding& b,const std::string& digest,
    const std::optional<std::string>& operation,const canonical_coverage_state* snapshot){
    binding(p,b,ns);if(original.size()!=36||digest.size()!=64)refuse("receipt coverage addressed identity bound");
    const auto rows=q("SELECT CASE WHEN typeof(namespace_id)='blob' AND length(namespace_id) BETWEEN 1 AND 256 THEN namespace_id END AS namespace_id,CASE WHEN typeof(outcome)='integer' THEN outcome END AS outcome FROM main._lattice_canonical_receipt WHERE original_id=? LIMIT 2",{bytes(original)});
    if(rows.empty())return canonical_coverage_lookup::no_original;if(rows.size()!=1)refuse("receipt global identity collision");
    const auto origin=q("SELECT CASE WHEN typeof(producer)='blob' AND length(producer) BETWEEN 1 AND 256 THEN producer END AS producer,CASE WHEN typeof(incarnation)='blob' AND length(incarnation)=36 THEN incarnation END AS incarnation,CASE WHEN typeof(digest)='blob' AND length(digest)=64 THEN digest END AS digest,CASE WHEN typeof(operation)='blob' AND length(operation)<=6 THEN operation END AS operation,CASE WHEN typeof(charge)='integer' THEN charge END AS charge FROM main._lattice_canonical_receipt_origin WHERE original_id=? LIMIT 2",{bytes(original)});
    if(origin.empty()){
        if(binary(rows[0],"namespace_id",256)!=ns)refuse("legacy-unbound receipt cannot establish another namespace's provenance");
        return canonical_coverage_lookup::legacy_original_namespace;
    }
    if(origin.size()!=1||integer(rows[0],"outcome")<1||integer(rows[0],"outcome")>2)refuse("receipt origin outcome invalid");const auto& r=origin[0];
    if(binary(r,"producer",256)!=b.producer.registration_id||binary(r,"incarnation",36)!=b.producer.incarnation||binary(r,"digest",64)!=digest||
       (operation&&binary(r,"operation",6)!=*operation)||integer(r,"charge")!=canonical_origin_charge(b.producer))refuse("receipt immutable producer or operation differs");
    const auto cells=q("SELECT CASE WHEN typeof(revision)='integer' THEN revision END AS revision,CASE WHEN typeof(charge)='integer' THEN charge END AS charge FROM main._lattice_canonical_receipt_coverage WHERE original_id=? AND namespace_id=? LIMIT 2",{bytes(original),bytes(ns)});
    if(cells.empty())return canonical_coverage_lookup::missing;
    const auto state=snapshot?*snapshot:read_canonical_coverage(q,p);
    if(cells.size()!=1||integer(cells[0],"revision")<=0||integer(cells[0],"revision")>state.mutation||integer(cells[0],"charge")!=canonical_coverage_charge(ns))refuse("receipt addressed coverage corrupt");
    return canonical_coverage_lookup::covered;
}
} // namespace
canonical_coverage_lookup lookup_canonical_coverage(const canonical_coverage_query& q,const canonical_coverage_profile& p,
    const std::string& original,const std::string& ns,const recovery_receipt_binding& b,const std::string& digest,const std::optional<std::string>& operation){
    return lookup_coverage(q,p,original,ns,b,digest,operation,nullptr);
}
canonical_coverage_snapshot::canonical_coverage_snapshot(const canonical_coverage_query& q,const canonical_coverage_profile& p)
    :profile_(p),state_(read_canonical_coverage(q,profile_)){}
canonical_coverage_lookup canonical_coverage_snapshot::lookup(const canonical_coverage_query& q,const std::string& original,
    const std::string& ns,const recovery_receipt_binding& b,const std::string& digest)const{
    return lookup_coverage(q,profile_,original,ns,b,digest,std::nullopt,&state_);
}
}
