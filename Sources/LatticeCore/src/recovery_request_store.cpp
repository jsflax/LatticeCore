#include "recovery_request_store.hpp"
#include "recovery_writer_access.hpp"
#include "vendor/picosha2/picosha2.h"
#include <array>
#include <limits>

namespace lattice::detail {
namespace {
using bytes=std::vector<uint8_t>;
using row=database::row_t;
constexpr const char* config_ddl="CREATE TABLE _lattice_recovery_request_config(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,max_rows INTEGER NOT NULL,max_frame INTEGER NOT NULL,max_context INTEGER NOT NULL,max_bytes INTEGER NOT NULL) WITHOUT ROWID";
constexpr const char* request_ddl="CREATE TABLE _lattice_recovery_request(channel BLOB PRIMARY KEY,incarnation INTEGER NOT NULL,generation INTEGER NOT NULL,barrier INTEGER NOT NULL,sequence INTEGER NOT NULL,journal_revision INTEGER NOT NULL,route INTEGER NOT NULL,domain BLOB NOT NULL,source_context BLOB NOT NULL,request_frame BLOB NOT NULL,manifest_frame BLOB NOT NULL) WITHOUT ROWID";
[[noreturn]] void refuse(const char* reason){throw db_error(reason);}
bytes blob(const std::string& value){return {value.begin(),value.end()};}
int64_t number(const row& value,const char* key) {
    const auto at=value.find(key);
    if(at==value.end()||!std::holds_alternative<int64_t>(at->second))refuse("recovery request integer metadata differs");
    return std::get<int64_t>(at->second);
}
std::string string(const row& value,const char* key) {
    const auto at=value.find(key);
    if(at==value.end()||!std::holds_alternative<bytes>(at->second))refuse("recovery request bounded blob metadata differs");
    const auto& data=std::get<bytes>(at->second);return {data.begin(),data.end()};
}
size_t charge(const recovery_request_row& value) {
    if(value.journal.channel.empty()||value.journal.channel.size()>4096||value.domain.size()!=64||
       value.source_context.empty()||value.source_context.size()>recovery_request_store::context_bytes||
       value.request_frame.empty()||value.request_frame.size()>recovery_request_store::frame_bytes||
       value.manifest_frame.size()>recovery_request_store::frame_bytes||value.journal.incarnation<=0||
       value.journal.generation<=0||value.barrier<=0||value.sequence<=0||value.journal_revision<=0||value.route<=0)
        refuse("recovery request framing or binding outside immutable caps");
    return 56+value.journal.channel.size()+value.domain.size()+value.source_context.size()+
        value.request_frame.size()+value.manifest_frame.size();
}
std::string fingerprint(const recovery_request_row& value) {
    (void)charge(value);picosha2::hash256_one_by_one hash;
    const auto integer=[&](uint64_t n){std::array<uint8_t,8> b{};for(size_t i=0;i<8;++i)b[7-i]=static_cast<uint8_t>(n>>(i*8));hash.process(b.begin(),b.end());};
    const auto text=[&](const std::string& s){integer(s.size());hash.process(s.begin(),s.end());};
    text(value.journal.channel);integer(value.journal.incarnation);integer(value.journal.generation);
    integer(value.barrier);integer(value.sequence);integer(value.journal_revision);integer(value.route);
    text(value.domain);text(value.source_context);text(value.request_frame);text(value.manifest_frame);
    hash.finish();return picosha2::get_hash_hex_string(hash);
}
void schema(database& db) {
    const auto rows=db.query("SELECT CASE WHEN length(CAST(name AS BLOB))<=128 THEN name END AS name,CASE WHEN length(CAST(sql AS BLOB))<=1024 THEN sql END AS sql FROM main.sqlite_schema WHERE name IN ('_lattice_recovery_request_config','_lattice_recovery_request') ORDER BY name LIMIT 3");
    if(rows.size()!=2)refuse("recovery request fixed schema inventory differs");
    const std::map<std::string,std::string> expected{{"_lattice_recovery_request_config",config_ddl},{"_lattice_recovery_request",request_ddl}};
    for(const auto& r:rows) {
        const auto name=r.find("name"),sql=r.find("sql");
        if(name==r.end()||sql==r.end()||!std::holds_alternative<std::string>(name->second)||!std::holds_alternative<std::string>(sql->second))refuse("recovery request schema text differs");
        const auto found=expected.find(std::get<std::string>(name->second));
        if(found==expected.end()||found->second!=std::get<std::string>(sql->second))refuse("recovery request schema changed");
    }
    if(!db.query("SELECT 1 FROM main.sqlite_schema WHERE type='trigger' AND tbl_name COLLATE NOCASE IN ('_lattice_recovery_request_config','_lattice_recovery_request') UNION ALL SELECT 1 FROM temp.sqlite_schema WHERE type='trigger' AND tbl_name COLLATE NOCASE IN ('_lattice_recovery_request_config','_lattice_recovery_request') LIMIT 1").empty())
        refuse("recovery request metadata triggers are not admitted");
    const auto rows_config=db.query("SELECT CASE WHEN typeof(id)='integer' THEN id END AS id,CASE WHEN typeof(version)='integer' THEN version END AS version,CASE WHEN typeof(max_rows)='integer' THEN max_rows END AS max_rows,CASE WHEN typeof(max_frame)='integer' THEN max_frame END AS max_frame,CASE WHEN typeof(max_context)='integer' THEN max_context END AS max_context,CASE WHEN typeof(max_bytes)='integer' THEN max_bytes END AS max_bytes FROM main._lattice_recovery_request_config LIMIT 2");
    if(rows_config.size()!=1)refuse("recovery request configuration missing or duplicate");const auto& r=rows_config[0];
    if(number(r,"id")!=1||number(r,"version")!=recovery_request_store::version||number(r,"max_rows")!=recovery_request_store::maximum_rows||
       number(r,"max_frame")!=recovery_request_store::frame_bytes||number(r,"max_context")!=recovery_request_store::context_bytes||number(r,"max_bytes")!=recovery_request_store::stored_bytes)
        refuse("recovery request immutable profile differs");
}
}
recovery_request_store::recovery_request_store(std::shared_ptr<lattice_db> owner):owner_(std::move(owner)) {
    if(!owner_)refuse("recovery request requires actual retained owner");
}
database& recovery_request_store::writer()const {
    auto* value=recovery_writer_access::active_writer(*owner_);if(!value)refuse("recovery request requires owned writer transaction");return *value;
}
void recovery_request_store::initialize() {
    auto& db=writer();const auto found=db.query("SELECT name FROM main.sqlite_schema WHERE name IN ('_lattice_recovery_request_config','_lattice_recovery_request') LIMIT 3");
    if(found.empty()) {
        db.execute(config_ddl);db.execute(request_ddl);
        db.execute("INSERT INTO _lattice_recovery_request_config VALUES(1,1,16,4194304,65536,134217728)");
    } else if(found.size()!=2)refuse("partial recovery request schema cannot be adopted");
    audit();
}
std::vector<std::string> recovery_request_store::channels()const {
    auto& db=writer();schema(db);
    const auto rows=db.query("SELECT CASE WHEN typeof(channel)='blob' AND length(channel) BETWEEN 1 AND 4096 THEN channel END AS channel FROM main._lattice_recovery_request ORDER BY channel LIMIT 17");
    if(rows.size()>maximum_rows)refuse("recovery request row count exceeds profile");
    std::vector<std::string> result;result.reserve(rows.size());for(const auto& r:rows)result.push_back(string(r,"channel"));return result;
}
std::optional<recovery_request_row> recovery_request_store::read(const std::string& channel)const {
    if(channel.empty()||channel.size()>4096)refuse("recovery request channel bound");auto& db=writer();schema(db);
    const auto found=db.query("SELECT CASE WHEN typeof(channel)='blob' AND length(channel)<=4096 THEN channel END AS channel,CASE WHEN typeof(incarnation)='integer' THEN incarnation END AS incarnation,CASE WHEN typeof(generation)='integer' THEN generation END AS generation,CASE WHEN typeof(barrier)='integer' THEN barrier END AS barrier,CASE WHEN typeof(sequence)='integer' THEN sequence END AS sequence,CASE WHEN typeof(journal_revision)='integer' THEN journal_revision END AS journal_revision,CASE WHEN typeof(route)='integer' THEN route END AS route,"
        "CASE WHEN typeof(domain)='blob' AND length(domain)=64 THEN domain END AS domain,"
        "CASE WHEN typeof(source_context)='blob' AND length(source_context) BETWEEN 1 AND 65536 THEN source_context END AS source_context,"
        "CASE WHEN typeof(request_frame)='blob' AND length(request_frame) BETWEEN 1 AND 4194304 THEN request_frame END AS request_frame,"
        "CASE WHEN typeof(manifest_frame)='blob' AND length(manifest_frame)<=4194304 THEN manifest_frame END AS manifest_frame "
        "FROM main._lattice_recovery_request WHERE channel=? LIMIT 2",{blob(channel)});
    if(found.empty())return std::nullopt;if(found.size()!=1)refuse("recovery request duplicate channel");const auto& r=found[0];
    recovery_request_row value{{string(r,"channel"),number(r,"incarnation"),number(r,"generation")},number(r,"barrier"),number(r,"sequence"),number(r,"journal_revision"),number(r,"route"),string(r,"domain"),string(r,"source_context"),string(r,"request_frame"),string(r,"manifest_frame")};
    if(value.journal.channel!=channel)refuse("recovery request channel lookup differs");(void)charge(value);return value;
}
std::map<std::string,std::string> recovery_request_store::fingerprints()const {
    // One bounded row at a time; do not materialize 16 complete Q/M pairs.
    std::map<std::string,std::string> result;size_t total=0;
    for(const auto& channel:channels()) {const auto value=read(channel);if(!value)refuse("recovery request disappeared within owned view");
        const auto size=charge(*value);if(size>stored_bytes-total)refuse("recovery request aggregate bytes exceed profile");total+=size;result.emplace(channel,fingerprint(*value));}
    return result;
}
void recovery_request_store::audit()const {(void)fingerprints();}
void recovery_request_store::insert(const recovery_request_row& value) {
    (void)charge(value);auto expected=fingerprints();
    if(expected.size()>=maximum_rows||!expected.emplace(value.journal.channel,fingerprint(value)).second)refuse("recovery request already exists or row capacity exhausted");
    writer().execute("INSERT INTO _lattice_recovery_request VALUES(?,?,?,?,?,?,?,?,?,?,?)",{blob(value.journal.channel),value.journal.incarnation,value.journal.generation,value.barrier,value.sequence,value.journal_revision,value.route,blob(value.domain),blob(value.source_context),blob(value.request_frame),blob(value.manifest_frame)});
    if(fingerprints()!=expected)refuse("recovery request insertion changed exact postimage");
}
void recovery_request_store::add_manifest(const recovery_request_row& before,const std::string& manifest) {
    if(manifest.empty()||manifest.size()>frame_bytes)refuse("recovery manifest byte bound");auto expected=fingerprints();
    if(read(before.journal.channel)!=std::optional<recovery_request_row>{before}||!before.manifest_frame.empty())refuse("recovery request changed before manifest retention");
    auto after=before;after.manifest_frame=manifest;expected.at(before.journal.channel)=fingerprint(after);
    writer().execute("UPDATE _lattice_recovery_request SET manifest_frame=? WHERE channel=?",{blob(manifest),blob(before.journal.channel)});
    if(fingerprints()!=expected)refuse("recovery manifest changed exact framing postimage");
}
void recovery_request_store::rebind(const recovery_request_row& before,int64_t route) {
    if(route<=0)refuse("recovery request replacement route bound");auto expected=fingerprints();
    if(read(before.journal.channel)!=std::optional<recovery_request_row>{before})refuse("recovery request changed before route rebind");
    auto after=before;after.route=route;expected.at(before.journal.channel)=fingerprint(after);
    writer().execute("UPDATE _lattice_recovery_request SET route=? WHERE channel=?",{route,blob(before.journal.channel)});
    if(fingerprints()!=expected)refuse("recovery route rebind changed frozen framing");
}
void recovery_request_store::erase(const recovery_request_row& before) {
    auto expected=fingerprints();if(read(before.journal.channel)!=std::optional<recovery_request_row>{before})refuse("recovery request changed before checked retirement");
    expected.erase(before.journal.channel);writer().execute("DELETE FROM _lattice_recovery_request WHERE channel=?",{blob(before.journal.channel)});
    if(fingerprints()!=expected)refuse("recovery request retirement changed another request");
}
} // namespace lattice::detail
