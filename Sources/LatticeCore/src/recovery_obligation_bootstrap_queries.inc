// Included once by the closed bootstrap statement implementation. These are
// library-generated recipes; a caller SQL string never selects a purpose.
namespace {
using bootstrap_kind=recovery_obligation_bootstrap_statement::kind;
using bootstrap_caps=recovery_obligation_producer_discovery_limits;
std::uint64_t bootstrap_add(std::uint64_t a,std::uint64_t b) {
    if(b>UINT64_MAX-a)throw db_error("bootstrap structured byte overflow");return a+b;
}
std::uint64_t bootstrap_mul(std::uint64_t a,std::uint64_t b) {
    if(a&&b>UINT64_MAX/a)throw db_error("bootstrap structured byte product overflow");return a*b;
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
constexpr const char* continuous_query="SELECT CASE WHEN typeof(id)='integer' THEN id END AS id,CASE WHEN typeof(version)='integer' THEN version END AS version,CASE WHEN typeof(manifest)='blob' AND length(manifest)<=1048576 THEN manifest END AS manifest,CASE WHEN typeof(digest)='blob' AND length(digest)=64 THEN digest END AS digest,CASE WHEN typeof(main_device)='integer' THEN main_device END AS main_device,CASE WHEN typeof(main_inode)='integer' THEN main_inode END AS main_inode,CASE WHEN typeof(parent_device)='integer' THEN parent_device END AS parent_device,CASE WHEN typeof(parent_inode)='integer' THEN parent_inode END AS parent_inode,CASE WHEN typeof(container_device)='integer' THEN container_device END AS container_device,CASE WHEN typeof(container_inode)='integer' THEN container_inode END AS container_inode,CASE WHEN typeof(incarnation)='integer' THEN incarnation END AS incarnation,CASE WHEN typeof(barrier)='integer' THEN barrier END AS barrier,CASE WHEN typeof(attempt)='integer' THEN attempt END AS attempt,CASE WHEN typeof(phase)='integer' THEN phase END AS phase FROM main._lattice_producer_continuity LIMIT 2";
struct bootstrap_recipe {
    std::string sql;
    std::uint64_t columns=1,rows=2,result_bytes=0;
};
bootstrap_recipe bootstrap_generate(bootstrap_kind kind,const bootstrap_caps& caps) {
    const auto& l=caps.obligations;const auto& p=caps.producers;
    bootstrap_recipe out;
    switch(kind) {
    case bootstrap_kind::family:
        out.sql="SELECT name FROM main.sqlite_schema WHERE name IN ('_lattice_obligation_producer_store','_lattice_obligation_producer_profile','_lattice_obligation_producer_stamp') LIMIT 4";out.rows=4;break;
    case bootstrap_kind::definition:
        out.sql="SELECT CASE WHEN typeof(sql)='text' AND length(CAST(sql AS BLOB))=? THEN CAST(sql AS BLOB) END AS definition FROM main.sqlite_schema WHERE name=? LIMIT 2";break;
    case bootstrap_kind::definition_else_null:
        out.sql="SELECT CASE WHEN typeof(sql)='text' AND length(CAST(sql AS BLOB))=? THEN CAST(sql AS BLOB) ELSE NULL END AS definition FROM main.sqlite_schema WHERE name=? LIMIT 2";break;
    case bootstrap_kind::obligation_shape:
        out.sql="SELECT ncol,wr FROM pragma_table_list WHERE schema='main' AND type='table' AND name=? LIMIT 2";out.columns=2;break;
    case bootstrap_kind::obligation_policy:
        out.sql="SELECT "+ints({"id","version","max_scopes","max_records","max_field","max_bytes"})+" FROM main._lattice_obligation_store LIMIT 2";out.columns=6;break;
    case bootstrap_kind::installation_policy:
        out.sql="SELECT "+ints({"id","version","max_channels","max_field_bytes","max_bytes","channels","bytes"})+" FROM main._lattice_install_store LIMIT 2";out.columns=7;break;
    case bootstrap_kind::producer_policy:
        out.sql="SELECT "+ints({"id","version","max_profiles","max_stamps","max_field","max_manifest","max_bytes"})+" FROM main._lattice_obligation_producer_store LIMIT 2";out.columns=7;break;
    case bootstrap_kind::obligation_config:
        out.sql="SELECT "+ints({"id","version","max_scopes","max_records","max_field","max_bytes","scopes","records","bytes","incarnation","record_sequence","export_sequence"})+" FROM main._lattice_obligation_store LIMIT 2";out.columns=12;break;
    case bootstrap_kind::producer_config:
        out.sql="SELECT "+ints({"id","version","max_profiles","max_stamps","max_field","max_manifest","max_bytes","profiles","stamps","bytes"})+" FROM main._lattice_obligation_producer_store LIMIT 2";out.columns=10;break;
    case bootstrap_kind::scope:
        out.sql="SELECT "+ints({"incarnation","generation","revision","last_attempt","freeze_revision","freeze_record","freeze_export","mode","installed_sequence","installed_revision","installed_head","bytes"})+blobs({"channel","authority","source","epoch","scope","schema_digest","profile_digest","receipt_namespace","installed_manifest"},l)+" FROM main._lattice_obligation_scope WHERE channel=? LIMIT 2";out.columns=21;break;
    case bootstrap_kind::entry:
        out.sql="SELECT "+ints({"audit_id","origin","record_sequence","first_export","stage","ack_position","ack_outcome","settled_sequence","bytes"})+",first_export IS NULL AS first_export_null,ack_position IS NULL AS ack_position_null,ack_outcome IS NULL AS ack_outcome_null"+blobs({"original","actual_original","table_name","target","actual_target"},l)+" FROM main._lattice_obligation_entry WHERE channel=? AND original=? LIMIT 2";out.columns=17;break;
    case bootstrap_kind::actual_audit: {
        const std::string total="length(CAST(globalId AS BLOB))+length(CAST(tableName AS BLOB))+length(CAST(globalRowId AS BLOB))";
        const auto project=[&](const char* n){return "CASE WHEN typeof("+std::string(n)+")='text' AND length(CAST("+n+" AS BLOB)) BETWEEN 1 AND "+std::to_string(l.field_bytes)+" AND ("+total+")<="+std::to_string(l.encoded_bytes)+" THEN CAST("+n+" AS BLOB) ELSE NULL END AS "+n;};
        out.sql="SELECT "+project("globalId")+","+project("tableName")+","+project("globalRowId")+","+ints({"isFromRemote","synthesized"})+" FROM main.AuditLog WHERE id=? LIMIT 2";out.columns=5;break;
    }
    case bootstrap_kind::entry_page:
        out.sql="SELECT "+ints({"audit_id"})+blobs({"original"},l)+" FROM main._lattice_obligation_entry WHERE channel=? AND audit_id>? ORDER BY audit_id LIMIT 1";out.columns=2;out.rows=1;break;
    case bootstrap_kind::scope_page_first:case bootstrap_kind::scope_page_after:
        out.sql="SELECT "+blobs({"channel"},l).substr(1)+" FROM main._lattice_obligation_scope "+(kind==bootstrap_kind::scope_page_first?"":"WHERE channel>? ")+"ORDER BY channel LIMIT 1";out.rows=1;break;
    case bootstrap_kind::record_count:
        out.sql="SELECT COUNT(*) AS n FROM (SELECT 1 FROM main._lattice_obligation_entry LIMIT ?)";out.rows=1;break;
    case bootstrap_kind::scope_count:
        out.sql="SELECT COUNT(*) AS n FROM (SELECT 1 FROM main._lattice_obligation_scope LIMIT ?)";out.rows=1;break;
    case bootstrap_kind::distinct_scopes:
        out.sql="SELECT COUNT(DISTINCT incarnation) AS n FROM (SELECT incarnation FROM main._lattice_obligation_scope LIMIT ?)";out.rows=1;break;
    case bootstrap_kind::distinct_entries:
        out.sql="SELECT COUNT(DISTINCT record_sequence) AS n FROM (SELECT record_sequence FROM main._lattice_obligation_entry LIMIT ?)";out.rows=1;break;
    case bootstrap_kind::alias_channels:
        out.sql="SELECT "+blobs({"channel"},l).substr(1)+" FROM main._lattice_obligation_scope WHERE authority=? AND scope=? ORDER BY channel LIMIT 17";out.rows=17;break;
    case bootstrap_kind::producer_profile:
        out.sql="SELECT "+ints({"incarnation","program_revision","bytes"})+",CASE WHEN typeof(program_digest)='blob' AND length(program_digest) BETWEEN 1 AND ? AND length(program_digest)+length(manifest)+length(channel)<=? THEN program_digest END AS program_digest,CASE WHEN typeof(manifest)='blob' AND length(manifest) BETWEEN 1 AND ? AND length(program_digest)+length(manifest)+length(channel)<=? THEN manifest END AS manifest FROM main._lattice_obligation_producer_profile WHERE channel=? LIMIT 2";out.columns=5;break;
    case bootstrap_kind::producer_stamp:
        out.sql="SELECT "+ints({"incarnation","program_revision","audit_id","record_sequence","generation","scope_revision","base_scopes","base_records","base_bytes","base_incarnation","base_export","producer_profiles","producer_stamps","producer_bytes","bytes"})+" FROM main._lattice_obligation_producer_stamp WHERE channel=? AND original=? LIMIT 2";out.columns=15;break;
    case bootstrap_kind::profile_page_first:case bootstrap_kind::profile_page_after:
        out.sql="SELECT CASE WHEN typeof(channel)='blob' AND length(channel) BETWEEN 1 AND ? THEN channel END AS channel FROM main._lattice_obligation_producer_profile "+std::string(kind==bootstrap_kind::profile_page_first?"":"WHERE channel>? ")+"ORDER BY channel LIMIT 1";out.rows=1;break;
    case bootstrap_kind::stamp_page_first:case bootstrap_kind::stamp_page_after:
        out.sql="SELECT CASE WHEN typeof(channel)='blob' AND length(channel) BETWEEN 1 AND ? THEN channel END AS channel,CASE WHEN typeof(original)='blob' AND length(original)=36 THEN original END AS original FROM main._lattice_obligation_producer_stamp "+std::string(kind==bootstrap_kind::stamp_page_first?"":"WHERE (channel,original)>(?,?) ")+"ORDER BY channel,original LIMIT 1";out.columns=2;out.rows=1;break;
    case bootstrap_kind::trigger_budget:
        out.sql="SELECT COUNT(*) AS n,COALESCE(SUM(length(CAST(name AS BLOB))+length(CAST(sql AS BLOB))),0) AS bytes FROM (SELECT name,sql FROM main.sqlite_master WHERE type='trigger' AND tbl_name=? LIMIT 17)";out.columns=2;out.rows=1;break;
    case bootstrap_kind::trigger_programs:
        out.sql="SELECT CASE WHEN length(CAST(name AS BLOB))<=128 THEN name END AS name,CASE WHEN length(CAST(sql AS BLOB))<=262144 THEN sql END AS sql FROM main.sqlite_master WHERE type='trigger' AND tbl_name=? LIMIT 17";out.columns=2;out.rows=17;break;
    case bootstrap_kind::producer_present:
        out.sql="SELECT 1 FROM main._lattice_obligation_producer_profile LIMIT 1";out.rows=1;break;
    case bootstrap_kind::model_definition:
        out.sql="SELECT CASE WHEN length(CAST(sql AS BLOB))<=262144 THEN sql END AS sql FROM main.sqlite_master WHERE type='table' AND name=?";break;
    case bootstrap_kind::model_columns:
        out.sql="SELECT CASE WHEN length(CAST(name AS BLOB))<=64 THEN name END AS name,CASE WHEN length(CAST(type AS BLOB))<=16 THEN type END AS type,CASE WHEN typeof(hidden)='integer' THEN hidden END AS hidden FROM pragma_table_xinfo(?) ORDER BY cid LIMIT 35";out.columns=3;out.rows=35;break;
    case bootstrap_kind::index_budget:
        out.sql="SELECT COUNT(*) AS n,COALESCE(SUM(length(CAST(name AS BLOB))+COALESCE(length(CAST(sql AS BLOB)),0)),0) AS bytes FROM (SELECT name,sql FROM main.sqlite_master WHERE type='index' AND tbl_name=? LIMIT 33)";out.columns=2;out.rows=1;break;
    case bootstrap_kind::indexes:
        out.sql="SELECT CASE WHEN length(CAST(name AS BLOB))<=128 THEN name END AS name,CASE WHEN sql IS NULL THEN '' WHEN length(CAST(sql AS BLOB))<=262144 THEN sql END AS sql FROM main.sqlite_master WHERE type='index' AND tbl_name=? ORDER BY name LIMIT 33";out.columns=2;out.rows=33;break;
    case bootstrap_kind::canonical_coverage:case bootstrap_kind::continuous_presence:
        out.sql="SELECT name FROM sqlite_master WHERE type='table' AND name=?";break;
    case bootstrap_kind::databases:
        out.sql="SELECT name FROM pragma_database_list LIMIT 3";out.rows=3;break;
    case bootstrap_kind::temporary_presence:
        out.sql="SELECT 1 FROM temp.sqlite_master LIMIT 1";out.rows=1;break;
    case bootstrap_kind::metadata_triggers:
        out.sql="SELECT 1 FROM main.sqlite_master WHERE type='trigger' AND substr(tbl_name,1,20)='_lattice_obligation_' LIMIT 1";out.rows=1;break;
    case bootstrap_kind::continuous_inventory:
        out.sql="SELECT type,CASE WHEN length(CAST(name AS BLOB))<=128 THEN name END AS name,CASE WHEN length(CAST(sql AS BLOB))<=4096 THEN sql END AS sql FROM main.sqlite_schema WHERE name GLOB '_lattice_producer_continuity*' LIMIT 5";out.columns=3;out.rows=5;break;
    case bootstrap_kind::continuous_row:
        out.sql=continuous_query;out.columns=14;break;
    case bootstrap_kind::enrollment_install_channel_presence:
        out.sql="SELECT 1 FROM main._lattice_install_channel LIMIT 1";out.rows=1;break;
    case bootstrap_kind::enrollment_audit_presence:
        out.sql="SELECT 1 FROM main.AuditLog LIMIT 1";out.rows=1;break;
    case bootstrap_kind::enrollment_ledger_presence:
        out.sql="SELECT 1 FROM main.sqlite_schema WHERE name GLOB '_lattice_obligation_*' OR name GLOB '_lattice_install_*' LIMIT 1";out.rows=1;break;
    case bootstrap_kind::enrollment_body_savepoint:
        out.sql="SAVEPOINT _lattice_recovery_body_settled";out.columns=0;out.rows=0;break;
    case bootstrap_kind::enrollment_body_release:
        out.sql="RELEASE _lattice_recovery_body_settled";out.columns=0;out.rows=0;break;
    case bootstrap_kind::enrollment_install_store_create:
    case bootstrap_kind::enrollment_install_channel_create:
    case bootstrap_kind::enrollment_install_domain_create:
    case bootstrap_kind::enrollment_obligation_store_create:
    case bootstrap_kind::enrollment_obligation_scope_create:
    case bootstrap_kind::enrollment_obligation_entry_create:
    case bootstrap_kind::enrollment_obligation_audit_create:
    case bootstrap_kind::enrollment_obligation_original_create:
    case bootstrap_kind::enrollment_obligation_domain_create:
    case bootstrap_kind::enrollment_producer_store_create:
    case bootstrap_kind::enrollment_producer_profile_create:
    case bootstrap_kind::enrollment_producer_stamp_create:
    case bootstrap_kind::enrollment_install_seed:
    case bootstrap_kind::enrollment_obligation_seed:
    case bootstrap_kind::enrollment_producer_seed:
        out.columns=0;out.rows=0;break; // SQL comes only from this actual Reader's compiled schema supplier
    case bootstrap_kind::journal_mode:out.sql="PRAGMA journal_mode";out.rows=1;break;
    case bootstrap_kind::synchronous:out.sql="PRAGMA synchronous";out.rows=1;break;
    }
    // Result keys, fixed cells and vectors are structured charge, in addition
    // to bounded selected values. Actual Request still debits the SAME aggregate
    // setup budget before owning each value; this is not an allocator/RSS claim.
    const auto value_bound=std::max<std::uint64_t>({std::uint64_t(l.encoded_bytes),std::uint64_t(p.encoded_bytes),1048576});
    out.result_bytes=bootstrap_mul(out.rows,bootstrap_add(value_bound,bootstrap_mul(out.columns,256)));
    return out;
}
}
