#include "TestHelpers.hpp"
#include "../../Sources/LatticeSwiftCppBridge/include/lattice.hpp"
#include "../../Sources/LatticeCore/src/ordinary_export.hpp"
#include <array>
#include <stdexcept>

namespace {
template<class F> std::exception_ptr diagnostic_exception(F&& body) {
    try { body(); }
    catch (...) { return std::current_exception(); }
    throw std::logic_error("diagnostic fixture did not throw its real exception");
}
std::string diagnostic_text(const std::exception& error) {
    std::string text;
    lattice::assign_ordinary_bridge_exception(text, error);
    return text;
}
void contains(const std::string& text, const std::string& value) {
    EXPECT_NE(text.find(value), std::string::npos) << text;
}
}

TEST(OrdinaryErrorDiagnosticBridge, UnrelatedShortWhatAndSealedSuccessStayUnchanged) {
    const lattice::db_error error("unchanged unrelated native error");
    EXPECT_EQ(diagnostic_text(error), error.what());
    lattice::last_bridge_error() = "prior diagnostic";
    EXPECT_FALSE(lattice::record_ordinary_bridge_exception(error));
    EXPECT_EQ(lattice::last_bridge_error(), "prior diagnostic");
    EXPECT_FALSE(lattice::sealed([&]() -> bool { throw error; }));
    EXPECT_EQ(lattice::last_bridge_error(), error.what());
    EXPECT_TRUE(lattice::sealed([] { return true; }));
    EXPECT_TRUE(lattice::last_bridge_error().empty());
}

TEST(OrdinaryErrorDiagnosticBridge, EmptyUnrelatedWhatStaysEmptyAndNestedCauseStaysExplicit) {
    using namespace lattice::detail;
    const std::runtime_error empty("");
    EXPECT_TRUE(diagnostic_text(empty).empty());
    lattice::last_bridge_error() = "prior diagnostic";
    EXPECT_FALSE(lattice::sealed([&]() -> bool { throw empty; }));
    EXPECT_TRUE(lattice::last_bridge_error().empty());
    recovery_install_result value; // Real error-value only; no transaction issued.
    value.primary_error = diagnostic_exception([&] { throw empty; });
    const ordinary_order_write_error owned(value);
    const auto text = diagnostic_text(owned);
    contains(text, "phase=refused");
    contains(text, "primary:[empty-exception-message]");
    EXPECT_EQ(owned.settlement.primary_error, value.primary_error);
    EXPECT_EQ(owned.settlement.state, recovery_install_state::refused);
    EXPECT_FALSE(owned.settlement.unexpected_commit_observed);
}

#ifndef __EMSCRIPTEN__
TEST(OrdinaryErrorDiagnosticBridge, NestedCheckedErrorsKeepActualOwnedRollbackAndRetirement) {
    using namespace lattice::detail;
    lattice::configuration config(":memory:"); config.audit_retention_seconds = 0;
    auto owner = std::make_shared<lattice::lattice_db>(config);
    const auto retirement = database_retirement_access::retain(owner->db());
    ASSERT_TRUE(retirement);
    const auto settled = recovery_writer_access::install(owner, [](lattice::database&) {
        throw lattice::db_error("actual first owned-body failure");
    });
    ASSERT_EQ(settled.state, recovery_install_state::rolled_back);
    ASSERT_TRUE(settled.primary_error);
    ASSERT_FALSE(settled.cleanup_error);
    const auto primary = settled.primary_error;
    const auto closed = owner->close_checked();
    ASSERT_FALSE(closed.error); ASSERT_FALSE(closed.cleanup_error);
    owner.reset();
    ASSERT_EQ(retirement->state(), database_retirement_state::phase::closed);
    ASSERT_EQ(retirement->checked_close_result(), SQLITE_OK);
    const auto owned = diagnostic_exception([&] { throw ordinary_order_write_error(settled); });
    const auto inner = diagnostic_exception([&] { throw ordinary_checked_open_error(owned, {}, {}, retirement); });
    const auto outer = diagnostic_exception([&] { throw ordinary_checked_open_error(inner, {}, {}, retirement); });
    EXPECT_FALSE(lattice::sealed([&]() -> bool { std::rethrow_exception(outer); }));
    const auto text = lattice::last_bridge_error();
    contains(text, "ordinary-checked-open-error{retirement=retained");
    contains(text, "retirement_phase_observed=closed,checked_close=0");
    contains(text, "ordinary-owned-error{phase=rolled_back,deferred=none,unexpected_commit=false");
    contains(text, "primary.primary.primary:actual first owned-body failure");
    EXPECT_LT(text.find("phase=rolled_back"), text.find("actual first owned-body failure"));
    EXPECT_EQ(settled.primary_error, primary);
    EXPECT_FALSE(settled.cleanup_error); EXPECT_FALSE(settled.postcommit_error);
    EXPECT_FALSE(settled.notification_error); EXPECT_FALSE(settled.unexpected_commit_observed);
    EXPECT_EQ(retirement->state(), database_retirement_state::phase::closed);
    EXPECT_LE(text.size(), 2048u);
}
#endif

TEST(OrdinaryErrorDiagnosticBridge, ActualRetainedErrorValuesKeepEveryStageAndNonStdUnknown) {
    using namespace lattice::detail;
    // Error-value formatting only: default refused state issues no transaction.
    recovery_install_result value;
    value.primary_error = diagnostic_exception([] { throw lattice::db_error("first-primary-marker"); });
    value.cleanup_error = diagnostic_exception([] { throw 17; });
    value.postcommit_error = diagnostic_exception([] { throw std::logic_error("postcommit-marker"); });
    value.notification_error = diagnostic_exception([] { throw std::runtime_error("notification-marker"); });
    const ordinary_order_write_error error(value);
    const auto text = diagnostic_text(error);
    contains(text, "phase=refused,deferred=none,unexpected_commit=false");
    contains(text, "primary=present,cleanup=present,postcommit=present,notification=present");
    contains(text, "primary:first-primary-marker");
    contains(text, "cleanup:[non-std-exception;message-unknown]");
    contains(text, "postcommit:postcommit-marker");
    contains(text, "notification:notification-marker");
    EXPECT_EQ(error.settlement.primary_error, value.primary_error);
    EXPECT_EQ(error.settlement.cleanup_error, value.cleanup_error);
    EXPECT_EQ(error.settlement.postcommit_error, value.postcommit_error);
    EXPECT_EQ(error.settlement.notification_error, value.notification_error);
    EXPECT_EQ(error.settlement.state, recovery_install_state::refused);
}

TEST(OrdinaryErrorDiagnosticBridge, OversizedCauseKeepsEarlierFactsAndExplicitAggregateTruncation) {
    using namespace lattice::detail;
    recovery_install_result value;
    value.primary_error = diagnostic_exception([] { throw std::runtime_error(std::string(12000, 'x')); });
    value.cleanup_error = diagnostic_exception([] { throw lattice::db_error("retained cleanup error"); });
    const ordinary_order_write_error error(value);
    const auto text = diagnostic_text(error);
    EXPECT_EQ(text.size(), 2048u);
    EXPECT_TRUE(text.ends_with("[truncated]"));
    contains(text, "primary=present,cleanup=present");
    EXPECT_LT(text.find("cleanup=present"), text.find(std::string(16, 'x')));
    EXPECT_EQ(error.settlement.primary_error, value.primary_error);
    EXPECT_EQ(error.settlement.cleanup_error, value.cleanup_error);
    EXPECT_FALSE(error.settlement.unexpected_commit_observed);
}

TEST(OrdinaryErrorDiagnosticBridge, ExactFitCauseMarksOmittedRetainedSiblingAsTruncated) {
    using namespace lattice::detail;
    recovery_install_result value; // Default refused error DATA, no transaction issued.
    value.primary_error = diagnostic_exception([] { throw std::runtime_error("exact-fit-seed"); });
    value.cleanup_error = diagnostic_exception([] { throw lattice::db_error("later retained cleanup"); });
    const auto sample = diagnostic_text(ordinary_order_write_error(value));
    const auto at = sample.find("exact-fit-seed");
    ASSERT_NE(at, std::string::npos); ASSERT_LT(at, 2048u);
    value.primary_error = diagnostic_exception([&] { throw std::runtime_error(std::string(2048u - at, 'x')); });
    const ordinary_order_write_error error(value);
    const auto text = diagnostic_text(error);
    EXPECT_EQ(text.size(), 2048u);
    EXPECT_TRUE(text.ends_with("[truncated]"));
    contains(text, "primary=present,cleanup=present");
    EXPECT_EQ(error.settlement.primary_error, value.primary_error);
    EXPECT_EQ(error.settlement.cleanup_error, value.cleanup_error);
    EXPECT_EQ(text.find("later retained cleanup"), std::string::npos);
}

TEST(OrdinaryErrorDiagnosticBridge, DeepRetainedExceptionChainStopsWithExplicitUnknown) {
    using namespace lattice::detail;
    auto first = diagnostic_exception([] { throw lattice::db_error("deep actual error"); });
    for (unsigned at = 0; at != 8; ++at)
        first = diagnostic_exception([&] { throw ordinary_checked_open_error(first, {}, {}, {}); });
    std::string text;
    try { std::rethrow_exception(first); }
    catch (const std::exception& error) { lattice::assign_ordinary_bridge_exception(text, error); }
    contains(text, "[depth-limit;further-errors-unknown]");
    EXPECT_LE(text.size(), 2048u);
    // Re-reading the same real retained original produces the same finite text.
    std::string observed_again;
    try { std::rethrow_exception(first); }
    catch (const std::exception& error) { lattice::assign_ordinary_bridge_exception(observed_again, error); }
    EXPECT_EQ(text, observed_again);
}

TEST(OrdinaryErrorDiagnosticBridge, SharedActualErrorTreeStopsAtFiniteNodeBudget) {
    using namespace lattice::detail;
    const auto leaf = diagnostic_exception([] { throw lattice::db_error("shared first leaf"); });
    recovery_install_result child;
    child.primary_error = child.cleanup_error = child.postcommit_error = child.notification_error = leaf;
    const auto retained = diagnostic_exception([&] { throw ordinary_order_write_error(child); });
    recovery_install_result root;
    root.primary_error = root.cleanup_error = root.postcommit_error = root.notification_error = retained;
    const ordinary_order_write_error error(root);
    const auto text = diagnostic_text(error);
    contains(text, "[node-limit;further-errors-unknown]");
    EXPECT_EQ(text.find("[cycle;"), std::string::npos); // Shared siblings are not an ancestor cycle.
    EXPECT_LE(text.size(), 2048u);
    EXPECT_EQ(error.settlement.primary_error, retained);
    EXPECT_EQ(error.settlement.notification_error, retained);
}

TEST(OrdinaryErrorDiagnosticBridge, ActualErrorValueCycleIsUnknownAndNeverRewrittenByFormatter) {
    using namespace lattice::detail;
    // Adversarial error-value graph only; no transaction/ownership is issued.
    try { throw ordinary_order_write_error(recovery_install_result{}); }
    catch (ordinary_order_write_error& error) {
        const auto original = std::current_exception();
        error.settlement.primary_error = original;
        const auto text = diagnostic_text(error);
        const bool preserved = error.settlement.primary_error == original;
        error.settlement.primary_error = {}; // Break this fixture's intentional self-reference before any assertion.
        EXPECT_TRUE(preserved);
        // exception_ptr runtimes may copy on rethrow; either bound must remain explicit.
        EXPECT_TRUE(text.find("[cycle;further-errors-unknown]") != std::string::npos ||
                    text.find("[depth-limit;further-errors-unknown]") != std::string::npos) << text;
        EXPECT_LE(text.size(), 2048u);
    }
}

TEST(OrdinaryErrorDiagnosticBridge, SealedFailuresKeepInvalidAndCapacityIndependentFromText) {
    using namespace lattice::detail;
    const auto primary = diagnostic_exception([] { throw lattice::db_error("checked failure marker"); });
    const auto failed = lattice::sealed([&]() -> lattice::ordinary_history_export_ref {
        throw ordinary_checked_open_error(primary, {}, {}, {});
    });
    EXPECT_FALSE(failed.valid()); EXPECT_FALSE(failed.capacity_exhausted());
    contains(lattice::last_bridge_error(), "primary:checked failure marker");
    const auto unknown = lattice::sealed([]() -> lattice::ordinary_sync_export_ref { throw 23; });
    EXPECT_FALSE(unknown.valid()); EXPECT_FALSE(unknown.capacity_exhausted());
    EXPECT_EQ(lattice::last_bridge_error(), "Unknown C++ bridge exception");
    EXPECT_FALSE(lattice::ordinary_mounted_history_source_ref{}.valid());
    EXPECT_FALSE(lattice::ordinary_mounted_history_source_ref{}.capacity_exhausted());
}

#ifndef __EMSCRIPTEN__
TEST(OrdinaryErrorDiagnosticBridge, GenuineFourCutCapacityRefusalAndReleaseKeepOriginalFlags) {
    TempDB path("ordinary-error-diagnostic-capacity");
    lattice::swift_configuration config(path.str(), std::make_shared<lattice::immediate_scheduler>());
    config.audit_retention_seconds = 0; config.busy_timeout_ms = 100;
    lattice::swift_schema_entry entry; entry.table_name = "DiagnosticErrorRow";
    lattice::property_descriptor property;
    property.name = "value"; property.type = lattice::column_type::text;
    property.kind = lattice::property_kind::primitive;
    entry.properties["value"] = property;
    lattice::SchemaVector schemas{entry};
#if LATTICE_HAS_FRT
    auto owner = std::unique_ptr<lattice::swift_lattice_ref>(lattice::swift_lattice_ref::create(config, schemas));
#else
    auto owner = std::make_unique<lattice::swift_lattice_ref>(lattice::swift_lattice_ref::create(config, schemas));
#endif
    ASSERT_TRUE(owner); ASSERT_TRUE(owner->valid());
    {
        std::array<lattice::ordinary_sync_export_ref, 4> held;
        for (auto& cut : held) {
            cut = owner->capture_ordinary_export({"DiagnosticErrorRow"});
            ASSERT_TRUE(cut.valid()) << cut.failure();
            EXPECT_FALSE(cut.capacity_exhausted());
        }
        const auto refused = owner->capture_ordinary_history_export({"DiagnosticErrorRow"});
        EXPECT_FALSE(refused.valid()); EXPECT_TRUE(refused.capacity_exhausted());
        const lattice::ordinary_export_capacity_exhausted capacity;
        EXPECT_EQ(refused.failure(), capacity.what());
        held[0] = {}; // Actual last native cut/reference release returns one slot.
        const auto successor = owner->capture_ordinary_history_export({"DiagnosticErrorRow"});
        EXPECT_TRUE(successor.valid()) << successor.failure();
        EXPECT_FALSE(successor.capacity_exhausted());
        EXPECT_TRUE(successor.failure().empty());
    }
    const auto closed = owner->close_checked();
    EXPECT_FALSE(closed.failed()); EXPECT_FALSE(closed.cleanup_failed());
    EXPECT_TRUE(closed.cleanup_complete());
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include <lattice/ordinary_program.hpp>
#include "../../Sources/LatticeCore/src/ordinary_schema_validation.hpp"
namespace {
using order_ref=std::unique_ptr<lattice::swift_lattice_ref>;
lattice::property_descriptor order_property(const char* name,lattice::column_type type,bool history=false){
    lattice::property_descriptor p{};p.name=name;p.type=type;p.kind=lattice::property_kind::primitive;
    p.nullable=false;p.no_history=history;return p;
}
lattice::swift_schema_entry order_schema(bool reverse,bool history=false,bool added=false){
    auto value=order_property("value",lattice::column_type::text,history);
    auto payload=order_property("payload",lattice::column_type::blob);
    auto count=order_property("count",lattice::column_type::integer);
    lattice::swift_schema_entry s;s.table_name="AuditOrderRow";
    if(reverse){if(added)s.properties[count.name]=count;s.properties[payload.name]=payload;s.properties[value.name]=value;}
    else{s.properties[value.name]=value;s.properties[payload.name]=payload;if(added)s.properties[count.name]=count;}
    return s;
}
order_ref order_open(const lattice::swift_configuration& config,const lattice::SchemaVector& schemas,bool checked=false){
#if LATTICE_HAS_FRT
    return order_ref(checked?lattice::swift_lattice_ref::create_checked_mounted_server(config,schemas):lattice::swift_lattice_ref::create(config,schemas));
#else
    return std::make_unique<lattice::swift_lattice_ref>(checked?lattice::swift_lattice_ref::create_checked_mounted_server(config,schemas):lattice::swift_lattice_ref::create(config,schemas));
#endif
}
void order_close(order_ref& ref){
    if(!ref||!ref->get())throw std::runtime_error("order fixture owner unavailable: "+lattice::last_bridge_error());
    const auto ended=ref->get()->close_checked();
    if(ended.error||ended.cleanup_error||!ended.cleanup_complete)throw std::runtime_error("order fixture actual close unproved");
    ref.reset();
}
std::vector<std::string> order_names(const lattice::swift_schema_entry& schema){
    std::vector<std::string> result;for(const auto& [name,p]:schema.properties)result.push_back(name);return result;
}
std::vector<std::string> order_physical(lattice::database& db){
    std::vector<std::string> result;
    for(const auto& row:db.query("SELECT name FROM pragma_table_xinfo('AuditOrderRow','main') WHERE name NOT IN ('id','globalId') ORDER BY cid"))
        result.push_back(std::get<std::string>(row.at("name")));
    return result;
}
std::string order_array(const std::vector<std::string>& names){
    std::string result="json_array(";for(const auto& name:names){if(result.back()!='(')result+=',';result+="'"+name+"'";}return result+")";
}
std::vector<std::string> order_programs(const lattice::swift_schema_entry& schema){
    std::vector<std::pair<std::string,lattice::column_type>> columns;std::set<std::string> history;
    for(const auto& [name,p]:schema.properties){columns.emplace_back(name,p.type);if(p.no_history)history.insert(name);}
    std::vector<std::string> result;
    lattice::detail::ordinary_model_audit_programs(schema.table_name,columns,history,{},[&](const std::string& sql){result.push_back(sql);});return result;
}
std::vector<lattice::database::row_t> order_state(lattice::database& db){
    return db.query("SELECT type,name,sql FROM main.sqlite_schema WHERE tbl_name='AuditOrderRow' UNION ALL SELECT 'meta',key,value FROM main._lattice_meta WHERE key LIKE 'schema_fingerprint:%' OR key IN ('schema_version','trigger_flags:AuditOrderRow') ORDER BY 1,2");
}
void order_seed(const lattice::swift_configuration& config,const lattice::swift_schema_entry& schema){
    auto ref=order_open(config,{schema});if(!ref||!ref->get())throw std::runtime_error(lattice::last_bridge_error());
    ref->get()->db().execute("INSERT INTO AuditOrderRow(globalId,value,payload) VALUES(?,?,?)",
        {std::string("00000000-0000-4000-8000-000000009901"),std::string("preserved"),std::vector<uint8_t>{0,1,255}});
    order_close(ref);
}
void order_check_data(lattice::database& db){
    const auto rows=db.query("SELECT value,payload FROM AuditOrderRow");ASSERT_EQ(rows.size(),1u);
    EXPECT_EQ(std::get<std::string>(rows[0].at("value")),"preserved");
    EXPECT_EQ(std::get<std::vector<uint8_t>>(rows[0].at("payload")),(std::vector<uint8_t>{0,1,255}));
}
void order_replace(std::string& text,const std::string& before,const std::string& after){
    const auto at=text.find(before);if(at==std::string::npos||text.find(before,at+before.size())!=std::string::npos)
        throw std::runtime_error("order fixture mutation must affect one exact source fragment");
    text.replace(at,before.size(),after);
}
}

TEST(OrdinaryAuditOrder, FreshEquivalentCatalogReopensTheExactTypedOldBundleWithoutMutation){
    TempDB file{"audit-order-initial"};lattice::swift_configuration config(file.str());config.audit_retention_seconds=0;config.busy_timeout_ms=100;
    const auto original=order_schema(false,true),fresh=order_schema(true,true);ASSERT_NE(order_names(original),order_names(fresh));
    order_seed(config,original);std::vector<lattice::database::row_t> before,cookie;
    {lattice::database read(file.str(),lattice::database::open_mode::read_only,100);before=order_state(read);cookie=read.query("PRAGMA main.schema_version");read.close();}
    {auto ref=order_open(config,{fresh},true);ASSERT_TRUE(ref&&ref->get())<<lattice::last_bridge_error();order_check_data(ref->get()->db());order_close(ref);}
    {lattice::database read(file.str(),lattice::database::open_mode::read_only,100);EXPECT_EQ(order_state(read),before);EXPECT_EQ(read.query("PRAGMA main.schema_version"),cookie);order_check_data(read);read.close();}
}

TEST(OrdinaryAuditOrder, GenuineFlagOnlyRecreationKeepsItsNonphysicalHistoricalOrder){
    TempDB file{"audit-order-flags"};lattice::swift_configuration config(file.str());config.audit_retention_seconds=0;config.busy_timeout_ms=100;
    order_seed(config,order_schema(false,false));const auto recreated=order_schema(true,true);
    {auto ref=order_open(config,{recreated});ASSERT_TRUE(ref&&ref->get())<<lattice::last_bridge_error();auto& db=ref->get()->db();
        ASSERT_NE(order_physical(db),order_names(recreated));
        const auto sql=std::get<std::string>(db.query("SELECT sql FROM main.sqlite_schema WHERE name='AuditAuditOrderRowInsert'").at(0).at("sql"));
        ASSERT_NE(sql.find(order_array(order_names(recreated))),std::string::npos);order_close(ref);}
    {auto ref=order_open(config,{order_schema(false,true)},true);ASSERT_TRUE(ref&&ref->get())<<lattice::last_bridge_error();order_check_data(ref->get()->db());order_close(ref);}
}

TEST(OrdinaryAuditOrder, GenuineAlterRecreationDoesNotAssumePhysicalCidIsTheAuditRecipe){
    TempDB file{"audit-order-alter"};lattice::swift_configuration config(file.str());config.audit_retention_seconds=0;config.busy_timeout_ms=100;
    order_seed(config,order_schema(false,true));const auto recreated=order_schema(false,true,true);config.target_schema_version=2;
    {auto ref=order_open(config,{recreated});ASSERT_TRUE(ref&&ref->get())<<lattice::last_bridge_error();auto& db=ref->get()->db();
        ASSERT_NE(order_physical(db),order_names(recreated));
        const auto sql=std::get<std::string>(db.query("SELECT sql FROM main.sqlite_schema WHERE name='AuditAuditOrderRowInsert'").at(0).at("sql"));
        ASSERT_NE(sql.find(order_array(order_names(recreated))),std::string::npos);
        EXPECT_EQ(std::get<int64_t>(db.query("SELECT count FROM AuditOrderRow").at(0).at("count")),0);order_close(ref);}
    {auto ref=order_open(config,{order_schema(true,true,true)},true);ASSERT_TRUE(ref&&ref->get())<<lattice::last_bridge_error();order_check_data(ref->get()->db());order_close(ref);}
}

TEST(OrdinaryAuditOrder, CompleteRecoveredBundleStillRefusesMixedMissingExtraAndTamperedPrograms){
    TempDB file{"audit-order-tamper"};lattice::swift_configuration config(file.str());config.audit_retention_seconds=0;config.busy_timeout_ms=100;
    const auto schema=order_schema(false,true),other=order_schema(true,true);
    auto ref=order_open(config,{schema});ASSERT_TRUE(ref&&ref->get())<<lattice::last_bridge_error();auto* owner=ref->get();auto& db=owner->db();
    db.execute("INSERT INTO AuditOrderRow(globalId,value,payload) VALUES(?,?,?)",{std::string("00000000-0000-4000-8000-000000009901"),std::string("preserved"),std::vector<uint8_t>{0,1,255}});
    const auto expected=order_programs(schema),permuted=order_programs(other);ASSERT_EQ(expected.size(),3u);ASSERT_EQ(permuted.size(),3u);
    const auto select=[&]{return lattice::detail::ordinary_export_access::observation_tables(*owner,{"AuditOrderRow"});};
    ASSERT_EQ(select(),(std::vector<std::string>{"AuditOrderRow"}));
    for(unsigned fault=0;fault<12;++fault){SCOPED_TRACE(fault);
        db.execute("DROP TRIGGER IF EXISTS AuditLog_Update_AuditOrderRow; DROP TRIGGER IF EXISTS AuditAuditOrderRowInsert; DROP TRIGGER IF EXISTS AuditAuditOrderRowDelete; DROP TRIGGER IF EXISTS AuditOrderExtra");
        auto programs=permuted;
        switch(fault){
            case 0:programs[0]=expected[0];break;
            case 1:order_replace(programs[1],order_array(order_names(other)),"json_array('value','value')");break;
            case 2:order_replace(programs[1],order_array(order_names(other)),"json_array('value')");break;
            case 3:order_replace(programs[1],order_array(order_names(other)),"json_array('value','payload','unapproved')");break;
            case 4:order_replace(programs[1],order_array(order_names(other)),"json_array(upper('value'),'payload')");break;
            case 5:order_replace(programs[1],"'value', NEW.value","'value', NEW.payload");break;
            case 6:order_replace(programs[1],"hex(NEW.payload)","NEW.payload");break;
            case 7:order_replace(programs[0],"'value', NULL","'value', NEW.value");break;
            case 8:order_replace(programs[1],"sync_disabled() = 0","sync_disabled() = 1");break;
            case 9:order_replace(programs[1]," END"," SELECT 1; END");break;
            case 10:programs.pop_back();break;
            case 11:programs.push_back("CREATE TRIGGER AuditOrderExtra AFTER UPDATE ON AuditOrderRow BEGIN SELECT 1; END");break;
        }
        for(const auto& sql:programs)db.execute(sql);
        const auto before=order_state(db),cookie=db.query("PRAGMA main.schema_version");
        EXPECT_THROW(select(),lattice::db_error);
        EXPECT_EQ(order_state(db),before);EXPECT_EQ(db.query("PRAGMA main.schema_version"),cookie);order_check_data(db);
    }
    order_close(ref);
}

TEST(OrdinaryAuditOrder, GeoExpansionAndQuotedUnionAliasesKeepTheirClosedGeneratedPrograms){
    // Malformed trusted descriptions must still refuse before any metadata I/O.
    // These are DATA-only rejection probes, never grants or fake engine rows.
    for(const std::string token:{"!","! =","/","/*unapproved*/"}){
        SCOPED_TRACE(token);lattice::detail::ordinary_schema_projection projection;
        projection.models={"Rejected"};lattice::detail::ordinary_projection_table table;
        table.name="Rejected";table.create_sql="CREATE TABLE Rejected(id INTEGER PRIMARY KEY)";
        table.audit_programs={"CREATE TRIGGER RejectedAudit BEFORE DELETE ON Rejected WHEN OLD.id "+token+" 0 BEGIN SELECT 1; END"};
        projection.tables.emplace(table.name,std::move(table));size_t queries=0;
        const lattice::detail::ordinary_metadata_query refuse_io=[&](const std::string&,const std::vector<lattice::column_value_t>&,size_t){++queries;return std::vector<lattice::database::row_t>{};};
        EXPECT_THROW(lattice::detail::validate_ordinary_schema(projection,refuse_io),lattice::detail::ordinary_schema_validation_error);
        EXPECT_EQ(queries,0u);
    }
    TempDB file{"audit-order-derived"};lattice::swift_configuration config(file.str());config.audit_retention_seconds=0;config.busy_timeout_ms=100;
    auto note=order_property("note",lattice::column_type::text);
    auto area=order_property("area",lattice::column_type::real);area.is_geo_bounds=true;area.nullable=true;
    auto choice=order_property("choice",lattice::column_type::text);choice.kind=lattice::property_kind::union_type;choice.is_union=true;choice.nullable=true;
    choice.union_desc.union_table_name="_AuditOrderComplex_choice";
    lattice::union_case text,number;text.case_name="text";number.case_name="number";
    lattice::union_case_value a,b;a.type=lattice::column_type::text;b.type=lattice::column_type::integer;
    text.values.push_back(a);number.values.push_back(b);choice.union_desc.cases={text,number};
    lattice::swift_schema_entry original,fresh;original.table_name=fresh.table_name="AuditOrderComplex";
    original.properties[note.name]=note;original.properties[area.name]=area;original.properties[choice.name]=choice;
    fresh.properties[choice.name]=choice;fresh.properties[area.name]=area;fresh.properties[note.name]=note;
    ASSERT_NE(order_names(original),order_names(fresh));
    {auto ref=order_open(config,{original});ASSERT_TRUE(ref&&ref->get())<<lattice::last_bridge_error();order_close(ref);}
    {auto ref=order_open(config,{fresh},true);ASSERT_TRUE(ref&&ref->get())<<lattice::last_bridge_error();
        auto* owner=ref->get();const auto before=owner->db().query("SELECT name,sql FROM main.sqlite_schema WHERE tbl_name IN ('AuditOrderComplex','_AuditOrderComplex_choice') ORDER BY name");
        const auto selected=lattice::detail::ordinary_export_access::observation_tables(*owner,{"AuditOrderComplex"});
        EXPECT_EQ(selected,(std::vector<std::string>{"AuditOrderComplex","_AuditOrderComplex_choice"}));
        EXPECT_EQ(owner->db().query("SELECT name,sql FROM main.sqlite_schema WHERE tbl_name IN ('AuditOrderComplex','_AuditOrderComplex_choice') ORDER BY name"),before);
        const auto trigger=owner->db().query("SELECT sql FROM main.sqlite_schema WHERE name='Audit_AuditOrderComplex_choiceInsert'");
        ASSERT_EQ(trigger.size(),1u);EXPECT_NE(std::get<std::string>(trigger[0].at("sql")).find("'\"case\"'"),std::string::npos);
        // The real ordinary generator requires != in the protected cascade.
        // Other valid SQLite spellings and comments do not become approved SQL.
        auto& db=owner->db();
        const auto cascade=db.query("SELECT sql FROM main.sqlite_schema WHERE name='_cascade_delete_AuditOrderComplex_choice_union'");
        ASSERT_EQ(cascade.size(),1u);const auto approved=std::get<std::string>(cascade[0].at("sql"));
        ASSERT_NE(approved.find("OLD.choice != ''"),std::string::npos);
        const auto all_schema=[&]{return db.query("SELECT type,name,tbl_name,sql FROM main.sqlite_schema ORDER BY type,name");};
        const auto markers=[&]{return db.query("SELECT key,value FROM main._lattice_meta ORDER BY key");};
        const auto original_schema=all_schema(),original_markers=markers();
        for(const std::string spelling:{" = "," <> "," /*unapproved*/ != "}){
            SCOPED_TRACE(spelling);auto changed=approved;order_replace(changed," != ",spelling);
            db.execute("DROP TRIGGER _cascade_delete_AuditOrderComplex_choice_union");db.execute(changed);
            const auto faulted_schema=all_schema(),faulted_cookie=db.query("PRAGMA main.schema_version");
            EXPECT_THROW(lattice::detail::ordinary_export_access::observation_tables(*owner,{"AuditOrderComplex"}),lattice::db_error);
            EXPECT_EQ(all_schema(),faulted_schema);EXPECT_EQ(markers(),original_markers);
            EXPECT_EQ(db.query("PRAGMA main.schema_version"),faulted_cookie);
            db.execute("DROP TRIGGER _cascade_delete_AuditOrderComplex_choice_union");db.execute(approved);
        }
        EXPECT_EQ(all_schema(),original_schema);EXPECT_EQ(markers(),original_markers);
        order_close(ref);
    }
}
#endif
