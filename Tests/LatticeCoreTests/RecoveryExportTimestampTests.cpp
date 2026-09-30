#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_export_adapter.hpp"
#include <cmath>
#include <locale>
#include <sstream>

struct ExportTimestampRow { std::string value; };
LATTICE_SCHEMA(ExportTimestampRow,value);

namespace {
using namespace lattice;
using namespace lattice::detail;
void require_commit(const recovery_install_result& result) {
    if(result.primary_error)std::rethrow_exception(result.primary_error);
    if(result.cleanup_error)std::rethrow_exception(result.cleanup_error);
    if(result.postcommit_error)std::rethrow_exception(result.postcommit_error);
    if(result.state!=recovery_install_state::committed)
        throw std::runtime_error("timestamp fixture transaction did not commit");
}
void generated_timestamp_case(const std::string& path,bool update) {
    SCOPED_TRACE(path);
    SCOPED_TRACE(update);
    configuration config(path);config.audit_retention_seconds=0;config.busy_timeout_ms=100;
    auto owner=std::make_shared<lattice_db>(config);
    if(!config.is_in_memory()) {
        auto* notifier=instance_registry::instance().get_or_create_notifier(path);
        if(notifier)notifier->stop_listening();
    }
    recovery_obligation_producer_discovery_limits limits{
        {4,128,128,2*1024*1024},{4,128,8192},{4,128,128,1048576,8*1024*1024}};
    recovery_obligation_address address;
    require_commit(recovery_writer_access::install(owner,[&](database&) {
        receive_install_store receiver(owner,limits.installations);receiver.initialize();
        receive_install_binding binding{"timestamp-contribution","authority","source","epoch","scope","schema"};
        receiver.bind(binding);
        recovery_obligation_store journal(owner,limits.obligations,limits.installations);journal.initialize();
        address=journal.bind({binding,"grant","receipts"}).address;
    }));
    require_commit(recovery_local_producer_adapter::enroll_for_qualification(
        owner,{address,{"ExportTimestampRow"},{'g'}},limits));
    const auto row=owner->add(ExportTimestampRow{"initial"});
    if(update)owner->db().execute("UPDATE ExportTimestampRow SET value='updated' WHERE globalId=?",{row.global_id()});
    const auto originals=owner->db().query(
        "SELECT id,globalId,timestamp,typeof(timestamp) AS kind FROM AuditLog WHERE tableName='ExportTimestampRow' ORDER BY id");
    ASSERT_EQ(originals.size(),update?2u:1u);
    for(const auto& original:originals) {
        ASSERT_EQ(std::get<std::string>(original.at("kind")),"real");
        ASSERT_TRUE(std::holds_alternative<double>(original.at("timestamp")));
        ASSERT_TRUE(std::isfinite(std::get<double>(original.at("timestamp"))));
    }
    for(int retry=0;retry<2;++retry) {
        SCOPED_TRACE(retry);
        auto prepared=recovery_export_adapter::prepare_pending(owner,"timestamp-route",1,100,{},false);
        ASSERT_TRUE(prepared.protected_store);ASSERT_TRUE(prepared.frame);
        const auto& entries=prepared.frame->entries();ASSERT_EQ(entries.size(),originals.size());
        for(size_t i=0;i<entries.size();++i) {
            const auto& entry=entries[i];const auto& original=originals[i];
            EXPECT_EQ(entry.id,std::get<int64_t>(original.at("id")));
            EXPECT_EQ(entry.global_id,std::get<std::string>(original.at("globalId")));
            std::istringstream decoded(entry.timestamp);decoded.imbue(std::locale::classic());
            double seconds=0;decoded>>seconds;ASSERT_FALSE(decoded.fail());
            EXPECT_EQ(decoded.peek(),std::char_traits<char>::eof());
            EXPECT_EQ(seconds,std::get<double>(original.at("timestamp")));
            // The real wire encoder/decoder must retain the same timestamp,
            // including subsecond precision, without mutating persisted data.
            const auto wire=server_sent_event::from_json("{\"auditLog\":["+entry.to_json()+"]}");
            ASSERT_TRUE(wire);ASSERT_EQ(wire->audit_logs.size(),1u);
            EXPECT_EQ(wire->audit_logs[0].timestamp,entry.timestamp);
        }
        EXPECT_EQ(owner->db().query(
            "SELECT id,globalId,timestamp,typeof(timestamp) AS kind FROM AuditLog WHERE tableName='ExportTimestampRow' ORDER BY id"),originals);
        EXPECT_FALSE(owner->db().is_in_transaction());
        require_commit(recovery_writer_access::install(owner,[&](database&) {
            recovery_obligation_store journal(owner,limits.obligations,limits.installations);
            for(const auto& original:originals) {
                const auto claimed=journal.find(address,std::get<std::string>(original.at("globalId")));
                ASSERT_TRUE(claimed);ASSERT_TRUE(claimed->first_export_claim);
                EXPECT_EQ(claimed->stage,recovery_obligation_stage::open);
            }
        }));
    }
}
}
TEST(RecoveryExportTimestamp, MemoryGeneratedInsertRoundTripsAndRetryKeepsOriginal) {
    generated_timestamp_case(":memory:",false);
}
TEST(RecoveryExportTimestamp, MemoryGeneratedUpdateRoundTripsAndRetryKeepsOriginal) {
    generated_timestamp_case(":memory:",true);
}
TEST(RecoveryExportTimestamp, FileGeneratedInsertRoundTripsAndRetryKeepsOriginal) {
    TempDB file{"export_timestamp_insert"};generated_timestamp_case(file.str(),false);
}
TEST(RecoveryExportTimestamp, FileGeneratedUpdateRoundTripsAndRetryKeepsOriginal) {
    TempDB file{"export_timestamp_update"};generated_timestamp_case(file.str(),true);
}
