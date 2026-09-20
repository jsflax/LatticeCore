#include "server_export_fixture.hpp"
#include "../../LatticeCore/src/recovery_local_producer.hpp"

namespace lattice::server_export_test_support {
namespace {
using namespace detail;
const recovery_obligation_producer_discovery_limits limits{{4,128,128,2*1024*1024},{4,128,8192},{4,128,128,1048576,8*1024*1024}};
const receive_install_binding binding{"sdk-server-fixture","fixture-authority","fixture-source","fixture-epoch","fixture-scope","fixture-schema"};
std::shared_ptr<lattice_db> actual_owner(const swift_lattice_ref& ref){
    // Test fixture acquisition only, with the caller's actual strong ref still
    // alive. The production endpoint factory separately copies its own impl_.
    auto owner=swift_lattice_ref::shared_for_lattice(const_cast<swift_lattice*>(ref.get()));
    if(!owner||owner->is_closed())throw db_error("server export fixture requires a live actual ref");return owner;
}
void committed(const recovery_install_result& result){
    if(result.primary_error)std::rethrow_exception(result.primary_error);
    if(result.cleanup_error)std::rethrow_exception(result.cleanup_error);
    if(result.postcommit_error)std::rethrow_exception(result.postcommit_error);
    if(result.notification_error)std::rethrow_exception(result.notification_error);
    if(result.state!=recovery_install_state::committed)throw db_error("server export fixture transaction did not commit");
}
void failure()noexcept{
    try{throw;}catch(const std::exception& error){record_bridge_error(error.what());}
    catch(...){record_bridge_error("Unknown server export test fixture failure");}
}
}
int32_t enroll(const swift_lattice_ref& ref,const std::string& model)noexcept{
    last_bridge_error().clear();
    try{
        if(model.empty()||model.size()>64)throw db_error("server export fixture model outside bound");
        auto owner=actual_owner(ref);recovery_obligation_address address;
        committed(recovery_writer_access::install(owner,[&](database&){
            receive_install_store receiver(owner,limits.installations);receiver.initialize();receiver.bind(binding);
            recovery_obligation_store journal(owner,limits.obligations,limits.installations);journal.initialize();
            if(journal.read(binding.channel))throw db_error("server export fixture must enroll only once");
            address=journal.bind({binding,"fixture-grant","fixture-receipts"}).address;
        }));
        committed(recovery_local_producer_adapter::enroll_for_qualification(owner,{address,{model},{'f','i','x','t','u','r','e'}},limits));
        return 0;
    }catch(...){failure();return 1;}
}
int32_t freeze(const swift_lattice_ref& ref)noexcept{
    last_bridge_error().clear();
    try{auto owner=actual_owner(ref);committed(recovery_writer_access::install(owner,[&](database&){
        recovery_obligation_store journal(owner,limits.obligations,limits.installations);const auto scope=journal.read(binding.channel);
        if(!scope)throw db_error("missing server export fixture contribution");journal.freeze(scope->address,1);
    }));return 0;}catch(...){failure();return 1;}
}
fixture_facts facts(const swift_lattice_ref& ref)noexcept{
    last_bridge_error().clear();fixture_facts result;
    try{auto owner=actual_owner(ref);committed(recovery_writer_access::install(owner,[&](database& writer){
        recovery_obligation_store journal(owner,limits.obligations,limits.installations);journal.audit();
        const auto counts=writer.query("SELECT (SELECT COUNT(*) FROM AuditLog) AS originals,"
            "(SELECT COUNT(*) FROM _lattice_obligation_entry WHERE first_export IS NOT NULL) AS claimed,"
            "(SELECT COUNT(*) FROM _lattice_obligation_producer_stamp) AS stamps");
        if(counts.size()!=1)throw db_error("missing server export fixture facts");
        result.originals=std::get<int64_t>(counts[0].at("originals"));result.claimed=std::get<int64_t>(counts[0].at("claimed"));result.stamps=std::get<int64_t>(counts[0].at("stamps"));
    }));result.status=0;}catch(...){failure();}return result;
}
}
