// Include after the actual original context/descriptor helpers, ledger002 and
// managed profile implementation. No source SQL writes; current eligibility
// is sampled separately and never substitutes for accepted original values.
namespace {
enum class managed_upstream_eligibility : std::int64_t { unfiltered=1,matched=2,not_matched=3,absent=4,unobserved=5 };
struct managed_upstream_evidence {
    managed_upstream_eligibility eligibility=managed_upstream_eligibility::unobserved;
    std::shared_ptr<const ordinary_managed_wss_receipt> wss_receipt;
    ordinary_wire_original_record original;
    ordinary_audit_original_descriptor membership;
    values exact_mapping,exact_audit;
    original_scope_context receipt_context;
    std::uint64_t device=0,inode=0;
    bool historical_receipt=false;
};
managed_upstream_evidence read_managed_upstream(database& writer,const std::string& scope,
    const std::string& gid,const recovery_owner_schema& catalog,budget& retained) {
    original_view(writer,true);original_context_cache context(writer,retained);
    managed_upstream_evidence evidence;
    // An absent mapping is never reconstructed from a fresh hub row, own-source
    // enrollment, an audit GID, or the requested source/epoch coordinates.
    evidence.exact_mapping=original_record(writer,scope,gid,retained,context);
    if(evidence.exact_mapping.empty())refuse("managed upstream original mapping unavailable");
    evidence.original=original_decode(writer,evidence.exact_mapping,retained,true,context);
    evidence.membership=canonical_descriptor(evidence.original,catalog,retained);
    evidence.exact_audit=original_audit(writer,gid,retained);
    if(evidence.exact_audit.empty()||integer(evidence.exact_audit[0])!=evidence.original.local_audit_id||
       original_seal("ordinary-wire-local-semantic-projection-v2",evidence.exact_audit,11,retained)!=evidence.original.projection_seal)
        refuse("managed upstream actual AuditLog association changed");
    const auto& actual=context.scope(scope);
    original_copy_charge(actual.binding,retained);
    retained.add(sizeof(rows)+actual.receipts.size()*sizeof(values));
    for(const auto& receipt:actual.receipts)original_copy_charge(receipt,retained);
    evidence.receipt_context=actual;
    for(const auto& receipt:actual.receipts)if(integer(receipt[0])==evidence.original.stamp.cut)evidence.historical_receipt=true;
    // The current receipt is mandatory. A guarded older original may outlive
    // that exact cut receipt; this preserves the distinction and grants no new
    // source installation or whole-partition completeness from its absence.
    const auto physical=writer.physical_identity("main",{},true);
    if(!physical)refuse("managed upstream physical source unavailable");
    evidence.device=physical->device;evidence.inode=physical->inode;
    // All data used by the published immutable observation is checked again
    // before leaving this closed owned turn. No external callback runs here.
    if(original_record(writer,scope,gid,retained,context)!=evidence.exact_mapping||
       original_audit(writer,gid,retained)!=evidence.exact_audit)
        refuse("managed upstream record changed before completion");
    context.finish();
    return evidence;
}
managed_upstream_eligibility managed_current_eligibility(database& writer,
    const ordinary_managed_ipc_route_description& route,const ordinary_wire_original_record& original,budget& retained) {
    // Metadata preflight above proves the actual declared table. Current-row
    // visibility is a separate observation; its values never replace the
    // accepted original envelope or establish an authored privacy command.
    if(!route.filter)return managed_upstream_eligibility::unfiltered;
    const auto found=std::find_if(route.filter->begin(),route.filter->end(),[&](const auto& entry){return entry.table_name==original.local_view.table_name;});
    if(found==route.filter->end())return managed_upstream_eligibility::not_matched;
    const auto& name=original.local_view.table_name;const auto& gid=original.local_view.global_row_id;
    const std::size_t predicate_size=found->where_clause?found->where_clause->size():1;
    retained.add(256+name.size()*2+gid.size()+predicate_size+sizeof(values)+sizeof(column_value_t));
    const auto sql="SELECT CASE WHEN ("+(found->where_clause?*found->where_clause:std::string("1"))+
        ") THEN 1 ELSE 0 END FROM main."+quote(name)+" WHERE globalId=? COLLATE NOCASE LIMIT 1";
    const auto observed=read(ordinary_export_storage_access::borrow(writer),sql,{gid},1,retained);
    if(observed.empty())return managed_upstream_eligibility::absent;
    if(observed.size()!=1||observed[0].size()!=1)refuse("managed upstream eligibility shape");
    return integer(observed[0][0])==1?managed_upstream_eligibility::matched:managed_upstream_eligibility::not_matched;
}
void managed_upstream_committed(const recovery_install_result& settled) {
    if(settled.state!=recovery_install_state::committed||settled.primary_error||settled.cleanup_error||settled.postcommit_error||settled.notification_error)
        throw ordinary_order_write_error(settled);
}
}
struct ordinary_managed_upstream_capture::implementation {
    // Allocation precedes retaining any foreign context. A callback/finalizer
    // can fork; quarantine this already allocated capsule before touching the
    // next context or the common-pool release machinery in the child.
    struct payload {
        const std::uint64_t creator=managed_pair_creator();
        std::optional<ordinary_resource_reservation<false>> reservation;
        std::optional<managed_upstream_evidence> evidence;
        std::optional<std::string> route_policy;
        std::shared_ptr<const ordinary_managed_product_state> product;
    };
    struct dispose_payload {
        void operator()(payload* capsule)const noexcept {
            if(!capsule||capsule->creator!=managed_pair_creator())return;
            capsule->product.reset();
            if(capsule->creator!=managed_pair_creator())return;
            capsule->evidence.reset(); // actual WSS/original/database captures
            if(capsule->creator!=managed_pair_creator())return;
            capsule->route_policy.reset();
            if(capsule->creator!=managed_pair_creator())return;
            capsule->reservation.reset(); // unchanged common pool, actual last
            if(capsule->creator!=managed_pair_creator())return;
            delete capsule;
        }
    };
    using custody=std::unique_ptr<payload,dispose_payload>;
    static custody retain(ordinary_resource_reservation<false>&& slot,managed_upstream_evidence&& captured,
        std::string&& policy,const std::shared_ptr<const ordinary_managed_product_state>& authority) {
        // First establish the empty guarded record. If any subsequent step
        // throws, its custom deleter preserves the same creator-veto order.
        custody result(new payload);
        // A partially constructed optional must never default-unwind an
        // already moved callback owner before the enclosing guard can act.
        static_assert(noexcept(ordinary_resource_reservation<false>(std::move(slot))));
        static_assert(noexcept(managed_upstream_evidence(std::move(captured))));
        static_assert(noexcept(std::string(std::move(policy))));
        result->reservation.emplace(std::move(slot));
        result->evidence.emplace(std::move(captured));
        result->route_policy.emplace(std::move(policy));
        result->product=authority;
        return result;
    }
    custody held;
    // Borrowed aliases never own or destroy a quarantined context.
    const managed_upstream_evidence& evidence;
    const std::string& route_policy;
    const std::shared_ptr<const ordinary_managed_product_state>& product;
    implementation(ordinary_resource_reservation<false>&& slot,managed_upstream_evidence&& captured,std::string&& policy,
        const std::shared_ptr<const ordinary_managed_product_state>& authority={})
        :held(retain(std::move(slot),std::move(captured),std::move(policy),authority)),
         evidence(*held->evidence),route_policy(*held->route_policy),product(held->product){}
    ~implementation()=default; // same deleter also covers constructor unwind

};
ordinary_managed_upstream_capture::ordinary_managed_upstream_capture(std::unique_ptr<implementation> value):impl_(std::move(value)){}
ordinary_managed_upstream_capture::~ordinary_managed_upstream_capture()=default;
ordinary_managed_upstream_capture::ordinary_managed_upstream_capture(ordinary_managed_upstream_capture&&)noexcept=default;
ordinary_managed_upstream_capture& ordinary_managed_upstream_capture::operator=(ordinary_managed_upstream_capture&&)noexcept=default;
#define LATTICE_MANAGED_UPSTREAM_ACCESSOR(type,name,expression) \
    type ordinary_managed_upstream_capture::name()const {if(!impl_)refuse("moved managed upstream observation");return (expression);}
LATTICE_MANAGED_UPSTREAM_ACCESSOR(const std::vector<std::uint8_t>&,envelope,impl_->evidence.original.envelope)
LATTICE_MANAGED_UPSTREAM_ACCESSOR(const std::vector<std::uint8_t>&,descriptor,impl_->evidence.original.descriptor)
LATTICE_MANAGED_UPSTREAM_ACCESSOR(const std::vector<std::string>&,original_fields,impl_->evidence.membership.fields)
LATTICE_MANAGED_UPSTREAM_ACCESSOR(const std::vector<std::string>&,original_names,impl_->evidence.membership.names)
LATTICE_MANAGED_UPSTREAM_ACCESSOR(const std::string&,source_id,impl_->evidence.original.stamp.source)
LATTICE_MANAGED_UPSTREAM_ACCESSOR(const std::string&,scope,impl_->evidence.original.stamp.scope)
LATTICE_MANAGED_UPSTREAM_ACCESSOR(const std::string&,schema,impl_->evidence.original.stamp.schema)
LATTICE_MANAGED_UPSTREAM_ACCESSOR(const std::string&,record_seal,impl_->evidence.original.record_seal)
LATTICE_MANAGED_UPSTREAM_ACCESSOR(std::int64_t,accepted_cut,impl_->evidence.original.stamp.cut)
LATTICE_MANAGED_UPSTREAM_ACCESSOR(std::int64_t,current_frontier_cut,integer(impl_->evidence.receipt_context.binding[7]))
LATTICE_MANAGED_UPSTREAM_ACCESSOR(std::int64_t,local_audit_id,impl_->evidence.original.local_audit_id)
LATTICE_MANAGED_UPSTREAM_ACCESSOR(bool,historical_receipt_observed,impl_->evidence.historical_receipt)
#undef LATTICE_MANAGED_UPSTREAM_ACCESSOR

ordinary_managed_upstream_capture ordinary_export_access::capture_managed_upstream(
    const ordinary_managed_ipc_local_route& route,const std::string& scope,const std::string& gid) {
    return capture_managed_upstream_until(route,scope,gid,std::chrono::steady_clock::now()+std::chrono::seconds(10));
}
ordinary_managed_upstream_capture ordinary_export_access::capture_managed_upstream_until(
    const ordinary_managed_ipc_local_route& route,const std::string& scope,const std::string& gid,ordinary_launch::deadline end) {
    managed_pair_deadline(end);
    // Same global capacity as existing v2/history exports, before allocation.
    route.validate_lifetime();ordinary_resource_reservation<false> slot;auto& source=route.source_owner();
    budget retained{wire_limits};original_key_charge(scope,gid,retained);
    retained.add(sizeof(ordinary_managed_upstream_capture::implementation)+sizeof(ordinary_managed_upstream_capture::implementation::payload)+sizeof(managed_upstream_evidence));
    const std::string selected_scope=scope,selected_gid=gid;
    const auto& description=route.description();std::string policy=ipc_route_policy(description,retained);
    // Lookup/promote retained native resources before entering the SQL owner.
    // Failed lookup and retired-candidate destruction never run under it.
    const auto accepted_wss=managed_product_wss_receipt(route.product_authority(),route.source_profile(),selected_scope);
    std::optional<managed_upstream_evidence> captured;
    const auto settled=recovery_writer_access::ordinary_owned_write(source,[&](database& writer){
        route.validate_lifetime();validate_managed_profile_until(route.source_profile(),source,writer,end);validate_managed_product_policy(route,source,writer);
        ordinary_export_frame declared;declared.wire_profile=true;
        capture_projected_tables(ordinary_export_storage_access::borrow(writer),source.recovery_schemas_,[](auto& out,const auto& property){lattice_db::serialize_property_for_fingerprint(out,property);},
            description.models,declared,retained,false,nullptr,nullptr);
        auto actual=read_managed_upstream(writer,selected_scope,selected_gid,source.recovery_schemas_,retained);
        actual.wss_receipt=accepted_wss;
        if(!accepted_wss->current())refuse("managed upstream WSS retired before source observation");
        const auto& accepted=*actual.wss_receipt;const auto& binding=actual.receipt_context.binding;
        retained.add(64);
        if(binding.size()!=9||text(binding[1])!=accepted.channel||text(binding[2])!=accepted.source||integer(binding[3])!=accepted.source_epoch||
            picosha2::hash256_hex_string(text(binding[6]))!=accepted.schema_digest||integer(binding[7])!=accepted.cut||text(binding[8])!=accepted.receipt_digest||
            actual.original.stamp.source!=accepted.source||actual.original.stamp.epoch!=accepted.source_epoch||actual.original.stamp.cut>accepted.cut)
            refuse("managed upstream mapping not covered by current actual WSS acceptance");
        // Scope/frontier/receipt and original record are read in one actual
        // owned view. NoHistory changes still come from the guarded envelope,
        // never from a fresh hub row or the directory membership response.

        if(std::none_of(declared.tables.begin(),declared.tables.end(),[&](const auto& table){return table.name==actual.original.local_view.table_name;}))
            refuse("managed upstream original table outside retained route");
        actual.eligibility=managed_current_eligibility(writer,description,actual.original,retained);
        captured.emplace(std::move(actual));
        route.validate_lifetime();validate_managed_profile_until(route.source_profile(),source,writer,end);validate_managed_product_policy(route,source,writer);
    });
    managed_upstream_committed(settled);route.validate_lifetime();managed_pair_deadline(end);
    if(!captured||!captured->wss_receipt||!captured->wss_receipt->current())refuse("managed upstream WSS retired during owned delivery");
    // This is still evidence only. Actual current eligibility is separate
    // from the accepted envelope and cannot invent original/author intent.
    // An endpoint still needs exact request/frame correlation and custody.
    return ordinary_managed_upstream_capture(std::make_unique<ordinary_managed_upstream_capture::implementation>(
        std::move(slot),std::move(*captured),std::move(policy),route.product_authority()));
}

// Actual-resource fixture support; it installs a real locally issued cut and
// its exact generated descriptor in the ordinary owning engine. No live IPC
// role, remote authentication, managed profile or completed transport is faked.
void ordinary_export_access::seed_managed_upstream_for_test(lattice_db& owner,const owned_server_export& issued,const std::string& channel) {
    if(!issued.frame_||!issued.frame_->wire_profile)refuse("test upstream requires real issued v2 capture");
    (void)apply_impl(owner,issued,std::nullopt,false,&channel);
    const auto result=with_original_store_for_test(owner,[&](ordinary_wire_original_turn& turn){
        budget retained{wire_limits};const auto& frame=*issued.frame_;retained.add(64);const auto digest=frame_fingerprint(frame);
        for(size_t n=0;n<frame.entries.size();++n){const auto& entry=frame.entries[n];
            const auto descriptor=encode_ordinary_original_descriptor(frame.originals.at(n),[&](size_t bytes){retained.add(bytes);});
            auto* writer=recovery_writer_access::active_writer(owner);if(!writer)refuse("test upstream lost owning writer");
            const auto actual=original_audit(*writer,entry.global_id,retained);if(actual.empty())refuse("test upstream missing actual AuditLog");
            turn.store({frame.scope,frame.source,frame.schema,digest,frame.epoch,frame.cut},entry,1,descriptor,integer(actual[0]));
        }
    });managed_upstream_committed(result);
}
ordinary_managed_upstream_capture ordinary_export_access::capture_managed_upstream_for_test(lattice_db& owner,const std::string& scope,const std::string& gid) {
    ordinary_resource_reservation<false> slot;budget retained{wire_limits};original_key_charge(scope,gid,retained);
    retained.add(sizeof(ordinary_managed_upstream_capture::implementation)+sizeof(ordinary_managed_upstream_capture::implementation::payload)+sizeof(managed_upstream_evidence));
    const std::string selected_scope=scope,selected_gid=gid;std::optional<managed_upstream_evidence> captured;
    const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){
        if(recovery_writer_access::active_writer(owner)!=&writer)refuse("test upstream lost actual writer");
        captured.emplace(read_managed_upstream(writer,selected_scope,selected_gid,owner.recovery_schemas_,retained));
    });managed_upstream_committed(settled);
    if(!captured)refuse("test upstream missing observation");
    return ordinary_managed_upstream_capture(std::make_unique<ordinary_managed_upstream_capture::implementation>(
        std::move(slot),std::move(*captured),std::string{}));
}

namespace {
ordinary_managed_contribution_observation observation_from_managed_capture(
    const managed_upstream_evidence& actual,budget& retained) {
    const auto& original=actual.original;
    // Source storage/copy work is charged before retaining one new copy. The
    // result's envelope and descriptor are the actual accepted bytes, durable
    // at the recipient even when all source/callback objects are later gone.
    retained.add(sizeof(ordinary_managed_contribution_observation)+original.envelope.size()+original.descriptor.size()+
        original.stamp.source.size()+original.stamp.scope.size()+original.stamp.schema.size()+original.stamp.digest.size()+
        original.record_seal.size()+original.local_view.table_name.size()+original.local_view.global_row_id.size()+original.gid.size());
    ordinary_managed_contribution_observation result;
    result.source=original.stamp.source;result.scope=original.stamp.scope;result.schema=original.stamp.schema;
    result.digest=original.stamp.digest;result.record_seal=original.record_seal;result.epoch=original.stamp.epoch;result.cut=original.stamp.cut;
    result.table=original.local_view.table_name;result.row_gid=original.local_view.global_row_id;result.original_gid=original.gid;
    result.envelope=original.envelope;result.descriptor=original.descriptor;
    const auto& binding=actual.receipt_context.binding;
    if(binding.size()!=9)refuse("managed upstream association binding unavailable");
    const auto association_bound=512+original.projection_seal.size()+text(binding[1]).size()+text(binding[8]).size()+original.record_seal.size();
    if(association_bound>ipc_association_bytes)refuse("managed exact association pre-copy bound");
    retained.add(sizeof(values)+14*sizeof(column_value_t)+association_bound);
    values association{std::string("managed-accepted-original-association-v1"),
        std::to_string(actual.device),std::to_string(actual.inode),original.local_audit_id,
        original.local_view.row_id,static_cast<std::int64_t>(original.local_view.is_from_remote),
        original.projection_seal,text(binding[1]),integer(binding[7]),text(binding[8]),
        static_cast<std::int64_t>(actual.historical_receipt),static_cast<std::int64_t>(actual.eligibility),
        original.stamp.cut,original.record_seal};
    // This is exact metadata, not a second copy of envelope/property values.
    // Original raw names/fields remain in envelope+descriptor without rewriting.
    (void)encoded_size(association,ipc_association_bytes); // before blob allocation
    result.projection=encode(association,retained);
    if(result.projection.size()>ipc_association_bytes)refuse("managed exact association metadata bound");
    return result;
}
}

ordinary_managed_contribution_observation_result ordinary_export_access::record_managed_upstream_observation(
    const ordinary_managed_ipc_local_route& route,const ordinary_managed_upstream_capture& capture) {
    route.validate_lifetime();
    if(!capture.impl_||!capture.impl_->product||capture.impl_->product!=route.product_authority())
        refuse("managed observation requires exact issued product capture");
    const auto& captured=capture.impl_->evidence;
    if(!captured.wss_receipt||!captured.wss_receipt->current())refuse("managed observation actual WSS association retired");
    if(captured.eligibility!=managed_upstream_eligibility::unfiltered&&captured.eligibility!=managed_upstream_eligibility::matched)
        refuse("managed observation requires separate visibility/removal decision");
    budget retained{wire_limits};const auto& description=route.description();
    if(ipc_route_policy(description,retained)!=capture.impl_->route_policy)refuse("managed observation route policy changed");
    auto& source=route.source_owner();
    // Fresh source turn rechecks the actual mapping, exact original AuditLog
    // association and eligibility. No source SQL lock is held during recipient
    // writes; this remains an observed contribution, not distributed atomicity.
    const auto source_settled=recovery_writer_access::ordinary_owned_write(source,[&](database& writer){
        route.validate_lifetime();route.source_profile().validate(source,writer);validate_managed_product_policy(route,source,writer);
        const auto fresh=read_managed_upstream(writer,captured.original.stamp.scope,captured.original.gid,source.recovery_schemas_,retained);
        if(fresh.exact_mapping!=captured.exact_mapping||fresh.exact_audit!=captured.exact_audit||
            fresh.receipt_context!=captured.receipt_context||fresh.device!=captured.device||fresh.inode!=captured.inode||
            managed_current_eligibility(writer,description,fresh.original,retained)!=captured.eligibility)
            refuse("managed observation changed before recipient delivery");
        if(!captured.wss_receipt->current())refuse("managed observation WSS changed during source check");
        route.validate_lifetime();route.source_profile().validate(source,writer);validate_managed_product_policy(route,source,writer);
    });managed_upstream_committed(source_settled);
    auto input=observation_from_managed_capture(captured,retained);
    auto& recipient=route.recipient_owner();ordinary_managed_contribution_observation_result result;
    const auto settled=recovery_writer_access::ordinary_owned_write(recipient,[&](database& writer){
        original_view(writer,true);route.validate_lifetime();route.recipient_profile().validate(recipient,writer);
        if(capture.impl_->product!=route.product_authority()||!captured.wss_receipt->current()||
            ipc_route_policy(description,retained)!=capture.impl_->route_policy)
            refuse("managed observation authority changed before write");
        ordinary_export_frame declared;declared.wire_profile=true;
        capture_projected_tables(ordinary_export_storage_access::borrow(writer),recipient.recovery_schemas_,[](auto& out,const auto& property){lattice_db::serialize_property_for_fingerprint(out,property);},
            description.models,declared,retained,false,nullptr,nullptr);
        if(std::none_of(declared.tables.begin(),declared.tables.end(),[&](const auto& table){return table.name==input.table;}))
            refuse("managed upstream observation table outside actual recipient");
        result=ipc_observation_record(writer,description,input,retained,ipc_observation_encoding::retained_original_association);
        route.validate_lifetime();route.recipient_profile().validate(recipient,writer);
        if(!captured.wss_receipt->current())refuse("managed observation WSS retired before recipient commit");
    });managed_upstream_committed(settled);
    // A post-COMMIT revocation still fails the call; the durable record remains
    // passive history, never an ACK, winner, projection or repair completion.
    route.validate_lifetime();if(!captured.wss_receipt->current())refuse("managed observation WSS retired during recipient settlement");
    return result;
}

ordinary_managed_contribution_observation_result ordinary_export_access::record_managed_upstream_observation_for_test(
    lattice_db& recipient,const ordinary_managed_ipc_route_description& description,const ordinary_managed_upstream_capture& capture) {
    if(!capture.impl_)refuse("test observation requires actual captured original");
    // Actual captured/source-committed byte persistence only. This seam cannot
    // issue a managed profile, product route, WSS acceptance, ACK or model write.
    budget retained{wire_limits};auto input=observation_from_managed_capture(capture.impl_->evidence,retained);
    ordinary_managed_contribution_observation_result result;
    const auto settled=recovery_writer_access::ordinary_owned_write(recipient,[&](database& writer){
        if(recovery_writer_access::active_writer(recipient)!=&writer)refuse("test observation lost actual writer");
        result=ipc_observation_record(writer,description,input,retained,ipc_observation_encoding::retained_original_association);
    });managed_upstream_committed(settled);return result;
}
