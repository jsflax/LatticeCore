// Included after upstream capture001+delivery001, peer owner and product policy.
// Only the closed peer delivery implementation below calls these private paths.
namespace {
constexpr std::size_t managed_peer_metadata_bytes=512*1024;
constexpr std::size_t managed_peer_request_bytes=ordinary_sync_limits::frame_bytes+managed_peer_metadata_bytes;
std::int64_t managed_peer_time(ordinary_launch::deadline value) {
    if(value==ordinary_launch::deadline::max())refuse("managed peer finite deadline required");
    const auto count=std::chrono::duration_cast<std::chrono::nanoseconds>(value.time_since_epoch()).count();
    if(count<=0)refuse("managed peer deadline representation");return count;
}
ordinary_launch::deadline managed_peer_deadline(std::int64_t value) {
    if(value<=0)refuse("managed peer invalid deadline");
    // Both fixed paired images use this same Core/std::chrono ABI on one
    // kernel. Hosted qualification must establish that actual clock domain;
    // this is neither wall time nor a deadline renewed upon receipt.
    const auto end=ordinary_launch::deadline(std::chrono::duration_cast<ordinary_launch::deadline::duration>(std::chrono::nanoseconds(value)));
    if(managed_peer_time(end)!=value)refuse("managed peer clock precision mismatch");
    return end;
}
std::string managed_peer_nonce(const ordinary_managed_cohort_link_facts& actual){
    return picosha2::bytes_to_hex_string(actual.nonce.begin(),actual.nonce.end());
}
values managed_peer_decode(std::span<const std::uint8_t> bytes,budget& retained,std::size_t cells=64) {
    if(bytes.size()>managed_peer_request_bytes)refuse("managed peer request byte bound");
    std::size_t at=0;
    const auto number=[&](){if(bytes.size()-at<8)refuse("managed peer truncated scalar");std::uint64_t result=0;
        for(unsigned n=0;n<8;++n)result=(result<<8)|bytes[at++];return result;};
    const auto count=number();if(count>cells)refuse("managed peer scalar count bound");
    retained.add(sizeof(values)+static_cast<std::size_t>(count)*sizeof(column_value_t));
    values result;result.reserve(static_cast<std::size_t>(count));
    for(std::uint64_t i=0;i<count;++i){
        if(at==bytes.size())refuse("managed peer missing scalar tag");const auto tag=bytes[at++];const auto length=number();
        if(tag==0){if(length)refuse("managed peer noncanonical null");result.emplace_back(nullptr);}
        else if(tag==1)result.emplace_back(std::bit_cast<std::int64_t>(length));
        else if(tag==3||tag==4){
            if(length>bytes.size()-at)refuse("managed peer scalar exceeds request");
            retained.add(static_cast<std::size_t>(length)+1);
            if(tag==3)result.emplace_back(std::string(reinterpret_cast<const char*>(bytes.data()+at),static_cast<std::size_t>(length)));
            else result.emplace_back(std::vector<std::uint8_t>(bytes.begin()+static_cast<std::ptrdiff_t>(at),bytes.begin()+static_cast<std::ptrdiff_t>(at+length)));
            at+=static_cast<std::size_t>(length);
        }else refuse("managed peer unsupported scalar");
    }
    if(at!=bytes.size())refuse("managed peer trailing bytes");return result;
}
const std::vector<std::uint8_t>& managed_peer_blob(const column_value_t& value){
    const auto* bytes=std::get_if<std::vector<std::uint8_t>>(&value);if(!bytes)refuse("managed peer expected bytes");return *bytes;
}
bool managed_peer_boolean(const column_value_t& value){const auto n=integer(value);if(n!=0&&n!=1)refuse("managed peer boolean shape");return n==1;}
struct managed_peer_decoded {
    ordinary_managed_ipc_route_description route;
    ordinary_managed_contribution_observation input;
    std::string source_schema,recipient_schema,policy_seal,principal,group,nonce;
    std::uint64_t device=0,inode=0;
    std::int64_t request=0,expires=0;
};
std::uint64_t managed_peer_unsigned(const std::string& value){
    if(value.empty()||value.size()>20||(value.size()>1&&value[0]=='0'))refuse("managed peer unsigned scalar");
    std::uint64_t out=0;for(const char c:value){if(c<'0'||c>'9'||out>(UINT64_MAX-static_cast<unsigned>(c-'0'))/10)refuse("managed peer unsigned overflow");out=out*10+static_cast<unsigned>(c-'0');}return out;
}
managed_peer_decoded managed_peer_parse(std::span<const std::uint8_t> bytes,budget& retained){
    const auto row=managed_peer_decode(bytes,retained);if(row.size()!=33||text(row[0])!="managed-peer-observation-v1")refuse("managed peer request shape");
    // One full decoded row already owns the copied fields. Charge a second
    // typed retained representation before copying any string/vector from it.
    original_copy_charge(row,retained);
    managed_peer_decoded result;auto& d=result.route;auto& v=result.input;
    d.route_key=text(row[1]);d.generation=text(row[2]);d.source_member=text(row[3]);d.recipient_member=text(row[4]);
    d.policy_revision=integer(row[5]);d.channel=text(row[6]);d.narrowing_emits_removals=managed_peer_boolean(row[7]);
    const auto models=managed_peer_decode(managed_peer_blob(row[8]),retained,3);
    retained.add(sizeof(std::vector<std::string>)+models.size()*sizeof(std::string));
    for(const auto& model:models){retained.add(text(model).size());d.models.push_back(text(model));}
    const auto filters=managed_peer_decode(managed_peer_blob(row[9]),retained,8);
    if(filters.empty())refuse("managed peer filter shape");
    if(managed_peer_boolean(filters[0])){
        if(filters.size()<2||integer(filters[1])<0||integer(filters[1])>3||filters.size()!=2+static_cast<std::size_t>(integer(filters[1]))*2)refuse("managed peer filter count");
        d.filter.emplace();retained.add(sizeof(std::vector<sync_filter_entry>)+3*sizeof(sync_filter_entry));
        for(std::size_t at=2;at<filters.size();at+=2){
            retained.add(text(filters[at]).size());sync_filter_entry entry;entry.table_name=text(filters[at]);
            if(!std::holds_alternative<std::nullptr_t>(filters[at+1])){retained.add(text(filters[at+1]).size());entry.where_clause=text(filters[at+1]);}
            d.filter->push_back(std::move(entry));
        }
    }else if(filters.size()!=1)refuse("managed peer null filter tail");
    result.source_schema=text(row[10]);result.recipient_schema=text(row[11]);result.policy_seal=text(row[12]);result.principal=text(row[13]);result.group=text(row[14]);
    result.device=managed_peer_unsigned(text(row[15]));result.inode=managed_peer_unsigned(text(row[16]));
    v.source=text(row[17]);v.epoch=integer(row[18]);v.scope=text(row[19]);v.schema=text(row[20]);v.digest=text(row[21]);v.cut=integer(row[22]);
    v.record_seal=text(row[23]);v.table=text(row[24]);v.row_gid=text(row[25]);v.original_gid=text(row[26]);
    v.envelope=managed_peer_blob(row[27]);v.descriptor=managed_peer_blob(row[28]);v.projection=managed_peer_blob(row[29]);
    result.nonce=text(row[30]);result.request=integer(row[31]);result.expires=integer(row[32]);
    (void)d.copy_charge();
    if(d.recipient_member!="memory.sqlite"||d.source_member=="memory.sqlite"||result.request<=0||result.nonce.size()!=32||
        result.principal.empty()||result.principal.size()>4096||result.group.size()>4096||result.source_schema!=result.recipient_schema)
        refuse("managed peer upstream-only request shape");
    const std::vector<std::string> personal{"Edge","Memory","SyncConfig"},group{"Memory"};
    if((result.group.empty()&&d.models!=personal)||(!result.group.empty()&&d.models!=group))refuse("managed peer exact upstream product models");
    ipc_observation_input(v,retained,ipc_observation_encoding::retained_original_association);
    (void)managed_peer_deadline(result.expires);return result;
}
void managed_peer_capture_live(const std::shared_ptr<const ordinary_managed_product_state>& product,
    const managed_upstream_evidence& capture){
    require_managed_product_live(product);
    if(!capture.wss_receipt||!capture.wss_receipt->current())refuse("managed peer actual accepted WSS retired");
    if(capture.eligibility!=managed_upstream_eligibility::matched&&capture.eligibility!=managed_upstream_eligibility::unfiltered)
        refuse("managed peer visibility requires a separate removal decision");
}
}
struct ordinary_managed_peer_request::implementation {
    const std::uint64_t creator=managed_pair_creator();
    std::shared_ptr<ordinary_managed_peer_owner> peer;
    std::shared_ptr<const ordinary_managed_ipc_local_route> route;
    std::unique_ptr<ordinary_managed_upstream_capture> capture_owner;
    ordinary_managed_upstream_capture& capture()const{return *capture_owner;}
    const ordinary_launch::deadline deadline;
    const std::int64_t request;
    std::vector<std::uint8_t> bytes;
    std::string digest,obligation,expected_receipt;
    bool durable=false,authorized=false,settled=false;
    implementation(ordinary_launch::deadline end,std::int64_t sequence):deadline(end),request(sequence){}

};
ordinary_managed_peer_request::ordinary_managed_peer_request(std::unique_ptr<implementation> value):impl_(std::move(value)){}
ordinary_managed_peer_request::~ordinary_managed_peer_request(){
    auto* held=impl_.release();if(!held||held->creator!=managed_pair_creator())return;
    held->route.reset();if(held->creator!=managed_pair_creator())return;
    held->capture_owner.reset();if(held->creator!=managed_pair_creator())return;
    held->peer.reset();if(held->creator!=managed_pair_creator())return;
    delete held; // all potentially foreign owners are empty
}
struct ordinary_managed_peer_staged::implementation {
    ordinary_resource_reservation<true> reservation; // payload destroyed first
    const std::uint64_t creator=managed_pair_creator();
    std::shared_ptr<ordinary_managed_peer_owner> peer;
    std::int64_t request=0;
    std::size_t expected_bytes=0;
    std::string digest;
    std::vector<std::uint8_t> bytes;
    bool ready=false,authorized=false,applied=false,cleanup_reconciled=false;
    ordinary_launch::deadline deadline; // set by real BEGIN before any body byte
    std::optional<managed_peer_decoded> decoded;
    ordinary_managed_contribution_observation_result result;
};
ordinary_managed_peer_staged::ordinary_managed_peer_staged(std::unique_ptr<implementation> value):impl_(std::move(value)){}
ordinary_managed_peer_staged::~ordinary_managed_peer_staged(){
    auto* held=impl_.release();if(!held||held->creator!=managed_pair_creator())return;
    held->peer.reset();if(held->creator!=managed_pair_creator())return;
    delete held; // plain payloads then unchanged receive reservation
}

std::unique_ptr<ordinary_managed_peer_request> ordinary_export_access::make_managed_peer_request(
    const std::shared_ptr<ordinary_managed_peer_owner>& peer,const std::shared_ptr<const ordinary_managed_ipc_local_route>& route,
    const std::string& scope,const std::string& gid,std::int64_t sequence,ordinary_launch::deadline end){
    if(!peer||!route||sequence<=0)refuse("managed peer actual producer missing");
    peer->validate(end);if(!peer->facts().local_is_daemon)refuse("only actual paired daemon issues upstream observations");
    route->validate_lifetime();
    const auto& description=route->description();
    if(description.recipient_member!="memory.sqlite"||description.generation!=peer->impl_->generation||
        &route->recipient_owner()!=peer->impl_->owner.get()||&route->recipient_profile()!=peer->impl_->profile.get())
        refuse("managed peer route does not retain its actual primary");
    auto capture=capture_managed_upstream_until(*route,scope,gid,end);
    if(!capture.impl_||capture.impl_->product!=route->product_authority())refuse("managed peer capture issuer changed");
    managed_peer_capture_live(route->product_authority(),capture.impl_->evidence);
    const auto& policy=route->product_policy();
    if(description.models.size()>3||description.filter&&description.filter->size()>3)refuse("managed peer product metadata bound");
    budget retained{wire_limits};retained.add(managed_peer_metadata_bytes*2+sizeof(ordinary_managed_peer_request::implementation)+sizeof(ordinary_managed_upstream_capture));
    auto input=observation_from_managed_capture(capture.impl_->evidence,retained);
    ipc_observation_input(input,retained,ipc_observation_encoding::retained_original_association);
    auto expected=ipc_observation_row(peer->impl_->physical,description,input,retained);
    retained.add(64);const auto expected_receipt=text(expected[21]);
    // Empty wrapper/capsule exists before acquiring callback-capable owners
    // or performing a fallible allocation/owned SQL turn.
    auto result=std::unique_ptr<ordinary_managed_peer_request>(new ordinary_managed_peer_request(
        std::make_unique<ordinary_managed_peer_request::implementation>(end,sequence)));
    auto& state=*result->impl_;state.peer=peer;state.route=route;
    state.capture_owner=std::make_unique<ordinary_managed_upstream_capture>(std::move(capture));
    state.expected_receipt=expected_receipt;
    values obligation;
    managed_peer_capture_live(route->product_authority(),state.capture().impl_->evidence);peer->validate(end);
    const auto& captured=state.capture().impl_->evidence;auto& source=route->source_owner();
    const auto settled=recovery_writer_access::ordinary_owned_write(source,[&](database& writer){
        validate_managed_profile_until(route->source_profile(),source,writer,end);validate_managed_product_policy(*route,source,writer);
        const auto fresh=read_managed_upstream(writer,scope,gid,source.recovery_schemas_,retained);
        if(fresh.exact_mapping!=captured.exact_mapping||fresh.exact_audit!=captured.exact_audit||fresh.receipt_context!=captured.receipt_context||
           fresh.device!=captured.device||fresh.inode!=captured.inode||
           managed_current_eligibility(writer,description,fresh.original,retained)!=captured.eligibility)
            refuse("managed peer pending original or policy changed");
        obligation=peer_retry_record(writer,state.capture().impl_->route_policy,captured.exact_mapping,
            peer->impl_->physical,state.expected_receipt,input.projection,retained);
        validate_managed_profile_until(route->source_profile(),source,writer,end);
        managed_peer_capture_live(route->product_authority(),captured);managed_pair_deadline(end);
    });managed_clean_turn(settled);
    // Only this actual clean owned COMMIT permits a first transport byte and
    // later physical retirement while the logical observation remains pending.
    state.durable=true;
    retained.add(text(obligation[0]).size()+text(obligation[7]).size()+managed_peer_blob(obligation[8]).size());
    state.obligation=text(obligation[0]);state.expected_receipt=text(obligation[7]);
    // Reuse the exact originally observed receipt context across reconnect or
    // a later cut of the SAME guarded original. This is historical evidence,
    // not current authority; authorization above/below uses fresh actual WSS.
    input.projection=managed_peer_blob(obligation[8]);
    const auto replay=ipc_observation_row(peer->impl_->physical,description,input,retained);
    if(text(replay[21])!=state.expected_receipt)refuse("managed peer durable association differs from actual original");
    values models;for(const auto& model:description.models)models.emplace_back(model);
    values filters{static_cast<std::int64_t>(description.filter.has_value())};
    if(description.filter){filters.emplace_back(static_cast<std::int64_t>(description.filter->size()));for(const auto& f:*description.filter){filters.emplace_back(f.table_name);if(f.where_clause)filters.emplace_back(*f.where_clause);else filters.emplace_back(nullptr);}}
    auto encoded_models=encode(models,retained),encoded_filters=encode(filters,retained);
    retained.add(input.envelope.size()+input.descriptor.size()+input.projection.size()+33*sizeof(column_value_t));
    values row{std::string("managed-peer-observation-v1"),description.route_key,description.generation,description.source_member,description.recipient_member,
        description.policy_revision,description.channel,static_cast<std::int64_t>(description.narrowing_emits_removals),std::move(encoded_models),std::move(encoded_filters),
        policy.source_schema,policy.recipient_schema,policy.policy_seal,policy.principal,policy.group,
        std::to_string(peer->impl_->physical.device),std::to_string(peer->impl_->physical.inode),
        input.source,input.epoch,input.scope,input.schema,input.digest,input.cut,input.record_seal,input.table,input.row_gid,input.original_gid,
        std::move(input.envelope),std::move(input.descriptor),std::move(input.projection),managed_peer_nonce(peer->facts()),sequence,managed_peer_time(end)};
    (void)encoded_size(row,managed_peer_request_bytes);
    state.bytes=encode(row,retained);retained.add(64);state.digest=picosha2::hash256_hex_string(state.bytes);
    managed_peer_capture_live(route->product_authority(),captured);peer->validate(end);return result;

}
std::unique_ptr<ordinary_managed_peer_request> ordinary_export_access::next_managed_peer_request(
    const std::shared_ptr<ordinary_managed_peer_owner>& peer,const std::shared_ptr<const ordinary_managed_ipc_local_route>& route,
    std::string& after_scope,std::string& after_gid,std::int64_t sequence,ordinary_launch::deadline end){
    route->validate_lifetime();peer->validate(end);const auto& description=route->description();
    if(description.recipient_member!="memory.sqlite")refuse("managed peer pump only accepts actual upstream routes");
    std::string scope,gid;budget charged{wire_limits};auto& source=route->source_owner();
    const auto selected=recovery_writer_access::ordinary_owned_write(source,[&](database& writer){
        validate_managed_profile_until(route->source_profile(),source,writer,end);validate_managed_product_policy(*route,source,writer);
        if(wire_storage(ordinary_export_storage_access::borrow(writer),charged)){
            original_key_charge(after_scope.empty()?"cursor":after_scope,after_gid.empty()?"cursor":after_gid,charged);
            const auto keys=read(ordinary_export_storage_access::borrow(writer),
                "SELECT scope,gid FROM main._lattice_ordered_wire_original_v2 WHERE scope>? OR (scope=? AND gid>? COLLATE NOCASE) ORDER BY scope,gid COLLATE NOCASE LIMIT 1",
                {after_scope,after_scope,after_gid},1,charged);
            if(!keys.empty()){original_key_charge(text(keys[0][0]),text(keys[0][1]),charged);scope=text(keys[0][0]);gid=text(keys[0][1]);}
        }
        validate_managed_profile_until(route->source_profile(),source,writer,end);managed_pair_deadline(end);
    });managed_clean_turn(selected);route->validate_lifetime();
    if(scope.empty()){after_scope.clear();after_gid.clear();return {};}
    // Empty/fresh/cut-zero acceptance and temporarily absent current WSS are
    // idle states, never source authority reconstructed from a stored mapping.
    const auto accepted=try_managed_product_wss_receipt(route->product_authority(),route->source_profile(),scope);
    if(!accepted)return {};
    bool already=false,eligible=false;
    {ordinary_resource_reservation<false> temporary;
    const auto checked=recovery_writer_access::ordinary_owned_write(source,[&](database& writer){
        validate_managed_profile_until(route->source_profile(),source,writer,end);validate_managed_product_policy(*route,source,writer);
        original_context_cache context(writer,charged);const auto mapping=original_record(writer,scope,gid,charged,context);
        if(mapping.empty())refuse("managed peer selected original changed");
        const auto original=original_decode(writer,mapping,charged,true,context);
        const auto& accepted_binding=context.scope(scope).binding;
        if(accepted_binding.size()!=9||text(accepted_binding[1])!=accepted->channel||text(accepted_binding[2])!=accepted->source||
            integer(accepted_binding[3])!=accepted->source_epoch||picosha2::hash256_hex_string(text(accepted_binding[6]))!=accepted->schema_digest||
            integer(accepted_binding[7])!=accepted->cut||text(accepted_binding[8])!=accepted->receipt_digest)
            refuse("managed peer selection mapping not current WSS acceptance");
        const auto& tables=route->product_policy().source_tables;
        if(std::find(tables.begin(),tables.end(),original.local_view.table_name)!=tables.end()){
            const auto observed=managed_current_eligibility(writer,description,original,charged);
            eligible=observed==managed_upstream_eligibility::matched||observed==managed_upstream_eligibility::unfiltered;
        }
        // A filter miss/absent current row remains a separate policy/deletion
        // observation. This pump issues no privacy/visibility-removal grant
        // and never changes the original or any delivery/repair flag.

        if(peer_retry_storage(ordinary_export_storage_access::borrow(writer),charged)){
            const auto policy=ipc_route_policy(description,charged);original_key_charge(policy,scope,charged);
            const auto found=read(ordinary_export_storage_access::borrow(writer),std::string("SELECT ")+peer_retry_columns+
                " FROM main._lattice_ordered_ipc_retry_v1 WHERE scope=? AND gid=? AND original=? AND policy=? AND recipient_device=? AND recipient_inode=? AND observed=1 LIMIT 1",
                {scope,gid,mapping[13],policy,std::to_string(peer->impl_->physical.device),std::to_string(peer->impl_->physical.inode)},1,charged);
            if(!found.empty()){peer_retry_verify(found[0],charged);already=true;}
        }
        context.finish();if(!accepted->current())refuse("managed peer selection WSS retired");
        validate_managed_profile_until(route->source_profile(),source,writer,end);managed_pair_deadline(end);
    });managed_clean_turn(checked);}
    std::unique_ptr<ordinary_managed_peer_request> result;
    if(eligible&&!already)result=make_managed_peer_request(peer,route,scope,gid,sequence,end);
    // Advancing this in-memory enumeration key never settles an obligation.
    // A process restart begins at the first actual key; guarded observed rows
    // suppress only the exact original/policy/recipient association.
    after_scope=std::move(scope);after_gid=std::move(gid);return result;
}

void ordinary_export_access::authorize_managed_peer_request(ordinary_managed_peer_request& request){
    if(!request.impl_||request.impl_->creator!=managed_pair_creator())refuse("managed peer request inherited or absent");
    auto& actual=*request.impl_;if(!actual.durable||actual.obligation.empty()||actual.authorized||actual.settled)refuse("managed peer request authorization consumed");
    auto& route=*actual.route;actual.peer->validate(actual.deadline);managed_peer_capture_live(route.product_authority(),actual.capture().impl_->evidence);
    const auto& captured=actual.capture().impl_->evidence;budget retained{wire_limits};auto& source=route.source_owner();
    const auto settled=recovery_writer_access::ordinary_owned_write(source,[&](database& writer){
        validate_managed_profile_until(route.source_profile(),source,writer,actual.deadline);validate_managed_product_policy(route,source,writer);
        const auto fresh=read_managed_upstream(writer,captured.original.stamp.scope,captured.original.gid,source.recovery_schemas_,retained);
        if(fresh.exact_mapping!=captured.exact_mapping||fresh.exact_audit!=captured.exact_audit||fresh.receipt_context!=captured.receipt_context||
           fresh.device!=captured.device||fresh.inode!=captured.inode||
           managed_current_eligibility(writer,route.description(),fresh.original,retained)!=captured.eligibility)
            refuse("managed peer original or eligibility changed before authorization");
        const auto obligation=peer_retry_read(writer,actual.obligation,retained);
        if(text(obligation[4])!=captured.original.record_seal||text(obligation[7])!=actual.expected_receipt)
            refuse("managed peer durable authorization association changed");
        validate_managed_profile_until(route.source_profile(),source,writer,actual.deadline);
        managed_peer_capture_live(route.product_authority(),captured);managed_pair_deadline(actual.deadline);
    });managed_clean_turn(settled);
    // Actual final admission after recipient readiness. Queueing or delivering
    // the earlier body gave no write authority. Later source revocation cannot
    // retroactively undo an already-admitted remote SQL transaction.
    managed_peer_capture_live(route.product_authority(),captured);actual.peer->validate(actual.deadline);actual.authorized=true;
}
ordinary_managed_contribution_observation_result ordinary_export_access::apply_managed_peer_request(
    ordinary_managed_peer_staged& request,ordinary_launch::deadline operation_end){
    if(!request.impl_||request.impl_->creator!=managed_pair_creator())refuse("managed peer stage inherited or absent");
    auto& actual=*request.impl_;if(!actual.ready||!actual.authorized||actual.applied||!actual.decoded||!actual.peer)
        refuse("managed peer lacks exact one-shot producer authorization");
    auto& peer=*actual.peer;auto& packet=*actual.decoded;
    if(peer.facts().local_is_daemon||packet.nonce!=managed_peer_nonce(peer.facts())||packet.request!=actual.request)
        refuse("managed peer opposite image or request association changed");
    const auto end=std::min(operation_end,managed_peer_deadline(packet.expires));peer.validate(end);
    const auto& retained=*peer.impl_;
    if(packet.route.generation!=retained.generation||packet.route.recipient_member!=retained.member||
       packet.device!=retained.physical.device||packet.inode!=retained.physical.inode)
        refuse("managed peer request targets another physical member");
    budget charged{wire_limits};ordinary_managed_contribution_observation_result result;
    const auto settled=recovery_writer_access::ordinary_owned_write(*retained.owner,[&](database& writer){
        peer.validate_lifetime();original_view(writer,true);
        validate_managed_profile_until(*retained.profile,*retained.owner,writer,end);
        const auto physical=writer.physical_identity("main",{},true);if(!physical||*physical!=retained.physical)refuse("managed peer write physical changed");
        const auto binding=retained.profile->impl_->admission->ipc_member_binding(retained.owner->config_.path,physical->device,physical->inode,end);
        if(binding.first!=packet.route.recipient_member||binding.second!=packet.route.generation)refuse("managed peer write cohort changed");
        ordinary_export_frame declared;declared.wire_profile=true;
        capture_projected_tables(ordinary_export_storage_access::borrow(writer),retained.owner->recovery_schemas_,[](auto& out,const auto& property){lattice_db::serialize_property_for_fingerprint(out,property);},
            packet.route.models,declared,charged,false,nullptr,nullptr);
        if(declared.schema!=packet.recipient_schema||std::none_of(declared.tables.begin(),declared.tables.end(),[&](const auto& t){return t.name==packet.input.table;}))
            refuse("managed peer actual recipient catalog differs");
        // Reconstruct/check the original domain and raw field/name intent
        // from its exact bytes against the recipient's actual catalog. This
        // validates the delegated assertion; it does not mint WSS authority.
        const auto& input=packet.input;
        charged.add(sizeof(ordinary_wire_original_record)+input.envelope.size()+input.descriptor.size()+
            input.scope.size()+input.source.size()+input.schema.size()+input.digest.size()+input.original_gid.size());
        ordinary_wire_original_record original;
        original.stamp={input.scope,input.source,input.schema,input.digest,input.epoch,input.cut};
        original.gid=input.original_gid;original.envelope=input.envelope;original.descriptor=input.descriptor;original.descriptor_codec=1;
        original.local_view=decode_ordinary_v2_retained_entry(original.envelope,[&](std::size_t n){charged.add(n);});
        if(original.local_view.global_id!=input.original_gid||original.local_view.global_row_id!=input.row_gid||original.local_view.table_name!=input.table)
            refuse("managed peer original identity differs from request");
        (void)canonical_descriptor(original,retained.owner->recovery_schemas_,charged);
        const auto association=managed_peer_decode(input.projection,charged,14);
        if(association.size()!=14||text(association[0])!="managed-accepted-original-association-v1"||
            integer(association[3])<=0||integer(association[8])<input.cut||
            integer(association[12])!=input.cut||text(association[13])!=input.record_seal||
            (integer(association[11])!=1&&integer(association[11])!=2))refuse("managed peer accepted association differs");
        (void)integer(association[4]); // raw source row-id, without inventing positivity
        (void)managed_peer_unsigned(text(association[1]));(void)managed_peer_unsigned(text(association[2]));
        (void)managed_peer_boolean(association[5]);(void)managed_peer_boolean(association[10]);
        original_digest(text(association[6]));original_digest(text(association[9]));
        if(text(association[7]).empty()||text(association[7]).size()>4096)refuse("managed peer accepted channel bound");
        // No received predicate is executed. Its exact policy fingerprint is
        // only associated with this trusted producer's passive observation.
        // Own-source enrollment cannot restamp the upstream source/receipt.
        result=ipc_observation_record(writer,packet.route,packet.input,charged,ipc_observation_encoding::retained_original_association);
        validate_managed_profile_until(*retained.profile,*retained.owner,writer,end);peer.validate_lifetime();managed_pair_deadline(end);
    });managed_clean_turn(settled);peer.validate(end);
    actual.result=result;actual.applied=true;return result;
}

void ordinary_export_access::settle_managed_peer_request(ordinary_managed_peer_request& request,
    const std::string& receipt,ordinary_launch::deadline operation_end){
    if(!request.impl_||request.impl_->creator!=managed_pair_creator())refuse("managed peer result inherited");
    auto& actual=*request.impl_;
    if(!actual.durable||!actual.authorized||actual.settled||receipt!=actual.expected_receipt)
        refuse("managed peer result differs from durable exact observation");
    const auto end=std::min(operation_end,actual.deadline);managed_pair_deadline(end);
    auto& route=*actual.route;auto& source=route.source_owner();budget charged{wire_limits};
    const auto settled=recovery_writer_access::ordinary_owned_write(source,[&](database& writer){
        validate_managed_profile_until(route.source_profile(),source,writer,end);
        peer_retry_observed(writer,actual.obligation,receipt,charged);
        validate_managed_profile_until(route.source_profile(),source,writer,end);managed_pair_deadline(end);
    });managed_clean_turn(settled);actual.settled=true;
}
void ordinary_export_access::reconcile_managed_peer_stage(ordinary_managed_peer_staged& request,ordinary_launch::deadline end){
    if(!request.impl_||request.impl_->creator!=managed_pair_creator())refuse("managed peer cleanup stage inherited");
    auto& actual=*request.impl_;if(!actual.authorized||actual.applied||actual.cleanup_reconciled)return;
    if(!actual.peer||!actual.decoded)refuse("managed peer cleanup lost exact packet");
    const auto& owner=*actual.peer->impl_;auto& packet=*actual.decoded;budget charged{wire_limits};
    // Physical cleanup after stop does not acquire new producer authority. The
    // real retained recipient profile/view settles uncertain SQL before any
    // descriptor or owner release; absence is not a successful observation.
    const auto settled=recovery_writer_access::ordinary_owned_write(*owner.owner,[&](database& writer){
        original_view(writer,true);validate_managed_profile_until(*owner.profile,*owner.owner,writer,end);
        const auto physical=writer.physical_identity("main",{},true);
        if(!physical||*physical!=owner.physical)refuse("managed peer cleanup physical owner changed");
        ipc_observation_input(packet.input,charged,ipc_observation_encoding::retained_original_association);
        const auto expected=ipc_observation_row(*physical,packet.route,packet.input,charged);
        if(ipc_observation_storage(ordinary_export_storage_access::borrow(writer),charged)){
            original_key_charge(text(expected[21]),text(expected[21]),charged);
            const auto found=read(ordinary_export_storage_access::borrow(writer),std::string("SELECT ")+ipc_observation_columns+
                " FROM main._lattice_ordered_ipc_observation_v1 WHERE seal=?",{expected[21]},1,charged);
            if(!found.empty()){verify_ipc_observation(found[0],charged);if(found[0]!=expected)refuse("managed peer cleanup observation changed");}
        }
        validate_managed_profile_until(*owner.profile,*owner.owner,writer,end);managed_pair_deadline(end);
    });managed_clean_turn(settled);actual.cleanup_reconciled=true;
}
