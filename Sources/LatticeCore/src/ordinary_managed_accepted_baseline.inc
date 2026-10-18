// Include after the managed product/WSS/profile and shared original helpers.
// No generic query callback, caller schema, decoded receipt or snapshot DTO
// enters this reader. The caller owns the real main WRITE for every query.
namespace {
values managed_baseline_decode(std::span<const std::uint8_t> input,
    std::size_t columns,budget& charged) {
    std::size_t at=0;
    const auto number=[&](){
        if(input.size()-at<8)refuse("managed baseline truncated scalar");
        std::uint64_t n=0;for(unsigned i=0;i<8;++i)n=(n<<8)|input[at++];return n;
    };
    const auto count=number();
    if(count!=columns||!count||count>charged.limits.columns)
        refuse("managed baseline logical column count changed");
    charged.add(sizeof(values)+columns*sizeof(column_value_t));
    values row;row.reserve(columns);
    for(std::size_t n=0;n<columns;++n) {
        if(at==input.size())refuse("managed baseline missing scalar tag");
        const auto tag=input[at++];const auto value=number();charged.cell();
        if(tag==0) {if(value)refuse("managed baseline noncanonical null");row.emplace_back(nullptr);}
        else if(tag==1)row.emplace_back(std::bit_cast<std::int64_t>(value));
        else if(tag==2) {
            const auto real=std::bit_cast<double>(value);
            if(!std::isfinite(real))refuse("managed baseline nonfinite real");row.emplace_back(real);
        } else if(tag==3||tag==4) {
            if(value>input.size()-at)refuse("managed baseline scalar length changed");
            // The SQLite BLOB remains borrowed until this row finishes.
            // Charge the one retained decoded copy before allocation.
            charged.add(static_cast<std::size_t>(value)+1);
            if(tag==3)row.emplace_back(std::string(reinterpret_cast<const char*>(input.data()+at),static_cast<std::size_t>(value)));
            else row.emplace_back(std::vector<std::uint8_t>(input.begin()+static_cast<std::ptrdiff_t>(at),input.begin()+static_cast<std::ptrdiff_t>(at+value)));
            at+=static_cast<std::size_t>(value);
        } else refuse("managed baseline unknown scalar tag");
    }
    if(at!=input.size())refuse("managed baseline trailing row bytes");return row;
}
// Stream each actual guarded BLOB once. Hash its original bytes before decode;
// do not duplicate the entire baseline just to establish its existing digest.
std::string managed_baseline_rows(sqlite3* handle,ordinary_export_frame& image,budget& charged,ordinary_launch::deadline end) {
    managed_pair_deadline(end);
    original_key_charge(image.scope,"accepted-baseline",charged);
    const values args{image.scope};
    const auto count=read(handle,"SELECT COUNT(*) FROM main._lattice_ordered_receiver_baseline_v1 WHERE scope=?",args,1,charged);
    if(count.size()!=1||integer(count[0][0])<0||static_cast<std::uint64_t>(integer(count[0][0]))>charged.limits.rows)
        refuse("managed baseline row bound");
    const auto total=static_cast<std::size_t>(integer(count[0][0]));
    fingerprint digest;digest.bytes(image.scope);digest.number(total);
    database::record_statement();sqlite3_stmt* raw=nullptr;
    const auto rc=::lattice::detail::store_physical_prepare_v2(handle,"SELECT model,gid,value FROM main._lattice_ordered_receiver_baseline_v1 WHERE scope=? ORDER BY model,gid",-1,&raw,nullptr);
    std::unique_ptr<sqlite3_stmt,decltype(&::lattice::detail::store_physical_finalize)> statement(raw,&::lattice::detail::store_physical_finalize);
    if(rc!=SQLITE_OK||!raw||sqlite3_bind_parameter_count(raw)!=1||sqlite3_column_count(raw)!=3||!sqlite3_stmt_readonly(raw)||
       sqlite3_bind_text(raw,1,image.scope.data(),static_cast<int>(image.scope.size()),SQLITE_STATIC)!=SQLITE_OK)
        refuse("managed baseline retained statement refused");
    int step=SQLITE_OK;std::size_t seen=0;
    for(;;) {
        managed_pair_deadline(end);step=::lattice::detail::store_physical_step(raw);if(step!=SQLITE_ROW)break;
        if(seen==total)refuse("managed baseline gained a row inside retained view");
        if(sqlite3_column_type(raw,0)!=SQLITE_TEXT||sqlite3_column_type(raw,1)!=SQLITE_TEXT||sqlite3_column_type(raw,2)!=SQLITE_BLOB)
            refuse("managed baseline exact storage changed");
        const auto model_bytes=sqlite3_column_bytes(raw,0),gid_bytes=sqlite3_column_bytes(raw,1),value_bytes=sqlite3_column_bytes(raw,2);
        if(model_bytes<=0||model_bytes>256||gid_bytes<=0||gid_bytes>4096||value_bytes<8||
           static_cast<std::size_t>(value_bytes)>charged.limits.bytes)
            refuse("managed baseline borrowed scalar bound");
        const auto* model_ptr=sqlite3_column_text(raw,0);const auto* gid_ptr=sqlite3_column_text(raw,1);
        const auto* value_ptr=static_cast<const std::uint8_t*>(sqlite3_column_blob(raw,2));
        if(!model_ptr||!gid_ptr||!value_ptr)refuse("managed baseline borrowed storage unavailable");
        const std::string_view model(reinterpret_cast<const char*>(model_ptr),static_cast<std::size_t>(model_bytes));
        const std::string_view gid(reinterpret_cast<const char*>(gid_ptr),static_cast<std::size_t>(gid_bytes));
        const std::span<const std::uint8_t> payload(value_ptr,static_cast<std::size_t>(value_bytes));
        if(model.find('\0')!=std::string_view::npos||gid.find('\0')!=std::string_view::npos)
            refuse("managed baseline row identity spelling");
        const auto found=std::lower_bound(image.tables.begin(),image.tables.end(),model,[](const auto& table,std::string_view name){return table.name<name;});
        if(found==image.tables.end()||found->name!=model)refuse("managed baseline table outside accepted declaration");
        // Exactly fingerprint::scalar(TEXT,TEXT,BLOB) used by actual install.
        digest.number(3);digest.bytes(model);digest.number(3);digest.bytes(gid);digest.number(4);digest.bytes(payload);
        auto decoded=managed_baseline_decode(payload,found->columns.size(),charged);
        if(text(decoded[0])!=gid)refuse("managed baseline key differs from retained row");
        found->rows.push_back(std::move(decoded));++seen;
    }
    if(step!=SQLITE_DONE||seen!=total)refuse("managed baseline scan did not settle exactly");
    if(::lattice::detail::store_physical_finalize(statement.release())!=SQLITE_OK)refuse("managed baseline statement finalization failed");
    managed_pair_deadline(end);
    image.rows=total;charged.add(64);return digest.finish();
}
struct managed_baseline_observation {
    ordinary_export_frame image;
    std::string cut_digest,baseline_digest;
    values exact_binding;
};
void read_managed_accepted_baseline(database& writer,const recovery_owner_schema& catalog,
    const std::function<void(std::ostringstream&,const property_descriptor&)>& serialize,
    const std::string& scope,managed_baseline_observation& output,budget& charged,ordinary_launch::deadline end) {
    managed_pair_deadline(end);
    original_view(writer,true);original_context_cache context(writer,charged);
    const auto& binding=context.scope(scope).binding;
    charged.add(scope.size()+charged.limits.models*sizeof(std::string));
    const auto names=scope_names(scope,charged.limits.models);
    // Include all actual source-scope logical tables to prove its complete
    // baseline digest. Do not infer a recipient partition from these names.
    output.image.wire_profile=true;ordinary_schema_projection projection;ordinary_schema_metadata metadata;
    capture_projected_tables(ordinary_export_storage_access::borrow(writer),catalog,serialize,names,output.image,charged,false,&projection,&metadata);
    if(output.image.scope!=scope||output.image.schema!=text(binding[6]))
        refuse("managed baseline source declaration differs from acceptance");
    original_copy_charge(binding,charged);output.exact_binding=binding;
    charged.add(text(binding[2]).size()+text(binding[4]).size()+text(binding[5]).size()+text(binding[8]).size());
    output.image.source=text(binding[2]);output.image.epoch=integer(binding[3]);output.image.cut=integer(binding[7]);
    output.image.device=text(binding[4]);output.image.inode=text(binding[5]);output.cut_digest=text(binding[8]);
    if(!canonical_uuid(output.image.source)||output.image.epoch<=0||output.image.cut<=0)
        refuse("managed baseline accepted coordinate changed");
    original_key_charge(scope,"baseline-frontier",charged);const values args{scope};
    const auto frontier=read(ordinary_export_storage_access::borrow(writer),"SELECT scope,source,epoch,cut,digest,device,inode,schema,baseline FROM main._lattice_ordered_receiver_v1 WHERE scope=?",args,1,charged);
    if(frontier.size()!=1)refuse("managed baseline actual frontier missing");
    constexpr std::array<std::size_t,8> indices{0,2,3,7,8,4,5,6};
    for(std::size_t n=0;n<indices.size();++n)if(frontier[0][n]!=binding[indices[n]])refuse("managed baseline frontier changed");
    original_digest(text(frontier[0][8]));
    output.baseline_digest=managed_baseline_rows(ordinary_export_storage_access::borrow(writer),output.image,charged,end);
    if(output.baseline_digest!=text(frontier[0][8]))refuse("managed baseline accepted row digest changed");
    validate_projected_values(output.image,projection,metadata,catalog,charged);
    if(read(ordinary_export_storage_access::borrow(writer),"SELECT scope,source,epoch,cut,digest,device,inode,schema,baseline FROM main._lattice_ordered_receiver_v1 WHERE scope=?",args,1,charged)!=frontier)
        refuse("managed baseline frontier changed before completion");
    context.finish(); // exact physical/schema/guards/receipt before actual COMMIT
    managed_pair_deadline(end);
}
}

struct ordinary_managed_accepted_baseline::implementation {
    struct record {
        const std::uint64_t creator=managed_pair_creator();
        std::optional<ordinary_resource_reservation<false>> reservation;
        std::shared_ptr<const ordinary_managed_ipc_local_route> route;
        std::shared_ptr<const ordinary_managed_wss_receipt> receipt;
        managed_baseline_observation observed;
    };
    struct dispose {
        void operator()(record* value)const noexcept {
            if(!value||value->creator!=managed_pair_creator())return;
            value->route.reset();if(value->creator!=managed_pair_creator())return;
            value->receipt.reset();if(value->creator!=managed_pair_creator())return;
            value->observed={}; // plain values only, no product callbacks
            if(value->creator!=managed_pair_creator())return;
            value->reservation.reset();if(value->creator!=managed_pair_creator())return;
            delete value;
        }
    };
    // Empty guarded storage exists before reservation and actual owner moves.
    std::unique_ptr<record,dispose> held{new record};
};
ordinary_managed_accepted_baseline::ordinary_managed_accepted_baseline(std::unique_ptr<implementation> value):impl_(std::move(value)){}
ordinary_managed_accepted_baseline::~ordinary_managed_accepted_baseline()=default;
ordinary_managed_accepted_baseline::ordinary_managed_accepted_baseline(ordinary_managed_accepted_baseline&&)noexcept=default;
ordinary_managed_accepted_baseline& ordinary_managed_accepted_baseline::operator=(ordinary_managed_accepted_baseline&&)noexcept=default;
const ordinary_managed_accepted_baseline::implementation& ordinary_managed_accepted_baseline::checked()const {
    if(!impl_||!impl_->held||impl_->held->creator!=managed_pair_creator())refuse("managed baseline moved or inherited observation");return *impl_;
}
#define LATTICE_MANAGED_BASELINE_ACCESSOR(type,name,field) \
    type ordinary_managed_accepted_baseline::name()const{return checked().held->observed.field;}
LATTICE_MANAGED_BASELINE_ACCESSOR(const std::string&,source_id,image.source)
LATTICE_MANAGED_BASELINE_ACCESSOR(const std::string&,scope,image.scope)
LATTICE_MANAGED_BASELINE_ACCESSOR(const std::string&,schema,image.schema)
LATTICE_MANAGED_BASELINE_ACCESSOR(const std::string&,cut_digest,cut_digest)
LATTICE_MANAGED_BASELINE_ACCESSOR(const std::string&,baseline_digest,baseline_digest)
LATTICE_MANAGED_BASELINE_ACCESSOR(std::int64_t,epoch,image.epoch)
LATTICE_MANAGED_BASELINE_ACCESSOR(std::int64_t,cut,image.cut)
LATTICE_MANAGED_BASELINE_ACCESSOR(const std::vector<ordinary_export_table>&,tables,image.tables)
#undef LATTICE_MANAGED_BASELINE_ACCESSOR

std::optional<ordinary_managed_accepted_baseline> ordinary_export_access::capture_managed_accepted_baseline(
    const std::shared_ptr<const ordinary_managed_ipc_local_route>& route,const std::string& scope,ordinary_launch::deadline end) {
    if(!route)refuse("managed baseline actual route missing");route->validate_lifetime();managed_pair_deadline(end);
    ordinary_managed_accepted_baseline result(std::make_unique<ordinary_managed_accepted_baseline::implementation>());
    auto& held=*result.impl_->held;held.reservation.emplace();held.route=route;
    held.receipt=try_managed_product_wss_receipt(route->product_authority(),route->source_profile(),scope);
    // A fresh negotiated/empty cut-zero boundary has no accepted row baseline.
    // This is absence of state, not a fabricated empty complete projection.
    if(!held.receipt)return std::nullopt;
    auto& source=route->source_owner();budget charged{wire_limits};
    charged.add(sizeof(ordinary_managed_accepted_baseline::implementation)+sizeof(ordinary_managed_accepted_baseline::implementation::record));
    const auto settled=recovery_writer_access::ordinary_owned_write(source,[&](database& writer){
        validate_managed_profile_until(route->source_profile(),source,writer,end);validate_managed_product_policy(*route,source,writer);
        read_managed_accepted_baseline(writer,source.recovery_schemas_,[](auto& out,const auto& p){lattice_db::serialize_property_for_fingerprint(out,p);},scope,held.observed,charged,end);
        const auto& b=held.observed.exact_binding;const auto& receipt=*held.receipt;
        charged.add(64);
        if(!receipt.current()||scope!=receipt.scope||text(b[1])!=receipt.channel||text(b[2])!=receipt.source||integer(b[3])!=receipt.source_epoch||
           picosha2::hash256_hex_string(text(b[6]))!=receipt.schema_digest||integer(b[7])!=receipt.cut||text(b[8])!=receipt.receipt_digest)
            refuse("managed baseline is not current accepted WSS state");
        for(const auto& model:route->description().models)
            if(!std::binary_search(held.observed.image.models.begin(),held.observed.image.models.end(),model))
                refuse("managed baseline misses actual route declaration");
        validate_managed_profile_until(route->source_profile(),source,writer,end);validate_managed_product_policy(*route,source,writer);managed_pair_deadline(end);
    });managed_clean_turn(settled);
    route->validate_lifetime();managed_pair_deadline(end);
    if(!held.receipt->current())refuse("managed baseline WSS changed during off-lock completion");
    return std::optional<ordinary_managed_accepted_baseline>(std::move(result));
}

ordinary_managed_accepted_baseline ordinary_export_access::capture_managed_accepted_baseline_for_test(
    lattice_db& owner,const std::string& scope) {
    // Real locally seeded receiver state only. Does not issue managed profile,
    // product route, authenticated WSS receipt or model-install authority.
    ordinary_managed_accepted_baseline result(std::make_unique<ordinary_managed_accepted_baseline::implementation>());
    const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(10);
    auto& held=*result.impl_->held;held.reservation.emplace();budget charged{wire_limits};
    charged.add(sizeof(ordinary_managed_accepted_baseline::implementation)+sizeof(ordinary_managed_accepted_baseline::implementation::record));
    const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){
        read_managed_accepted_baseline(writer,owner.recovery_schemas_,[](auto& out,const auto& p){lattice_db::serialize_property_for_fingerprint(out,p);},scope,held.observed,charged,end);
    });managed_clean_turn(settled);return result;
}
