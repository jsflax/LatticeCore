// Include after accepted-baseline and actual profile/product/WSS/original
// helpers. This prerequisite has no socket or original-observation-v1 effect.
namespace {
constexpr std::size_t managed_baseline_stage_bytes=128*1024*1024;
constexpr std::size_t managed_baseline_stage_cuts=64;
constexpr std::size_t managed_baseline_stage_chunks=8192;
constexpr std::size_t managed_baseline_stage_members=8192*64;
void baseline_stream_add(std::size_t& size,std::size_t amount) {
    if(amount>managed_baseline_stage_bytes-size)refuse("baseline stream aggregate bound");size+=amount;
}
std::span<const std::uint8_t> baseline_text_bytes(std::string_view value) {
    return {reinterpret_cast<const std::uint8_t*>(value.data()),value.size()};
}
// Produces exactly the bytes consumed by the existing accepted baseline
// fingerprint, including empty-table/empty-baseline cuts. No encode(row),
// whole encoded baseline, or second decoded snapshot is allocated.
struct managed_baseline_transfer_state {
    const std::uint64_t creator=managed_pair_creator();
    std::optional<ordinary_managed_accepted_baseline> baseline;
    ordinary_launch::deadline end{};
    bool issued=false,poisoned=false;
    std::atomic<bool> turning{false},page_live{false};
    std::atomic<std::size_t> produced{0};
    std::size_t total=0,table=0,row=0,cell=0,part=0;
    enum class phase { scope_size,scope,rows,model_tag,model_size,model,gid_tag,gid_size,gid,
        row_tag,row_size,column_count,cell_tag,cell_number,cell_payload,done } next=phase::scope_size;
    std::array<std::uint8_t,8> number{};
    picosha2::hash256_one_by_one hash;
    std::string expected;
    const ordinary_export_table& current_table()const{return baseline->tables().at(table);}
    const values& current_row()const{return current_table().rows.at(row);}
    std::span<const std::uint8_t> numeric(std::uint64_t n) {
        for(std::size_t i=8;i;--i){number[i-1]=static_cast<std::uint8_t>(n);n>>=8;}return number;
    }
    std::span<const std::uint8_t> segment() {
        switch(next) {
        case phase::scope_size:return numeric(baseline->scope().size());
        case phase::scope:return baseline_text_bytes(baseline->scope());
        case phase::rows:{std::size_t count=0;for(const auto& t:baseline->tables())count+=t.rows.size();return numeric(count);}
        case phase::model_tag:case phase::gid_tag:return numeric(3);
        case phase::model_size:return numeric(current_table().name.size());
        case phase::model:return baseline_text_bytes(current_table().name);
        case phase::gid_size:return numeric(text(current_row().at(0)).size());
        case phase::gid:return baseline_text_bytes(text(current_row().at(0)));
        case phase::row_tag:return numeric(4);
        case phase::row_size:return numeric(encoded_size(current_row(),managed_baseline_stage_bytes));
        case phase::column_count:return numeric(current_row().size());
        case phase::cell_tag:number[0]=scalar_tag(current_row().at(cell));return {number.data(),1};
        case phase::cell_number:{const auto& v=current_row().at(cell);
            if(const auto* p=std::get_if<std::int64_t>(&v))return numeric(std::bit_cast<std::uint64_t>(*p));
            if(const auto* p=std::get_if<double>(&v))return numeric(std::bit_cast<std::uint64_t>(*p));
            if(const auto* p=std::get_if<std::string>(&v))return numeric(p->size());
            if(const auto* p=std::get_if<std::vector<std::uint8_t>>(&v))return numeric(p->size());
            return numeric(0);}
        case phase::cell_payload:{const auto& v=current_row().at(cell);
            if(const auto* p=std::get_if<std::string>(&v))return baseline_text_bytes(*p);
            if(const auto* p=std::get_if<std::vector<std::uint8_t>>(&v))return *p;
            return {};}
        case phase::done:return {};
        }refuse("baseline cursor phase");
    }
    void select_row() {
        while(table<baseline->tables().size()&&row==baseline->tables()[table].rows.size()){++table;row=0;}
        next=table==baseline->tables().size()?phase::done:phase::model_tag;
    }
    void advance() {
        part=0;
        switch(next) {
        case phase::scope_size:next=phase::scope;break;
        case phase::scope:next=phase::rows;break;
        case phase::rows:select_row();break;
        case phase::model_tag:next=phase::model_size;break;
        case phase::model_size:next=phase::model;break;
        case phase::model:next=phase::gid_tag;break;
        case phase::gid_tag:next=phase::gid_size;break;
        case phase::gid_size:next=phase::gid;break;
        case phase::gid:next=phase::row_tag;break;
        case phase::row_tag:next=phase::row_size;break;
        case phase::row_size:next=phase::column_count;break;
        case phase::column_count:cell=0;next=phase::cell_tag;break;
        case phase::cell_tag:next=phase::cell_number;break;
        case phase::cell_number:next=phase::cell_payload;break;
        case phase::cell_payload:if(++cell<current_row().size())next=phase::cell_tag;else{++row;select_row();}break;
        case phase::done:break;
        }
    }
    void initialize() {
        if(!baseline)refuse("baseline cursor missing accepted observation");
        total=0;baseline_stream_add(total,16);baseline_stream_add(total,baseline->scope().size());
        for(const auto& t:baseline->tables())for(const auto& r:t.rows){
            baseline_stream_add(total,48);baseline_stream_add(total,t.name.size());
            baseline_stream_add(total,text(r.at(0)).size());baseline_stream_add(total,encoded_size(r,managed_baseline_stage_bytes));
        }
        expected=baseline->baseline_digest();original_digest(expected);
    }
};
struct baseline_transfer_dispose {
    void operator()(managed_baseline_transfer_state* s)const noexcept {
        if(!s||s->creator!=managed_pair_creator())return;
        s->expected.clear();s->baseline.reset();if(s->creator!=managed_pair_creator())return;delete s;
    }
};
class baseline_turn final {
    managed_baseline_transfer_state& s_;
public:
    explicit baseline_turn(managed_baseline_transfer_state& s):s_(s) {
        if(s.creator!=managed_pair_creator())refuse("baseline inherited turn");
        bool idle=false;if(!s.turning.compare_exchange_strong(idle,true))refuse("baseline overlapping turn");
    }
    ~baseline_turn(){if(s_.creator==managed_pair_creator())s_.turning.store(false);}
};
// Checks the actual accepted cut again without decoding another snapshot.
// Called only from the actual owning WRITE, never from a metadata getter.
void baseline_exact_current(database& writer,const recovery_owner_schema& catalog,
    const std::function<void(std::ostringstream&,const property_descriptor&)>& serialize,
    const managed_baseline_observation& observed,budget& b,ordinary_launch::deadline end,bool verify_values) {
    original_view(writer,true);managed_pair_deadline(end);original_context_cache context(writer,b);
    if(context.scope(observed.image.scope).binding!=observed.exact_binding)refuse("baseline source cut is no longer current");
    ordinary_export_frame declaration;declaration.wire_profile=true;
    capture_projected_tables(ordinary_export_storage_access::borrow(writer),catalog,serialize,observed.image.models,declaration,b,false,nullptr,nullptr);
    if(declaration.scope!=observed.image.scope||declaration.schema!=observed.image.schema||declaration.tables.size()!=observed.image.tables.size())
        refuse("baseline source catalog changed");
    const auto frontier=read(ordinary_export_storage_access::borrow(writer),"SELECT source,epoch,cut,digest,schema,baseline FROM main._lattice_ordered_receiver_v1 WHERE scope=?",{observed.image.scope},1,b);
    const values exact{observed.image.source,observed.image.epoch,observed.image.cut,observed.cut_digest,observed.image.schema,observed.baseline_digest};
    if(frontier.size()!=1||frontier[0]!=exact)refuse("baseline accepted frontier changed");
    // Paging only publishes bytes from the already validated immutable held
    // observation. Its current binding/frontier/receipt/policy must stay exact.
    // Preparation and END additionally rehash source bytes; intermediate
    // passive DATA does not perform a whole-baseline scan for each fragment.
    if(!verify_values){context.finish();managed_pair_deadline(end);return;}
    // Rehash borrowed actual BLOBs too: a stored digest/guard inventory alone
    // cannot prove that guarded accepted rows stayed unchanged. No second
    // decoded snapshot or owning whole-row BLOB copy is retained here.
    b.add(8*1024+observed.image.scope.size());database::record_statement();sqlite3_stmt* raw=nullptr;
    const auto rc=::lattice::detail::store_physical_prepare_v2(ordinary_export_storage_access::borrow(writer),"SELECT model,gid,value FROM main._lattice_ordered_receiver_baseline_v1 WHERE scope=? ORDER BY model,gid",-1,&raw,nullptr);
    std::unique_ptr<sqlite3_stmt,decltype(&::lattice::detail::store_physical_finalize)> statement(raw,&::lattice::detail::store_physical_finalize);
    if(rc!=SQLITE_OK||!raw||sqlite3_bind_parameter_count(raw)!=1||sqlite3_column_count(raw)!=3||!sqlite3_stmt_readonly(raw)||
        sqlite3_bind_text(raw,1,observed.image.scope.data(),static_cast<int>(observed.image.scope.size()),SQLITE_STATIC)!=SQLITE_OK)
        refuse("baseline current borrowed scan unavailable");
    fingerprint digest;digest.bytes(observed.image.scope);digest.number(observed.image.rows);
    std::size_t rows=0,total=0;int step=SQLITE_OK;
    for(;;){managed_pair_deadline(end);step=::lattice::detail::store_physical_step(raw);if(step!=SQLITE_ROW)break;
        if(rows==observed.image.rows||sqlite3_column_type(raw,0)!=SQLITE_TEXT||sqlite3_column_type(raw,1)!=SQLITE_TEXT||sqlite3_column_type(raw,2)!=SQLITE_BLOB)
            refuse("baseline current exact storage changed");
        const auto model_size=sqlite3_column_bytes(raw,0),gid_size=sqlite3_column_bytes(raw,1),payload_size=sqlite3_column_bytes(raw,2);
        if(model_size<=0||model_size>256||gid_size<=0||gid_size>4096||payload_size<8||static_cast<std::size_t>(payload_size)>managed_baseline_stage_bytes)
            refuse("baseline current borrowed scalar bound");
        const auto* model=sqlite3_column_text(raw,0);const auto* gid=sqlite3_column_text(raw,1);const auto* payload=static_cast<const std::uint8_t*>(sqlite3_column_blob(raw,2));
        if(!model||!gid||!payload)refuse("baseline current borrowed storage missing");
        baseline_stream_add(total,static_cast<std::size_t>(payload_size));
        digest.number(3);digest.bytes(std::string_view(reinterpret_cast<const char*>(model),static_cast<std::size_t>(model_size)));
        digest.number(3);digest.bytes(std::string_view(reinterpret_cast<const char*>(gid),static_cast<std::size_t>(gid_size)));
        digest.number(4);digest.bytes(std::span<const std::uint8_t>(payload,static_cast<std::size_t>(payload_size)));++rows;
    }
    if(step!=SQLITE_DONE||rows!=observed.image.rows)refuse("baseline current scan did not settle");
    if(::lattice::detail::store_physical_finalize(statement.release())!=SQLITE_OK)refuse("baseline current borrowed finalize failed");
    if(digest.finish()!=observed.baseline_digest)refuse("baseline current accepted bytes changed");
    context.finish();managed_pair_deadline(end);
}
}

struct ordinary_managed_baseline_cursor::implementation {
    std::shared_ptr<managed_baseline_transfer_state> held{new managed_baseline_transfer_state,baseline_transfer_dispose{}};
};
struct ordinary_managed_baseline_page::implementation {
    struct record {
        const std::uint64_t creator=managed_pair_creator();
        std::shared_ptr<managed_baseline_transfer_state> owner;
        std::vector<std::uint8_t> payload;
        std::size_t offset=0;
    };
    struct dispose {void operator()(record* p)const noexcept {
        if(!p||p->creator!=managed_pair_creator())return;
        std::vector<std::uint8_t>{}.swap(p->payload);
        // Exactly one retained page per transfer; its last destructor retires
        // page ownership only after its bounded payload has been released.
        if(p->owner)p->owner->page_live.store(false);
        p->owner.reset();if(p->creator!=managed_pair_creator())return;delete p;
    }};
    std::unique_ptr<record,dispose> held{new record};
};
struct ordinary_managed_baseline_stage::implementation {
    struct record {
        const std::uint64_t creator=managed_pair_creator();
        std::optional<ordinary_resource_reservation<true>> receive;
        std::shared_ptr<managed_baseline_transfer_state> producer;
        values metadata;
        std::string identity;
        bool issued=false;
    };
    struct dispose {void operator()(record* p)const noexcept {
        if(!p||p->creator!=managed_pair_creator())return;
        values{}.swap(p->metadata);std::string{}.swap(p->identity);p->receive.reset();if(p->creator!=managed_pair_creator())return;
        p->producer.reset();if(p->creator!=managed_pair_creator())return;delete p;
    }};
    std::unique_ptr<record,dispose> held{new record};
};
#define LATTICE_BASELINE_PAGING_LIFETIME(type) \
type::type(std::unique_ptr<implementation> p):impl_(std::move(p)){} \
type::~type()=default;type::type(type&&)noexcept=default; \
type& type::operator=(type&&)noexcept=default; \
const type::implementation& type::checked()const { \
    if(!impl_||!impl_->held||impl_->held->creator!=managed_pair_creator())refuse("baseline paging moved or inherited owner");return *impl_;}
LATTICE_BASELINE_PAGING_LIFETIME(ordinary_managed_baseline_cursor)
LATTICE_BASELINE_PAGING_LIFETIME(ordinary_managed_baseline_page)
LATTICE_BASELINE_PAGING_LIFETIME(ordinary_managed_baseline_stage)
#undef LATTICE_BASELINE_PAGING_LIFETIME
std::size_t ordinary_managed_baseline_cursor::total_bytes()const{return checked().held->total;}
std::size_t ordinary_managed_baseline_cursor::produced_bytes()const{return checked().held->produced.load();}
const std::string& ordinary_managed_baseline_cursor::baseline_digest()const{return checked().held->expected;}
std::size_t ordinary_managed_baseline_page::offset()const{return checked().held->offset;}
std::span<const std::uint8_t> ordinary_managed_baseline_page::bytes()const{return checked().held->payload;}
const std::string& ordinary_managed_baseline_stage::identity()const{return checked().held->identity;}

std::optional<ordinary_managed_baseline_cursor> ordinary_export_access::prepare_managed_baseline_cursor(
    const std::shared_ptr<const ordinary_managed_ipc_local_route>& route,const std::string& scope,ordinary_launch::deadline end) {
    if(!route)refuse("baseline cursor actual route missing");route->validate_lifetime();managed_pair_deadline(end);
    // Privacy boundary is checked BEFORE capturing or forwarding source rows.
    // Required filtered/group recovery needs the separate accepted-row scope
    // witness described in COMPOSITION; a caller flag cannot substitute.
    const auto& policy=route->product_policy();
    if(!policy.full_source_partition||route->description().filter.has_value())
        refuse("baseline filtered accepted membership witness not implemented");
    auto captured=capture_managed_accepted_baseline(route,scope,end);if(!captured)return std::nullopt;
    const auto& image=captured->impl_->held->observed.image;
    if(route->description().models!=image.models)refuse("baseline route is not the exact complete source scope");
    std::vector<std::string> tables;tables.reserve(image.tables.size());for(const auto& t:image.tables)tables.push_back(t.name);
    if(policy.source_tables!=tables||policy.recipient_tables!=tables||policy.source_schema!=image.schema||policy.recipient_schema!=image.schema)
        refuse("baseline actual complete catalog differs from product policy");
    ordinary_managed_baseline_cursor result(std::make_unique<ordinary_managed_baseline_cursor::implementation>());
    auto& state=*result.impl_->held;state.end=end;state.baseline.emplace(std::move(*captured));state.initialize();state.issued=true;
    validate_managed_baseline_current(result,true);return std::optional<ordinary_managed_baseline_cursor>(std::move(result));
}
void ordinary_export_access::validate_managed_baseline_current(ordinary_managed_baseline_cursor& cursor,bool verify_values) {
    auto& state=*cursor.checked().held;
    if(!state.issued)refuse("baseline test observation has no live source authority");
    managed_pair_deadline(state.end);auto& capture=*state.baseline->impl_->held;
    const auto& route=*capture.route;route.validate_lifetime();
    if(!capture.receipt||!capture.receipt->current())refuse("baseline WSS receipt retired");
    auto& source=route.source_owner();budget b{wire_limits};
    const auto settled=recovery_writer_access::ordinary_owned_write(source,[&](database& writer){
        validate_managed_profile_until(route.source_profile(),source,writer,state.end);validate_managed_product_policy(route,source,writer);
        baseline_exact_current(writer,source.recovery_schemas_,[](auto& out,const auto& p){lattice_db::serialize_property_for_fingerprint(out,p);},capture.observed,b,state.end,verify_values);
        if(!capture.receipt->current())refuse("baseline WSS changed inside actual source turn");
        validate_managed_profile_until(route.source_profile(),source,writer,state.end);validate_managed_product_policy(route,source,writer);
    });managed_clean_turn(settled);route.validate_lifetime();managed_pair_deadline(state.end);
    if(!capture.receipt->current())refuse("baseline WSS changed during off-lock source completion");
}
ordinary_managed_baseline_page ordinary_export_access::next_managed_baseline_page(ordinary_managed_baseline_cursor& cursor) {
    auto owner=cursor.checked().held;baseline_turn turn(*owner);managed_pair_deadline(owner->end);
    if(owner->poisoned)refuse("baseline cursor abandoned after failed publication");
    if(owner->issued)validate_managed_baseline_current(cursor);
    if(owner->page_live.load())refuse("baseline previous bounded page still retained");
    if(owner->produced.load()==owner->total)refuse("baseline cursor already ended");
    struct poison_on_error {
        managed_baseline_transfer_state& state;bool completed=false;
        ~poison_on_error(){if(!completed&&state.creator==managed_pair_creator())state.poisoned=true;}
    } poison{*owner};
    ordinary_managed_baseline_page result(std::make_unique<ordinary_managed_baseline_page::implementation>());
    auto& page=*result.impl_->held;page.offset=owner->produced.load();
    // Reserve BEFORE publishing ownership. One page, at most 256 KiB, is the
    // only encoded payload retained for this common-pool export reservation.
    page.payload.reserve(ordinary_managed_baseline_page::maximum_bytes);page.owner=owner;owner->page_live.store(true);
    while(page.payload.size()<ordinary_managed_baseline_page::maximum_bytes&&owner->next!=managed_baseline_transfer_state::phase::done){
        managed_pair_deadline(owner->end);const auto part=owner->segment();
        if(owner->part==part.size()){owner->advance();continue;}
        const auto count=std::min(part.size()-owner->part,ordinary_managed_baseline_page::maximum_bytes-page.payload.size());
        const auto first=part.begin()+static_cast<std::ptrdiff_t>(owner->part);
        page.payload.insert(page.payload.end(),first,first+static_cast<std::ptrdiff_t>(count));
        owner->hash.process(first,first+static_cast<std::ptrdiff_t>(count));owner->part+=count;
        owner->produced.fetch_add(count);
    }
    // Advance zero-length payload/trailing phase even when the last nonempty
    // segment exactly filled a page. END is based on exact declared bytes.
    if(owner->produced.load()==owner->total){
        while(owner->next!=managed_baseline_transfer_state::phase::done){
            const auto part=owner->segment();if(owner->part!=part.size())refuse("baseline cursor total differs from segments");owner->advance();
        }
        owner->hash.finish();if(picosha2::get_hash_hex_string(owner->hash)!=owner->expected)refuse("baseline paged fingerprint differs from accepted cut");
    }
    if(page.payload.empty()||owner->produced.load()>owner->total)refuse("baseline empty/overflow page");
    if(owner->issued)validate_managed_baseline_current(cursor);managed_pair_deadline(owner->end);poison.completed=true;return result;
}

namespace {
constexpr const char* baseline_stage_table="_lattice_ordered_baseline_stage_v1";
constexpr const char* baseline_chunk_table="_lattice_ordered_baseline_chunk_v1";
constexpr const char* baseline_member_table="_lattice_ordered_baseline_member_v1";
constexpr const char* baseline_capacity_table="_lattice_ordered_baseline_capacity_v1";
// Fixed id + seal are included in the unchanged logical aggregate cap.
constexpr std::int64_t baseline_capacity_metadata_bytes=8+64;
constexpr const char* baseline_capacity_ddl="CREATE TABLE _lattice_ordered_baseline_capacity_v1 ("
    "id TEXT PRIMARY KEY NOT NULL CHECK(id='capacity'),cuts INTEGER NOT NULL CHECK(cuts>=0 AND cuts<=64),"
    "chunks INTEGER NOT NULL CHECK(chunks>=0 AND chunks<=8192),members INTEGER NOT NULL CHECK(members>=0 AND members<=524288),"
    "bytes INTEGER NOT NULL CHECK(bytes>=72 AND bytes<=134217728),schema_version INTEGER NOT NULL CHECK(schema_version>=0),"
    "seal TEXT NOT NULL CHECK(length(seal)=64))";
constexpr const char* baseline_capacity_columns="id,cuts,chunks,members,bytes,schema_version,seal";
constexpr const char* baseline_stage_ddl="CREATE TABLE _lattice_ordered_baseline_stage_v1 ("
    "identity TEXT PRIMARY KEY NOT NULL,source TEXT NOT NULL,epoch INTEGER NOT NULL CHECK(epoch>0),scope TEXT NOT NULL,schema TEXT NOT NULL,"
    "cut INTEGER NOT NULL CHECK(cut>0),cut_digest TEXT NOT NULL,baseline TEXT NOT NULL,route TEXT NOT NULL,policy TEXT NOT NULL,"
    "generation TEXT NOT NULL,recipient_device TEXT NOT NULL,recipient_inode TEXT NOT NULL,"
    "total INTEGER NOT NULL CHECK(total>0 AND total<=134217728),rows INTEGER NOT NULL CHECK(rows>=0 AND rows<=8192),"
    "offset INTEGER NOT NULL CHECK(offset>=0 AND offset<=total),pages INTEGER NOT NULL CHECK(pages>=0 AND pages<=8192),"
    "complete INTEGER NOT NULL CHECK(complete IN(0,1)),receipt TEXT NOT NULL,seal TEXT NOT NULL)";
constexpr const char* baseline_stage_columns="identity,source,epoch,scope,schema,cut,cut_digest,baseline,route,policy,generation,recipient_device,recipient_inode,total,rows,offset,pages,complete,receipt,seal";
constexpr const char* baseline_chunk_ddl="CREATE TABLE _lattice_ordered_baseline_chunk_v1 ("
    "identity TEXT NOT NULL,offset INTEGER NOT NULL CHECK(offset>=0),payload BLOB NOT NULL CHECK(length(payload)>0 AND length(payload)<=262144),"
    "digest TEXT NOT NULL,PRIMARY KEY(identity,offset))";
constexpr const char* baseline_member_ddl="CREATE TABLE _lattice_ordered_baseline_member_v1 ("
    "identity TEXT NOT NULL,model TEXT NOT NULL,gid TEXT NOT NULL COLLATE NOCASE,seal TEXT NOT NULL,PRIMARY KEY(identity,model,gid))";
constexpr const char* baseline_member_index="CREATE INDEX _lattice_ordered_baseline_member_v1_row ON _lattice_ordered_baseline_member_v1(model,gid,identity)";
bool baseline_stage_storage(sqlite3* h,budget& b) {
    b.add(32*1024);
    if(!read(h,"SELECT 1 FROM temp.sqlite_master WHERE name GLOB '_lattice_ordered_baseline_*' OR tbl_name GLOB '_lattice_ordered_baseline_*' LIMIT 1",{},1,b).empty())
        refuse("baseline stage TEMP shadow");
    const auto objects=read(h,"SELECT type,name,tbl_name,sql FROM main.sqlite_master WHERE name GLOB '_lattice_ordered_baseline_*' OR tbl_name GLOB '_lattice_ordered_baseline_*' ORDER BY name",{},22,b);
    if(objects.empty())return false;
    std::map<std::string,values> expected;
    for(const auto& [name,ddl]:std::array<std::pair<const char*,const char*>,4>{{{baseline_stage_table,baseline_stage_ddl},{baseline_chunk_table,baseline_chunk_ddl},{baseline_member_table,baseline_member_ddl},{baseline_capacity_table,baseline_capacity_ddl}}}){
        expected.emplace(name,values{"table",name,name,ddl});
        const std::string autoindex="sqlite_autoindex_"+std::string(name)+"_1";expected.emplace(autoindex,values{"index",autoindex,name,nullptr});
        for(const char* op:{"INSERT","UPDATE","DELETE"}){
            const std::string trigger=std::string(name)+'_'+op;expected.emplace(trigger,values{"trigger",trigger,name,receiver_guard(name,op)});
        }
    }
    expected.emplace("_lattice_ordered_baseline_member_v1_row",values{"index","_lattice_ordered_baseline_member_v1_row",baseline_member_table,baseline_member_index});
    if(objects.size()!=expected.size())refuse("baseline staging inventory changed");
    for(const auto& row:objects){const auto at=expected.find(text(row[1]));if(at==expected.end()||at->second!=row)refuse("baseline staging program changed");}return true;
}
void baseline_capacity_initialize(database&,budget&);
void baseline_stage_install(database& w,budget& b) {
    original_view(w,true);if(baseline_stage_storage(ordinary_export_storage_access::borrow(w),b))return;
    w.execute(baseline_stage_ddl);w.execute(baseline_chunk_ddl);w.execute(baseline_member_ddl);w.execute(baseline_capacity_ddl);w.execute(baseline_member_index);
    for(const char* table:{baseline_stage_table,baseline_chunk_table,baseline_member_table,baseline_capacity_table})for(const char* op:{"INSERT","UPDATE","DELETE"})w.execute(receiver_guard(table,op));
    baseline_capacity_initialize(w,b);
    if(!baseline_stage_storage(ordinary_export_storage_access::borrow(w),b))refuse("baseline staging atomic install failed");
}
struct baseline_stage_usage {std::int64_t cuts=0,chunks=0,members=0,bytes=0;bool operator==(const baseline_stage_usage&)const=default;};
baseline_stage_usage baseline_stage_accounting(sqlite3* h,budget& b) {
    const auto result=read(h,"SELECT (SELECT COUNT(*) FROM main._lattice_ordered_baseline_stage_v1),"
        "(SELECT COUNT(*) FROM main._lattice_ordered_baseline_chunk_v1),(SELECT COUNT(*) FROM main._lattice_ordered_baseline_member_v1),"
        "(SELECT COALESCE(SUM(length(CAST(identity AS BLOB))+length(CAST(source AS BLOB))+length(CAST(scope AS BLOB))+length(CAST(schema AS BLOB))+"
        "length(CAST(cut_digest AS BLOB))+length(CAST(baseline AS BLOB))+length(CAST(route AS BLOB))+length(CAST(policy AS BLOB))+length(CAST(generation AS BLOB))+"
        "length(CAST(recipient_device AS BLOB))+length(CAST(recipient_inode AS BLOB))+length(CAST(receipt AS BLOB))+length(CAST(seal AS BLOB))),0) FROM main._lattice_ordered_baseline_stage_v1)+"
        "(SELECT COALESCE(SUM(length(CAST(identity AS BLOB))+length(payload)+length(CAST(digest AS BLOB))),0) FROM main._lattice_ordered_baseline_chunk_v1)+"
        "(SELECT COALESCE(SUM(length(CAST(identity AS BLOB))+length(CAST(model AS BLOB))+length(CAST(gid AS BLOB))+length(CAST(seal AS BLOB))),0) FROM main._lattice_ordered_baseline_member_v1)+72",{},1,b);
    if(result.size()!=1||result[0].size()!=4)refuse("baseline staging accounting missing");
    baseline_stage_usage usage{integer(result[0][0]),integer(result[0][1]),integer(result[0][2]),integer(result[0][3])};
    if(usage.cuts<0||static_cast<std::uint64_t>(usage.cuts)>managed_baseline_stage_cuts||usage.chunks<0||static_cast<std::uint64_t>(usage.chunks)>managed_baseline_stage_chunks||
        usage.members<0||static_cast<std::uint64_t>(usage.members)>managed_baseline_stage_members||usage.bytes<0||static_cast<std::uint64_t>(usage.bytes)>managed_baseline_stage_bytes)
        refuse("baseline durable staging capacity exhausted");return usage;
}

// Only the closed engine issues counters. Their seals alone prove no continuity.
// The actual connection-owned witness below couples the counter to native
// mutation observations; missing or changed observations require a full audit.
std::int64_t baseline_capacity_schema(sqlite3* h,budget& b) {
    const auto rows=read(h,"PRAGMA main.schema_version",{},1,b);
    if(rows.size()!=1||rows[0].size()!=1||integer(rows[0][0])<0)refuse("baseline capacity schema version unavailable");
    return integer(rows[0][0]);
}
baseline_stage_usage baseline_capacity_usage(const values& row) {
    if(row.size()!=7||text(row[0])!="capacity")refuse("baseline capacity singleton changed");
    baseline_stage_usage u{integer(row[1]),integer(row[2]),integer(row[3]),integer(row[4])};
    if(u.cuts<0||static_cast<std::uint64_t>(u.cuts)>managed_baseline_stage_cuts||u.chunks<0||static_cast<std::uint64_t>(u.chunks)>managed_baseline_stage_chunks||
        u.members<0||static_cast<std::uint64_t>(u.members)>managed_baseline_stage_members||u.bytes<baseline_capacity_metadata_bytes||static_cast<std::uint64_t>(u.bytes)>managed_baseline_stage_bytes)
        refuse("baseline durable counter capacity exhausted");return u;
}
values baseline_capacity_read(sqlite3* h,budget& b,bool full_inventory) {
    auto rows=read(h,std::string("SELECT ")+baseline_capacity_columns+" FROM main._lattice_ordered_baseline_capacity_v1",{},1,b);
    if(rows.size()!=1)refuse("baseline actual capacity singleton missing");
    const auto usage=baseline_capacity_usage(rows[0]);
    if(integer(rows[0][5])!=baseline_capacity_schema(h,b)||text(rows[0][6])!=original_seal("ordinary-accepted-baseline-capacity-v1",rows[0],6,b))
        refuse("baseline capacity seal or installed schema changed");
    if(full_inventory&&usage!=baseline_stage_accounting(h,b))refuse("baseline actual inventory differs from durable counters");
    return std::move(rows[0]);
}
void baseline_capacity_initialize(database& w,budget& b) {
    // Installation has just created the complete new layout in this owning
    // WRITE. Derive from the actual tables, even for that expected empty view.
    const auto actual=baseline_stage_accounting(ordinary_export_storage_access::borrow(w),b);
    if(actual.cuts||actual.chunks||actual.members||actual.bytes!=baseline_capacity_metadata_bytes)refuse("baseline new capacity has preexisting custody");
    values row{std::string("capacity"),actual.cuts,actual.chunks,actual.members,actual.bytes,baseline_capacity_schema(ordinary_export_storage_access::borrow(w),b)};
    row.emplace_back(original_seal("ordinary-accepted-baseline-capacity-v1",row,6,b));
    w.execute("INSERT INTO main._lattice_ordered_baseline_capacity_v1 VALUES(?,?,?,?,?,?,?)",row);
    if(w.changes()!=1||baseline_capacity_read(ordinary_export_storage_access::borrow(w),b,true)!=row)refuse("baseline capacity atomic initialization refused");
}
std::int64_t baseline_capacity_postimage_bytes(const values& actual) {
    std::size_t bytes=0;
    for(const auto& value:actual){
        const auto* text=std::get_if<std::string>(&value);const auto* blob=std::get_if<std::vector<std::uint8_t>>(&value);
        const auto n=text?text->size():(blob?blob->size():0);
        if(n>managed_baseline_stage_bytes-bytes)refuse("baseline actual postimage byte bound");bytes+=n;
    }
    return static_cast<std::int64_t>(bytes);
}
void baseline_capacity_publish(database& w,const values& previous,const baseline_stage_usage& issued,budget& b,bool full_inventory) {
    // issued is a local accumulator populated only after exact successful SQL
    // postimages below. No public/caller/wire count or logical row total enters.
    if(issued.cuts<0||issued.chunks<0||issued.members<0||issued.bytes<0)refuse("baseline negative issued capacity delta");
    if(baseline_capacity_read(ordinary_export_storage_access::borrow(w),b,false)!=previous)refuse("baseline capacity changed within actual owning turn");
    const auto old=baseline_capacity_usage(previous);
    const auto add=[](std::int64_t before,std::int64_t amount,std::size_t maximum){
        if(static_cast<std::uint64_t>(amount)>maximum-static_cast<std::uint64_t>(before))refuse("baseline issued durable capacity exhausted");return before+amount;
    };
    const baseline_stage_usage next{add(old.cuts,issued.cuts,managed_baseline_stage_cuts),add(old.chunks,issued.chunks,managed_baseline_stage_chunks),
        add(old.members,issued.members,managed_baseline_stage_members),add(old.bytes,issued.bytes,managed_baseline_stage_bytes)};
    if(full_inventory&&baseline_stage_accounting(ordinary_export_storage_access::borrow(w),b)!=next)refuse("baseline issued changes differ from actual full inventory");
    if(next==old)return; // Exact DATA/END retry issues no new counter mutation.
    values row{std::string("capacity"),next.cuts,next.chunks,next.members,next.bytes,previous[5]};
    row.emplace_back(original_seal("ordinary-accepted-baseline-capacity-v1",row,6,b));
    w.execute("UPDATE main._lattice_ordered_baseline_capacity_v1 SET cuts=?,chunks=?,members=?,bytes=?,seal=? WHERE id='capacity' AND seal=?",
        {row[1],row[2],row[3],row[4],row[6],previous[6]});
    if(w.changes()!=1||baseline_capacity_read(ordinary_export_storage_access::borrow(w),b,full_inventory)!=row)refuse("baseline capacity postimage differs");
}

void baseline_stage_verify(const values& row,budget& b) {
    if(row.size()!=20||!canonical_uuid(text(row[1]))||integer(row[2])<=0||integer(row[5])<=0||
        integer(row[13])<=0||static_cast<std::uint64_t>(integer(row[13]))>managed_baseline_stage_bytes||integer(row[14])<0||static_cast<std::uint64_t>(integer(row[14]))>wire_limits.rows||
        integer(row[15])<0||integer(row[15])>integer(row[13])||integer(row[16])<0||static_cast<std::uint64_t>(integer(row[16]))>managed_baseline_stage_chunks||
        (integer(row[17])!=0&&integer(row[17])!=1)||text(row[19])!=original_seal("ordinary-accepted-baseline-stage-v1",row,19,b))
        refuse("baseline staging sealed row changed");
    for(const auto i:{0u,6u,7u,9u,19u})original_digest(text(row[i]));
    for(const auto i:{3u,4u,8u,10u,11u,12u})if(text(row[i]).empty()||text(row[i]).find('\0')!=std::string::npos)refuse("baseline staging scalar changed");
    if(integer(row[17])){if(integer(row[15])!=integer(row[13]))refuse("baseline partial stage marked complete");original_digest(text(row[18]));}
    else if(!text(row[18]).empty())refuse("baseline incomplete stage has result");
}
values baseline_stage_read(sqlite3* h,const std::string& id,budget& b) {
    original_digest(id);b.add(id.size()+sizeof(values)+sizeof(column_value_t));
    auto rows=read(h,std::string("SELECT ")+baseline_stage_columns+" FROM main._lattice_ordered_baseline_stage_v1 WHERE identity=?",{id},1,b);
    if(rows.size()!=1)refuse("baseline actual durable stage missing");baseline_stage_verify(rows[0],b);return std::move(rows[0]);
}
void baseline_stage_matches(const values& actual,const values& exact) {
    if(exact.size()!=15||actual.size()!=20||!std::equal(exact.begin(),exact.end(),actual.begin()))refuse("baseline stage belongs to another immutable cut/route");
}
void baseline_stage_physical(database& w,const values& stage) {
    const auto physical=w.physical_identity("main",{},true);
    if(!physical||text(stage[11])!=std::to_string(physical->device)||text(stage[12])!=std::to_string(physical->inode))refuse("baseline staging physical owner changed");
}
// A streaming borrowed SQLite BLOB scan: bounded statement/metadata and one
// <=256 KiB chunk, never read(all payloads). Reopen is validation only; it does
// not mint a live source/route or renew the original operation deadline.
void baseline_stage_stream(sqlite3* h,const values& stage,budget& b,ordinary_launch::deadline end) {
    managed_pair_deadline(end);b.add(8*1024+text(stage[0]).size());
    database::record_statement();sqlite3_stmt* raw=nullptr;
    const auto rc=::lattice::detail::store_physical_prepare_v2(h,"SELECT offset,payload,digest FROM main._lattice_ordered_baseline_chunk_v1 WHERE identity=? ORDER BY offset",-1,&raw,nullptr);
    std::unique_ptr<sqlite3_stmt,decltype(&::lattice::detail::store_physical_finalize)> statement(raw,&::lattice::detail::store_physical_finalize);
    if(rc!=SQLITE_OK||!raw||sqlite3_bind_parameter_count(raw)!=1||sqlite3_column_count(raw)!=3||!sqlite3_stmt_readonly(raw)||
        sqlite3_bind_text(raw,1,text(stage[0]).data(),static_cast<int>(text(stage[0]).size()),SQLITE_STATIC)!=SQLITE_OK)refuse("baseline stage scan unavailable");
    picosha2::hash256_one_by_one stream;std::size_t offset=0,pages=0;int step=SQLITE_OK;
    for(;;){managed_pair_deadline(end);step=::lattice::detail::store_physical_step(raw);if(step!=SQLITE_ROW)break;
        if(sqlite3_column_type(raw,0)!=SQLITE_INTEGER||sqlite3_column_int64(raw,0)!=static_cast<std::int64_t>(offset)||
            sqlite3_column_type(raw,1)!=SQLITE_BLOB||sqlite3_column_type(raw,2)!=SQLITE_TEXT)refuse("baseline staged chunk order/storage changed");
        const auto count=sqlite3_column_bytes(raw,1),digest_size=sqlite3_column_bytes(raw,2);
        if(count<=0||static_cast<std::size_t>(count)>ordinary_managed_baseline_page::maximum_bytes||digest_size!=64||pages==managed_baseline_stage_chunks||
            static_cast<std::size_t>(count)>static_cast<std::size_t>(integer(stage[13]))-offset)refuse("baseline staged chunk bound");
        const auto* bytes=static_cast<const std::uint8_t*>(sqlite3_column_blob(raw,1));const auto* digest=sqlite3_column_text(raw,2);
        if(!bytes||!digest)refuse("baseline staged borrowed bytes missing");
        const std::span<const std::uint8_t> part(bytes,static_cast<std::size_t>(count));
        if(picosha2::hash256_hex_string(part.begin(),part.end())!=std::string_view(reinterpret_cast<const char*>(digest),64))refuse("baseline staged chunk digest changed");
        stream.process(part.begin(),part.end());offset+=part.size();++pages;
    }
    if(step!=SQLITE_DONE||offset!=static_cast<std::size_t>(integer(stage[15]))||pages!=static_cast<std::size_t>(integer(stage[16])))refuse("baseline durable progress differs from chunks");
    if(::lattice::detail::store_physical_finalize(statement.release())!=SQLITE_OK)refuse("baseline staged statement finalization failed");
    if(integer(stage[17])){stream.finish();if(offset!=static_cast<std::size_t>(integer(stage[13]))||picosha2::get_hash_hex_string(stream)!=text(stage[7]))refuse("baseline completed durable cut digest changed");}
    managed_pair_deadline(end);
}
void baseline_stage_post(database& w,const values& expected,budget& b,bool full_inventory=true) {
    if(baseline_stage_read(ordinary_export_storage_access::borrow(w),text(expected[0]),b)!=expected)refuse("baseline durable stage postimage differs");
    (void)baseline_capacity_read(ordinary_export_storage_access::borrow(w),b,full_inventory);if(!baseline_stage_storage(ordinary_export_storage_access::borrow(w),b))refuse("baseline staging inventory disappeared");
}
void baseline_stage_members_reopen(sqlite3* h,const values& stage,ordinary_launch::deadline end,budget& b) {
    b.add(8*1024+text(stage[0]).size());database::record_statement();sqlite3_stmt* raw=nullptr;
    const auto rc=::lattice::detail::store_physical_prepare_v2(h,"SELECT model,gid,seal FROM main._lattice_ordered_baseline_member_v1 WHERE identity=? ORDER BY model,gid",-1,&raw,nullptr);
    std::unique_ptr<sqlite3_stmt,decltype(&::lattice::detail::store_physical_finalize)> statement(raw,&::lattice::detail::store_physical_finalize);
    if(rc!=SQLITE_OK||!raw||sqlite3_bind_parameter_count(raw)!=1||sqlite3_column_count(raw)!=3||!sqlite3_stmt_readonly(raw)||
        sqlite3_bind_text(raw,1,text(stage[0]).data(),static_cast<int>(text(stage[0]).size()),SQLITE_STATIC)!=SQLITE_OK)refuse("baseline member reopen scan unavailable");
    std::size_t seen=0;int step=SQLITE_OK;
    for(;;){managed_pair_deadline(end);step=::lattice::detail::store_physical_step(raw);if(step!=SQLITE_ROW)break;
        if(!integer(stage[17])||seen==static_cast<std::size_t>(integer(stage[14])))refuse("baseline reopened unexpected member");
        std::array<std::string_view,3> fields;
        for(std::size_t i=0;i<fields.size();++i){
            if(sqlite3_column_type(raw,static_cast<int>(i))!=SQLITE_TEXT)refuse("baseline reopened member storage changed");
            const auto bytes=sqlite3_column_bytes(raw,static_cast<int>(i));const auto* p=sqlite3_column_text(raw,static_cast<int>(i));
            const std::size_t maximum=i==0?256:(i==1?4096:64);
            if(bytes<=0||static_cast<std::size_t>(bytes)>maximum||!p||(i==2&&bytes!=64))refuse("baseline reopened member scalar bound");
            fields[i]={reinterpret_cast<const char*>(p),static_cast<std::size_t>(bytes)};
            if(fields[i].find('\0')!=std::string_view::npos)refuse("baseline reopened member spelling");
        }
        // Each small sealed-key copy has its own finite workspace, released
        // before the next row. This is integrity checking, not source issuance.
        budget one{wire_limits};one.add(16*1024);
        const values member{stage[0],std::string(fields[0]),std::string(fields[1])};
        if(original_seal("ordinary-accepted-baseline-member-v1",member,3,one)!=fields[2])refuse("baseline reopened member seal changed");++seen;
    }
    if(step!=SQLITE_DONE||seen!=(integer(stage[17])?static_cast<std::size_t>(integer(stage[14])):0))refuse("baseline reopened membership count changed");
    if(::lattice::detail::store_physical_finalize(statement.release())!=SQLITE_OK)refuse("baseline reopened member finalize failed");
}

// This cell belongs to database_retirement_state for this physical connection,
// follows its moves, and disappears on reopen. It is not a reconstructed grant
// or a sidecar. Only the private owned-write entry points can obtain the cell.
struct baseline_capacity_witness {
    const std::uint64_t creator=managed_pair_creator();
    sqlite3_int64 changes=0;std::int64_t data_version=0,schema_version=0;
    std::string counter_seal;
};
struct baseline_capacity_witness_dispose {void operator()(baseline_capacity_witness* p)const noexcept {
    if(!p||p->creator!=managed_pair_creator())return;delete p;
}};
std::int64_t baseline_capacity_data_version(sqlite3* h,budget& b) {
    const auto rows=read(h,"PRAGMA main.data_version",{},1,b);
    if(rows.size()!=1||rows[0].size()!=1||integer(rows[0][0])<0)refuse("baseline actual writer data version unavailable");
    return integer(rows[0][0]);
}
void baseline_capacity_audit(database& w,const baseline_stage_usage& actual,budget& b,ordinary_launch::deadline end) {
    // Aggregate limits precede all retained-row reads. Stream validation borrows
    // <=256 KiB from SQLite; each small stage/member workspace is retired before
    // the next stage. Count equality also refuses orphan chunks/member rows.
    const auto ids=read(ordinary_export_storage_access::borrow(w),"SELECT identity FROM main._lattice_ordered_baseline_stage_v1 ORDER BY identity",{},managed_baseline_stage_cuts,b);
    std::int64_t chunks=0,members=0;
    for(const auto& id:ids){
        managed_pair_deadline(end);budget one{wire_limits};one.add(16*1024);
        const auto stage=baseline_stage_read(ordinary_export_storage_access::borrow(w),text(id[0]),one);baseline_stage_physical(w,stage);
        baseline_stage_stream(ordinary_export_storage_access::borrow(w),stage,one,end);baseline_stage_members_reopen(ordinary_export_storage_access::borrow(w),stage,end,one);
        chunks+=integer(stage[16]);members+=integer(stage[17])?integer(stage[14]):0;
    }
    if(static_cast<std::int64_t>(ids.size())!=actual.cuts||chunks!=actual.chunks||members!=actual.members)
        refuse("baseline full audit found orphan custody");
}
struct baseline_capacity_turn {values row;sqlite3_int64 changes=0;};
baseline_capacity_turn baseline_capacity_admit(database& w,std::shared_ptr<void>& cell,budget& b,ordinary_launch::deadline end,bool full) {
    managed_pair_deadline(end);auto* h=ordinary_export_storage_access::borrow(w);
    const auto changes=sqlite3_total_changes64(h);if(changes<0)refuse("baseline writer mutation count unavailable");
    const auto version=baseline_capacity_data_version(h,b),schema=baseline_capacity_schema(h,b);
    auto row=baseline_capacity_read(h,b,false);
    const auto held=std::static_pointer_cast<baseline_capacity_witness>(cell);
    if(held&&held->creator!=managed_pair_creator())refuse("baseline inherited capacity witness");
    const bool current=held&&held->changes==changes&&held->data_version==version&&held->schema_version==schema&&held->counter_seal==text(row[6]);
    if(full||!current){
        const auto actual=baseline_stage_accounting(h,b);
        if(actual!=baseline_capacity_usage(row))refuse("baseline changed writer inventory differs from counters");
        baseline_capacity_audit(w,actual,b,end);
    }
    if(sqlite3_total_changes64(h)!=changes||baseline_capacity_data_version(h,b)!=version||baseline_capacity_schema(h,b)!=schema||baseline_capacity_read(h,b,false)!=row)
        refuse("baseline writer changed during counter admission");
    managed_pair_deadline(end);return {std::move(row),changes};
}
void baseline_capacity_remember(database& w,std::shared_ptr<void>& cell,const baseline_capacity_turn& turn,
    std::size_t issued_mutations,budget& b) {
    // Each issuer SQL mutation has changes()==1 and an exact postimage. Detect
    // extra same-handle DML, including unsuccessful/rolled-back earlier turns.
    // A COMMIT denial can leave this prospective cell ahead of durable storage;
    // next admission then sees its different row seal and performs a full audit.
    auto* h=ordinary_export_storage_access::borrow(w);const auto now=sqlite3_total_changes64(h);
    if(now<turn.changes||static_cast<std::uint64_t>(now-turn.changes)!=issued_mutations)
        refuse("baseline unrecognized mutation inside actual owning turn");
    const auto row=baseline_capacity_read(h,b,false);const auto version=baseline_capacity_data_version(h,b),schema=baseline_capacity_schema(h,b);
    if(sqlite3_total_changes64(h)!=now)refuse("baseline writer changed while remembering capacity");
    std::shared_ptr<baseline_capacity_witness> next(new baseline_capacity_witness,baseline_capacity_witness_dispose{});
    next->changes=now;next->data_version=version;next->schema_version=schema;next->counter_seal=text(row[6]);cell=std::move(next);
}

values baseline_stage_metadata(database& w,const managed_baseline_transfer_state& state,
    const std::string& route,const std::string& policy,const std::string& generation,budget& b) {
    const auto physical=w.physical_identity("main",{},true);if(!physical)refuse("baseline recipient physical identity missing");
    const auto& accepted=*state.baseline;
    b.add(sizeof(values)+20*sizeof(column_value_t)+accepted.source_id().size()+accepted.scope().size()+accepted.schema().size()+
        accepted.cut_digest().size()+accepted.baseline_digest().size()+route.size()+policy.size()+generation.size()+256);
    values row{std::string{},accepted.source_id(),accepted.epoch(),accepted.scope(),accepted.schema(),accepted.cut(),accepted.cut_digest(),accepted.baseline_digest(),
        route,policy,generation,std::to_string(physical->device),std::to_string(physical->inode),static_cast<std::int64_t>(state.total),std::int64_t{0}};
    for(const auto& t:accepted.tables())row[14]=integer(row[14])+static_cast<std::int64_t>(t.rows.size());
    // Version identity preserves same-lineage cuts separately; source cuts
    // from different sources/epochs/scopes/schemas are never ordered together.
    row[0]=original_seal("ordinary-accepted-baseline-cut-identity-v1",row,15,b);return row;
}
void baseline_stage_begin_write(database& w,values& metadata,budget& b,std::shared_ptr<void>& witness,ordinary_launch::deadline end) {
    original_view(w,true);register_mutation_guard(ordinary_export_storage_access::borrow(w));mutation_scope permit(ordinary_export_storage_access::borrow(w));
    baseline_stage_physical(w,metadata);
    baseline_stage_install(w,b);const auto turn=baseline_capacity_admit(w,witness,b,end,true);const auto& capacity=turn.row;
    const auto prior=read(ordinary_export_storage_access::borrow(w),std::string("SELECT ")+baseline_stage_columns+" FROM main._lattice_ordered_baseline_stage_v1 WHERE identity=?",{metadata[0]},1,b);
    if(!prior.empty()){baseline_stage_verify(prior[0],b);baseline_stage_matches(prior[0],metadata);baseline_capacity_remember(w,witness,turn,0,b);return;}
    original_copy_charge(metadata,b);values row=metadata;row.insert(row.end(),{std::int64_t{0},std::int64_t{0},std::int64_t{0},std::string{}});
    row.emplace_back(original_seal("ordinary-accepted-baseline-stage-v1",row,19,b));
    w.execute("INSERT INTO main._lattice_ordered_baseline_stage_v1 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",row);
    if(w.changes()!=1)refuse("baseline durable stage insert refused");
    const auto actual=baseline_stage_read(ordinary_export_storage_access::borrow(w),text(row[0]),b);if(actual!=row)refuse("baseline BEGIN postimage differs");
    baseline_stage_usage issued;issued.cuts=1;issued.bytes=baseline_capacity_postimage_bytes(actual);
    baseline_capacity_publish(w,capacity,issued,b,true);baseline_stage_post(w,row,b);baseline_capacity_remember(w,witness,turn,2,b);
}
void baseline_stage_append_write(database& w,const values& metadata,const ordinary_managed_baseline_page& page,budget& b,ordinary_launch::deadline end,std::shared_ptr<void>& witness) {
    original_view(w,true);managed_pair_deadline(end);if(!baseline_stage_storage(ordinary_export_storage_access::borrow(w),b))refuse("baseline stage missing at DATA");
    const auto turn=baseline_capacity_admit(w,witness,b,end,false);const auto& capacity=turn.row;auto stage=baseline_stage_read(ordinary_export_storage_access::borrow(w),text(metadata[0]),b);baseline_stage_matches(stage,metadata);baseline_stage_physical(w,stage);
    const auto part=page.bytes();const auto offset=page.offset();
    if(part.empty()||part.size()>ordinary_managed_baseline_page::maximum_bytes||offset>static_cast<std::size_t>(integer(stage[13]))||
        part.size()>static_cast<std::size_t>(integer(stage[13]))-offset)refuse("baseline DATA bound");
    b.add(3*ordinary_managed_baseline_page::maximum_bytes+8*1024); // payload copy, readback, existing retry read
    const auto digest=picosha2::hash256_hex_string(part.begin(),part.end());
    if(offset<static_cast<std::size_t>(integer(stage[15]))){
        const auto old=read(ordinary_export_storage_access::borrow(w),"SELECT payload,digest FROM main._lattice_ordered_baseline_chunk_v1 WHERE identity=? AND offset=?",{metadata[0],static_cast<std::int64_t>(offset)},1,b);
        if(old.size()!=1||!std::holds_alternative<std::vector<std::uint8_t>>(old[0][0])||text(old[0][1])!=digest||
            std::get<std::vector<std::uint8_t>>(old[0][0]).size()!=part.size()||!std::equal(part.begin(),part.end(),std::get<std::vector<std::uint8_t>>(old[0][0]).begin()))refuse("baseline DATA retry differs");baseline_capacity_remember(w,witness,turn,0,b);return;
    }
    if(integer(stage[17])||offset!=static_cast<std::size_t>(integer(stage[15])))refuse("baseline DATA is not the exact next offset");
    register_mutation_guard(ordinary_export_storage_access::borrow(w));mutation_scope permit(ordinary_export_storage_access::borrow(w));
    values chunk{metadata[0],static_cast<std::int64_t>(offset),std::vector<std::uint8_t>(part.begin(),part.end()),digest};
    w.execute("INSERT INTO main._lattice_ordered_baseline_chunk_v1 VALUES(?,?,?,?)",chunk);if(w.changes()!=1)refuse("baseline chunk insert refused");
    const auto post=read(ordinary_export_storage_access::borrow(w),"SELECT identity,offset,payload,digest FROM main._lattice_ordered_baseline_chunk_v1 WHERE identity=? AND offset=?",{metadata[0],chunk[1]},1,b);
    if(post.size()!=1||post[0]!=chunk)refuse("baseline chunk postimage differs");
    baseline_stage_usage issued;issued.chunks=1;issued.bytes=baseline_capacity_postimage_bytes(post[0]);
    stage[15]=integer(stage[15])+static_cast<std::int64_t>(part.size());stage[16]=integer(stage[16])+1;stage[19]=original_seal("ordinary-accepted-baseline-stage-v1",stage,19,b);
    w.execute("UPDATE main._lattice_ordered_baseline_stage_v1 SET offset=?,pages=?,seal=? WHERE identity=?",{stage[15],stage[16],stage[19],metadata[0]});
    if(w.changes()!=1)refuse("baseline durable progress update refused");
    if(baseline_stage_read(ordinary_export_storage_access::borrow(w),text(metadata[0]),b)!=stage)refuse("baseline DATA progress postimage differs");
    baseline_capacity_publish(w,capacity,issued,b,false);baseline_stage_post(w,stage,b,false);baseline_capacity_remember(w,witness,turn,3,b);managed_pair_deadline(end);
}
ordinary_managed_baseline_staging_result baseline_stage_finish_write(database& w,const values& metadata,const managed_baseline_transfer_state& state,budget& b,std::shared_ptr<void>& witness) {
    original_view(w,true);managed_pair_deadline(state.end);if(!baseline_stage_storage(ordinary_export_storage_access::borrow(w),b))refuse("baseline stage missing at END");
    const auto turn=baseline_capacity_admit(w,witness,b,state.end,true);const auto& capacity=turn.row;auto stage=baseline_stage_read(ordinary_export_storage_access::borrow(w),text(metadata[0]),b);baseline_stage_matches(stage,metadata);baseline_stage_physical(w,stage);
    const auto previous_stage_bytes=baseline_capacity_postimage_bytes(stage);baseline_stage_usage issued;
    if(integer(stage[15])!=integer(stage[13]))refuse("baseline partial transfer cannot END");
    original_copy_charge(stage,b);auto candidate=stage;candidate[17]=std::int64_t{1};
    // Stream validation is required even for an exact completed retry.
    baseline_stage_stream(ordinary_export_storage_access::borrow(w),candidate,b,state.end);
    register_mutation_guard(ordinary_export_storage_access::borrow(w));mutation_scope permit(ordinary_export_storage_access::borrow(w));
    ordinary_managed_baseline_staging_result result;
    for(const auto& t:state.baseline->tables())for(const auto& row:t.rows){
        managed_pair_deadline(state.end);budget one{wire_limits};one.add(16*1024);
        values member{metadata[0],t.name,text(row.at(0))};member.emplace_back(original_seal("ordinary-accepted-baseline-member-v1",member,3,one));
        const auto prior=read(ordinary_export_storage_access::borrow(w),"SELECT identity,model,gid,seal FROM main._lattice_ordered_baseline_member_v1 WHERE identity=? AND model=? AND gid=?",{member[0],member[1],member[2]},1,one);
        if(prior.empty()){
            if(integer(stage[17]))refuse("baseline completed stage missing member");
            w.execute("INSERT INTO main._lattice_ordered_baseline_member_v1 VALUES(?,?,?,?)",member);if(w.changes()!=1)refuse("baseline accepted membership insert refused");
            const auto post=read(ordinary_export_storage_access::borrow(w),"SELECT identity,model,gid,seal FROM main._lattice_ordered_baseline_member_v1 WHERE identity=? AND model=? AND gid=?",{member[0],member[1],member[2]},1,one);
            if(post.size()!=1||post[0]!=member)refuse("baseline member postimage differs");
            ++issued.members;issued.bytes+=baseline_capacity_postimage_bytes(post[0]);
        }else if(prior.size()!=1||prior[0]!=member)refuse("baseline accepted membership changed");
        // A collision is an unresolved observation, not a visible winner.
        if(!read(ordinary_export_storage_access::borrow(w),"SELECT 1 FROM main._lattice_ordered_baseline_member_v1 m JOIN main._lattice_ordered_baseline_stage_v1 s ON s.identity=m.identity WHERE m.model=? AND m.gid=? AND s.complete=1 AND NOT(s.source=? AND s.epoch=? AND s.scope=? AND s.schema=?) LIMIT 1",
            {member[1],member[2],metadata[1],metadata[2],metadata[3],metadata[4]},1,one).empty())result.other_lineage_observed=true;
    }
    const auto members=read(ordinary_export_storage_access::borrow(w),"SELECT COUNT(*) FROM main._lattice_ordered_baseline_member_v1 WHERE identity=?",{metadata[0]},1,b);
    if(members.size()!=1||members[0][0]!=metadata[14])refuse("baseline accepted membership is not exact whole cut");
    if(!integer(stage[17])){
        candidate[18]=original_seal("ordinary-accepted-baseline-passive-staged-result-v1",metadata,15,b);
        candidate[19]=original_seal("ordinary-accepted-baseline-stage-v1",candidate,19,b);
        w.execute("UPDATE main._lattice_ordered_baseline_stage_v1 SET complete=1,receipt=?,seal=? WHERE identity=?",{candidate[18],candidate[19],metadata[0]});
        if(w.changes()!=1)refuse("baseline completed stage publication refused");stage=std::move(candidate);
    }else if(text(stage[18])!=original_seal("ordinary-accepted-baseline-passive-staged-result-v1",metadata,15,b))refuse("baseline staged result changed");
    const auto actual=baseline_stage_read(ordinary_export_storage_access::borrow(w),text(metadata[0]),b);if(actual!=stage)refuse("baseline END postimage differs");
    const auto current_stage_bytes=baseline_capacity_postimage_bytes(actual);
    if(current_stage_bytes<previous_stage_bytes)refuse("baseline END removed passive custody");issued.bytes+=current_stage_bytes-previous_stage_bytes;
    baseline_capacity_publish(w,capacity,issued,b,true);baseline_stage_post(w,stage,b);
    const auto mutations=issued.members+(issued.bytes==0?0:2);
    baseline_capacity_remember(w,witness,turn,static_cast<std::size_t>(mutations),b);result.staged_cut_seal=text(stage[18]);managed_pair_deadline(state.end);return result;
}
}

ordinary_managed_baseline_stage ordinary_export_access::begin_managed_baseline_stage(ordinary_managed_baseline_cursor& cursor) {
    auto producer=cursor.checked().held;baseline_turn turn(*producer);validate_managed_baseline_current(cursor);
    ordinary_managed_baseline_stage result(std::make_unique<ordinary_managed_baseline_stage::implementation>());
    auto& held=*result.impl_->held;held.receive.emplace();held.producer=producer;
    auto& cap=*producer->baseline->impl_->held;const auto& route=*cap.route;auto& owner=route.recipient_owner();budget b{wire_limits};
    const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){
        route.validate_lifetime();validate_managed_profile_until(route.recipient_profile(),owner,writer,producer->end);
        ordinary_export_frame declaration;declaration.wire_profile=true;
        capture_projected_tables(ordinary_export_storage_access::borrow(writer),owner.recovery_schemas_,[](auto& out,const auto& p){lattice_db::serialize_property_for_fingerprint(out,p);},route.description().models,declaration,b,false,nullptr,nullptr);
        if(declaration.scope!=cap.observed.image.scope||declaration.schema!=cap.observed.image.schema)refuse("baseline recipient declaration differs");
        if(!cap.receipt->current())refuse("baseline source receipt changed before BEGIN commit");
        held.metadata=baseline_stage_metadata(writer,*producer,route.description().route_key,ipc_route_policy(route.description(),b),route.description().generation,b);
        held.identity=text(held.metadata[0]);baseline_stage_begin_write(writer,held.metadata,b,baseline_capacity_connection_witness(writer),producer->end);
        route.validate_lifetime();validate_managed_profile_until(route.recipient_profile(),owner,writer,producer->end);
        if(!cap.receipt->current())refuse("baseline source receipt changed before BEGIN completion");
    });managed_clean_turn(settled);validate_managed_baseline_current(cursor);held.issued=true;return result;
}
void ordinary_export_access::append_managed_baseline_stage(ordinary_managed_baseline_stage& staged,const ordinary_managed_baseline_page& page) {
    auto& held=*staged.checked().held;auto producer=held.producer;baseline_turn turn(*producer);
    if(!held.issued||page.checked().held->owner!=producer)refuse("baseline page has another actual producer");
    ordinary_managed_baseline_cursor cursor(std::make_unique<ordinary_managed_baseline_cursor::implementation>());cursor.impl_->held=producer;
    validate_managed_baseline_current(cursor);auto& cap=*producer->baseline->impl_->held;const auto& route=*cap.route;auto& owner=route.recipient_owner();budget b{wire_limits};
    const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){
        route.validate_lifetime();validate_managed_profile_until(route.recipient_profile(),owner,writer,producer->end);
        if(!cap.receipt->current())refuse("baseline source receipt changed before DATA");
        baseline_stage_append_write(writer,held.metadata,page,b,producer->end,baseline_capacity_connection_witness(writer));
        route.validate_lifetime();validate_managed_profile_until(route.recipient_profile(),owner,writer,producer->end);
        if(!cap.receipt->current())refuse("baseline source receipt changed before DATA commit");
    });managed_clean_turn(settled);validate_managed_baseline_current(cursor);
}
ordinary_managed_baseline_staging_result ordinary_export_access::finish_managed_baseline_stage(ordinary_managed_baseline_stage& staged) {
    auto& held=*staged.checked().held;auto producer=held.producer;baseline_turn turn(*producer);if(!held.issued)refuse("baseline test stage cannot report active completion");
    ordinary_managed_baseline_cursor cursor(std::make_unique<ordinary_managed_baseline_cursor::implementation>());cursor.impl_->held=producer;
    validate_managed_baseline_current(cursor,true);auto& cap=*producer->baseline->impl_->held;const auto& route=*cap.route;auto& owner=route.recipient_owner();budget b{wire_limits};
    ordinary_managed_baseline_staging_result result;
    const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){
        route.validate_lifetime();validate_managed_profile_until(route.recipient_profile(),owner,writer,producer->end);
        if(!cap.receipt->current())refuse("baseline source receipt changed before END");
        result=baseline_stage_finish_write(writer,held.metadata,*producer,b,baseline_capacity_connection_witness(writer));
        route.validate_lifetime();validate_managed_profile_until(route.recipient_profile(),owner,writer,producer->end);
        if(!cap.receipt->current())refuse("baseline source receipt changed before END commit");
    });managed_clean_turn(settled);validate_managed_baseline_current(cursor,true);return result;
}

ordinary_managed_baseline_cursor ordinary_export_access::baseline_cursor_for_test(lattice_db& owner,const std::string& scope,ordinary_launch::deadline end) {
    managed_pair_deadline(end);auto accepted=capture_managed_accepted_baseline_for_test(owner,scope);
    ordinary_managed_baseline_cursor result(std::make_unique<ordinary_managed_baseline_cursor::implementation>());
    result.impl_->held->end=end;result.impl_->held->baseline.emplace(std::move(accepted));result.impl_->held->initialize();return result;
}
ordinary_managed_baseline_stage ordinary_export_access::baseline_stage_for_test(lattice_db& owner,ordinary_managed_baseline_cursor& cursor) {
    auto producer=cursor.checked().held;baseline_turn turn(*producer);managed_pair_deadline(producer->end);
    if(producer->issued)refuse("baseline passive fixture cannot consume production capability");
    ordinary_managed_baseline_stage result(std::make_unique<ordinary_managed_baseline_stage::implementation>());
    auto& held=*result.impl_->held;held.receive.emplace();held.producer=producer;budget b{wire_limits};
    const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){
        held.metadata=baseline_stage_metadata(writer,*producer,"actual-storage-fixture",std::string(64,'0'),"passive-no-route-issuer",b);
        held.identity=text(held.metadata[0]);baseline_stage_begin_write(writer,held.metadata,b,baseline_capacity_connection_witness(writer),producer->end);managed_pair_deadline(producer->end);
    });managed_clean_turn(settled);return result;
}
void ordinary_export_access::baseline_append_for_test(lattice_db& owner,ordinary_managed_baseline_stage& staged,const ordinary_managed_baseline_page& page) {
    auto& held=*staged.checked().held;auto producer=held.producer;baseline_turn turn(*producer);
    if(held.issued||producer->issued||page.checked().held->owner!=producer)refuse("baseline passive fixture producer mismatch");budget b{wire_limits};
    const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){baseline_stage_append_write(writer,held.metadata,page,b,producer->end,baseline_capacity_connection_witness(writer));});managed_clean_turn(settled);
}
ordinary_managed_baseline_staging_result ordinary_export_access::baseline_finish_for_test(lattice_db& owner,ordinary_managed_baseline_stage& staged) {
    auto& held=*staged.checked().held;auto producer=held.producer;baseline_turn turn(*producer);
    if(held.issued||producer->issued)refuse("baseline passive fixture cannot select production completion");budget b{wire_limits};ordinary_managed_baseline_staging_result result;
    const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){result=baseline_stage_finish_write(writer,held.metadata,*producer,b,baseline_capacity_connection_witness(writer));});managed_clean_turn(settled);return result;
}
void ordinary_export_access::baseline_reopen_for_test(lattice_db& owner,const std::string& identity) {
    const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(10);budget b{wire_limits};
    const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){
        original_view(writer,true);if(!baseline_stage_storage(ordinary_export_storage_access::borrow(writer),b))refuse("baseline persisted stage missing");
        (void)baseline_capacity_admit(writer,baseline_capacity_connection_witness(writer),b,end,true);const auto stage=baseline_stage_read(ordinary_export_storage_access::borrow(writer),identity,b);
        const auto physical=writer.physical_identity("main",{},true);
        if(!physical||text(stage[11])!=std::to_string(physical->device)||text(stage[12])!=std::to_string(physical->inode))refuse("baseline stage physical owner changed");
        baseline_stage_stream(ordinary_export_storage_access::borrow(writer),stage,b,end);
        baseline_stage_members_reopen(ordinary_export_storage_access::borrow(writer),stage,end,b);
    });managed_clean_turn(settled);
}

std::shared_ptr<void>& ordinary_export_access::baseline_capacity_connection_witness(database& writer) {
    // This physical-connection state is privately allocated by database and
    // transferred with db_ on moves; callers cannot supply/restore it. Public
    // raw-handle escape permanently refuses this strict issuer path.
    if(!writer.retirement_||writer.retirement_->state()!=database_retirement_state::phase::live||writer.retirement_->raw_handle_escaped())
        refuse("baseline exact nonescaped writer witness unavailable");
    const auto held=std::static_pointer_cast<baseline_capacity_witness>(writer.retirement_->ordinary_baseline_capacity_witness_);
    if(held&&held->creator!=managed_pair_creator())refuse("baseline inherited physical-writer capacity cell");
    return writer.retirement_->ordinary_baseline_capacity_witness_;
}

void ordinary_export_access::baseline_capacity_drift_for_test(lattice_db& owner) {
    // Fixed deliberate integrity fault in the private real-storage fixture.
    // No count parameter, production issuance, route or returned authority.
    budget b{wire_limits};
    const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){
        original_view(writer,true);if(!baseline_stage_storage(ordinary_export_storage_access::borrow(writer),b))refuse("baseline drift fixture missing actual layout");
        auto row=baseline_capacity_read(ordinary_export_storage_access::borrow(writer),b,true);row[4]=integer(row[4])+1;
        row[6]=original_seal("ordinary-accepted-baseline-capacity-v1",row,6,b);
        register_mutation_guard(ordinary_export_storage_access::borrow(writer));mutation_scope permit(ordinary_export_storage_access::borrow(writer));
        writer.execute("UPDATE main._lattice_ordered_baseline_capacity_v1 SET bytes=?,seal=? WHERE id='capacity'",{row[4],row[6]});
        if(writer.changes()!=1)refuse("baseline fixed counter drift was not installed");
    });managed_clean_turn(settled);
}
