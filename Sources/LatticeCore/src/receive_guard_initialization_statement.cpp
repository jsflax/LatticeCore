#include "receive_guard_initialization_statement.hpp"
#include "receive_delivery_guard.hpp"
#include "database_initialization_budget.hpp"
#include "database_sql_dispatcher.hpp"
#include <lattice/detail/observer_delivery_startup.hpp>
#include <lattice/native_retirement_storage.hpp>
#include <lattice/store_physical_api.hpp>
#include <array>
#include <atomic>
#include <cstring>
#include <limits>
#include <thread>

namespace lattice::detail {
namespace {
using kind=receive_guard_initialization_statement::kind;
constexpr receive_guard_limits caps{};
constexpr const char* store_name="_lattice_receive_guard_store";
constexpr const char* channel_name="_lattice_receive_guard";
constexpr const char* index_name="_lattice_receive_guard_state";
struct recipe {const char* sql;std::uint64_t columns,rows,bytes;};
recipe generate(kind id) {
    switch(id) {
    case kind::schema_inventory:return {"SELECT name,sql FROM main.sqlite_master WHERE name IN ('_lattice_receive_guard_store','_lattice_receive_guard','_lattice_receive_guard_state') ORDER BY name LIMIT 4",2,4,8192};
    case kind::store_row:return {"SELECT id,version,legacy_origin,last_incarnation,channels,channel_bytes,capacity_refused FROM main._lattice_receive_guard_store LIMIT 2",7,2,8192};
    case kind::channel_count_bytes:return {"SELECT COUNT(*) AS actual_count,COALESCE(SUM(length(channel)),0) AS actual_bytes FROM (SELECT channel FROM main._lattice_receive_guard LIMIT 257)",2,1,4096};
    case kind::channel_keys:return {"SELECT channel FROM main._lattice_receive_guard LIMIT 257",1,257,1048576+257*1024};
    case kind::channel_row:return {"SELECT incarnation,generation,state,reason,checkpoint FROM main._lattice_receive_guard WHERE channel=?",5,2,16384};
    case kind::create_store:return {ordinary_receive_recipe_access::store(),0,0,0};
    case kind::create_channel:return {ordinary_receive_recipe_access::channel(),0,0,0};
    case kind::create_state_index:return {ordinary_receive_recipe_access::index(),0,0,0};
    case kind::seed_store:return {"INSERT INTO main._lattice_receive_guard_store VALUES(1,1,?,0,0,0,0)",0,0,0};
    }
    throw db_error("receive initializer recipe is not closed");
}
bool equal(const char* a,const char* b)noexcept{return a&&b&&sqlite3_stricmp(a,b)==0;}
[[noreturn]] void refuse(const char* text){throw db_error(std::string("receive guard: ")+text);}
const column_value_t& cell(const database::row_t& row,const char* name) {
    const auto found=row.find(name);if(found==row.end())refuse("missing fixed result column");return found->second;
}
std::int64_t integer(const database::row_t& row,const char* name) {
    const auto* value=std::get_if<std::int64_t>(&cell(row,name));if(!value)refuse("expected integer state");return *value;
}
struct store_state {bool legacy=false,overflow=false;std::int64_t sequence=0,count=0,bytes=0,version=1;};
store_state parse_store(const std::vector<database::row_t>& rows) {
    if(rows.size()!=1)refuse("missing or unsupported store version");const auto& row=rows.front();
    const auto version=integer(row,"version"),legacy=integer(row,"legacy_origin"),overflow=integer(row,"capacity_refused");
    if(integer(row,"id")!=1||(version!=1&&version!=2))refuse("missing or unsupported store version");
    store_state s{legacy==1,overflow==1,integer(row,"last_incarnation"),integer(row,"channels"),integer(row,"channel_bytes"),version};
    if((legacy!=0&&legacy!=1)||(overflow!=0&&overflow!=1)||s.sequence<0||s.count<0||s.count>caps.channels||
       s.bytes<0||s.bytes>caps.channel_bytes||s.sequence<s.count)refuse("invalid bounded store counters");
    return s;
}
}
struct receive_guard_initialization_statement::snapshot final {
    const std::uint64_t creator;
    const std::thread::id builder=std::this_thread::get_id();
    database* owner=nullptr; // exact retained constructor owner; dispatch/comparison only
    std::shared_ptr<const observer_delivery_open_startup> open;
    std::shared_ptr<const database_initialization_episode> episode;
    std::atomic<bool> active{false};
    explicit snapshot(std::uint64_t issuer):creator(issuer){}
};
struct receive_guard_initialization_statement::storage final {
    using phase=observer_delivery_open_startup::receive_preimage_phase;
    const std::uint64_t creator;
    std::shared_ptr<snapshot> view;
    std::shared_ptr<const observer_delivery_initialization_preimage> proof;
    phase proof_phase=phase::read_existing;
    std::uint64_t revision=0;
    kind id=kind::schema_inventory;
    recipe actual{};
    std::vector<column_value_t> params;
    explicit storage(std::uint64_t issuer):creator(issuer){}
};
struct receive_guard_initialization_reader::state final {
    using phase=observer_delivery_open_startup::receive_preimage_phase;
    const std::uint64_t creator;
    const mode operation;
    database* owner=nullptr;
    std::shared_ptr<receive_guard_initialization_statement::snapshot> view;
    std::shared_ptr<const observer_delivery_initialization_preimage> proof;
    phase proof_phase=phase::read_existing;
    std::vector<database::row_t> rows,keys;
    native_retirement_storage<std::shared_ptr<const receive_guard_initialization_statement>> issued;
    state(std::uint64_t issuer,mode selected):creator(issuer),operation(selected){}
};
receive_guard_initialization_statement::receive_guard_initialization_statement(std::uint64_t issuer,storage* held)noexcept
    :creator_(issuer),storage_(held){}
receive_guard_initialization_statement::~receive_guard_initialization_statement()noexcept {
    if(creator_==database_native_process())delete storage_;
}
bool receive_guard_initialization_statement::creator_current()const noexcept {
    return creator_==database_native_process()&&storage_&&storage_->creator==creator_&&storage_->view&&
        storage_->view->creator==creator_&&storage_->view->builder==std::this_thread::get_id();
}
bool receive_guard_initialization_statement::work_current_for_(const database& actual)const noexcept {
    using phase=observer_delivery_open_startup::receive_preimage_phase;
    if(!creator_current()||!actual.creator_current_()||actual.catalog_quarantined_())return false;
    const auto& view=*storage_->view;
    if(!view.active.load(std::memory_order_acquire)||view.owner!=&actual||!view.open||!view.episode||
       actual.delivery_startup_.get()!=view.open.get()||!actual.initialization_snapshot_current_(*view.episode,*view.open))return false;
    std::uint64_t revision=0;
    if(!actual.initialization_schema_revision_(*view.episode,*view.open,revision)||revision!=storage_->revision)return false;
    if(storage_->id==kind::schema_inventory)return true;
    if(!view.open->preimage_receive_phase_for_(actual,storage_->proof,storage_->proof_phase))return false;
    const char* plain=nullptr;
    switch(storage_->id) {
    case kind::store_row:plain=store_name;break;
    case kind::channel_count_bytes:case kind::channel_keys:case kind::channel_row:plain=channel_name;break;
    case kind::seed_store:
        return storage_->proof_phase==phase::strict_seed&&view.open->preimage_receive_seed_for_(actual,storage_->proof);
    case kind::create_store:return storage_->proof_phase==phase::before_create_store;
    case kind::create_channel:return storage_->proof_phase==phase::before_create_channel;
    case kind::create_state_index:return storage_->proof_phase==phase::before_create_index;
    default:return false;
    }
    return view.open->preimage_plain_table_for_(actual,storage_->proof,plain);
}
void receive_guard_initialization_statement::check_for_(const database& actual)const {
    if(!work_current_for_(actual))refuse("initializer statement lost its actual open/BEGIN/revision/preimage");
    actual.require_creator_();storage_->view->open->require_work_current_(actual);
}
receive_guard_initialization_statement::kind receive_guard_initialization_statement::kind_id()const {
    if(!creator_current())refuse("inherited initializer kind");return storage_->id;
}
std::string_view receive_guard_initialization_statement::sql()const {
    if(!creator_current())refuse("inherited initializer SQL");return storage_->actual.sql;
}
const std::vector<column_value_t>& receive_guard_initialization_statement::parameters()const {
    if(!creator_current())refuse("inherited initializer parameters");return storage_->params;
}
std::uint64_t receive_guard_initialization_statement::column_limit()const {
    if(!creator_current())refuse("inherited initializer columns");return storage_->actual.columns;
}
std::uint64_t receive_guard_initialization_statement::row_limit()const {
    if(!creator_current())refuse("inherited initializer rows");return storage_->actual.rows;
}
std::uint64_t receive_guard_initialization_statement::result_byte_limit()const {
    if(!creator_current())refuse("inherited initializer bytes");return storage_->actual.bytes;
}
bool receive_guard_initialization_statement::schema_may_change_()const noexcept {
    // Denial-only invalidation; seed INSERT cannot alter schema because its
    // exact strict no-trigger preimage is consumed before the real effect.
    if(!creator_current())return true;
    return storage_->id==kind::create_store||storage_->id==kind::create_channel||storage_->id==kind::create_state_index;
}
int receive_guard_initialization_statement::authorize_(int action,const char* first,const char* second,
    const char* schema,const char* origin)const noexcept {
    if(!creator_current()||!storage_->view->active.load(std::memory_order_acquire)||(origin&&*origin))return SQLITE_DENY;
    const auto id=storage_->id;const bool main=equal(schema,"main");
    const bool ddl=id==kind::create_store||id==kind::create_channel||id==kind::create_state_index;
    const bool catalog=equal(first,"sqlite_master")||equal(first,"sqlite_schema");
    if(action==SQLITE_SELECT)return id<=kind::channel_row?SQLITE_OK:SQLITE_DENY;
    if(action==SQLITE_FUNCTION) {
        if(id==kind::channel_count_bytes&&(equal(second,"count")||equal(second,"sum")||equal(second,"length")||equal(second,"coalesce")))return SQLITE_OK;
        if(id==kind::create_channel&&(equal(second,"typeof")||equal(second,"length")))return SQLITE_OK;
        return SQLITE_DENY;
    }
    if(action==SQLITE_READ) {
        if(!main)return SQLITE_DENY;
        if(id==kind::schema_inventory&&catalog&&(equal(second,"name")||equal(second,"sql")))return SQLITE_OK;
        if(id==kind::store_row&&equal(first,store_name)) {
            for(const char* column:{"id","version","legacy_origin","last_incarnation","channels","channel_bytes","capacity_refused"})
                if(equal(second,column))return SQLITE_OK;
        }
        if((id==kind::channel_count_bytes||id==kind::channel_keys)&&equal(first,channel_name)&&
           (equal(second,"channel")||(second&&!*second)))return SQLITE_OK;
        if(id==kind::channel_row&&equal(first,channel_name)) {
            for(const char* column:{"channel","incarnation","generation","state","reason","checkpoint"})
                if(equal(second,column))return SQLITE_OK;
        }
        if(ddl&&catalog&&equal(second,"ROWID"))return SQLITE_OK;
        if(id==kind::create_state_index&&equal(first,channel_name)&&equal(second,"state"))return SQLITE_OK;
        if(id==kind::create_store&&equal(first,store_name)&&equal(second,"id"))return SQLITE_OK;
        if(id==kind::create_channel&&equal(first,channel_name)&&
           (equal(second,"channel")||equal(second,"incarnation")))return SQLITE_OK;
        if(id==kind::seed_store&&equal(first,store_name)&&equal(second,"id"))return SQLITE_OK;
        return SQLITE_DENY;
    }
    if(!main)return SQLITE_DENY;
    if(action==SQLITE_INSERT) {
        if(ddl&&catalog)return SQLITE_OK;
        return id==kind::seed_store&&equal(first,store_name)?SQLITE_OK:SQLITE_DENY;
    }
    if(action==SQLITE_UPDATE&&ddl&&catalog) {
        for(const char* column:{"type","name","tbl_name","rootpage","sql"})if(equal(second,column))return SQLITE_OK;
        return SQLITE_DENY;
    }
    if(action==SQLITE_CREATE_TABLE)return (id==kind::create_store&&equal(first,store_name))||
        (id==kind::create_channel&&equal(first,channel_name))?SQLITE_OK:SQLITE_DENY;
    if(action==SQLITE_CREATE_INDEX) {
        if(id==kind::create_state_index&&equal(first,index_name)&&equal(second,channel_name))return SQLITE_OK;
        if(id==kind::create_channel&&equal(second,channel_name)&&
           (equal(first,"sqlite_autoindex__lattice_receive_guard_1")||equal(first,"sqlite_autoindex__lattice_receive_guard_2")))return SQLITE_OK;
    }
    if(action==SQLITE_REINDEX&&id==kind::create_state_index&&equal(first,index_name))return SQLITE_OK;
    return SQLITE_DENY;
}
receive_guard_initialization_reader::receive_guard_initialization_reader(std::uint64_t issuer,state* held)noexcept
    :creator_(issuer),state_(held){}
receive_guard_initialization_reader::~receive_guard_initialization_reader()noexcept {
    if(creator_!=database_native_process()||!state_)return;
    if(state_->view)state_->view->active.store(false,std::memory_order_release);
    delete state_;
}
void receive_guard_initialization_reader::require_current_()const {
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_||!state_->owner||!state_->view)
        refuse("inherited initializer reader");
    auto& actual=*state_->owner;actual.require_creator_();const auto& view=*state_->view;
    if(view.builder!=std::this_thread::get_id()||!view.active.load(std::memory_order_acquire)||!view.open||!view.episode||
       actual.delivery_startup_.get()!=view.open.get()||!actual.initialization_snapshot_current_(*view.episode,*view.open))
        refuse("initializer reader lost actual live BEGIN/open");
    view.open->require_work_current_(actual);
}
void receive_guard_initialization_reader::require_write_()const {
    require_current_();auto& actual=*state_->owner;
    store_physical_native_scope native(actual);if(!native.entered())refuse("initializer Native original unavailable");
    database::catalog_native_admission_ leaf(actual);require_current_();
    auto* h=actual.internal_handle();if(!h)refuse("initializer writer unavailable");
    const int autocommit=sqlite3_get_autocommit(h);require_current_();
    const int transaction=sqlite3_txn_state(h,"main");require_current_();
    if(autocommit||transaction!=SQLITE_TXN_WRITE)refuse("schema initialization requires owned bootstrap WRITE");
}
void receive_guard_initialization_reader::fail_()const noexcept {
    if(creator_==database_native_process()&&state_&&state_->creator==creator_&&state_->view&&state_->view->open)
        state_->view->open->fail_initialization_();
}
void receive_guard_initialization_reader::capture_(std::uint8_t selected) {
    using phase=observer_delivery_open_startup::receive_preimage_phase;
    const auto actual_phase=static_cast<phase>(selected);
    if(creator_!=database_native_process()||!state_)refuse("inherited receive capture mode");
    if(state_->operation==mode::final_observation) {
        require_current_();if(actual_phase!=phase::read_existing)refuse("final receive observation cannot select a write prefix");
    }else require_write_();
    state_->proof.reset();require_current_();
    state_->proof=state_->owner->delivery_startup_->capture_receive_guard_preimage_(*state_->owner,actual_phase);
    require_current_();if(!state_->proof)refuse("receive preimage absent");state_->proof_phase=actual_phase;
}
void receive_guard_initialization_reader::issue_(kind id,const std::vector<std::uint8_t>* channel,bool legacy,bool write) {
    if(creator_!=database_native_process()||!state_)refuse("inherited receive issue mode");
    if(state_->operation==mode::final_observation) {
        require_current_();if(write||id>=kind::create_store)refuse("final receive observation has no DML/DDL operator");
    }else require_write_();
    auto& actual=*state_->owner;auto& view=*state_->view;
    if((id>=kind::create_store)!=write||(id==kind::channel_row)!=(channel!=nullptr))refuse("receive closed recipe mode mismatch");
    if(channel&&(channel->empty()||channel->size()>std::uint64_t(caps.key_bytes)))refuse("opaque key exceeds qualification bound");
    const auto bytes=channel?channel->size():0;
    view.open->claim_workspace_(actual,sizeof(receive_guard_initialization_statement::storage)+
        sizeof(receive_guard_initialization_statement)+128+sizeof(column_value_t)+bytes);
    require_current_();
    state_->rows.clear();require_current_();state_->issued.release<0>();require_current_();
    receive_guard_initialization_statement::storage* held=nullptr;
    try {
        held=new receive_guard_initialization_statement::storage(creator_);require_current_();
        held->view=state_->view;held->proof=state_->proof;held->proof_phase=state_->proof_phase;
        held->id=id;held->actual=generate(id);
        if(!actual.initialization_schema_revision_(*view.episode,*view.open,held->revision))refuse("receive schema revision absent");
        if(channel) {held->params.reserve(1);require_current_();held->params.emplace_back(*channel);require_current_();}
        if(id==kind::seed_store) {held->params.reserve(1);require_current_();held->params.emplace_back(std::int64_t(legacy));require_current_();}
        auto* raw=new receive_guard_initialization_statement(creator_,held);held=nullptr;require_current_();
        state_->issued.get<0>()=std::shared_ptr<const receive_guard_initialization_statement>(raw,
            [issuer=creator_](auto* value)noexcept{if(issuer==database_native_process())delete value;});
        require_current_();const auto& statement=state_->issued.get<0>();statement->check_for_(actual);
        auto descriptor=observer_delivery_initialization_sql::from_receive_guard_(statement,*view.open,actual);
        if(write)actual.execute_initialization_(descriptor);
        else state_->rows=actual.query_initialization_(descriptor);
        require_current_();
        // The descriptor has now returned. No proof is reused after a DDL
        // return: the next transition captures its actual new prefix afresh.
    }catch(...) {if(creator_==database_native_process())delete held;fail_();throw;}
}
const std::vector<database::row_t>& receive_guard_initialization_reader::query_(kind id,const std::vector<std::uint8_t>* channel) {
    issue_(id,channel,false,false);return state_->rows;
}
void receive_guard_initialization_reader::execute_(kind id,bool legacy){issue_(id,nullptr,legacy,true);}
void receive_guard_initialization_reader::initialize_(bool legacy) {
    using phase=observer_delivery_open_startup::receive_preimage_phase;
    const auto inspect=[&]() {
        const auto& rows=query_(kind::schema_inventory);if(rows.empty())return false;
        if(rows.size()!=3)refuse("missing or partial schema");
        const char* names[]={channel_name,index_name,store_name};
        const char* definitions[]={ordinary_receive_recipe_access::channel(),ordinary_receive_recipe_access::index(),ordinary_receive_recipe_access::store()};
        for(std::size_t i=0;i<3;++i) {
            const auto* name=std::get_if<std::string>(&cell(rows[i],"name"));
            const auto* sql=std::get_if<std::string>(&cell(rows[i],"sql"));
            if(!name||!sql||*name!=names[i]||*sql!=definitions[i])refuse("unsupported schema or index");
        }
        return true;
    };
    if(!inspect()) {
        if(state_->operation==mode::final_observation)refuse("final required receive family is absent");
        capture_(std::uint8_t(phase::before_create_store));execute_(kind::create_store);
        capture_(std::uint8_t(phase::before_create_channel));execute_(kind::create_channel);
        capture_(std::uint8_t(phase::before_create_index));execute_(kind::create_state_index);
        capture_(std::uint8_t(phase::strict_seed));execute_(kind::seed_store,legacy);
        if(!inspect())refuse("bootstrap family vanished");
        capture_(std::uint8_t(phase::read_existing));const auto s=parse_store(query_(kind::store_row));
        if(s.legacy!=legacy||s.sequence||s.count||s.bytes||s.overflow)refuse("bootstrap postimage mismatch");
        return;
    }
    capture_(std::uint8_t(phase::read_existing));const auto s=parse_store(query_(kind::store_row));
    const auto& counts=query_(kind::channel_count_bytes);
    if(counts.size()!=1||integer(counts.front(),"actual_count")!=s.count||integer(counts.front(),"actual_bytes")!=s.bytes)
        refuse("guard count/byte audit mismatch");
    query_(kind::channel_keys);state_->keys=std::move(state_->rows);require_current_();
    for(const auto& row:state_->keys) {
        require_current_();const auto* channel=std::get_if<std::vector<std::uint8_t>>(&cell(row,"channel"));
        if(!channel||channel->empty()||channel->size()>std::uint64_t(caps.key_bytes))refuse("NULL or invalid channel key");
        // Preserve the original per-key schema + store + point audit, rather
        // than replacing it with stored counters or a copied-row shortcut.
        if(!inspect())refuse("guard schema vanished");const auto current=parse_store(query_(kind::store_row));
        const auto& points=query_(kind::channel_row,channel);if(points.empty())continue;
        if(points.size()!=1)refuse("duplicate channel row");const auto& point=points.front();
        const auto incarnation=integer(point,"incarnation"),generation=integer(point,"generation"),
            state=integer(point,"state"),reason=integer(point,"reason");
        if(incarnation<=0||incarnation>current.sequence||generation<=0||state<0||state>(current.version==2?4:3)||reason<0||reason>4)
            refuse("malformed channel state");
        const auto& checkpoint=cell(point,"checkpoint");const auto* blob=std::get_if<std::vector<std::uint8_t>>(&checkpoint);
        const bool absent=std::holds_alternative<std::nullptr_t>(checkpoint);
        if(!absent&&(!blob||blob->empty()||blob->size()>std::uint64_t(caps.checkpoint_bytes)))refuse("expected bounded opaque BLOB checkpoint");
        if((state<=1&&reason!=0)||(state==2&&(reason==0||reason==4))||(state==3&&reason==0))refuse("inconsistent state/reason");
        if(state==4&&(current.legacy||reason!=0||!absent))refuse("inconsistent canonical receive state");
    }
}
bool receive_guard_initialization_reader::initialize_if_pending_(database& actual,bool legacy) {
    return run_pending_(actual,legacy,mode::initialize);
}
bool receive_guard_initialization_reader::observe_final_pending_(database& actual) {
    // A real final-role observer reuses every existing exact schema/store/
    // key/point/counter check. It owns no BEGIN, ending or write operator.
    return run_pending_(actual,false,mode::final_observation);
}
bool receive_guard_initialization_reader::run_pending_(database& actual,bool legacy,mode operation) {
    actual.require_creator_();if(!actual.delivery_startup_)return false;
    auto* open=actual.delivery_startup_.get();
    open->require_work_current_(actual);
    if(!open->pending_for_(actual))refuse("installed receive initializer is not pending");
    if(actual.initialization_callback_active_())refuse("initialization refuses SQLite callback reentry");
    const auto issuer=database_native_process();state* held=nullptr;receive_guard_initialization_reader* result=nullptr;
    try {
        // Both retained key rows and all per-key descriptor/parameter copies
        // have fixed independent maximums. Charge before allocating any of
        // them; actual Request also meters SQL/result ownership before copies.
        constexpr std::uint64_t keys=1048576+257*1024;
        constexpr std::uint64_t parameters=256*(4096+3*1024);
        open->claim_workspace_(actual,keys+parameters+sizeof(state)+sizeof(receive_guard_initialization_statement::snapshot)+
            native_retirement_storage<std::shared_ptr<const receive_guard_initialization_statement>>::allocation_bytes());
        actual.require_creator_();if(issuer!=database_native_process())refuse("inherited initializer admission");
        held=new state(issuer,operation);if(issuer!=database_native_process())refuse("inherited initializer state allocation");
        actual.require_creator_();held->owner=&actual;
        auto* raw=new receive_guard_initialization_statement::snapshot(issuer);
        if(issuer!=database_native_process())refuse("inherited initializer snapshot allocation");
        held->view=std::shared_ptr<receive_guard_initialization_statement::snapshot>(raw,
            [issuer](auto* value)noexcept{if(issuer==database_native_process())delete value;});
        if(issuer!=database_native_process())refuse("inherited initializer snapshot custody");
        raw->owner=&actual;raw->open=open->initialization_original_(actual);
        raw->episode=actual.capture_initialization_snapshot_(*open);
        if(issuer!=database_native_process())refuse("inherited initializer actual snapshot");
        raw->active.store(true,std::memory_order_release);
        result=new receive_guard_initialization_reader(issuer,held);held=nullptr;
        if(issuer!=database_native_process())refuse("inherited initializer wrapper allocation");
        result->initialize_(legacy);delete result;result=nullptr;
        if(issuer!=database_native_process())refuse("inherited initializer ending return");
        actual.require_creator_();return true;
    }catch(...) {
        if(issuer==database_native_process()) {open->fail_initialization_();if(result)delete result;else delete held;}
        throw;
    }
}
}
