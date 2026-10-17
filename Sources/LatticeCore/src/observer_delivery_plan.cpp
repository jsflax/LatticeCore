#include "observer_delivery_plan.hpp"
#include "observer_delivery_original.hpp"
#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <type_traits>

namespace lattice::detail {
uint64_t database_native_process() noexcept;
namespace {
uint64_t add(uint64_t a,uint64_t b) {
    if(a>observer_delivery_meter::integer_ceiling||b>observer_delivery_meter::integer_ceiling-a)
        throw std::length_error("delivery plan input arithmetic overflow");
    return a+b;
}
void identifier(std::string_view name) {
    if(name.empty()||name.size()>255||name.find('\0')!=std::string_view::npos||
       !observer_delivery_meter::valid_utf8(name))
        throw std::length_error("delivery plan input identifier bound");
}
uint64_t value_bytes(const observer_delivery_plan::value_view& value) {
    return std::visit([](const auto& actual)->uint64_t {
        using T=std::decay_t<decltype(actual)>;
        if constexpr(std::is_same_v<T,std::string_view>){
            if(!observer_delivery_meter::valid_utf8(actual))throw std::invalid_argument("delivery plan TEXT must already be UTF8");
            return add(actual.size(),1);
        }
        else if constexpr(std::is_same_v<T,std::span<const uint8_t>>)return actual.size();
        else if constexpr(std::is_same_v<T,double>){
            if(!std::isfinite(actual))throw std::invalid_argument("delivery plan REAL must be finite");
            return 0;
        }
        else return 0;
    },value);
}
bool same_identifier(std::string_view a,std::string_view b)noexcept {
    if(a.size()!=b.size())return false;
    for(size_t i=0;i<a.size();++i) {
        const auto folded=[](char c){return c>='A'&&c<='Z'?char(c+('a'-'A')):c;};
        if(folded(a[i])!=folded(b[i]))return false;
    }
    return true;
}
uint64_t identifier_bytes(std::string_view name) {
    uint64_t bytes=2;for(char c:name)bytes=add(bytes,c=='"'?2:1);return bytes;
}
uint64_t statement_bytes(const observer_delivery_plan::row_view& row) {
    // Exact INSERT emitter below; include its terminator and both parentheses.
    uint64_t bytes=add(32,identifier_bytes(row.table));
    for(const auto& field:row.fields)bytes=add(bytes,add(identifier_bytes(field.name),4));
    return bytes;
}
void quote_identifier(std::string& sql,std::string_view name) {
    sql+='"';for(char c:name){if(c=='"')sql+='"';sql+=c;}sql+='"';
}
observer_delivery_plan::value own(const observer_delivery_plan::value_view& value) {
    return std::visit([](const auto& actual)->observer_delivery_plan::value {
        using T=std::decay_t<decltype(actual)>;
        if constexpr(std::is_same_v<T,std::string_view>)return std::string(actual);
        else if constexpr(std::is_same_v<T,std::span<const uint8_t>>)return std::vector<uint8_t>(actual.begin(),actual.end());
        else return actual;
    },value);
}
}
struct observer_delivery_plan::state {
    const uint64_t creator;
    // Destroy all immutable input data before returning this actual charge.
    observer_delivery_store::lease credit;
    std::shared_ptr<const observer_delivery_original> borrowed;
    mutable std::vector<row> rows;
    bool update=false;size_t update_kind=0;uint64_t update_maximum=0;
    mutable std::atomic<bool> update_supplied{false},update_supply_attempted{false},update_matched{false},update_executed{false};
    mutable std::vector<observer_delivery_write::inserted_row> inserted;
    observer_delivery_meter_budget budget;
    mutable std::atomic<bool> original_issued{false},preparation_issued{false};
    mutable std::atomic<uint64_t> workspace_claimed{0};
    uint64_t bytes=0;
    explicit state(uint64_t original):creator(original){}
    static void destroy(const state* value)noexcept {
        if(value&&value->creator==database_native_process())delete value;
    }
};
uint64_t observer_delivery_plan::minimum_workspace_bytes()noexcept {
    return sizeof(state)+sizeof(observer_delivery_original)+staging_storage_bytes;
}
template<typename Read>
observer_delivery_plan::result observer_delivery_plan::prepare_rows(const observer_delivery_store::client& client,
    observer_delivery_meter_budget budget,observer_delivery_store::clock::time_point end,
    size_t count,const Read& read,const observer_delivery_store::cancellation* stop,bool identities,
    const std::shared_ptr<const observer_delivery_original>& borrowed) {
    const auto creator=database_native_process();
    const auto charge=observer_delivery_meter::admission_charge(budget);
    if(borrowed&&(borrowed->budget_.events!=budget.events||borrowed->budget_.event_bytes!=budget.event_bytes||
       borrowed->budget_.workspace_bytes!=budget.workspace_bytes))
        throw std::invalid_argument("delivery subplan declaration differs from its actual BEGIN envelope");
    if(!count||count>budget.events||count>observer_delivery_meter::integer_ceiling/sizeof(row))
        return {observer_delivery_store::status::oversized,{}};
    // No allocation or collector invocation in this pass. Include actual
    // owned container elements/string bytes/terminators. Row/column ceilings
    // also bound allocation COUNT. This is structured requested storage, not
    // a claim to inspect allocator bookkeeping or arbitrary opaque captures.
    uint64_t fixed=add(minimum_workspace_bytes(),count*sizeof(row));
    if(identities)fixed=add(fixed,count*sizeof(observer_delivery_write::inserted_row));
    const auto measured_row=[identities](const row_view& row)->uint64_t {
        identifier(row.table);
        const bool has_identity=std::any_of(row.fields.begin(),row.fields.end(),[](const auto& field){return field.name=="globalId";});
        const size_t appended=identities&&!has_identity?1:0;
        if(row.fields.empty()||row.fields.size()+appended>observer_delivery_meter::column_ceiling)
            return UINT64_MAX; // Not a representable admitted row.
        uint64_t bytes=add(row.table.size(),1);
        bytes=add(bytes,row.fields.size()*sizeof(field));
        if(appended)bytes=add(bytes,sizeof(field)+9+37+2*(4+identifier_bytes("globalId")));
        for(size_t i=0;i<row.fields.size();++i) {
            const auto& field=row.fields[i];identifier(field.name);
            for(size_t prior=0;prior<i;++prior)
                if(same_identifier(row.fields[prior].name,field.name))throw std::invalid_argument("delivery plan duplicate field");
            const auto actual_bytes=identities&&field.name=="globalId"?UINT64_C(37):value_bytes(field.contents);
            bytes=add(bytes,add(add(field.name.size(),1),actual_bytes));
        }
        return add(bytes,add(statement_bytes(row),statement_bytes(row)));
    };
    uint64_t bytes=fixed;
    observer_delivery_values::field_buffer scratch;
    for(size_t index=0;index<count;++index) {
        const auto measured=measured_row(read(index,scratch));
        if(measured==UINT64_MAX)return {observer_delivery_store::status::oversized,{}};
        bytes=add(bytes,measured);
    }
    if(bytes>budget.workspace_bytes)return {observer_delivery_store::status::oversized,{}};
    // Admission occurs BEFORE state/control-block/vector/string/value copies,
    // and before any SQL/Native/store operation. It has one original end.
    observer_delivery_store::lease credit;
    if(borrowed) {
        // An explicit caller already owns the physical transaction. NEVER
        // wait/reacquire here: spend its upfront declaration monotonically.
        // Failed copies/rollback-to retain that charge until actual ending.
        borrowed->claim_workspace(bytes);
    }else {
        auto admission=client.acquire(charge,end,stop);
        if(admission.code!=observer_delivery_store::status::admitted)return {admission.code,{}};
        credit=std::move(admission.held);
    }
    auto staged=std::shared_ptr<state>(new state(creator),state::destroy);
    staged->credit=std::move(credit);staged->borrowed=borrowed;staged->bytes=bytes;staged->budget=budget;
    staged->workspace_claimed.store(bytes,std::memory_order_relaxed);
    if(creator!=database_native_process())throw std::runtime_error("inherited delivery plan allocation");
    staged->rows.reserve(count);
    if(creator!=database_native_process())throw std::runtime_error("inherited delivery plan row allocation");
    if(identities)staged->inserted.resize(count);
    if(creator!=database_native_process())throw std::runtime_error("inherited delivery result allocation");
    uint64_t copied=fixed;
    observer_delivery_values::field_buffer stable;
    for(size_t row_index=0;row_index<count;++row_index) {
        if(creator!=database_native_process())throw std::runtime_error("inherited delivery plan borrowed input");
        const auto offered=read(row_index,scratch);
        // Snapshot only bounded borrowed metadata, with no allocation/callback.
        // Reentry cannot change a later field's TYPE/extent in this snapshot.
        // The backing bytes must still obey the documented stable lifetime.
        stable.reset();
        for(const auto& field:offered.fields)stable.append(field.name,field.contents);
        const row_view source{offered.table,stable.fields()};
        const auto measured=measured_row(source);
        if(measured==UINT64_MAX||copied>bytes||measured>bytes-copied)
            throw std::length_error("delivery borrowed input grew beyond its admitted storage");
        copied=add(copied,measured); // BEFORE any owning row/field/SQL/value copy.
        // Publish empty DATA custody before any potentially allocating copy;
        // no owning partial row/field lives on a fork-crossing stack.
        staged->rows.emplace_back();auto& actual=staged->rows.back();
        actual.table=source.table;
        if(creator!=database_native_process())throw std::runtime_error("inherited delivery plan table copy");
        const bool has_identity=std::any_of(source.fields.begin(),source.fields.end(),[](const auto& field){return field.name=="globalId";});
        const bool append_identity=identities&&!has_identity;
        actual.fields.reserve(source.fields.size()+(append_identity?1:0));
        if(creator!=database_native_process())throw std::runtime_error("inherited delivery plan field allocation");
        actual.insert_sql.reserve(statement_bytes(source)+(append_identity?4+identifier_bytes("globalId"):0));
        if(creator!=database_native_process())throw std::runtime_error("inherited delivery plan SQL reserve");
        actual.insert_sql="INSERT INTO ";quote_identifier(actual.insert_sql,source.table);actual.insert_sql+='(';
        for(size_t index=0;index<source.fields.size();++index){if(index)actual.insert_sql+=',';quote_identifier(actual.insert_sql,source.fields[index].name);}
        if(append_identity){actual.insert_sql+=',';quote_identifier(actual.insert_sql,"globalId");}
        actual.insert_sql+=") VALUES(";
        for(size_t index=0;index<source.fields.size();++index){if(index)actual.insert_sql+=',';actual.insert_sql+='?';}
        if(append_identity)actual.insert_sql+=",?";
        actual.insert_sql+=')';
        if(creator!=database_native_process())throw std::runtime_error("inherited delivery plan SQL emission");
        for(const auto& field:source.fields){
            actual.fields.emplace_back();auto& owned=actual.fields.back();owned.name=field.name;
            if(creator!=database_native_process())throw std::runtime_error("inherited delivery plan field copy");
            if(identities&&field.name=="globalId")continue;
            const auto bound=value_bytes(field.contents);
            actual.bind_bytes=add(actual.bind_bytes,bound);actual.largest_value_bytes=std::max(actual.largest_value_bytes,bound);
            owned.contents=own(field.contents);
            if(creator!=database_native_process())throw std::runtime_error("inherited delivery plan value copy");
        }
        if(identities) {
            // The fixed36-byte canonical identity is generated only AFTER
            // admission; both its owning field and SQL placeholders were
            // charged before any copy/SQLite work.
            if(append_identity){actual.fields.emplace_back();actual.fields.back().name="globalId";}
            auto& identity=*std::find_if(actual.fields.begin(),actual.fields.end(),
                [](const auto& field){return field.name=="globalId";});
            const auto generated=uuid_t::generate();
            if(creator!=database_native_process())throw std::runtime_error("inherited delivery identity randomness");
            auto& spelling=identity.contents.template emplace<std::string>(36,'\0');
            if(creator!=database_native_process())throw std::runtime_error("inherited delivery identity generation");
            constexpr char hex[]="0123456789abcdef";size_t out=0;
            for(size_t index=0;index<generated.bytes.size();++index) {
                if(index==4||index==6||index==8||index==10)spelling[out++]='-';
                spelling[out++]=hex[generated.bytes[index]>>4];spelling[out++]=hex[generated.bytes[index]&15];
            }
            actual.bind_bytes=add(actual.bind_bytes,37);actual.largest_value_bytes=std::max(actual.largest_value_bytes,UINT64_C(37));
        }
        if(identities) {
            const auto at=std::find_if(actual.fields.begin(),actual.fields.end(),[](const auto& field){return field.name=="globalId";});
            if(at==actual.fields.end()||!std::holds_alternative<std::string>(at->contents))
                throw std::invalid_argument("delivery INSERT requires a TEXT globalId");
            const auto& identity=std::get<std::string>(at->contents);
            if(identity.size()!=36)throw std::invalid_argument("delivery INSERT identity must be canonical UUID");
            for(size_t i=0;i<identity.size();++i) {
                const bool dash=i==8||i==13||i==18||i==23;
                if(dash?identity[i]!='-':!((identity[i]>='0'&&identity[i]<='9')||(identity[i]>='a'&&identity[i]<='f')))
                    throw std::invalid_argument("delivery INSERT identity must be canonical UUID");
            }
            staged->inserted[row_index].global_id=identity;
        }
    }
    return {observer_delivery_store::status::admitted,observer_delivery_plan(std::move(staged))};
}
observer_delivery_plan::result observer_delivery_plan::prepare(const observer_delivery_store::client& client,
    observer_delivery_meter_budget budget,observer_delivery_store::clock::time_point end,
    std::span<const row_view> input,const observer_delivery_store::cancellation* stop) {
    return prepare_rows(client,budget,end,input.size(),
        [input](size_t index,observer_delivery_values::field_buffer&){return input[index];},stop,false);
}
observer_delivery_plan::result observer_delivery_plan::prepare(const observer_delivery_store::client& client,
    observer_delivery_meter_budget budget,observer_delivery_store::clock::time_point end,
    const observer_delivery_values::cursor& input,const observer_delivery_store::cancellation* stop) {
    return prepare_rows(client,budget,end,input.size(),
        [&input](size_t index,observer_delivery_values::field_buffer& scratch){return input.read(index,scratch);},stop,false);
}
observer_delivery_plan::result observer_delivery_plan::prepare_insert(const observer_delivery_store::client& client,
    observer_delivery_meter_budget budget,observer_delivery_store::clock::time_point end,
    const observer_delivery_values::cursor& input,const observer_delivery_store::cancellation* stop) {
    return prepare_rows(client,budget,end,input.size(),
        [&input](size_t index,observer_delivery_values::field_buffer& scratch){return input.read(index,scratch);},stop,true);
}
observer_delivery_plan::result observer_delivery_plan::prepare_insert(const observer_delivery_store::client& client,
    observer_delivery_meter_budget budget,observer_delivery_store::clock::time_point end,
    std::span<const row_view> input,const observer_delivery_store::cancellation* stop) {
    return prepare_rows(client,budget,end,input.size(),
        [input](size_t index,observer_delivery_values::field_buffer&){return input[index];},stop,true);
}
observer_delivery_plan::result observer_delivery_plan::prepare_insert(const observer_delivery_store::client& client,
    observer_delivery_meter_budget budget,const observer_delivery_values::cursor& input,
    const std::shared_ptr<const observer_delivery_original>& borrowed) {
    return prepare_rows(client,budget,observer_delivery_store::clock::time_point{},input.size(),
        [&input](size_t index,observer_delivery_values::field_buffer& scratch){return input.read(index,scratch);},nullptr,true,borrowed);
}
observer_delivery_plan::result observer_delivery_plan::prepare_insert(const observer_delivery_store::client& client,
    observer_delivery_meter_budget budget,std::span<const row_view> input,
    const std::shared_ptr<const observer_delivery_original>& borrowed) {
    return prepare_rows(client,budget,observer_delivery_store::clock::time_point{},input.size(),
        [input](size_t index,observer_delivery_values::field_buffer&){return input[index];},nullptr,true,borrowed);
}
const std::shared_ptr<const observer_delivery_original>& observer_delivery_plan::borrowed_original()const {
    if(!valid())throw std::runtime_error("delivery plan original unavailable");return state_->borrowed;
}
bool observer_delivery_plan::valid()const noexcept {
    return state_&&state_->creator==database_native_process()&&
        (state_->credit.valid()||(state_->borrowed&&state_->borrowed->creator_==state_->creator&&state_->borrowed->credit_.valid()));
}
const std::vector<observer_delivery_plan::row>& observer_delivery_plan::rows()const {
    if(!valid())throw std::runtime_error("delivery plan original unavailable");return state_->rows;
}
uint64_t observer_delivery_plan::input_bytes()const {
    if(!valid())throw std::runtime_error("delivery plan original unavailable");return state_->bytes;
}
observer_delivery_meter_budget observer_delivery_plan::declared_budget()const {
    if(!valid())throw std::runtime_error("delivery plan original unavailable");return state_->budget;
}
observer_delivery_store::lease observer_delivery_plan::take_credit()const {
    if(!valid())throw std::runtime_error("delivery plan original unavailable");
    if(state_->borrowed)throw std::runtime_error("delivery subplan cannot issue/renew its caller original");
    bool unused=false;
    if(!state_->original_issued.compare_exchange_strong(unused,true,std::memory_order_acq_rel))
        throw std::runtime_error("delivery plan already belongs to its original BEGIN");
    return state_->credit;
}
}

namespace lattice::detail {
std::weak_ptr<const void> observer_delivery_plan::input_identity()const {
    if(!valid())throw std::runtime_error("delivery plan original unavailable");return state_;
}
}

namespace lattice::detail {
void observer_delivery_plan::claim_preparation()const {
    if(!valid())throw std::runtime_error("delivery plan original unavailable");
    if(state_->update&&!state_->update_supplied.load(std::memory_order_acquire))
        throw std::runtime_error("delivery UPDATE requires actual supplied immutable value");
    bool unused=false;
    if(!state_->preparation_issued.compare_exchange_strong(unused,true,std::memory_order_acq_rel))
        throw std::runtime_error("delivery input already owns its one prepared shell");
}
void observer_delivery_plan::release_unissued_preparation()const noexcept {
    if(state_&&state_->creator==database_native_process()&&!state_->original_issued.load(std::memory_order_acquire))
        state_->preparation_issued.store(false,std::memory_order_release);
}
void observer_delivery_plan::reserve_output_workspace(uint64_t bytes)const {
    if(!valid())throw std::runtime_error("delivery output input unavailable");
    if(state_->borrowed) {
        state_->borrowed->claim_workspace(bytes);
        state_->workspace_claimed.fetch_add(bytes,std::memory_order_acq_rel);
        return;
    }
    auto current=state_->workspace_claimed.load(std::memory_order_acquire);
    do {
        if(current>state_->budget.workspace_bytes||bytes>state_->budget.workspace_bytes-current)
            throw std::length_error("delivery output exceeds the original admitted workspace");
    }while(!state_->workspace_claimed.compare_exchange_weak(current,current+bytes,std::memory_order_acq_rel));
}
uint64_t observer_delivery_plan::remaining_workspace()const {
    if(!valid())throw std::runtime_error("delivery output input unavailable");
    if(state_->borrowed)return state_->borrowed->remaining_workspace();
    const auto current=state_->workspace_claimed.load(std::memory_order_acquire);
    if(current>state_->budget.workspace_bytes)throw std::runtime_error("delivery original workspace corrupt");
    return state_->budget.workspace_bytes-current;
}
std::span<const observer_delivery_write::inserted_row> observer_delivery_plan::inserted_rows()const {
    if(!valid())throw std::runtime_error("delivery result input unavailable");return state_->inserted;
}
std::string_view observer_delivery_plan::global_id(size_t index)const {
    if(!valid()||index>=state_->inserted.size())throw std::out_of_range("delivery result identity unavailable");
    return state_->inserted[index].global_id;
}
void observer_delivery_plan::record_inserted(size_t index,int64_t id)const {
    if(!valid()||index>=state_->inserted.size()||id<=0)throw std::runtime_error("delivery actual INSERT identity differs");
    if(state_->inserted[index].rowid)throw std::runtime_error("delivery actual INSERT identity already recorded");
    state_->inserted[index].rowid=id;
}
}

namespace lattice::detail {
observer_delivery_plan::result observer_delivery_plan::prepare_update(
    const observer_delivery_store::client& client,observer_delivery_meter_budget budget,
    observer_delivery_store::clock::time_point end,std::string_view table,std::string_view column,
    int64_t rowid,std::string_view global_id,size_t kind,uint64_t maximum,
    const std::shared_ptr<const observer_delivery_original>& borrowed) {
    const auto creator=database_native_process();identifier(table);identifier(column);
    if(rowid<=0||global_id.empty()||global_id.size()>budget.workspace_bytes||
       !observer_delivery_meter::valid_utf8(global_id)||kind>4||
       (kind<3&&maximum!=0)||budget.events<2)
        throw std::invalid_argument("delivery UPDATE declaration differs from bound builtin target");
    if(borrowed&&(borrowed->budget_.events!=budget.events||borrowed->budget_.event_bytes!=budget.event_bytes||
       borrowed->budget_.workspace_bytes!=budget.workspace_bytes))
        throw std::invalid_argument("delivery UPDATE differs from actual BEGIN declaration");
    // One known row/field, immutable target, two generated statements and their
    // prospective strings. Two value extents cover conversion + owning copy;
    // the terminators are distinct. No SQLite internal cache/RSS claim is made.
    uint64_t bytes=add(minimum_workspace_bytes(),sizeof(row)+sizeof(field));
    bytes=add(bytes,add(add(add(table.size(),1),add(column.size(),1)),add(global_id.size(),1)));
    bytes=add(bytes,add(256,add(4*identifier_bytes(table),2*identifier_bytes(column))));
    bytes=add(bytes,add(add(maximum,maximum),2));
    if(bytes>budget.workspace_bytes)return {observer_delivery_store::status::oversized,{}};
    observer_delivery_store::lease credit;
    if(borrowed)borrowed->claim_workspace(bytes);
    else {
        auto actual=client.acquire(observer_delivery_meter::admission_charge(budget),end);
        if(actual.code!=observer_delivery_store::status::admitted)return {actual.code,{}};
        credit=std::move(actual.held);
    }
    auto staged=std::shared_ptr<state>(new state(creator),state::destroy);
    if(creator!=database_native_process())throw std::runtime_error("inherited delivery UPDATE shell");
    staged->credit=std::move(credit);staged->borrowed=borrowed;staged->budget=budget;
    staged->bytes=bytes;staged->workspace_claimed.store(bytes,std::memory_order_relaxed);
    staged->update=true;staged->update_kind=kind;staged->update_maximum=maximum;
    staged->rows.emplace_back();if(creator!=database_native_process())throw std::runtime_error("inherited delivery UPDATE row");
    auto& row=staged->rows.back();row.is_update=true;row.target_rowid=rowid;
    row.table=table;if(creator!=database_native_process())throw std::runtime_error("inherited delivery UPDATE table");
    row.target_global_id=global_id;if(creator!=database_native_process())throw std::runtime_error("inherited delivery UPDATE identity");
    row.fields.emplace_back();if(creator!=database_native_process())throw std::runtime_error("inherited delivery UPDATE field");
    row.fields[0].name=column;if(creator!=database_native_process())throw std::runtime_error("inherited delivery UPDATE column");
    row.identity_sql.reserve(128+2*identifier_bytes(table));
    if(creator!=database_native_process())throw std::runtime_error("inherited delivery UPDATE comparison reserve");
    row.identity_sql="SELECT \"globalId\"=? FROM ";quote_identifier(row.identity_sql,row.table);
    row.identity_sql+=" WHERE \"id\"=?";
    row.insert_sql.reserve(128+identifier_bytes(table)+identifier_bytes(column));
    if(creator!=database_native_process())throw std::runtime_error("inherited delivery UPDATE SQL reserve");
    row.insert_sql="UPDATE ";quote_identifier(row.insert_sql,row.table);row.insert_sql+=" SET ";
    quote_identifier(row.insert_sql,row.fields[0].name);row.insert_sql+="=? WHERE \"id\"=? AND \"globalId\"=?";
    if(creator!=database_native_process())throw std::runtime_error("inherited delivery UPDATE SQL emission");
    return {observer_delivery_store::status::admitted,observer_delivery_plan(std::move(staged))};
}
void observer_delivery_plan::supply_update(const value_view& input)const {
    if(!valid()||!state_->update)
        throw std::invalid_argument("delivery UPDATE allowance is unavailable");
    bool unused=false;
    if(!state_->update_supply_attempted.compare_exchange_strong(unused,true,std::memory_order_acq_rel))
        throw std::runtime_error("delivery UPDATE supply is one use");
    if(input.index()!=state_->update_kind)
        throw std::invalid_argument("delivery UPDATE supplied builtin differs from its admitted kind");
    const auto extent=value_bytes(input); // strict UTF8/finite, allocation free
    const auto payload_extent=std::holds_alternative<std::string_view>(input)?extent-1:extent;
    if(payload_extent>state_->update_maximum)
        throw std::length_error("delivery UPDATE supplied bytes exceed preconversion allowance");
    // The owning target/SQL metadata is already private and retained. The
    // caller's borrowed value has no authority after this one owning copy.
    state_->rows[0].fields[0].contents=own(input);
    if(state_->creator!=database_native_process())throw std::runtime_error("inherited delivery UPDATE value copy");
    state_->rows[0].bind_bytes=add(extent,add(state_->rows[0].target_global_id.size(),1));
    state_->rows[0].largest_value_bytes=std::max(extent,add(state_->rows[0].target_global_id.size(),1));
    state_->update_supplied.store(true,std::memory_order_release);
}
bool observer_delivery_plan::update_row_present()const {
    if(!valid()||!state_->update||!state_->update_executed.load(std::memory_order_acquire))
        throw std::runtime_error("delivery UPDATE actual row result unavailable");
    return state_->update_matched.load(std::memory_order_acquire);
}
void observer_delivery_plan::record_update_present(bool present)const {
    if(!valid()||!state_->update)throw std::runtime_error("delivery UPDATE actual result differs");
    state_->update_matched.store(present,std::memory_order_relaxed);
    state_->update_executed.store(true,std::memory_order_release);
}
}
