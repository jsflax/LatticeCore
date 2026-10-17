#include "ordinary_timestamp.hpp"
#include "ordinary_wire.hpp"
#include "ordinary_export.hpp"
#include "vendor/picosha2/picosha2.h"
#include <nlohmann/json.hpp>
#include <atomic>
#include <cmath>
#include <cstring>
#include <limits>
#include <mutex>
#include <array>

#include "ordinary_capacity_notification.hpp"
#include "ordinary_resource_pool.hpp"
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <thread>
#if defined(__EMSCRIPTEN__)
#include <emscripten/emscripten.h>
#if defined(__EMSCRIPTEN_PTHREADS__)
#include <emscripten/proxying.h>
#include <emscripten/threading.h>
#endif
#elif defined(__APPLE__) || defined(__linux__)
#include <unistd.h>
#else
#include <process.h>
#endif

namespace lattice::detail {
namespace {
uint32_t capacity_process()noexcept {
#if defined(__EMSCRIPTEN__)
    return 1; // The module runtime is the process; there is no fork.
#elif defined(__APPLE__) || defined(__linux__)
    return static_cast<uint32_t>(getpid());
#else
    return static_cast<uint32_t>(_getpid());
#endif
}
// Checked before a function-local initialization guard, registry lock or worker
// can be touched. A fork child cannot borrow the parent's dispatcher.
std::atomic<uint32_t> capacity_creator{0};
std::atomic<bool> capacity_shutdown_requested{false},capacity_initializing{false};
static_assert(std::atomic<uint32_t>::is_always_lock_free);
bool capacity_creator_process()noexcept {
    auto value=capacity_creator.load(std::memory_order_acquire);
    return value&&value==capacity_process();
}
void claim_capacity_process() {
    auto expected=uint32_t(0);const auto current=capacity_process();
    if(!capacity_creator.compare_exchange_strong(expected,current,std::memory_order_acq_rel)&&expected!=current)
        throw db_error("ordinary export capacity service belongs to another process");
}
}
struct ordinary_export_capacity_observation final {
    const uint32_t creator=capacity_process();
    std::atomic<bool> active{true};
    const std::function<void()> callback;
    // Inherited callbacks may own native/Swift handles with inherited locks.
    // At most 64 registered owners are quarantined in a fork child, without
    // invoking callbacks/context destructors or touching an inherited mutex.
    std::atomic<bool> foreign_retained{false};
    std::shared_ptr<ordinary_export_capacity_observation> foreign_keepalive;
    explicit ordinary_export_capacity_observation(std::function<void()> value):callback(std::move(value)){}
};
void drop_capacity_owner(std::shared_ptr<ordinary_export_capacity_observation> owner,bool cancel=false)noexcept {
    if(!owner)return;
    if(cancel||owner->creator!=capacity_process())owner->active.store(false,std::memory_order_release);
    if(owner->creator!=capacity_process()) {
        bool expected=false;
        if(owner->foreign_retained.compare_exchange_strong(expected,true,std::memory_order_acq_rel))
            owner->foreign_keepalive=owner;
    }
    // Last callback/Swift-context owner is released here, outside all locks.
}
namespace {
// Explicit settlement checks each owner independently. If a callback/context
// forks, the child quarantines all remaining inherited callback owners rather
// than invoking their destructors or touching the registry's copied locks.
struct capacity_snapshot final:std::array<std::shared_ptr<ordinary_export_capacity_observation>,64> {
    capacity_snapshot()=default;
    capacity_snapshot(capacity_snapshot&&)=default;
    capacity_snapshot& operator=(capacity_snapshot&&)=default;
    ~capacity_snapshot(){for(auto it=rbegin();it!=rend();++it)drop_capacity_owner(std::move(*it));}
};
struct capacity_registry;
std::atomic<capacity_registry*> capacity_live{nullptr};
thread_local capacity_registry* capacity_dispatching=nullptr;
struct capacity_registry {
    std::mutex mutex;
    std::condition_variable changed;
    std::vector<std::weak_ptr<ordinary_export_capacity_observation>> observers;
    bool active=false,pending=false,queued=false,closing=false,drained=false;
#if !defined(__EMSCRIPTEN__)
    std::mutex join_mutex;
    std::thread worker;
#elif defined(__EMSCRIPTEN_PTHREADS__)
    em_proxying_queue* browser_queue=nullptr;
#endif
    capacity_registry(){observers.reserve(64);}
    capacity_snapshot snapshot_locked()noexcept {
        capacity_snapshot result{};size_t count=0;
        for(auto it=observers.begin();it!=observers.end();) {
            if(auto owner=it->lock()){result[count++]=std::move(owner);++it;}
            else it=observers.erase(it);
        }
        return result;
    }
    void start() {
#if !defined(__EMSCRIPTEN__)
        worker=std::thread([this]{
            for(;;) {
                if(!capacity_creator_process())return; // callback may have forked
                bool terminal;
                {std::unique_lock lock(mutex);changed.wait(lock,[&]{return queued||drained;});
                    if(drained)return;queued=false;terminal=closing;}
                pass(terminal); // One finite snapshot per dispatched turn.
            }
        });
#elif defined(__EMSCRIPTEN_PTHREADS__)
        browser_queue=emscripten_proxying_queue_create();
        if(!browser_queue)throw db_error("ordinary export capacity dispatcher creation failed");
#endif
    }
#if defined(__EMSCRIPTEN__)
    static void browser_turn(void* value)noexcept {
        auto& registry=*static_cast<capacity_registry*>(value);bool terminal;
        {std::lock_guard lock(registry.mutex);registry.queued=false;terminal=registry.closing;}
        registry.pass(terminal);
    }
#if defined(__EMSCRIPTEN_PTHREADS__)
    static void browser_post(void* value)noexcept {
        // A proxy queue may execute newly queued entries in the same drain.
        // Always hand the actual pass to a distinct main-runtime event turn.
        emscripten_async_call(browser_turn,value,0);
    }
#endif
#endif
    void schedule()noexcept {
#if !defined(__EMSCRIPTEN__)
        changed.notify_one();
#elif defined(__EMSCRIPTEN_PTHREADS__)
        if(!emscripten_proxy_async(browser_queue,emscripten_main_runtime_thread_id(),browser_post,this)) {
            // No dropped hint: close admission first and run one terminal pass.
            // SDK callbacks then observe a semantic service-closed refusal.
            {std::lock_guard lock(mutex);queued=false;closing=true;pending=false;}
            pass(true);
        }
#else
        emscripten_async_call(browser_turn,this,0);
#endif
    }
    void pass(bool terminal)noexcept {
        auto* previous=capacity_dispatching;capacity_dispatching=this;
        // Destruction of every snapshot owner, including canceled contexts,
        // occurs while ownership is active but outside every registry lock.
        // Nested releases only set pending. They cannot recursively call pass.
        {
            capacity_snapshot retained;
            {std::lock_guard lock(mutex);pending=false;retained=snapshot_locked();}
            for(const auto& owner:retained)if(owner&&owner->active.load(std::memory_order_acquire)) {
                try{owner->callback();}catch(...){} // release settlement is fixed
                if(!capacity_creator_process())break; // recheck after user code
            }
            if(terminal&&capacity_creator_process())for(const auto& owner:retained)if(owner)owner->active.store(false,std::memory_order_release);
        }
        capacity_dispatching=previous;
        if(!capacity_creator_process())return; // final context may also have forked
        bool handoff=false;
        std::vector<std::weak_ptr<ordinary_export_capacity_observation>> retired;
        {std::lock_guard lock(mutex);
            if(terminal){pending=false;queued=false;active=false;drained=true;retired.swap(observers);}
            else if(pending||closing){queued=true;handoff=true;}
            else active=false;
        }
        // Weak-control storage, callbacks and final owners all settle off-lock.
        retired.clear();changed.notify_all();
        if(handoff)schedule();
    }
    void released()noexcept {
        bool run=false;
        {std::lock_guard lock(mutex);
            if(closing)return;
            pending=true;
            if(!active){active=true;run=true;}
        }
        if(run)pass(false);
    }
    void request_close()noexcept {
        bool run=false;
        {std::lock_guard lock(mutex);
            if(closing)return;
            closing=true;pending=true;
            if(!active){active=true;run=true;}
        }
        if(run)pass(true);
    }
    bool drain()noexcept {
        if(capacity_dispatching==this)return false; // close from a callback defers
#if !defined(__EMSCRIPTEN__)
        // Joining is serialized; callback/final owner settlement precedes drained.
        std::lock_guard join_guard(join_mutex);
        {std::unique_lock lock(mutex);changed.wait(lock,[&]{return drained;});}
        if(worker.joinable())worker.join();
        return true;
#else
        // Never block the browser's main loop. Host must request close, allow
        // the single queued turn to settle, then observe successful drain.
        {std::lock_guard lock(mutex);if(!drained)return false;}
#if defined(__EMSCRIPTEN_PTHREADS__)
        // Only the main runtime owns queue destruction; foreign callers report
        // not drained until the host completes this explicit main-runtime step.
        if(!emscripten_is_main_runtime_thread())return false;
        auto* queue=browser_queue;browser_queue=nullptr;
        if(queue)emscripten_proxying_queue_destroy(queue);
#endif
        return true;
#endif
    }
};
void capacity_exit()noexcept {
    // A fork child must not lock or join copied parent state at exit.
    if(!capacity_creator_process())return;
    if(auto* registry=capacity_live.load(std::memory_order_acquire)) {
        registry->request_close();if(capacity_creator_process())(void)registry->drain();
    }
}
capacity_registry& capacity_observers() {
    claim_capacity_process();
    if(capacity_shutdown_requested.load(std::memory_order_acquire))throw db_error("ordinary export capacity service closed");
    static auto* const registry=[] {
        capacity_initializing.store(true,std::memory_order_release);
        struct finish_initializing {~finish_initializing(){capacity_initializing.store(false,std::memory_order_release);}} finish;
        if(capacity_shutdown_requested.load(std::memory_order_acquire))throw db_error("ordinary export capacity service closed");
        auto value=std::make_unique<capacity_registry>();
        value->start(); // failure precedes observer/cut admission and publication
        if(std::atexit(capacity_exit)!=0) {
            value->request_close();(void)value->drain();
            throw db_error("ordinary export capacity shutdown registration failed");
        }
        auto* pointer=value.release();capacity_live.store(pointer,std::memory_order_release);
        if(capacity_shutdown_requested.load(std::memory_order_acquire))pointer->request_close();
        return pointer;
    }();
    return *registry;
}
capacity_snapshot capacity_snapshot_locked(capacity_registry& registry)noexcept{return registry.snapshot_locked();}
}
void prepare_ordinary_export_capacity_notifications() {
    auto& registry=capacity_observers();std::lock_guard lock(registry.mutex);
    if(registry.closing||capacity_shutdown_requested.load(std::memory_order_acquire))throw db_error("ordinary export capacity service closed");
}
void reserve_ordinary_export_capacity() {
    auto& registry=capacity_observers();
    // No source, payload, callback or foreign destructor occurs in this leaf.
    // Closed-service admission and the one shared count linearize together.
    std::lock_guard lock(registry.mutex);
    if(registry.closing||capacity_shutdown_requested.load(std::memory_order_acquire))
        throw db_error("ordinary export capacity service closed");
    auto used=ordinary_export_slots.load(std::memory_order_relaxed);
    do {if(used>=ordinary_sync_limits::simultaneous_exports)throw ordinary_export_capacity_exhausted();}
    while(!ordinary_export_slots.compare_exchange_weak(used,used+1,std::memory_order_acq_rel));
}
void release_ordinary_export_capacity()noexcept {
    // A fork child cannot decrement the parent's copied allowance or enter its
    // inherited dispatcher. This is a lifetime check, never a cleanup receipt.
    if(!capacity_creator_process())return;
    ordinary_export_slots.fetch_sub(1,std::memory_order_release);
    ordinary_export_capacity_released();
}
void ordinary_export_capacity_released()noexcept {
    if(!capacity_creator_process())return;
    if(auto* registry=capacity_live.load(std::memory_order_acquire))registry->released();
}
void request_ordinary_export_capacity_notification_close()noexcept {
    auto expected=uint32_t(0);const auto current=capacity_process();
    if(!capacity_creator.compare_exchange_strong(expected,current,std::memory_order_acq_rel)&&expected!=current)return;
    capacity_shutdown_requested.store(true,std::memory_order_release);
    if(auto* registry=capacity_live.load(std::memory_order_acquire))registry->request_close();
}
bool await_ordinary_export_capacity_notification_idle(std::chrono::steady_clock::time_point deadline)noexcept {
    if(capacity_creator.load(std::memory_order_acquire)==0)return true;
    if(!capacity_creator_process())return false;
    auto* registry=capacity_live.load(std::memory_order_acquire);if(!registry)return true;
    if(capacity_dispatching==registry)return false;
#if !defined(__EMSCRIPTEN__)
    std::unique_lock lock(registry->mutex);
    return registry->changed.wait_until(lock,deadline,[&]{return !registry->active;});
#else
    (void)deadline;std::lock_guard lock(registry->mutex);return !registry->active;
#endif
}
bool drain_ordinary_export_capacity_notifications()noexcept {
    request_ordinary_export_capacity_notification_close();
    if(!capacity_creator_process())return false;
    auto* registry=capacity_live.load(std::memory_order_acquire);
    // Creation in flight is explicit incomplete custody, never an absence proof.
    if(!registry)return !capacity_initializing.load(std::memory_order_acquire);
    return registry->drain();
}
namespace {

using json=nlohmann::json;
using caps=ordinary_sync_limits;
[[noreturn]] void reject(const char* message){throw db_error(std::string("ordinary v2: ")+message);}
using export_reservation=ordinary_resource_reservation<false>;
// Each admitted export reserves the full logical source/frame/workspace budget
// before capture, allocation or serialization. Per-buffer checks below prevent
// an individual frame/scratch buffer from consuming another object's budget.
void append(std::string& output,std::string_view value,size_t maximum=caps::frame_bytes) {
    if(output.size()>maximum||value.size()>maximum-output.size())reject("frame byte bound");
    output.append(value);
}
void quoted(std::string& output,const std::string& value) {
    if(value.size()>caps::scalar_bytes)reject("scalar byte bound");
    size_t bound=2;
    for(unsigned char c:value) {
        const size_t extra=c<32?6:(c=='"'||c=='\\'?2:1);
        if(extra>caps::frame_bytes-bound)reject("escaped scalar byte bound");bound+=extra;
    }
    if(output.size()>caps::frame_bytes||bound>caps::frame_bytes-output.size())reject("escaped frame byte bound");
    // The charged bound precedes both the JSON scalar copy and encoded copy;
    // dump also rejects invalid UTF-8 instead of publishing malformed frames.
    const auto encoded=json(value).dump();append(output,encoded);
}
void number(std::string& output,uint64_t value){quoted(output,std::to_string(value));}
std::string digest(std::string_view value){return picosha2::hash256_hex_string(value.begin(),value.end());}
bool workspace_fits(size_t bytes,size_t nodes){
    return bytes<=caps::parser_workspace_bytes/caps::parser_wire_copies&&
        nodes<=(caps::parser_workspace_bytes-bytes*caps::parser_wire_copies)/caps::parser_node_logical_bytes;
}
void scalar(std::string& output,const column_value_t& value) {
    append(output,"[");
    if(std::holds_alternative<std::nullptr_t>(value))append(output,"\"null\"");
    else if(const auto* p=std::get_if<int64_t>(&value)){append(output,"\"integer\",");quoted(output,std::to_string(*p));}
    else if(const auto* p=std::get_if<double>(&value)) {
        if(!std::isfinite(*p))reject("nonfinite scalar");append(output,"\"real\",");append(output,json(*p).dump());
    } else if(const auto* p=std::get_if<std::string>(&value)){append(output,"\"text\",");quoted(output,*p);}
    else {
        const auto& data=std::get<std::vector<uint8_t>>(value);
        if(data.size()>caps::scalar_bytes/2||data.size()>(caps::frame_bytes-output.size())/2)reject("blob scalar byte bound");
        append(output,"\"blob\",\"");static constexpr char hex[]="0123456789abcdef";
        for(uint8_t byte:data){const char pair[]{hex[byte>>4],hex[byte&15]};append(output,std::string_view(pair,2));}
        append(output,"\"");
    }
    append(output,"]");
}
void preflight_original(const audit_log_entry& entry) {
    (void)normalize_required_ordinary_timestamp(entry.timestamp);
    size_t bytes=1024;
    const auto text=[&](const std::string& value) {
        if(value.size()>caps::scalar_bytes||value.size()>(caps::frame_bytes-bytes)/6)reject("original scalar byte bound");
        bytes+=value.size()*6;
    };
    const auto name=[&](const std::string& value){if(value.empty()||value.size()>256||value.find('\0')!=std::string::npos)reject("original property name bound");text(value);};
    text(entry.global_id);text(entry.table_name);text(entry.operation);text(entry.global_row_id);text(entry.timestamp);
    if(entry.changed_fields.size()>caps::columns_per_table||entry.changed_fields_names.size()>caps::columns_per_table)reject("original property count bound");
    for(const auto& [key,value]:entry.changed_fields) {
        name(key);if(bytes>caps::frame_bytes-128)reject("original object byte bound");bytes+=128;
        if(const auto* p=std::get_if<std::string>(&value.value))text(*p);
        if(const auto* p=std::get_if<std::vector<uint8_t>>(&value.value)) {
            if(p->size()>caps::scalar_bytes/2||p->size()>(caps::frame_bytes-bytes)/2)reject("original blob byte bound");bytes+=p->size()*2;
        }
    }
    for(const auto& key:entry.changed_fields_names)name(key);
    if(entry.original_identity){if(entry.original_identity->changed_fields_names.size()>caps::columns_per_table)reject("original identity property count bound");
        for(const auto& key:entry.original_identity->changed_fields_names)name(key);text(entry.original_identity->digest);}
}
std::string prefix(const ordinary_export_frame& image,const char* kind) {
    std::string out;append(out,"{\"latticeOrdinarySync\":{\"version\":2,\"kind\":");quoted(out,kind);
    append(out,",\"source\":");quoted(out,image.source);append(out,",\"epoch\":");number(out,static_cast<uint64_t>(image.epoch));
    append(out,",\"cut\":");number(out,static_cast<uint64_t>(image.cut));return out;
}
}

struct ordinary_wire_export_state {
    // Declared first, released last, after all retained source/frame buffers.
    export_reservation reservation;
    owned_server_export issued;
    std::vector<std::string> frames;
    size_t bytes=0;
    ordinary_wire_export_state(export_reservation&& charge,owned_server_export value)
        :reservation(std::move(charge)),issued(std::move(value)){}
};

bool reserved_ordinary_wire_frame(std::string_view raw)noexcept {
    // Bound temporary key decoding, while scanning the existing borrowed frame
    // once. This does not copy ordinary frames or pretend to validate JSON.
    size_t depth=0;bool key=false;
    for(size_t n=0;n<raw.size();++n) {
        const char c=raw[n];
        if(c=='"') {
            const size_t first=n;bool closed=false;
            for(++n;n<raw.size();++n){if(raw[n]=='\\'){if(n+1<raw.size())++n;continue;}if(raw[n]=='"'){closed=true;break;}}
            if(!closed)return false;
            if(depth==1&&key) {
                if(n-first+1<=256)try{if(json::parse(raw.substr(first,n-first+1))=="latticeOrdinarySync")return true;}catch(...){return false;}
                key=false;
            }
        } else if(c=='{'||c=='['){++depth;if(depth==1){if(c!='{')return false;key=true;}}
        else if(c=='}'||c==']'){if(!depth)return false;if(--depth==0)return false;}
        else if(c==','&&depth==1)key=true;
    }
    return false;
}

ordinary_sync_export ordinary_wire_access::capture(lattice_db& owner,const std::vector<std::string>& models) {
    return capture_impl(owner,[](const void* context) {
        const auto& models=*static_cast<const std::vector<std::string>*>(context);
        if(models.empty()||models.size()>caps::models)reject("requested model count bound");
        for(const auto& name:models)if(name.empty()||name.size()>256)reject("requested model name bound");
        return models;
    },&models);
}
ordinary_sync_export ordinary_wire_access::capture(lattice_db& owner,const char* const* models,size_t count) {
    struct input {const char* const* models;size_t count;};const input borrowed{models,count};
    return capture_impl(owner,[](const void* context) {
        const auto& value=*static_cast<const input*>(context);const auto* models=value.models;const auto count=value.count;
        if(!models||!count||count>caps::models)reject("requested model count bound");
        std::vector<std::string> names;names.reserve(count);
        for(size_t n=0;n<count;++n) {
            if(!models[n])reject("null requested model");const auto length=strnlen(models[n],257);
            if(!length||length>256)reject("requested model name bound");names.emplace_back(models[n],length);
        }
        return names;
    },&borrowed);
}
ordinary_sync_export ordinary_wire_access::capture_impl(lattice_db& owner,std::vector<std::string> (*requested)(const void*),const void* context) {
    export_reservation reservation;
    const auto models=requested(context);
    auto issued=ordinary_export_access::capture_wire(owner,models);
    const auto image=issued.frame_;if(!image)reject("missing issued source capture");
    if(image->tables.size()>caps::projection_tables||image->rows>caps::rows||image->entries.size()>caps::originals||
       image->schema.size()>caps::schema_bytes||image->epoch<=0||image->cut<=0)reject("issued cut exceeds wire profile");
    auto state=std::make_shared<ordinary_wire_export_state>(std::move(reservation),std::move(issued));
    state->frames.reserve(caps::frames);state->frames.emplace_back();
    // Preserve room for the descriptor before retaining any content page.
    size_t header_reservation=caps::frame_bytes;
    const auto retain=[&](std::string wire) {
        if(state->frames.size()>=caps::frames||wire.size()>caps::retained_wire_bytes-header_reservation-state->bytes)
            reject("retained page count/byte bound");
        state->bytes+=wire.size();state->frames.push_back(std::move(wire));
    };
    picosha2::hash256_one_by_one content;
    std::string page;size_t records=0,page_nodes=128;
    const auto finish=[&] {
        if(!records)return;
        append(page,"]");const auto checksum=digest(page);
        append(page,",\"digest\":");quoted(page,checksum);append(page,"}}");
        content.process(page.begin(),page.end());retain(std::move(page));page.clear();records=0;page_nodes=128;
    };
    const auto push=[&](const std::string& record,size_t record_nodes) {
        // Envelope metadata and digest reserve are charged before concatenation.
        if(record.size()>caps::frame_bytes-1024)reject("record requires a later large-value profile");
        if(record_nodes>caps::parser_nodes-128)reject("record parser node bound");
        if(!workspace_fits(record.size()+1024,record_nodes+128))reject("record parser workspace bound");
        if(records&&(records==caps::records_per_page||record.size()+128>caps::frame_bytes-page.size()||record_nodes>caps::parser_nodes-page_nodes||
            !workspace_fits(page.size()+record.size()+128,page_nodes+record_nodes)))finish();
        if(!records){page=prefix(*image,"page");append(page,",\"index\":");number(page,state->frames.size()-1);append(page,",\"records\":[");}
        if(records)append(page,",");append(page,record);++records;page_nodes+=record_nodes;
    };
    for(const auto& table:image->tables)for(const auto& row:table.rows) {
        std::string record;append(record,"{\"row\":{\"table\":");quoted(record,table.name);append(record,",\"values\":[");
        if(row.size()!=table.columns.size()||row.size()>caps::columns_per_table)reject("issued row shape differs");
        for(size_t i=0;i<row.size();++i){if(i)append(record,",");scalar(record,row[i]);}
        append(record,"]}}");push(record,8+3*row.size());
    }
    if(image->originals.size()!=image->entries.size())reject("issued original descriptor count differs");
    for(size_t n=0;n<image->entries.size();++n) {
        const auto& entry=image->entries[n];const auto& original=image->originals[n];
        preflight_original(entry);
        if(original.fields.size()>caps::columns_per_table||original.names.size()>caps::columns_per_table||original.digest.size()!=64)reject("original descriptor bound");
        const auto body=entry.to_json();if(body.size()>caps::frame_bytes-32)reject("original record byte bound");
        std::string record;append(record,"{\"original\":");append(record,body);append(record,",\"originalDescriptor\":{\"version\":1,\"fields\":[");
        for(size_t i=0;i<original.fields.size();++i){if(i)append(record,",");quoted(record,original.fields[i]);}
        append(record,"],\"names\":[");for(size_t i=0;i<original.names.size();++i){if(i)append(record,",");quoted(record,original.names[i]);}
        append(record,"],\"digest\":");quoted(record,original.digest);append(record,"}}");
        push(record,160+8*entry.changed_fields.size()+2*entry.changed_fields_names.size()+2*original.fields.size()+2*original.names.size()+(entry.original_identity?2*entry.original_identity->changed_fields_names.size():0));
    }
    finish();content.finish();
    std::string header=prefix(*image,"cut");append(header,",\"mode\":\"full\",\"base\":null,\"scope\":");quoted(header,image->scope);
    append(header,",\"schema\":");quoted(header,digest(image->schema));
    append(header,",\"catalog\":");quoted(header,image->schema);
    // Source capture anchors are checksum inputs only. They never authenticate
    // a remote endpoint or replace the receiver's actual local physical check.
    append(header,",\"captureDevice\":");quoted(header,image->device);append(header,",\"captureInode\":");quoted(header,image->inode);
    append(header,",\"rows\":");number(header,image->rows);append(header,",\"originals\":");number(header,image->entries.size());
    append(header,",\"pages\":");number(header,state->frames.size()-1);append(header,",\"contentDigest\":");quoted(header,picosha2::get_hash_hex_string(content));
    append(header,",\"models\":[");for(size_t m=0;m<image->models.size();++m){if(m)append(header,",");quoted(header,image->models[m]);}append(header,"]");
    append(header,",\"tables\":[");size_t header_nodes=128+image->models.size();
    for(size_t t=0;t<image->tables.size();++t) {
        const auto& table=image->tables[t];if(t)append(header,",");append(header,"{\"name\":");quoted(header,table.name);
        if(table.columns.size()>caps::projection_columns||(16+2*table.columns.size())>caps::parser_nodes-header_nodes)reject("descriptor parser node bound");
        header_nodes+=16+2*table.columns.size();
        append(header,",\"columns\":[");for(size_t c=0;c<table.columns.size();++c){if(c)append(header,",");quoted(header,table.columns[c]);}
        append(header,"],\"types\":[");for(size_t c=0;c<table.types.size();++c){if(c)append(header,",");append(header,std::to_string(static_cast<int>(table.types[c])));}
        append(header,"]}");
    }
    append(header,"]}}");header_reservation=0;
    if(!workspace_fits(header.size(),header_nodes))reject("descriptor parser workspace bound");
    if(header.size()>caps::retained_wire_bytes-state->bytes)reject("descriptor retention byte bound");
    state->bytes+=header.size();state->frames[0]=std::move(header);
    return ordinary_sync_export(std::move(state));
}
std::vector<std::string> ordinary_wire_access::apply(lattice_db& owner,const ordinary_sync_export& value) {
    const auto state=value.state_;if(!state)reject("moved issued wire export");return apply_owned_server_export(owner,state->issued);
}
}

namespace lattice {
ordinary_export_capacity_observer::ordinary_export_capacity_observer(std::shared_ptr<detail::ordinary_export_capacity_observation> value):state_(std::move(value)){}
ordinary_export_capacity_observer::~ordinary_export_capacity_observer(){detail::drop_capacity_owner(std::move(state_));}
ordinary_export_capacity_observer::ordinary_export_capacity_observer(const ordinary_export_capacity_observer& other):state_(other.state_){}
ordinary_export_capacity_observer::ordinary_export_capacity_observer(ordinary_export_capacity_observer&& other)noexcept:state_(std::move(other.state_)){}
ordinary_export_capacity_observer& ordinary_export_capacity_observer::operator=(const ordinary_export_capacity_observer& other) {
    if(this!=&other){auto incoming=other.state_;detail::drop_capacity_owner(std::move(state_));state_=std::move(incoming);}return *this;
}
ordinary_export_capacity_observer& ordinary_export_capacity_observer::operator=(ordinary_export_capacity_observer&& other)noexcept {
    if(this!=&other){detail::drop_capacity_owner(std::move(state_));state_=std::move(other.state_);}return *this;
}

bool ordinary_export_capacity_observer::valid()const noexcept{return state_&&state_->creator==detail::capacity_process()&&state_->active.load(std::memory_order_acquire);}
void ordinary_export_capacity_observer::cancel()noexcept {
    detail::drop_capacity_owner(std::move(state_),true);
}
ordinary_export_capacity_observer observe_ordinary_export_capacity(std::function<void()> callback) {
    if(!callback)throw db_error("ordinary export capacity callback absent");
    // Refuse foreign/closed service before creating any callback owner.
    detail::prepare_ordinary_export_capacity_notifications();
    auto state=std::make_shared<detail::ordinary_export_capacity_observation>(std::move(callback));
    auto& registry=detail::capacity_observers();
    detail::capacity_snapshot retained;bool admitted=false;
    {std::lock_guard guard(registry.mutex);
        retained=detail::capacity_snapshot_locked(registry);
        if(!registry.closing&&registry.observers.size()<64){registry.observers.push_back(state);admitted=true;}
    }
    if(!admitted)throw db_error("ordinary export capacity observer limit");
    return ordinary_export_capacity_observer(std::move(state));
}
std::vector<std::string> receive_ordinary_source_upload(lattice_db& owner,const uint8_t* bytes,size_t count,const std::vector<std::string>& models) {
    if(!bytes||!count||count>ordinary_sync_limits::frame_bytes)throw db_error("ordinary source upload byte bound");
    return detail::ordinary_export_access::receive_upload(owner,std::string_view(reinterpret_cast<const char*>(bytes),count),models);
}

ordinary_upload_acknowledgment_result receive_ordinary_source_upload_with_acknowledgment(lattice_db& owner,const uint8_t* bytes,size_t count,const std::vector<std::string>& models) {
    if(!bytes||!count||count>ordinary_sync_limits::frame_bytes)throw db_error("ordinary source upload byte bound");
    return detail::ordinary_export_access::receive_upload_with_acknowledgment(owner,std::string_view(reinterpret_cast<const char*>(bytes),count),models);
}

ordinary_sync_export::ordinary_sync_export(std::shared_ptr<const detail::ordinary_wire_export_state> state):state_(std::move(state)){}
size_t ordinary_sync_export::frame_count()const {if(!state_)throw db_error("moved ordinary export");return state_->frames.size();}
size_t ordinary_sync_export::retained_wire_bytes()const {if(!state_)throw db_error("moved ordinary export");return state_->bytes;}
const std::string& ordinary_sync_export::frame(size_t index)const {if(!state_||index>=state_->frames.size())throw db_error("ordinary export frame index");return state_->frames[index];}
const std::string& ordinary_sync_export::source_id()const {if(!state_)throw db_error("moved ordinary export");return state_->issued.source_id();}
int64_t ordinary_sync_export::epoch()const {if(!state_)throw db_error("moved ordinary export");return state_->issued.epoch();}
int64_t ordinary_sync_export::cut()const {if(!state_)throw db_error("moved ordinary export");return state_->issued.cut();}
ordinary_sync_export capture_ordinary_sync_export(lattice_db& owner,const std::vector<std::string>& models){return detail::ordinary_wire_access::capture(owner,models);}
ordinary_sync_export capture_ordinary_sync_export(lattice_db& owner,const char* const* models,size_t count){return detail::ordinary_wire_access::capture(owner,models,count);}
std::vector<std::string> apply_ordinary_sync_export(lattice_db& owner,const ordinary_sync_export& value){return detail::ordinary_wire_access::apply(owner,value);}
std::vector<std::string> ordinary_sync_observation_tables(lattice_db& owner,const std::vector<std::string>& models){return detail::ordinary_export_access::observation_tables(owner,models);}
bool ordinary_sync_reserved_frame(const uint8_t* bytes,size_t count)noexcept {
    return bytes&&detail::reserved_ordinary_wire_frame(std::string_view(reinterpret_cast<const char*>(bytes),count));
}
}
