#pragma once
#ifdef __cplusplus
#include <lattice/ordinary_sync.hpp>
#include <error.hpp>
#include <lattice/ordinary_history.hpp>
#include <bridging.hpp>
#include <cstring>
#include <optional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace lattice {
class swift_lattice_ref;
// The real native observer alone retains this callback context. A canceled
// in-flight snapshot may finish; fork-child native quarantine keeps its
// inherited context. Every release is only a retry hint, never capacity.
class ordinary_sync_export_capacity_ref {
    std::optional<ordinary_export_capacity_observer> observer_;
public:
    ordinary_sync_export_capacity_ref()=default;
    static ordinary_sync_export_capacity_ref observe(void* context,void(*notify)(void*),void(*release)(void*))noexcept SWIFT_NAME(observe(_:notify:release:)) {
        ordinary_sync_export_capacity_ref result;
        try {
            std::shared_ptr<void> owned(context,[release](void* value){if(value&&release)release(value);});
            if(!context||!notify||!release)return result;
            result.observer_.emplace(observe_ordinary_export_capacity([owned,notify]{notify(owned.get());}));
        }catch(...){}
        return result;
    }
    bool valid()const noexcept SWIFT_NAME(isValid()){return observer_.has_value()&&observer_->valid();}
    void cancel()noexcept{if(observer_)observer_->cancel();observer_.reset();}
};
// Value result; no exception crosses the Swift boundary. Failure is distinct
// from an empty frame. Byte access borrows this result until the call returns.
class ordinary_sync_frame_result {
    friend class ordinary_sync_export_ref;
    std::optional<ordinary_sync_export> owner_;
    size_t index_=0;
    bool valid_=false;
public:
    ordinary_sync_frame_result()=default;
    bool valid()const noexcept SWIFT_NAME(isValid()) {return valid_;}
    size_t byte_count()const noexcept SWIFT_NAME(byteCount()) {
        if(!valid_)return 0;try{return owner_->frame(index_).size();}catch(...){return 0;}
    }
    uint8_t byte_at(size_t index)const noexcept SWIFT_NAME(byteAt(_:)) {
        if(!valid_)return 0;try{const auto& bytes=owner_->frame(index_);return index<bytes.size()?static_cast<uint8_t>(bytes[index]):0;}catch(...){return 0;}
    }
    // Synchronous copy into caller-owned, already charged storage. The buffer
    // cannot escape; the actual issued frame remains retained for this call.
    bool copy_to(void* destination,size_t capacity)const noexcept SWIFT_NAME(copyTo(_:capacity:)) {
        if(!valid_||!destination)return false;
        try{const auto& bytes=owner_->frame(index_);if(capacity<bytes.size())return false;std::memcpy(destination,bytes.data(),bytes.size());return true;}
        catch(...){return false;}
    }
};
// Copies retain one native issued cut and one capacity reservation. There is
// no JSON/field constructor. The SDK must retain this through queued sends and
// release its last copy on the actual I/O completion/cancellation path.
class ordinary_sync_export_ref {
    friend class swift_lattice_ref;
    std::optional<ordinary_sync_export> value_;
    std::string failure_;
    bool capacity_exhausted_=false;
    static ordinary_sync_export_ref capture(lattice_db& owner,const std::vector<std::string>& models)noexcept {
        ordinary_sync_export_ref result;
        try{result.value_.emplace(capture_ordinary_sync_export(owner,models));}
        catch(const ordinary_export_capacity_exhausted& error){result.capacity_exhausted_=true;assign_ordinary_bridge_exception(result.failure_,error);}
        catch(const std::exception& error){assign_ordinary_bridge_exception(result.failure_,error);}
        catch(...){try{result.failure_="ordinary export capture failed";}catch(...){}}
        return result;
    }
public:
    ordinary_sync_export_ref()=default;
    bool valid()const noexcept SWIFT_NAME(isValid()) {return value_.has_value();}
    bool capacity_exhausted()const noexcept SWIFT_NAME(capacityExhausted()){return capacity_exhausted_;}
    std::string failure()const noexcept {try{return failure_;}catch(...){return {};}}
    size_t frame_count()const noexcept SWIFT_NAME(frameCount()) {
        if(!value_)return 0;try{return value_->frame_count();}catch(...){return 0;}
    }
    ordinary_sync_frame_result frame(size_t index)const noexcept {
        ordinary_sync_frame_result result;if(!value_)return result;
        try{if(index>=value_->frame_count())return result;result.owner_=value_;result.index_=index;result.valid_=true;}catch(...){}
        return result;
    }
};
// IDs-only actual receive outcome. Copies share one immutable payload, not
// copied ID vectors. Only the held-owner bridge can call the private producer.
// Its payload allocation and publication pointer precede every Native effect.
class ordinary_upload_ids_result final {
    friend class swift_lattice_ref;
    struct payload final {
        bool valid=false;
        std::vector<std::string> ids;
        std::string failure;
    };
    std::shared_ptr<const payload> value_;
    static ordinary_upload_ids_result receive(lattice_db& owner,const std::vector<uint8_t>& data,
        const std::vector<std::string>& models)noexcept {
        static_assert(std::is_nothrow_move_assignable_v<std::vector<std::string>>);
        ordinary_upload_ids_result result;
        std::shared_ptr<payload> issued;
        try{
            issued=std::make_shared<payload>();
            result.value_=issued; // no allocation after the Native return to own its IDs
            issued->ids=::lattice::receive_ordinary_source_upload(owner,data.data(),data.size(),models);
            issued->valid=true; // only the actual normal Native return issues success
        }catch(const std::exception& error){
            if(issued){try{issued->failure.assign(error.what(),strnlen(error.what(),2048));}catch(...){}}
        }catch(...){
            if(issued){try{issued->failure="ordinary source upload failed";}catch(...){}}
        }
        return result;
    }
public:
    ordinary_upload_ids_result()noexcept=default;
    bool valid()const noexcept SWIFT_NAME(isValid()){return value_&&value_->valid;}
    size_t id_count()const noexcept SWIFT_NAME(idCount()){return valid()?value_->ids.size():0;}
    std::string id_at(size_t index)const noexcept SWIFT_NAME(idAt(_:)){
        try{return valid()&&index<value_->ids.size()?value_->ids[index]:std::string();}catch(...){return {};}
    }
    std::string failure()const noexcept{
        try{return value_?value_->failure:std::string();}catch(...){return {};}
    }
};
// Names come from actual validated schema projection, never a raw public list
// of table names. This value has no capture or receiver-issuance capability.
class ordinary_sync_observation_ref {
    friend class swift_lattice_ref;
    std::vector<std::string> tables_;
    std::string failure_;
    bool valid_=false;
    static ordinary_sync_observation_ref select(lattice_db& owner,const std::vector<std::string>& models)noexcept {
        ordinary_sync_observation_ref result;
        try{result.tables_=ordinary_sync_observation_tables(owner,models);result.valid_=true;}
        catch(const std::exception& error){assign_ordinary_bridge_exception(result.failure_,error);}
        catch(...){try{result.failure_="ordinary observation selection failed";}catch(...){}}
        return result;
    }
public:
    ordinary_sync_observation_ref()=default;
    bool valid()const noexcept SWIFT_NAME(isValid()) {return valid_;}
    std::string failure()const noexcept {try{return failure_;}catch(...){return {};}}
    size_t table_count()const noexcept SWIFT_NAME(tableCount()){return tables_.size();}
    std::string table(size_t index)const noexcept {
        try{return valid_&&index<tables_.size()?tables_[index]:std::string();}catch(...){return {};}
    }
};
// A genuine native history page lease, not copied authority or an array issuer.
// Its optional retains the exact immutable native page/child/export reservation
// through the SDK's actual send-return AND completion ownership interval.
class ordinary_history_page_result {
    friend class ordinary_history_export_ref;
    std::optional<ordinary_history_page> page_;
public:
    ordinary_history_page_result()=default;
    bool valid()const noexcept SWIFT_NAME(isValid()){return page_.has_value();}
    size_t byte_count()const noexcept SWIFT_NAME(byteCount()){
        if(!page_)return 0;try{return page_->bytes().size();}catch(...){return 0;}
    }
    bool copy_to(void* destination,size_t capacity)const noexcept SWIFT_NAME(copyTo(_:capacity:)){
        if(!page_||!destination)return false;
        try{const auto bytes=page_->bytes();if(bytes.empty()||bytes.size()>ordinary_history_limits{}.page_bytes||capacity<bytes.size())return false;std::memcpy(destination,bytes.data(),bytes.size());return true;}catch(...){return false;}
    }
};
// Only swift_lattice_ref can ask the existing native issuer to capture. There
// is no stage-ID resume, pointer alias, JSON constructor or authority fallback.
class ordinary_history_export_ref {
    friend class swift_lattice_ref;
    friend class ordinary_mounted_history_source_ref;
    std::optional<ordinary_history_export> value_;
    std::string failure_;
    bool capacity_exhausted_=false;
    static ordinary_history_export_ref capture(lattice_db& owner,const std::vector<std::string>& models)noexcept{
        ordinary_history_export_ref result;
        try{result.value_.emplace(capture_ordinary_history_export(owner,models));}
        catch(const ordinary_export_capacity_exhausted& error){result.capacity_exhausted_=true;assign_ordinary_bridge_exception(result.failure_,error);}
        catch(const std::exception& error){assign_ordinary_bridge_exception(result.failure_,error);}
        catch(...){try{result.failure_="ordinary history capture failed";}catch(...){}}
        return result;
    }
public:
    ordinary_history_export_ref()=default;
    bool valid()const noexcept SWIFT_NAME(isValid()){return value_.has_value();}
    bool capacity_exhausted()const noexcept SWIFT_NAME(capacityExhausted()){return capacity_exhausted_;}
    std::string failure()const noexcept {try{return failure_;}catch(...){return {};}}
    std::string stage_id()const noexcept SWIFT_NAME(stageID()){try{return value_?value_->stage_id():std::string();}catch(...){return {};}}
    std::string source_id()const noexcept SWIFT_NAME(sourceID()){try{return value_?value_->source_id():std::string();}catch(...){return {};}}
    int64_t epoch()const noexcept{try{return value_?value_->epoch():0;}catch(...){return 0;}}
    int64_t cut()const noexcept{try{return value_?value_->cut():0;}catch(...){return 0;}}
    uint64_t frame_count()const noexcept SWIFT_NAME(frameCount()){try{return value_?value_->frame_count():0;}catch(...){return 0;}}
    uint64_t durable_bytes()const noexcept SWIFT_NAME(durableBytes()){try{return value_?value_->durable_bytes():0;}catch(...){return 0;}}
    std::string mounted_source_relation_observation()const noexcept SWIFT_NAME(mountedSourceRelationObservation()){try{return value_?value_->mounted_source_relation_observation():std::string();}catch(...){return {};}}
    bool has_prior_mounted_source()const noexcept SWIFT_NAME(hasPriorMountedSource()){try{return value_&&value_->prior_mounted_source().has_value();}catch(...){return false;}}
    std::string prior_scope()const noexcept SWIFT_NAME(priorScope()){try{if(value_){const auto p=value_->prior_mounted_source();if(p)return p->scope;}}catch(...){}return {};}
    std::string prior_source_id()const noexcept SWIFT_NAME(priorSourceID()){try{if(value_){const auto p=value_->prior_mounted_source();if(p)return p->source;}}catch(...){}return {};}
    std::string prior_schema_digest()const noexcept SWIFT_NAME(priorSchemaDigest()){try{if(value_){const auto p=value_->prior_mounted_source();if(p)return p->schema_digest;}}catch(...){}return {};}
    int64_t prior_epoch()const noexcept SWIFT_NAME(priorEpoch()){try{if(value_){const auto p=value_->prior_mounted_source();if(p)return p->epoch;}}catch(...){}return 0;}
    int64_t prior_issued_frontier()const noexcept SWIFT_NAME(priorIssuedFrontier()){try{if(value_){const auto p=value_->prior_mounted_source();if(p)return p->issued_frontier;}}catch(...){}return 0;}
    ordinary_history_page_result page(uint64_t index)const noexcept{
        ordinary_history_page_result result;if(!value_)return result;
        try{if(index>=value_->frame_count())return result;result.page_.emplace(value_->page(index));}catch(...){}
        return result;
    }
};
class ordinary_mounted_history_source_ref {
    friend class swift_lattice_ref;
    std::optional<ordinary_mounted_history_source> value_;
    std::string failure_;
    bool capacity_exhausted_=false;
    static ordinary_mounted_history_source_ref issue(std::shared_ptr<lattice_db> owner,const std::vector<std::string>& mounted)noexcept{
        ordinary_mounted_history_source_ref result;
        try{result.value_.emplace(ordinary_mounted_history_source::issue(std::move(owner),mounted));}
        catch(const ordinary_export_capacity_exhausted& error){result.capacity_exhausted_=true;assign_ordinary_bridge_exception(result.failure_,error);}
        catch(const std::exception& error){assign_ordinary_bridge_exception(result.failure_,error);}
        catch(...){try{result.failure_="mounted history source issuance failed";}catch(...){}}
        return result;
    }
public:
    ordinary_mounted_history_source_ref()=default;
    bool valid()const noexcept SWIFT_NAME(isValid()){return value_.has_value();}
    bool capacity_exhausted()const noexcept SWIFT_NAME(capacityExhausted()){return capacity_exhausted_;}
    std::string failure()const noexcept {try{return failure_;}catch(...){return {};}}
    ordinary_history_export_ref capture_with_lineage_observation(const std::vector<std::string>& selected)const noexcept SWIFT_NAME(captureWithLineageObservation(models:)){
        ordinary_history_export_ref result;if(!value_)return result;
        try{result.value_.emplace(value_->capture_with_lineage_observation(selected));}
        catch(const std::exception& error){assign_ordinary_bridge_exception(result.failure_,error);}
        catch(...){try{result.failure_="mounted history capture failed";}catch(...){}}
        return result;
    }
    ordinary_history_export_ref capture(const std::vector<std::string>& selected)const noexcept SWIFT_NAME(capture(models:)){
        ordinary_history_export_ref result;if(!value_)return result;
        try{result.value_.emplace(value_->capture(selected));}
        catch(const std::exception& error){assign_ordinary_bridge_exception(result.failure_,error);}
        catch(...){try{result.failure_="mounted history capture failed";}catch(...){}}
        return result;
    }
};
}
#endif
