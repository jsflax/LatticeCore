#pragma once
#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace lattice::detail {
// Passive tracking outlives a retired synchronizer only while an admitted
// frame owns an exclusion. It retains no owner, database, transport or callback.
struct sync_upload_tracking {
    std::mutex mutex;
    std::unordered_map<std::string,int64_t> ids;
    std::unordered_map<std::string,uint64_t> pre_handoff;
    // Passive per-send identity, retained after handoff until the matching ACK,
    // timeout or lifecycle clear. An original ID may be sent again later.
    std::unordered_map<std::string,uint64_t> delivery_tokens;
    std::atomic<int64_t> pending{0};
    uint64_t generation=1,next_token=0;
    // Caller holds mutex. Token zero is the legacy, unregistered delivery path
    // and may never observe or erase a newer protected registration.
    uint64_t registration_locked(const std::string& id)const noexcept {
        const auto found=delivery_tokens.find(id);return found==delivery_tokens.end()?0:found->second;
    }
    bool matches_locked(const std::string& id,uint64_t physical,uint64_t token,bool handed_off)const noexcept {
        if(generation!=physical||!ids.count(id)||registration_locked(id)!=token)return false;
        return !handed_off||!token||!pre_handoff.count(id);
    }
    bool erase_locked(const std::string& id,uint64_t physical,uint64_t token,bool handed_off)noexcept {
        if(!matches_locked(id,physical,token,handed_off))return false;
        ids.erase(id);delivery_tokens.erase(id);pre_handoff.erase(id);return true;
    }
    void clear_ids_locked()noexcept {ids.clear();pre_handoff.clear();delivery_tokens.clear();}
};
class sync_upload_exclusion {
    std::shared_ptr<sync_upload_tracking> state_;
    std::vector<std::pair<std::string,int64_t>> ids_;
    uint64_t generation_=0,token_=0;
    bool active_=true; // guarded by state_->mutex
    void register_ids() {
        std::lock_guard<std::mutex> lock(state_->mutex);
        if(state_->generation!=generation_)throw std::runtime_error("upload exclusion lifecycle retired");
        if(state_->next_token==std::numeric_limits<uint64_t>::max())throw std::runtime_error("upload exclusion identity exhausted");
        for(const auto& [id,audit]:ids_)if(state_->ids.count(id))throw std::runtime_error("upload original already in flight");
        token_=++state_->next_token;
        for(const auto& [id,audit]:ids_){state_->pre_handoff[id]=token_;state_->delivery_tokens[id]=token_;state_->ids.emplace(id,audit);}
        state_->pending.store(static_cast<int64_t>(state_->ids.size()));
    }
public:
    sync_upload_exclusion(std::shared_ptr<sync_upload_tracking> state,uint64_t generation,std::vector<std::pair<std::string,int64_t>> ids)
        :state_(std::move(state)),ids_(std::move(ids)),generation_(generation){}
    static std::shared_ptr<sync_upload_exclusion> create(std::shared_ptr<sync_upload_tracking> state,uint64_t generation,std::vector<std::pair<std::string,int64_t>> ids) {
        auto result=std::make_shared<sync_upload_exclusion>(std::move(state),generation,std::move(ids));
        // If a map allocation fails partway, the already-created lease removes
        // only this token's partial registration after register_ids unlocks.
        result->register_ids();return result;
    }
    ~sync_upload_exclusion(){release();}
    void release() noexcept {
        std::lock_guard<std::mutex> lock(state_->mutex);
        if(!active_)return;active_=false;
        for(const auto& [id,audit]:ids_){
            const auto found=state_->pre_handoff.find(id);
            if(found==state_->pre_handoff.end()||found->second!=token_)continue;
            const auto current=state_->ids.find(id);
            if(current!=state_->ids.end()&&current->second==audit)state_->ids.erase(current);
            const auto delivery=state_->delivery_tokens.find(id);
            if(delivery!=state_->delivery_tokens.end()&&delivery->second==token_)state_->delivery_tokens.erase(delivery);
            state_->pre_handoff.erase(found);
        }
        state_->pending.store(static_cast<int64_t>(state_->ids.size()));
    }
    uint64_t delivery_token()const noexcept{return token_;}
    void handed_off() noexcept {
        std::lock_guard<std::mutex> lock(state_->mutex);
        if(!active_)return;active_=false;
        for(const auto& [id,audit]:ids_){const auto found=state_->pre_handoff.find(id);if(found!=state_->pre_handoff.end()&&found->second==token_)state_->pre_handoff.erase(found);}
        // The actual send transferred these IDs to ordinary ACK tracking.
        // Completion cancellation must not erase or prematurely settle them.
    }
    size_t retained_bytes(size_t cap)const {
        std::lock_guard<std::mutex> lock(state_->mutex);size_t bytes=sizeof(*this);
        const auto add=[&](size_t count,size_t width=1){if(bytes>cap||count>(cap-bytes)/width)bytes=cap+1;else bytes+=count*width;};
        add(ids_.capacity(),sizeof(decltype(ids_)::value_type));
        add(state_->ids.bucket_count()+state_->pre_handoff.bucket_count()+state_->delivery_tokens.bucket_count(),sizeof(void*));
        for(const auto& [id,audit]:ids_){add(id.capacity()+1);add(3*(id.size()+1));add(sizeof(decltype(state_->ids)::value_type)+sizeof(decltype(state_->pre_handoff)::value_type)+sizeof(decltype(state_->delivery_tokens)::value_type)+12*sizeof(void*));}
        return bytes;
    }
};
} // namespace lattice::detail
