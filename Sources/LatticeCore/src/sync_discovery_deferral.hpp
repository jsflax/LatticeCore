#pragma once
#include <array>
#include <algorithm>
#include <limits>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <atomic>
#include <cstdint>

namespace lattice::detail {
// Private ownership for the ordinary sync discovery seam. Work returns false
// only at its explicit first no-effect discovery stage. Exceptions are never
// retried here; in particular no background_operation closure is replayed.
enum class sync_discovery_kind { intake,ack,upload,initial_upload };
struct sync_discovery_operation {
        using clock=std::chrono::steady_clock;
        using kind=sync_discovery_kind;
        kind type;
        const char* label;
        uint64_t generation;
        size_t charge;
        std::function<bool(sync_discovery_operation&)> step;
        clock::time_point deadline=clock::time_point::max();
        unsigned attempts=0;
        std::atomic<bool> coalescible{true};
};
class sync_discovery_deferral {
public:
    using clock=std::chrono::steady_clock;
    using kind=sync_discovery_kind;
    using operation=sync_discovery_operation;
    enum class admission { accepted,coalesced,obsolete,exhausted };
    struct ticket {uint64_t generation=0,serial=0;explicit operator bool()const{return serial!=0;}};
    static constexpr size_t capacity=64,byte_limit=16*1024*1024;
    static constexpr unsigned attempt_limit=32,turn_limit=4;
private:
    mutable std::mutex mutex_;
    std::array<std::shared_ptr<operation>,capacity> slots_{};
    size_t head_=0,count_=0,bytes_=0;
    uint64_t generation_=1,serial_=0;
    bool active_=false,dispatched_=false,closed_=false,failed_=false,reported_=false;
    clock::time_point next_{};
    std::atomic<uint64_t> revision_{0};
    void changed()noexcept{revision_.fetch_add(1,std::memory_order_release);}
public:
    uint64_t revision()const noexcept{return revision_.load(std::memory_order_acquire);}
    admission push(std::shared_ptr<operation> work) {
        // All captures are constructed before this leaf. Rejected work is
        // destroyed after the lock guard, never under the queue mutex.
        std::lock_guard<std::mutex> lock(mutex_);
        if(closed_||work->generation!=generation_)return admission::obsolete;
        if(failed_)return admission::exhausted;
        if(work->type==kind::upload)for(size_t i=active_?1:0;i<count_;++i)
            if(slots_[(head_+i)%capacity]->type==kind::upload&&
               slots_[(head_+i)%capacity]->coalescible.load(std::memory_order_acquire))return admission::coalesced;
        if(count_==capacity || work->charge>byte_limit-bytes_) {
            failed_=true;changed();return admission::exhausted;
        }
        bytes_+=work->charge;slots_[(head_+count_)%capacity]=std::move(work);++count_;
        if(count_==1)next_=clock::now();changed();return admission::accepted;
    }
    ticket dispatch(clock::time_point now) {
        std::lock_guard<std::mutex> lock(mutex_);
        if(closed_||failed_||active_||dispatched_||!count_||now<next_)return {};
        if(serial_==std::numeric_limits<uint64_t>::max()){failed_=true;changed();return {};}
        // Each admission has its own identity, including successive dispatches
        // in one generation. A throwing inline scheduler can have completed an
        // older callback while another reservation is already outstanding.
        ++serial_;dispatched_=true;return {generation_,serial_};
    }
    bool reject_unbegun(ticket addressed) {
        std::lock_guard<std::mutex> lock(mutex_);
        if(closed_||failed_||addressed.generation!=generation_||addressed.serial!=serial_||
           !dispatched_||active_||!count_)return false;
        // Scheduler admission failure is terminal, never a generic retry.
        // Keep every accepted payload until explicit lifecycle disposition.
        dispatched_=false;failed_=true;changed();return true;
    }
    std::shared_ptr<operation> begin(ticket addressed,clock::time_point now) {
        std::lock_guard<std::mutex> lock(mutex_);
        if(closed_||failed_||addressed.generation!=generation_||addressed.serial!=serial_||active_||!dispatched_||!count_)return {};
        dispatched_=false;
        if(now>=slots_[head_]->deadline){failed_=true;changed();return {};}
        active_=true;return slots_[head_];
    }
    // Increase retention charge only before returning a newly staged busy
    // continuation. Failure leaves the old charge and requires a stage-local
    // durable replay refusal; oversized payloads must not be parked here.
    bool resize(operation* work,size_t charge) {
        std::lock_guard<std::mutex> lock(mutex_);
        if(closed_||failed_||!active_||!count_||slots_[head_].get()!=work)return false;
        const auto other=bytes_-work->charge;
        if(charge>byte_limit-other)return false;
        bytes_=other+charge;work->charge=charge;return true;
    }
    bool finish(ticket addressed,const std::shared_ptr<operation>& work,bool done,clock::time_point now) {
        std::shared_ptr<operation> released;
        std::lock_guard<std::mutex> lock(mutex_);
        if(addressed.generation!=generation_||addressed.serial!=serial_||!active_||!count_||slots_[head_]!=work)return false;
        active_=false;
        if(done) {
            bytes_-=work->charge;released=std::move(slots_[head_]);head_=(head_+1)%capacity;--count_;next_=now;
        } else {
            if(work->attempts==0)work->deadline=now+std::chrono::seconds(5);
            ++work->attempts;
            if(work->attempts>=attempt_limit||now>=work->deadline)failed_=true;
            else {
                const unsigned delay=work->attempts>5?100:(5u<<(work->attempts-1));
                next_=std::min(work->deadline,now+std::chrono::milliseconds(delay));
            }
        }
        changed();return failed_;
    }
    clock::time_point wake_at()const {
        std::lock_guard<std::mutex> lock(mutex_);
        if(closed_)return clock::time_point::max();
        // A drop may happen before the pacer snapshots revision_. Pending
        // failure reporting must be level-triggered, not only a notification.
        if(failed_)return reported_?clock::time_point::max():clock::time_point::min();
        return active_||dispatched_||!count_?clock::time_point::max():next_;
    }
    bool failed(uint64_t generation)const {
        std::lock_guard<std::mutex> lock(mutex_);return generation_==generation&&failed_;
    }
    bool take_failure(uint64_t generation) {
        std::lock_guard<std::mutex> lock(mutex_);
        if(generation_!=generation||!failed_||reported_)return false;
        reported_=true;return true;
    }
    bool pending(uint64_t generation)const {
        std::lock_guard<std::mutex> lock(mutex_);return generation_==generation&&count_!=0;
    }
    // No abandoned memory is called durable. Cancel is an explicit lifecycle
    // disposition: unacknowledged real audit rows remain owned by their sender.
    // Unsupported one-shot/synthetic replay authority is never invented here.
    void cancel(uint64_t generation,bool close=false) {
        std::array<std::shared_ptr<operation>,capacity> released;
        {std::lock_guard<std::mutex> lock(mutex_);
         released.swap(slots_);head_=count_=bytes_=0;active_=dispatched_=failed_=reported_=false;
         generation_=generation;closed_=closed_||close||serial_==std::numeric_limits<uint64_t>::max();
         if(!closed_)++serial_;changed();}
        // Captures may retire owners. Release them outside the leaf lock.
    }
};
} // namespace lattice::detail
