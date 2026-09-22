#pragma once
#include "sync_upload_exclusion.hpp"
#include <array>
#include <algorithm>
#include <limits>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <atomic>
#include <cstdint>
#include <exception>

namespace lattice::detail {
// Private ownership for the ordinary sync discovery seam. Work returns false
// only at its explicit first no-effect discovery stage. Exceptions are never
// retried here; in particular no background_operation closure is replayed.
enum class sync_discovery_kind { intake,ack,upload,initial_upload,drain_upload };
// Passive completion ownership. No callbacks or owner pointers; admission and
// cancellation settle it outside the queue leaf. A running turn retains its
// first real failure even if cancellation/owner retirement races its tail.
struct sync_discovery_completion {
    enum class outcome { pending,running,completed,cancelled,rejected,expired };
    struct result {outcome state;std::exception_ptr error;};
private:
    mutable std::mutex mutex_;
    outcome state_=outcome::pending,cancellation_=outcome::pending;
    std::exception_ptr error_;
public:
    bool start() {
        std::lock_guard<std::mutex> lock(mutex_);
        if(state_!=outcome::pending)return false;state_=outcome::running;return true;
    }
    void cancel(outcome why=outcome::cancelled,std::exception_ptr error={}) {
        std::lock_guard<std::mutex> lock(mutex_);
        if(error&&!error_)error_=std::move(error);
        if(state_==outcome::running)cancellation_=why;
        else if(state_==outcome::pending)state_=why;
    }
    void finish(bool done,std::exception_ptr error={}) {
        std::lock_guard<std::mutex> lock(mutex_);
        if(error&&!error_)error_=std::move(error);
        if(state_!=outcome::running)return;
        state_=cancellation_!=outcome::pending?cancellation_:(done||error_?outcome::completed:outcome::pending);
    }
    result read()const {std::lock_guard<std::mutex> lock(mutex_);return {state_,error_};}
};
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
        std::shared_ptr<sync_discovery_completion> completion;
        std::shared_ptr<sync_upload_exclusion> upload_exclusion;
};
class sync_discovery_deferral {
public:
    using clock=std::chrono::steady_clock;
    using kind=sync_discovery_kind;
    using operation=sync_discovery_operation;
    enum class admission { accepted,coalesced,obsolete,exhausted };
    struct ticket {uint64_t generation=0,serial=0;std::shared_ptr<sync_discovery_completion> completion;explicit operator bool()const{return serial!=0;}};
    struct admission_result {admission state;ticket reserved;};
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
    admission push_locked(std::shared_ptr<operation>& work,clock::time_point now) {
        if(closed_||work->generation!=generation_)return admission::obsolete;
        if(failed_)return admission::exhausted;
        if(work->type==kind::upload)for(size_t i=active_?1:0;i<count_;++i)
            if(slots_[(head_+i)%capacity]->type==kind::upload&&
               slots_[(head_+i)%capacity]->coalescible.load(std::memory_order_acquire))return admission::coalesced;
        if(count_==capacity || work->charge>byte_limit-bytes_) {
            failed_=true;changed();return admission::exhausted;
        }
        bytes_+=work->charge;slots_[(head_+count_)%capacity]=std::move(work);++count_;
        if(count_==1)next_=now;changed();return admission::accepted;
    }
    ticket dispatch_locked(clock::time_point now) {
        if(closed_||failed_||active_||dispatched_||!count_||now<next_)return {};
        if(serial_==std::numeric_limits<uint64_t>::max()){failed_=true;changed();return {};}
        // Each admission has its own identity, including successive dispatches
        // in one generation. A throwing inline scheduler can have completed an
        // older callback while another reservation is already outstanding.
        ++serial_;dispatched_=true;return {generation_,serial_,slots_[head_]->completion};
    }
public:
    uint64_t revision()const noexcept{return revision_.load(std::memory_order_acquire);}
    admission push(std::shared_ptr<operation> work) {
        std::lock_guard<std::mutex> lock(mutex_);return push_locked(work,clock::now());
    }
    admission_result push_and_dispatch(std::shared_ptr<operation> work,clock::time_point now) {
        // Publish and reserve one eligible idle head in the same leaf turn.
        // The timer cannot steal this caller's initial scheduler admission.
        // Rejected/coalesced captures are destroyed after the leaf unlocks.
        std::lock_guard<std::mutex> lock(mutex_);
        const auto state=push_locked(work,now);
        if(state==admission::obsolete||state==admission::exhausted)return {state,{}};
        return {state,dispatch_locked(now)};
    }
    ticket dispatch(clock::time_point now) {
        std::lock_guard<std::mutex> lock(mutex_);return dispatch_locked(now);
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
        std::shared_ptr<operation> released;
        std::unique_lock<std::mutex> lock(mutex_);
        if(closed_||failed_||addressed.generation!=generation_||addressed.serial!=serial_||active_||!dispatched_||!count_)return {};
        dispatched_=false;
        if(now>=slots_[head_]->deadline) {
            failed_=true;changed();const auto completion=slots_[head_]->completion;const auto exclusion=slots_[head_]->upload_exclusion;lock.unlock();
            if(completion)completion->cancel(sync_discovery_completion::outcome::expired);
            if(exclusion)exclusion->release();return {};
        }
        if(slots_[head_]->completion&&!slots_[head_]->completion->start()) {
            bytes_-=slots_[head_]->charge;released=std::move(slots_[head_]);head_=(head_+1)%capacity;--count_;next_=now;changed();
            lock.unlock();if(released->upload_exclusion)released->upload_exclusion->release();
            return {}; // released capture destructs after leaf unlock
        }
        active_=true;return slots_[head_];
    }
    bool attach_exclusion(operation* work,std::shared_ptr<sync_upload_exclusion> exclusion) {
        std::lock_guard<std::mutex> lock(mutex_);
        if(closed_||failed_||!active_||!count_||slots_[head_].get()!=work||slots_[head_]->upload_exclusion)return false;
        slots_[head_]->upload_exclusion=std::move(exclusion);return true;
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
private:
    struct settlement {bool failed=false;ticket continuation;};
    settlement settle(ticket addressed,const std::shared_ptr<operation>& work,bool done,
                      clock::time_point now,bool retain_dispatch) {
        std::shared_ptr<operation> released;
        std::unique_lock<std::mutex> lock(mutex_);
        if(addressed.generation!=generation_||addressed.serial!=serial_||!active_||!count_||slots_[head_]!=work)return {};
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
        // This is the same already-admitted scheduler callback, not a new
        // dispatch. Its immutable RAII ticket owns at most four FIFO turns.
        // A BUSY result, terminal failure or quantum boundary releases it.
        const bool continued=done&&retain_dispatch&&count_&&!failed_&&!closed_;
        if(continued)dispatched_=true;
        changed();const auto result=settlement{failed_,continued?ticket{addressed.generation,addressed.serial,slots_[head_]->completion}:ticket{}};
        const auto exclusion=work->upload_exclusion;lock.unlock();
        if((done||result.failed)&&exclusion)exclusion->release();
        return result;
    }
public:
    bool finish(ticket addressed,const std::shared_ptr<operation>& work,bool done,clock::time_point now) {
        return settle(addressed,work,done,now,false).failed;
    }
    ticket finish_and_continue(ticket addressed,const std::shared_ptr<operation>& work,bool done,
                               clock::time_point now,bool within_quantum) {
        return settle(addressed,work,done,now,within_quantum).continuation;
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
        std::array<std::shared_ptr<sync_upload_exclusion>,capacity> released;
        {std::lock_guard<std::mutex> lock(mutex_);
         if(generation_!=generation||!failed_||reported_)return false;
         reported_=true;
         // Active foreign send retains its lease until its own settlement.
         for(size_t i=active_?1:0;i<count_;++i)released[i]=slots_[(head_+i)%capacity]->upload_exclusion;}
        for(const auto& exclusion:released)if(exclusion)exclusion->release();return true;
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
        for(const auto& work:released)if(work){if(work->completion)work->completion->cancel();if(work->upload_exclusion)work->upload_exclusion->release();}
        // Captures may retire owners. Release them outside the leaf lock.
    }
};
} // namespace lattice::detail
