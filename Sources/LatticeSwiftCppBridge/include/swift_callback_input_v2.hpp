#pragma once
#ifdef __cplusplus
#include <lattice/db.hpp>
#include <atomic>
#include <cstdint>
#include <functional>
#include <limits>
#include <exception>
#include <utility>

namespace lattice::detail {
// Versioned immutable *input* ownership only. The real public Swift setters
// create their original target once; retained configurations share that same
// target without executing its copy constructor. This grants no Native,
// store, SQL, startup, account or callback-execution admission.
template<class Signature> class swift_callback_input_v2;
template<class Result,class... Arguments>
class swift_callback_input_v2<Result(Arguments...)> final {
    using function_type=std::function<Result(Arguments...)>;
    struct payload {
        const std::uint64_t creator;
        std::atomic<std::uint64_t> references{1};
        function_type function;
        payload(std::uint64_t process,function_type&& target)noexcept
            :creator(process),function(std::move(target)){}
    };
    const std::uint64_t creator_=database_native_process();
    payload* value_=nullptr;
    static payload* retain_(payload* value,std::uint64_t creator)noexcept {
        if(creator!=database_native_process()||!value)return nullptr;
        if(value->creator!=creator)std::terminate();
        auto count=value->references.load(std::memory_order_relaxed);
        for(;;) {
            if(!count||count==std::numeric_limits<std::uint64_t>::max())std::terminate();
            if(value->references.compare_exchange_weak(count,count+1,std::memory_order_relaxed))return value;
        }
    }
    static void release_(payload* value,std::uint64_t creator)noexcept {
        if(creator!=database_native_process()||!value)return;
        if(value->creator!=creator)std::terminate();
        if(value->references.fetch_sub(1,std::memory_order_acq_rel)!=1)return;
        // This is our LAST outer reference. Preserve the original target's
        // destruction semantics, then recheck before touching/deleting our
        // shell. Arbitrary user destroy callbacks and their OWN std runtime
        // cleanup after a fork inside that callback are not proved safe here.
        value->function=nullptr;
        if(creator==database_native_process())delete value;
    }
    struct release_pin {
        const std::uint64_t creator;payload* const value;
        ~release_pin()noexcept{release_(value,creator);}
    };
public:
    swift_callback_input_v2()noexcept=default;
    swift_callback_input_v2(const swift_callback_input_v2& other)noexcept {
        // Inherited DATA never reads its payload/control. A child receives
        // an empty new-process input, without retaining/changing the source.
        if(other.creator_==creator_)value_=retain_(other.value_,creator_);
    }
    swift_callback_input_v2(swift_callback_input_v2&& other)noexcept {
        if(other.creator_==creator_)value_=std::exchange(other.value_,nullptr);
    }
    swift_callback_input_v2& operator=(const swift_callback_input_v2& other)noexcept {
        if(creator_!=database_native_process()||this==&other)return *this;
        auto* retained=other.creator_==creator_?retain_(other.value_,creator_):nullptr;
        auto* prior=std::exchange(value_,retained);release_(prior,creator_);return *this;
    }
    swift_callback_input_v2& operator=(swift_callback_input_v2&& other)noexcept {
        if(creator_!=database_native_process()||this==&other)return *this;
        auto* retained=other.creator_==creator_?std::exchange(other.value_,nullptr):nullptr;
        auto* prior=std::exchange(value_,retained);release_(prior,creator_);return *this;
    }
    ~swift_callback_input_v2()noexcept {
        if(creator_!=database_native_process())return;
        auto* prior=std::exchange(value_,nullptr);release_(prior,creator_);
    }
    swift_callback_input_v2& operator=(function_type target) {
        if(creator_!=database_native_process())throw db_error("inherited Swift callback input assignment");
        if(!target){auto* prior=std::exchange(value_,nullptr);release_(prior,creator_);return *this;}
        auto* pending=new payload(creator_,std::move(target));
        if(creator_!=database_native_process())throw db_error("inherited Swift callback input allocation");
        auto* prior=std::exchange(value_,pending);release_(prior,creator_);return *this;
    }
    explicit operator bool()const noexcept {
        return creator_==database_native_process()&&value_&&static_cast<bool>(value_->function);
    }
    Result operator()(Arguments... arguments)const {
        if(creator_!=database_native_process())throw db_error("inherited Swift callback input invocation");
        const release_pin pinned{creator_,retain_(value_,creator_)};
        if(!pinned.value||!pinned.value->function)throw std::bad_function_call();
        return pinned.value->function(std::forward<Arguments>(arguments)...);
    }
};
} // namespace lattice::detail
#endif
