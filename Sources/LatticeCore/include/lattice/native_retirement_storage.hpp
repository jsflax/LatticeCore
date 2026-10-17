#pragma once
#include <memory>
#include <cstddef>
#include <tuple>
#include <mutex>
#include <utility>
#include <unistd.h>

namespace lattice::detail {
// This actual lock retains the original leaf in its native process and skips
// inherited unlock after an arbitrary callback-copy continuation.
class native_retirement_leaf_lock final {
    std::mutex& leaf_;const pid_t creator_=::getpid();
public:
    explicit native_retirement_leaf_lock(std::mutex& leaf):leaf_(leaf){leaf_.lock();}
    ~native_retirement_leaf_lock(){if(creator_==::getpid())leaf_.unlock();}
    native_retirement_leaf_lock(const native_retirement_leaf_lock&)=delete;
    native_retirement_leaf_lock& operator=(const native_retirement_leaf_lock&)=delete;
};
// Source-internal destruction custody only. It issues no store, route, source,
// admission or completion fact. Native PID is captured before allocation.
// Prepare empty ownership before any user copy or extraction; foreign copies
// deliberately abandon the remaining graph rather than run its destructors.
template<typename... Values> class native_retirement_storage final {
    struct fields { std::tuple<Values...> values; };
    template<std::size_t I> static bool clear(fields& value,pid_t creator) noexcept {
        if(creator!=::getpid())return false;
        if constexpr(I<sizeof...(Values)) {
            auto& item=std::get<I>(value.values);
            if constexpr(requires {item.reset();})item.reset();else item={};
            // An actual callable/object destructor may have forked.
            if(creator!=::getpid())return false;
            return clear<I+1>(value,creator);
        } else return true;
    }
    struct native_deleter {
        pid_t creator;
        void operator()(fields* value)const noexcept {
            if(value&&clear<0>(*value,creator))delete value;
        }
    };
    std::unique_ptr<fields,native_deleter> fields_;
public:
    // Exact actual separate `new fields{}` request, without wrapper/control guesses.
    static constexpr std::size_t allocation_bytes()noexcept{return sizeof(fields);}
    native_retirement_storage():fields_(nullptr,native_deleter{::getpid()}) {
        fields_.reset(new fields{});
    }
    native_retirement_storage(const native_retirement_storage&)=delete;
    native_retirement_storage& operator=(const native_retirement_storage&)=delete;
    bool in_creator_process()const noexcept{return fields_.get_deleter().creator==::getpid();}
    template<std::size_t I> auto& get()noexcept{return std::get<I>(fields_->values);}
    template<std::size_t I> bool release()noexcept {
        if(!in_creator_process())return false;
        auto& item=get<I>();
        if constexpr(requires {item.reset();})item.reset();else item={};
        return in_creator_process();
    }
};
}
