#pragma once

// Private prospective storage accounting for the synchronous startup worker.
// An account is only a debit sink: it cannot issue SQL, a profile, an Open or
// a transaction original. No allocator retains the stack scope or debit sink.
#include <cstddef>
#include <lattice/detail/core_model_registry_storage.hpp>
#include <functional>
#include <cstdint>
#include <limits>
#include <memory>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include <stdexcept>
#include <thread>
#include <utility>
#include <unistd.h>

namespace lattice::detail {
class ordinary_initialization_storage_error : public std::runtime_error {
public: using std::runtime_error::runtime_error;
};
class ordinary_initialization_storage_scope final {
    friend struct ordinary_initialization_storage_test_access;
public:
    using debit = void(*)(void*,void*,std::uint64_t);
private:
    const std::uint64_t creator_=static_cast<std::uint64_t>(::getpid());
    const std::thread::id builder_=std::this_thread::get_id();
    void* const account_;
    void* const owner_;
    const debit debit_;
    ordinary_initialization_storage_scope* const previous_;
    static inline thread_local ordinary_initialization_storage_scope* current_=nullptr;
public:
    ordinary_initialization_storage_scope(void* account,void* owner,debit charge)
        :account_(account),owner_(owner),debit_(charge),previous_(current_) {
        if(!account||!owner||!charge)
            throw ordinary_initialization_storage_error("initialization storage requires its retained debit sink");
        if(previous_&&(previous_->creator_!=creator_||previous_->builder_!=builder_||
           previous_->account_!=account_||previous_->owner_!=owner_||previous_->debit_!=debit_))
            throw ordinary_initialization_storage_error("initialization storage cannot replace a current account");
        current_=this;
    }
    ~ordinary_initialization_storage_scope()noexcept {
        if(current_!=this)return;
        // A child may unwind this inherited stack frame. Never leave its TLS
        // pointing to ended stack storage or follow its inherited predecessor.
        current_=creator_==static_cast<std::uint64_t>(::getpid())?previous_:nullptr;
    }
    ordinary_initialization_storage_scope(const ordinary_initialization_storage_scope&)=delete;
    ordinary_initialization_storage_scope& operator=(const ordinary_initialization_storage_scope&)=delete;
    // DATA-only phase check for explicit legacy boundary adapters. It does
    // not dereference a scope, return an account or authorize any operation.
    static bool debit_scope_active()noexcept{return current_!=nullptr;}
    // Denial-only exact lexical identity observation. No borrowed sink is
    // dereferenced, returned or retained; actual Open authority is checked by
    // its real caller separately before any allocation.
    static bool matches_debit_scope(const void* account,const void* owner,debit charge)noexcept {
        const auto* current=current_;return current&&current->creator_==static_cast<std::uint64_t>(::getpid())&&
            current->builder_==std::this_thread::get_id()&&current->account_==account&&
            current->owner_==owner&&current->debit_==charge;
    }
    static std::uint64_t add(std::uint64_t left,std::uint64_t right) {
        if(right>UINT64_MAX-left)
            throw ordinary_initialization_storage_error("initialization storage addition overflow");
        return left+right;
    }
    static std::uint64_t multiply(std::uint64_t count,std::uint64_t width) {
        if(width&&count>UINT64_MAX/width)
            throw ordinary_initialization_storage_error("initialization storage product overflow");
        return count*width;
    }
    static void charge(std::uint64_t bytes) {
        auto* current=current_;
        if(!current)return; // Ordinary non-startup use keeps its own existing account.
        // Check fixed process/thread fields before touching the borrowed sink.
        if(current->creator_!=static_cast<std::uint64_t>(::getpid())||
           current->builder_!=std::this_thread::get_id())
            throw ordinary_initialization_storage_error("inherited initialization storage debit");
        current->debit_(current->account_,current->owner_,bytes);
        if(current->creator_!=static_cast<std::uint64_t>(::getpid()))
            throw ordinary_initialization_storage_error("inherited initialization storage debit return");
    }
};
// Counts the implementation's real rebound allocation request, including a
// container's node/control block/capacity. Opaque malloc/RSS overhead is not
// represented. Failed allocations and deallocation NEVER refund the original
// monotonic debit. A returned DATA container retains no account/scope pointer;
// later ordinary use cannot renew or resurrect the startup account.
template<class T> struct ordinary_initialization_allocator {
    using value_type=T;
    using is_always_equal=std::false_type;
    std::uint64_t creator=static_cast<std::uint64_t>(::getpid());
    ordinary_initialization_allocator()noexcept=default;
    template<class U> ordinary_initialization_allocator(const ordinary_initialization_allocator<U>& other)noexcept
        :creator(other.creator){}
    T* allocate(std::size_t count) {
        if(creator!=static_cast<std::uint64_t>(::getpid()))
            throw ordinary_initialization_storage_error("inherited initialization allocator");
        if(count>std::numeric_limits<std::size_t>::max()/sizeof(T))
            throw ordinary_initialization_storage_error("initialization allocation size overflow");
        ordinary_initialization_storage_scope::charge(
            ordinary_initialization_storage_scope::multiply(count,sizeof(T)));
        auto* result=std::allocator<T>{}.allocate(count);
        if(creator!=static_cast<std::uint64_t>(::getpid()))
            throw ordinary_initialization_storage_error("inherited initialization allocation return");
        return result;
    }
    void deallocate(T* pointer,std::size_t count)noexcept {
        if(creator==static_cast<std::uint64_t>(::getpid()))std::allocator<T>{}.deallocate(pointer,count);
    }
    template<class U> bool operator==(const ordinary_initialization_allocator<U>& other)const noexcept {
        return creator==other.creator;
    }
    template<class U> bool operator!=(const ordinary_initialization_allocator<U>& other)const noexcept {
        return !(*this==other);
    }
};
// Shared source-private owning DATA types. Every dynamic request uses the
// same prospective allocator above, including rebind nodes/control blocks and
// actual string/vector growth. The aliases retain no scope/account pointer.
using ordinary_initialization_text=std::basic_string<char,std::char_traits<char>,
    ordinary_initialization_allocator<char>>;
template<class T>using ordinary_initialization_vector=std::vector<T,ordinary_initialization_allocator<T>>;
template<class K,class V,class Compare=std::less<K>>
using ordinary_initialization_map=std::map<K,V,Compare,
    ordinary_initialization_allocator<std::pair<const K,V>>>;
template<class T,class Compare=std::less<T>>
using ordinary_initialization_set=std::set<T,Compare,ordinary_initialization_allocator<T>>;
// Exact case-sensitive lexical ordering, matching std::less<std::string>.
// Borrowed lookup converts either standard or allocator-owned text to a view;
// comparison creates no owning copy and changes no spelling/case semantics.
struct ordinary_initialization_text_less final {
    using is_transparent=void;
    bool operator()(std::string_view left,std::string_view right)const noexcept {
        return left<right;
    }
};
template<class V>using ordinary_initialization_text_map=ordinary_initialization_map<
    ordinary_initialization_text,V,ordinary_initialization_text_less>;
using ordinary_initialization_text_set=ordinary_initialization_set<
    ordinary_initialization_text,ordinary_initialization_text_less>;
// Source-private value compatibility keeps the original complete comparisons
// when DATA containers change allocator type. No copy, allocation or grant is
// made: compare both lengths and every corresponding value.
template<class Text>
requires std::is_same_v<std::remove_cvref_t<Text>,ordinary_initialization_text>
inline bool operator==(const Text& left,std::string_view right)noexcept {
    return std::string_view(left.data(),left.size())==right;
}
// Two different owning custom strings must select one exact borrowed-value
// comparison, rather than tie between their string_view conversions. Both
// types are deduced: this never adds a candidate for two native operands.
template<class Left,class Right>
requires (std::is_same_v<std::remove_cvref_t<Left>,ordinary_initialization_text>&&
          std::is_same_v<std::remove_cvref_t<Right>,core_model_registry_text>)
inline bool operator==(const Left& left,const Right& right)noexcept {
    return std::string_view(left.data(),left.size())==std::string_view(right.data(),right.size());
}
template<class T,class U,class Alloc>
requires (!std::is_same_v<ordinary_initialization_vector<T>,std::vector<U,Alloc>>)
bool operator==(const ordinary_initialization_vector<T>& left,const std::vector<U,Alloc>& right) {
    if(left.size()!=right.size())return false;
    for(std::size_t index=0;index<left.size();++index)
        if(!(left[index]==right[index]))return false;
    return true;
}
// An explicit ordinary/generic API boundary may need the original standard
// string type. It cannot be an uncharged fallback during the actual startup
// debit scope. Core startup consumers must continue with borrowed views or
// actual allocator-owned storage, never call this adapter.
inline std::string ordinary_initialization_legacy_text(std::string_view value) {
    if(ordinary_initialization_storage_scope::debit_scope_active())
        throw ordinary_initialization_storage_error("startup cannot materialize unaccounted legacy text");
    return std::string(value.data(),value.size());
}
// Preserve std::map::at's missing-key refusal while borrowing a lookup key
// across the private allocator boundary. Never dereference an end iterator,
// create an owning standard key or invent an absent default DATA value.
template<class Mapping>
decltype(auto) ordinary_initialization_map_at(Mapping& values,std::string_view key) {
    const auto at=values.find(key);
    if(at==values.end())throw std::out_of_range("ordinary metadata map key absent");
    return (at->second);
}
// Synchronous private callback view. The actual issuer owns the callable on
// its stack until the complete metadata procedure returns; this value owns no
// capture storage, Open, database, Native original or account. Generic public
// APIs keep their existing std::function value semantics and wrap their live
// reference only inside the synchronous worker.
template<class Signature>class ordinary_initialization_function_ref;
template<class Result,class... Args>
class ordinary_initialization_function_ref<Result(Args...)> final {
    const std::uint64_t creator_=static_cast<std::uint64_t>(::getpid());
    const std::thread::id builder_=std::this_thread::get_id();
    const void* target_=nullptr;
    Result(*invoke_)(const void*,Args...)=nullptr;
public:
    ordinary_initialization_function_ref()noexcept=default;
    ordinary_initialization_function_ref(const ordinary_initialization_function_ref&)=default;
    template<class Callable>
    requires (!std::is_same_v<std::remove_cvref_t<Callable>,ordinary_initialization_function_ref>&&
        std::is_invocable_r_v<Result,const Callable&,Args...>)
    ordinary_initialization_function_ref(Callable& actual) {
        if constexpr(requires{static_cast<bool>(actual);})if(!static_cast<bool>(actual))return;
        target_=std::addressof(actual);
        invoke_=[](const void* target,Args... args)->Result {
            return std::invoke(*static_cast<const Callable*>(target),std::forward<Args>(args)...);
        };
    }
    explicit operator bool()const noexcept {
        return creator_==static_cast<std::uint64_t>(::getpid())&&
            builder_==std::this_thread::get_id()&&target_&&invoke_;
    }
    Result operator()(Args... args)const {
        // Fixed creator/thread fields are checked before any borrowed capture.
        if(!static_cast<bool>(*this))
            throw ordinary_initialization_storage_error("initialization callback lost its synchronous creator");
        return invoke_(target_,std::forward<Args>(args)...);
    }
};
}
