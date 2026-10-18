#pragma once
// Private Core cascade DATA storage. The fixed compiled debit entry can only
// charge; it supplies no SQL, owner, Native, profile or completion authority.
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <unistd.h>

namespace lattice::detail {
void debit_core_model_registry_storage_(std::uint64_t bytes);
std::string core_model_registry_legacy_text_(std::string_view);
template<class T>struct core_model_registry_allocator {
    using value_type=T;
    using is_always_equal=std::false_type;
    std::uint64_t creator=static_cast<std::uint64_t>(::getpid());
    core_model_registry_allocator()noexcept=default;
    template<class U>core_model_registry_allocator(const core_model_registry_allocator<U>& other)noexcept:creator(other.creator){}
    T* allocate(std::size_t count) {
        if(creator!=static_cast<std::uint64_t>(::getpid()))throw std::bad_alloc();
        if(count>std::numeric_limits<std::size_t>::max()/sizeof(T))throw std::bad_array_new_length();
        const auto bytes=count*sizeof(T);
        if(bytes>(std::uint64_t(32)<<20))throw std::bad_array_new_length();
        debit_core_model_registry_storage_(bytes); // actual rebound request BEFORE allocation
        auto* result=std::allocator<T>{}.allocate(count);
        if(creator!=static_cast<std::uint64_t>(::getpid()))throw std::bad_alloc();
        return result;
    }
    void deallocate(T* pointer,std::size_t count)noexcept {
        if(creator==static_cast<std::uint64_t>(::getpid()))std::allocator<T>{}.deallocate(pointer,count);
    }
    template<class U>bool operator==(const core_model_registry_allocator<U>& other)const noexcept{return creator==other.creator;}
    template<class U>bool operator!=(const core_model_registry_allocator<U>& other)const noexcept{return !(*this==other);}
};
using core_model_registry_text=std::basic_string<char,std::char_traits<char>,core_model_registry_allocator<char>>;
struct core_model_registry_hash {
    using is_transparent=void;
    std::size_t operator()(std::string_view value)const noexcept{return std::hash<std::string_view>{}(value);}
};
struct core_model_registry_equal {
    using is_transparent=void;
    bool operator()(std::string_view left,std::string_view right)const noexcept{return left==right;}
};
// Deduce the actual operand BEFORE conversion. Namespace lookup must not turn
// unrelated native text into either allocator-owned type as an == candidate.
template<class Text>
requires std::is_same_v<std::remove_cvref_t<Text>,core_model_registry_text>
inline bool operator==(const Text& left,std::string_view right)noexcept{return std::string_view(left.data(),left.size())==right;}
using core_model_registry_set=std::unordered_set<core_model_registry_text,core_model_registry_hash,core_model_registry_equal,
    core_model_registry_allocator<core_model_registry_text>>;
template<class V>using core_model_registry_map=std::unordered_map<core_model_registry_text,V,core_model_registry_hash,
    core_model_registry_equal,core_model_registry_allocator<std::pair<const core_model_registry_text,V>>>;
using core_model_registry_list=std::vector<core_model_registry_text,core_model_registry_allocator<core_model_registry_text>>;
inline core_model_registry_text core_model_registry_text_from_(std::string_view value){return {value.data(),value.size()};}
}
