#pragma once
#include "ordinary_owned_launch.hpp"
#include "../include/lattice/ordinary_history.hpp"
#include <array>
#include <cstddef>
#include <limits>
#include <memory>
#include <new>
#include <thread>
namespace lattice::detail {
// Fixed first request DATA in the actual pre-Native Conn state. This is not a
// second resource pool: all requests consume the SAME original owner allowance.
struct ordinary_target_terminal_allocation final {
    struct request {
        std::size_t bytes=0;
        bool reserved=false,fixed_fields=false,entered=false,returned=false,exception_returned=false,containing_constructor_returned=false;
    };
    const ordinary_launch::process_identity process=ordinary_launch::current_process();
    const std::thread::id thread=std::this_thread::get_id();
    std::array<request,8> requests{};
    const void* charge=nullptr;std::size_t predecessor_bytes=0,total_bytes=0;
    bool selected=false,sealed=false;
    ordinary_target_terminal_allocation()=default;
    ordinary_target_terminal_allocation(const ordinary_target_terminal_allocation&)=delete;
    ordinary_target_terminal_allocation& operator=(const ordinary_target_terminal_allocation&)=delete;
    bool current()const noexcept{return process==ordinary_launch::current_process()&&thread==std::this_thread::get_id();}
    bool begin(std::size_t bytes,const void* original)noexcept {
        if(!current()||selected||!original||!bytes||bytes>ordinary_history_limits::owner_bytes)return false;
        selected=true;charge=original;predecessor_bytes=total_bytes=bytes;return true;
    }
    bool reserve(std::size_t index,std::size_t bytes,bool fixed)noexcept {
        if(!current()||!selected||sealed||!charge||index>=requests.size()||requests[index].reserved||!bytes||
           bytes>ordinary_history_limits::owner_bytes-total_bytes)return false;
        auto& value=requests[index];value.reserved=true;value.fixed_fields=fixed;value.bytes=bytes;total_bytes+=bytes;return true;
    }
    bool reserve_fields(std::size_t index,std::size_t bytes)noexcept{return reserve(index,bytes,true);}
    // A returned containing typed constructor proves its real member's fixed
    // `new fields{}` returned. An unknown constructor never receives this fact.
    void fields_constructor_returned(std::size_t index)noexcept {
        if(current()&&selected&&!sealed&&index<requests.size()&&requests[index].reserved&&requests[index].fixed_fields)
            requests[index].containing_constructor_returned=true;
    }
    bool finish(std::size_t used)noexcept {
        if(!current()||!selected||sealed||used>requests.size())return false;
        for(std::size_t at=0;at<requests.size();++at) {
            const auto& value=requests[at];
            if(at>=used){if(value.reserved)return false;continue;}
            if(!value.reserved||!value.bytes||value.exception_returned||
               (value.fixed_fields?!value.containing_constructor_returned:(!value.entered||!value.returned)))return false;
        }
        sealed=true;return true;
    }
    template<class T> struct allocator {
        using value_type=T;
        ordinary_target_terminal_allocation* original=nullptr;std::size_t index=8;
        allocator(ordinary_target_terminal_allocation& first,std::size_t selected)noexcept:original(&first),index(selected){}
        template<class U> allocator(const allocator<U>& other)noexcept:original(other.original),index(other.index){}
        template<class U> struct rebind {using other=allocator<U>;};
        T* allocate(std::size_t n) {
            if(!original||!n||n>std::numeric_limits<std::size_t>::max()/sizeof(T)||!original->reserve(index,n*sizeof(T),false))throw std::bad_alloc();
            auto& first=original->requests[index];first.entered=true;
            try {auto* returned=std::allocator<T>{}.allocate(n);first.returned=true;return returned;}
            catch(...){first.exception_returned=true;throw;}
        }
        void deallocate(T* pointer,std::size_t n)noexcept {
            // The actual source journal can already be dead. Never read it.
            std::allocator<T>{}.deallocate(pointer,n);
        }
        template<class U> bool operator==(const allocator<U>& other)const noexcept{return original==other.original&&index==other.index;}
    };
};
}
