#pragma once
#if defined(LATTICE_RESOURCE_COLD_RESTART_TESTING) && defined(__APPLE__) && TARGET_OS_OSX
#include "../../Sources/LatticeCore/src/ordinary_resource_cold_restart_test_probe.hpp"
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <poll.h>
#include <sys/stat.h>
#include <unistd.h>

namespace resource_cold_fault_control {
using cut=lattice::detail::resource_cold_restart_test::cut;
inline cut selected=cut::count;
inline unsigned occurrence=0,seen=0;
inline int notify=-1;
inline pid_t parent=0;
inline void observe(cut actual) noexcept {
    if(actual!=selected||++seen!=occurrence)return;
    const std::uint32_t message[]{0xc01d7101u,static_cast<std::uint32_t>(actual),seen};
    const auto* at=reinterpret_cast<const char*>(message);std::size_t remaining=sizeof(message);
    while(remaining){const auto n=::write(notify,at,remaining);if(n<0&&errno==EINTR)continue;if(n<=0)::_exit(76);at+=n;remaining-=static_cast<std::size_t>(n);}
    // The owning test parent must observe and kill this exact child. No
    // destructor, rollback or normal installer return simulates the crash.
    for(unsigned seconds=0;seconds!=30;++seconds){if(::getppid()!=parent)::_exit(76);pollfd pipe{notify,0,0};::poll(&pipe,1,1000);if(pipe.revents&(POLLERR|POLLHUP|POLLNVAL))::_exit(76);}
    ::_exit(76);
}
inline unsigned bounded_number(const char* raw,unsigned minimum,unsigned maximum){
    if(!raw||!*raw||*raw=='-')throw std::runtime_error("invalid crash-control argument");char* end=nullptr;errno=0;
    const auto value=std::strtoul(raw,&end,10);if(errno||!end||*end||value<minimum||value>maximum)throw std::runtime_error("invalid crash-control argument");return static_cast<unsigned>(value);
}
inline void select(const char* name,const char* count,const char* descriptor){
    for(unsigned i=0;i<static_cast<unsigned>(cut::count);++i)if(std::strcmp(name,lattice::detail::resource_cold_restart_test::names[i])==0)selected=static_cast<cut>(i);
    if(selected==cut::count)throw std::runtime_error("unknown crash-control cut");
    occurrence=bounded_number(count,1,4096);notify=static_cast<int>(bounded_number(descriptor,3,1023));
    struct stat pipe{};if(::fstat(notify,&pipe)||!S_ISFIFO(pipe.st_mode))throw std::runtime_error("actual stop pipe required");
    parent=::getppid();lattice::detail::resource_cold_restart_test::callback=observe;
}
}
#endif
