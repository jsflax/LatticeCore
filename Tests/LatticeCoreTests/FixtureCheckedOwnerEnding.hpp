#pragma once
#include <cstdio>
#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include <unistd.h>
namespace lattice::detail {
// Test-only refusal to unwind TempDB after a known incomplete owner close.
// Successful logical close is not independent proof of all physical holders;
// normal fixture reference retirement and production physical ending still apply.
template<class Owner>
void end_fixture_owner_checked(Owner* owner,const char* stage) noexcept {
    if(!owner)return;
    const auto result=owner->close_checked();
    if(result.error||result.cleanup_error||!result.cleanup_complete) {
        std::fprintf(stderr,"fixture owner-ending-debt stage=%s namespaceRetained=1 process=%ld group=%ld error=%d cleanupError=%d cleanupComplete=%d\n",
            stage,long(::getpid()),long(::getpgrp()),int(bool(result.error)),int(bool(result.cleanup_error)),int(result.cleanup_complete));
        std::fflush(stderr);::_exit(96); // Do not destroy retained Native owners or TempDB.
    }
}
}
#endif
