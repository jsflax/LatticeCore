#include "../../Sources/LatticeCore/src/ordinary_target_reclamation_fixture.hpp"
#include <chrono>
#include <cstdio>
#include <string_view>
#include <libproc.h>
#include <unistd.h>
int main(int argc,char** argv) {
    // Dedicated real images are installed/signed/stamped before launch. No
    // native store opens, GTest worker, thread or copied admission precedes it.
    proc_taskinfo tasks{};
    if(argc!=2||::proc_pidinfo(::getpid(),PROC_PIDTASKINFO,0,&tasks,sizeof(tasks))!=sizeof(tasks)||
       tasks.pti_threadnum!=1)return 74;
    const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(30);
    try {
        using fixture=lattice::detail::ordinary_target_reclamation_fixture;
        const std::string_view mode(argv[1]);
        if(mode=="--lattice-managed-pair-history-v1")return fixture::run_memory(end);
        if(mode=="--lattice-managed-daemon-history-v1")return fixture::run_daemon(end);
        return 64;
    }catch(...) {
        std::fputs("constructor-fixture: genuine admission or first ending unproved; originals retained\n",stderr);
        return 74;
    }
}
