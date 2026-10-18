#include "../../Sources/LatticeCore/src/ordinary_resource_normal_startup.hpp"
#include <chrono>
#include <cstdio>
#include <string_view>
#include <string>
#include <stdexcept>
#if defined(__APPLE__)
#include <TargetConditionals.h>
#include <unistd.h>
#endif
#include "cold_restart_fault_controls.hpp"

// Dedicated explicit resource installer. It neither starts an ambient broker
// nor changes existing primary-MCP commands. New app launches use their actual
// independently image-checked normal factory or the retained child protocol.
int main(int argc,char** argv){
#if defined(__APPLE__) && TARGET_OS_OSX
    const auto creator=::getpid();
    try{
        using namespace lattice::detail::ordinary_installation;
#if defined(LATTICE_RESOURCE_COLD_RESTART_TESTING)
        if(argc==5&&std::string_view(argv[1])=="test-activate-resource-cold-restart"){
            resource_cold_fault_control::select(argv[2],argv[3],argv[4]);
            const auto original_end=std::chrono::steady_clock::now()+std::chrono::seconds(120);
            auto restart=retained_resource_cold_restart::open_staged(original_end);
            auto installed=restart.retire_and_activate(original_end);
            if(::getpid()!=creator)::_exit(74);
            // Returning normally means the requested cut was NOT observed.
            std::fputs("requested crash cut was not reached\n",stderr);return 76;
        }
#endif
        if(argc==3&&std::string_view(argv[1])=="prepare-resource-cold-restart"){
            const auto package=std::string_view(argv[2]);
            if(package.empty()||package.size()>4096)throw std::runtime_error("invalid package argument");
            const auto original_end=std::chrono::steady_clock::now()+std::chrono::seconds(30);
            retained_resource_cold_restart::prepare_package(std::string(package),original_end);
            if(::getpid()!=creator)::_exit(74);
            std::fputs("resource-restart-prepared; epoch not active\n",stdout);return 0;
        }
        if(argc==2&&std::string_view(argv[1])=="activate-staged-resource-cold-restart"){
            const auto original_end=std::chrono::steady_clock::now()+std::chrono::seconds(120);
            auto restart=retained_resource_cold_restart::open_staged(original_end);
            auto installed=restart.retire_and_activate(original_end);
            if(::getpid()!=creator)::_exit(74);
            std::fputs("resource-epoch-active after actual original retirement and namespace exchange\n",stdout);return 0;
        }
    }catch(...){
        if(::getpid()!=creator)::_exit(74);
        std::fputs("resource restart incomplete; original and partial namespace debt retained\n",stderr);return 74;
    }
#endif
    std::fputs("unsupported resource installer command\n",stderr);return 64;
}
