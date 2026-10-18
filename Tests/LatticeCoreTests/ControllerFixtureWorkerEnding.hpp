#pragma once
#include "RecoveryControllerExecutorTestAccess.hpp"
#include "../../Sources/LatticeCore/src/recovery_receiver_controller.hpp"
#include <array>
#include <cstdio>
#include <cstdlib>
#include <utility>
#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include <unistd.h>
namespace lattice::detail {
// Test-only custody of DATA observation tickets, never a controller, grant,
// live writer or alternate worker. Each actual incarnation remains distinct.
class controller_fixture_worker_endings final {
    using access=recovery_controller_executor_test_access;
    const uint64_t creator_=database_native_process();
    std::mutex mutex_;
    std::array<access::ticket,64> originals_{};
    size_t count_=0;
    [[noreturn]] static void debt(const char* stage)noexcept {
        std::fprintf(stderr,"controller-fixture worker-ending-debt stage=%s namespaceRetained=1 process=%ld group=%ld\n",stage,long(::getpid()),long(::getpgrp()));
        std::fflush(stderr);::_exit(95);
    }
public:
    [[noreturn]] static void fail_ending(const char* stage)noexcept {debt(stage);}
    void observe(const recovery_controller_executor& actual) {
        if(creator_!=database_native_process())debt("foreign-observation");
        auto ticket=access::observe(actual); // Actual prepublication executor state.
        if(!ticket)debt("missing-original");
        std::lock_guard lock(mutex_);
        for(size_t i=0;i<count_;++i)if(originals_[i]==ticket)return;
        if(count_==originals_.size())debt("observation-capacity");
        originals_[count_++]=std::move(ticket);
    }
    void join_originals()noexcept {
        if(creator_!=database_native_process())debt("foreign-ending");
        try {
            std::array<access::ticket,64> originals;size_t count;
            {std::lock_guard lock(mutex_);originals=originals_;count=count_;}
            // Ending-only budget used by the existing genuine executor fixture;
            // not extra test progress, source authorization or admission time.
            const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(3);
            for(size_t i=0;i<count;++i){
                const auto now=std::chrono::steady_clock::now();
                const auto remaining=now<end?std::chrono::duration_cast<std::chrono::milliseconds>(end-now):std::chrono::milliseconds(0);
                if(!access::joined(originals[i],remaining))debt("actual-worker-join-unproved");
                const auto fact=access::snapshot(originals[i]);
                if(!fact.available||!fact.transferred||!fact.loop_ended||!fact.joined||fact.active||fact.pending||fact.timed||fact.retirement_error||fact.worker_error)
                    debt("actual-worker-ending-failed");
            }
            // Successful distinct generations can now release only their DATA.
            {std::lock_guard lock(mutex_);for(size_t i=0;i<count;++i)originals_[i].reset();count_=0;}
        }catch(...){debt("ending-observation-threw");}
    }
};
struct controller_fixture_worker_selection final {
    using controller=recovery_receiver_controller;
    struct resources {
        std::shared_ptr<const controller::worker_observer> previous,installed;
    };
    const uint64_t creator=database_native_process();
    resources* original=nullptr;
    explicit controller_fixture_worker_selection(std::shared_ptr<controller_fixture_worker_endings> value) {
        auto staged=std::make_unique<resources>();
        staged->installed=std::make_shared<const controller::worker_observer>(
            [value=std::move(value)](const recovery_controller_executor& actual){value->observe(actual);});
        {std::lock_guard lock(controller::test_mutex_);
            staged->previous=std::exchange(controller::test_worker_observer_,staged->installed);}
        original=staged.release();
    }
    ~controller_fixture_worker_selection()noexcept {
        if(creator!=database_native_process()){original=nullptr;return;} // Abandon inherited capture capsule before any leaf.
        if(!original)return;
        try {
            bool exact=false;
            {std::lock_guard lock(controller::test_mutex_);
                exact=controller::test_worker_observer_==original->installed;
                if(exact)controller::test_worker_observer_=std::move(original->previous);}
            if(!exact)controller_fixture_worker_endings::fail_ending("worker-registration-replaced");
            delete original;original=nullptr; // Callback/collector destruction off the test leaf.
        }catch(...){controller_fixture_worker_endings::fail_ending("worker-registration-ending-threw");}
    }
    controller_fixture_worker_selection(const controller_fixture_worker_selection&)=delete;
    controller_fixture_worker_selection& operator=(const controller_fixture_worker_selection&)=delete;
};
}
#endif
