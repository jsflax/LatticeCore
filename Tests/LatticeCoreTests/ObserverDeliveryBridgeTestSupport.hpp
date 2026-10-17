#pragma once
#include <gtest/gtest.h>
#include <lattice.hpp>
#include <dynamic_object.hpp>
#include "FixtureCheckedOwnerEnding.hpp"
#include "../../Sources/LatticeCore/src/observer_delivery_connection.hpp"
#include <deque>
#include <filesystem>
#include <random>
#include <array>
#include <mutex>
#include <thread>
#include <atomic>

namespace lattice::detail {
// Same existing private enrollment adapter as the Native mechanism controls.
// No synthetic profile flag, catalog pruning, granted SQL or constructor key.
struct observer_delivery_connection_test_access {
    using Connection=observer_delivery_connection;
    static void enroll(lattice_db& owner,Connection::policy policy) {
        Connection::enroll(owner,policy,Connection::activation_key{});
    }
};
}
namespace delivery_bridge_testing {
// Only this component TU's real production declarations are registered. Do not
// include TestHelpers.hpp: its unrelated model registrations belong to the
// broad CoreTests application and are not this component's catalog.
struct File {
    std::filesystem::path path;
    explicit File(const std::string& name) {
        std::random_device random;
        path=std::filesystem::temp_directory_path()/(name+"_"+std::to_string(random())+".sqlite");
    }
    ~File()noexcept {
        // Every owner/callback/worker is retired before this member. Refused
        // checked ending uses _exit without invoking namespace cleanup.
        std::error_code error;
        for(const auto& name:std::array<std::filesystem::path,3>{path,path.string()+"-wal",path.string()+"-shm"}) {
            std::filesystem::remove(name,error);
            if(error){std::fprintf(stderr,"delivery component namespace-ending-debt code=%d path=%s\n",error.value(),name.c_str());std::fflush(stderr);::_exit(96);}
        }
    }
    std::string str()const{return path.string();}
    File(const File&)=delete;
};
struct Queue final:lattice::scheduler {
    const std::thread::id thread=std::this_thread::get_id();
    std::mutex mutex;
    std::deque<std::function<void()>> pending;
    std::atomic<bool> closed{false};
    void invoke(std::function<void()>&& work)override {
        std::lock_guard lock(mutex);
        if(!closed.load(std::memory_order_relaxed))pending.push_back(std::move(work));
    }
    bool is_on_thread()const noexcept override{return std::this_thread::get_id()==thread;}
    bool is_same_as(const lattice::scheduler* other)const noexcept override{return other==this;}
    bool can_invoke()const noexcept override{return !closed.load(std::memory_order_acquire);}
    void drain() {
        for(;;){
            std::function<void()> work;
            {std::lock_guard lock(mutex);if(pending.empty())return;work=std::move(pending.front());pending.pop_front();}
            work(); // user/capture completion outside the queue leaf
        }
    }
    void shutdown()override {
        std::deque<std::function<void()>> retired;
        {std::lock_guard lock(mutex);closed.store(true,std::memory_order_release);retired.swap(pending);}
        // Last capture releases are off the queue leaf before owner/File end.
    }
};
}
