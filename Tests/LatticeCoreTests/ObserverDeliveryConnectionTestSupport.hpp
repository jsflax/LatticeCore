#pragma once
#include "TestHelpers.hpp"
#include "FixtureCheckedOwnerEnding.hpp"
#include "ObserverDeliveryRegistrationObservation.hpp"
#include "../../Sources/LatticeCore/src/observer_delivery_connection.hpp"
#include <deque>

namespace lattice::detail {
struct observer_delivery_connection_test_access {
    using Connection=observer_delivery_connection;
    static void enroll(lattice_db& owner,Connection::policy policy) {
        observer_delivery_registration_testing::observe([&]{
            Connection::enroll(owner,policy,Connection::activation_key{});
        });
    }
};
}
namespace observer_delivery_testing {
// The complete actual retained catalog is validated by real enroll(). No
// partial schema/permission/original/ending bits are fabricated by this helper.
struct Queue final:lattice::scheduler {
    std::mutex mutex;
    std::deque<std::function<void()>> pending;
    void invoke(std::function<void()>&& work)override{
        std::lock_guard lock(mutex);pending.push_back(std::move(work));
    }
    bool is_on_thread()const noexcept override{return true;}
    bool is_same_as(const lattice::scheduler* other)const noexcept override{return other==this;}
    bool can_invoke()const noexcept override{return true;}
    void drain(){
        for(;;){
            std::function<void()> work;
            {std::lock_guard lock(mutex);if(pending.empty())return;work=std::move(pending.front());pending.pop_front();}
            work(); // Actual callback and payload retirement stay outside the queue leaf.
        }
    }
};
class FixtureOwner final:public lattice::lattice_db {
    static lattice::detail::recovery_owner_schema declarations() {
        const auto* actual=lattice::schema_registry::instance().get_schema(typeid(TestPerson));
        lattice::detail::recovery_owner_schema catalog;
        if(!actual||!catalog.admit_model(*actual))throw std::runtime_error("actual fixture declaration unavailable");
        return catalog;
    }
public:
    explicit FixtureOwner(const lattice::configuration& config):lattice_db(config,false,declarations()){}
};
struct Fixture {
    using Connection=lattice::detail::observer_delivery_connection;
    using Store=lattice::detail::observer_delivery_store;
    TempDB file{"observer_delivery_connection"};
    std::shared_ptr<Queue> queue=std::make_shared<Queue>();
    std::shared_ptr<lattice::lattice_db> owner;
    Connection::policy policy;
    Store store;
    Fixture():policy{{2,8,{16,2*1024*1024},{16,2*1024*1024}},
        {16,256*1024,1024*1024},std::chrono::milliseconds(1000)} {
        lattice::configuration config(file.str());config.sched=queue;config.audit_retention_seconds=0;
        owner=std::make_shared<FixtureOwner>(config);
        lattice::instance_registry::instance().get_or_create_notifier(file.str())->stop_listening();queue->drain();
        {auto seed=owner->add(TestPerson{"seed",7,std::nullopt});}
        queue->drain();
        lattice::detail::observer_delivery_connection_test_access::enroll(*owner,policy);
        store=Store::bind(owner->db(),policy.domain);
    }
    ~Fixture(){if(owner)lattice::detail::end_fixture_owner_checked(owner.get(),"observer-delivery-connection");}
    Fixture(const Fixture&)=delete;
};
}
