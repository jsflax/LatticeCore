#pragma once
#include "observer_delivery_domain.hpp"
#include <memory>

namespace lattice { class database; }
namespace lattice::detail {
class database_initialization_budget;
class observer_delivery_data_domain;
// Private physical binding. This does not enroll SQL, register an observer,
// grant a Native operation or refund a transaction/delivery. Those producers
// must retain the returned exact lease through their real ending/tail.
class observer_delivery_store {
    friend class database_initialization_budget;
    using state=observer_delivery_data_domain;
    std::shared_ptr<state> state_;
    explicit observer_delivery_store(std::shared_ptr<state> value):state_(std::move(value)){}
public:
    using domain=observer_delivery_domain;
    using status=domain::status;
    using units=domain::units;
    using clock=domain::clock;
    class cancellation;
    class client;
    class delivery_scope;
    class lease {
        // Declaration order matters: actual domain lease retires BEFORE the
        // last physical binding pin. A lease-only tail still binds the store.
        std::shared_ptr<state> store_;
        domain::lease held_;
        lease(std::shared_ptr<state>,domain::lease);
        friend class observer_delivery_store;
        friend class database_initialization_budget;
        friend class client;
        friend class delivery_scope;
    public:
        lease()=default;
        lease(const lease&)=default;
        lease(lease&&)noexcept=default;
        lease& operator=(lease)noexcept;
        bool valid()const noexcept;
        // Validation finishes before the caller enters its Native operation.
        // This reads the already-bound physical identity; it creates no FD.
        bool current_for(database&)const;
        units charge()const noexcept;
        void reset()noexcept;
    };
    struct admission {status code=status::invalid;lease held;};
    class client {
        std::shared_ptr<state> store_;
        domain::client client_;
        client(std::shared_ptr<state>,domain::client);
        friend class observer_delivery_store;
        friend class database_initialization_budget;
    public:
        client()=default;
        client(const client&)=default;
        client(client&&)noexcept=default;
        client& operator=(client)noexcept;
        admission try_acquire(units)const;
        admission acquire(units,clock::time_point,const cancellation* = nullptr)const;
        void close()const noexcept;
        void reset()noexcept;
    };
    struct registration {status code=status::invalid;client value;};
    class delivery_scope {
        const std::shared_ptr<state> store_;
        domain::delivery_scope actual_;
    public:
        explicit delivery_scope(const lease& value):store_(value.store_),actual_(value.held_){}
        delivery_scope(const delivery_scope&)=delete;
        delivery_scope& operator=(const delivery_scope&)=delete;
    };
    class cancellation {
        const std::shared_ptr<state> store_;
        domain::cancellation stop_;
        friend class client;
    public:
        explicit cancellation(const observer_delivery_store&);
        void request_stop()noexcept;
        bool stop_requested()const noexcept;
    };
    observer_delivery_store()=default;
    // Called off all Native/store/registry leaves. Uses the real currently
    // validated device/inode; never a normalized path or caller identity hash.
    // Fixed directory: at most64 live physical bindings. No domain is replaced
    // while any actual client, admission or last-payload lease still pins it.
    static observer_delivery_store bind(database&,domain::limits);
    registration register_client()const;
    domain::snapshot inspect()const noexcept;
    bool same_domain(const observer_delivery_store&)const noexcept;
    bool current_for(database&)const;
};
}
