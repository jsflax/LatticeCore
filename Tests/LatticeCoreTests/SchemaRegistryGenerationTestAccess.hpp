#pragma once
#include <lattice/schema.hpp>

namespace lattice::detail {
struct schema_registry_generation_test_access {
    template<class F> static void isolated(F&& body) {schema_registry actual;body(actual);}
    template<class F> static void observe(schema_registry& actual,void(*call)(void*,schema_registry&),void* context,F&& body) {
        schema_registry::publication_probe probe{call,context};
        const auto* previous=actual.fields_().observation.exchange(&probe,std::memory_order_acq_rel);
        struct restore {schema_registry& owner;const schema_registry::publication_probe* previous;
            ~restore(){owner.fields_().observation.store(previous,std::memory_order_release);}} returned{actual,previous};
        body();
    }
    template<class F> static void gate(schema_registry& actual,F&& body) {
        schema_registry::publication_hold held(actual);body();
    }
    static bool gate_free(schema_registry& actual) {
        if(!actual.fields_().gate.try_lock())return false;actual.fields_().gate.unlock();return true;
    }
    static std::weak_ptr<const schema_generation_v2> weak(const schema_snapshot_v2& actual){return actual.shared_();}
    static std::uint64_t entered_generation() {
        if(!schema_read_scope_v2::current_)throw std::runtime_error("actual schema entry missing");
        return schema_read_scope_v2::current_->view_.generation();
    }
};
}
