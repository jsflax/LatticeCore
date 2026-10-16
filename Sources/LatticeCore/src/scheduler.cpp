#include "lattice/scheduler.hpp"
#if defined(__APPLE__) || defined(__linux__)
#include <unistd.h>
#endif

namespace lattice {

void generic_scheduler::execute_work(void* work) {
    struct release_work {
        std::function<void()>* fn;
#if defined(__APPLE__) || defined(__linux__)
        const pid_t creator = ::getpid();
#endif
        ~release_work() noexcept {
#if defined(__APPLE__) || defined(__linux__)
            // A callback may fork before returning or throwing. Its inherited
            // captures must not be newly released by this cleanup in the child.
            if (creator != ::getpid()) return;
#endif
            delete fn;
        }
    } retained{static_cast<std::function<void()>*>(work)};
    if (retained.fn && *retained.fn) (*retained.fn)();
}

generic_scheduler::generic_scheduler(void* context,
                                     void (*invoke_fn)(void* work, void* ctx),
                                     bool (*is_on_thread_fn)(void*),
                                     bool (*is_same_as_fn)(const scheduler*, void*),
                                     bool (*can_invoke_fn)(void*),
                                     void (*destroy_fn)(void*))
    : context_(context)
    , invoke_fn_([invoke_fn](std::function<void()>&& fn, void* ctx) {
        invoke_fn(new std::function<void()>(std::move(fn)), ctx);
    })
    , is_on_thread_fn_(is_on_thread_fn)
    , is_same_as_fn_(is_same_as_fn)
    , can_invoke_fn_(can_invoke_fn)
    , destroy_fn_(destroy_fn)
{}

std::shared_ptr<scheduler> generic_scheduler::make_shared() const {
    return std::make_shared<generic_scheduler>(context_, invoke_fn_, is_on_thread_fn_, is_same_as_fn_,
                                               can_invoke_fn_, destroy_fn_);
}

} // namespace lattice
