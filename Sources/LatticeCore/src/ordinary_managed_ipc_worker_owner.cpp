#include "ordinary_managed_ipc_role.hpp"
#include "ordinary_managed_ipc_workers.hpp"
#include <stdexcept>
#include <utility>

namespace lattice::detail {
struct ordinary_managed_ipc_worker_owner::implementation {
    const std::shared_ptr<ordinary_managed_ipc_workers> record;
    explicit implementation(std::shared_ptr<ordinary_managed_ipc_workers> value) : record(std::move(value)) {}
};
ordinary_managed_ipc_worker_owner::ordinary_managed_ipc_worker_owner(std::unique_ptr<implementation> value)
    : impl_(std::move(value)) {}
ordinary_managed_ipc_worker_owner::~ordinary_managed_ipc_worker_owner() { request_stop(); }
ordinary_managed_ipc_worker_owner::ordinary_managed_ipc_worker_owner(ordinary_managed_ipc_worker_owner&&) noexcept = default;
ordinary_managed_ipc_worker_owner& ordinary_managed_ipc_worker_owner::operator=(ordinary_managed_ipc_worker_owner&& other) noexcept {
    if (this != &other) { request_stop(); impl_ = std::move(other.impl_); }
    return *this;
}
void ordinary_managed_ipc_worker_owner::start_reader(std::function<void()>&& function) {
    if (!impl_) throw std::runtime_error("managed IPC worker owner moved");
    impl_->record->start(ordinary_managed_ipc_workers::slot::reader, std::move(function));
}
void ordinary_managed_ipc_worker_owner::start_writer(std::function<void()>&& function) {
    if (!impl_) throw std::runtime_error("managed IPC worker owner moved");
    impl_->record->start(ordinary_managed_ipc_workers::slot::writer, std::move(function));
}
void ordinary_managed_ipc_worker_owner::bind_endpoint(std::shared_ptr<ordinary_managed_ipc_endpoint> endpoint) {
    if (!impl_) throw std::runtime_error("managed IPC worker owner moved");
    impl_->record->bind_endpoint(std::move(endpoint));
}
void ordinary_managed_ipc_worker_owner::request_stop() noexcept { if (impl_) impl_->record->request_stop(); }
bool ordinary_managed_ipc_worker_owner::join(std::chrono::steady_clock::time_point deadline) {
    if (!impl_) return false;
    return impl_->record->join(deadline);
}
ordinary_managed_ipc_worker_owner ordinary_managed_ipc_access::make_workers(std::shared_ptr<ordinary_managed_ipc_workers> record) {
    if (!record) throw std::invalid_argument("managed IPC worker reservation missing");
    const auto facts = record->observe();
    if (facts.inherited || facts.stopping || facts.released || facts.starting[0] || facts.starting[1] || facts.started[0] || facts.started[1])
        throw std::runtime_error("managed IPC worker reservation unavailable");
    record->claim_worker_owner();
    return ordinary_managed_ipc_worker_owner(std::make_unique<ordinary_managed_ipc_worker_owner::implementation>(std::move(record)));
}
}
