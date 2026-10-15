#pragma once
#include <lattice/db.hpp>
#include <chrono>
#include <memory>
namespace lattice {class lattice_db;}
namespace lattice::detail {
class ordinary_export_access;
class ordinary_history_receive_state;
class ordinary_managed_receiver_preservation;
struct recovery_writer_access;
// Only actual native complete-history intake can construct this guard. It is
// neither a caller callback/flag nor a decoded source/receipt capability.
class ordinary_history_final_install_guard final {
    friend class ordinary_export_access;
    friend struct recovery_writer_access;
    struct retained;
    struct dispose {void operator()(retained*)const noexcept;};
    std::unique_ptr<retained,dispose> held_;
    ordinary_history_final_install_guard(lattice_db&,std::shared_ptr<ordinary_history_receive_state>);
    void validate(lattice_db&,database&)const;
public:
    ~ordinary_history_final_install_guard();
    ordinary_history_final_install_guard(const ordinary_history_final_install_guard&)=delete;
    ordinary_history_final_install_guard& operator=(const ordinary_history_final_install_guard&)=delete;
};
struct ordinary_history_accepted_storage_test_access;
}
