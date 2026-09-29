#pragma once
#include "../../Sources/LatticeCore/src/recovery_admission_test_probe.hpp"
#include <future>
#include <thread>

namespace recovery_admission_test {
struct row_probe {
    std::function<void(lattice::database&,sqlite3_stmt*)> callback;
    row_probe* previous;
    void (*prior)(lattice::database&,sqlite3_stmt*);
    static inline thread_local row_probe* current=nullptr;
    explicit row_probe(std::function<void(lattice::database&,sqlite3_stmt*)> f)
        :callback(std::move(f)),previous(current),prior(lattice::detail::recovery_admission_test_hooks::after_read_row) {
        current=this;lattice::detail::recovery_admission_test_hooks::after_read_row=[](auto& db,auto* statement){current->callback(db,statement);};
    }
    ~row_probe(){lattice::detail::recovery_admission_test_hooks::after_read_row=prior;current=previous;}
};
// A real engine SELECT remains SQLITE_ROW/busy on another thread, between
// SQLite calls. The hook does not manufacture cursor/read ownership.
class held_read {
    std::promise<void> arrived_,release_;
    std::future<void> arrived=arrived_.get_future(),released=release_.get_future();
    std::atomic<bool> released_once{false};
    std::thread worker;
public:
    std::exception_ptr error;
    size_t rows=0;
    explicit held_read(lattice::database& db,bool managed=false,std::string sql="SELECT name FROM TestPerson")
        :worker([this,&db,managed,sql=std::move(sql)]{
            bool signaled=false;
            try {
                row_probe probe([&](auto& actual,auto*){
                    if(&actual!=&db||signaled)return;signaled=true;arrived_.set_value();
                    if(released.wait_for(std::chrono::seconds(12))!=std::future_status::ready)std::abort();
                });
                if(managed)rows=db.query_managed_cell("SELECT name FROM TestPerson WHERE id=?","name",1).has_value()?1:0;
                else rows=db.query(sql).size();
            }catch(...){error=std::current_exception();}
            if(!signaled)arrived_.set_value();
        }) {if(arrived.wait_for(std::chrono::seconds(12))!=std::future_status::ready)std::abort();}
    void finish(){if(!released_once.exchange(true))release_.set_value();if(worker.joinable())worker.join();}
    ~held_read(){finish();}
};
}
