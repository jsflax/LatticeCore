#pragma once

#include "sync.hpp"
#include "ipc.hpp"

namespace lattice::detail {

// Native IPC ownership boundary. The factory supplies an owned synchronizer with a dedicated
// asynchronous scheduler. No callback captures the parent lattice_db, config vector, or slot.
class ipc_publication_state : public std::enable_shared_from_this<ipc_publication_state> {
public:
    using factory_type=std::function<std::shared_ptr<synchronizer>(std::unique_ptr<ipc_socket_client>,const sync_config&)>;
    ipc_publication_state(sync_config config,factory_type factory)
        :config_(std::move(config)),factory_(std::move(factory)) {}

    void accept(std::unique_ptr<ipc_socket_client> transport) {
        uint64_t ticket;sync_config config;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(!accepting_) return;
            ticket=++generation_;config=config_;
        }
        std::shared_ptr<synchronizer> next,old;
        try {
            next=factory_(std::move(transport),config);
            if(!next) throw std::runtime_error("IPC factory did not create a synchronizer");
            const std::weak_ptr<ipc_publication_state> weak=shared_from_this();
            next->set_on_progress([weak,ticket](const synchronizer::sync_progress& value) {
                if(const auto state=weak.lock()) state->deliver_progress(ticket,value);
            });
            next->set_on_state_change([weak,ticket](bool value) {
                if(const auto state=weak.lock()) state->deliver_state(ticket,value);
            });
            next->set_on_error([weak,ticket](const std::string& value) {
                if(const auto state=weak.lock()) state->deliver_error(ticket,value);
            });
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if(!accepting_ || generation_!=ticket) return;
                old=std::move(current_);current_=next;published_generation_=ticket;
                // Update requests only enqueue on the dedicated scheduler. Keep their ordering
                // under this lock so an older request cannot overtake a newer accepted policy.
                if(config_.sync_filter) next->update_sync_filter(*config_.sync_filter);
                else next->clear_sync_filter();
            }
            // Never destroy/disconnect an old owner under the publication lock. Disconnect
            // before dropping it: its last callback must not destroy a reader on its own thread.
            if(old) old->disconnect();old.reset();
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if(!accepting_ || published_generation_!=ticket || current_!=next) return;
                // Native factory callbacks are always queued, including transport progress.
                // Close linearizes after this bounded local connect, then revokes before stop.
                next->connect();failure_.reset();
            }
        } catch(const std::exception& error) {
            admission_failed(ticket,next,old,error.what());
        } catch(...) {
            admission_failed(ticket,next,old,"Unknown IPC connection admission failure");
        }
    }

    // Parent close revokes this first, stops/drains the endpoint next, then destroys the returned
    // owner outside all publication/collection locks. Already admitted user callbacks may finish.
    std::shared_ptr<synchronizer> revoke() {
        std::lock_guard<std::mutex> lock(mutex_);accepting_=false;++generation_;
        return std::exchange(current_,{});
    }
    std::shared_ptr<synchronizer> current() const {
        std::lock_guard<std::mutex> lock(mutex_);return accepting_ ? current_ : nullptr;
    }
    bool accepting() const {std::lock_guard<std::mutex> lock(mutex_);return accepting_;}
    std::optional<std::string> failure() const {
        std::lock_guard<std::mutex> lock(mutex_);return failure_;
    }
    void update_filter(std::optional<std::vector<sync_filter_entry>> filter) {
        std::lock_guard<std::mutex> lock(mutex_);
        if(!accepting_) return;
        config_.sync_filter=std::move(filter);
        if(current_) {
            if(config_.sync_filter) current_->update_sync_filter(*config_.sync_filter);
            else current_->clear_sync_filter();
        }
    }
    void set_progress(synchronizer::on_progress_handler callback) {
        std::lock_guard<std::mutex> lock(mutex_);progress_=std::move(callback);
    }
    void set_state(std::function<void(bool)> callback) {
        std::lock_guard<std::mutex> lock(mutex_);state_=std::move(callback);
    }
    void set_error(std::function<void(const std::string&)> callback) {
        std::lock_guard<std::mutex> lock(mutex_);error_=std::move(callback);
    }
private:
    void admission_failed(uint64_t ticket,const std::shared_ptr<synchronizer>& next,
            const std::shared_ptr<synchronizer>& old,const std::string& message) {
        std::shared_ptr<synchronizer> failed;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(published_generation_==ticket && current_==next) failed=std::move(current_);
            if(generation_==ticket || failed) failure_=message;
        }
        // A faulty transport's disconnect must not throw through the endpoint accept thread.
        for(const auto& owner:{old,failed}) if(owner) {
            try {owner->disconnect();} catch(...) {LOG_ERROR("ipc_sync","IPC failed-owner disconnect failed");}
        }
        LOG_ERROR("ipc_sync","IPC connection admission failed: %s",message.c_str());
    }
    bool current_ticket(uint64_t ticket) const {return accepting_ && published_generation_==ticket && current_;}
    void deliver_progress(uint64_t ticket,const synchronizer::sync_progress& value) {
        synchronizer::on_progress_handler callback;
        {std::lock_guard<std::mutex> lock(mutex_);if(current_ticket(ticket)) callback=progress_;}
        if(callback) callback(value);
    }
    void deliver_state(uint64_t ticket,bool value) {
        std::function<void(bool)> callback;
        {std::lock_guard<std::mutex> lock(mutex_);if(current_ticket(ticket)) callback=state_;}
        if(callback) callback(value);
    }
    void deliver_error(uint64_t ticket,const std::string& value) {
        std::function<void(const std::string&)> callback;
        {std::lock_guard<std::mutex> lock(mutex_);if(current_ticket(ticket)) callback=error_;}
        if(callback) callback(value);
    }
    mutable std::mutex mutex_;
    bool accepting_=true;
    uint64_t generation_=0;
    uint64_t published_generation_=0;
    sync_config config_;
    const factory_type factory_;
    std::shared_ptr<synchronizer> current_;
    synchronizer::on_progress_handler progress_;
    std::function<void(bool)> state_;
    std::function<void(const std::string&)> error_;
    std::optional<std::string> failure_;
};

} // namespace lattice::detail
