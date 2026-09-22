#include "recovery_writer_access.hpp"
#include "receive_delivery_guard.hpp"

namespace lattice::detail {
namespace recovery_channel_reset_test_hooks {
thread_local void (*after_writer_capture)()=nullptr;
thread_local void (*after_write_admission)()=nullptr;
}

struct recovery_writer_access::frame {
    lattice_db& owner;
    database& writer;
    database::sync_apply_chunk_state& settlement;
    frame* previous;
    frame(lattice_db& o, database& w, database::sync_apply_chunk_state& s)
        : owner(o), writer(w), settlement(s), previous(current_) { current_ = this; }
    ~frame() { current_ = previous; }
};
thread_local recovery_writer_access::frame* recovery_writer_access::current_ = nullptr;

struct recovery_writer_access::channel_reset_frame {
    lattice_db& owner;
    database& writer;
    database::sync_apply_chunk_state& settlement;
    channel_reset_frame* previous;
    channel_reset_frame(lattice_db& o,database& w,database::sync_apply_chunk_state& s)
        :owner(o),writer(w),settlement(s),previous(reset_current_){reset_current_=this;}
    ~channel_reset_frame(){reset_current_=previous;}
    channel_reset_frame(const channel_reset_frame&)=delete;
};
thread_local recovery_writer_access::channel_reset_frame* recovery_writer_access::reset_current_=nullptr;

bool recovery_writer_access::active_channel_reset_for(const lattice_db& owner,const database& writer) noexcept {
    for(auto* f=reset_current_;f;f=f->previous) {
        if(&f->owner!=&owner)continue;
        const auto* hook=writer.lattice_update_hook_context_.get();
        return &f->writer==&writer&&hook&&hook->sync_chunk==&f->settlement&&
            f->settlement.state==database::sync_apply_chunk_state::phase::active&&
            !f->settlement.commit_attempted;
    }
    return false;
}

bool recovery_writer_access::active_install_for(const lattice_db* owner, sqlite3* connection) noexcept {
    for (auto* f = current_; f; f = f->previous) {
        if (&f->owner != owner) continue;
        auto* hook = f->writer.lattice_update_hook_context_.get();
        return f->writer.internal_handle() == connection && hook &&
            hook->owner == owner && hook->connection == connection && hook->sync_chunk == &f->settlement &&
            f->settlement.state == database::sync_apply_chunk_state::phase::active &&
            !f->settlement.commit_attempted && !f->settlement.premature_commit;
    }
    return false;
}

database* recovery_writer_access::active_writer(lattice_db& owner) {
    for (auto* f = current_; f; f = f->previous) {
        if (&f->owner != &owner) continue;
        // A consumed scope must not fall through to public ownership of a
        // successor. Logical close does not revoke this admitted physical turn.
        auto* h = f->writer.internal_handle();
        auto* hook = f->writer.lattice_update_hook_context_.get();
        if (f->writer.channel_reset_unsettled_.load(std::memory_order_acquire) ||
            !hook || hook->owner != &owner || hook->connection != h || hook->sync_chunk != &f->settlement ||
            f->settlement.state != database::sync_apply_chunk_state::phase::active ||
            f->settlement.commit_attempted || f->settlement.premature_commit ||
            sqlite3_get_autocommit(h) != 0 || sqlite3_txn_state(h, "main") != SQLITE_TXN_WRITE)
            return nullptr;
        return &f->writer;
    }
    // Refuse another thread before any SQLite call: the admitted maintenance
    // frame may hold that connection while waiting for this caller to finish.
    if (owner.is_closed() || owner.txn_owner_thread_.load(std::memory_order_acquire) != std::this_thread::get_id() ||
        !owner.owns_write_transaction()) return nullptr;
    auto& writer = owner.db();
    auto* h = writer.internal_handle();
    if (writer.is_closed() || writer.channel_reset_unsettled_.load(std::memory_order_acquire) || sqlite3_get_autocommit(h) != 0 ||
        sqlite3_txn_state(h, "main") != SQLITE_TXN_WRITE) return nullptr;
    return &writer;
}

recovery_install_result recovery_writer_access::install(std::shared_ptr<lattice_db> owner,
    const std::function<void(database&)>& body) {
    return install_impl(std::move(owner), body, {});
}

sqlite3* recovery_writer_access::active_handle(lattice_db& owner, database& expected_writer) {
    if(active_writer(owner)!=&expected_writer)
        throw db_error("recovery handle requires the exact active owned writer");
    return expected_writer.internal_handle();
}

namespace legacy_sync_write_test_hooks {
thread_local void (*after_writer_capture)()=nullptr;
thread_local void (*after_write_admission)()=nullptr;
}
struct recovery_writer_access::legacy_frame {
    database& writer;
    database::sync_apply_chunk_state& settlement;
    legacy_frame* previous;
    legacy_frame(database& w,database::sync_apply_chunk_state& s)
        :writer(w),settlement(s),previous(legacy_current_){legacy_current_=this;}
    ~legacy_frame(){legacy_current_=previous;}
};
thread_local recovery_writer_access::legacy_frame* recovery_writer_access::legacy_current_=nullptr;

void recovery_writer_access::legacy_sync_write(lattice_db& owner,const std::function<void(database&)>& body) {
    std::shared_ptr<database> writer;
    {
        std::lock_guard<std::mutex> lock(owner.connection_ownership_mutex_);
        if(owner.closed_.load())throw db_error("legacy sync: owner closed");
        writer=owner.db_;
    }
    if(!writer)throw db_error("legacy sync: no writer");
    if(legacy_sync_write_test_hooks::after_writer_capture)legacy_sync_write_test_hooks::after_writer_capture();
    // Probe before the shared-cache gate without requiring idle: exact
    // caller-owned transactions are admitted below under the physical mutex.
    auto* mutex=sqlite3_db_mutex(writer->internal_handle());
    if(sqlite3_mutex_try(mutex)!=SQLITE_OK)throw db_error("legacy sync: writer busy");
    const bool in_callback=database::update_hook_scope::active_for(writer->internal_handle());
    sqlite3_mutex_leave(mutex);
    if(in_callback)throw db_error("legacy sync: update callback reentry");
    {
        lattice_db::store_write_gate_hold gate(owner);
        legacy_sync_write_impl(*writer,&owner,body);
    }
    writer->drain_if_settled();
}
void recovery_writer_access::legacy_sync_write(database& writer,const std::function<void(database&)>& body) {
    // Borrowed database API: the caller retains the physical wrapper/parent.
    // Do not obtain a replacement writer through the parent during this unit.
    legacy_sync_write_impl(writer,nullptr,body);
    writer.drain_if_settled();
}
void recovery_writer_access::legacy_sync_write_impl(database& writer,lattice_db* expected_owner,
    const std::function<void(database&)>& body) {
    using phase=database::sync_apply_chunk_state::phase;
    auto* h=writer.internal_handle();auto* mutex=h?sqlite3_db_mutex(h):nullptr;
#ifndef __EMSCRIPTEN__
    if(!mutex)throw db_error("legacy sync requires a serialized connection");
#endif
    if(sqlite3_mutex_try(mutex)!=SQLITE_OK)throw db_error("legacy sync: writer busy");
    struct unlock {sqlite3_mutex* mutex;~unlock(){sqlite3_mutex_leave(mutex);}} release{mutex};
    if(!body||!h||writer.is_closed()||writer.channel_reset_unsettled_.load(std::memory_order_acquire)||
       database::update_hook_scope::active_for(h))throw db_error("legacy sync: unavailable writer");
    auto* hook=writer.lattice_update_hook_context_.get();
    if(hook&&hook->connection!=h)throw db_error("legacy sync: hook connection mismatch");
    if(expected_owner) {
        std::lock_guard<std::mutex> lock(expected_owner->connection_ownership_mutex_);
        if(expected_owner->closed_.load()||expected_owner->db_.get()!=&writer||!hook||hook->owner!=expected_owner)
            throw db_error("legacy sync: captured writer changed");
    }
    database::sync_apply_chunk_state local;
    auto* settlement=&local;
    const bool nested=sqlite3_get_autocommit(h)==0;
    if(nested) {
        bool admitted=false,has_frame=false;
        for(auto* f=legacy_current_;f;f=f->previous)if(&f->writer==&writer) {
            has_frame=true;settlement=&f->settlement;
            admitted=settlement->state==phase::active&&!settlement->commit_attempted;break;
        }
        if(!has_frame&&hook&&hook->owner) {
            for(auto* f=current_;f;f=f->previous)if(&f->owner==hook->owner) {
                has_frame=true;
                if(&f->writer==&writer&&hook->sync_chunk==&f->settlement) {
                    settlement=&f->settlement;
                    admitted=settlement->state==phase::active&&!settlement->commit_attempted;
                }
                break;
            }
        }
        if(!has_frame) {
            // Preserve the legacy raw/database::begin_transaction caller
            // contract. This borrows its explicit transaction; it never creates an
            // install frame, producer phase, or public active_writer authority.
            // Known foreign Core transactions and an unrelated installed
            // settlement frame are not this caller's raw turn.
            if(hook&&(hook->sync_chunk||
               (hook->owner&&hook->owner->txn_owner_thread_.load(std::memory_order_acquire)!=std::thread::id{}&&
                hook->owner->txn_owner_thread_.load(std::memory_order_acquire)!=std::this_thread::get_id())))
                throw db_error("legacy sync refuses foreign transaction custody");
            local.state=phase::active;admitted=true;
        }
        if(!admitted||(hook&&hook->connection!=h)||
           (hook&&hook->sync_chunk&&hook->sync_chunk!=settlement))
            throw db_error("legacy sync requires the caller's current explicit transaction");
    } else if(sqlite3_txn_state(h,nullptr)!=SQLITE_TXN_NONE)
        throw db_error("legacy sync requires an idle transaction state");
    if(hook&&!hook->sync_chunk)hook->sync_chunk=settlement;
    struct detach {
        database::lattice_update_hook_context* hook;database::sync_apply_chunk_state* local;
        ~detach(){if(hook&&hook->sync_chunk==local)hook->sync_chunk=nullptr;}
    } detach_marker{hook,&local};
    // Fixed transaction statements use the retained handle even if a user
    // logically closes its owner after this unit has been admitted.
    const auto run=[&](const char* sql) {
        database::record_statement();sqlite3_stmt* raw=nullptr;
        const int prepared=sqlite3_prepare_v2(h,sql,-1,&raw,nullptr);
        std::unique_ptr<sqlite3_stmt,decltype(&sqlite3_finalize)> statement(raw,&sqlite3_finalize);
        if(prepared!=SQLITE_OK||!raw||sqlite3_step(raw)!=SQLITE_DONE)
            throw db_error(std::string("SQL execution failed: ")+sqlite3_errmsg(h)+" (SQL: "+sql+")");
    };
    // The physical mutex, not a new busy-statement policy, owns this unit.
    // In particular an escaped PRAGMA ROW retains the legacy COMMIT failure
    // and rollback oracle. Raw COMMIT below does not run the memory drain;
    // the public overload drains only after marker classification/unlocking.
    legacy_frame frame(writer,*settlement);
    bool opened=false,own_commit_started=false;
    const auto current_turn=[&] {
        const int main_state=sqlite3_txn_state(h,"main");
        return settlement->state==phase::active&&!settlement->commit_attempted&&
            (!hook||hook->sync_chunk==settlement)&&sqlite3_get_autocommit(h)==0&&
            (main_state==SQLITE_TXN_WRITE||(nested&&main_state==SQLITE_TXN_READ));
    };
    try {
        if(!nested){writer.begin_transaction();local.state=phase::active;opened=true;}
        // Helper-owned turns already hold WRITE. For a borrowed deferred/read
        // turn, this real main-schema/profile read pins that same transaction's
        // snapshot. Its first mutation either upgrades that snapshot or SQLite
        // refuses a stale upgrade; a newer enrollment cannot be overwritten.
        // Trusted body/test hooks must not settle a borrowed turn: read-only
        // COMMIT need not fire the commit hook, so the marker is not proof
        // against a forbidden read-only COMMIT -> successor sequence.
        require_recovery_local_producer_maintenance_absent(writer);
        if(legacy_sync_write_test_hooks::after_write_admission)legacy_sync_write_test_hooks::after_write_admission();
        if(!current_turn()||writer.is_closed())throw db_error("legacy sync admission consumed before effects");
        body(writer);
        if(!current_turn()||writer.is_closed())
            throw db_error("legacy sync body settled or closed its admitted writer");
        if(!nested) {
            own_commit_started=true;
            run("COMMIT");
            // Memory/no-WAL success has no hook notification. File WAL marks
            // commit BEFORE callbacks, so a callback-opened successor is never
            // mistaken for this unit, including when the callback throws.
            if(local.state==phase::active) {
                if(hook)hook->note_settled(true);else local.state=phase::committed;
            }
        }
    } catch(...) {
        const auto primary=std::current_exception();
        // Standalone databases have no engine hooks and no supported external
        // transaction callbacks. Here only SQLite itself can consume the turn
        // (e.g. RAISE(ROLLBACK)); never use this fallback for a Core writer.
        if(!hook&&local.state==phase::active&&sqlite3_get_autocommit(h)!=0)local.state=phase::rolled_back;
        // A body/admission callback's COMMIT attempt consumes our permission
        // even when memory/no-page commits produce no WAL settlement. Only
        // our own final COMMIT may be cleaned up after an attempted commit:
        // the retained engine commit hook cannot run SQL, and a successful
        // file COMMIT marks the phase before any user callback.
        const bool cleanup_owned=settlement->state==phase::active&&
            (!settlement->commit_attempted||own_commit_started)&&(!hook||hook->sync_chunk==settlement);
        if(opened&&cleanup_owned) {
            try {
                run("ROLLBACK");if(!hook)local.state=phase::rolled_back;
            } catch(...) {
                writer.channel_reset_unsettled_.store(true,std::memory_order_release);
                throw legacy_sync_write_error(primary,std::current_exception());
            }
        }
        // Caller-owned failures remain the caller's responsibility. Do not
        // roll back their turn, reset buffers, or touch a successor transaction.
        std::rethrow_exception(primary);
    }
}

void recovery_writer_access::reset_channel(lattice_db& owner,const std::string& channel,bool retire) {
    std::shared_ptr<database> writer;
    {
        std::lock_guard<std::mutex> lock(owner.connection_ownership_mutex_);
        if(owner.closed_.load())throw db_error("channel reset: owner closed");
        writer=owner.db_;
    }
    if(!writer)throw db_error("channel reset: no actual writer");
    if(recovery_channel_reset_test_hooks::after_writer_capture)recovery_channel_reset_test_hooks::after_writer_capture();
    using phase=database::sync_apply_chunk_state::phase;
    if(writer->channel_reset_unsettled_.load(std::memory_order_acquire))throw db_error("channel reset unsettled; explicit rollback required");
    // These fixed statements belong to the retained admission, including an
    // ordinary caller-owned turn that is logically closed during the unit.
    // Checked native execution avoids database::execute's post-close no-op.
    const auto prepare=[&](const char* sql,const std::string* parameter=nullptr) {
        database::record_statement();sqlite3_stmt* raw=nullptr;
        const int prepared=sqlite3_prepare_v2(writer->internal_handle(),sql,-1,&raw,nullptr);
        std::unique_ptr<sqlite3_stmt,decltype(&sqlite3_finalize)> statement(raw,&sqlite3_finalize);
        if(prepared!=SQLITE_OK||!raw)throw db_error(std::string("channel reset prepare failed: ")+sqlite3_errmsg(writer->internal_handle()));
        if(parameter&&(parameter->size()>static_cast<size_t>(std::numeric_limits<int>::max())||
           sqlite3_bind_text(raw,1,parameter->data(),static_cast<int>(parameter->size()),SQLITE_TRANSIENT)!=SQLITE_OK))
            throw db_error("channel reset bind failed");
        return statement;
    };
    const auto run=[&](const char* sql,const std::string* parameter=nullptr) {
        auto statement=prepare(sql,parameter);
        if(sqlite3_step(statement.get())!=SQLITE_DONE)throw db_error(std::string("channel reset step failed: ")+sqlite3_errmsg(writer->internal_handle()));
    };
    const auto exists=[&](const char* sql) {
        auto statement=prepare(sql,&channel);const int rc=sqlite3_step(statement.get());
        if(rc==SQLITE_DONE)return false;
        if(rc!=SQLITE_ROW||sqlite3_step(statement.get())!=SQLITE_DONE)
            throw db_error("channel reset addressed read failed or was ambiguous");
        return true;
    };
    const auto mutate=[&] {
        const auto receive_before = receive_delivery_guard_access::read_owned(owner,*writer,channel);
        const bool had_slot=exists("SELECT 1 FROM _lattice_replication_slots WHERE sync_id=? LIMIT 2");
        run("DELETE FROM _lattice_sync_state WHERE sync_id=?",&channel);
        run("DELETE FROM _lattice_sync_set WHERE sync_id=?",&channel);
        if(retire)run("DELETE FROM _lattice_replication_slots WHERE sync_id=?",&channel);
        else run("UPDATE _lattice_replication_slots SET confirmed_audit_id=0,upload_floor=0 WHERE sync_id=?",&channel);
        if(exists("SELECT 1 FROM _lattice_sync_state WHERE sync_id=? LIMIT 1")||
           exists("SELECT 1 FROM _lattice_sync_set WHERE sync_id=? LIMIT 1"))
            throw db_error("channel reset postimage mismatch: retained channel state");
        auto slot=prepare("SELECT CASE WHEN typeof(confirmed_audit_id)='integer' THEN confirmed_audit_id END,CASE WHEN typeof(upload_floor)='integer' THEN upload_floor END FROM _lattice_replication_slots WHERE sync_id=? LIMIT 2",&channel);
        const int rc=sqlite3_step(slot.get());
        if(retire||!had_slot) {
            if(rc!=SQLITE_DONE)throw db_error("channel reset postimage mismatch: unexpected slot");
        } else if(rc!=SQLITE_ROW||sqlite3_column_type(slot.get(),0)!=SQLITE_INTEGER||sqlite3_column_int64(slot.get(),0)!=0||
                  sqlite3_column_type(slot.get(),1)!=SQLITE_INTEGER||sqlite3_column_int64(slot.get(),1)!=0||sqlite3_step(slot.get())!=SQLITE_DONE)
            throw db_error("channel reset postimage mismatch: slot floors not zero integers");
        const auto receive_after = retire
            ? receive_delivery_guard_access::retire(owner,*writer,receive_before) : receive_before;
        // Upload/filter reset cannot erase receive ambiguity. Permanent remove
        // retains a tombstone and invalidates old delivery tokens atomically.
        receive_delivery_guard_access::verify_owned(owner,*writer,receive_after);
    };
    auto* active=active_writer(owner);
    if(active) {
        if(active!=writer.get())throw db_error("channel reset: captured writer changed");
        auto* h=writer->internal_handle();auto* mutex=sqlite3_db_mutex(h);
        if(sqlite3_mutex_try(mutex)!=SQLITE_OK)throw db_error("channel reset: owned writer is busy");
        struct unlock {sqlite3_mutex* mutex;~unlock(){sqlite3_mutex_leave(mutex);}} release{mutex};
        if(active_writer(owner)!=writer.get()||database::update_hook_scope::active_for(h))throw db_error("channel reset: owned transaction changed or callback reentry");
        auto* hook=writer->lattice_update_hook_context_.get();
        if(!hook||hook->owner!=&owner||hook->connection!=h)throw db_error("channel reset: hook identity unavailable");
        require_recovery_local_producer_maintenance_absent(*writer);
        database::sync_apply_chunk_state local;local.state=phase::active;
        auto* settlement=hook->sync_chunk?hook->sync_chunk:&local;
        if(settlement->state!=phase::active||settlement->commit_attempted)
            throw db_error("channel reset: caller transaction is settled or attempted commit");
        if(!hook->sync_chunk)hook->sync_chunk=&local;
        struct detach {
            database::lattice_update_hook_context& hook;database::sync_apply_chunk_state& local;
            ~detach(){if(hook.sync_chunk==&local)hook.sync_chunk=nullptr;}
        } detach_marker{*hook,local};
        channel_reset_frame admitted(owner,*writer,*settlement);
        if(recovery_channel_reset_test_hooks::after_write_admission)recovery_channel_reset_test_hooks::after_write_admission();
        if(settlement->state!=phase::active||settlement->commit_attempted||hook->sync_chunk!=settlement||
           sqlite3_get_autocommit(h)!=0||sqlite3_txn_state(h,"main")!=SQLITE_TXN_WRITE)
            throw db_error("channel reset: admitted caller transaction settled before effects");
        bool opened=false;
        try {
            run("SAVEPOINT _lattice_producer_channel_reset");opened=true;
            mutate();
            run("RELEASE _lattice_producer_channel_reset");
        } catch(...) {
            const auto original=std::current_exception();
            if(opened&&settlement->state==phase::active) {
                // ABORT restores only this unit. RAISE(ROLLBACK) already
                // consumed the caller's transaction and must not be repeated.
                try {
                    run("ROLLBACK TO _lattice_producer_channel_reset");
                    run("RELEASE _lattice_producer_channel_reset");
                } catch(...) {
                    writer->channel_reset_unsettled_.store(true,std::memory_order_release);
                    throw recovery_channel_reset_error(original,std::current_exception());
                }
            }
            std::rethrow_exception(original);
        }
        return; // Never COMMIT or ROLLBACK caller-owned work.
    }
    database::maintenance_scope::probe_before_store_gate(*writer);
    std::exception_ptr failure;
    bool owned_started = false;
    try {
        lattice_db::store_write_gate_hold gate(owner);
        database::maintenance_scope maintenance(*writer);
        auto* hook=writer->lattice_update_hook_context_.get();
        {
            std::lock_guard<std::mutex> lock(owner.connection_ownership_mutex_);
            if(owner.closed_.load()||owner.db_!=writer||!hook||hook->owner!=&owner||hook->connection!=writer->internal_handle()||hook->sync_chunk)
                throw db_error("channel reset: writer admission changed");
        }
        database::sync_apply_chunk_state settlement;
        settlement.policy = database::sync_apply_chunk_state::commit_policy::owner_body;
        hook->sync_chunk=&settlement;
        struct detach {
            database::lattice_update_hook_context& hook;database::sync_apply_chunk_state& settlement;
            ~detach(){if(hook.sync_chunk==&settlement)hook.sync_chunk=nullptr;}
        } detach_marker{*hook,settlement};
        const auto current_body = [&] {
            return hook->owner == &owner && hook->connection == writer->internal_handle() &&
                hook->sync_chunk == &settlement && settlement.state == phase::active &&
                !settlement.commit_attempted && !settlement.premature_commit &&
                sqlite3_get_autocommit(writer->internal_handle()) == 0 &&
                sqlite3_txn_state(writer->internal_handle(), "main") == SQLITE_TXN_WRITE;
        };
        bool own_commit_started = false;
        try {
            writer->begin_transaction();settlement.state=phase::active;owned_started=true;
            require_recovery_local_producer_maintenance_absent(*writer);
            if(recovery_channel_reset_test_hooks::after_write_admission)recovery_channel_reset_test_hooks::after_write_admission();
            if (!current_body()) throw db_error("channel reset: owned admission was consumed before effects");
            mutate();
            if (!current_body()) throw db_error("channel reset: owned transaction changed before finalization");
            settlement.policy = database::sync_apply_chunk_state::commit_policy::owner_finalizing;
            own_commit_started = true;
            writer->commit();
            if(settlement.state==phase::active && hook->sync_chunk == &settlement &&
               sqlite3_get_autocommit(writer->internal_handle()) != 0) hook->note_settled(true);
            if (settlement.state != phase::committed)
                throw db_error("channel reset: final COMMIT did not settle the owned transaction");
        } catch(...) {
            const auto original=std::current_exception();
            // WAL marks the real COMMIT before observers. Do not infer our
            // ownership from a successor transaction opened by a callback.
            if(settlement.state==phase::active && hook->sync_chunk == &settlement &&
               (!settlement.commit_attempted || own_commit_started)) {
                try {writer->rollback();}
                catch(...) {
                    writer->channel_reset_unsettled_.store(true,std::memory_order_release);
                    throw recovery_channel_reset_error(original,std::current_exception());
                }
            }
            std::rethrow_exception(original);
        }
    } catch (...) { failure = std::current_exception(); }
    // A consumed admission may have committed an ordinary successor before
    // returning/throwing. Memory delivery was deferred by the outer scope;
    // drain only now, with the original error still retained. An open successor
    // remains pending because drain_if_settled checks actual autocommit.
    try { if (owned_started) writer->drain_if_settled(); }
    catch (...) {
        if (failure) throw recovery_channel_reset_notification_error(failure, std::current_exception());
        throw;
    }
    if (failure) std::rethrow_exception(failure);
}

void reset_sync_channel_with_producer_fence(lattice_db& owner,const std::string& channel,bool retire) {
    recovery_writer_access::reset_channel(owner,channel,retire);
}

recovery_install_result recovery_writer_access::install_impl(std::shared_ptr<lattice_db> owner,
    const std::function<void(database&)>& body, const std::function<void()>& after_unlock,
    const std::function<void()>& after_writer_capture, bool* initial_admission_busy) {
    recovery_install_result result;
    if (initial_admission_busy) *initial_admission_busy = false;
    std::shared_ptr<database> writer;
    lattice_db::recovery_commit_batch batch;
    using phase = database::sync_apply_chunk_state::phase;
    database::sync_apply_chunk_state settlement;
    settlement.policy = database::sync_apply_chunk_state::commit_policy::owner_body;
    bool own_commit_started = false;
    try {
        if (!owner || !body) throw db_error("recovery install requires an owning store and body");
        {
            std::lock_guard<std::mutex> lock(owner->connection_ownership_mutex_);
            if (owner->closed_.load()) throw db_error("recovery install: owner is closed");
            writer = owner->db_;
        }
        if (!writer) throw db_error("recovery install: no published writer");
        // Private deterministic test rendezvous only; no owner/store/SQLite
        // lock is held here. Production supplies no callback.
        if (after_writer_capture) after_writer_capture();
        if (initial_admission_busy) {
            if (!database::maintenance_scope::try_probe_before_store_gate(*writer)) {
                *initial_admission_busy = true;
                return result; // No gate, owned body, mutation or COMMIT entered.
            }
        } else database::maintenance_scope::probe_before_store_gate(*writer);
        {
            lattice_db::store_write_gate_hold gate(*owner);
            std::unique_lock<std::recursive_timed_mutex> memory_gate;
#ifdef __EMSCRIPTEN__
            constexpr bool memory_maintenance = true;
#else
            const bool memory_maintenance = owner->config_.is_in_memory();
#endif
            if (memory_maintenance && !owner->store_write_gate_)
                memory_gate = std::unique_lock<std::recursive_timed_mutex>(owner->vec0_memory_maintenance_gate_);
            database::maintenance_scope maintenance(*writer);
            auto* context = writer->lattice_update_hook_context_.get();
            {
                std::lock_guard<std::mutex> lock(owner->connection_ownership_mutex_);
                // Reader publication increments connection_revision_ too, but
                // does not revoke this exact strongly retained writer. Closing
                // or replacing the writer remains fenced by pointer/owner/hook
                // identity plus maintenance admission; retention prevents ABA.
                if (owner->closed_.load() || owner->db_ != writer ||
                    !context || context->owner != owner.get() || context->connection != writer->internal_handle() ||
                    context->sync_chunk || context->entry_cursor_active || context->recovery_delivery_deferred)
                    throw db_error("recovery install: admission invalidated or hook ownership unavailable");
                const auto producer_allowed = std::atomic_load(&writer->local_producer_write_allowed_);
                if (producer_allowed && !producer_allowed->load(std::memory_order_acquire))
                    throw db_error("recovery install: local producer admission was revoked");
                // Admission linearizes here. A later close may fence new work,
                // but the retained writer can settle under maintenance_scope.
            }
            {
                std::lock_guard<std::mutex> lock(owner->change_buffer_mutex_);
                if (owner->is_flushing_ || owner->recovery_change_buffer_reserved_ || !owner->change_buffer_.empty())
                    throw db_error("recovery install: existing notification delivery is unsettled");
                owner->recovery_change_buffer_reserved_ = true;
                settlement.owns_recovery_reservation = true;
                owner->active_recovery_install_operations_.fetch_add(1, std::memory_order_acq_rel);
            }
            struct release_reservation {
                lattice_db& owner;
                database& writer;
                database::lattice_update_hook_context& context;
                database::sync_apply_chunk_state& settlement;
                ~release_reservation() {
                    if (context.sync_chunk == &settlement) context.sync_chunk = nullptr;
                    // Rollback may already have consumed this reservation.
                    // Its successor now owns any buffered rows and dirty flag.
                    if (context.consume_recovery_reservation(&settlement)) {
                        writer.txn_dirty_.store(false, std::memory_order_relaxed);
                        std::lock_guard<std::mutex> lock(owner.change_buffer_mutex_);
                        owner.recovery_change_buffer_reserved_ = false;
                    }
                    owner.active_recovery_install_operations_.fetch_sub(1, std::memory_order_release);
                }
            } release{*owner, *writer, *context, settlement};
            context->sync_chunk = &settlement;
            context->entry_cursor_active = true;
            context->entry_cursor_present = false;
            context->recovery_delivery_deferred = true;
            frame authority(*owner, *writer, settlement);
            const auto current_body = [&] {
                return context->owner == owner.get() && context->connection == writer->internal_handle() &&
                    context->sync_chunk == &settlement && settlement.state == phase::active &&
                    !settlement.commit_attempted && !settlement.premature_commit &&
                    settlement.owns_recovery_reservation && context->recovery_delivery_deferred &&
                    sqlite3_get_autocommit(writer->internal_handle()) == 0 &&
                    sqlite3_txn_state(writer->internal_handle(), "main") == SQLITE_TXN_WRITE;
            };
            try {
                writer->begin_transaction();
                settlement.state = phase::active;
                body(*writer);
                if (!current_body())
                    throw db_error("recovery install body settled its owned transaction");
                // R-tree holds an internal blob cursor until xSavepoint/xSync.
                // Let SQLite ask each virtual table to settle its own resources
                // before checking for escaped caller statements. Do not reset
                // arbitrary statements, exempt SQL-less blobs, or commit early.
                // This nested savepoint cannot commit the owned outer BEGIN.
                writer->execute("SAVEPOINT _lattice_recovery_body_settled");
                writer->execute("RELEASE _lattice_recovery_body_settled");
                if (!current_body()) throw db_error("recovery install lost ownership at body settlement");
                for (auto* stmt = sqlite3_next_stmt(writer->internal_handle(), nullptr); stmt;
                     stmt = sqlite3_next_stmt(writer->internal_handle(), stmt)) {
                    if (sqlite3_stmt_busy(stmt)) throw db_error("recovery install body left an active statement");
                }
                // Derive exact audit/model events while this write transaction
                // still pins its view. After COMMIT another handle may write.
                owner->flush_changes_once_impl(writer.get(), &batch);
                if (!current_body()) throw db_error("recovery install lost body ownership before finalization");
                settlement.policy = database::sync_apply_chunk_state::commit_policy::owner_finalizing;
                own_commit_started = true;
                writer->commit();
                // Memory / DELETE-journal paths have no WAL callback. Their
                // successful COMMIT return is still before any deferred observer.
                if (settlement.state == phase::active && context->sync_chunk == &settlement &&
                    sqlite3_get_autocommit(writer->internal_handle()) != 0) context->note_settled(true);
                if (settlement.state != phase::committed)
                    throw db_error("recovery install commit did not settle the owned transaction");
            } catch (...) {
                result.primary_error = std::current_exception();
                if (settlement.state == phase::active && context->sync_chunk == &settlement &&
                    (!settlement.commit_attempted || own_commit_started)) {
                    try {
                        writer->rollback();
                        if (settlement.state != phase::rolled_back)
                            throw db_error("recovery install rollback did not settle the owned transaction");
                    } catch (...) {
                        result.cleanup_error = std::current_exception();
                        // Do not publish this still-active transaction to new
                        // ordinary operations. Raw-handle repair is not promised.
                        writer->closed_.store(true, std::memory_order_release);
                    }
                }
            }
            if (settlement.state == phase::committed && own_commit_started) {
                result.state = recovery_install_state::committed;
                if (batch.audit_frontier) {
                    const auto frontier = *batch.audit_frontier;
                    auto advance = [frontier](lattice_db* target) {
                        auto previous = target->last_seen_audit_id_.load(std::memory_order_acquire);
                        while (previous < frontier && !target->last_seen_audit_id_.compare_exchange_weak(
                            previous, frontier, std::memory_order_acq_rel)) {}
                    };
                    if (owner->storage_shared_across_instances())
                        instance_registry::instance().for_each_alive(owner->config_.path, advance);
                    else advance(owner.get());
                }
            } else if (settlement.state == phase::committed) {
                result.state = recovery_install_state::ownership_lost;
                result.unexpected_commit_observed = true;
            } else if (settlement.state == phase::rolled_back) result.state = recovery_install_state::rolled_back;
            else if (settlement.state == phase::active) result.state = recovery_install_state::unsettled;
        }
    } catch (...) {
        if (!result.primary_error) result.primary_error = std::current_exception();
        if (settlement.state == phase::committed) {
            result.state = own_commit_started ? recovery_install_state::committed : recovery_install_state::ownership_lost;
            result.unexpected_commit_observed = !own_commit_started;
        }
    }
    // All owned SQLite/store scopes and private helper authority ended above.
    // Payloads and callback copies likewise die outside those scopes.
    if (result.state == recovery_install_state::committed) {
        try {
            if (after_unlock) after_unlock(); // private deterministic test rendezvous only
            deliver(*owner, batch);
        } catch (...) { result.postcommit_error = std::current_exception(); }
    } else if (writer && settlement.state != phase::not_started) {
        // This belongs to a possible ordinary successor, never to the failed
        // install. Preserve both failures and never run its activation tail.
        try { writer->drain_if_settled(); }
        catch (...) { result.notification_error = std::current_exception(); }
    }
    return result;
}

void recovery_writer_access::deliver(lattice_db& owner, const lattice_db::recovery_commit_batch& batch) {
    auto alive = [&](auto&& fn) {
        if (owner.storage_shared_across_instances()) {
            instance_registry::instance().for_each_alive(owner.config_.path, [&](lattice_db* target) {
                if (!target->is_closed()) fn(target);
            });
        } else if (!owner.is_closed()) fn(&owner);
    };
    // Even a zero-event commit invalidates every remaining alive recipient.
    // Closing the source must not hide its durable commit from a sibling.
    alive([&](lattice_db* target) {
        target->fire_invalidation_hooks_local(batch.invalidations, lattice_db::invalidation_reason::commit);
    });
    if (!batch.events.empty()) alive([&](lattice_db* target) { target->notify_changes_batched_impl(batch.events, &batch.typed_event_indices); });

    // Protect existing source-side hint resources against concurrent close.
    // This is not raw synchronizer ownership and does not enable recovery sync.
    auto guard = owner.guard_;
    {
        std::lock_guard<std::mutex> lock(owner.connection_ownership_mutex_);
        if (owner.closed_.load() || !guard->alive.load()) return;
        ++instance_guard::tls_depths()[guard.get()]; // allocation before refcount publication
        guard->notify_refcount.fetch_add(1, std::memory_order_seq_cst);
    }
    struct release_hold {
        std::shared_ptr<instance_guard> guard;
        ~release_hold() {
            auto& depths = instance_guard::tls_depths();
            if (--depths[guard.get()] == 0) depths.erase(guard.get());
            guard->notify_refcount.fetch_sub(1, std::memory_order_seq_cst);
        }
    } release{guard};
    if (!owner.is_closed() && batch.needs_upload_hint) owner.trigger_sync_upload();
    if (!owner.is_closed() && owner.shared_xproc_notifier_ && !owner.config_.read_only)
        owner.shared_xproc_notifier_->post_notification();
}
} // namespace lattice::detail
