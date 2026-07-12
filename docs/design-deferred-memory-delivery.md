# Design: Transaction-Settled Observer Delivery for In-Memory Lattices

- **Status:** Proposed (design only — no implementation in this document's commit)
- **Date:** 2026-07-12
- **Baseline:** latticecore `8285332` (post-0.10.8), lattice (Swift) `Sources/Lattice/Lattice.swift` delivery-ordering contract (lines ~1497–1511)
- **Defects addressed:**
  - **DEFECT 1 — in-txn delivery lock:** in-memory lattices deliver observer callbacks synchronously from inside the writer's still-open transaction, per statement (`Sources/LatticeCore/src/lattice.cpp:96–136` AuditLog direct-notify, `:185–198` immediate `flush_changes()`).
  - **DEFECT 2 — sync fan-out gap:** `notify_observers` in `Sources/LatticeCore/src/sync.cpp:2413–2431` delivers mark-synced AuditLog UPDATE events only to the local instance for any in-memory DB, never fanning out to same-name shared-cache handles.

---

## 1. Problem statement

### 1.1 DEFECT 1

`lattice_db::setup_change_hook()` (`src/lattice.cpp:67–212`) registers a `sqlite3_update_hook` on the write connection. For file-backed DBs, the hook only **buffers** each row change (`append_to_change_buffer`) and delivery waits for the `sqlite3_wal_hook`, which SQLite invokes **after the commit has taken place and the associated write-lock has been released** — so observer callbacks always run post-commit with a consistent, readable database.

Memory databases cannot use WAL (`PRAGMA journal_mode = WAL` on a memory DB silently stays `MEMORY`), so the WAL hook never fires. The current workaround is delivery **directly from the update hook**:

- `lattice.cpp:192–194` — `if (self->config_.is_in_memory()) self->flush_changes();` — per **statement**, not per transaction;
- `lattice.cpp:104–136` — AuditLog INSERTs additionally run a `SELECT globalId` on the invoking connection and call `notify_changes_batched` immediately, fanning out to all same-name instances via `instance_registry::for_each_alive` when the path contains `cache=shared`.

Since the default scheduler is `immediate_scheduler` (`lattice.hpp:815`, `scheduler.hpp:259–276` — `invoke` runs the closure inline), the entire observer callback chain executes:

1. **inside the writer's open transaction** (the update hook fires mid-`sqlite3_step`, before COMMIT),
2. **inside SQLite's C hook frame**, and
3. for named shared-cache memory DBs (`file:<name>?mode=memory&cache=shared`, new in 0.10.6), **on other instances** whose callbacks read through *their own* connection to the same shared-cache store.

Consequences:

- **SQLITE_LOCKED:** shared-cache table locks held by the open write transaction cause any read through a second same-name connection (`read_db()` falls back to the write connection for memory DBs, `lattice.hpp:2665`) to fail immediately with `SQLITE_LOCKED` — the busy timeout does *not* apply to shared-cache table locks. `database::query` throws `db_error` (`db.cpp:555`).
- **UB on unwind:** that `db_error` (or any exception from a user callback) unwinds through `notify_changes_batched` → `flush_changes` → the update-hook lambda → **SQLite's C frames inside `sqlite3_step`**. Throwing across C frames is undefined behavior and can leave the VDBE/pager in a corrupt state even where the platform ABI happens to tolerate it.
- **Contract violation:** `add_table_observer`'s documented contract ("a single logical transaction is one fire", `lattice.hpp:1740–1744`) is violated for memory DBs — a multi-statement transaction produces one fire per statement, and the observer can read *uncommitted* state (or fail to, per above).
- **Live production shape:** the synchronizer applies remote changes in explicit chunked transactions (`sync.cpp:2777–2948`), and its dedicated sync handle shares the named-memory store with the app's handle — every applied chunk currently fires cross-instance callbacks inside the sync transaction.

### 1.2 DEFECT 2

`sync.cpp:2413–2431`:

```cpp
if (db.config().is_in_memory()) {
    db.notify_changes_batched(events);          // local instance only
} else {
    instance_registry::instance().for_each_alive(db.config().path, ...);
}
```

Compare `flush_changes` (`lattice.hpp:1416–1424`), whose predicate treats shared-cache memory DBs like file DBs:

```cpp
bool is_file_db = !config_.is_in_memory() ||
                  config_.path.find("cache=shared") != std::string::npos;
```

So when sync marks AuditLog entries synchronized (`mark_audit_entries_synced`, `mark_audit_entries_synced_for` — both call `notify_observers` at `sync.cpp:2481` / `:2567`; callers include the Swift bridge `receive_sync_data` and the C API), the resulting `AuditLog UPDATE` events reach only the handle the synchronizer happens to own. Other same-name handles — e.g. the app's main handle observing sync progress — never hear that entries became `isSynchronized=1`. On file DBs the fan-out exists; on named memory DBs it silently doesn't.

---

## 2. Investigation findings

### 2.1 Current delivery topology

| Storage | Buffering | Delivery point | Txn state at delivery | Cross-instance fan-out |
|---|---|---|---|---|
| File (WAL) | `append_to_change_buffer` in update hook | `sqlite3_wal_hook` → `flush_changes` | **committed, write-lock released** (documented safe to read/write) | yes (`for_each_alive`) |
| `:memory:` (isolated) | none for AuditLog; buffer + immediate flush for model rows | update hook, per statement | **open transaction** | no (correct — storage isolated) |
| Named / anonymous shared-cache memory | same as `:memory:` | update hook, per statement | **open transaction** | yes for update-hook path (`lattice.cpp:128`), **no for sync mark-synced** (DEFECT 2) |
| Emscripten (DELETE journal) | buffer + immediate flush | update hook, per statement | **open transaction** | n/a (single instance typical) |

### 2.2 (a) Is `sqlite3_commit_hook` a viable per-transaction flush point? — **No for delivery, unnecessary as a signal.**

Findings against the SQLite contract (system SQLite on Apple; amalgamation 3.45.0 in the CMake build — semantics identical):

- **Timing/locks:** the commit hook fires from within the commit path (`sqlite3VdbeHalt`) *before* the commit is finalized — its return value can still convert the COMMIT into a ROLLBACK. Shared-cache table write locks and the pager exclusive lock are therefore **still held** while the hook runs. A callback reading through a second same-name connection hits exactly the same `SQLITE_LOCKED` as today; nothing is gained on that axis.
- **Documented restriction:** "The callback implementation must not do anything that will modify the database connection that invoked the commit hook. … running any other SQL statements, including SELECT statements, or merely calling sqlite3_prepare_v2() and sqlite3_step() will modify the database connection." The same paragraph governs `sqlite3_update_hook`. This codebase already bends that rule with SELECTs inside the update hook (`lattice.cpp:113`, `:171`) — tolerated in practice for same-connection reads — but user observer callbacks are arbitrary code and can *write*, which is firmly out.
- **C-frame problem unchanged:** the commit hook runs inside `sqlite3_step`'s C frames, so the exception-unwind UB remains.
- **As a boundary detector it is redundant:** the same "transaction just closed" fact is available *strictly later and outside all SQLite frames* by checking `sqlite3_get_autocommit()` after the top-level `sqlite3_step`/`sqlite3_exec` returns in the `db.cpp` wrappers. There is no case the commit hook catches that the post-statement check misses (every statement — including COMMIT itself — funnels through those wrappers; see 2.3).

Verdict: reject commit-hook delivery; do not add a commit hook at all. (Note `~database` already defensively clears one at `db.cpp:130`.)

### 2.3 (b) Does a post-statement drain point exist? — **Yes, and it is airtight.**

Every statement that can close a transaction funnels through `database`'s public wrappers in `src/db.cpp`:

- **Autocommit DML/DDL:** `execute()` (`db.cpp:176`, both the `sqlite3_exec` fast path and the prepared path), `insert()` (`:368`), `update()` (`:477`), `remove()` (`:521`). When no explicit transaction is open, the implicit transaction commits inside that call; on return `sqlite3_get_autocommit() != 0` and **all locks are released**.
- **Explicit transactions:** `BEGIN` goes through `database::begin_transaction` / `try_begin_immediate` (raw `sqlite3_exec`, leaves autocommit=0 — no drain), and **`COMMIT`/`ROLLBACK` go through `database::commit()`/`rollback()` which are `execute("COMMIT")` / `execute("ROLLBACK")`** (`db.cpp:700–706`). So the drain point after `execute` returns covers explicit commit, on the writing thread, outside every SQLite frame. The `transaction` RAII guard (`db.cpp:714–736`) routes through the same functions.
- **`lattice_db` level:** there is no `commitTransaction`; the equivalents are `lattice_db::begin_transaction()/commit()/rollback()/write()` (`lattice.hpp:2628–2642`), all of which delegate to the above. The Swift bridge (`CxxBackend.swift:284 → ref.begin_transaction()`) and every internal transactional path (`add_bulk` `lattice.hpp:1062`, `remove` cascade `:1958`, `heal_collapsed_sync_state` `:868`, sync apply `sync.cpp:2777/2948`, mark-synced `:2441/2472`, `:2498`) use them.
- **Raw `sqlite3_step` sites** outside `db.cpp` (`lattice.hpp:1118` bulk-insert loop; `:4836` inside the `sync_disabled()` SQL function) execute strictly *inside* an already-open transaction or inside statement execution — they can never be the statement that closes a transaction, so the drain at the enclosing `commit()` covers them.
- **`query()`** cannot normally close a transaction, but adding the same (one relaxed atomic load) drain check there is free insurance for DML-via-RETURNING issued through `query`.

Two facts make this point *the* correct one:

1. It is the exact post-commit-post-unlock point the WAL hook gives file DBs ("the callback … may read, write or checkpoint the database as required") — but reached through plain C++ frames, so callback exceptions propagate normally to the caller of `add()`/`write()` instead of unwinding through SQLite.
2. It stays **on the writing thread, synchronously** — required by `flush_changes`' thread-local contract (`tls_notify_local_object_observers_`, "flush_changes always runs synchronously on the writing thread", `lattice.hpp:4015–4021`) and by tests/consumers that rely on immediate-scheduler promptness (callback fires before `add()` returns for autocommit writes, before `write{}` returns for explicit transactions — both preserved, see 3.4).

### 2.4 (c) Change-buffer interaction and the exactly-once / commit-order contract

The Swift contract (`Lattice.swift:1497–1511`): every committed change from a same-process writer is delivered **exactly once**, and each **batch is walked synchronously in commit order**; cross-batch arrival order is best-effort; consumers sort by AuditLog id for total order. Pinned by `ObservationOrderingTests`.

How the buffer behaves under deferral:

- `append_to_change_buffer` (`lattice.hpp:1382–1386`) appends in update-hook firing order — which *is* intra-transaction commit order. Deferring the flush to transaction close changes nothing about buffer order; it changes only how many statements' entries share one flush. One drain per top-level transaction ⇒ one `notify_changes_batched` batch per logical transaction — the same batching file DBs get from the WAL hook, and what `add_table_observer`'s doc already promises.
- **AuditLog events join the buffer instead of a side channel.** Today's in-memory direct-notify exists only because `flush_changes()` used to run *during the model row's update hook, before the AuditLog trigger fired* (`lattice.cpp:106–107`). With delivery at transaction close, the trigger has long since fired; the AuditLog INSERT can simply be buffered by the hook like everything else. Buffered order becomes `[model row, its audit row, model row, its audit row, …]` — interleaved rather than file-DB's "models then audits (Pass 2)" shape, but `notify_changes_batched` groups by table for table observers (`lattice.hpp:1833–1852`), so each observer still sees its table's slice in commit order. Per-object observers fire per-row in input order — unchanged.
- **Exactly-once:** the buffer is moved-out under `change_buffer_mutex_` once per drain; `is_flushing_` prevents re-entrant double delivery; keeping `flush_changes`' Pass 2/3 `is_in_memory` skips (`lattice.hpp:1554–1560`, `:1607`) prevents AuditLog events from being derived a second time on top of the buffered ones.
- **Rollback must discard, not deliver.** Today nothing clears the buffer on ROLLBACK — for in-memory the immediate flush mostly hides this, but for *file* DBs a rolled-back transaction's buffered model events linger and are delivered as phantoms by the **next** WAL flush (Pass 1 emits them even though Pass 2 finds no audit row). A `sqlite3_rollback_hook` that clears the buffer fixes the new design's requirement and this latent file-DB bug with one mechanism. (The rollback hook only mutates a C++ vector and an atomic — no SQLite calls, no allocation-failure paths that throw — so it is safe inside the C hook frame.)
- **Writes from observer callbacks (re-entrancy):** with immediate delivery, a callback that writes triggers a nested `flush_changes` that the `is_flushing_` guard early-returns, stranding those entries until the *next* unrelated write (pre-existing bug). The drain design fixes this: after delivery completes, re-check the buffer and loop (bounded, e.g. 64 iterations + `LOG_WARN`) until empty.
- **Racing writers on one connection:** the connection is FULLMUTEX-serialized; a second thread's statements between COMMIT and the drain's autocommit check can merge two transactions into one delivered batch. This is parity with file-DB WAL-hook behavior (shared buffer, `is_flushing_` coalescing) and allowed by the contract (cross-batch order is best-effort; intra-batch order still commit order).

### 2.5 Rejected alternatives

| Alternative | Why rejected |
|---|---|
| **Commit-hook flush** | Locks still held (SQLITE_LOCKED unchanged); documented no-modify restriction; C-frame unwind UB unchanged; veto semantics (nonzero return rolls back) make it an actively dangerous place for arbitrary callbacks; redundant as a boundary signal (2.2). |
| **Scheduler-hop async delivery** (dispatch `flush_changes` via `scheduler_->invoke` from the hook) | `immediate_scheduler` is the default and synchronous — `invoke` runs inline, fixing nothing. Forcing a real hop (thread/actor) breaks the thread-local contract (`tls_notify_local_object_observers_` read on the wrong thread ⇒ upsert object-observer refresh lost), breaks promptness that tests and the C++ API rely on, and re-orders delivery relative to `add()` return. |
| **`PRAGMA read_uncommitted` on observer-side connections** | Dirty reads: an observer would see half of another handle's open transaction — violates snapshot semantics the whole observation model assumes. Also fixes neither per-statement batching nor the exception-through-C-frame UB. |
| **Swallow exceptions in the hook and keep in-txn delivery** | Papers over the UB while leaving the SQLITE_LOCKED read failures — observers would just silently see errors/no data mid-transaction. |

---

## 3. Chosen design: transaction-settled drain (post-statement, autocommit-gated)

### 3.1 Mechanism

One sentence: **in-memory (and Emscripten) lattices stop delivering from inside the update hook; the hook only buffers and sets a per-connection dirty flag, and the `db.cpp` statement wrappers drain the buffer via `flush_changes()` immediately after any successful statement that leaves the connection in autocommit mode — i.e. at the close of every top-level transaction, on the writing thread, outside all SQLite frames.**

```
writer thread                                   SQLite frames
─────────────                                   ─────────────
lattice_db::write {                             
  begin_transaction()          BEGIN IMMEDIATE
  add(a) ── db_->insert ─────► step ─ update_hook: buffer(a), buffer(audit_a), dirty=1
  add(b) ── db_->insert ─────► step ─ update_hook: buffer(b), buffer(audit_b), dirty=1
  commit() ── execute("COMMIT") ► exec ─ (commit; locks released)
      └─ post-statement: dirty && autocommit ⇒ drain
            └─ flush_changes(): one batch [a, audit_a, b, audit_b]
                 └─ for_each_alive(shared-storage handles) → notify_changes_batched
                      └─ scheduler_->invoke (immediate ⇒ callbacks run HERE, txn closed)
}
rollback path: execute("ROLLBACK") ► sqlite3_rollback_hook clears buffer + dirty ⇒ drain no-ops
```

File DBs are untouched: their dirty flag is never set, the WAL hook remains their flush point. The only file-DB change is the rollback-hook buffer discard (a strict bug fix, 2.4).

### 3.2 Function-level change list

**`Sources/LatticeCore/include/lattice/db.hpp` / `src/db.cpp` — class `database`:**

1. **New members:** `std::atomic<bool> txn_dirty_{false}`; `std::function<void()> on_txn_settled_`; `std::function<void()> on_txn_rolled_back_`.
2. **New API:** `void set_txn_hooks(std::function<void()> settled, std::function<void()> rolled_back)` — stores both; installs `sqlite3_rollback_hook(db_, trampoline, this)` where the trampoline calls `on_txn_rolled_back_` and clears `txn_dirty_` (C-safe: no SQLite calls, no throw). `void mark_txn_dirty()` — relaxed store, callable from the update hook.
3. **New private helper:** `void drain_if_settled()` —
   `if (on_txn_settled_ && txn_dirty_.load(relaxed) && sqlite3_get_autocommit(db_) != 0) { txn_dirty_.store(false, relaxed); on_txn_settled_(); }`
4. **Call `drain_if_settled()`** on the success tail of `execute()` (both branches), `insert()`, `update()`, `remove()`, `query()`. On error paths where `sqlite3_get_autocommit() != 0` (implicit-transaction rollback already happened), clear `txn_dirty_` and invoke `on_txn_rolled_back_` defensively in case SQLite's rollback hook did not fire for the failed statement.
5. **`~database` (`db.cpp:128–130`):** also null the rollback hook alongside the existing update/wal/commit hook clearing.

**`Sources/LatticeCore/src/lattice.cpp` — `lattice_db::setup_change_hook()`:**

6. **AuditLog branch (`:96–155`), in-memory/`__EMSCRIPTEN__` arm:** delete the `SELECT globalId` + `notify_changes_batched` + `cache=shared` fan-out block (`:108–136`). Replace with: `if (operation == SQLITE_INSERT) { self->append_to_change_buffer("AuditLog", "INSERT", rowid, ""); self->db_->mark_txn_dirty(); }`. (GlobalId resolution moves to flush time — the row is committed and readable there; this also removes one same-connection SELECT from inside the hook, narrowing the existing documented-restriction bend rather than widening it. The file-DB arm at `:137–153` is unchanged.)
7. **Model-table tail (`:185–198`):** replace both immediate `self->flush_changes()` calls (Emscripten and `is_in_memory()`) with `self->db_->mark_txn_dirty()`. The pre-existing `SELECT globalId FROM <table>` at `:170` stays (all storage kinds; changing the file-DB path is out of scope — flagged in §5).
8. **After the WAL-hook registration (`:203–211`):** add
   `db_->set_txn_hooks([this]{ flush_changes(); }, [this]{ discard_change_buffer(); });`
   The settled hook is harmless for file DBs (dirty flag never set); the rollback hook applies to all storage kinds (bug fix, 2.4).

**`Sources/LatticeCore/include/lattice/lattice.hpp` — `lattice_db`:**

9. **New:** `bool storage_shared_across_instances() const { return !config_.is_in_memory() || config_.path.find("cache=shared") != std::string::npos; }` (public — sync.cpp needs it). Replace the `is_file_db` local in `flush_changes` (`:1416–1417`) with this helper.
10. **New:** `void discard_change_buffer()` — lock `change_buffer_mutex_`, clear `change_buffer_`. (Wholesale clear is correct: the buffer only ever holds the currently-open transaction's rows — every commit flushes it via WAL hook or drain.)
11. **`flush_changes()` (`:1390–1663`):**
    - New early step for in-memory: backfill `global_id` for buffered `("AuditLog","INSERT", id, "")` entries with one `SELECT id, globalId FROM AuditLog WHERE id IN (…)`.
    - Keep the `is_in_memory` skips in Pass 2 (`:1560`) and Pass 3 (`:1607`) — AuditLog events now arrive via the buffer, so deriving them again would double-deliver.
    - Tail: after `is_flushing_ = false`, if `change_buffer_` is non-empty (observer callbacks wrote during delivery), loop back to the top; cap iterations (e.g. 64) with `LOG_WARN` on overflow. Fixes the pre-existing stranded-nested-write bug (2.4).

**`Sources/LatticeCore/src/sync.cpp` — DEFECT 2 (see 3.3):**

12. **`notify_observers` (`:2413–2431`):** replace the `is_in_memory()` branch with the shared-storage predicate, and defer when a transaction is open:

```cpp
static void notify_observers(lattice_db& db,
                             const std::vector<std::pair<int64_t, std::string>>& notify_list) {
    if (notify_list.empty()) return;
    if (db.db().is_in_transaction()) {
        // Caller holds the txn (nested mark-synced): ride the drain at its commit.
        for (const auto& [row_id, gid] : notify_list)
            db.append_to_change_buffer("AuditLog", "UPDATE", row_id, gid);
        db.db().mark_txn_dirty();
        return;
    }
    std::vector<lattice_db::change_event> events;   // as today
    ...
    if (db.storage_shared_across_instances()) {
        instance_registry::instance().for_each_alive(db.config().path,
            [&events](lattice_db* inst) { inst->notify_changes_batched(events); });
    } else {
        db.notify_changes_batched(events);          // isolated :memory: — registry keys
    }                                               // collide but storage doesn't; fan-out
}                                                   // would deliver phantoms
```

No other sync change is needed: both `mark_audit_entries_synced` (`:2481`) and `mark_audit_entries_synced_for` (`:2567`) already call `notify_observers` after their own commits, and all external callers (Swift bridge `receive_sync_data`, C API `lattice.cpp:1015/:1179`) route through those two.

### 3.3 Why the DEFECT 2 fix is exactly this

- The predicate must be the *same* one `flush_changes` uses — shared-cache memory DBs share storage and must share notifications; plain `:memory:` instances share a registry key (`":memory:"`) but have **isolated** storage, so fanning out to them would notify observers about rows that don't exist in their DB. `storage_shared_across_instances()` centralizes that policy (today it lives in two places, `lattice.hpp:1416` and implicitly `lattice.cpp:128`).
- The in-transaction deferral closes the defensive path where `mark_audit_entries_synced` is entered with a caller-held transaction (`sync.cpp:2437–2443`): without it, adding fan-out would create a brand-new DEFECT-1-shaped in-txn cross-connection delivery. Buffered `("AuditLog","UPDATE", …)` tuples flow through `flush_changes` Pass 1 verbatim (the `table != "AuditLog"` guard at `lattice.hpp:1529` skips the changed-fields lookup; Pass 2's `AuditLog` skip at `:1558` prevents double-derivation) and are delivered post-commit with correct fan-out — on file DBs by the WAL hook, on memory DBs by the new drain.
- Update-hook interaction: `UPDATE AuditLog SET isSynchronized=1` fires the update hook with `op=UPDATE`, which the AuditLog branch ignores (only INSERT is handled) — `notify_observers` remains the sole source of these events. No double delivery.

### 3.4 Contract and behavior analysis

- **Exactly-once:** unchanged mechanics (single buffer move under mutex per drain; `for_each_alive` guard refcounts; cursor advance logic untouched — memory DBs have no cross-process notifier, `lattice.cpp:41`). `NamedMemory.CrossInstanceObservationExactlyOnce` continues to pass by construction: one autocommit write ⇒ one drain ⇒ one batch per instance.
- **Commit order:** buffer order is update-hook order is commit order; one batch per transaction; `notify_changes_batched` preserves input order within each table slice and for per-object observers. AuditLog ids remain the total-order tiebreaker per the Swift contract.
- **Promptness:** with `immediate_scheduler`, an autocommit `add()` still delivers before `add()` returns (drain runs inside `database::insert`'s tail); an explicit transaction delivers inside `commit()` before `write{}` returns. No test that "writes then immediately asserts the callback fired" changes behavior for single-statement writes.
- **Visible change #1 — per-transaction batching:** in-memory multi-statement transactions now produce **one** callback with N events instead of N callbacks. This matches file DBs, matches `add_table_observer`'s documented contract, and matches the Swift `observe()` wrapper (which fans batches out per-row, so Swift-visible per-row semantics are unchanged, `Lattice.swift:1512–1533`). C++ tests that counted per-statement fires inside transactions must be updated (audit: `ObservationTests` use autocommit `add()` — unaffected; `CascadeDeleteTests` gain the single-fire behavior the cascade-transaction wrap was introduced to produce, `lattice.hpp:1935–1955`).
- **Visible change #2 — no mid-transaction observation:** observers no longer see uncommitted state. Anything that depended on being called mid-transaction on a memory DB was depending on the defect.
- **Visible change #3 — rollback silence:** rolled-back transactions deliver nothing (previously: in-memory delivered mid-txn events for changes that then vanished; file DBs delivered phantoms on the next commit).
- **Exceptions:** an observer exception now propagates through plain C++ frames to the writer's `add()`/`commit()` call — after the commit has durably happened (parity with the semantic that the write succeeded). It no longer crosses SQLite C frames. (File DBs retain the WAL-hook C-frame exposure — pre-existing, see §5.)
- **`applying_remote_changes_` / TLS flags:** the sync apply path commits at `sync.cpp:2948` while `applying_remote_changes_` is still true (cleared at `:2956`), so the drain-at-commit reads the flag correctly; `tls_notify_local_object_observers_` is set on the writing thread before `db_->insert`/bulk commit and consumed by the drain on the same thread — contract intact (`lattice.hpp:1052–1058`, `:1250–1258`).
- **Emscripten:** included in the same branch (both immediate-flush sites replaced). Single-threaded, so no lock hazard existed, but WASM gains per-transaction batching and loses in-C-frame callbacks. Flagged for WASM CI verification.
- **Overhead:** one relaxed atomic load per statement in the wrappers (which already do a relaxed `fetch_add` for `g_statement_count`), plus one atomic store per row change in the hook. No new locks, no new threads, no new connections.

### 3.5 Risks

| Risk | Mitigation |
|---|---|
| A transaction-closing statement bypasses the `db.cpp` wrappers | Audit in 2.3 found none; the drain-until-empty loop plus "next write drains leftovers" gives self-healing; a debug assertion (`buffer non-empty && autocommit && !is_flushing_` at `begin_transaction`) can pin it in tests. |
| Test expectations of per-statement fires on memory DBs | Enumerated in 3.4; fix tests, not behavior — the documented contract is per-transaction. |
| Rollback hook interplay with statement-level aborts | Statement aborts inside a surviving transaction don't fire the rollback hook (correct — the txn may still commit; hook-buffered rows from the aborted statement are a pre-existing, unchanged edge shared with file DBs). Implicit-txn failures are covered by the defensive clear in change #4 of 3.2. |
| Observer writes causing unbounded drain loops | Iteration cap + warning; behavior today is worse (stranded events). |

---

## 4. Test plan (pinning tests)

New file `Tests/LatticeCoreTests/DeferredDeliveryTests.cpp` (plus additions to `NamedMemoryTests.cpp`), all on `file:<name>?mode=memory&cache=shared` URIs with default (immediate) scheduler unless noted:

1. **`DeferredDelivery.ObserverReadsThroughSecondHandleDuringMultiWriteTxn`** *(the required repro — fails with SQLITE_LOCKED/db_error or UB today, passes with the fix)*: handles `a` (writer) and `b` (observer, same name). `b`'s table observer callback executes `b.db().query("SELECT COUNT(*) FROM TestPerson")` and a `b.find_by_global_id` on every fire, recording any thrown exception; `a.write([&]{ a.add(p1); a.add(p2); a.add(p3); })`. Assert: no exception, no deadlock (bounded watchdog), callback fired **once** with 3 model events, callback observed `a.db().is_in_transaction() == false`, and the COUNT read returned 3 (post-commit state).
2. **`DeferredDelivery.PerTransactionBatchingAndOrder`**: observer on the writing handle; one explicit transaction inserting rows A,B,C ⇒ exactly one fire, events in [A,B,C] order; AuditLog observer sees exactly 3 INSERT events in id order in one fire. Then one autocommit `add()` ⇒ exactly one more fire with one event.
3. **`DeferredDelivery.AutocommitDeliversBeforeAddReturns`**: immediate scheduler; flag set in callback; assert flag is already true on the line after `add()` (pins promptness — guards against any future scheduler-hop regression).
4. **`DeferredDelivery.RollbackDeliversNothing`**: begin, add two rows, rollback ⇒ zero fires (grace wait); subsequent committed add delivers exactly its own event (no phantoms). Run the same body against a `TempDB` file lattice — pins the rollback-hook fix for file DBs too.
5. **`DeferredDelivery.ObserverWritebackIsDeliveredNotStranded`**: observer callback performs one `add()` on first fire ⇒ assert a second fire delivers it (drain-until-empty), no deadlock, no stack overflow.
6. **`DeferredDelivery.ExceptionFromObserverSurfacesToWriter`**: observer throws `std::runtime_error`; assert it arrives at the `add()` call site as a C++ exception (catchable), the row is committed, and a subsequent write still delivers (buffer/flags not wedged).
7. **`NamedMemory.MarkSyncedFansOutToAllHandles`** *(DEFECT 2 — fails today)*: handles `a`,`b`; write one row via `a`; register AuditLog observers on both counting UPDATE events; `lattice::mark_audit_entries_synced(a, {gid})` ⇒ both observers receive exactly one AuditLog UPDATE for `gid` (exactly-once each, no double fire on `a`).
8. **`NamedMemory.MarkSyncedIsolatedMemoryDoesNotCrossNotify`**: two plain `:memory:` lattices (colliding registry key, isolated storage); mark-synced on `a` ⇒ `b` receives nothing (guards the predicate against naive registry fan-out).
9. **`NamedMemory.MarkSyncedInsideCallerTransactionDefersToCommit`**: open a transaction on `a`, call `mark_audit_entries_synced` (nested path), assert no fire while the txn is open; commit ⇒ both handles receive the UPDATE event.
10. **Regression sweeps:** existing `ObservationTests`, `NamedMemoryTests`, `CascadeDeleteTests`, `SyncUnitTests`/`SyncIntegrationTests`, `MultiConnectionTests`, `LatticeSwiftCppTests`; downstream lattice repo `ObservationOrderingTests` (pins the Swift 1.0 delivery contract) and the WASM CI lane for the Emscripten batching change.

---

## 5. Out of scope / future work

- **File-DB WAL-hook callbacks still run inside `sqlite3_step`'s C frame** (post-commit, locks released — safe for SQLITE_LOCKED, but observer exceptions still cross C frames). The same `set_txn_hooks` drain could replace the WAL hook for file DBs later; deliberately not bundled to keep this change memory-DB-scoped.
- **Same-connection `SELECT globalId` inside the update hook for model tables** (`lattice.cpp:170`) bends the documented hook restriction for all storage kinds; deferring it (buffer rowid, resolve at flush — DELETE rows excepted, as today) is a follow-up.
- **Statement-level aborts** after the hook fired (row change undone by `ABORT` conflict handling inside a surviving transaction) can still buffer a row that never commits — pre-existing for file DBs, unchanged here, detectable later via Pass-2 audit-row absence.
- **Cross-process semantics are untouched** — memory DBs are process-local by definition (`lattice.cpp:37–41`).
