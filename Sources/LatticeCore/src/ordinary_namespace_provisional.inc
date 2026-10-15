// Included only inside the POSIX namespace implementation. This is the
// separately versioned absent-root protocol; existing journals stay unchanged.
    bool provisional_root = false, empty_provisional = false, old_winner = false;
    file_identity provisional_id{}, candidate_id{}, empty_slot_id{};
    fd empty_slot;
    std::vector<std::uint8_t> provisional_intent, primary_intent, finalization_intent;
    mode_t provisional_observed_mode = 0, winner_mode = 0;

    std::vector<std::uint8_t> read_phase(const std::string& name, deadline end) const {
        fd input(::openat(control.n, name.c_str(), O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC));
        if (identity(held(input.n)) != identity(named(control.n, name))) fail(code::changed);
        auto bytes = checked_read(input.n, end);
        sync_fd(input.n, end); sync_dir(control.n, name, end); return bytes;
    }
    std::vector<std::string> read_directory_names(int descriptor, deadline end) const {
        // Reading directory entries needs read permission, not traversal.
        // Do not open "." through the mode-0400 directory or grant search just
        // to prove a private empty object. The retained controller is serial.
        auto scan = duplicate(descriptor);
        if (::lseek(scan.n, 0, SEEK_SET) < 0) fail(code::unavailable);
        DIR* raw = ::fdopendir(scan.n); if (!raw) fail(code::unavailable); scan.n = -1;
        std::unique_ptr<DIR, decltype(&::closedir)> holder(raw, &::closedir);
        std::vector<std::string> result;
        for (;;) {
            bound(end); errno = 0; const auto* entry = ::readdir(raw);
            if (!entry) { if (errno) fail(code::unavailable); break; }
            const std::string name(entry->d_name);
            if (name == "." || name == "..") continue;
            if (name.size() > 255 || name.find('/') != std::string::npos || result.size() == 4096) fail(code::invalid);
            result.push_back(name);
        }
        std::sort(result.begin(), result.end()); return result;
    }
    std::vector<std::uint8_t> encode_provisional() const {
        std::vector<std::uint8_t> b{'L','A','T','N','P','R','1',0};
        for (const auto value : {data_parent_id, data_id, control_parent_id, control_id,
             generation_id, lock_id, candidate_id}) append(b, value);
        append(b, data_leaf); append(b, control_leaf); append(b, data_original_mode); append(b, 0400);
        return b;
    }
    void decode_provisional(const std::vector<std::uint8_t>& b) {
        const std::vector<std::uint8_t> magic{'L','A','T','N','P','R','1',0};
        if (b.size() < 8 || !std::equal(magic.begin(), magic.end(), b.begin())) fail(code::invalid);
        reader in{b}; in.at = 8;
        for (const auto value : {data_parent_id, data_id, control_parent_id, control_id, generation_id, lock_id})
            if (in.id() != value) fail(code::changed);
        provisional_id = candidate_id = in.id();
        if (!provisional_id.inode || provisional_id.device != data_id.device || provisional_id == data_id ||
            provisional_id == control_id || provisional_id == generation_id || provisional_id == lock_id ||
            in.text() != data_leaf || in.text() != control_leaf) fail(code::changed);
        const auto mode = in.u64();
        if (mode > 0777 || (mode & 0700) != 0700 || (mode & 0022) || in.u64() != 0400) fail(code::invalid);
        data_original_mode = mode_t(mode);
        if (in.at != b.size() || encode_provisional() != b) fail(code::invalid);
        provisional_root = true; use_mutation_barrier = true; provisional_intent = b;
    }
    void primary_census(deadline end) {
        items.clear(); sync_members.clear();
        const auto top = list(data.n, end); std::set<std::string> accepted;
        retain_item("memory.sqlite", false); accepted.insert("memory.sqlite");
        for (const std::string suffix : {"-wal", "-shm", "-journal"}) {
            const auto leaf = "memory.sqlite" + suffix;
            if (std::binary_search(top.begin(), top.end(), leaf)) { retain_item(leaf, false); accepted.insert(leaf); }
        }
        for (const auto& leaf : top) if (store_shaped(leaf) && !accepted.contains(leaf)) fail(code::invalid);
        for (const auto& value : items) require_no_granting_acl(value.retained.n);
    }
    std::vector<std::uint8_t> encode_primary() const {
        auto b = provisional_intent; append(b, std::string("primary-members")); append(b, items.size());
        for (const auto& value : items) { append(b, value.leaf); append(b, value.original); append(b, value.mode); }
        return b;
    }
    void decode_primary(const std::vector<std::uint8_t>& b) {
        if (b.size() < provisional_intent.size() || !std::equal(provisional_intent.begin(), provisional_intent.end(), b.begin())) fail(code::changed);
        reader in{b}; in.at = provisional_intent.size();
        if (in.text() != "primary-members") fail(code::invalid);
        const auto count = in.u64(); if (!count || count > 4) fail(code::invalid);
        std::set<std::string> names; std::set<std::pair<std::uint64_t,std::uint64_t>> identities;
        items.clear();
        for (std::uint64_t n = 0; n != count; ++n) {
            item value; value.leaf = in.text(); value.original = in.id(); const auto mode = in.u64();
            if ((n == 0 && value.leaf != "memory.sqlite") || !names.insert(value.leaf).second ||
                !(value.leaf == "memory.sqlite" || value.leaf == "memory.sqlite-wal" || value.leaf == "memory.sqlite-shm" || value.leaf == "memory.sqlite-journal") ||
                !value.original.inode || value.original.device != data_id.device || value.original == provisional_id ||
                !identities.emplace(value.original.device,value.original.inode).second || mode > 0777 ||
                (mode & 07700) != 0600 || (mode & 0022)) fail(code::invalid);
            value.mode = mode_t(mode); items.push_back(std::move(value));
        }
        if (in.at != b.size() || encode_primary() != b) fail(code::invalid);
        primary_intent = b;
    }
    void check_provisional_identity(deadline end) {
        owner(end); const auto actual = named(data.n, "sync"); check_kind(actual, true);
        if (identity(actual) != provisional_id) fail(code::changed);
        const auto mode = actual.st_mode & 07777;
        if(old_winner&&mode==0400&&!has_record("absent.final-census"))fail(code::changed);
        if (mode != 0400 && mode != 0700 && !(old_winner && mode == winner_mode) &&
            !(has_record("absent.finalize") && mode == 0500)) fail(code::changed);
        if (barrier_sync.n < 0) barrier_sync = fd(::openat(data.n, "sync", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
        if (identity(held(barrier_sync.n)) != provisional_id) fail(code::changed);
        require_no_granting_acl(barrier_sync.n);
    }
    void load_winner(deadline end) {
        if(!has_record("absent.winner"))return;
        const auto base=encode_provisional();const auto bytes=read_phase("absent.winner",end);
        if(bytes.size()!=base.size()+24||!std::equal(base.begin(),base.end(),bytes.begin()))fail(code::changed);
        reader in{bytes};in.at=base.size();const auto winner=in.id();const auto mode=in.u64();
        if(!winner.inode||winner.device!=data_id.device||winner==candidate_id||winner==data_id||
            winner==control_id||winner==generation_id||winner==lock_id||mode>0777||(mode&0700)!=0700||(mode&0022))fail(code::changed);
        fd unused(::openat(control.n,"provisional.sync",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
        if(identity(held(unused.n))!=candidate_id||(held(unused.n).st_mode&07777)!=0400||
            !read_directory_names(unused.n,end).empty())fail(code::changed);
        old_winner=true;winner_mode=mode_t(mode);provisional_id=winner;provisional_intent=bytes;
    }
    void publish_provisional(deadline end, bool publication_raced = false) {
        owner(end);load_winner(end);
        if(old_winner){
            check_provisional_identity(end);sync_fd(barrier_sync.n,end);
            sync_dir(data.n,"winner:data",end);sync_dir(control.n,"winner:control",end);
            event(control.n,"absent.published",provisional_intent,end);return;
        }
        struct stat current{}, private_name{};
        const bool private_present = ::fstatat(control.n,"provisional.sync",&private_name,AT_SYMLINK_NOFOLLOW) == 0;
        if (!private_present && errno != ENOENT) fail(code::changed);
        const bool present = ::fstatat(data.n,"sync",&current,AT_SYMLINK_NOFOLLOW) == 0;
        if (!present && errno != ENOENT) fail(code::changed);
        if (present && identity(current) != provisional_id) {
            // A second old invocation may still hold its stale !exists
            // decision. EEXIST therefore requires the same first role phase.
            if (!private_present || identity(private_name) != provisional_id || has_record("absent.published")) fail(code::changed);
            check_kind(current,true);
            if((current.st_mode&0700)!=0700)fail(code::changed);
            // The old creator may not have flushed its directory name. Do not
            // publish a durable winner identity before settling that actual
            // inode/name; otherwise a controller crash could bind a vanished
            // pre-fsync winner forever. An old 0700 repair remains permitted.
            fd winner(::openat(data.n,"sync",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
            if(identity(held(winner.n))!=identity(current))fail(code::changed);
            require_no_granting_acl(winner.n);sync_fd(winner.n,end);sync_dir(data.n,"winner-before-binding",end);
            const auto after=named(data.n,"sync");check_kind(after,true);
            if(identity(after)!=identity(current)||((after.st_mode&07777)!=(current.st_mode&07777)&&
                (after.st_mode&07777)!=0700))fail(code::changed);
            auto b = provisional_intent; append(b,identity(current));append(b,current.st_mode&07777);
            event(control.n,"absent.winner",std::move(b),end);
            load_winner(end);check_provisional_identity(end);sync_fd(barrier_sync.n,end);
            sync_dir(data.n,"winner:data",end);sync_dir(control.n,"winner:control",end);
            event(control.n,"absent.published",provisional_intent,end);return;
        }
        if (!present) {
            if (!private_present || identity(private_name) != provisional_id || has_record("absent.published")) fail(code::changed);
            fd candidate(::openat(control.n,"provisional.sync",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
            if (identity(held(candidate.n)) != provisional_id || (held(candidate.n).st_mode & 07777) != 0400 ||
                !read_directory_names(candidate.n,end).empty()) fail(code::changed);
            sync_fd(candidate.n,end); sync_dir(control.n,"provisional.sync",end);
            hook(namespace_retirement_test_hooks::boundary::before_provisional_publish,"sync");
#if defined(__APPLE__)
            const int result = ::renameatx_np(control.n,"provisional.sync",data.n,"sync",RENAME_EXCL);
#elif defined(__linux__) && defined(SYS_renameat2)
            const long result = ::syscall(SYS_renameat2,control.n,"provisional.sync",data.n,"sync",1U);
#else
            const int result = -1; errno = ENOSYS;
#endif
            if (result != 0) {
                if (errno == EEXIST && !publication_raced) { publish_provisional(end,true); return; }
                fail(code::durability_unproved);
            }
            hook(namespace_retirement_test_hooks::boundary::after_provisional_publish,"sync");
        } else if (private_present) fail(code::changed);
        check_provisional_identity(end);
        sync_fd(barrier_sync.n,end); sync_dir(data.n,"provisional:data",end); sync_dir(control.n,"provisional:control",end);
        event(control.n,"absent.published",provisional_intent,end);
    }
    void prepare_provisional(deadline end) {
        owner(end);
        if (!has_record("absent.start")) {
            // An unpublished private prefix has not authorized any source
            // mutation. Only this exact empty candidate can be replaced.
            verify_unpublished_provisional_prefix(end);
            struct stat stale{};
            if (::fstatat(control.n,"provisional.sync",&stale,AT_SYMLINK_NOFOLLOW) == 0) {
                check_kind(stale,true);
                if ((stale.st_mode & 07777) != 0400 && (stale.st_mode & 07777) != 0700) fail(code::changed);
                fd old(::openat(control.n,"provisional.sync",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
                if (identity(held(old.n)) != identity(stale) || !read_directory_names(old.n,end).empty()) fail(code::changed);
                if (::unlinkat(control.n,"provisional.sync",AT_REMOVEDIR)) fail(code::durability_unproved);
                sync_dir(control.n,"provisional-prefix",end);
            } else if (errno != ENOENT) fail(code::changed);
            if (::mkdirat(control.n,"provisional.sync",0700)) fail(code::durability_unproved);
            fd candidate(::openat(control.n,"provisional.sync",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
            check_kind(held(candidate.n),true); require_no_granting_acl(candidate.n);
            provisional_id = candidate_id = identity(held(candidate.n)); data_original_mode = held(data.n).st_mode & 07777;
            if ((data_original_mode & 0700) != 0700 || ::fchmod(candidate.n,0400)) fail(code::changed);
            sync_fd(candidate.n,end); sync_dir(control.n,"provisional-prefix",end);
            provisional_intent = encode_provisional(); event(control.n,"absent.start",provisional_intent,end);
        } else decode_provisional(read_phase("absent.start",end));
        provisional_root = true; use_mutation_barrier = true;
    }
    void verify_unpublished_provisional_prefix(deadline end) const {
        for (const auto& name : list(control.n,end)) {
            if (name == "controller.lock" || name == "generation" || name == "provisional.sync") continue;
            if (name != "pending.absent.start") fail(code::changed);
            const auto s = named(control.n,name); check_kind(s,false);
            if ((s.st_mode & 07777) != 0600 || s.st_size < 0 || s.st_size > 65536) fail(code::changed);
        }
        if (!list(generation.n,end).empty()) fail(code::changed);
    }
    void establish_primary_denial(deadline end) {
        check_provisional_identity(end);
        const auto mode = held(data.n).st_mode & 07777;
        if (mode != data_original_mode && mode != frozen_mode(data_original_mode)) fail(code::changed);
        for (const auto& node : data_anchor.nodes) require_no_granting_acl(node.descriptor.n);
        for (const auto& node : control_anchor.nodes) require_no_granting_acl(node.descriptor.n);
        require_no_granting_acl(data.n);
        if (::fchmod(data.n,frozen_mode(data_original_mode))) fail(code::durability_unproved);
        sync_dir(data.n,"provisional:parent-barrier",end); sync_dir(data_parent.n,"provisional:parent-name",end);
        event(control.n,"absent.parent",provisional_intent,end);
        if (has_record("absent.primary")) decode_primary(read_phase("absent.primary",end));
        else {
            primary_census(end); primary_intent = encode_primary(); event(control.n,"absent.primary",primary_intent,end);
        }
        std::set<std::string> expected;
        for (auto& value : items) {
            expected.insert(value.leaf); const auto actual = named(data.n,value.leaf); check_kind(actual,false);
            if (identity(actual) != value.original || ((actual.st_mode & 07777) != value.mode && (actual.st_mode & 07777) != 0400)) fail(code::changed);
            value.retained = fd(::openat(data.n,value.leaf.c_str(),O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
            if (identity(held(value.retained.n)) != value.original) fail(code::changed);
            require_no_granting_acl(value.retained.n);
            hook(namespace_retirement_test_hooks::boundary::before_denial,value.leaf);
            if (::fchmod(value.retained.n,0400)) fail(code::durability_unproved);
            hook(namespace_retirement_test_hooks::boundary::after_denial,value.leaf);
            sync_fd(value.retained.n,end); sync_dir(data.n,value.leaf,end);
        }
        for (const auto& name : list(data.n,end)) if (store_shaped(name) && !expected.contains(name)) fail(code::changed);
        event(control.n,"absent.ready",primary_intent,end); check_provisional_identity(end);
    }
    historical_namespace_binding preliminary_binding(deadline end) {
        owner(end);
        if (!provisional_root || primary_intent.empty() || read_phase("absent.ready",end) != primary_intent) fail(code::incomplete);
        if (intent.empty()) {
            check_provisional_identity(end);
            if ((held(data.n).st_mode & 07777) != frozen_mode(data_original_mode)) fail(code::changed);
            for (const auto& value : items) {
                if(value.directory)continue;
                const auto s = named(data.n,value.leaf); check_kind(s,false);
                if (identity(s) != value.original || (s.st_mode & 07777) != 0400) fail(code::changed);
            }
        } else reconcile(end);
        historical_namespace_binding result; result.data = data_id; result.control = control_id;
        picosha2::hash256(primary_intent.begin(),primary_intent.end(),result.barrier.begin(),result.barrier.end());
        // Decode the immutable preliminary primary identities; final items may
        // already include the late directory or have moved during preservation.
        reader in{primary_intent}; in.at = provisional_intent.size();
        if (in.text() != "primary-members") fail(code::changed);
        const auto count = in.u64();
        for (std::uint64_t n = 0; n != count; ++n) { (void)in.text(); result.originals.push_back(in.id()); (void)in.u64(); }
        result.originals.push_back(provisional_id); return result;
    }
    int destination(const item& value) const {
        return empty_provisional && value.directory ? empty_slot.n : generation.n;
    }
    void open_empty_slot(deadline end, bool create) {
        if (create && ::mkdirat(control.n,"empty-sync",0700)) fail(code::durability_unproved);
        empty_slot = fd(::openat(control.n,"empty-sync",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
        const auto s = held(empty_slot.n); check_kind(s,true);
        if ((s.st_mode & 07777) != 0700 || s.st_dev != held(control.n).st_dev ||
            (empty_slot_id.inode && identity(s) != empty_slot_id)) fail(code::changed);
        empty_slot_id = identity(s); require_no_granting_acl(empty_slot.n); sync_dir(empty_slot.n,"empty-sync",end);
    }
    std::vector<std::uint8_t> encode_absent_barrier() const {
        auto b = provisional_intent; append(b,std::string("final-member-barrier"));
        append(b,primary_intent.size()); b.insert(b.end(),primary_intent.begin(),primary_intent.end());
        append(b,finalization_intent.size()); b.insert(b.end(),finalization_intent.begin(),finalization_intent.end());
        append(b,empty_provisional ? 1 : 0); append(b,empty_slot_id); append(b,sync_original_mode); return b;
    }
    void finalize_absent(const std::function<void(deadline)>& verify, deadline end) {
        verify(end); owner(end);
        if (!intent.empty()) { reconcile(end); verify(end); return; }
        if (has_record("absent.final-census")) {
            load_final_census(end); publish_final_census(end); reconcile(end); verify(end); return;
        }
        check_provisional_identity(end);
        if (has_record("absent.finalize")) {
            finalization_intent = read_phase("absent.finalize",end);
            reader in{finalization_intent};
            if (finalization_intent.size() < primary_intent.size() || !std::equal(primary_intent.begin(),primary_intent.end(),finalization_intent.begin())) fail(code::changed);
            in.at = primary_intent.size(); const auto mode = in.u64();
            if ((old_winner&&mode==0400) || (mode != 0400 && mode != 0700 && !(old_winner&&mode==winner_mode)) || in.at != finalization_intent.size()) fail(code::invalid);
            provisional_observed_mode = mode_t(mode);
        } else {
            provisional_observed_mode = held(barrier_sync.n).st_mode & 07777;
            if ((old_winner&&provisional_observed_mode==0400) || (provisional_observed_mode != 0400 && provisional_observed_mode != 0700 &&
                !(old_winner&&provisional_observed_mode==winner_mode))) fail(code::changed);
            finalization_intent = primary_intent; append(finalization_intent,provisional_observed_mode);
            event(control.n,"absent.finalize",finalization_intent,end);
        }
        verify(end);
        if (::fchmod(barrier_sync.n,0500)) fail(code::durability_unproved);
        sync_fd(barrier_sync.n,end); sync_dir(data.n,"provisional:final-census",end);
        hook(namespace_retirement_test_hooks::boundary::after_provisional_freeze,"sync");
        verify(end);
        // Reuse the original complete member validation; primary mode/identity
        // authority comes from its pre-denial durable record, not a new census.
        decode_primary(primary_intent);
        item directory; directory.leaf="sync"; directory.directory=true; directory.original=provisional_id;
        directory.retained=duplicate(barrier_sync.n); directory.mode=provisional_observed_mode;
        items.push_back(std::move(directory));
        census_sync_members(end);
        for(const auto& member:sync_members)require_no_granting_acl(member.retained.n);
        empty_provisional=!old_winner&&sync_members.empty();
        if (!empty_provisional && (provisional_observed_mode & 0700) != 0700) fail(code::changed);
        sync_original_mode=empty_provisional ? 0400 : provisional_observed_mode;
        items.back().mode=sync_original_mode; barrier_sync_id=provisional_id;
        verify(end);
        // Refusal objects remain private until an immutable final census
        // binds their actual identities. A torn private prefix grants nothing.
        if (empty_provisional) {
            struct stat existing{};
            if (::fstatat(control.n,"empty-sync",&existing,AT_SYMLINK_NOFOLLOW)==0) open_empty_slot(end,false);
            else { if(errno!=ENOENT)fail(code::changed); open_empty_slot(end,true); }
        }
        discard_uncommitted_refusals(end); stage_refusals(end);
        barrier_intent=encode_absent_barrier(); intent=encode();
        std::vector<std::uint8_t> record;
        append(record,barrier_intent.size()); record.insert(record.end(),barrier_intent.begin(),barrier_intent.end());
        append(record,intent.size()); record.insert(record.end(),intent.begin(),intent.end());
        event(control.n,"absent.final-census",std::move(record),end);
        publish_final_census(end);
        reconcile(end); verify(end);
    }
    void publish_final_census(deadline end) {
        event(control.n,"barrier.intent",barrier_intent,end);
        write_barrier_phase("barrier.complete",end);
        event(control.n,"intent.v1",intent,end);
    }
    void load_final_census(deadline end) {
        finalization_intent=read_phase("absent.finalize",end);
        if(finalization_intent.size()!=primary_intent.size()+8 ||
            !std::equal(primary_intent.begin(),primary_intent.end(),finalization_intent.begin()))fail(code::changed);
        reader final_mode{finalization_intent};final_mode.at=primary_intent.size();
        const auto mode=final_mode.u64();if((old_winner&&mode==0400)||(mode!=0400&&mode!=0700&&!(old_winner&&mode==winner_mode)))fail(code::changed);
        provisional_observed_mode=mode_t(mode);
        const auto bytes=read_phase("absent.final-census",end);reader in{bytes};
        const auto barrier_size=in.u64();if(barrier_size>bytes.size()-in.at)fail(code::invalid);
        const std::vector<std::uint8_t> barrier(bytes.begin()+in.at,bytes.begin()+in.at+barrier_size);in.at+=barrier_size;
        const auto intent_size=in.u64();if(intent_size!=bytes.size()-in.at)fail(code::invalid);
        const std::vector<std::uint8_t> final_intent(bytes.begin()+in.at,bytes.end());
        decode_absent_barrier(barrier,end);items.clear();sync_members.clear();decode(final_intent);intent=final_intent;
        if(items.empty()||items.back().original!=provisional_id||items.back().mode!=sync_original_mode||
            (empty_provisional&&!sync_members.empty()))fail(code::changed);
    }
    void decode_absent_barrier(const std::vector<std::uint8_t>& bytes, deadline end) {
        // Parse a fixed-length prefix using the already revalidated immutable
        // preliminary and finalization records. Only outcome/slot/mode follow.
        auto prefix=provisional_intent; append(prefix,std::string("final-member-barrier"));
        append(prefix,primary_intent.size()); prefix.insert(prefix.end(),primary_intent.begin(),primary_intent.end());
        append(prefix,finalization_intent.size()); prefix.insert(prefix.end(),finalization_intent.begin(),finalization_intent.end());
        if(bytes.size()<prefix.size()||!std::equal(prefix.begin(),prefix.end(),bytes.begin()))fail(code::changed);
        reader in{bytes};in.at=prefix.size();const auto empty=in.u64();empty_slot_id=in.id();const auto mode=in.u64();
        if(empty>1||(empty?(old_winner||mode!=0400):(mode!=provisional_observed_mode||(mode&0700)!=0700))||in.at!=bytes.size()||
            (empty?(!empty_slot_id.inode||empty_slot_id.device!=data_id.device):empty_slot_id.inode!=0))fail(code::invalid);
        empty_provisional=empty!=0;sync_original_mode=mode_t(mode);barrier_sync_id=provisional_id;
        if(empty_provisional)open_empty_slot(end,false);
        if(encode_absent_barrier()!=bytes)fail(code::changed);barrier_intent=bytes;
    }
