#include "ordinary_installation_seed.hpp"
#include "ordinary_normal_startup.hpp"
#include <cstdlib>
#include "vendor/picosha2/picosha2.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <exception>
#include <limits>
#include <thread>
#include <utility>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <fcntl.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#define LATTICE_INSTALLATION_SEED_POSIX 1
#endif
namespace lattice::detail::ordinary_installation {
namespace {
[[noreturn]] void seed_fail(seed_error_code code) { throw seed_error(code); }
#ifdef LATTICE_INSTALLATION_SEED_POSIX
struct seed_fd {
    int value = -1;
    explicit seed_fd(int fd) : value(fd) {}
    ~seed_fd() { if (value >= 0) ::close(value); }
    seed_fd(const seed_fd&) = delete;
};
void seed_bound(ordinary_launch::deadline end) {
    if (std::chrono::steady_clock::now() >= end) seed_fail(seed_error_code::expired);
}
file_identity directory_fact(int fd) {
    struct stat value{};
    if (::fstat(fd, &value) || !S_ISDIR(value.st_mode) || value.st_uid != ::geteuid() ||
        (value.st_mode & 07777) != 0700) seed_fail(seed_error_code::invalid_offer);
    if constexpr (sizeof(value.st_dev) > sizeof(std::uint64_t) || sizeof(value.st_ino) > sizeof(std::uint64_t))
        seed_fail(seed_error_code::unavailable);
    return {static_cast<std::uint64_t>(value.st_dev), static_cast<std::uint64_t>(value.st_ino)};
}
std::string actual_directory_path(int fd, const file_identity& expected) {
    if (directory_fact(fd) != expected) seed_fail(seed_error_code::invalid_offer);
    std::array<char, 4097> bytes{};
#if defined(__APPLE__)
    if (::fcntl(fd, F_GETPATH, bytes.data())) seed_fail(seed_error_code::unavailable);
    const auto end = std::find(bytes.begin(), bytes.end(), '\0');
    if (end == bytes.end()) seed_fail(seed_error_code::unavailable);
    std::string path(bytes.begin(), end);
#else
    const auto link = "/proc/self/fd/" + std::to_string(fd);
    const auto count = ::readlink(link.c_str(), bytes.data(), bytes.size());
    if (count <= 0 || static_cast<std::size_t>(count) >= bytes.size()) seed_fail(seed_error_code::unavailable);
    std::string path(bytes.data(), static_cast<std::size_t>(count));
#endif
    if (path.empty() || path.front() != '/' || path.size() > 4096 || path.ends_with(" (deleted)"))
        seed_fail(seed_error_code::invalid_offer);
    seed_fd named(::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
    if (named.value < 0 || directory_fact(named.value) != expected) seed_fail(seed_error_code::invalid_offer);
    return path;
}
void append64(ordinary_launch::frame& bytes, std::uint64_t value) {
    for (unsigned shift = 0; shift < 64; shift += 8) bytes.push_back(static_cast<std::uint8_t>(value >> shift));
}
std::uint64_t read64(const ordinary_launch::frame& bytes, std::size_t& offset) {
    if (offset > bytes.size() || bytes.size() - offset < 8) seed_fail(seed_error_code::invalid_offer);
    std::uint64_t value = 0;
    for (unsigned shift = 0; shift < 64; shift += 8) value |= std::uint64_t(bytes[offset++]) << shift;
    return value;
}
void append_process(ordinary_launch::frame& bytes, const ordinary_launch::process_identity& value) {
    append64(bytes, static_cast<std::uint64_t>(value.pid)); append64(bytes, static_cast<std::uint64_t>(value.parent));
    append64(bytes, value.birth_major); append64(bytes, value.birth_minor);
}
ordinary_launch::process_identity read_process(const ordinary_launch::frame& bytes, std::size_t& offset) {
    const auto pid = read64(bytes, offset), parent = read64(bytes, offset);
    const auto major = read64(bytes, offset), minor = read64(bytes, offset);
    if (!pid || !parent || pid > static_cast<std::uint64_t>(std::numeric_limits<pid_t>::max()) ||
        parent > static_cast<std::uint64_t>(std::numeric_limits<pid_t>::max()) || !major) seed_fail(seed_error_code::invalid_offer);
    return {static_cast<std::int64_t>(pid), static_cast<std::int64_t>(parent), major, minor};
}
// Entire message is fixed-size: magic/version, phase, nonce, exact parent/child
// incarnations, data-directory identity and fixed product. It names no arbitrary
// path, schema, flags, environment or callback. Each subsequent frame differs
// only in its fixed phase byte and must match the complete original bytes.
constexpr std::size_t seed_offer_size = 8 + 8 + 16 + 32 + 32 + 16 + 8;
ordinary_launch::frame make_offer(const ordinary_launch::process_identity& child, const file_identity& directory) {
    ordinary_launch::frame result{'L','A','T','S','E','E','1',0};
    append64(result, 1);
    std::array<std::uint8_t, 16> nonce{};
    if (::getentropy(nonce.data(), nonce.size()) ||
        std::all_of(nonce.begin(), nonce.end(), [](auto byte) { return byte == 0; })) seed_fail(seed_error_code::unavailable);
    result.insert(result.end(), nonce.begin(), nonce.end());
    append_process(result, ordinary_launch::current_process()); append_process(result, child);
    append64(result, directory.device); append64(result, directory.inode); append64(result, static_cast<std::uint64_t>(product::engram));
    return result;
}
ordinary_launch::frame phase(ordinary_launch::frame value, std::uint8_t next) {
    if (value.size() != seed_offer_size) seed_fail(seed_error_code::invalid_offer);
    value[8] = next; return value;
}
file_identity inspect_offer(const ordinary_launch::frame& value) {
    const std::array<std::uint8_t, 8> magic{'L','A','T','S','E','E','1',0};
    if (value.size() != seed_offer_size || !std::equal(magic.begin(), magic.end(), value.begin())) seed_fail(seed_error_code::invalid_offer);
    std::size_t offset = 8;
    if (read64(value, offset) != 1 || std::all_of(value.begin() + 16, value.begin() + 32, [](auto byte) { return byte == 0; }))
        seed_fail(seed_error_code::invalid_offer);
    offset = 32; const auto parent = read_process(value, offset), child = read_process(value, offset);
    const auto device = read64(value, offset), inode = read64(value, offset), app = read64(value, offset);
    if (app != static_cast<std::uint64_t>(product::engram)) seed_fail(seed_error_code::wrong_product);
    if (!inode || child != ordinary_launch::current_process() || parent != ordinary_launch::current_parent_process() || child.parent != parent.pid)
        seed_fail(seed_error_code::invalid_offer);
    return {device, inode};
}
void missing_seed_file(int directory) {
    struct stat value{};
    // These are the exact initializer's ordinary primary-store names. A
    // partial/colliding creation is never repaired or treated as empty.
    for (const auto* leaf : {"memory.sqlite", "memory.sqlite-wal", "memory.sqlite-shm"})
        if (::fstatat(directory, leaf, &value, AT_SYMLINK_NOFOLLOW) == 0 || errno != ENOENT)
            seed_fail(seed_error_code::existing_file);
}
#endif
}

int receive_engram_seed(seed_callback callback, void* context, ordinary_launch::deadline end) {
#ifdef LATTICE_INSTALLATION_SEED_POSIX
    // Take both explicitly inherited descriptors before any fallible setup.
    seed_fd directory(4);
    ordinary_launch::inherited_channel channel(3);
    if (::fcntl(directory.value, F_SETFD, FD_CLOEXEC) || !callback) seed_fail(seed_error_code::unavailable);
    const auto offer = channel.receive(end); const auto expected = inspect_offer(offer);
    if (directory_fact(directory.value) != expected) seed_fail(seed_error_code::invalid_offer);
    const auto path = actual_directory_path(directory.value, expected) + "/memory.sqlite";
    missing_seed_file(directory.value);
    channel.send(phase(offer, 2), end);
    if (channel.receive(end) != phase(offer, 3)) seed_fail(seed_error_code::invalid_offer);
    seed_bound(end); (void)inspect_offer(offer);
    if (directory_fact(directory.value) != expected) seed_fail(seed_error_code::invalid_offer);
    missing_seed_file(directory.value);
    const auto status = callback(path.c_str(), context);
    seed_bound(end);
    if (status != 0) seed_fail(seed_error_code::initializer_failed);
    if (directory_fact(directory.value) != expected || actual_directory_path(directory.value, expected) + "/memory.sqlite" != path)
        seed_fail(seed_error_code::invalid_offer);
    channel.send(phase(offer, 4), end);
    // No ordinary application startup is permitted after this return. The
    // product branch exits; its actual terminal status remains controller-owned.
    return 0;
#else
    (void)callback; (void)context; (void)end; seed_fail(seed_error_code::unavailable);
#endif
}

#ifdef LATTICE_INSTALLATION_SEED_POSIX
struct seeded_installation_store::implementation {
    const pid_t creator = ::getpid();
    const std::thread::id thread = std::this_thread::get_id();
    created_launch_cohort cohort;
    created_launch_cohort::store_namespace space;
    retained_installation_file executable;
    executable_fact initializer_fact;
    file_identity main;
    ordinary_launch::terminal_observation terminal;
    direct_cohort_completion completion;
    bool registration_attempted = false;
    bool normal_attempted = false;
    std::string installation_leaf;
    struct registration {
        manifest catalog;
        ordinary_admission::record closed;
        retained_installation_file catalog_file, origin_file, external_file, launcher;
    };
    std::unique_ptr<registration> registered;
    implementation(created_launch_cohort&& c, created_launch_cohort::store_namespace&& s, retained_installation_file&& e, const executable_fact& fact)
        : cohort(std::move(c)), space(std::move(s)), executable(std::move(e)), initializer_fact(fact) {}
    void owner() const {
        if (::getpid() != creator || std::this_thread::get_id() != thread) seed_fail(seed_error_code::inherited_use);
    }
};
seeded_installation_store seeded_installation_store::create_engram_with_output(int parent, const std::string& leaf,
        const executable_fact& initializer, ordinary_launch::deadline end, int initializer_output) {
    if(initializer_output!=1 && initializer_output!=2)seed_fail(seed_error_code::invalid_offer);
    auto executable = retained_installation_file::open_executable(initializer, end);
    auto cohort = created_launch_cohort::create_before_child(parent, leaf);
    auto space = cohort.create_store_namespace("data");
    auto state = std::make_unique<implementation>(std::move(cohort), std::move(space), std::move(executable), initializer);
    state->installation_leaf = leaf;
    try {
        seed_fd data(state->space.duplicate_for_child());
        ordinary_launch::launch_specification launch;
        launch.executable = initializer.path; launch.working_directory = actual_directory_path(data.value, state->space.directory_identity());
        launch.arguments = {"--lattice-seed-store-v1"}; launch.environment = {"PATH=/usr/bin:/bin"};
        launch.output = initializer_output;
        launch.inherited_directories = {data.value};
        state->executable.verify(end);
        const auto index = state->cohort.launch(launch);
        // Controlled installer mutation exclusion is still required across
        // path-based posix_spawn. This check is not a hostile execve race proof.
        state->executable.verify(end);
        const auto offer = make_offer(state->cohort.child_identity(index), state->space.directory_identity());
        state->cohort.send(index, offer, end);
        if (state->cohort.receive(index, end) != phase(offer, 2)) seed_fail(seed_error_code::invalid_offer);
        state->cohort.send(index, phase(offer, 3), end);
        if (state->cohort.receive(index, end) != phase(offer, 4)) seed_fail(seed_error_code::initializer_failed);
        std::optional<ordinary_launch::terminal_observation> terminal;
        while (!(terminal = state->cohort.observe_terminal(index, end))) {
            seed_bound(end); std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (!WIFEXITED(terminal->wait_status) || WEXITSTATUS(terminal->wait_status) != 0)
            seed_fail(seed_error_code::terminal_unproved);
        // The actual child has exited normally; closing the durable gate now
        // admits no later initializer and does not manufacture a descendant fact.
        const auto completion = state->cohort.close_and_join(end);
        if (completion.children.size() != 1 || completion.children.front().child != terminal->child)
            seed_fail(seed_error_code::terminal_unproved);
        state->main = state->space.inspect_after_join("memory.sqlite"); state->terminal = *terminal;
        state->completion = completion;
        state->executable.verify(end);
        return seeded_installation_store(std::move(state));
    } catch (...) {
        const auto original = std::current_exception();
        const auto cleanup = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        try { (void)state->cohort.close_and_join(cleanup); } catch (...) { /* Real owners remain in the cohort registry. */ }
        std::rethrow_exception(original);
    }
}
#else
struct seeded_installation_store::implementation {};
seeded_installation_store seeded_installation_store::create_engram_with_output(int, const std::string&, const executable_fact&, ordinary_launch::deadline, int) { seed_fail(seed_error_code::unavailable); }
#endif
seeded_installation_store seeded_installation_store::create_engram(int parent,const std::string& leaf,
        const executable_fact& initializer,ordinary_launch::deadline end) {
    return create_engram_with_output(parent,leaf,initializer,end,1);
}
void seeded_installation_store::register_unadopted(const executable_fact& actual_launcher,
        const digest& package_provenance, const std::string& source_revision,
        const std::string& core_revision, ordinary_launch::deadline end) {
#ifdef LATTICE_INSTALLATION_SEED_POSIX
    if (!impl_) seed_fail(seed_error_code::unavailable);
    impl_->owner(); seed_bound(end);
    if (impl_->registration_attempted) seed_fail(seed_error_code::invalid_offer);
    // A partial durable registration is an orphan. This controller never retries
    // it, rewrites its original anchor, or reopens the already closed launch gate.
    impl_->registration_attempted = true;
    auto revision = [](const std::string& value) {
        return value.size() == 40 && std::all_of(value.begin(), value.end(), [](char byte) {
            return (byte >= '0' && byte <= '9') || (byte >= 'a' && byte <= 'f');
        });
    };
    if (!revision(source_revision) || !revision(core_revision) ||
        std::all_of(package_provenance.begin(), package_provenance.end(), [](auto byte) { return byte == 0; }) ||
        impl_->completion.children.size() != 1 ||
        impl_->completion.children.front().child != impl_->terminal.child ||
        impl_->completion.children.front().wait_status != impl_->terminal.wait_status ||
        !WIFEXITED(impl_->terminal.wait_status) || WEXITSTATUS(impl_->terminal.wait_status) != 0)
        seed_fail(seed_error_code::terminal_unproved);
    if (main_file() != impl_->main) seed_fail(seed_error_code::invalid_offer);
    impl_->executable.verify(end);
    auto launcher = retained_installation_file::open_executable(actual_launcher, end);
    seed_fd root(impl_->cohort.duplicate_closed_root(end));
    auto sync = [&](int fd) {
        seed_bound(end);
        if (::fsync(fd)) seed_fail(seed_error_code::unavailable);
        seed_bound(end);
    };
    // This fixed new directory is outside the data namespace. It cannot enroll
    // an existing catalog or add an alias for a store outside this owned origin.
    if (::mkdirat(root.value, "control", 0700)) seed_fail(seed_error_code::existing_file);
    seed_fd control(::openat(root.value, "control", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
    if (control.value < 0) seed_fail(seed_error_code::unavailable);
    const auto control_identity = directory_fact(control.value);
    auto verify_control_name = [&] {
        seed_bound(end); struct stat named{};
        if (directory_fact(control.value) != control_identity ||
            ::fstatat(root.value, "control", &named, AT_SYMLINK_NOFOLLOW) ||
            !S_ISDIR(named.st_mode) || named.st_uid != ::geteuid() || (named.st_mode & 07777) != 0700 ||
            static_cast<std::uint64_t>(named.st_dev) != control_identity.device ||
            static_cast<std::uint64_t>(named.st_ino) != control_identity.inode)
            seed_fail(seed_error_code::invalid_offer);
    };
    verify_control_name(); sync(control.value); sync(root.value);
    auto new_id = [&] {
        identifier value{};
        if (::getentropy(value.data(), value.size()) ||
            std::all_of(value.begin(), value.end(), [](auto byte) { return byte == 0; }))
            seed_fail(seed_error_code::unavailable);
        return value;
    };
    ordinary_admission::store_binding binding{new_id(), new_id(), new_id(), impl_->main, parent_directory()};
    auto journal = ordinary_admission::journal::create_unadopted(control.value, binding);
    const auto initial = journal.read();
    if (initial.state != ordinary_admission::stage::unadopted || initial.revision != 1 ||
        initial.binding != binding || initial.control != control_identity)
        seed_fail(seed_error_code::invalid_offer);
    // Actual creator/channel/exit facts accompany the durable closed cohort.
    // This is origin registration, not schema recovery/adoption or permission
    // for ordinary roles. The supported role is only the retired initializer.
    manifest catalog;
    catalog.application = product::engram; catalog.installation = binding.installation;
    catalog.revision = 1; catalog.catalog_directory = impl_->completion.directory;
    catalog.launch_gate = impl_->completion.gate; catalog.supervisor = actual_launcher;
    store_fact store;
    store.binding = binding; store.controls = {initial.control, initial.entry, initial.generation};
    store.control_leaf = "control";
    seed_fd data(::openat(root.value, "data", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
    if (data.value < 0) seed_fail(seed_error_code::unavailable);
    const auto data_path = actual_directory_path(data.value, binding.parent);
    store.aliases.push_back({data_path, "memory.sqlite", binding.parent});
    catalog.stores.push_back(std::move(store));
    role_fact role;
    role.name = "engram-initializer-v1"; role.executable = impl_->initializer_fact;
    role.working_directory = data_path; role.arguments = {"--lattice-seed-store-v1"};
    role.environment = {"PATH=/usr/bin:/bin"}; role.stores = {binding.store};
    catalog.roles.push_back(std::move(role));
    const auto encoded = encode_manifest(catalog);
    auto write_new = [&](const char* name, const std::vector<std::uint8_t>& bytes) {
        seed_bound(end);
        seed_fd target(::openat(root.value, name, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600));
        if (target.value < 0) seed_fail(seed_error_code::existing_file);
        std::size_t at = 0;
        while (at != bytes.size()) {
            seed_bound(end);
            const auto written = ::write(target.value, bytes.data() + at, bytes.size() - at);
            if (written < 0 && errno == EINTR) continue;
            if (written <= 0) seed_fail(seed_error_code::unavailable);
            at += static_cast<std::size_t>(written);
        }
        sync(target.value);
        struct stat opened{}, named{};
        if (::fstat(target.value, &opened) || !S_ISREG(opened.st_mode) || opened.st_nlink != 1 ||
            (opened.st_mode & 07777) != 0600 || opened.st_uid != ::geteuid() ||
            opened.st_size != static_cast<off_t>(bytes.size()) ||
            ::fstatat(root.value, name, &named, AT_SYMLINK_NOFOLLOW) || !S_ISREG(named.st_mode) ||
            opened.st_dev != named.st_dev || opened.st_ino != named.st_ino)
            seed_fail(seed_error_code::invalid_offer);
        sync(root.value);
        return file_identity{static_cast<std::uint64_t>(opened.st_dev), static_cast<std::uint64_t>(opened.st_ino)};
    };
    const auto catalog_identity = write_new("catalog.v1", encoded);
    digest catalog_hash{}; picosha2::hash256(encoded.begin(), encoded.end(), catalog_hash.begin(), catalog_hash.end());
    auto held_catalog = retained_installation_file::open_record(root.value, "catalog.v1", catalog_identity,
        catalog_hash, maximum_manifest_bytes, end);
    std::vector<std::uint8_t> anchor{'L','A','T','O','R','G','1',0};
    append64(anchor, 1);
    anchor.insert(anchor.end(), binding.installation.begin(), binding.installation.end());
    append64(anchor, impl_->completion.directory.device); append64(anchor, impl_->completion.directory.inode);
    append64(anchor, catalog_identity.device); append64(anchor, catalog_identity.inode);
    anchor.insert(anchor.end(), catalog_hash.begin(), catalog_hash.end());
    anchor.insert(anchor.end(), package_provenance.begin(), package_provenance.end());
    anchor.insert(anchor.end(), source_revision.begin(), source_revision.end());
    anchor.insert(anchor.end(), core_revision.begin(), core_revision.end());
    append_process(anchor, impl_->terminal.child);
    append64(anchor, static_cast<std::uint64_t>(impl_->terminal.wait_status));
    append64(anchor, impl_->completion.revision);
    // Last publication, after the catalog and journal are durable. A separately
    // retained installer record must anchor these exact bytes for fresh reopen;
    // reading a replacement by pathname must never relearn an expected identity.
    launcher.verify(end); impl_->executable.verify(end); verify_control_name(); held_catalog.verify(end);
    if (main_file() != binding.main || journal.read() != initial) seed_fail(seed_error_code::invalid_offer);
    seed_fd checked_root(impl_->cohort.duplicate_closed_root(end));
    if (anchor.size() != 256) seed_fail(seed_error_code::invalid_offer);
    const auto origin_identity = write_new("origin.v1", anchor);
    launcher.verify(end); impl_->executable.verify(end); verify_control_name(); held_catalog.verify(end);
    if (main_file() != binding.main || journal.read() != initial) seed_fail(seed_error_code::invalid_offer);
    seed_fd final_root(impl_->cohort.duplicate_closed_root(end));
    std::vector<std::uint8_t> external{'L','A','T','R','E','G','1',0};
    append64(external, 1);
    append64(external, origin_identity.device); append64(external, origin_identity.inode);
    digest origin_hash{}; picosha2::hash256(anchor.begin(), anchor.end(), origin_hash.begin(), origin_hash.end());
    auto held_origin = retained_installation_file::open_record(root.value, "origin.v1", origin_identity,
        origin_hash, anchor.size(), end);
    external.insert(external.end(), origin_hash.begin(), origin_hash.end());
    external.insert(external.end(), anchor.begin(), anchor.end());
    verify_control_name(); held_catalog.verify(end); held_origin.verify(end);
    auto held_external = impl_->cohort.commit_origin_anchor(external, end);
    verify_control_name(); held_catalog.verify(end); held_origin.verify(end); held_external.verify(end);
    // Retain independently captured named anchor/file custody on this exact
    // creator controller. A later process has no constructor for this object.
    impl_->registered = std::make_unique<implementation::registration>(implementation::registration{
        std::move(catalog), initial, std::move(held_catalog), std::move(held_origin),
        std::move(held_external), std::move(launcher)});
#else
    (void)actual_launcher; (void)package_provenance; (void)source_revision; (void)core_revision; (void)end;
    seed_fail(seed_error_code::unavailable);
#endif
}
int seeded_installation_store::run_registered_mcp(ordinary_launch::deadline end) {
#ifdef LATTICE_INSTALLATION_SEED_POSIX
    if(!impl_)seed_fail(seed_error_code::unavailable);impl_->owner();seed_bound(end);
    if(!impl_->registered || impl_->normal_attempted)seed_fail(seed_error_code::invalid_offer);
    impl_->normal_attempted=true;
    auto& registration=*impl_->registered;
    auto verify=[&] {
        seed_bound(end);impl_->executable.verify(end);registration.launcher.verify(end);
        registration.catalog_file.verify(end);registration.origin_file.verify(end);registration.external_file.verify(end);
        if(main_file()!=registration.closed.binding.main)seed_fail(seed_error_code::invalid_offer);
    };
    verify();
    seed_fd root(impl_->cohort.duplicate_closed_root(end));
    seed_fd parent(impl_->cohort.duplicate_installer_parent());
    seed_fd data(::openat(root.value,"data",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
    seed_fd control(::openat(root.value,"control",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
    if(data.value<0 || control.value<0 || directory_fact(data.value)!=registration.closed.binding.parent ||
       directory_fact(control.value)!=registration.closed.control)seed_fail(seed_error_code::invalid_offer);
    auto journal=ordinary_admission::journal::open_existing(control.value,registration.closed.binding,
        {registration.closed.control,registration.closed.entry,registration.closed.generation});
    if(journal.read()!=registration.closed)seed_fail(seed_error_code::invalid_offer);
    // A separate new cohort; the retired initializer's launch gate never reopens.
    auto normal=created_launch_cohort::create_before_child(root.value,"normal-runtime");
    seed_fd normal_root(normal.duplicate_accepting_root());
    seed_fd gate(::openat(normal_root.value,"launch.lock",O_RDONLY|O_NOFOLLOW|O_CLOEXEC));
    struct stat gate_stat{};
    if(gate.value<0 || ::fstat(gate.value,&gate_stat) || !S_ISREG(gate_stat.st_mode) ||
       gate_stat.st_uid!=::geteuid() || (gate_stat.st_mode&07777)!=0600 || gate_stat.st_nlink!=1)
        seed_fail(seed_error_code::invalid_offer);
    manifest catalog=registration.catalog;
    catalog.catalog_directory=directory_fact(normal_root.value);
    catalog.launch_gate={static_cast<std::uint64_t>(gate_stat.st_dev),static_cast<std::uint64_t>(gate_stat.st_ino)};
    auto& role=catalog.roles.front();role.name="engram-primary-mcp-v1";
    role.arguments={"--lattice-managed-mcp-v1"};
    role.environment={"PATH=/usr/bin:/bin","CLAUDE_MEMORY_DB="+role.working_directory+"/memory.sqlite"};
    // Preserve these existing non-authority MCP settings in the exact bounded
    // invocation. No inherited setting can name a different database or role.
    for(const auto* name:{"CLAUDE_MEMORY_MODEL","CLAUDE_SESSION_ID","ENGRAM_LATTICE_LOG_LEVEL"})
        if(const auto* value=std::getenv(name))role.environment.push_back(std::string(name)+"="+value);
    validate_primary_mcp_contract(registration.catalog,catalog);
    const auto catalog_bytes=encode_manifest(catalog);
    if(catalog_bytes.size()>16*1024)seed_fail(seed_error_code::invalid_offer);
    seed_fd catalog_file(::openat(normal_root.value,"normal-role.v1",O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600));
    if(catalog_file.value<0)seed_fail(seed_error_code::existing_file);
    std::size_t at=0;
    while(at!=catalog_bytes.size()){
        seed_bound(end);const auto n=::write(catalog_file.value,catalog_bytes.data()+at,catalog_bytes.size()-at);
        if(n<0 && errno==EINTR)continue;if(n<=0)seed_fail(seed_error_code::unavailable);at+=static_cast<std::size_t>(n);
    }
    if(::fsync(catalog_file.value) || ::fsync(normal_root.value))seed_fail(seed_error_code::unavailable);
    struct stat catalog_stat{};
    if(::fstat(catalog_file.value,&catalog_stat))seed_fail(seed_error_code::unavailable);
    const file_identity catalog_identity{static_cast<std::uint64_t>(catalog_stat.st_dev),static_cast<std::uint64_t>(catalog_stat.st_ino)};
    digest catalog_digest{};picosha2::hash256(catalog_bytes.begin(),catalog_bytes.end(),catalog_digest.begin(),catalog_digest.end());
    auto held_normal=retained_installation_file::open_record(normal_root.value,"normal-role.v1",catalog_identity,catalog_digest,16*1024,end);
    normal_launch_offer offer;offer.supervisor=ordinary_launch::current_process();
    if(::getentropy(offer.nonce.data(),offer.nonce.size()) ||
        std::all_of(offer.nonce.begin(),offer.nonce.end(),[](auto c){return c==0;}))seed_fail(seed_error_code::unavailable);
    offer.external_anchor=registration.external_file.read_record(end);offer.external_identity=registration.external_file.identity();
    offer.external_leaf=impl_->installation_leaf+".origin";offer.origin_catalog=registration.catalog_file.read_record(end);
    offer.normal_catalog=catalog_bytes;offer.normal_catalog_identity=catalog_identity;offer.admission=registration.closed;
    ordinary_launch::launch_specification specification;
    specification.executable=role.executable.path;specification.working_directory=role.working_directory;
    specification.arguments=role.arguments;specification.environment=role.environment;
    specification.inherited_directories={data.value,control.value,root.value,parent.value,normal_root.value};
    try {
        verify();held_normal.verify(end);
        const auto index=normal.launch(specification);offer.child=normal.child_identity(index);
        verify();held_normal.verify(end);
        normal.send(index,encode_normal_offer(offer),end);
        auto response=offer;response.phase=2;
        if(normal.receive(index,end)!=encode_normal_offer(response))seed_fail(seed_error_code::invalid_offer);
        verify();held_normal.verify(end);
        offer.admission=normal_origin_authority::admit(journal,registration.closed,offer.nonce);
        offer.phase=3;normal.send(index,encode_normal_offer(offer),end);
        response=offer;response.phase=4;
        if(normal.receive(index,end)!=encode_normal_offer(response))seed_fail(seed_error_code::invalid_offer);
        // Normal service duration is owned until the actual MCP child exits.
        // Each OS observation has its own bounded attempt; no timeout/reply is
        // substituted for a terminal event. This controller admits no second role.
        std::optional<ordinary_launch::terminal_observation> terminal;
        while(!(terminal=normal.observe_terminal(index,std::chrono::steady_clock::now()+std::chrono::seconds(1))))
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        const auto cleanup=std::chrono::steady_clock::now()+std::chrono::seconds(5);
        (void)journal.begin_retirement(offer.nonce);
        const auto completed=normal.close_and_join(cleanup);
        if(completed.children.size()!=1 || completed.children.front().child!=terminal->child ||
           completed.children.front().wait_status!=terminal->wait_status)seed_fail(seed_error_code::terminal_unproved);
        if(WIFEXITED(terminal->wait_status))return WEXITSTATUS(terminal->wait_status);
        if(WIFSIGNALED(terminal->wait_status))return 128+WTERMSIG(terminal->wait_status);
        seed_fail(seed_error_code::terminal_unproved);
    } catch (...) {
        const auto original=std::current_exception();
        // Unknown/failed/partial launches remain charged in the cohort registry
        // and durable record. No later issuer can reopen this origin as fresh.
        try{(void)journal.begin_retirement(offer.nonce);}catch(...){}
        try{(void)normal.close_and_join(std::chrono::steady_clock::now()+std::chrono::seconds(5));}catch(...){}
        std::rethrow_exception(original);
    }
#else
    (void)end;seed_fail(seed_error_code::unavailable);
#endif
}
seeded_installation_store::seeded_installation_store(std::unique_ptr<implementation> value) : impl_(std::move(value)) {}
seeded_installation_store::~seeded_installation_store() = default;
seeded_installation_store::seeded_installation_store(seeded_installation_store&&) noexcept = default;
seeded_installation_store& seeded_installation_store::operator=(seeded_installation_store&&) noexcept = default;
file_identity seeded_installation_store::main_file() const {
#ifdef LATTICE_INSTALLATION_SEED_POSIX
    if (!impl_) seed_fail(seed_error_code::unavailable); impl_->owner();
    if (impl_->space.inspect_after_join("memory.sqlite") != impl_->main) seed_fail(seed_error_code::invalid_offer);
    return impl_->main;
#else
    seed_fail(seed_error_code::unavailable);
#endif
}
file_identity seeded_installation_store::parent_directory() const {
#ifdef LATTICE_INSTALLATION_SEED_POSIX
    if (!impl_) seed_fail(seed_error_code::unavailable); impl_->owner(); return impl_->space.directory_identity();
#else
    seed_fail(seed_error_code::unavailable);
#endif
}
ordinary_launch::terminal_observation seeded_installation_store::initializer_terminal() const {
#ifdef LATTICE_INSTALLATION_SEED_POSIX
    if (!impl_) seed_fail(seed_error_code::unavailable); impl_->owner(); return impl_->terminal;
#else
    seed_fail(seed_error_code::unavailable);
#endif
}
} // namespace lattice::detail::ordinary_installation
