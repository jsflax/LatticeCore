#include <lattice/ipc.hpp>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <csignal>
#include <pthread.h>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__) && GTEST_HAS_DEATH_TEST
namespace {
namespace fs = std::filesystem;
struct owned_directory {
    fs::path root;
    int original = -1;
    owned_directory() {
        original = ::open(".", O_RDONLY | O_CLOEXEC);
        if (original < 0) throw std::runtime_error("test original directory");
        auto pattern = (fs::temp_directory_path()/"ipc-path-XXXXXX").string();
        std::vector<char> bytes(pattern.begin(), pattern.end()); bytes.push_back(0);
        const auto created = ::mkdtemp(bytes.data());
        if (!created) { ::close(original); throw std::runtime_error("test directory"); }
        root = created;
        if (::chdir(root.c_str()) != 0) throw std::runtime_error("test chdir");
    }
    ~owned_directory() {
        if (original >= 0) { ::fchdir(original); ::close(original); }
        std::error_code error; fs::remove_all(root, error);
    }
};
void default_environment(bool original_111 = false) {
    // Relative HOME/runtime are child-owned under the actual new temporary
    // directory. This avoids changing the parent process environment or store.
    const auto home = original_111 ? std::string(29, 'h') : "home";
    const auto runtime = original_111 ? std::string(48, 'r') : "runtime";
    if (::setenv("HOME", home.c_str(), 1) || ::setenv("XDG_RUNTIME_DIR", runtime.c_str(), 1))
        throw std::runtime_error("test environment");
}
std::string default_base(bool original_111 = false) {
#ifdef __APPLE__
    return (original_111 ? std::string(29, 'h') : "home") + "/Library/Caches/Lattice/ipc";
#else
    return (original_111 ? std::string(48, 'r') : "runtime") + "/lattice";
#endif
}
void write_file(const std::string& path) { std::ofstream output(path); output << "original"; if (!output) throw std::runtime_error("test file"); }
bool intact(const std::string& path) { std::ifstream input(path); std::string value; input >> value; return value == "original"; }
bool refuses_server(const std::string& path) {
    lattice::ipc_server server(path);
    try { server.start([](auto) {}); } catch (const std::invalid_argument&) { return !server.is_listening(); }
    return false;
}
enum class path_case { exact_limit, over_limit, nul_alias, empty, explicit_lazy,
    explicit_endpoint_refusal, short_default, full_hash, different_long_channels,
    default_111_connect, default_nul, base_refusal };
int exercise(path_case which) {
    // Match the existing IPC regression child: alarm must not inherit an
    // ignored disposition or blocked mask from global test initialization.
    if (std::signal(SIGALRM, SIG_DFL) == SIG_ERR) return 90;
    sigset_t unblocked; sigemptyset(&unblocked); sigaddset(&unblocked, SIGALRM);
    if (pthread_sigmask(SIG_UNBLOCK, &unblocked, nullptr) != 0) return 91;
    alarm(5); // The same finite ordinary-child clock; no renewal.
    try {
        owned_directory owned;
        sockaddr_un address{}; const auto capacity = sizeof(address.sun_path);
        if (which == path_case::exact_limit) {
            const std::string path(capacity - 1, 'e');
            lattice::ipc_server server(path); server.start([](auto) {});
            const bool exists = fs::is_socket(path) && server.is_listening();
            server.stop(); return exists && !fs::exists(path) ? 0 : 10;
        }
        if (which == path_case::over_limit) {
            const std::string prefix(capacity - 1, 'p'), full = prefix + 'z';
            write_file(prefix); write_file(full);
            bool error = false; lattice::ipc_socket_client client(full);
            client.set_on_error([&](const auto&) { error = true; }); client.connect("");
            return refuses_server(full) && intact(prefix) && intact(full) && error &&
                client.state() == lattice::transport_state::closed ? 0 : 11;
        }
        if (which == path_case::nul_alias) {
            write_file("kept"); const std::string path("kept\0suffix", 11);
            bool error = false; lattice::ipc_socket_client client(path);
            client.set_on_error([&](const auto&) { error = true; }); client.connect("");
            return refuses_server(path) && intact("kept") && error ? 0 : 12;
        }
        if (which == path_case::empty) return refuses_server("") ? 0 : 13;
        if (which == path_case::explicit_lazy) {
            const std::string too_long(capacity, 'u');
            ::setenv("HOME", too_long.c_str(), 1); ::setenv("XDG_RUNTIME_DIR", too_long.c_str(), 1);
            lattice::ipc_endpoint endpoint("unused-long-default", std::string("explicit/endpoint.sock"));
            endpoint.start([](auto) {}); const bool server = endpoint.is_server(); endpoint.stop();
            return server && !fs::exists(too_long) && !fs::exists("explicit/endpoint.sock") ? 0 : 14;
        }
        if (which == path_case::explicit_endpoint_refusal) {
            default_environment(); const auto base = default_base();
            const std::string path = "new-parent/" + std::string(capacity, 'x');
            try { lattice::ipc_endpoint endpoint("unused", path); } catch (const std::invalid_argument&) {
                return !fs::exists("new-parent") && !fs::exists(base) ? 0 : 15;
            }
            return 16;
        }
        if (which == path_case::short_default) {
            default_environment(); const auto expected = default_base()+"/short.sock";
            return lattice::resolve_ipc_socket_path("short") == expected && fs::is_directory(default_base()) ? 0 : 17;
        }
        const std::string group = "engram-group-00000000-0000-0000-0000-000000000001";
        if (which == path_case::full_hash) {
            default_environment(true);
            const auto path = lattice::resolve_ipc_socket_path(group);
            return path == default_base(true)+"/h-86sw5hNiZrXxYMR1llxSYeOx14RrQ4As9CAZUvwY5JA" && path.size() == 102 &&
                path.size() < capacity && fs::is_directory(default_base(true)) ? 0 : 18;
        }
        if (which == path_case::different_long_channels) {
            default_environment(); const std::string common(256, 'c');
            const auto first = lattice::resolve_ipc_socket_path(common+'a');
            const auto second = lattice::resolve_ipc_socket_path(common+'b');
            if (first == second || first != lattice::resolve_ipc_socket_path(common+'a')) return 19;
            lattice::ipc_server a(first), b(second); a.start([](auto) {}); b.start([](auto) {});
            const bool both = a.is_listening() && b.is_listening(); a.stop(); b.stop();
            return both && !fs::exists(first) && !fs::exists(second) ? 0 : 20;
        }
        if (which == path_case::default_111_connect) {
            default_environment(true); const auto plain = default_base(true)+"/"+group+".sock";
            if (plain.size() != 111) return 21;
            const auto path = lattice::resolve_ipc_socket_path(group);
            std::mutex accepted_mutex;
            std::vector<std::unique_ptr<lattice::ipc_socket_client>> accepted;
            lattice::ipc_endpoint server(group), client(group);
            server.start([&](auto actual) {
                std::lock_guard<std::mutex> lock(accepted_mutex);
                accepted.push_back(std::move(actual));
            });
            std::unique_ptr<lattice::ipc_socket_client> dialer;
            client.start([&](auto actual) { dialer = std::move(actual); });
            // The endpoint's real probe already connected to the bound server;
            // adopt that exact fd through the normal client transport API too.
            bool opened = false; if (dialer) { dialer->set_on_open([&] { opened = true; }); dialer->connect(""); }
            const bool good = server.is_server() && !client.is_server() && dialer && opened &&
                dialer->state() == lattice::transport_state::open && fs::is_socket(path);
            if (dialer) dialer->disconnect(); server.stop(); client.stop();
            return good && !fs::exists(path) && !fs::exists(plain) ? 0 : 22;
        }
        if (which == path_case::default_nul) {
            default_environment();
            try { (void)lattice::resolve_ipc_socket_path(std::string("channel\0alias", 13)); }
            catch (const std::invalid_argument&) { return !fs::exists(default_base()) ? 0 : 23; }
            return 24;
        }
        if (which == path_case::base_refusal) {
            const std::string too_long(capacity, 'd');
            ::setenv("HOME", too_long.c_str(), 1); ::setenv("XDG_RUNTIME_DIR", too_long.c_str(), 1);
            try { (void)lattice::resolve_ipc_socket_path("channel"); }
            catch (const std::invalid_argument&) { return !fs::exists(too_long) ? 0 : 25; }
            return 26;
        }
    } catch (...) { return 99; }
    return 100;
}
void expect_child_success(path_case which) {
    struct restore_style { std::string original = ::testing::FLAGS_gtest_death_test_style;
        ~restore_style() { ::testing::FLAGS_gtest_death_test_style = original; } } restore;
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT({ _exit(exercise(which)); }, ::testing::ExitedWithCode(0), "");
}
}
TEST(IPCPathLength, ExactSunPathMinusTerminatorBindsAndCleansSameName) { expect_child_success(path_case::exact_limit); }
TEST(IPCPathLength, FullSunPathRefusesBeforeDeletingEitherOriginalOrPrefix) { expect_child_success(path_case::over_limit); }
TEST(IPCPathLength, EmbeddedNULCannotDeleteOrConnectToPrefixAlias) { expect_child_success(path_case::nul_alias); }
TEST(IPCPathLength, EmptyExplicitServerPathRefuses) { expect_child_success(path_case::empty); }
TEST(IPCPathLength, ExplicitPathDoesNotEvaluateUnusedOverlongDefault) { expect_child_success(path_case::explicit_lazy); }
TEST(IPCPathLength, InvalidExplicitEndpointDoesNotCreateDefaultOrParent) { expect_child_success(path_case::explicit_endpoint_refusal); }
TEST(IPCPathLength, ExistingFittingDefaultNameRemainsByteExact) { expect_child_success(path_case::short_default); }
TEST(IPCPathLength, FullSHA256VectorFitsOriginal111ByteDefaultEnvelope) { expect_child_success(path_case::full_hash); }
TEST(IPCPathLength, LongChannelsWithSameTruncatedPrefixBindDifferentNames) { expect_child_success(path_case::different_long_channels); }
TEST(IPCPathLength, Original111ByteGroupDefaultActuallyBindsAndConnects) { expect_child_success(path_case::default_111_connect); }
TEST(IPCPathLength, DefaultNULRefusesBeforeDirectoryCreation) { expect_child_success(path_case::default_nul); }
TEST(IPCPathLength, UnrepresentableDefaultDirectoryRefusesBeforeCreation) { expect_child_success(path_case::base_refusal); }
#endif
