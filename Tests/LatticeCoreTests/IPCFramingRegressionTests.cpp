#include <lattice/ipc.hpp>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#ifndef LATTICE_IPC_STANDALONE_TEST_MAIN
#include <gtest/gtest.h>
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include <arpa/inet.h>
#include <pthread.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {
enum class framing_case { closed_header, closed_payload, roundtrip, pipe_roundtrip };

bool prepare_child() {
    if (std::signal(SIGALRM, SIG_DFL) == SIG_ERR ||
        std::signal(SIGPIPE, SIG_DFL) == SIG_ERR) return false;
    sigset_t unblocked;
    sigemptyset(&unblocked);
    sigaddset(&unblocked, SIGPIPE);
    sigaddset(&unblocked, SIGALRM);
    if (pthread_sigmask(SIG_UNBLOCK, &unblocked, nullptr) != 0) return false;
    alarm(5);
    return true;
}

bool signal_policy_is_default() {
    struct sigaction action {};
    return sigaction(SIGPIPE, nullptr, &action) == 0 && action.sa_handler == SIG_DFL;
}

bool configure_socket(int fd) {
#ifdef __APPLE__
    // Match normal ipc_socket_client/ipc_server setup. The unchanged Darwin
    // raw framing helper alone does not configure arbitrary socket pairs.
    int on = 1;
    return setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on)) == 0;
#else
    (void)fd;
    return true;
#endif
}

bool read_exact(int fd, void* destination, size_t size) {
    auto* bytes = static_cast<unsigned char*>(destination);
    size_t done = 0;
    while (done < size) {
        const auto count = ::read(fd, bytes + done, size - done);
        if (count <= 0) return false;
        done += static_cast<size_t>(count);
    }
    return true;
}

int exercise(framing_case which) {
    if (!prepare_child()) return 10;
    int fds[2];
    if (which == framing_case::pipe_roundtrip) {
        if (::pipe(fds) != 0) return 11;
        const std::string expected("pipe\0payload", 12);
        const bool sent = lattice::write_length_prefixed(fds[1], expected.data(), expected.size());
        const auto received = lattice::read_length_prefixed(fds[0]);
        ::close(fds[0]); ::close(fds[1]);
        return sent && std::string(received.begin(), received.end()) == expected && signal_policy_is_default() ? 0 : 12;
    }
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) return 13;
    if (!configure_socket(fds[0])) return 14;
    if (which == framing_case::closed_header) {
        ::close(fds[1]);
        const char byte = 'h';
        const bool sent = lattice::write_length_prefixed(fds[0], &byte, 1);
        const int error = errno;
        ::close(fds[0]);
        std::fprintf(stderr, "closed_header sent=%d errno=%d\n", sent, error);
        return !sent && error == EPIPE && signal_policy_is_default() ? 0 : 15;
    }
    if (which == framing_case::closed_payload) {
        int requested = 1024;
        if (setsockopt(fds[0], SOL_SOCKET, SO_SNDBUF, &requested, sizeof(requested)) != 0) return 16;
        int actual = 0; socklen_t size = sizeof(actual);
        if (getsockopt(fds[0], SOL_SOCKET, SO_SNDBUF, &actual, &size) != 0 || actual <= 0) return 17;
        const std::vector<uint8_t> payload(2 * 1024 * 1024, 0xA7);
        if (static_cast<size_t>(actual) >= payload.size() / 4) return 18;
        bool saw_header_and_payload = false;
        std::thread peer([&] {
            uint32_t header = 0; uint8_t first = 0;
            saw_header_and_payload = read_exact(fds[1], &header, sizeof(header)) &&
                ntohl(header) == payload.size() && read_exact(fds[1], &first, 1) && first == payload[0];
            // The payload is much larger than the send buffer. Closing after
            // its first byte forces the payload loop to encounter peer loss.
            ::shutdown(fds[1], SHUT_RDWR);
            ::close(fds[1]);
        });
        const bool sent = lattice::write_length_prefixed(fds[0], payload.data(), payload.size());
        const int error = errno;
        peer.join();
        ::close(fds[0]);
        std::fprintf(stderr, "closed_payload saw_header_and_payload=%d sent=%d errno=%d\n",
                     saw_header_and_payload, sent, error);
        return saw_header_and_payload && !sent && (error == EPIPE || error == ECONNRESET) &&
            signal_policy_is_default() ? 0 : 19;
    }
    const std::vector<uint8_t> payload{0, 1, 2, 0, 254, 255};
    const bool sent = lattice::write_length_prefixed(fds[0], payload.data(), payload.size());
    const auto received = lattice::read_length_prefixed(fds[1]);
    ::close(fds[0]); ::close(fds[1]);
    return sent && received == payload && signal_policy_is_default() ? 0 : 20;
}
} // namespace

#ifdef LATTICE_IPC_STANDALONE_TEST_MAIN
#include <lattice/log.hpp>
// Standalone framing-only linkage: ipc.cpp's unused transport methods need
// this logging symbol. No database, fake transport, framing stub or TestHelpers.
namespace lattice { std::atomic<log_level> g_log_level{log_level::off}; }
int main(int argc, char** argv) {
    if (argc != 2) return 21;
    if (std::strcmp(argv[1], "header") == 0) return exercise(framing_case::closed_header);
    if (std::strcmp(argv[1], "payload") == 0) return exercise(framing_case::closed_payload);
    if (std::strcmp(argv[1], "roundtrip") == 0) return exercise(framing_case::roundtrip);
    if (std::strcmp(argv[1], "pipe") == 0) return exercise(framing_case::pipe_roundtrip);
    return 22;
}
#elif GTEST_HAS_DEATH_TEST
namespace {
void expect_child_success(framing_case which) {
    struct restore_style {
        std::string previous = ::testing::FLAGS_gtest_death_test_style;
        ~restore_style() { ::testing::FLAGS_gtest_death_test_style = std::move(previous); }
    } restore;
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    // Fresh exec rather than inherited SQLite/worker locks. Alarm bounds this
    // child body; the normal test runner also bounds pre-body global setup.
    ASSERT_EXIT({ _exit(exercise(which)); }, ::testing::ExitedWithCode(0), "");
}
}
TEST(IPCFramingRegression, ClosedPeerDuringHeaderDoesNotRaiseSIGPIPE) {
    expect_child_success(framing_case::closed_header);
}
TEST(IPCFramingRegression, ClosedPeerDuringPayloadDoesNotRaiseSIGPIPE) {
    expect_child_success(framing_case::closed_payload);
}
TEST(IPCFramingRegression, SocketRoundTripPreservesBinaryPayloadAndSignalPolicy) {
    expect_child_success(framing_case::roundtrip);
}
TEST(IPCFramingRegression, NonSocketPipeRoundTripPreservesGenericDescriptorSupport) {
    expect_child_success(framing_case::pipe_roundtrip);
}
#endif
#endif
