#include "../../Sources/LatticeCore/src/ordinary_installation_seed.hpp"
#include <filesystem>
#include <string>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <fcntl.h>
#include <unistd.h>
namespace {
int write_seed(const char* path, void* context) {
    const auto& mode = *static_cast<const std::string*>(context);
    if (mode == "callback-failure") return 1;
    if (mode == "missing-file") return 0;
    const auto fd = ::open(path, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600);
    if (fd < 0) return 1;
    constexpr char bytes[] = "physical-receiver-fixture";
    const auto count = ::write(fd, bytes, sizeof(bytes)); const auto closed = ::close(fd);
    return count == sizeof(bytes) && closed == 0 ? 0 : 1;
}
}
#endif
int main(int argc, char** argv) {
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    if (argc != 2 || std::string(argv[1]) != "--lattice-seed-store-v1") return 64;
    try {
        // The actual owning controller chooses a fresh test directory. This
        // executable is test-only; it has no catalog/context/adoption issuer.
        auto mode = std::filesystem::current_path().parent_path().filename().string();
        if (mode == "bad-reply") {
            lattice::detail::ordinary_launch::inherited_channel channel(3);
            const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            auto offer = channel.receive(end); if (offer.size() != 120) return 65;
            offer[16] ^= 1; offer[8] = 2;
            channel.send(offer, end); return 0;
        }
        const auto status = lattice::detail::ordinary_installation::receive_engram_seed(
            write_seed, &mode, std::chrono::steady_clock::now() + std::chrono::seconds(5));
        if (mode == "reply-nonzero-exit") return 17;
        if (mode == "reply-still-running") for (;;) ::pause();
        return status;
    } catch (...) { return 74; }
#else
    (void)argc; (void)argv; return 69;
#endif
}
