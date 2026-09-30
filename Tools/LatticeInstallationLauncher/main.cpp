#include "../../Sources/LatticeCore/src/ordinary_installation_seed.hpp"
#include "engram_product_installer.hpp"
#include <charconv>
#include <cstdio>
#include <string_view>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <fcntl.h>
#include <unistd.h>
#endif
namespace {
std::uint64_t integer(std::string_view value) {
    std::uint64_t result = 0;
    const auto parsed = std::from_chars(value.data(), value.data() + value.size(), result);
    if (value.empty() || parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size())
        throw std::runtime_error("invalid installer fact");
    return result;
}
lattice::detail::ordinary_installation::digest hash(std::string_view value) {
    if (value.size() != 64) throw std::runtime_error("invalid installer fact");
    lattice::detail::ordinary_installation::digest result{};
    auto nibble = [](char byte) -> unsigned {
        if (byte >= '0' && byte <= '9') return static_cast<unsigned>(byte - '0');
        if (byte >= 'a' && byte <= 'f') return static_cast<unsigned>(byte - 'a' + 10);
        throw std::runtime_error("invalid installer fact");
    };
    for (std::size_t i = 0; i != result.size(); ++i)
        result[i] = static_cast<std::uint8_t>((nibble(value[2*i]) << 4) | nibble(value[2*i+1]));
    return result;
}
}
int main(int argc, char** argv) {
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    // Dedicated, single-threaded native installer process. It owns all of its
    // spawn/wait operations and opens no database or application runtime. The
    // ordinary role proxy/supervisor command will be a separate accepted slice.
    // This explicit installer command does not handle arbitrary existing stores.
    if (argc == 4 && std::string_view(argv[1]) == "create-and-run-engram-mcp") {
        int parent=-1;
        try {
            parent=::open(argv[2],O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC);
            if(parent<0)throw std::runtime_error("installer directory unavailable");
            const auto result=lattice::detail::ordinary_installation::engram_product_installer::create_and_run_primary_mcp(parent,argv[3],
                std::chrono::steady_clock::now()+std::chrono::seconds(30));
            ::close(parent);return result;
        } catch(...) {
            if(parent>=0)::close(parent);
            std::fputs("installation launcher: managed MCP refused\n",stderr);return 74;
        }
    }
    if (argc == 4 && std::string_view(argv[1]) == "create-engram") {
        int parent = -1;
        try {
            parent = ::open(argv[2], O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
            if (parent < 0) throw std::runtime_error("installer directory unavailable");
            lattice::detail::ordinary_installation::engram_product_installer::create_registered(parent, argv[3],
                std::chrono::steady_clock::now() + std::chrono::seconds(30));
            ::close(parent);
            std::fputs("registered-unadopted-origin\n", stdout); return 0;
        } catch (...) {
            if (parent >= 0) ::close(parent);
            std::fputs("installation launcher: registration refused\n", stderr); return 74;
        }
    }
    if (argc != 8 || std::string_view(argv[1]) != "seed-engram") {
        std::fputs("installation launcher: unsupported invocation\n", stderr); return 64;
    }
    int parent = -1;
    try {
        using namespace lattice::detail::ordinary_installation;
        executable_fact initializer{argv[4], {integer(argv[5]), integer(argv[6])}, hash(argv[7])};
        parent = ::open(argv[2], O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        if (parent < 0) throw std::runtime_error("installer directory unavailable");
        auto initialized = seeded_installation_store::create_engram(parent, argv[3], initializer,
            std::chrono::steady_clock::now() + std::chrono::seconds(30));
        (void)initialized.main_file(); (void)initialized.parent_directory(); (void)initialized.initializer_terminal();
        // This reports only this controller-owned initialization and actual
        // direct-child exit. It is not installation registration/adoption.
        std::fputs("initialized-unadopted\n", stdout);
        ::close(parent); return 0;
    } catch (...) {
        if (parent >= 0) ::close(parent);
        std::fputs("installation launcher: initialization refused\n", stderr); return 74;
    }
#else
    (void)argc; (void)argv; std::fputs("installation launcher: unsupported platform\n", stderr); return 69;
#endif
}
