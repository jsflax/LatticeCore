#pragma once
#include <array>
#include <string_view>
// Generated only by reviewed product qualification/packaging from actual
// pinned legacy build/artifact bytes. Runtime bootstrap JSON cannot supply or
// repair this semantic profile. The final signed controller binds these values.
#ifdef LATTICE_ENGRAM_LEGACY_PROFILE_HEADER
#include LATTICE_ENGRAM_LEGACY_PROFILE_HEADER
#else
namespace lattice::detail::ordinary_installation::engram_legacy_profile {
inline constexpr bool available = false;
inline constexpr std::string_view profile = "engram-v0.14.10-darwin-qualified-spawn-v1";
inline constexpr std::string_view engram_revision = "5e0862b6af90578a0e2899bd379fb2b9be9f9703";
inline constexpr std::string_view sdk_revision = "0461b210aa028f3c4bb6165fed895e0f4385b565";
inline constexpr std::string_view core_revision = "eb9a99c5d3945108ab9d9d0d39622616d82f3586";
// memory, memory-hooks, memory-sync, GUI. Empty is unsupported, never wildcard.
inline constexpr std::array<std::string_view, 4> image_sha256{};
}
#endif
