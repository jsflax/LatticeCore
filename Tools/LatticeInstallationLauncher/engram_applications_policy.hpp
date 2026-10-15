#pragma once
#include <sys/stat.h>
#include <optional>

namespace lattice::detail::ordinary_installation::applications_policy {
// Metadata predicates only; these values cannot issue namespace/process
// custody. The sole production caller separately proves the actual / parent
// and opens the fixed Applications name with O_NOFOLLOW.
inline bool strict_directory(const struct stat& value, uid_t account) {
    return S_ISDIR(value.st_mode) && (value.st_uid == 0 || value.st_uid == account)
        && !(value.st_mode & 0022);
}
inline bool directory(const struct stat& value, uid_t account, std::optional<gid_t> admin) {
    if (strict_directory(value, account)) return true;
    return admin && S_ISDIR(value.st_mode) && value.st_uid == 0
        && value.st_gid == *admin && (value.st_mode & 07777) == 0775;
}
inline bool unchanged(const struct stat& original, const struct stat& now) {
    return S_ISDIR(original.st_mode) && S_ISDIR(now.st_mode)
        && original.st_dev == now.st_dev && original.st_ino == now.st_ino
        && original.st_uid == now.st_uid && original.st_gid == now.st_gid
        && original.st_mode == now.st_mode;
}
}
