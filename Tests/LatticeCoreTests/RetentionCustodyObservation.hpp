#pragma once

#include <lattice/db.hpp>
#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>
#if defined(__APPLE__) || defined(__linux__)
#include <sys/stat.h>

namespace retention_fixture_observation {
// Path inspection only: never acquire an independent fd for the SQLite inode.
struct identity {
    dev_t device;
    ino_t inode;
    mode_t mode;
    uid_t owner;
    bool operator==(const identity&) const = default;
};
inline struct stat inspect(const std::filesystem::path& path) {
    struct stat value{};
    if(::lstat(path.c_str(),&value)!=0)throw std::runtime_error("fixture restored identity unavailable");
    return value;
}
inline identity identify(const struct stat& value) {
    return {value.st_dev,value.st_ino,value.st_mode,value.st_uid};
}
struct physical_binding {
    std::filesystem::path main_path,parent_path,custody_path;
    identity main,parent,custody;
    static physical_binding capture(const std::filesystem::path& path) {
        const auto main_path=std::filesystem::canonical(path),parent_path=main_path.parent_path();
        const auto main=inspect(main_path);
        const auto custody_path=parent_path/(".lattice-retention-"+
            std::to_string(static_cast<uint64_t>(main.st_dev))+"-"+std::to_string(static_cast<uint64_t>(main.st_ino)));
        const auto parent=inspect(parent_path),custody=inspect(custody_path);
        if(!S_ISREG(main.st_mode)||main.st_nlink!=1||!S_ISDIR(parent.st_mode)||!S_ISDIR(custody.st_mode)||
           (custody.st_mode&07777)!=0700)
            throw std::runtime_error("fixture initial file binding is malformed");
        return {main_path,parent_path,custody_path,identify(main),identify(parent),identify(custody)};
    }
    void require_restored() const {
        const auto current=inspect(main_path);
        if(identify(current)!=main||current.st_nlink!=1||identify(inspect(parent_path))!=parent||
           identify(inspect(custody_path))!=custody)
            throw std::runtime_error("fixture did not restore exact main, parent and custody identities");
    }
};
struct retained_rows {
    std::vector<lattice::database::row_t> profile,attempts;
};
inline retained_rows read(lattice::database& observer) {
    return {observer.query("SELECT * FROM main._lattice_canonical_retention"),
            observer.query("SELECT * FROM main._lattice_canonical_attempt")};
}
inline retained_rows fresh_read(const physical_binding& binding) {
    binding.require_restored();
    // Plain READONLY joins the existing WAL. No lattice owner initialization,
    // canonical attachment, migration, checkpoint or immutable URI is involved.
    lattice::database observer(binding.main_path.string(),lattice::database::open_mode::read_only,100);
    observer.execute("BEGIN");
    auto rows=read(observer);
    observer.execute("COMMIT");
    binding.require_restored();
    return rows;
}
} // namespace retention_fixture_observation
#endif
