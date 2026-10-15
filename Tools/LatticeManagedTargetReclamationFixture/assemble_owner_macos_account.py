#!/usr/bin/env python3
"""SOURCE ONLY: build/assemble a fresh real owner qualification account.

Load the exact reviewed private owner primitive definitions; no constructor
main or direct runtime exists in that selected helper. Normal native phases1/2/3/4 are
unchanged. No source authoring request runs this script. All first failures,
accounts, images and stores remain for owned disposable-VM teardown.
"""
import argparse
import grp
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import shutil
import shlex
import sqlite3
import stat
import sys
import types
import urllib.parse
import uuid


HELPERS_SHA256 = "086a6653d28cf8e001451337fae00947b68aae9dce7a4abc16f2403330b616a9"
# Exact DATA emitted by the real SwiftPM mirror commands after the preserved
# first resolution failure. Both upstream URL spellings map only to the
# required owned MCP fork; no global config or arbitrary remap is eligible.
OWNER_DEPENDENCY_CONFIG_SHA256 = "a76adb121f43bfaf2fd9e6eef2d45d1c7620f886c3eaed1ef0b3f4905ca1e349"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def local(path):
    if "localdev" not in path.parts:
        raise RuntimeError("Project/cache/build/source roots must be localdev-backed")
    return path


def _census_custody_metadata(observed):
    # Hashing may update access time. Retain every identity/permission field
    # from stat_result equality and strengthen mtime/ctime to nanoseconds.
    return (observed.st_dev, observed.st_ino, observed.st_uid, observed.st_gid,
            observed.st_mode, observed.st_nlink, observed.st_size,
            observed.st_mtime_ns, observed.st_ctime_ns)


def verify_census(root, seal, expected_format):
    if seal.get("format") != expected_format:
        raise RuntimeError("Exact source/cache seal format differs")
    pinned = {item["path"]: item for item in seal["files"]}
    files = sorted(p for p in root.rglob("*") if p.is_file())
    if (len(files) > 50000 or len(pinned) != len(seal["files"])
            or set(pinned) != {str(p.relative_to(root)) for p in files}):
        raise RuntimeError("Whole source/cache census differs from its exact seal")
    for path in root.rglob("*"):
        if stat.S_ISLNK(path.lstat().st_mode):
            raise RuntimeError("Sealed source/cache contains an alias")
    for path in files:
        fact = pinned[str(path.relative_to(root))]
        original = path.lstat()
        if (not stat.S_ISREG(original.st_mode) or original.st_nlink != 1 or original.st_size > 512 * 1024 * 1024
                or original.st_size != fact["bytes"] or sha(path) != fact["sha256"]
                or _census_custody_metadata(original) != _census_custody_metadata(path.lstat())):
            raise RuntimeError("Exact source/cache file differs")
    return pinned


def verify_dependency_config(root, seal):
    pinned = verify_census(root, seal, "owner-sealed-dependency-config-v1")
    if (set(pinned) != {"mirrors.json"}
            or pinned["mirrors.json"]["bytes"] != 330
            or pinned["mirrors.json"]["sha256"] != OWNER_DEPENDENCY_CONFIG_SHA256):
        raise RuntimeError("Only the exact generated owned-fork mirror DATA is eligible")
    return pinned

def qualification_work(requested):
    # A fresh direct child keeps configured Darwin scratch/socket paths short.
    # The installed SDK has sun_path[104]; reserve a slash, 48-byte leaf and NUL.
    root = Path(requested).absolute()
    if (root.parent != Path("/Users/jason/localdev")
            or not re.fullmatch("lq[0-9a-f]{8}", root.name)
            or root.resolve() != root):
        raise RuntimeError("Fresh exact /Users/jason/localdev/lq<8-hex> root without aliases required")
    work = root / "account-work"
    for leaf in ["scratch", "run-tmp"]:
        if len(os.fsencode(str(work / leaf))) + 1 + 48 + 1 > 104:
            raise RuntimeError("Configured scratch exceeds the fixed Darwin socket-path budget")
    return root, work


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ephemeral-ci-vm", action="store_true", required=True)
    parser.add_argument("--graph", type=Path, required=True)
    parser.add_argument("--graph-pins", type=Path, required=True)
    parser.add_argument("--graph-pins-sha256", required=True)
    parser.add_argument("--required-sources", type=Path, required=True)
    parser.add_argument("--required-sources-sha256", required=True)
    parser.add_argument("--owner-primitives-source", type=Path, required=True)
    parser.add_argument("--owner-package-resolved", type=Path, required=True)
    parser.add_argument("--owner-package-resolved-sha256", required=True)
    parser.add_argument("--dependency-cache", type=Path, required=True)
    parser.add_argument("--dependency-cache-pins", type=Path, required=True)
    parser.add_argument("--dependency-cache-pins-sha256", required=True)
    parser.add_argument("--dependency-config", type=Path, required=True)
    parser.add_argument("--dependency-config-pins", type=Path, required=True)
    parser.add_argument("--dependency-config-pins-sha256", required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--legacy-package", type=Path, required=True)
    parser.add_argument("--qualified-legacy-profile", type=Path, required=True)
    parser.add_argument("--qualified-legacy-profile-sha256", required=True)
    parser.add_argument("--provider", required=True)
    parser.add_argument("--provider-private-credentials", type=Path, required=True)
    parser.add_argument("--case", choices=["joined", "extra-entry-debt"], required=True)
    args = parser.parse_args()
    if not args.ephemeral_ci_vm or sys.platform != "darwin" or os.geteuid() != 0:
        raise RuntimeError("Only the explicit disposable macOS VM/root account-creation lane is eligible")
    source = local(args.graph.resolve(strict=True))
    root, work = qualification_work(args.root)
    if (not root.is_relative_to(Path("/Users/jason/localdev"))
            or root.exists() or root.is_symlink() or (source / ".git").exists()):
        raise RuntimeError("Fresh assembly root and sealed plain source graph required; no reuse or reset")
    dependency_cache = local(args.dependency_cache.resolve(strict=True))
    cache_pins_path = local(args.dependency_cache_pins.resolve(strict=True))
    dependency_config = local(args.dependency_config.absolute())
    config_pins_path = local(args.dependency_config_pins.absolute())
    if (dependency_config.resolve(strict=True) != dependency_config
            or config_pins_path.resolve(strict=True) != config_pins_path):
        raise RuntimeError("Exact dependency config roots and seal must have no aliases")
    required_path = local(args.required_sources.resolve(strict=True))
    graph_pins_path = local(args.graph_pins.resolve(strict=True))
    resolved_path = local(args.owner_package_resolved.resolve(strict=True))
    for path, pin in [(required_path, args.required_sources_sha256),
                      (graph_pins_path, args.graph_pins_sha256),
                      (cache_pins_path, args.dependency_cache_pins_sha256),
                      (config_pins_path, args.dependency_config_pins_sha256),
                      (resolved_path, args.owner_package_resolved_sha256)]:
        if not re.fullmatch("[0-9a-f]{64}", pin) or sha(path) != pin:
            raise RuntimeError("Exact dependency/source graph input changed")
    required = json.loads(required_path.read_text())
    sealed = json.loads(graph_pins_path.read_text())
    if (sealed.get("ownerCheckpointDeltaSHA256") != required["owner_source_patch"]
            or sealed.get("constructorGraphSealSHA256") != required["core_sparse_union_required"]["constructor_graph_seal"]):
        raise RuntimeError("Following owner graph must record the accepted exact sparse union and newer constructor graph")
    pinned = verify_census(source, sealed, "owner-sealed-source-graph-v1")
    cache_seal = json.loads(cache_pins_path.read_text())
    verify_census(dependency_cache, cache_seal, "owner-sealed-dependency-cache-v1")
    config_seal = json.loads(config_pins_path.read_text())
    verify_dependency_config(dependency_config, config_seal)
    for item in required["graph_required"]:
        if item["disposition"] == "REVIEWED_SPARSE_CORE_DELTA_REFERENCE_REQUIRES_NEWER_GRAPH_UNION_NOT_WHOLE_LEAF_OVERLAY":
            # The entire newer graph is separately sealed/reviewed above.
            # NEVER replace its ecf witness/SQL/current guards with old leaves.
            if item["path"] not in pinned:
                raise RuntimeError("Following sparse Core union lacks an actual required path")
            continue
        if pinned.get(item["path"], {}).get("sha256") != item["sha256"]:
            raise RuntimeError("Required authenticated owner/SDK/Engram/provider source input missing: " + item["path"])
    provider = urllib.parse.urlsplit(args.provider)
    if (provider.scheme != "https" or provider.hostname not in ("localhost", "127.0.0.1")
            or provider.username or provider.password or provider.path not in ("", "/") or provider.query or provider.fragment):
        raise RuntimeError("Only the actual trusted disposable loopback HTTPS provider is eligible")
    helper_path = local(args.owner_primitives_source.resolve(strict=True))
    helper_body = helper_path.read_bytes()
    if hashlib.sha256(helper_body).hexdigest() != HELPERS_SHA256:
        raise RuntimeError("Exact private owner primitive implementation differs")
    # Execute definitions only in this FUTURE invocation. The selected private
    # helper has no main, constructor fixture or phase5/6/7 invocation.
    helpers = types.ModuleType("owner_qualification_frozen_primitives")
    helpers.__dict__["__name__"] = "owner_qualification_frozen_primitives"
    exec(compile(helper_body, str(helper_path), "exec"), helpers.__dict__)
    mkdir, write, copy = helpers.mkdir, helpers.write, helpers.copy
    canonical, sync, directory_fact = helpers.canonical, helpers.sync, helpers.directory_fact
    legacy = args.legacy_package.resolve(strict=True)
    profile = args.qualified_legacy_profile.resolve(strict=True)
    if sha(profile) != args.qualified_legacy_profile_sha256:
        raise RuntimeError("Separately reviewed actual legacy profile differs")
    profile_text = profile.read_text()
    for pin in ["engram-v0.14.10-darwin-qualified-spawn-v1", "5e0862b6af90578a0e2899bd379fb2b9be9f9703",
                "0461b210aa028f3c4bb6165fed895e0f4385b565", "eb9a99c5d3945108ab9d9d0d39622616d82f3586"]:
        if pin not in profile_text:
            raise RuntimeError("Actual fixed legacy qualification pin is missing")
    for leaf in ["memory", "memory-hooks", "memory-sync"]:
        if helpers.digest(legacy / leaf) not in profile_text:
            raise RuntimeError("Actual legacy image differs from the reviewed profile")
    credential_stat = args.provider_private_credentials.lstat()
    if (not stat.S_ISREG(credential_stat.st_mode) or credential_stat.st_mode & 0o077
            or credential_stat.st_size > 16384):
        raise RuntimeError("Actual disposable provider credential file must be private and regular")
    credential_bytes = args.provider_private_credentials.read_bytes()

    # Same reviewed OS-account observations; a distinct name prevents reuse
    # of the direct-constructor account and makes the installed caller refuse
    # every ordinary user account. OS-required real home is not overridden.
    os.mkdir(root, 0o711)
    mkdir(work, 0, 0)
    scratch = work / "scratch"
    mkdir(scratch, 0, 0)
    def run(argv, user=None, cwd=None, timeout=600):
        return helpers.run(argv, user=user, cwd=work if cwd is None else cwd,
                           timeout=timeout, scratch=scratch)
    name = "lattice-owner-" + uuid.uuid4().hex[:8]
    occupied = {int(line.split()[-1]) for line in run(["/usr/bin/dscl", ".", "-list", "/Users", "UniqueID"], timeout=10).splitlines()
                if line.split() and line.split()[-1].isdigit()}
    uid = next(value for value in range(600, 4096) if value not in occupied)
    gid = grp.getgrnam("staff").gr_gid
    home = Path("/Users") / name
    if home.exists() or home.is_symlink():
        raise RuntimeError("Existing account home must never be reused")
    parent = directory_fact(home.parent)
    if parent["uid"] != 0 or parent["mode"] & 0o022:
        raise RuntimeError("Real account home parent fails unchanged native policy")
    mkdir(home, uid, gid)
    try:
        pwd.getpwnam(name)
    except KeyError:
        pass
    else:
        raise RuntimeError("Actual account-name collision")
    node = "/Users/" + name
    run(["/usr/bin/dscl", ".", "-create", node], timeout=10)
    for field, value in [("UniqueID", uid), ("PrimaryGroupID", gid), ("NFSHomeDirectory", home),
                         ("UserShell", "/bin/zsh"), ("RealName", "Lattice owner qualification"), ("Password", "*")]:
        run(["/usr/bin/dscl", ".", "-create", node, field, value], timeout=10)
    observed = pwd.getpwnam(name)
    if observed.pw_uid != uid or Path(observed.pw_dir) != home:
        raise RuntimeError("Real account observation differs from actual creation")
    # Fresh project work remains in Jason's requested localdev root. The actual
    # OS home and required .claude/.engram-installation state remain unchanged.
    os.chown(work, uid, gid); os.chown(scratch, uid, gid)
    for parent_path in [root, *root.parents]:
        item = parent_path.lstat()
        if not stat.S_ISDIR(item.st_mode) or stat.S_ISLNK(item.st_mode):
            raise RuntimeError("Actual work parent must be a directory without aliases")
        search = (item.st_mode & 0o100 if item.st_uid == uid else
                  item.st_mode & 0o010 if item.st_gid == gid else item.st_mode & 0o001)
        if not search:
            raise RuntimeError("Actual fresh account cannot search work parent; no permission rewrite")
    account = canonical(dict(account=name, uid=uid, actualHome=observed.pw_dir, actualWorkRoot=str(work), homeOverride=False,
                             caseData=args.case, disposition="PRESERVE_ACCOUNT_AND_FIRST_FAILURES"))
    write(work / "ACCOUNT.json", account, uid, gid); write(root / "ACCOUNT.json", account, 0, 0)
    graph = work / "graph"
    shutil.copytree(source, graph, symlinks=False)
    for path in [graph, *graph.rglob("*")]:
        os.chown(path, uid, gid)
    verify_census(graph, sealed, "owner-sealed-source-graph-v1")
    # Only this new, verified account work copy needs private write access
    # for user-scoped Git/SwiftPM. Original inputs and file modes stay intact.
    for path in [graph, *graph.rglob("*")]:
        if path.is_dir():
            os.chmod(path, 0o700)

    # Exact recorded local dependency rewrites; SDK Core/bridge and Engram
    # cannot accidentally resolve to published libraries without this fixture.
    rewrites = [
        ("SDK/Package.swift", '.package(url: "https://github.com/jsflax/LatticeCore.git", from: "2.0.4")',
         '.package(name: "LatticeCore", path: "../Core")'),
        ("Engram/Package.swift", '.package(url: "https://github.com/jsflax/lattice.git", from: "1.6.2")',
         '.package(name: "lattice", path: "../SDK")'),
        ("Engram/Package.swift", '.package(url: "https://github.com/modelcontextprotocol/swift-sdk.git", from: "0.12.0")',
         '.package(url: "https://github.com/jsflax/swift-sdk.git", exact: "0.13.0")')]
    rewrite_receipt = []
    for relative, old, new in rewrites:
        path = graph / relative
        before = path.read_text()
        if before.count(old) != 1:
            raise RuntimeError("Exact manifest preimage differs; no guessed dependency override")
        path.write_text(before.replace(old, new, 1))
        rewrite_receipt.append(dict(path=relative, beforeSHA256=hashlib.sha256(before.encode()).hexdigest(), afterSHA256=sha(path)))
    # Bind ONLY qualification DATA locations into both real Swift source images
    # before the source revision/build/sign. This path grants no native authority.
    marker = '"__LATTICE_OWNER_QUALIFICATION_WORK_ROOT__"'
    bound = json.dumps(str(work), ensure_ascii=False)
    if not str(work).isascii() or any(ord(c) < 32 or ord(c) == 127 for c in str(work)):
        raise RuntimeError("Exact qualification work path must have an ASCII Swift literal")
    for relative in ["OwnerImages/Sources/OwnerImage/main.swift", "OwnerImages/Sources/PairedImage/main.swift"]:
        path = graph / relative
        before = path.read_text()
        if before.count(marker) != 1:
            raise RuntimeError("Exact signed-image work-root source preimage differs")
        path.write_text(before.replace(marker, bound, 1))
        rewrite_receipt.append(dict(path=relative, beforeSHA256=hashlib.sha256(before.encode()).hexdigest(),
                                   afterSHA256=sha(path), boundQualificationDataWorkRoot=str(work)))
    write(work / "DEPENDENCY-REWRITES.json", canonical(rewrite_receipt), uid, gid)
    # Lock/cache are explicit qualified build inputs, not a floating resolver.
    owner_lock = graph / "OwnerImages/Package.resolved"
    if owner_lock.exists():
        if sha(owner_lock) != args.owner_package_resolved_sha256:
            raise RuntimeError("Existing sealed owner lock differs; no replacement or resolver refresh")
    else:
        copy(resolved_path, owner_lock, uid, gid, 0o600)
    cache = work / "swift-cache"
    shutil.copytree(dependency_cache, cache, symlinks=False)
    for path in [cache, *cache.rglob("*")]:
        os.chown(path, uid, gid)
    verify_census(cache, cache_seal, "owner-sealed-dependency-cache-v1")
    configuration = work / "swift-config"
    shutil.copytree(dependency_config, configuration, symlinks=False)
    for path in [configuration, *configuration.rglob("*")]:
        os.chown(path, uid, gid)
    verify_dependency_config(configuration, config_seal)
    run(["/usr/bin/git", "init"], user=name, cwd=graph, timeout=10)
    run(["/usr/bin/git", "add", "."], user=name, cwd=graph, timeout=60)
    run(["/usr/bin/git", "-c", "user.name=Lattice Fixture", "-c", "user.email=fixture@localhost", "commit", "-m", "Frozen authenticated owner fixture source"], user=name, cwd=graph, timeout=60)
    revision = run(["/usr/bin/git", "rev-parse", "HEAD"], user=name, cwd=graph, timeout=10).strip()
    if not re.fullmatch("[0-9a-f]{40}", revision):
        raise RuntimeError("Actual source graph revision observation refused")
    swift = shutil.which("swift"); cmake = shutil.which("cmake")
    if not swift or not cmake:
        raise RuntimeError("Required disposable VM Swift/CMake toolchain unavailable")
    macros = ["LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE"]
    if args.case == "extra-entry-debt":
        macros.append("LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_EXTRA_ENTRY")
    swift_command = ["/usr/bin/env", "LATTICE_RECLAMATION_COMPOSED_ENGRAM_SOURCE=" + str(graph / "Engram"),
        swift, "build", "--package-path", graph / "OwnerImages", "--scratch-path", work / "owner-swift-build",
        "--cache-path", cache, "--config-path", configuration, "--configuration", "debug",
        "--disable-automatic-resolution", "--skip-update"]
    module_cache = scratch / "clang-module-cache"
    mkdir(module_cache, uid, gid)
    swift_command += ["-Xswiftc", "-module-cache-path", "-Xswiftc", module_cache,
                      "-Xcc", "-fmodules-cache-path=" + str(module_cache),
                      "-Xcxx", "-fmodules-cache-path=" + str(module_cache)]
    for macro in macros:
        swift_command += ["-Xswiftc", "-D" + macro, "-Xcxx", "-D" + macro]
    run(swift_command, user=name, cwd=work)
    binary_path = run([*swift_command, "--show-bin-path"], user=name, cwd=work, timeout=30).strip()
    binary_directory = Path(binary_path).resolve(strict=True)
    if not binary_directory.is_relative_to(work):
        raise RuntimeError("Actual Swift build output must remain in the exact Jason-localdev account work")
    built_memory, built_daemon = [binary_directory / leaf for leaf in ["memory", "memory-sync"]]
    for image in [built_memory, built_daemon]:
        run(["/usr/bin/codesign", "--force", "--sign", "-", "--timestamp=none", image], user=name, cwd=work, timeout=30)
    memory_hash, daemon_hash = [helpers.digest(image) for image in [built_memory, built_daemon]]
    if memory_hash == daemon_hash:
        raise RuntimeError("Distinct actual normal paired/SDK owner images required; no native direct runtime substitute")
    generated = work / "generated"; mkdir(generated, uid, gid)
    array = lambda value: ",".join("0x" + value[i:i+2] for i in range(0, len(value), 2))
    header = ('#pragma once\n#include <array>\n#include <cstdint>\n#include <string_view>\n'
              'namespace lattice::detail::ordinary_installation::engram_build_stamp {\n'
              'inline constexpr bool available=true;\n'
              'inline constexpr std::string_view product="engram",initializer_leaf="memory";\n'
              'inline constexpr std::string_view source_revision="' + revision + '",core_revision="' + revision + '";\n'
              'inline constexpr std::array<std::uint8_t,32> initializer_sha256{' + array(memory_hash) + '};\n}\n')
    write(generated / "build.hpp", header.encode(), uid, gid)
    header = ('#pragma once\n#include <array>\n#include <cstdint>\n'
              'namespace lattice::detail::ordinary_installation::engram_managed_pair_stamp {\n'
              'inline constexpr bool available=true;\n'
              'inline constexpr std::array<std::uint8_t,32> daemon_sha256{' + array(daemon_hash) + '};\n}\n')
    write(generated / "pair.hpp", header.encode(), uid, gid)
    copy(profile, generated / "legacy.hpp", uid, gid, 0o600)
    build = work / "controller-build"
    run([cmake, "-S", graph / "Core", "-B", build, "-DCMAKE_BUILD_TYPE=Debug",
         "-DLATTICE_MANAGED_TARGET_RECLAMATION_FIXTURE=OFF",
         "-DLATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE=ON",
         "-DCMAKE_CXX_FLAGS=" + " ".join("-D" + m for m in macros) +
         " -fmodules-cache-path=" + shlex.quote(str(module_cache)),
         "-DLATTICE_RECLAMATION_BUILD_STAMP_HEADER=" + str(generated / "build.hpp"),
         "-DLATTICE_RECLAMATION_MANAGED_PAIR_STAMP_HEADER=" + str(generated / "pair.hpp"),
         "-DLATTICE_RECLAMATION_LEGACY_PROFILE_HEADER=" + str(generated / "legacy.hpp")], user=name, cwd=work)
    run([cmake, "--build", build, "--target", "LatticeInstallationLauncher", "--parallel", "2"], user=name, cwd=work)
    launcher = build / "LatticeInstallationLauncher"
    run(["/usr/bin/codesign", "--force", "--sign", "-", "--timestamp=none", launcher], user=name, cwd=work, timeout=30)

    credential = work / "provider-credentials.json"
    write(credential, credential_bytes, uid, gid)
    qualification = work / "owner-qualification"
    run([sys.executable, graph / "FixtureScripts/provider_setup.py", "--endpoint", args.provider,
         "--private-credentials", credential, "--output", qualification], user=name, cwd=work, timeout=90)
    metadata = json.loads((qualification / "PROVIDER.json").read_text())
    group = str(uuid.UUID(metadata["group"]))
    if (metadata["legacy_group_leaf"] != "group-" + group + ".sqlite"
            or metadata["unsubscribed_guard_status"] != 402 or metadata["active_guard_status"] != 200):
        raise RuntimeError("Actual real provider group/guard observations differ")
    data = home / ".claude"; mkdir(data, uid, gid)
    old_bin = data / "bin"; mkdir(old_bin, uid, gid)
    for leaf in ["memory", "memory-hooks", "memory-sync"]:
        copy(legacy / leaf, old_bin / leaf, uid, gid)
    old_sync = data / "sync"; mkdir(old_sync, uid, gid)
    # The primary inert physical store is never used as GROUP declarations or
    # application/history authority. The GROUP is actual declaration-backed.
    primary = data / "memory.sqlite"
    connection = sqlite3.connect(primary)
    try:
        connection.execute("CREATE TABLE fixture_history(value TEXT)"); connection.commit()
    finally:
        connection.close()
    os.chown(primary, uid, gid); os.chmod(primary, 0o600)
    group_path = old_sync / ("group-" + group + ".sqlite")
    run([built_daemon, "--seed-owned-group", group_path], user=name, cwd=work, timeout=30)
    os.chmod(group_path, 0o600)
    for store in [primary, group_path]:
        with open(store, "rb") as stream:
            os.fsync(stream.fileno())
    sync(old_sync); sync(data)
    state = home / ".engram-installation"; mkdir(state, uid, gid)
    stage = state / "upgrade-bootstrap.v1"; mkdir(stage, uid, gid)
    controller, configs, images = [stage / leaf for leaf in ["controller", "original-configurations", "managed-runtime.v2"]]
    for path in [controller, configs, images]:
        mkdir(path, uid, gid)
    installed_launcher = copy(launcher, controller / "memory-installation-launcher", uid, gid)
    memory = copy(built_memory, images / "memory", uid, gid)
    daemon = copy(built_daemon, images / "memory-sync", uid, gid)
    provenance = (b"LATPKG1\0" + (1).to_bytes(8, "little") * 2 + bytes.fromhex(revision) * 2 +
                  bytes.fromhex(memory["sha256"]) + bytes.fromhex(installed_launcher["sha256"]))
    paired = (b"LATMRT2\0" + (2).to_bytes(8, "little") * 2 + bytes.fromhex(revision) * 2 +
              bytes.fromhex(memory["sha256"]) + bytes.fromhex(daemon["sha256"]) + bytes.fromhex(installed_launcher["sha256"]))
    if len(provenance) != 128 or len(paired) != 160:
        raise RuntimeError("Actual normal native provenance encoding refused")
    write(images / "image.json", canonical(dict(format="engram-managed-runtime-image-v2", accountUID=uid,
          provenanceHex=provenance.hex(), pairedProvenanceHex=paired.hex(), executable=memory, daemonExecutable=daemon)), uid, gid)
    anchor = dict(format="engram-global-applications-anchor-v1", root=directory_fact(Path("/")), present=Path("/Applications").exists())
    if anchor["present"]:
        actual = directory_fact(Path("/Applications")); anchor["original"] = actual
        anchor["adminGID"] = grp.getgrnam("admin").gr_gid if actual["mode"] & 0o7777 == 0o775 else None
    guis = [dict(path=str(path), present=False, absentComponent=helpers.first_absent(path)) for path in
        [Path("/Applications/Engram.app/Contents/MacOS/Engram"), home / "Applications/Engram.app/Contents/MacOS/Engram"]]
    policies = [dict(path=relative, present=False, absentComponent=str(Path(helpers.first_absent(home / relative)).relative_to(home)))
        for relative in [".claude.json", ".claude/settings.json", ".codex/config.toml", "Library/LaunchAgents/io.engram.sync.plist", "Library/LaunchAgents/io.engram.sync-daemon.plist"]]
    bootstrap = dict(format="engram-upgrade-bootstrap-v1", accountUID=uid, dataDirectory=directory_fact(data),
        legacyBuildProvenanceObserved=False, coveredCachedAliases=False, wholeCohortRetired=False, historicalAdoptionAuthorized=False,
        stagedController=installed_launcher, stagedProvenanceHex=provenance.hex(),
        originalInstalledImages=[dict(leaf=leaf, **helpers.file_fact(old_bin / leaf)) for leaf in ["memory", "memory-hooks", "memory-sync"]],
        originalInstalledGUIImages=guis, globalApplicationsAnchor=anchor, originalLaunchConfigurations=policies)
    raw = canonical(bootstrap)
    write(stage / "bootstrap.json", raw, uid, gid); write(stage / "bootstrap.sha256", (hashlib.sha256(raw).hexdigest() + "\n").encode(), uid, gid)
    write(stage / "lock", b"", uid, gid)
    for path in [controller, configs, images, stage, state, home]:
        sync(path)
    # This assembly receipt is DATA; no READY, admission, already-closed/native
    # success field exists. A separate ONE normal controller run is required.
    receipt = canonical(dict(format="owner-image-account-assembly-v1", account=name, uid=uid,
        sourceRevision=revision, actualWorkRoot=str(work), sourceGraphSealSHA256=args.graph_pins_sha256,
        dependencyCacheSealSHA256=args.dependency_cache_pins_sha256, ownerResolvedSHA256=args.owner_package_resolved_sha256,
        qualificationCaseData=args.case, memorySHA256=memory_hash, daemonSHA256=daemon_hash,
        controllerSHA256=helpers.digest(launcher), legacyProfileSHA256=sha(profile),
        actualProviderGroup=group, launchPerformed=False, ownerQualified=False, wholeCohortRetirementQualified=False,
        futureNormalCommand=[sys.executable, str(graph / "FixtureScripts/launch_installed_owner.py"), "--ephemeral-ci-vm",
                             "--work-root", str(work), "--expect", args.case],
        disposition="RETAIN_ACCOUNT_IMAGES_STORES_AND_FIRST_FAILURES"))
    write(work / "OWNER-ASSEMBLY.json", receipt, uid, gid); write(root / "OWNER-ASSEMBLY.json", receipt, 0, 0)
    print(str(work / "OWNER-ASSEMBLY.json"))


if __name__ == "__main__":
    main()
