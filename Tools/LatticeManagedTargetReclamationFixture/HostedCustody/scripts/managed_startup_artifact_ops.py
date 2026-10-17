#!/usr/bin/env python3
"""Fixed hosted artifact steps for the dedicated-account managed startup job.

This helper runs only as that actual non-admin account and creates no authority
record. The existing authorized publisher remains a separate actual command.
All executable children here are fixed read-only Git commands in the guardian's
owned group. Receipts describe bytes; none can enroll or admit an installation.
"""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import shutil
import stat
import subprocess
import sys

from managed_startup_qualification import (read_regular, parse_json, authenticate_source_bytes,
    validate_private_inputs, graph_nodes, url_key, launcher_compiler_inputs, MAXIMUM_METADATA)
from generate_installation_build_stamp import require, signed_input_facts

BUNDLES = ('Engram_EngramKit.bundle', 'swift-transformers_Hub.bundle',
           'SwiftLM_SwiftLM.bundle', 'swift-crypto_Crypto.bundle')


def git(root, *args):
    return subprocess.run(['git', '-c', 'core.fsmonitor=false', '-c', 'core.hooksPath=/dev/null',
        '-C', str(root), *args], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, timeout=60).stdout.strip()


def write_new(path, value):
    raw = (json.dumps(value, indent=2, sort_keys=True) + '\n').encode()
    require(len(raw) <= 16 * 1024 * 1024, 'receipt exceeds fixed bound')
    with path.open('xb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def digest(path, maximum=1024 * 1024 * 1024):
    require(path.resolve(strict=True) == path and path.is_absolute(), 'canonical artifact required')
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1
                and before.st_uid == os.geteuid() and before.st_size <= maximum, 'invalid artifact')
        def identity(v):
            return (v.st_dev, v.st_ino, v.st_mode, v.st_uid, v.st_nlink,
                    v.st_size, v.st_mtime_ns, v.st_ctime_ns)
        require(identity(before) == identity(path.lstat()), 'artifact naming changed')
        result, count = hashlib.sha256(), 0
        while count < before.st_size:
            part = os.read(fd, min(65536, before.st_size - count))
            require(part, 'artifact truncated')
            count += len(part)
            result.update(part)
        require(not os.read(fd, 1) and identity(before) == identity(os.fstat(fd))
                == identity(path.lstat()), 'artifact changed')
        return {'bytes': count, 'sha256': result.hexdigest(), 'mode': stat.S_IMODE(before.st_mode),
                'device': before.st_dev, 'inode': before.st_ino}
    finally:
        os.close(fd)


def copy_file(source, destination):
    before = digest(source)
    require(not destination.exists() and not destination.is_symlink(), 'copy destination already exists')
    # Copies have their own inode. Hardlinks cannot become artifact provenance.
    with source.open('rb') as src, destination.open('xb') as dst:
        remaining = before['bytes']
        while remaining:
            block = src.read(min(65536, remaining))
            require(block, 'copy source truncated')
            dst.write(block)
            remaining -= len(block)
        require(not src.read(1), 'copy source grew')
        dst.flush()
        os.fsync(dst.fileno())
    os.chmod(destination, before['mode'] & 0o755)
    after, copied = digest(source), digest(destination)
    require(before == after and before['bytes'] == copied['bytes']
            and before['sha256'] == copied['sha256'], 'copy bytes changed')
    return {'source': str(source), 'destination': str(destination), 'before': before, 'copied': copied}


def copy_tree(source, destination):
    require(source.resolve(strict=True) == source and source.is_dir() and not destination.exists(),
            'new canonical bundle copy required')
    destination.mkdir(mode=0o700)
    rows, total, entries = [], 0, 0
    for parent, directories, files in os.walk(source, followlinks=False):
        relative = Path(parent).relative_to(source)
        entries += len(directories) + len(files)
        require(len(relative.parts) <= 32 and entries <= 65536,
                'resource graph exceeds bound')
        for name in sorted(directories):
            child = Path(parent) / name
            require(not child.is_symlink() and child.is_dir(), 'resource directory alias refused')
            (destination / relative / name).mkdir(mode=0o700)
        for name in sorted(files):
            child = Path(parent) / name
            row = copy_file(child, destination / relative / name)
            total += row['before']['bytes']
            require(total <= 2 * 1024 * 1024 * 1024, 'resource bytes exceed bound')
            rows.append(row)
    return rows


def prebuild_graph(root, workspace, profile, private, receipts):
    """Check the exact graph before any compiler; no decoded grant is created."""
    require(set(profile) == {'schemaVersion', 'scope', 'core', 'sdk', 'publicEngramResolvedSHA256',
        'sdkResolvedSHA256', 'supplementalPins', 'runtimeAuthority'} and profile['schemaVersion'] == 1
        and profile['runtimeAuthority'] is False and profile['scope'] == 'private-new-origin-primary-mcp-qualification',
        'exact private profile required')
    original = read_regular(private / 'original-Package.resolved', MAXIMUM_METADATA)
    mirror_original = read_regular(private / 'original-mirrors.json', MAXIMUM_METADATA)
    require(hashlib.sha256(original).hexdigest() == profile['publicEngramResolvedSHA256'], 'original lock changed')
    resolved = parse_json(original)
    entries = resolved['pins'] + profile['supplementalPins']
    pins = {row['identity']: row for row in entries}
    require(len(pins) == len(entries) == 48, 'complete private pin set required')
    prepared = dict(resolved); prepared['pins'] = entries
    require(parse_json(read_regular(root / 'Package.resolved', MAXIMUM_METADATA)) == prepared,
            'working lock differs from exact supplemental graph')
    port = os.environ['ENGRAM_MANAGED_MIRROR_PORT']
    require(port.isdecimal() and 0 < int(port) < 65536, 'actual mirror port required')
    mirror = parse_json(read_regular(root / '.swiftpm/configuration/mirrors.json', MAXIMUM_METADATA))
    old_mirror = parse_json(mirror_original)
    expected_rows = old_mirror['object'] + [{'original': 'https://github.com/sparkle-project/Sparkle/releases/download/2.8.1/Sparkle-for-Swift-Package-Manager.zip',
        'mirror': 'http://127.0.0.1:' + port + '/Sparkle-for-Swift-Package-Manager.zip'}]
    require(set(mirror) == {'object', 'version'} and mirror['version'] == old_mirror['version'] == 1
        and sorted(mirror['object'], key=lambda x: x['original']) == sorted(expected_rows, key=lambda x: x['original']),
        'working mirror differs from the exact temporary addition')
    # Only these two fully checked metadata mutations may differ. Every source
    # byte about to compile must match its actual frozen Git blob.
    replacements = {'Package.resolved': original, '.swiftpm/configuration/mirrors.json': mirror_original}
    physical = hashlib.sha256()
    for record in filter(None, git(root, 'ls-tree', '-rz', 'HEAD').split('\0')):
        metadata, name = record.split('\t', 1); mode, kind, oid = metadata.split()
        require(kind == 'blob' and mode in {'100644', '100755', '120000'}, 'unsupported source entry')
        path = root / name
        raw = replacements.get(name)
        if raw is None:
            raw = os.fsencode(os.readlink(path)) if mode == '120000' else read_regular(path, 64 * 1024 * 1024, allow_empty=True)
        require(hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest() == oid,
                'physical compile source differs from exact frozen source')
        physical.update(record.encode() + b'\0' + hashlib.sha256(raw).digest())
    graph_raw = read_regular(receipts / 'effective-graph-before.json', MAXIMUM_METADATA)
    graph = parse_json(graph_raw)
    require(graph.get('path') == str(root) and graph.get('identity', graph.get('name', '')).lower() == 'engram',
            'graph belongs to another root')
    nodes = graph_nodes(graph)
    state = parse_json(read_regular(root / '.build/workspace-state.json', MAXIMUM_METADATA))['object']['dependencies']
    states = {entry['packageRef']['identity']: entry for entry in state}
    require(len(states) == len(state) and set(states) == set(nodes) == set(pins), 'graph membership mismatch')
    overrides = {'lattice': workspace / 'lattice', 'latticecore': workspace / 'LatticeCore'}
    rows, physical_sources = [], {'engram': physical.hexdigest()}
    for identity in sorted(pins):
        pin, node, entry = pins[identity], nodes[identity], states[identity]
        path = Path(node['path'])
        require(path.is_absolute() and path.resolve(strict=True) == path, 'dependency path not canonical')
        require(url_key(entry['packageRef']['location']) == url_key(pin['location']), 'dependency origin differs')
        if identity in overrides:
            key = 'sdk' if identity == 'lattice' else 'core'; binding = profile[key]
            require(set(binding) == {'commit', 'tree'} and all(type(v) is str and re.fullmatch('[0-9a-f]{40}', v)
                for v in binding.values()), 'invalid private binding')
            require(path == overrides[identity] and entry['state']['name'] == 'edited'
                and git(path, 'rev-parse', 'HEAD', 'HEAD^{tree}').splitlines() == [binding['commit'], binding['tree']],
                'private source binding mismatch')
            revision = binding['commit']; physical_sources[identity] = authenticate_source_bytes(path, git)
        else:
            relative = Path(entry['subpath']); checkout = entry['state']['checkoutState']
            require(not relative.is_absolute() and '..' not in relative.parts
                and set(checkout) <= {'revision', 'version', 'branch'}
                and all(checkout.get(k) == pin['state'].get(k) for k in ('revision', 'version', 'branch')),
                'non-private revision differs')
            require(path == root / '.build/checkouts' / relative and entry['state']['name'] == 'sourceControlCheckout'
                and url_key(node['url']) == url_key(pin['location']), 'non-private dependency changed')
            revision = pin['state']['revision']
        require(git(path, 'rev-parse', 'HEAD') == revision and not git(path, 'status', '--porcelain', '--untracked-files=all'),
                'dependency not exact clean source')
        rows.append({'identity': identity, 'revision': revision, 'path': str(path)})
    require(hashlib.sha256(read_regular(overrides['lattice'] / 'Package.resolved', MAXIMUM_METADATA)).hexdigest()
        == profile['sdkResolvedSHA256'], 'SDK public lock differs')
    binding = parse_json(read_regular(overrides['lattice'] / 'Scripts/development-core.json', MAXIMUM_METADATA))
    require(binding['coreCommit'] == profile['core']['commit'] and binding['coreTree'] == profile['core']['tree'],
            'SDK private Core binding differs')
    write_new(private / 'prebuild-workspace-dependencies.json', state)
    return {'effectiveDependencies': rows, 'physicalSourceManifests': physical_sources,
            'effectiveGraphSHA256': hashlib.sha256(graph_raw).hexdigest()}


def main():
    require(sys.platform == 'darwin' and os.geteuid() != 0
            and os.environ.get('GITHUB_ACTIONS') == 'true'
            and os.environ.get('RUNNER_ENVIRONMENT') == 'github-hosted'
            and os.environ.get('ENGRAM_MANAGED_HOSTED_GATE') == '1' and len(sys.argv) == 2,
            'actual hosted dedicated-account operation required')
    root = Path(__file__).resolve().parents[1]
    workspace = root.parent
    account = pwd.getpwuid(os.geteuid())
    home = Path(account.pw_dir)
    require(account.pw_name == os.environ['USER'] and home == Path(os.environ['HOME'])
            and account.pw_uid == int(os.environ['ENGRAM_MANAGED_UID'])
            and account.pw_gid == os.getegid() and account.pw_uid > 0
            and re.fullmatch(r'latq[0-9a-f]{16}', account.pw_name)
            and home == Path('/Users') / account.pw_name and root.name == 'engram'
            and workspace.parent == home / 'localdev', 'actual OS-account workspace mismatch')
    expected = os.environ['ENGRAM_MANAGED_SOURCE']
    require(re.fullmatch(r'[0-9a-f]{40}', expected) and git(root, 'rev-parse', 'HEAD') == expected,
            'exact source required')
    receipts = workspace / 'managed-startup-receipts'
    private = workspace / 'private'
    profile = parse_json(read_regular(root / 'scripts/managed-startup-qualification.json', MAXIMUM_METADATA))
    step = sys.argv[1]
    result = {'schemaVersion': 1, 'operation': step, 'engramCommit': expected,
              'runtimeAuthority': False, 'actualCommandCustodyRequired': True}
    if step == 'prepare-lock':
        require(not git(root, 'status', '--porcelain', '--untracked-files=no'), 'source initially dirty')
        result['initialPhysicalSourceManifestSHA256'] = authenticate_source_bytes(root, git)
        source = read_regular(root / 'Package.resolved', MAXIMUM_METADATA)
        require(hashlib.sha256(source).hexdigest() == profile['publicEngramResolvedSHA256'], 'public lock changed')
        original = parse_json(source)
        identities = {row['identity'] for row in original['pins']}
        require(len(original['pins']) == len(identities) == 46
                and not identities.intersection(row['identity'] for row in profile['supplementalPins']),
                'supplemental graph collision')
        original['pins'] += profile['supplementalPins']
        require(len(original['pins']) == 48, 'incomplete locked private graph')
        with (private / 'original-Package.resolved').open('xb') as stream:
            stream.write(source)
        mirror = root / '.swiftpm/configuration/mirrors.json'
        with (private / 'original-mirrors.json').open('xb') as stream:
            stream.write(read_regular(mirror, MAXIMUM_METADATA))
        (root / 'Package.resolved').write_text(json.dumps(original, indent=2) + '\n')
        result['originalLockSHA256'] = hashlib.sha256(source).hexdigest()
        result['preparedLockSHA256'] = hashlib.sha256((root / 'Package.resolved').read_bytes()).hexdigest()
        result['identities'] = sorted(row['identity'] for row in original['pins'])
    elif step == 'verify-graph':
        result['prebuildGraph'] = prebuild_graph(root, workspace, profile, private, receipts)
    elif step == 'restore-public':
        before = read_regular(receipts / 'effective-graph-before.json', MAXIMUM_METADATA)
        after = read_regular(receipts / 'effective-graph-after.json', MAXIMUM_METADATA)
        require(graph_nodes(parse_json(before)) == graph_nodes(parse_json(after)), 'effective graph changed during build')
        state = parse_json(read_regular(root / '.build/workspace-state.json', MAXIMUM_METADATA))['object']['dependencies']
        require(state == parse_json(read_regular(private / 'prebuild-workspace-dependencies.json', MAXIMUM_METADATA)),
                'exact dependency revisions changed during build')
        source = read_regular(private / 'original-Package.resolved', MAXIMUM_METADATA)
        require(hashlib.sha256(source).hexdigest() == profile['publicEngramResolvedSHA256'], 'retained original lock changed')
        mirror = read_regular(private / 'original-mirrors.json', MAXIMUM_METADATA)
        require(git(root, 'show', 'HEAD:.swiftpm/configuration/mirrors.json').encode().strip() == mirror.strip(),
                'retained original mirror changed')
        (root / 'Package.resolved').write_bytes(source)
        (root / '.swiftpm/configuration/mirrors.json').write_bytes(mirror)
        require(not git(root, 'status', '--porcelain', '--untracked-files=no'), 'strict source restoration failed')
        result['sourceManifestSHA256'] = authenticate_source_bytes(root, git)
        result['beforeGraphSHA256'] = hashlib.sha256(before).hexdigest()
        result['afterGraphSHA256'] = hashlib.sha256(after).hexdigest()
    elif step == 'package-memory':
        require(not (workspace / 'package').exists(), 'package must be new')
        (workspace / 'package').mkdir(mode=0o700)
        cli = workspace / 'package/cli'
        cli.mkdir(mode=0o700)
        release = (root / '.build/release').resolve(strict=True)
        require(release.is_relative_to(root / '.build') and release.name == 'release', 'actual build output required')
        result['memoryCopy'] = copy_file(release / 'Engram', cli / 'memory')
        result['resources'] = {name: copy_tree(release / name, cli / name) for name in BUNDLES}
    elif step == 'prove-private-build':
        core, proof = validate_private_inputs(root, git)
        require(len(proof['compilerInputs']) == 71 and len(proof['effectiveDependencies']) == 48,
                'complete fixed compiler graph required')
        result['sourceAndCompilerInputs'] = proof
        result['signedMemory'] = signed_input_facts(workspace / 'package/cli/memory')
    elif step == 'prove-unstamped':
        command = parse_json(read_regular(receipts / 'build-unstamped-command.json', MAXIMUM_METADATA))
        require(command['label'] == 'build-unstamped' and command['started'] is True and command['success'] is True
                and command['exitCode'] == 0 and command['leaderReaped'] is True and command['groupGone'] is True,
                'actual unstamped build must close successfully')
        names = ('main.cpp', 'owned_launch.cpp', 'installation_manifest.cpp', 'installation_cohort.cpp',
                 'installation_files.cpp', 'installation_seed.cpp', 'store_admission.cpp',
                 'engram_product_installer.cpp', 'normal_startup.cpp')
        paths = [workspace / 'LatticeCore/Tools/LatticeInstallationLauncher' / name for name in names]
        require(command['argv'] == ['xcrun', '--sdk', 'macosx', 'clang++', '-std=c++20', '-O2', '-pthread',
            *map(str, paths), '-o', str(workspace / 'unstamped-launcher'), '-v'], 'unstamped build command differs')
        log = read_regular(private / 'build-unstamped.log', 256 * 1024 * 1024)
        require(len(log) == command['logBytes'] and hashlib.sha256(log).hexdigest() == command['logSHA256'],
                'unstamped closed log differs')
        result['compilerInputs'] = launcher_compiler_inputs(log,
            {str(path): hashlib.sha256(read_regular(path, MAXIMUM_METADATA)).hexdigest() for path in paths})
        result['commandSHA256'] = hashlib.sha256(read_regular(receipts / 'build-unstamped-command.json', MAXIMUM_METADATA)).hexdigest()
        result['logSHA256'] = command['logSHA256']
        result['binary'] = digest(workspace / 'unstamped-launcher')
    elif step == 'install-package':
        require(not os.path.lexists(home / '.claude'), 'dedicated fixed installation must be new')
        (home / '.claude').mkdir(mode=0o700)
        installed = home / '.claude/bin'
        result['copies'] = copy_tree(workspace / 'package/cli', installed)
        require((installed / 'memory').is_file() and (installed / 'memory-installation-launcher').is_file()
                and (installed / 'engram-installation.provenance').is_file(), 'actual final package missing')
        result['installedRoot'] = str(installed)
    elif step == 'prove-final':
        core, proof = validate_private_inputs(root, git)
        initial = parse_json(read_regular(receipts / 'prove-private-build.json', MAXIMUM_METADATA))
        require(proof == initial['sourceAndCompilerInputs'], 'private graph/source/closed build changed')
        provenance = parse_json(read_regular(workspace / 'package/cli/engram-installation-provenance.json', MAXIMUM_METADATA))
        require(provenance['sourceRevision'] == expected and provenance['coreRevision'] == profile['core']['commit']
                and provenance['coreTree'] == profile['core']['tree'] and provenance['isRegistration'] is False,
                'final product provenance changed')
        result['finalProvenance'] = provenance
        result['packageMemory'] = signed_input_facts(workspace / 'package/cli/memory')
        result['packageLauncher'] = signed_input_facts(workspace / 'package/cli/memory-installation-launcher', 'memory-installation-launcher')
        require(result['packageMemory'] == provenance['initializer'] and result['packageLauncher'] == provenance['launcher'],
                'final signed bytes changed after stamping')
        result['installedMemory'] = signed_input_facts(home / '.claude/bin/memory')
        result['installedLauncher'] = signed_input_facts(home / '.claude/bin/memory-installation-launcher', 'memory-installation-launcher')
        for label, member in (('Memory', 'initializer'), ('Launcher', 'launcher')):
            require(result['installed' + label]['sha256'] == provenance[member]['sha256']
                    and result['installed' + label]['bytes'] == provenance[member]['bytes'], 'installed bytes differ')
        record = read_regular(home / '.claude/installation/managed-package.v1', 168)
        require(len(record) == 168 and record[:8] == b'LATINS1\0'
                and hashlib.sha256(record[:-32]).digest() == record[-32:], 'actual publisher record absent')
        result['installedAuthorityRecordSHA256'] = hashlib.sha256(record).hexdigest()
        result['sourceAndCompilerInputs'] = proof
    else:
        raise ValueError('unknown fixed artifact operation')
    write_new(receipts / (step + '.json'), result)


if __name__ == '__main__':
    main()
