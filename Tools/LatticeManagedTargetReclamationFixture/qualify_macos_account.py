#!/usr/bin/env python3
"""SOURCE-ONLY proposal. Future execution requires a disposable macOS CI VM.

Creates one actual OS account; builds, signs and installs private Native images;
then runs the genuine installed paired constructor fixture. It does not supply
a copied admission, a legacy semantic profile, or a successful native return.
No existing user, account, installation, source graph or store is modified.
All first failures and the account remain for disposable-VM teardown.
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
import sqlite3
import stat
import subprocess
import sys
import uuid


def run(argv, user=None, cwd=None, timeout=600):
    if user:
        argv = ['/usr/bin/sudo', '-u', user, '--', *map(str, argv)]
    environment={'PATH': '/usr/bin:/bin:/usr/sbin:/sbin'}
    if user:
        scratch=Path(pwd.getpwnam(user).pw_dir)/'localdev'/'scratch'
        environment['TMPDIR']=str(scratch)
    try:
        result = subprocess.run(list(map(str, argv)), cwd=cwd, check=False,
                                capture_output=True, timeout=timeout, env=environment)
    except subprocess.TimeoutExpired as error:
        if cwd is not None:
            prefix=Path(cwd)/('first-timeout-'+uuid.uuid4().hex)
            prefix.with_suffix('.stdout').write_bytes(error.stdout or b'')
            prefix.with_suffix('.stderr').write_bytes(error.stderr or b'')
        raise
    if result.returncode:
        if cwd is not None:
            prefix=Path(cwd)/('first-failure-'+uuid.uuid4().hex)
            prefix.with_suffix('.stdout').write_bytes(result.stdout)
            prefix.with_suffix('.stderr').write_bytes(result.stderr)
        sys.stderr.buffer.write(result.stdout+result.stderr)
        raise subprocess.CalledProcessError(result.returncode,argv,result.stdout,result.stderr)
    return result.stdout.decode()


def digest(path):
    value = hashlib.sha256()
    with open(path, 'rb') as stream:
        for chunk in iter(lambda: stream.read(65536), b''):
            value.update(chunk)
    return value.hexdigest()


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n').encode()


def file_fact(path):
    first = os.lstat(path)
    assert stat.S_ISREG(first.st_mode) and first.st_nlink == 1
    content = digest(path)
    assert first == os.lstat(path)
    return dict(device=first.st_dev, inode=first.st_ino, uid=first.st_uid,
                mode=first.st_mode, links=first.st_nlink, bytes=first.st_size,
                modifiedNanoseconds=first.st_mtime_ns,
                changedNanoseconds=first.st_ctime_ns, sha256=content)


def directory_fact(path):
    item = os.lstat(path)
    assert stat.S_ISDIR(item.st_mode)
    return dict(device=item.st_dev, inode=item.st_ino, uid=item.st_uid,
                gid=item.st_gid, mode=item.st_mode)


def mkdir(path, uid, gid):
    os.mkdir(path, 0o700)
    os.chown(path, uid, gid)


def write(path, content, uid, gid, mode=0o600):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    with os.fdopen(descriptor, 'wb') as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    os.chown(path, uid, gid)
    os.chmod(path, mode)


def copy(source, destination, uid, gid, mode=0o700):
    # Bounded streaming, exclusive birth, no alias/hardlink or replacement.
    original=os.lstat(source)
    if not stat.S_ISREG(original.st_mode) or original.st_nlink!=1 or original.st_size>512*1024*1024:
        raise RuntimeError('Source image/profile exceeds fixed Native file class')
    source_fd=os.open(source,os.O_RDONLY|os.O_NOFOLLOW)
    target_fd=os.open(destination,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,mode)
    with os.fdopen(source_fd,'rb') as input_stream,os.fdopen(target_fd,'wb') as output_stream:
        shutil.copyfileobj(input_stream,output_stream,65536)
        output_stream.flush();os.fsync(output_stream.fileno())
        if original!=os.fstat(input_stream.fileno()) or original!=os.lstat(source):
            raise RuntimeError('Source image changed during real copy; original target retained')
    os.chown(destination,uid,gid);os.chmod(destination,mode)
    return file_fact(destination)


def sync(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def first_absent(path):
    current = Path('/')
    for component in Path(path).parts[1:]:
        current /= component
        try:
            item = os.lstat(current)
        except FileNotFoundError:
            return str(current)
        if stat.S_ISLNK(item.st_mode):
            raise RuntimeError('Fixture path contains a symlink')
    raise RuntimeError('Dedicated VM must have no Engram GUI/configuration at ' + str(path))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ephemeral-ci-vm', action='store_true', required=True)
    parser.add_argument('--graph', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--legacy-package', type=Path, required=True)
    parser.add_argument('--qualified-legacy-profile', type=Path, required=True)
    parser.add_argument('--qualified-legacy-profile-sha256', required=True)
    args = parser.parse_args()
    if sys.platform != 'darwin' or os.geteuid() != 0:
        raise RuntimeError('Explicit disposable macOS VM and root account-creation lane required')
    source = args.graph.resolve(strict=True)
    root = args.root.absolute()
    if 'localdev' not in source.parts or 'localdev' not in root.parts:
        raise RuntimeError('Source/account/project/build roots must be under localdev')
    if root.exists() or root.is_symlink():
        raise RuntimeError('Existing fixture root is never reused, removed or reset')
    legacy = args.legacy_package.resolve(strict=True)
    profile = args.qualified_legacy_profile.resolve(strict=True)
    if not re.fullmatch('[0-9a-f]{64}', args.qualified_legacy_profile_sha256) or digest(profile) != args.qualified_legacy_profile_sha256:
        raise RuntimeError('Separately reviewed legacy profile changed')
    profile_text = profile.read_text()
    for pin in ['engram-v0.14.10-darwin-qualified-spawn-v1',
                '5e0862b6af90578a0e2899bd379fb2b9be9f9703',
                '0461b210aa028f3c4bb6165fed895e0f4385b565',
                'eb9a99c5d3945108ab9d9d0d39622616d82f3586']:
        if pin not in profile_text:
            raise RuntimeError('Legacy semantic qualification pin missing')
    # The profile must come from actual earlier product qualification. Never
    # generate available=true or substitute arbitrary legacy executable hashes.
    for leaf in ['memory', 'memory-hooks', 'memory-sync']:
        if digest(legacy / leaf) not in profile_text:
            raise RuntimeError('Actual legacy artifact does not match the reviewed profile: ' + leaf)
    os.mkdir(root, 0o711)
    name = 'lattice-reclaim-' + uuid.uuid4().hex[:8]
    occupied = set()
    for line in run(['/usr/bin/dscl', '.', '-list', '/Users', 'UniqueID'], timeout=10).splitlines():
        values = line.split()
        if values and values[-1].isdigit():
            occupied.add(int(values[-1]))
    uid = next(value for value in range(600, 4096) if value not in occupied)
    gid = grp.getgrnam('staff').gr_gid
    # OS-managed account home is at its required /Users path; all project,
    # build, scratch and evidence below it is explicitly ~/localdev.
    home = Path('/Users') / name
    if home.exists() or home.is_symlink():
        raise RuntimeError('Existing account home is never reused')
    parent_fact=directory_fact(home.parent)
    if parent_fact['uid']!=0 or parent_fact['mode'] & 0o022:
        raise RuntimeError('Real OS account parent must pass unchanged Native ownership policy')
    mkdir(home, uid, gid)
    try:
        pwd.getpwnam(name)
    except KeyError:
        pass
    else:
        raise RuntimeError('Unexpected OS account-name collision')
    node = '/Users/' + name
    run(['/usr/bin/dscl', '.', '-create', node], timeout=10)
    for field, value in [('UniqueID', uid), ('PrimaryGroupID', gid), ('NFSHomeDirectory', home),
                         ('UserShell', '/bin/zsh'), ('RealName', 'Lattice constructor qualification'), ('Password', '*')]:
        run(['/usr/bin/dscl', '.', '-create', node, field, value], timeout=10)
    account = pwd.getpwnam(name)
    if account.pw_uid != uid or Path(account.pw_dir) != home:
        raise RuntimeError('Fresh actual OS-account observation differs')
    work = home / 'localdev'
    mkdir(work, uid, gid)
    mkdir(work/'scratch', uid, gid)
    account_receipt=canonical(dict(account=name, uid=uid, actualHome=account.pw_dir,
          homeOverride=False, disposition='PRESERVE_ACCOUNT_AND_FIRST_FAILURES'))
    write(work / 'ACCOUNT.json', account_receipt, uid, gid)
    write(root / 'ACCOUNT.json', account_receipt, 0, 0)
    graph = work / 'graph'
    if (source / '.git').exists():
        raise RuntimeError('Use the sealed plain source graph, never a live checkout')
    shutil.copytree(source, graph, symlinks=False)
    for path in [graph, *graph.rglob('*')]:
        os.chown(path, uid, gid)
    run(['/usr/bin/git', 'init'], user=name, cwd=graph, timeout=10)
    run(['/usr/bin/git', 'add', '.'], user=name, cwd=graph, timeout=60)
    run(['/usr/bin/git', '-c', 'user.name=Lattice Fixture', '-c', 'user.email=fixture@localhost',
         'commit', '-m', 'Frozen genuine constructor fixture source'], user=name, cwd=graph, timeout=60)
    revision = run(['/usr/bin/git', 'rev-parse', 'HEAD'], user=name, cwd=graph, timeout=10).strip()
    assert re.fullmatch('[0-9a-f]{40}', revision)
    cmake = shutil.which('cmake')
    if not cmake:
        raise RuntimeError('Dedicated VM CMake is unavailable')
    build = work / 'build'
    common = [cmake, '-S', graph / 'Core', '-B', build,
              '-DLATTICE_MANAGED_TARGET_RECLAMATION_FIXTURE=ON',
              '-DLATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE=OFF', '-DCMAKE_BUILD_TYPE=Debug']
    run(common, user=name, cwd=work)
    run([cmake, '--build', build, '--target', 'LatticeManagedTargetReclamationRuntime',
         'LatticeManagedTargetReclamationTests', '--parallel', '2'], user=name, cwd=work)
    runtime = build / 'LatticeManagedTargetReclamationRuntime'
    run(['/usr/bin/codesign', '--force', '--sign', '-', '--timestamp=none', runtime], user=name, cwd=work, timeout=30)
    runtime_hash = digest(runtime)
    generated = work / 'generated'
    mkdir(generated, uid, gid)
    array = lambda value: ','.join('0x' + value[i:i+2] for i in range(0, len(value), 2))
    header = ('#pragma once\n#include <array>\n#include <cstdint>\n#include <string_view>\n'
              'namespace lattice::detail::ordinary_installation::engram_build_stamp {\n'
              'inline constexpr bool available=true;\n'
              'inline constexpr std::string_view product="engram",initializer_leaf="memory";\n'
              'inline constexpr std::string_view source_revision="' + revision + '",core_revision="' + revision + '";\n'
              'inline constexpr std::array<std::uint8_t,32> initializer_sha256{' + array(runtime_hash) + '};\n}\n')
    write(generated / 'build.hpp', header.encode(), uid, gid)
    header = ('#pragma once\n#include <array>\n#include <cstdint>\n'
              'namespace lattice::detail::ordinary_installation::engram_managed_pair_stamp {\n'
              'inline constexpr bool available=true;\n'
              'inline constexpr std::array<std::uint8_t,32> daemon_sha256{' + array(runtime_hash) + '};\n}\n')
    write(generated / 'pair.hpp', header.encode(), uid, gid)
    copy(profile, generated / 'legacy.hpp', uid, gid, 0o600)
    run([*common, '-DLATTICE_RECLAMATION_BUILD_STAMP_HEADER=' + str(generated / 'build.hpp'),
         '-DLATTICE_RECLAMATION_MANAGED_PAIR_STAMP_HEADER=' + str(generated / 'pair.hpp'),
         '-DLATTICE_RECLAMATION_LEGACY_PROFILE_HEADER=' + str(generated / 'legacy.hpp')], user=name, cwd=work)
    run([cmake, '--build', build, '--target', 'LatticeInstallationLauncher', '--parallel', '2'], user=name, cwd=work)
    launcher = build / 'LatticeInstallationLauncher'
    run(['/usr/bin/codesign', '--force', '--sign', '-', '--timestamp=none', launcher], user=name, cwd=work, timeout=30)
    # Real inert historical stores; no retirement/closed-cohort record is seeded.
    data = home / '.claude'
    mkdir(data, uid, gid)
    old_bin = data / 'bin'
    mkdir(old_bin, uid, gid)
    for leaf in ['memory', 'memory-hooks', 'memory-sync']:
        copy(legacy / leaf, old_bin / leaf, uid, gid)
    old_sync = data / 'sync'
    mkdir(old_sync, uid, gid)
    for path in [data / 'memory.sqlite', old_sync / 'group-00000000-0000-0000-0000-000000000001.sqlite']:
        connection = sqlite3.connect(path)
        try:
            connection.execute('CREATE TABLE fixture_history(value TEXT)')
            connection.commit()
        finally:
            connection.close()
        os.chown(path, uid, gid)
        os.chmod(path, 0o600)
        with open(path, 'rb') as stream:
            os.fsync(stream.fileno())
    sync(old_sync)
    sync(data)
    state = home / '.engram-installation'
    mkdir(state, uid, gid)
    stage = state / 'upgrade-bootstrap.v1'
    mkdir(stage, uid, gid)
    controller, configs, images = [stage / leaf for leaf in ['controller', 'original-configurations', 'managed-runtime.v2']]
    for path in [controller, configs, images]:
        mkdir(path, uid, gid)
    installed_launcher = copy(launcher, controller / 'memory-installation-launcher', uid, gid)
    memory = copy(runtime, images / 'memory', uid, gid)
    daemon = copy(runtime, images / 'memory-sync', uid, gid)
    provenance = (b'LATPKG1\0' + (1).to_bytes(8, 'little') * 2 + bytes.fromhex(revision) * 2 +
                  bytes.fromhex(memory['sha256']) + bytes.fromhex(installed_launcher['sha256']))
    paired = (b'LATMRT2\0' + (2).to_bytes(8, 'little') * 2 + bytes.fromhex(revision) * 2 +
              bytes.fromhex(memory['sha256']) + bytes.fromhex(daemon['sha256']) + bytes.fromhex(installed_launcher['sha256']))
    assert len(provenance) == 128 and len(paired) == 160
    write(images / 'image.json', canonical(dict(format='engram-managed-runtime-image-v2', accountUID=uid,
          provenanceHex=provenance.hex(), pairedProvenanceHex=paired.hex(), executable=memory, daemonExecutable=daemon)), uid, gid)
    anchor = dict(format='engram-global-applications-anchor-v1', root=directory_fact(Path('/')),
                  present=Path('/Applications').exists())
    if anchor['present']:
        value = directory_fact(Path('/Applications'))
        anchor['original'] = value
        anchor['adminGID'] = grp.getgrnam('admin').gr_gid if value['mode'] & 0o7777 == 0o775 else None
    guis = [dict(path=str(path), present=False, absentComponent=first_absent(path)) for path in
            [Path('/Applications/Engram.app/Contents/MacOS/Engram'), home / 'Applications/Engram.app/Contents/MacOS/Engram']]
    policies = []
    for relative in ['.claude.json', '.claude/settings.json', '.codex/config.toml',
                     'Library/LaunchAgents/io.engram.sync.plist', 'Library/LaunchAgents/io.engram.sync-daemon.plist']:
        policies.append(dict(path=relative, present=False,
                        absentComponent=str(Path(first_absent(home / relative)).relative_to(home))))
    original = [dict(leaf=leaf, **file_fact(old_bin / leaf)) for leaf in ['memory', 'memory-hooks', 'memory-sync']]
    bootstrap = dict(format='engram-upgrade-bootstrap-v1', accountUID=uid, dataDirectory=directory_fact(data),
                     legacyBuildProvenanceObserved=False, coveredCachedAliases=False, wholeCohortRetired=False,
                     historicalAdoptionAuthorized=False, stagedController=installed_launcher,
                     stagedProvenanceHex=provenance.hex(), originalInstalledImages=original,
                     originalInstalledGUIImages=guis, globalApplicationsAnchor=anchor, originalLaunchConfigurations=policies)
    raw = canonical(bootstrap)
    write(stage / 'bootstrap.json', raw, uid, gid)
    write(stage / 'bootstrap.sha256', (hashlib.sha256(raw).hexdigest() + '\n').encode(), uid, gid)
    write(stage / 'lock', b'', uid, gid)
    for path in [controller, configs, images, stage, state, home]:
        sync(path)
    command = [build / 'LatticeManagedTargetReclamationTests',
               '--gtest_filter=ManagedTargetReclamation.GenuineInstalledConstructorReceipt',
               '--gtest_repeat=1', '--gtest_color=no']
    write(work / 'QUALIFICATION-COMMAND.json', canonical(dict(argv=list(map(str, command)), user=name,
          sourceRevision=revision, constructorOnly=True, receiverWorkerAcceptance=False)), uid, gid)
    # Never erase a failed run or mistake a timeout for a completed child.
    # Native helper/custody code owns terminal observation and bounded cleanup.
    try:
        result = subprocess.run(['/usr/bin/sudo', '-u', name, '--', *map(str, command)], cwd=work,
                                capture_output=True, timeout=40,
                                env={'PATH': '/usr/bin:/bin:/usr/sbin:/sbin', 'TMPDIR':str(work/'scratch')})
    except subprocess.TimeoutExpired as error:
        write(work/'QUALIFICATION.stdout',error.stdout or b'',uid,gid)
        write(work/'QUALIFICATION.stderr',error.stderr or b'',uid,gid)
        write(work/'CLEANUP-OBSERVATION.json',canonical(dict(account=name,uid=uid,
              observation='EXTERNAL_TIMEOUT_NATIVE_CUSTODY_UNKNOWN',cleanupDisposition='RETAIN_ACCOUNT_AND_STORES')),
              uid,gid)
        raise
    write(work / 'QUALIFICATION.stdout', result.stdout, uid, gid)
    write(work / 'QUALIFICATION.stderr', result.stderr, uid, gid)
    processes = run(['/bin/ps', '-axo', 'uid=,pid=,ppid=,command='], timeout=10)
    remaining = [line for line in processes.splitlines() if line.split() and int(line.split()[0]) == uid]
    write(work / 'CLEANUP-OBSERVATION.json', canonical(dict(account=name, uid=uid, nativeTestExit=result.returncode,
          remainingAccountProcesses=remaining, cleanupDisposition='RETAIN_ACCOUNT_AND_STORES_FOR_VM_TEARDOWN',
          sourceRevision=revision, legacyProfileSHA256=digest(profile), runtimeSHA256=runtime_hash,
          controllerSHA256=digest(launcher))), uid, gid)
    if result.returncode or remaining:
        raise RuntimeError('Native fixture or actual process ending unproved; account and first failures preserved')
    print(str(work / 'CLEANUP-OBSERVATION.json'))


if __name__ == '__main__':
    main()
