#!/usr/bin/env python3
"""Private owner qualification primitives. No account builder or direct fixture main.

Definitions derived from the exact frozen847 source; only run scratch/failure
custody and file_fact/copy atime comparisons differ. No source request imports
or executes this module. Future invocation requires the actual owner builder.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import uuid


def stable_file_custody(observed):
    # The original stat_result equality fields, strengthened to ns timestamps.
    # Only self-read access time is excluded; no identity/owner/permission waiver.
    return (observed.st_dev, observed.st_ino, observed.st_uid, observed.st_gid,
            observed.st_mode, observed.st_nlink, observed.st_size,
            observed.st_mtime_ns, observed.st_ctime_ns)


def run(argv, user=None, cwd=None, timeout=600, *, scratch):
    # Exact qualification scratch root, for root and real account commands alike.
    # Never derive project/output paths from HOME or the account's ordinary state.
    scratch = Path(scratch)
    if (not scratch.is_absolute() or not scratch.is_relative_to(Path('/Users/jason/localdev'))
            or not scratch.is_dir() or scratch.is_symlink()
            or scratch.resolve(strict=True) != scratch):
        raise RuntimeError('Exact fresh Jason-localdev scratch directory required')
    environment = {'PATH': '/usr/bin:/bin:/usr/sbin:/sbin', 'TMPDIR': str(scratch),
        'CLANG_MODULE_CACHE_PATH': str(scratch / 'clang-module-cache'),
        'SWIFTPM_MODULECACHE_OVERRIDE': str(scratch / 'clang-module-cache')}
    if user:
        # Set configured output roots AFTER sudo's environment filtering.
        # No HOME variable or source/store authority is introduced.
        argv = ['/usr/bin/sudo', '-u', user, '--', '/usr/bin/env',
                *[key + '=' + value for key, value in environment.items()], *map(str, argv)]
    def preserve(path, content):
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(content); stream.flush(); os.fsync(stream.fileno())
    try:
        result = subprocess.run(list(map(str, argv)), cwd=cwd, check=False,
                                capture_output=True, timeout=timeout, env=environment)
    except subprocess.TimeoutExpired as error:
        if cwd is not None:
            prefix = Path(cwd) / ('first-timeout-' + uuid.uuid4().hex)
            preserve(prefix.with_suffix('.stdout'), error.stdout or b'')
            preserve(prefix.with_suffix('.stderr'), error.stderr or b'')
        raise
    if result.returncode:
        if cwd is not None:
            prefix = Path(cwd) / ('first-failure-' + uuid.uuid4().hex)
            preserve(prefix.with_suffix('.stdout'), result.stdout)
            preserve(prefix.with_suffix('.stderr'), result.stderr)
        sys.stderr.buffer.write(result.stdout + result.stderr)
        raise subprocess.CalledProcessError(result.returncode, argv, result.stdout, result.stderr)
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
    assert stable_file_custody(first) == stable_file_custody(os.lstat(path))
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
        if (stable_file_custody(original)!=stable_file_custody(os.fstat(input_stream.fileno()))
                or stable_file_custody(original)!=stable_file_custody(os.lstat(source))):
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
