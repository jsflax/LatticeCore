"""Small supervisor for ordinary qualification commands and retained raw logs."""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time

def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')

def file_fact(path):
    # Stream large build/test artifacts; a known-ending file must be stable.
    path = Path(path)
    digest = hashlib.sha256()
    with path.open('rb') as source:
        before = os.fstat(source.fileno())
        if not __import__('stat').S_ISREG(before.st_mode):
            raise RuntimeError('artifact is not a regular file: '+str(path))
        for block in iter(lambda: source.read(1024*1024), b''):
            digest.update(block)
        after = os.fstat(source.fileno())
    identity = lambda s: (s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
    if identity(before) != identity(after):
        raise RuntimeError('artifact changed while hashing: '+str(path))
    return dict(path=str(path),bytes=after.st_size,sha256=digest.hexdigest(),
                mode=oct(after.st_mode & 0o777))

def source_manifest(repositories):
    result = []
    for repo in repositories:
        names = subprocess.check_output(['git', '-C', str(repo), 'ls-files', '-c', '-o', '--exclude-standard', '-z'])
        for name in sorted(set(os.fsdecode(n) for n in names.split(b'\0') if n)):
            path = repo / name
            if path.is_file():
                result.append(dict(repository=str(repo), relative=name, **file_fact(path)))
    return result

def group_exists(pid):
    try:
        os.killpg(pid, 0)
        return True
    except ProcessLookupError:
        return False

def group_state(pid):
    try:
        return dict(absent=not group_exists(pid), error=None)
    except OSError as error:
        return dict(absent=None, error=dict(errno=error.errno, message=str(error)))

def run_stage(out, name, command, cwd, env, cap_seconds, disk_floor=12*1024**3):
    out = Path(out)
    start = time.monotonic()
    row = dict(stage=name, command=command, cwd=str(cwd), capSeconds=cap_seconds,
               signals=[], signalErrors=[], stopReason=None, globalDescendantEndingNotClaimed=True)
    process = None
    with (out/(name+'.stdout')).open('wb') as stdout, (out/(name+'.stderr')).open('wb') as stderr:
        try:
            disk = os.statvfs(out)
            free_minimum = disk.f_bavail * disk.f_frsize
            row['minimumFreeBytes'] = free_minimum
            if free_minimum < disk_floor:
                raise RuntimeError('disk admission floor before launch')
            process = subprocess.Popen(command, cwd=cwd, env=env, stdout=stdout,
                                       stderr=stderr, start_new_session=True)
            row.update(pid=process.pid, pgid=process.pid)
            write_json(out/'CURRENT.json', row)
            while process.poll() is None:
                disk = os.statvfs(out)
                free = disk.f_bavail * disk.f_frsize
                free_minimum = min(free_minimum, free)
                if free < disk_floor:
                    row['stopReason'] = 'disk floor'
                    break
                if time.monotonic()-start >= cap_seconds:
                    row['stopReason'] = 'stage deadline'
                    break
                time.sleep(0.25)
            row['minimumFreeBytes'] = free_minimum
        except BaseException as error:
            row['stopReason'] = repr(error)
        finally:
            if process is not None:
                # Never signal after reaping the leader: its PID/PGID could be
                # recycled. While it remains our unreaped child, its PID stays
                # reserved even if it exits between observation and signal.
                if row['stopReason'] and process.returncode is None:
                    for sig in (signal.SIGTERM, signal.SIGKILL):
                        if process.returncode is not None:
                            break
                        try:
                            os.killpg(process.pid, sig)
                            row['signals'].append(int(sig))
                        except OSError as error:
                            row['signalErrors'].append(dict(signal=int(sig),errno=error.errno,message=str(error)))
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            pass
                try:
                    row['exitCode'] = process.wait(timeout=5)
                    row['leaderReaped'] = True
                except subprocess.TimeoutExpired:
                    row.update(exitCode=None, leaderReaped=False)
                state = group_state(process.pid)
                end = time.monotonic()+5
                while state['absent'] is False and time.monotonic() < end:
                    time.sleep(0.05)
                    state = group_state(process.pid)
                row['processGroupAbsent'] = state['absent']
                row['groupQueryError'] = state['error']
                if state['absent'] is not True:
                    row['stopReason'] = row['stopReason'] or 'owned group ending unproved after leader ending; no post-reap signal'
            else:
                row.update(exitCode=None, leaderReaped=False, processGroupAbsent=None,noChildIssued=True)
    row['elapsedSeconds'] = time.monotonic()-start
    row['knownEnding'] = bool(row['leaderReaped'] and row['processGroupAbsent'])
    # Never hash or parse a log that may still be written by a live owned group.
    if row['knownEnding']:
        row['stdout'] = file_fact(out/(name+'.stdout'))
        row['stderr'] = file_fact(out/(name+'.stderr'))
    write_json(out/(name+'.json'), row)
    print(json.dumps(row), flush=True)
    if row['exitCode'] != 0 or row['stopReason'] or not row['knownEnding']:
        raise RuntimeError(name+' failed; see retained stage ending/logs')
    return row
