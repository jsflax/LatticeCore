"""Dedicated hosted guardian command custody, with no installation authority.

Only the reviewed caller supplies fixed command roles. This is not a host sandbox
or a descendant census. Every direct Popen stays retained; Darwin kqueue observes
exit without consuming the actual checked waitpid. No Popen poll/wait or competing reaper
is allowed. The source-bound compiler graph stays in that leader's process group;
known product sessions require the separate unchanged-case/native-gate observer.
"""
from __future__ import annotations

import ast
import hashlib
import os
from pathlib import Path
import signal
import select
import stat
import subprocess
import sys
import time


class CustodyFailure(RuntimeError):
    """Fixed diagnostic code; never includes command output or environment."""


def require(value, code):
    if not value:
        raise CustodyFailure(code)


def exception_fact(failure):
    """Closed scalar diagnostic; never exports an exception's text or path."""
    kind = 'other'
    for cls, name in ((CustodyFailure, 'custody_failure'),
                      (PermissionError, 'permission_error'), (ProcessLookupError, 'process_lookup_error'),
                      (ChildProcessError, 'child_process_error'), (InterruptedError, 'interrupted_error'),
                      (TimeoutError, 'timeout_error'), (OSError, 'os_error'), (ValueError, 'value_error')):
        if isinstance(failure, cls):
            kind = name
            break
    number = failure.errno if isinstance(failure, OSError) else None
    return {'kind': kind, 'errno': number if type(number) is int and 0 <= number <= 65535 else None}


def bounded_regular(path: Path, maximum: int, *, empty=False) -> bytes:
    require(path.is_absolute() and path.resolve(strict=True) == path, "noncanonical_file")
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1
                and (0 if empty else 1) <= before.st_size <= maximum, "unbounded_file")
        def identity(value):
            return (value.st_dev, value.st_ino, value.st_mode, value.st_uid,
                    value.st_nlink, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
        require(identity(before) == identity(path.lstat()), "file_name_changed")
        result = bytearray()
        while len(result) < before.st_size:
            part = os.read(fd, min(65536, before.st_size - len(result)))
            require(part, "file_truncated")
            result.extend(part)
        require(not os.read(fd, 1) and identity(before) == identity(os.fstat(fd))
                == identity(path.lstat()), "file_changed")
        return bytes(result)
    finally:
        os.close(fd)


def file_fact(path: Path, maximum: int, *, empty=False):
    raw = bounded_regular(path, maximum, empty=empty)
    return {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def python_runtime():
    """Actual hosted interpreter/source identity; local explanatory snippets fail."""
    require(sys.implementation.name == "cpython" and sys.version_info >= (3, 11)
            and os.name == "posix", "unsupported_python")
    require(sys.platform == 'darwin' and all(hasattr(select, name) for name in (
        "kqueue", "kevent", "KQ_FILTER_PROC", "KQ_EV_ADD", "KQ_EV_ONESHOT", "KQ_EV_ERROR", "KQ_NOTE_EXIT"))
        and all(hasattr(os, name) for name in ("WNOHANG", "waitpid", "waitstatus_to_exitcode")),
        "no_darwin_retained_exit_observer")
    executable = Path(sys.executable).resolve(strict=True)
    source = Path(subprocess.__file__).resolve(strict=True)
    raw = bounded_regular(source, 1024 * 1024)
    tree = ast.parse(raw)
    popen = [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Popen"]
    require(len(popen) == 1, "unknown_popen_source")
    methods = {}
    for node in ast.walk(popen[0]):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in {
                "_wait", "_try_wait", "_internal_poll", "wait", "poll", "_execute_child"}:
            methods.setdefault(node.name, []).append(hashlib.sha256(
                ast.get_source_segment(raw.decode(), node).encode()).hexdigest())
    require(set(methods) == {"_wait", "_try_wait", "_internal_poll", "wait", "poll", "_execute_child"},
            "missing_popen_source")
    return {"implementation": "cpython", "version": list(sys.version_info[:3]),
            "executable": str(executable), "executableFact": file_fact(executable, 128 * 1024 * 1024),
            "subprocessSource": str(source), "subprocessFact": {"bytes": len(raw),
                "sha256": hashlib.sha256(raw).hexdigest()}, "waitSourceSHA256": methods,
            "compiledRuntimeAndSourceReviewStillRequired": True,
            "exitObserver": "Darwin EVFILT_PROC/NOTE_EXIT/NOTE_EXITSTATUS then checked waitpid",
            "execGate": file_fact(Path(__file__).resolve().with_name("managed_startup_exec_gate.py"), 65536)}


class StopFlag:
    """Signal handlers only set a scalar; they never interrupt spawn publication."""
    def __init__(self):
        self.requested = False
        self.signal = None
        self.previous = {}

    def install(self):
        require(not self.previous, "handlers_already_installed")
        for number in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            self.previous[number] = signal.getsignal(number)
            signal.signal(number, self.receive)

    def receive(self, number, frame):
        self.requested = True
        self.signal = int(number)

    def restore(self):
        for number, previous in self.previous.items():
            signal.signal(number, previous)
        self.previous.clear()


class OwnedCommand:
    """One retained direct child. All methods are single-thread guardian-only."""
    __slots__ = ("stop", "row", "process", "unproved", "terminal", "stdout", "stderr", "queue", "gate", "reader")

    def __init__(self, stop, label, argv, cwd, out, err, deadline_seconds):
        require(type(label) is str and label.isascii() and label.replace('-', '').isalnum(), "bad_label")
        require(type(argv) is list and argv and all(type(a) is str and '\0' not in a for a in argv), "bad_argv")
        require(type(deadline_seconds) in (int, float) and 0 < deadline_seconds <= 5400, "bad_budget")
        require(cwd.is_absolute() and cwd.resolve(strict=True) == cwd, "bad_cwd")
        self.stop, self.process, self.unproved, self.terminal = stop, None, False, False
        self.stdout, self.stderr = out, err
        self.queue, self.gate, self.reader = None, None, None
        self.row = {"label": label, "argv": argv, "cwd": str(cwd), "started": False,
                    "pid": None, "parentPID": os.getpid(), "startedMonotonicNS": None,
                    "deadlineSeconds": deadline_seconds, "primaryFailure": None,
                    "cleanupFailure": None, "primaryException": None, "cleanupException": None,
                    "terminalObservedWithoutReap": False,
                    "exitObserverRegistered": False, "execGateReleased": False,
                    "leaderReaped": False, "rawWaitStatus": None, "exitCode": None,
                    "observedRawWaitStatus": None, "observedExitCode": None,
                    "groupGone": False, "groupSignals": [], "success": False,
                    "positiveCustodyScope": "direct-leader-and-same-group-only",
                    "unknownSessionEscapesProvenAbsent": False}

    def default_reaping(self):
        require(signal.getsignal(signal.SIGCHLD) == signal.SIG_DFL, "sigchld_not_default")

    def spawn(self, env, *, uid=None, gid=None):
        self.default_reaping()
        require(self.process is None and not self.stop.requested, "spawn_refused")
        require((uid is None and gid is None) or (type(uid) is int and type(gid) is int
                and uid > 0 and gid > 0 and os.geteuid() == 0), "bad_credentials")
        # The caller retains this owner before Popen. Python signal handlers
        # cannot throw between its return and publication. The fixed isolated
        # child waits for registration before it execs any tool or product.
        self.queue = select.kqueue()
        os.set_inheritable(self.queue.fileno(), False)
        self.reader, self.gate = os.pipe()
        reader = self.reader
        os.set_inheritable(reader, False)
        os.set_inheritable(self.gate, False)
        self.row["startedMonotonicNS"] = time.monotonic_ns()
        end_ns = self.row["startedMonotonicNS"] + int(self.row["deadlineSeconds"] * 1_000_000_000)
        gate_source = Path(__file__).resolve().with_name('managed_startup_exec_gate.py')
        bootstrap = [sys.executable, '-I', '-S', '-B', str(gate_source), str(reader), str(end_ns),
                     '--', *self.row['argv']]
        options = {} if uid is None else {"user": uid, "group": gid, "extra_groups": []}
        try:
            self.process = subprocess.Popen(bootstrap, cwd=self.row["cwd"], env=env,
                stdin=subprocess.DEVNULL, stdout=self.stdout, stderr=self.stderr,
                start_new_session=True, close_fds=True, restore_signals=True,
                pass_fds=(reader,), **options)
            self.row["pid"], self.row["started"] = self.process.pid, True
            self.default_reaping()
            # Apple sys/event.h documents this child exit-status input flag.
            # Python need not expose its optional constant; no ABI layout is
            # guessed. A returned data value is provisional until waitpid.
            note_exitstatus = 0x04000000
            event = select.kevent(self.process.pid, filter=select.KQ_FILTER_PROC,
                flags=select.KQ_EV_ADD | select.KQ_EV_ONESHOT,
                fflags=select.KQ_NOTE_EXIT | note_exitstatus)
            self.queue.control([event], 0, 0)
            self.row['exitObserverRegistered'] = True
            require(not self.stop.requested and time.monotonic_ns() < end_ns, 'exec_gate_canceled_or_late')
            while True:
                try:
                    require(os.write(self.gate, b'GO01') == 4, 'exec_gate_short_write')
                    break
                except InterruptedError:
                    require(not self.stop.requested and time.monotonic_ns() < end_ns, 'exec_gate_canceled_or_late')
            os.close(self.gate)
            self.gate = None
            self.row['execGateReleased'] = True
        except BaseException:
            # Registration/token errors retain an actual returned Popen for
            # negative cleanup. An unreturned constructor has no such proof.
            if self.process is None:
                self.unproved = True
                self.row["primaryFailure"] = "spawn_unproved"
            raise
        finally:
            if self.reader is not None:
                reader, self.reader = self.reader, None
                os.close(reader)

    def observe_terminal(self, end):
        require(self.process is not None and not self.row["leaderReaped"] and not self.unproved
                and self.row['exitObserverRegistered'], "terminal_without_custody")
        self.default_reaping()
        if self.terminal:
            return True
        while time.monotonic() < end:
            try:
                events = self.queue.control(None, 1, 0)
            except InterruptedError:
                continue
            if not events:
                return False
            require(len(events) == 1, 'unexpected_exit_event_count')
            event = events[0]
            require(event.ident == self.process.pid and event.filter == select.KQ_FILTER_PROC
                    and event.fflags & select.KQ_NOTE_EXIT and not event.flags & select.KQ_EV_ERROR,
                    'unexpected_exit_event')
            self.terminal = True
            self.row["terminalObservedWithoutReap"] = True
            # NOTE_EXITSTATUS carries a wait status, not the decoded exit code.
            # Notification is still provisional: only checked waitpid reaps.
            require(type(event.data) is int and 0 <= event.data <= 0xffffffff,
                    'exit_notification_status_unproved')
            self.row["observedRawWaitStatus"] = event.data
            self.row["observedExitCode"] = os.waitstatus_to_exitcode(event.data)
            return True
        return False

    def signal_group(self, number):
        require(self.process is not None and not self.row["leaderReaped"] and not self.unproved,
                "signal_without_custody")
        self.default_reaping()
        # A kqueue-observed terminal child is still unreaped and reserves its PID.
        # A successful actual reap permanently forbids this operation.
        require(len(self.row["groupSignals"]) < 2, "group_signal_limit")
        failure_fact = None
        try:
            os.killpg(self.process.pid, number)
            outcome = "sent"
        except ProcessLookupError as failure:
            outcome = "group_absent"
            failure_fact = exception_fact(failure)
        except OSError as failure:
            # The signal did not establish closure. Preserve its exact finite
            # observation and continue to the owned child's checked wait/reap.
            outcome = "os_error"
            failure_fact = exception_fact(failure)
        self.row["groupSignals"].append({"signal": int(number), "result": outcome,
                                         "exception": failure_fact})

    def reap(self, end, *, failure_cleanup=False):
        require((self.terminal or failure_cleanup) and not self.unproved and not self.row["leaderReaped"],
                "reap_without_terminal")
        self.default_reaping()
        while time.monotonic() < end:
            try:
                pid, status = os.waitpid(self.process.pid, os.WNOHANG)
            except InterruptedError:
                continue
            except ChildProcessError:
                self.unproved = True
                raise CustodyFailure("child_custody_lost")
            if pid == 0:
                # NOTE_EXIT may precede waitability by a kernel scheduling
                # boundary. Keep the original deadline; never infer a reap.
                time.sleep(min(.01, max(0, end - time.monotonic())))
                continue
            if pid != self.process.pid:
                self.unproved = True
                raise CustodyFailure("checked_reap_missing")
            # Record the actual reap immediately. A later clock or metadata
            # failure cannot restore signaling rights or erase the real status.
            self.row["leaderReaped"] = True
            self.row["rawWaitStatus"] = status
            self.process.returncode = os.waitstatus_to_exitcode(status)
            self.row["exitCode"] = self.process.returncode
            if self.row["observedRawWaitStatus"] is not None:
                require(status == self.row["observedRawWaitStatus"], "exit_notification_wait_mismatch")
            return
        raise CustodyFailure("reap_deadline")

    def group_absent(self):
        require(self.row["leaderReaped"], "group_check_before_reap")
        try:
            os.killpg(self.process.pid, 0)
        except ProcessLookupError:
            self.row["groupGone"] = True
            return True
        # Read-only after reap: PID reuse may refuse, never permit a signal.
        return False

    def retire_failed(self, end):
        if self.gate is not None:
            os.close(self.gate)
            self.gate = None
        if self.process is None or self.unproved:
            raise CustodyFailure("cleanup_custody_unproved")
        if not self.row["leaderReaped"]:
            self.signal_group(signal.SIGTERM)
            term_end = min(end, time.monotonic() + 5)
            if self.row['exitObserverRegistered']:
                try:
                    while time.monotonic() < term_end:
                        if self.observe_terminal(term_end):
                            break
                        time.sleep(min(.01, max(0, term_end - time.monotonic())))
                except BaseException as failure:
                    # Broken notification grants nothing; the actual child has
                    # not been reaped. Final checked cleanup remains separate.
                    self.row['notificationCleanupFailure'] = 'exit_notification_unproved'
                    self.row['notificationCleanupException'] = exception_fact(failure)
            # Kill the failure group before the sole reap. No later signal is
            # possible once waitpid returns that actual child PID.
            self.signal_group(signal.SIGKILL)
            self.reap(end, failure_cleanup=True)
        while time.monotonic() < end:
            if self.group_absent():
                return
            time.sleep(min(.01, max(0, end - time.monotonic())))
        raise CustodyFailure("cleanup_group_unproved")

    def close_observer(self):
        failed = False
        for field in ('reader', 'gate'):
            fd = getattr(self, field)
            if fd is not None:
                setattr(self, field, None)  # Never retry ambiguous close.
                try:
                    os.close(fd)
                except OSError:
                    failed = True
        if self.queue is not None:
            queue, self.queue = self.queue, None
            try:
                queue.close()
            except OSError:
                failed = True
        require(not failed, 'observer_close_unproved')

    def run(self, env, *, uid=None, gid=None):
        try:
            self.spawn(env, uid=uid, gid=gid)
            end = self.row["startedMonotonicNS"] / 1_000_000_000 + self.row["deadlineSeconds"]
            while time.monotonic() < end:
                require(not self.stop.requested, "guardian_canceled")
                if self.observe_terminal(end):
                    # On a failed leader retain its unreaped identity while
                    # retiring remaining same-group compiler descendants.
                    require(self.row["observedExitCode"] == 0, "command_nonzero")
                    self.reap(end)
                    require(self.group_absent(), "remaining_group_unproved")
                    require(self.row["exitCode"] == 0, "command_nonzero")
                    require(not self.stop.requested, "guardian_canceled")
                    # Reap and group observation can themselves return late.
                    # Keep the real reap recorded, but never qualify late success.
                    require(time.monotonic() < end, "command_deadline")
                    self.row["success"] = True
                    return self.row
                time.sleep(min(.01, max(0, end - time.monotonic())))
            raise CustodyFailure("command_deadline")
        except BaseException as failure:
            if self.row["primaryFailure"] is None:
                self.row["primaryFailure"] = str(failure) if isinstance(failure, CustodyFailure) else "command_exception"
                self.row["primaryException"] = exception_fact(failure)
            try:
                self.retire_failed(time.monotonic() + 30)
            except BaseException as cleanup:
                self.row["cleanupFailure"] = str(cleanup) if isinstance(cleanup, CustodyFailure) else "cleanup_exception"
                self.row["cleanupException"] = exception_fact(cleanup)
            raise


class CommandOwner:
    """Retains all command objects through guardian exit, including failures."""
    def __init__(self, stop, receipt_directory, private_directory):
        self.stop, self.receipts, self.private = stop, receipt_directory, private_directory
        self.owners = []
        self.failed = False

    def run(self, label, argv, *, cwd, env, seconds, uid=None, gid=None, context=None,
            stdout_path=None, stderr_separate=False):
        require(not self.failed and len(self.owners) < 192 and not self.stop.requested, "command_owner_stopped")
        require(not any(o.row["label"] == label for o in self.owners), "duplicate_command_label")
        output = stdout_path or self.private / (label + '.log')
        error = self.private / (label + '.stderr') if stderr_separate else output
        # Fixed caller-created directories; each log/receipt is first-write only.
        out = output.open('xb')
        err = error.open('xb') if stderr_separate else out
        if uid is not None:
            os.fchown(out.fileno(), uid, gid)
            if err is not out:
                os.fchown(err.fileno(), uid, gid)
        owner = OwnedCommand(self.stop, label, argv, cwd, out, err, seconds)
        self.owners.append(owner)
        try:
            owner.run(env, uid=uid, gid=gid)
        except BaseException:
            self.failed = True
            raise
        finally:
            row = owner.row
            try:
                owner.close_observer()
            except BaseException:
                row['observerCloseFailure'] = 'unproved_observer_close'
                row['success'] = False
                self.failed = True
            for stream in (out,) if err is out else (out, err):
                try:
                    stream.close()
                except BaseException:
                    row['logCloseFailure'] = 'unproved_log_close'
                    row['success'] = False
                    self.failed = True
            row['stdout'] = str(output)
            row['stderr'] = str(error)
            try:
                row.update(logBytes=output.stat().st_size,
                    logSHA256=file_fact(output, 256 * 1024 * 1024, empty=True)['sha256'])
                if err is not out:
                    row['stderrFact'] = file_fact(error, 256 * 1024 * 1024, empty=True)
            except BaseException:
                row['logObservationFailure'] = 'unproved_log'
                row['success'] = False
                self.failed = True
            if context is not None:
                require(not (set(context) & set(row)), "command_context_collision")
                row.update(context)
            # Imported only in the hosted caller, never by source preparation.
            import json
            target = self.receipts / (label + '-command.json')
            raw = (json.dumps(row, indent=2, sort_keys=True) + '\n').encode()
            require(len(raw) <= 64 * 1024, "command_receipt_overflow")
            fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
            try:
                if uid is not None:
                    os.fchown(fd, uid, gid)
                at = 0
                while at < len(raw):
                    count = os.write(fd, raw[at:])
                    require(count > 0, "command_receipt_short_write")
                    at += count
                os.fsync(fd)
            finally:
                os.close(fd)
        require(row['success'] and not self.failed, "command_not_qualified")
        return row


def qualify_owner(owner, *, cwd, env, uid=None, gid=None):
    """Two real harmless commands before use; never a simulated exit/reap."""
    require(not owner.owners and not owner.failed, 'preflight_owner_already_used')
    runtime = python_runtime()
    commands = []
    for label in ('owner-preflight-one', 'owner-preflight-two'):
        row = owner.run(label, ['/usr/bin/true'], cwd=cwd, env=env, seconds=10, uid=uid, gid=gid)
        require(row['exitObserverRegistered'] and row['execGateReleased']
                and row['terminalObservedWithoutReap'] and row['leaderReaped']
                and row['observedRawWaitStatus'] == 0 and row['observedExitCode'] == 0
                and row['rawWaitStatus'] == 0 and row['exitCode'] == 0 and row['groupGone']
                and not row['groupSignals'] and row['primaryFailure'] is None
                and row['cleanupFailure'] is None, 'actual_owner_preflight_failed')
        commands.append(row)
    return {'runtime': runtime, 'actualCommands': commands,
            'positiveScope': 'two actually retained direct-child exec/exit/reap observations',
            'descendantOrInstallationAuthority': False}


def qualify_nonzero_owner(owner, *, cwd, env, uid=None, gid=None):
    """Real failure-path proof on a separate owner that stays permanently failed."""
    require(not owner.owners and not owner.failed, 'negative_preflight_owner_already_used')
    try:
        owner.run('owner-preflight-nonzero', ['/usr/bin/false'], cwd=cwd, env=env,
                  seconds=10, uid=uid, gid=gid)
    except CustodyFailure as failure:
        require(str(failure) == 'command_nonzero', 'negative_preflight_wrong_failure')
    else:
        raise CustodyFailure('negative_preflight_unexpected_success')
    require(owner.failed and len(owner.owners) == 1, 'negative_preflight_owner_not_failed')
    row = owner.owners[0].row
    require(row['started'] and row['exitObserverRegistered'] and row['execGateReleased']
            and row['terminalObservedWithoutReap'] and row['observedExitCode'] == 1
            and type(row['observedRawWaitStatus']) is int
            and row['rawWaitStatus'] == row['observedRawWaitStatus']
            and row['leaderReaped'] and row['exitCode'] == 1 and row['groupGone']
            and not row['success'] and row['primaryFailure'] == 'command_nonzero'
            and row['cleanupFailure'] is None and not owner.owners[0].unproved
            and not any(key in row for key in ('notificationCleanupFailure', 'observerCloseFailure',
                                               'logCloseFailure', 'logObservationFailure')),
            'negative_preflight_closure_unproved')
    # Signal errors remain in groupSignals. They grant no custody; the exact
    # actual status/reap/group absence above is the closure evidence. No failed
    # owner is reset or reused for a successful product/admin command.
