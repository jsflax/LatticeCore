#!/usr/bin/env python3
"""Hosted observation of one unchanged real-product qualification case.

This driver owns no native installation authority. It observes the fixture's
actual Popen objects and native post-wait records; it never replaces a wait,
extends a test deadline, repairs a store, or infers reaping from a process scan.
"""
from __future__ import annotations
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import signal
import stat
import struct
import subprocess
import sys
import types
import unittest


ORIGIN = "test_managed_installation_origin.ActualManagedInstallationOrigin."
NORMAL = "test_managed_normal_startup.ActualManagedNormalStartup."
# Ordered actual fixture invocations, followed by the native gates that must
# contain the actual controller-owned child waits on a passing execution.
CASES = {
    ORIGIN + "test_actual_product_registers_exact_closed_origin":
        (("create-engram", "create-engram"), ("seed",)),
    ORIGIN + "test_unstamped_core_launcher_cannot_register":
        (("create-engram",), ()),
    ORIGIN + "test_actual_package_rejects_changed_memory_before_namespace_creation":
        (("create-engram",), ()),
    ORIGIN + "test_actual_package_rejects_current_provenance_revision_replacement":
        (("create-engram",), ()),
    ORIGIN + "test_actual_package_rejects_initializer_symlink_before_child":
        (("create-engram",), ()),
    ORIGIN + "test_existing_namespace_never_becomes_registration":
        (("create-engram",), ()),
    ORIGIN + "test_external_anchor_collision_preserves_original_failure_and_bytes":
        (("create-engram",), ("seed",)),
    NORMAL + "test_actual_managed_mcp_keeps_original_schema_and_committed_checkpoint_behavior":
        (("create-and-run-engram-mcp", "create-and-run-engram-mcp"), ("seed", "normal")),
    NORMAL + "test_direct_normal_flag_and_environment_cannot_issue_context_or_open_store":
        (("direct-normal-receiver",), ()),
    NORMAL + "test_separate_restarted_issuer_cannot_admit_a_registered_unadopted_origin":
        (("create-engram", "create-and-run-engram-mcp"), ("seed",)),
    NORMAL + "test_missing_installed_provenance_cannot_create_a_normal_origin":
        (("create-and-run-engram-mcp",), ()),
    NORMAL + "test_stale_installed_provenance_cannot_create_a_normal_origin":
        (("create-and-run-engram-mcp",), ()),
    NORMAL + "test_self_consistent_copied_package_cannot_enroll_itself":
        (("create-and-run-engram-mcp",), ()),
}


def require(value):
    if not value:
        raise RuntimeError("managed case observation unqualified")


def regular(path, maximum):
    require(path.is_absolute() and path.resolve(strict=True) == path)
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1
                and before.st_uid == os.geteuid() and 0 < before.st_size <= maximum)
        data = bytearray()
        while len(data) < before.st_size:
            block = os.read(fd, min(65536, before.st_size - len(data)))
            require(block)
            data.extend(block)
        require(not os.read(fd, 1))
        def stable(value):
            return (value.st_dev, value.st_ino, value.st_mode, value.st_uid,
                    value.st_nlink, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
        require(stable(before) == stable(os.fstat(fd)) == stable(path.lstat()))
        return bytes(data)
    finally:
        os.close(fd)


def save(path, value):
    raw = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    require(len(raw) <= 256 * 1024)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        at = 0
        while at != len(raw):
            wrote = os.write(fd, raw[at:])
            require(wrote > 0)
            at += wrote
        os.fsync(fd)
    finally:
        os.close(fd)


class Registry:
    def __init__(self, expected, cli, evidence, unstamped):
        self.expected, self.cli, self.evidence, self.unstamped = expected, cli, evidence, unstamped
        self.objects, self.rows = [], []
        self.observation_failed = False

    def reserve(self, argv, options):
        # Popen treats ECHILD as exit zero when SIGCHLD is ignored. The
        # source-bound driver must retain the default disposition and remain
        # the exclusive reaper; otherwise its return code cannot prove a wait.
        require(signal.getsignal(signal.SIGCHLD) == signal.SIG_DFL)
        require(len(self.rows) < len(self.expected) <= 2 and type(argv) is list
                and all(type(value) is str for value in argv)
                and options.get("start_new_session") is True)
        executable = Path(argv[0])
        require(executable.is_absolute())
        if len(argv) == 2:
            require(executable == self.cli / "memory" and argv[1] == "--lattice-managed-mcp-v1")
            role = "direct-normal-receiver"
        else:
            require(len(argv) == 4 and argv[1] in ("create-engram", "create-and-run-engram-mcp"))
            require(executable == self.cli / "memory-installation-launcher"
                    or executable == self.unstamped
                    or (executable.is_relative_to(self.evidence)
                        and executable.name == "memory-installation-launcher"))
            parent = Path(argv[2])
            require(parent.is_relative_to(self.evidence) and parent.resolve(strict=True) == parent
                    and argv[3] == "owned")
            role = argv[1]
        require(role == self.expected[len(self.rows)])
        row = {"index": len(self.rows), "role": role, "started": False, "pid": None,
               "parentPID": os.getpid(), "actualWaitObserved": False, "exitCode": None,
               "waitEvents": [], "groupAbsentAfterReap": False}
        self.rows.append(row)
        return row

    def observed(self, row, process, operation, result):
        # This is observation only. Failure must not replace the original
        # wait/poll return value or exception in the unchanged fixture.
        try:
            require(operation in ("wait", "poll") and len(row["waitEvents"]) < 64
                    and (result is None or type(result) is int))
            row["waitEvents"].append({"operation": operation, "result": result})
            if result is not None:
                require(process.returncode == result)
                if row["actualWaitObserved"]:
                    require(row["exitCode"] == result)
                row["actualWaitObserved"], row["exitCode"] = True, result
        except BaseException:
            self.observation_failed = True

    def factory(self):
        registry = self
        class ObservedPopen(subprocess.Popen):
            def __init__(self, argv, *args, **kwargs):
                require(not args)
                self.observation_row = registry.reserve(argv, kwargs)
                # A constructor interrupted before it returns has no positive
                # closure. The reserved unfinished row remains a refusal fact.
                super().__init__(argv, **kwargs)
                self.observation_row.update(started=True, pid=self.pid)
                registry.objects.append(self)  # Retain the actual exclusive owner.

            def wait(self, *args, **kwargs):
                result = super().wait(*args, **kwargs)
                registry.observed(self.observation_row, self, "wait", result)
                return result

            def poll(self):
                result = super().poll()
                registry.observed(self.observation_row, self, "poll", result)
                return result
        return ObservedPopen

    def finish(self):
        require(signal.getsignal(signal.SIGCHLD) == signal.SIG_DFL)
        require(not self.observation_failed and len(self.rows) == len(self.expected)
                and len(self.objects) == len(self.rows))
        for row, process in zip(self.rows, self.objects):
            require(row["started"] and row["pid"] == process.pid and row["actualWaitObserved"]
                    and type(row["exitCode"]) is int and process.returncode == row["exitCode"])
            # Read-only after actual fixture reap. Never signal a remembered
            # process/group here. PID reuse can refuse, never grant closure.
            try:
                os.killpg(process.pid, 0)
            except ProcessLookupError:
                row["groupAbsentAfterReap"] = True
            require(row["groupAbsentAfterReap"])


def native_gate(path, kind, launcher_pid):
    raw = regular(path, 4096)
    require(kind in ("seed", "normal") and len(raw) == (236 if kind == "seed" else 208)
            and raw[:8] == b"LATCOH1\0" and hashlib.sha256(raw[:-32]).digest() == raw[-32:]
            and not os.path.lexists(path.parent / "launch.pending"))
    def number(at):
        return struct.unpack_from("<Q", raw, at)[0]
    require(number(8) == 1 and number(16) > 0 and number(24) == 3)
    parent, directory, lock = path.parent.parent.lstat(), path.parent.lstat(), (path.parent / "launch.lock").lstat()
    require(stat.S_ISDIR(parent.st_mode) and stat.S_ISDIR(directory.st_mode)
            and stat.S_ISREG(lock.st_mode) and lock.st_nlink == 1
            and all(value.st_uid == os.geteuid() for value in (parent, directory, lock)))
    for at, actual in ((32, parent), (48, directory), (64, lock)):
        require((number(at), number(at + 8)) == (actual.st_dev, actual.st_ino))
    require(number(80) == launcher_pid and number(88) == os.getpid() and number(96) > 0)
    if kind == "seed":
        data = (path.parent / "data").lstat()
        require(stat.S_ISDIR(data.st_mode) and data.st_uid == os.geteuid()
                and number(112) == 1 and (number(120), number(128)) == (data.st_dev, data.st_ino)
                and number(136) == 4 and raw[144:148] == b"data")
        children = 148
    else:
        require(number(112) == 0)
        children = 120
    require(number(children) == 1 and number(children + 8) == 2
            and number(children + 16) > 0 and number(children + 24) == launcher_pid
            and number(children + 32) > 0 and number(children + 48) == 0)
    return {"role": kind, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
            "recordHex": raw.hex(), "ownerPID": launcher_pid, "childPID": number(children + 16),
            "childBirthMajor": number(children + 32), "childBirthMinor": number(children + 40),
            "actualWaitStatus": number(children + 48), "durablePhase": 3}


def gates(case, expected, registry):
    require(hasattr(case, "parent") and case.parent.is_relative_to(registry.evidence))
    fixed = {"seed": case.parent / "owned/launch.v1",
             "normal": case.parent / "owned/normal-runtime/launch.v1"}
    observed = []
    for kind, path in fixed.items():
        require(os.path.lexists(path) == (kind in expected))
        if kind in expected:
            observed.append(native_gate(path, kind, registry.rows[0]["pid"]))
    require([row["role"] for row in observed] == list(expected))
    return observed


class Result(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.outcomes = []

    def addSuccess(self, test):
        self.outcomes.append("passed")
        super().addSuccess(test)

    def addFailure(self, test, err):
        self.outcomes.append("failed")
        super().addFailure(test, err)

    def addError(self, test, err):
        self.outcomes.append("error")
        super().addError(test, err)

    def addSkip(self, test, reason):
        self.outcomes.append("skipped")
        super().addSkip(test, reason)

    def addExpectedFailure(self, test, err):
        self.outcomes.append("expected_failure")
        super().addExpectedFailure(test, err)

    def addUnexpectedSuccess(self, test):
        self.outcomes.append("unexpected_success")
        super().addUnexpectedSuccess(test)


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    require(spec is not None and spec.loader is not None and name not in sys.modules)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def main():
    require(platform.system() == "Darwin" and os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted"
            and os.environ.get("ENGRAM_MANAGED_HOSTED_GATE") == "1"
            and os.geteuid() != 0 and len(sys.argv) == 3 and sys.argv[1] in CASES)
    require(signal.getsignal(signal.SIGCHLD) == signal.SIG_DFL)
    name, output = sys.argv[1], Path(sys.argv[2])
    root = Path(__file__).resolve().parents[1]
    cli = Path(os.environ["ENGRAM_INSTALLATION_CLI"]).resolve(strict=True)
    evidence = Path(os.environ["ENGRAM_INSTALLATION_EVIDENCE"]).resolve(strict=True)
    unstamped = Path(os.environ["ENGRAM_UNSTAMPED_INSTALLER"]).resolve(strict=True)
    require(output.is_absolute() and output.parent.resolve(strict=True) == output.parent
            and output.parent.is_relative_to(evidence) and not output.exists())
    expected, native_expected = CASES[name]
    registry = Registry(expected, cli, evidence, unstamped)
    receipt = {"schemaVersion": 1, "case": name, "runtimeAuthority": False,
               "testOutcomes": [], "fixtureProcesses": registry.rows, "nativeGates": [],
               "custodyQualified": False, "qualified": False, "observationError": None}
    originals = {}
    result = None
    try:
        origin = load(root / "Tests/test_managed_installation_origin.py", "test_managed_installation_origin")
        normal = load(root / "Tests/test_managed_normal_startup.py", "test_managed_normal_startup")
        facade = types.SimpleNamespace(Popen=registry.factory(), TimeoutExpired=subprocess.TimeoutExpired,
                                       PIPE=subprocess.PIPE, DEVNULL=subprocess.DEVNULL)
        for module in (origin, normal):
            originals[module] = module.subprocess
            module.subprocess = facade
        module_name, class_name, method = name.split(".")
        module = origin if module_name == "test_managed_installation_origin" else normal
        case = getattr(module, class_name)(method)
        result = unittest.TextTestRunner(stream=sys.stderr, verbosity=2, resultclass=Result).run(unittest.TestSuite([case]))
        receipt["testOutcomes"] = result.outcomes
        # Failure observations survive; neither cleanup nor an empty census can
        # convert the original test result into a qualifying one.
        registry.finish()
        receipt["nativeGates"] = gates(case, native_expected, registry)
        receipt["custodyQualified"] = True
        receipt["qualified"] = result.testsRun == 1 and result.outcomes == ["passed"] and result.wasSuccessful()
    except BaseException:
        receipt["observationError"] = "unproved_case_observation"
    finally:
        for module, value in originals.items():
            module.subprocess = value
        if result is not None:
            receipt["testOutcomes"] = result.outcomes
        save(output, receipt)
    return 0 if receipt["qualified"] else 1


if __name__ == "__main__":
    sys.exit(main())
