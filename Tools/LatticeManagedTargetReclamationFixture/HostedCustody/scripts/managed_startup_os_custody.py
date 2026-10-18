"""Finite hosted OS-service custody. Never a product descendant join.

The native facts parser returns DATA only. A permission/ending additionally
requires this guardian's actual compiled helper command and, for agents, its
actual documented launchctl job/PID enrollment command.
"""
from __future__ import annotations
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time

from managed_startup_command_owner import CustodyFailure, bounded_regular, exception_fact, file_fact, require

AGENTS = {
    "com.apple.distnoted.xpc.agent", "com.apple.lsd", "com.apple.cfprefsd.xpc.agent",
    "com.apple.trustd.agent", "com.apple.secd", "com.apple.containermanagerd",
}
APPLICATION = "com.apple.filesystems.netfs.PlugInLibraryService"
PREPRODUCT_STAGES = {"account-setup", "source-checkout", "resolve", "build-memory", "sign-and-package"}
ROOT_KEYS = {"schemaVersion", "mode", "qualified", "productDescendantJoin", "processes", "firstFailure"}
PROCESS_KEYS = {
    "pid", "uid", "auditToken", "pidVersion", "startSeconds", "startMicroseconds",
    "executable", "identifier", "cdhash", "codeStatus", "descriptor", "jobLabel", "kind",
    "exitRegistered", "registeredExitObserved", "signals", "firstFailure",
}


def unique_json(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            require(key not in value, "os_custody_duplicate_json_key")
            value[key] = item
        return value
    require(type(raw) is bytes and 0 < len(raw) <= 1024 * 1024, "os_custody_json_bound")
    return json.loads(raw, object_pairs_hook=pairs,
                      parse_constant=lambda _: (_ for _ in ()).throw(CustodyFailure("os_custody_nonfinite_json")))


def parse_native_data(raw, mode, uid, pids):
    """Strict shape/association DATA. Calling this does not issue custody."""
    require(mode in ("observe", "preproduct-census") and type(uid) is int and 60000 <= uid < 80000
            and type(pids) is list and 0 < len(pids) <= 64
            and all(type(pid) is int and 1 < pid < 2**31 for pid in pids), "os_custody_requested_inventory")
    value = unique_json(raw)
    require(type(value) is dict and set(value) == ROOT_KEYS and value["schemaVersion"] == 1
            and type(value["schemaVersion"]) is int and value["mode"] == mode
            and value["qualified"] is True and value["productDescendantJoin"] is False
            and value["firstFailure"] is None, "os_custody_native_failure_or_shape")
    rows = value["processes"]
    require(type(rows) is list and 0 < len(rows) <= 64
            and len(rows) == len(pids) == len(set(pids)), "os_custody_native_inventory")
    observed = {}
    for row in rows:
        require(type(row) is dict and set(row) == PROCESS_KEYS, "os_custody_process_shape")
        require(all(type(row[key]) is int for key in (
            "pid", "uid", "pidVersion", "startSeconds", "startMicroseconds", "codeStatus")),
            "os_custody_integer_type")
        require(row["pid"] in pids and row["pid"] not in observed and row["uid"] == uid
                and row["pid"] > 1 and row["pidVersion"] != 0 and -2**31 <= row["pidVersion"] < 2**31
                and 0 < row["startSeconds"] < 2**64 and 0 <= row["startMicroseconds"] < 1000000,
                "os_custody_process_association")
        require(type(row["auditToken"]) is str and re.fullmatch(r"[0-9a-f]{64}", row["auditToken"])
                and type(row["cdhash"]) is str and re.fullmatch(r"(?:[0-9a-f]{2}){16,64}", row["cdhash"]),
                "os_custody_incarnation_signature_bytes")
        require(0 <= row["codeStatus"] < 2**32 and row["codeStatus"] & 0x1 and row["codeStatus"] & 0x04000000
                and not row["codeStatus"] & 0x10000000, "os_custody_platform_signature")
        for key in ("executable", "identifier", "descriptor", "jobLabel", "kind"):
            require(type(row[key]) is str and 0 < len(row[key]) <= 4096
                    and "\0" not in row[key], "os_custody_string_bound")
        require(row["executable"].startswith(("/usr/", "/System/Library/"))
                and row["firstFailure"] is None, "os_custody_native_image_or_first_fault")
        if row["kind"] == "background-agent":
            require(row["jobLabel"] in AGENTS
                    and row["descriptor"] == "/System/Library/LaunchAgents/" + row["jobLabel"] + ".plist"
                    and row["exitRegistered"] is False and row["registeredExitObserved"] is False
                    and row["signals"] == [], "os_custody_agent_shape")
        else:
            require(row["kind"] == "application-xpc" and row["jobLabel"] == APPLICATION
                    and row["descriptor"] == "/System/Library/Frameworks/NetFS.framework/Versions/A/"
                        "XPCServices/PlugInLibraryService.xpc/Contents/Info.plist",
                    "os_custody_application_scope")
            if mode == "preproduct-census":
                require(row["exitRegistered"] is True and row["registeredExitObserved"] is True
                        and type(row["signals"]) is list and row["signals"] in ([], [15], [15, 9])
                        and all(type(number) is int for number in row["signals"]), "os_custody_application_exit")
            else:
                require(row["exitRegistered"] is False and row["registeredExitObserved"] is False
                        and row["signals"] == [], "os_custody_application_exit")
        observed[row["pid"]] = row
    require(set(observed) == set(pids), "os_custody_native_inventory")
    return observed


def same_incarnation(first, current):
    return all(first[key] == current[key] for key in (
        "pid", "uid", "auditToken", "pidVersion", "startSeconds", "startMicroseconds",
        "executable", "identifier", "cdhash", "descriptor", "jobLabel", "kind"))


def kickstart_pid(raw):
    require(type(raw) is bytes and re.fullmatch(rb"[1-9][0-9]{0,9}\n", raw), "os_custody_job_pid_grammar")
    pid = int(raw)
    require(1 < pid < 2**31, "os_custody_job_pid_range")
    return pid


def parse_native_failure_data(raw):
    """Closed first-error DATA only. Never rescues the failed command owner."""
    value = unique_json(raw)
    require(type(value) is dict and set(value) == {
        "schemaVersion", "qualified", "productDescendantJoin", "firstFailure", "firstErrorDomain",
        "firstErrorCode", "observedErrno", "completedProcesses"}
        and type(value["schemaVersion"]) is int and value["schemaVersion"] == 1
        and value["qualified"] is False and value["productDescendantJoin"] is False,
        "os_custody_failure_shape")
    require(type(value["firstFailure"]) is str and 0 < len(value["firstFailure"]) <= 1024
        and type(value["firstErrorDomain"]) is str and value["firstErrorDomain"] in {"source-precondition", "posix-return", "mach-return",
            "security-return", "libproc-return", "cpp-exception"}
        and type(value["firstErrorCode"]) is int and -2**63 <= value["firstErrorCode"] < 2**63
        and type(value["observedErrno"]) is int and 0 <= value["observedErrno"] <= 65535
        and type(value["completedProcesses"]) is list and len(value["completedProcesses"]) <= 64,
        "os_custody_failure_fields")
    return {"firstFailure": value["firstFailure"] if re.fullmatch(r"[a-z0-9_]{1,96}", value["firstFailure"])
                else "native_cpp_exception",
        "firstErrorDomain": value["firstErrorDomain"], "firstErrorCode": value["firstErrorCode"],
        "observedErrno": value["observedErrno"], "completedProcessCount": len(value["completedProcesses"])}


class DarwinClock:
    """Typed SDK mach_timebase_info_data_t; no guessed audit-token layout."""
    class Info(ctypes.Structure):
        _fields_ = [("numer", ctypes.c_uint32), ("denom", ctypes.c_uint32)]

    def __init__(self):
        self.lib = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
        self.lib.mach_absolute_time.argtypes = []
        self.lib.mach_absolute_time.restype = ctypes.c_uint64
        self.lib.mach_timebase_info.argtypes = [ctypes.POINTER(self.Info)]
        self.lib.mach_timebase_info.restype = ctypes.c_int32
        self.scale = self.Info()
        require(ctypes.sizeof(self.Info) == 8 and self.lib.mach_timebase_info(ctypes.byref(self.scale)) == 0
                and self.scale.numer > 0 and self.scale.denom > 0, "os_custody_mach_clock")

    def now(self):
        result = self.lib.mach_absolute_time() * self.scale.numer // self.scale.denom
        require(0 < result < 2**64, "os_custody_mach_clock_range")
        return result


class HostedOSCustody:
    """Only real guardian commands create enrollments. First errors never reset."""
    def __init__(self, guardian):
        self.guardian = guardian
        self.sequence = 0
        self.enrollments = []
        self.applications_ended = []
        self.first_failure = None
        self.first_exception = None
        self.cleanup_failures = []
        self.native_failures = []
        self.clock = None
        self.image = guardian.private / "os-process-observer"
        self.image_fact = None
        self.source_fact = None
        self.build = None
        self.prepare_attempted = False

    def prepare(self):
        """Guardian retains this object BEFORE the first clock/file/build effect."""
        require(not self.prepare_attempted and self.first_failure is None, "os_custody_prepare_reused")
        self.prepare_attempted = True
        try:
            self._prepare()
        except BaseException as failure:
            self.remember_failure(failure)
            raise

    def _prepare(self):
        guardian = self.guardian
        self.clock = DarwinClock()
        source = guardian.source / "scripts/managed_startup_os_process_observer.cpp"
        raw = bounded_regular(source, 128 * 1024)
        frozen = guardian.private / "os-process-observer.cpp"
        fd = os.open(frozen, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
        failure = None
        try:
            at = 0
            while at < len(raw):
                count = os.write(fd, raw[at:])
                require(count > 0, "os_custody_source_short_write")
                at += count
            os.fsync(fd)
        except BaseException as caught:
            failure = caught
            self.remember_failure(caught)
        finally:
            try:
                os.close(fd)  # One attempt; an ambiguous close is not retried.
            except BaseException as caught:
                self.cleanup_failures.append(exception_fact(caught))
                if failure is None:
                    failure = caught
                    self.remember_failure(caught)
        if failure is not None:
            raise failure
        self.build = guardian.admin_run("os-observer-build", [
            "xcrun", "--sdk", "macosx", "clang++", "-std=c++20", "-O2",
            "-mmacosx-version-min=26.0", str(frozen), "-framework", "Security",
            "-framework", "CoreFoundation", "-lbsm", "-o", str(self.image)], seconds=120)
        info = self.image.lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and not info.st_mode & 0o022
                and self.image.resolve(strict=True) == self.image, "os_custody_private_image")
        self.image_fact = file_fact(self.image, 16 * 1024 * 1024)
        self.source_fact = {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}

    def label(self, kind):
        require(self.first_failure is None and self.sequence < 128, "os_custody_first_failure_or_bound")
        self.sequence += 1  # First attempt precedes command/helper effects, never cleared.
        return "os-" + kind + "-" + str(self.sequence)

    def observe(self, pids, preproduct):
        guardian = self.guardian
        require(file_fact(self.image, 16 * 1024 * 1024) == self.image_fact, "os_custody_image_changed")
        first_mach = self.clock.now()
        # Native deadline precedes this retained command and the ORIGINAL
        # guardian budget. The second clock read can only shorten its allowance.
        remaining = int((guardian.end - time.monotonic()) * 1_000_000_000) - 35_000_000_000
        budget = min(20_000_000_000, remaining)
        require(budget > 0, "os_custody_original_deadline")
        guardian.budget(budget / 1_000_000_000 + 1)
        mode = "preproduct-census" if preproduct else "observe"
        env = {**guardian.root_env, "ENGRAM_MANAGED_HOSTED_OS_CUSTODY": "1"}
        label = self.label("observe")
        before = len(guardian.admin.owners)
        try:
            row = guardian.admin.run(label, [
                str(self.image), mode, str(guardian.uid), str(first_mach + budget),
                *[str(pid) for pid in pids]], cwd=guardian.control, env=env,
                seconds=budget / 1_000_000_000 + 1, stderr_separate=True)
        except BaseException:
            if len(guardian.admin.owners) == before + 1 and guardian.admin.owners[-1].row["label"] == label:
                actual = guardian.admin.owners[-1].row
                first = {"command": actual, "nativeFirstError": None, "failureOutputUnproved": True}
                self.native_failures.append(first)
                try:
                    raw = bounded_regular(Path(actual["stdout"]), 1024 * 1024)
                    first["nativeFirstError"] = parse_native_failure_data(raw)
                    first["outputFact"] = {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
                    first["failureOutputUnproved"] = False
                except BaseException:
                    pass  # Preserve the original failed owned command, never retry or rescue it.
            raise
        require(row["success"] and row["leaderReaped"] and row["groupGone"]
                and file_fact(self.image, 16 * 1024 * 1024) == self.image_fact,
                "os_custody_observer_command_unproved")
        facts = parse_native_data(bounded_regular(Path(row["stdout"]), 1024 * 1024),
                                  mode, guardian.uid, pids)
        return facts, row

    def classify(self, records, preproduct):
        guardian = self.guardian
        try:
            require(self.first_failure is None and self.build is not None and self.image_fact is not None,
                    "os_custody_first_failure_or_unprepared")
            require(type(preproduct) is bool and (not preproduct or (
                guardian.results["stage"] in PREPRODUCT_STAGES
                and not guardian.results["caseObservations"])), "os_custody_preproduct_scope")
            require(type(records) is list and len(records) <= 64
                    and all(type(row) is dict and type(row.get("pid")) is int
                        and 1 < row["pid"] < 2**31
                        and all(type(row.get(key)) is int and row[key] == guardian.uid
                            for key in ("uid", "ruid", "svuid")) for row in records)
                    and len({row["pid"] for row in records}) == len(records), "os_custody_census_bound")
            if not records:
                return []
            facts, observation = self.observe([row["pid"] for row in records], preproduct)
            pending = []
            for row in records:
                current = facts[row["pid"]]
                if current["kind"] == "application-xpc":
                    require(preproduct and current["exitRegistered"] and current["registeredExitObserved"],
                            "application_runtime_custody_unproved")
                    self.applications_ended.append({"facts": current, "nativeCommand": observation,
                        "scope": "exact-preproduct-test-helper-only", "productDescendantJoin": False})
                    continue
                existing = [item for item in self.enrollments if same_incarnation(item["facts"], current)]
                if existing:
                    continue
                require(preproduct and len(self.enrollments) < 64, "new_os_job_runtime_or_bound_unproved")
                target = "user/" + str(guardian.uid) + "/" + current["jobLabel"]
                command = guardian.admin_run(self.label("job"), ["/bin/launchctl", "kickstart", "-p", target], seconds=10)
                require(kickstart_pid(bounded_regular(Path(command["stdout"]), 128)) == row["pid"],
                        "os_custody_launch_job_pid_mismatch")
                pending.append((current, command, target))
            if pending:
                after, command = self.observe([row[0]["pid"] for row in pending], False)
                for original, launch, target in pending:
                    require(same_incarnation(original, after[original["pid"]]),
                            "os_custody_launch_job_incarnation_changed")
                    self.enrollments.append({"facts": original, "target": target, "launchCommand": launch,
                        "beforeNativeCommand": observation, "afterNativeCommand": command,
                        "lifetime": "same-background-agent-incarnation-through-this-disabled-account-guardian",
                        "productDescendantJoin": False, "jobConfigurationOrProductCleanupAccepted": False})
            return [{"pid": row["pid"], "auditToken": facts[row["pid"]]["auditToken"],
                "disposition": "actual-preproduct-helper-exit" if facts[row["pid"]]["kind"] == "application-xpc"
                    else "actual-background-job-enrollment",
                "productDescendantJoin": False} for row in records]
        except BaseException as failure:
            self.remember_failure(failure)
            raise

    def remember_failure(self, failure):
        if self.first_failure is None:
            self.first_failure = str(failure) if isinstance(failure, CustodyFailure) else "os_custody_exception"
            self.first_exception = exception_fact(failure)

    def receipt(self):
        return {"scope": "hosted-test-environment-os-services-only", "source": self.source_fact,
            "helperImage": self.image_fact, "actualBuildCommand": self.build,
            "agentEnrollments": self.enrollments, "applicationHelperEndings": self.applications_ended,
            "firstFailure": self.first_failure, "firstException": self.first_exception,
            "nativeFailures": self.native_failures, "cleanupFailures": self.cleanup_failures,
            "productDescendantJoin": False,
            "accountDeletionOrVMTeardownIsClosureProof": False}
