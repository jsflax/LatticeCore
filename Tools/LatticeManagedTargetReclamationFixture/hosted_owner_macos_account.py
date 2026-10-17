#!/usr/bin/env python3
"""SOURCE ONLY: one genuine hosted owner invocation, never run on a shared Mac.

The current composed graph is a hashed build input, not an invented Git commit.
The real builder observes its new revision and actual signed image hashes.
Historical thirteen MCP cases and fixed four-image legacy qualification remain
separate suppliers; neither is claimed by this entry's OS-service custody.
"""
from __future__ import annotations
import argparse
from contextlib import contextmanager
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import pwd
import platform
import re
import signal
import stat
import sys
import threading
import time

_HELD = None  # One actual owner retained even on failure; no clear/retry API.


def demand(value, code):
    if not value:
        raise RuntimeError(code)


def option(argv, flag):
    places = [n for n, value in enumerate(argv) if value == flag]
    demand(len(places) == 1 and places[0] + 1 < len(argv), "one_exact_assembly_input_required")
    return argv[places[0] + 1]


def source_identity(observed):
    return (observed.st_dev, observed.st_ino, observed.st_uid, observed.st_gid,
            observed.st_mode, observed.st_nlink, observed.st_size,
            observed.st_mtime_ns, observed.st_ctime_ns)


@contextmanager
def source_read_descriptor(path, directory=False):
    # No symlink following or FIFO/device wait before fstat. Own precisely this
    # descriptor; a failed close is never retried or represented as success.
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
    if directory:
        flags |= os.O_DIRECTORY
    descriptor = os.open(path, flags)
    try:
        yield descriptor
    except BaseException as first_failure:
        try:
            os.close(descriptor)
        except BaseException as close_failure:
            raise first_failure from close_failure
        raise
    else:
        os.close(descriptor)


def bounded_source_bytes(path, limit):
    # The size/type/identity checks precede allocation. The extra byte detects
    # growth; both fd and path facts must still match the original observation.
    first = path.lstat()
    demand(stat.S_ISREG(first.st_mode) and first.st_nlink == 1
           and 0 <= first.st_size <= limit, "source_read_size_or_kind")
    with source_read_descriptor(path) as descriptor:
        demand(source_identity(first) == source_identity(os.fstat(descriptor)), "source_read_open_identity")
        # The outer descriptor owner is the only closer, including fdopen
        # allocation failure; the bounded buffered reader never owns the fd.
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            body = stream.read(first.st_size + 1)
            demand(len(body) == first.st_size
                   and source_identity(first) == source_identity(os.fstat(descriptor)), "source_read_size_or_identity_changed")
    demand(source_identity(first) == source_identity(path.lstat()), "source_read_path_identity_changed")
    return body


def bounded_source_inventory(graph):
    # scandir consumes the owned directory fd incrementally. Each queued
    # directory carries its original identity; never enumerate a later alias.
    initial = graph.lstat()
    demand(stat.S_ISDIR(initial.st_mode), "source_root_directory_required")
    files, pending, directories, visited = {}, [(graph, initial)], [], 0
    while pending:
        directory, first = pending.pop()
        demand(source_identity(first) == source_identity(directory.lstat()), "source_directory_identity_changed")
        with source_read_descriptor(directory, directory=True) as descriptor:
            demand(source_identity(first) == source_identity(os.fstat(descriptor)), "source_directory_open_identity")
            with os.scandir(descriptor) as entries:
                for entry in entries:
                    visited += 1
                    demand(visited <= 100000, "source_path_inventory_bound")
                    path = directory / entry.name
                    observed = entry.stat(follow_symlinks=False)
                    demand(not stat.S_ISLNK(observed.st_mode), "source_alias_refused")
                    if stat.S_ISDIR(observed.st_mode):
                        pending.append((path, observed))
                    else:
                        demand(stat.S_ISREG(observed.st_mode) and observed.st_nlink == 1, "source_member_kind")
                        demand(len(files) < 50000, "source_file_inventory_bound")
                        files[str(path.relative_to(graph))] = path
            demand(source_identity(first) == source_identity(os.fstat(descriptor))
                   and source_identity(first) == source_identity(directory.lstat()), "source_directory_enumeration_changed")
        directories.append((directory, source_identity(first)))
    # Retain scalar identities, never fd/file objects, until hashes finish.
    return files, directories


def frozen_graph(argv):
    graph = Path(option(argv, "--graph")).absolute()
    seal_path = Path(option(argv, "--graph-pins")).absolute()
    pin = option(argv, "--graph-pins-sha256")
    demand(graph.resolve(strict=True) == graph and seal_path.resolve(strict=True) == seal_path
           and "localdev" in graph.parts and re.fullmatch(r"[0-9a-f]{64}", pin), "canonical_source_inputs_required")
    raw = bounded_source_bytes(seal_path, 16 * 1024 * 1024)
    demand(len(raw) > 0 and hashlib.sha256(raw).hexdigest() == pin, "source_seal_changed")
    def unique(items):
        out = {}
        for key, value in items:
            demand(key not in out, "source_seal_duplicate_key")
            out[key] = value
        return out
    value = json.loads(raw, object_pairs_hook=unique)
    demand(value.get("format") == "owner-sealed-source-graph-v1", "source_seal_format")
    rows = value["files"]
    demand(type(rows) is list and 0 < len(rows) <= 50000, "source_inventory_bound")
    by_path = {row["path"]: row for row in rows}
    files, directories = bounded_source_inventory(graph)
    demand(len(by_path) == len(rows) and set(files) == set(by_path), "source_inventory_changed")
    for name, path in files.items():
        first = path.lstat()
        demand(first.st_size == by_path[name]["bytes"], "source_member_size_changed")
        body = bounded_source_bytes(path, 512 * 1024 * 1024)
        demand(source_identity(first) == source_identity(path.lstat())
               and len(body) == by_path[name]["bytes"]
               and hashlib.sha256(body).hexdigest() == by_path[name]["sha256"], "source_member_changed")
    for directory, identity in directories:
        demand(identity == source_identity(directory.lstat()), "source_directory_changed_during_hashes")
    here = Path(__file__).absolute()
    demand(here.resolve(strict=True) == here and here.is_relative_to(graph)
           and str(here.relative_to(graph)) in by_path, "entry_not_in_actual_sealed_graph")
    return graph, pin


def import_custody(graph):
    root = graph / "Core/Tools/LatticeManagedTargetReclamationFixture/HostedCustody"
    scripts = root / "scripts"
    # These actual005 and real sibling bodies are unchanged; old CORE/SDK and
    # thirteen-case paths are not rewritten or invoked as a current owner run.
    expected = {'managed_startup_artifact_ops.py': '632fc8c3f303241734d48efc22b3d059bc4e03ceee45e5cd7c30a189673dbc2e', 'managed_startup_case_observer.py': 'de0c00c121bae74bfa1c684565f6b1da49f205658949c4029e219ef58c125d42', 'managed_startup_command_owner.py': 'aab985543261f94b181a367bce56275c3203048bc2d2030b6e2708ecfca95f1b', 'managed_startup_exec_gate.py': 'b69a4a80b6b2c18653b52f26915ec9e21a0c8c90d41402915d5ec06559a8d8b8', 'managed_startup_hosted.py': 'a2a3883b2b5c8ae3fdf0da55ff6c2bcd5bc4a29fe99a3025ab2f99f81b8dbe8a', 'managed_startup_os_custody.py': 'b09261f1d2739f8e09e74a52d67522fa8fb599d6c5109d7ad379f7746d325de6', 'managed_startup_os_process_observer.cpp': '200816592776f075710aeecb9dba1a1b839d9ca1068979858f670b4da452d39f'}
    for leaf, pin in expected.items():
        path = scripts / leaf
        demand(path.is_file() and not path.is_symlink()
               and hashlib.sha256(bounded_source_bytes(path, 512 * 1024 * 1024)).hexdigest() == pin, "exact_guardian_custody_source_changed")
    sys.path.insert(0, str(scripts))
    import managed_startup_hosted as original
    return original, root


class OwnerImageGuardian:
    """Same root owner through actual build/account/user invocation and failure."""
    def __init__(self, original, custody_source, graph, source_pin, harness_commit, control, runner_uid):
        self.original, self.source, self.graph = original, custody_source, graph
        self.source_pin, self.harness_commit = source_pin, harness_commit
        self.control, self.runner_uid = control, runner_uid
        runner = pwd.getpwuid(runner_uid)
        self.work_base = Path(runner.pw_dir) / "localdev"
        self.process, self.thread = os.getpid(), threading.get_ident()
        self.end = time.monotonic() + original.BUDGET_SECONDS  # Original12600, never renewed.
        self.stop = original.StopFlag()
        self.admin = self.product = self.os_custody = None
        self.private = self.public = None
        self.negative_owners, self.negative_observations = {}, {}
        self.sequence = 0
        self.reserved = self.started = self.account_attempted = self.product_begun = False
        self.account = None
        self.uid = self.gid = self.home = self.work = None
        self.args = None
        self.build_graph = self.build_source = None
        self.build_capture_attempted = False
        self.build_checks = []
        self.provider = None
        self.results = {"schemaVersion": 1, "scope": "current-owner-invocation-and-test-os-services-only",
            "stage": "account-setup", "caseObservations": [], "primaryFailure": None,
            "primaryException": None, "cleanupFailures": [], "qualified": False,
            "directInvocationCustodyQualified": False, "selectedNativeObservationPassed": False,
            "wholeOwnerCollectionQualified": False, "wholeCohortRetirementQualified": False,
            "historical13CasesQualified": False, "fixedLegacyFourRolesQualified": False,
            "providerLifecycleQualified": False, "performanceAccepted": False, "releaseAccepted": False,
            "runtimeAuthorityFromReceipts": False, "harnessWorkflowCommit": harness_commit,
            "sourceGraphSealSHA256": source_pin, "productGraphCommitInvented": False,
            "controllerFirstOutcome": None, "finalAccountCensusAttempted": False}

    def current(self):
        self.original.require(self.process == os.getpid() and self.thread == threading.get_ident()
                              and os.geteuid() == 0, "owner_guardian_creator_refusal")

    def budget(self, seconds):
        self.current()
        self.original.require(not self.stop.requested and time.monotonic() + seconds + 30 <= self.end,
                              "guardian_budget_or_cancel_refusal")

    # Exact source methods, no relaxed process names/PIDs/OS enrollment rules.
    def census(self, label):
        self.current()
        return self.original.Guardian.census(self, label)

    def negative_preflight(self, role, **values):
        self.current()
        return self.original.Guardian.negative_preflight(self, role, **values)

    def admin_run(self, label, argv, *, cwd=None, seconds=30, stdout=None, separate=False):
        self.budget(seconds)
        return self.admin.run(label, [str(x) for x in argv], cwd=self.control if cwd is None else cwd,
            env=self.root_env, seconds=seconds, stdout_path=stdout, stderr_separate=separate)

    def reserve_control(self):
        self.current()
        self.original.require(not self.reserved, "owner_control_reused")
        self.reserved = True
        self.stop.install()
        self.original.directory(self.control, mode=0o711)
        self.public = self.original.directory(self.control / "public", mode=0o755)
        self.private = self.original.directory(self.control / "private")
        receipts = self.original.directory(self.control / "admin-receipts")
        tmp = self.original.directory(self.control / "tmp")
        self.root_env = {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "HOME": pwd.getpwuid(0).pw_dir,
            "USER": "root", "LOGNAME": "root", "LANG": "en_US.UTF-8", "LC_ALL": "en_US.UTF-8",
            "TMPDIR": str(tmp), "PYTHONDONTWRITEBYTECODE": "1", "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_TERMINAL_PROMPT": "0"}
        for key in ("DEVELOPER_DIR", "SDKROOT"):
            if key in os.environ:
                self.root_env[key] = os.environ[key]
        self.admin = self.original.CommandOwner(self.stop, receipts, self.private)

    def start_verified_inputs(self, args, source):
        self.current()
        self.original.require(self.reserved and self.admin is not None and not self.started
                              and source == self.graph and args.root.parent == self.work_base
                              and self.control != args.root and not self.control.is_relative_to(args.root)
                              and not args.root.is_relative_to(self.control), "owner_guardian_input_binding")
        self.started = True  # Before first child/clock/build effect; never retried.
        self.args = args
        # Original real positive and permanently-failed negative command owners
        # precede even the diagnostic Native observer build.
        self.original.save(self.public / "root-owner-preflight.json", self.original.qualify_owner(
            self.admin, cwd=self.control, env=self.root_env))
        self.negative_preflight("root", cwd=self.control, env=self.root_env)
        self.os_custody = self.original.HostedOSCustody(self)  # Retained BEFORE prepare.
        self.os_custody.prepare()

    def bind_created_account(self, name, uid, gid, home, work):
        self.current()
        self.original.require(self.started and not self.account_attempted, "owner_account_binding_reused")
        self.account_attempted = True
        self.account = {"name": name, "uid": uid, "gid": gid, "home": str(home), "work": str(work),
                        "retainedUntilVMTeardown": True}
        actual = pwd.getpwnam(name)
        self.original.require(60000 <= uid < 80000 and gid > 0 and actual.pw_uid == uid
            and actual.pw_gid == gid and actual.pw_dir == str(home) and name.startswith("lattice-owner-")
            and Path(actual.pw_dir) == Path("/Users") / name and work.parent.parent == self.work_base
            and work.resolve(strict=True) == work and work.lstat().st_uid == uid,
            "owner_actual_account_binding_unproved")
        self.uid, self.gid, self.home, self.work = uid, gid, home, work
        self.results["stage"] = "source-checkout"
        self.original.save(self.public / "actual-owner-account.json", self.account)
        receipts = self.original.directory(self.control / "product-receipts")
        private = self.original.directory(self.control / "product-private")
        self.product = self.original.CommandOwner(self.stop, receipts, private)
        env = self.user_env(work / "scratch")
        self.original.save(self.public / "dedicated-owner-preflight.json", self.original.qualify_owner(
            self.product, cwd=work, env=env, uid=uid, gid=gid))
        self.negative_preflight("dedicated", cwd=work, env=env, uid=uid, gid=gid)
        self.census("initial-owner")

    def user_env(self, scratch):
        self.current()
        actual = pwd.getpwnam(self.account["name"])
        self.original.require(actual.pw_uid == self.uid and actual.pw_gid == self.gid
            and actual.pw_dir == str(self.home), "owner_account_changed")
        return {**self.root_env, "HOME": actual.pw_dir, "USER": actual.pw_name, "LOGNAME": actual.pw_name,
            "TMPDIR": str(scratch), "CLANG_MODULE_CACHE_PATH": str(scratch / "clang-module-cache"),
            "SWIFTPM_MODULECACHE_OVERRIDE": str(scratch / "clang-module-cache"),
            "GITHUB_ACTIONS": "true", "RUNNER_ENVIRONMENT": "github-hosted"}

    def begin_product(self):
        self.current()
        self.original.require(self.product is not None and not self.product_begun, "owner_product_phase_reused")
        self.product_begun = True
        self.results["stage"] = "actual-owner-product"  # Never restored by a tool/build label.
        if self.provider is not None:
            self.provider.start()  # Actual service effects only AFTER the permanent boundary.

    def run(self, argv, user=None, cwd=None, timeout=600, *, scratch):
        self.current()
        self.original.require(self.started and self.sequence < 128, "owner_command_first_bound")
        self.sequence += 1
        self.budget(timeout)
        scratch = Path(scratch)
        self.original.require(scratch.is_dir() and not scratch.is_symlink()
            and scratch.resolve(strict=True) == scratch and scratch.is_relative_to(self.work_base), "owner_scratch_binding")
        argv = [str(x) for x in argv]
        context = {"sourceGraphSealSHA256": self.source_pin, "harnessWorkflowCommit": self.harness_commit}
        label = "owner-root-" + str(self.sequence) if user is None else "owner-user-" + str(self.sequence)
        if user is None:
            env = {**self.root_env, "TMPDIR": str(scratch),
                   "CLANG_MODULE_CACHE_PATH": str(scratch / "clang-module-cache"),
                   "SWIFTPM_MODULECACHE_OVERRIDE": str(scratch / "clang-module-cache")}
            row = self.admin.run(label, argv, cwd=cwd, env=env, seconds=timeout,
                                 context=context, stderr_separate=True)
        else:
            self.original.require(self.product is not None and user == self.account["name"], "owner_user_not_bound")
            row = self.product.run(label, argv, cwd=cwd, env=self.user_env(scratch), seconds=timeout,
                uid=self.uid, gid=self.gid, context=context, stderr_separate=True)
        # Preserve capture_output semantics: Git/Swift/dscl parsers never read
        # merged stderr. Both first diagnostics remain in the retained owner.
        output = self.original.bounded_regular(Path(row["stdout"]), 256 * 1024 * 1024, empty=True).decode()
        if self.provider is not None and self.provider.started:
            self.provider.check_alive()
        if self.uid is not None:
            self.census("after-owner-" + str(self.sequence))
        return output

    def source_files(self, graph):
        self.current()
        self.original.require(graph.resolve(strict=True) == graph and graph.is_relative_to(self.work), "owner_build_source_root")
        rows = {}
        visited = 0
        for path in graph.rglob("*"):
            visited += 1
            self.original.require(visited <= 100000 and not path.is_symlink(), "owner_build_source_alias_or_bound")
            relative = path.relative_to(graph)
            if relative.parts[0] == ".git":
                continue  # Only actual Git-created metadata, never product source.
            if path.is_file():
                self.original.require(len(rows) < 50000, "owner_build_source_bound")
                first = path.lstat()
                content = self.original.file_fact(path, 512 * 1024 * 1024, empty=True)
                last = path.lstat()
                def identity(value):
                    return (value.st_dev, value.st_ino, value.st_uid, value.st_gid, value.st_mode,
                            value.st_nlink, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
                self.original.require(identity(first) == identity(last), "owner_build_source_identity_changed")
                rows[str(relative)] = {**content, "identity": identity(first)}
        return rows

    def capture_build_source(self, graph):
        self.current()
        self.original.require(not self.build_capture_attempted and not (graph / ".git").exists(), "owner_build_capture_reused")
        self.build_capture_attempted = True
        self.build_graph = graph
        self.build_source = self.source_files(graph)
        self.original.save(self.public / "actual-materialized-build-source.json", {
            "sourceGraphSealSHA256": self.source_pin, "files": self.build_source,
            "meaning": "actual exact dependency/work-root rewrites and owner lock before real Git/build; DATA only"})

    def verify_build_source(self, graph, boundary):
        self.current()
        self.original.require(self.build_source is not None and graph == self.build_graph
            and boundary not in self.build_checks, "owner_build_source_first_binding")
        self.original.require(self.source_files(graph) == self.build_source, "owner_build_source_changed")
        self.build_checks.append(boundary)
        self.original.save(self.public / (boundary + "-source.json"), {
            "sourceGraphSealSHA256": self.source_pin, "sameActualMaterializedSource": True,
            "sourceCount": len(self.build_source), "boundary": boundary, "runtimeAuthority": False})

    def invoke(self, assembly_path):
        self.current()
        self.original.require(self.product_begun and self.product is not None, "owner_product_not_entered")
        assembly = self.original.load(assembly_path)
        self.original.require(assembly["uid"] == self.uid and assembly["account"] == self.account["name"]
            and assembly["actualWorkRoot"] == str(self.work) and assembly["sourceGraphSealSHA256"] == self.source_pin
            and assembly["qualificationCaseData"] == self.args.case and re.fullmatch(r"[0-9a-f]{40}", assembly["sourceRevision"]),
            "actual_owner_assembly_binding")
        self.results["actualBuildReceipt"] = assembly
        self.verify_build_source(self.work / "graph", "before-normal-owner-invocation")
        helper = self.work / "graph/FixtureScripts/launch_installed_owner.py"
        try:
            self.run([sys.executable, "-I", "-S", "-B", helper, "--ephemeral-ci-vm", "--work-root", self.work,
                "--expect", self.args.case, "--hosted-runner-uid", self.runner_uid], user=self.account["name"],
                cwd=self.work, timeout=60, scratch=self.work / "scratch")
            self.verify_build_source(self.work / "graph", "after-normal-owner-invocation")
        finally:
            # Actual first outcome is diagnostic even when this owned command
            # or subsequent OS census failed. It never repairs that failure.
            try:
                outcome = self.original.load(self.work / "owner-qualification/FIRST-OUTCOME.json")
                self.results["controllerFirstOutcome"] = outcome
                self.results["selectedNativeObservationPassed"] = outcome.get("ownerObservation", {}).get("passed") is True
                self.results["controllerExitedZero"] = type(outcome.get("controllerExit")) is int and outcome["controllerExit"] == 0
            except BaseException:
                self.results["cleanupFailures"].append("controller_first_outcome_unobserved")
        self.results["stage"] = "owner-invocation-returned"

    def remember(self, failure):
        if self.results["primaryFailure"] is None:
            self.results["primaryFailure"] = str(failure) if isinstance(failure, self.original.CustodyFailure) else "owner_stage_exception"
            self.results["primaryException"] = self.original.exception_fact(failure)

    def finish(self):
        self.current()
        if not self.reserved or self.public is None:
            return False
        if self.provider is not None:
            try:
                self.results["providerDirectCustodyQualified"] = self.provider.finish(
                    owner_invocation_returned=self.results["stage"] == "owner-invocation-returned")
                self.results["providerDirectCustody"] = self.provider.results
                self.original.require(self.results["providerDirectCustodyQualified"], "provider_direct_ending_unproved")
            except BaseException as failure:
                self.results["cleanupFailures"].append("provider_direct_ending_unproved")
                self.remember(failure)
        if self.uid is not None and self.admin is not None and not self.admin.failed and not self.stop.requested:
            self.results["finalAccountCensusAttempted"] = True
            try:
                self.census("final-owner")
            except BaseException as failure:
                self.remember(failure)
        roots = [] if self.admin is None else [x.row for x in self.admin.owners]
        products = [] if self.product is None else [x.row for x in self.product.owners]
        self.results.update(rootCommands=roots, productCommands=products, negativeOwnerPreflights=self.negative_observations,
            account=self.account, osServiceCustody=None if self.os_custody is None else self.os_custody.receipt(),
            originalDeadlineSeconds=self.original.BUDGET_SECONDS,
            actualSourceGraph=str(self.graph), buildSourceChecks=self.build_checks, accountDeletionUsedAsClosureProof=False,
            unknownSessionEscapesProvenAbsent=False)
        negatives = set(self.negative_observations) == {"root", "dedicated"} and all(
            row["qualified"] and row["ownerFailed"] for row in self.negative_observations.values())
        custody = bool(roots and products) and all(row["leaderReaped"] and row["groupGone"] and row["success"] for row in roots + products)
        self.results["directInvocationCustodyQualified"] = custody and negatives
        try:
            self.stop.restore()
        except BaseException as failure:
            self.results["cleanupFailures"].append("signal_handler_restoration_unproved")
            self.remember(failure)
        self.results["qualified"] = (self.results["primaryFailure"] is None and not self.stop.requested
            and time.monotonic() < self.end and not self.results["cleanupFailures"] and custody and negatives
            and self.results["finalAccountCensusAttempted"] and self.os_custody is not None
            and self.os_custody.first_failure is None and not self.os_custody.cleanup_failures
            and self.results["selectedNativeObservationPassed"] and self.results.get("controllerExitedZero") is True)
        try:
            self.original.save(self.public / "owner-invocation-result.json", self.results)
        except BaseException as failure:
            self.results["cleanupFailures"].append("first_result_receipt_unproved")
            self.remember(failure)
            self.results["qualified"] = False
            # No second write, fresh owner, invented return or custody clear.
        return self.results["qualified"]


def main():
    global _HELD
    raw = sys.argv[1:]
    demand(raw.count("--") == 1, "separate_harness_and_assembly_arguments_required")
    split = raw.index("--")
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--harness-commit", required=True)
    parser.add_argument("--control-root", type=Path, required=True)
    parser.add_argument("--owned-provider-root", type=Path)
    parser.add_argument("--owned-provider-executable", type=Path)
    parser.add_argument("--owned-provider-sha256")
    parser.add_argument("--owned-provider-certificate", type=Path)
    parser.add_argument("--owned-provider-private-key", type=Path)
    parser.add_argument("--owned-provider-port", type=int, default=8443)
    own = parser.parse_args(raw[:split]); assembly_args = raw[split + 1:]
    demand(sys.platform == "darwin" and sys.flags.isolated and sys.flags.no_site and sys.flags.dont_write_bytecode and os.geteuid() == 0
        and os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted"
        and os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch" and os.environ.get("GITHUB_RUN_ATTEMPT") == "1"
        and os.environ.get("ENGRAM_MANAGED_HOSTED_GATE") == "1"
        and os.environ.get("GITHUB_REPOSITORY", "").lower() == "jsflax/engram"
        and os.environ.get("ENGRAM_WORKFLOW_SHA") == own.harness_commit
        and os.environ.get("GITHUB_SHA") == own.harness_commit and re.fullmatch(r"[0-9a-f]{40}", own.harness_commit),
        "exact_first_hosted_dispatch_required")
    demand(signal.getsignal(signal.SIGCHLD) == signal.SIG_DFL and _HELD is None, "fresh_default_reaper_owner_required")
    demand(int(platform.mac_ver()[0].split(".")[0]) >= 26, "actual_os_custody_helper_requires_macos26")
    runner_uid = int(os.environ["SUDO_UID"])
    demand(runner_uid > 0, "actual_runner_account_required")
    runner_home = Path(pwd.getpwuid(runner_uid).pw_dir)
    work_base = runner_home / "localdev"
    control = own.control_root.absolute()
    demand(work_base.is_dir() and work_base.resolve(strict=True) == work_base
        and control.parent == work_base and control.parent.resolve(strict=True) == control.parent
        and re.fullmatch(r"lqh[0-9a-f]{8}", control.name) and not os.path.lexists(control), "fresh_actual_runner_localdev_control_required")
    graph, pin = frozen_graph(assembly_args)
    original, custody_source = import_custody(graph)
    module_path = graph / "Core/Tools/LatticeManagedTargetReclamationFixture/assemble_owner_macos_account.py"
    spec = importlib.util.spec_from_file_location("actual_current_owner_assembler", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    held = OwnerImageGuardian(original, custody_source, graph, pin, own.harness_commit, control, runner_uid)
    _HELD = held  # Strong first owner precedes every signal/directory/child effect.
    try:
        held.reserve_control()  # First input errors still retain a root-owned first receipt.
        provider_inputs = [own.owned_provider_root, own.owned_provider_executable, own.owned_provider_sha256,
                           own.owned_provider_certificate, own.owned_provider_private_key]
        demand(all(value is None for value in provider_inputs) or all(value is not None for value in provider_inputs),
               "complete_owned_provider_inputs_required")
        if own.owned_provider_root is not None:
            demand(option(assembly_args, "--provider") == "https://127.0.0.1:" + str(own.owned_provider_port)
                   and Path(option(assembly_args, "--provider-private-credentials")).absolute()
                       == own.owned_provider_root.absolute() / "provider-credentials.json",
                   "same_actual_owned_provider_arguments_required")
            provider_path = graph / "FixtureScripts/hosted_provider_owner.py"
            provider_spec = importlib.util.spec_from_file_location("actual_current_guarded_provider_owner", provider_path)
            provider_module = importlib.util.module_from_spec(provider_spec)
            provider_spec.loader.exec_module(provider_module)
            held.provider = provider_module.HostedProviderOwner(original, root=own.owned_provider_root,
                executable=own.owned_provider_executable, executable_sha256=own.owned_provider_sha256,
                certificate=own.owned_provider_certificate, private_key=own.owned_provider_private_key,
                runner_uid=runner_uid, port=own.owned_provider_port, outer_deadline=held.end, stop=held.stop)
            held.provider.prepare()  # Source/private-input namespace only; no commands/account/HTTP.
        assembly_path = module.main(argv=assembly_args, hosted_commands=held)
        held.invoke(assembly_path)
    except BaseException as failure:
        held.remember(failure)
    finally:
        accepted = held.finish()
    return 0 if accepted else 1


if __name__ == "__main__":
    sys.exit(main())
