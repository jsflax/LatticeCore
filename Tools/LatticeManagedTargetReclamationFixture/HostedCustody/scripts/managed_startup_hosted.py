#!/usr/bin/env python3
"""Dedicated-account actual Engram managed startup qualification guardian.

Never run locally. This file does not adopt a populated store, modify an existing
account, or grant native authority from metadata. Only the existing fixed-root
installer publisher may publish installation state after actual final signing.
All13 existing product methods are unchanged. Missing custody stays unproved.
"""
from __future__ import annotations
import argparse
import hashlib
import http.server
import json
import os
from pathlib import Path
import platform
import pwd
import grp
import re
import secrets
import shutil
import signal
import stat
import sys
import threading
import time
import uuid

# The guardian itself is invoked with -I -S. Only its frozen sibling helpers are
# added after isolated startup; no site/user hook is evaluated by that startup.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from managed_startup_command_owner import (CommandOwner, StopFlag, CustodyFailure,
                                          bounded_regular, exception_fact, file_fact,
                                          qualify_nonzero_owner, qualify_owner, require)
from managed_startup_case_observer import CASES
from managed_startup_os_custody import HostedOSCustody, PREPRODUCT_STAGES

CORE = '9ab6a910b3d471edfc8a4154693bcaa7071d892b'
CORE_TREE = 'd74b41942e136ff37f62132785e207131b614830'
SDK = '08a763f92d649b2191df1c34d73e2ebd86c6a47f'
SDK_TREE = '18472412d719191ee47524b05d9a766e25ed9959'
ZIP = 'Sparkle-for-Swift-Package-Manager.zip'
SPARKLE = 'https://github.com/sparkle-project/Sparkle/releases/download/2.8.1/' + ZIP
BUDGET_SECONDS = 12600


def save(path, value):
    raw = (json.dumps(value, indent=2, sort_keys=True) + '\n').encode()
    require(len(raw) <= 16 * 1024 * 1024, 'guardian_receipt_overflow')
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        at = 0
        while at < len(raw):
            count = os.write(fd, raw[at:])
            require(count > 0, 'guardian_receipt_short_write')
            at += count
        os.fsync(fd)
    finally:
        os.close(fd)
    if path.parent.name == 'public':
        # Make each fully written first receipt readable immediately. A later
        # guardian failure must not hide earlier observations behind root-only
        # permissions. The containing namespace remains root-controlled.
        runner_uid = int(os.environ['SUDO_UID'])
        require(runner_uid > 0, 'public_receipt_runner_missing')
        os.chown(path, runner_uid, -1)


def load(path, maximum=16 * 1024 * 1024):
    def unique(pairs):
        out = {}
        for key, value in pairs:
            require(key not in out, 'duplicate_json_key')
            out[key] = value
        return out
    return json.loads(bounded_regular(path, maximum), object_pairs_hook=unique)


def directory(path, *, uid=0, gid=0, mode=0o700):
    path.mkdir(mode=mode, exist_ok=False)
    os.chown(path, uid, gid)
    os.chmod(path, mode)
    observed = path.lstat()
    require(stat.S_ISDIR(observed.st_mode) and observed.st_uid == uid and stat.S_IMODE(observed.st_mode) == mode,
            'directory_creation_unproved')
    return path


def identity(path):
    value = path.lstat()
    return {'device': value.st_dev, 'inode': value.st_ino, 'uid': value.st_uid,
            'mode': stat.S_IMODE(value.st_mode)}


class Mirror:
    """One retained thread, fixed loopback bytes only; no child or broad server."""
    def __init__(self, data):
        require(0 < len(data) <= 64 * 1024 * 1024, 'sparkle_size')
        self.stop = threading.Event()
        self.error = False
        outer = self
        class Handler(http.server.BaseHTTPRequestHandler):
            def setup(self):
                self.request.settimeout(2)
                super().setup()
            def log_message(self, fmt, *args):
                pass  # No arbitrary request/log export.
            def do_GET(self):
                self.close_connection = True
                if self.path != '/' + ZIP:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header('Content-Type', 'application/zip')
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                for at in range(0, len(data), 65536):
                    if outer.stop.is_set():
                        return
                    self.wfile.write(data[at:at + 65536])
        self.server = http.server.HTTPServer(('127.0.0.1', 0), Handler)
        self.server.timeout = .1
        self.port = self.server.server_address[1]
        def loop():
            try:
                while not self.stop.is_set():
                    self.server.handle_request()
            except BaseException:
                if not self.stop.is_set():
                    self.error = True
        self.thread = threading.Thread(target=loop, name='fixed-sparkle-mirror', daemon=False)
        self.thread.start()
    def close(self):
        self.stop.set()
        self.server.server_close()
        self.thread.join(5)
        require(not self.thread.is_alive() and not self.error, 'mirror_thread_closure_unproved')


class Guardian:
    def __init__(self, source, control, expected, runner_uid):
        self.source, self.control, self.expected, self.runner_uid = source, control, expected, runner_uid
        self.stop = StopFlag()
        self.stop.install()
        self.end = time.monotonic() + BUDGET_SECONDS
        os.chmod(control, 0o711)
        self.public = directory(control / 'public', mode=0o755)
        self.private = directory(control / 'private')
        self.admin_receipts = directory(control / 'admin-receipts')
        self.admin = CommandOwner(self.stop, self.admin_receipts, self.private)
        self.negative_owners = {}
        self.negative_observations = {}
        self.product = None
        self.os_custody = None
        self.account = None
        self.mirror = None
        self.results = {'schemaVersion': 1, 'scope': 'new-origin-primary-engram-mcp-only',
                        'engramCommit': expected, 'coreCommit': CORE, 'coreTree': CORE_TREE,
                        'sdkCommit': SDK, 'sdkTree': SDK_TREE, 'stage': 'preflight',
                        'primaryFailure': None, 'cleanupFailures': [], 'caseObservations': [],
                        'all13OriginalMethodsPassed': False, 'qualified': False,
                        'historicalStoreAdoptionAccepted': False, 'performanceAccepted': False,
                        'releaseAccepted': False, 'runtimeAuthorityFromReceipts': False}
        self.root_env = {'PATH': os.environ['PATH'], 'HOME': str(pwd.getpwuid(0).pw_dir),
            'USER': 'root', 'LOGNAME': 'root', 'LANG': 'en_US.UTF-8', 'LC_ALL': 'en_US.UTF-8',
            'TMPDIR': str(directory(control / 'tmp')), 'PYTHONDONTWRITEBYTECODE': '1',
            'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': '/dev/null', 'GIT_TERMINAL_PROMPT': '0'}
        for key in ('DEVELOPER_DIR', 'SDKROOT'):
            if key in os.environ:
                self.root_env[key] = os.environ[key]

    def budget(self, seconds):
        require(not self.stop.requested and time.monotonic() + seconds + 30 <= self.end,
                'guardian_budget_or_cancel_refusal')

    def admin_run(self, label, argv, *, cwd=None, seconds=30, stdout=None, separate=False):
        self.budget(seconds)
        return self.admin.run(label, argv, cwd=cwd or self.control, env=self.root_env, seconds=seconds,
                              stdout_path=stdout, stderr_separate=separate)

    def product_run(self, label, argv, *, seconds=120, stdout=None, separate=False, credentials=False):
        self.budget(seconds)
        env = dict(self.env)
        if credentials:
            token = os.environ.get('SWIFTLM_CLONE_TOKEN', '')
            require(bool(re.fullmatch(r'[A-Za-z0-9_]+', token)), 'scoped_dependency_credential_missing')
            env.update(GIT_CONFIG_COUNT='1', GIT_CONFIG_KEY_0='url.https://x-access-token:' + token
                       + '@github.com/jsflax/SwiftLM.insteadOf',
                       GIT_CONFIG_VALUE_0='https://github.com/jsflax/SwiftLM')
        row = self.product.run(label, argv, cwd=self.engram, env=env, seconds=seconds,
            uid=self.uid, gid=self.gid, stdout_path=stdout, stderr_separate=separate,
            context={'engramCommit': self.expected, 'sdkCommit': SDK, 'coreCommit': CORE})
        self.census('after-' + label)
        return row

    def census(self, label):
        # Preserve the full original supplemental census first. Names/PPID never
        # grant an exemption. Separate actual native/job producers below may
        # qualify only the explicitly scoped test-environment OS services.
        row = self.admin_run('census-' + label,
            ['/bin/ps', '-axo', 'pid=,ppid=,pgid=,uid=,ruid=,svuid=,ucomm='], seconds=30)
        raw = bounded_regular(Path(row['stdout']), 4 * 1024 * 1024, empty=True)
        require(len(raw.splitlines()) <= 32768, 'census_size')
        matching = []
        for line in raw.splitlines():
            cells = line.split(maxsplit=6)
            require(len(cells) >= 6 and all(re.fullmatch(rb'[0-9]+', part) for part in cells[:6]), 'census_unproved')
            values = [int(part) for part in cells[:6]]
            if self.uid in values[3:]:
                observed = dict(zip(('pid', 'ppid', 'pgid', 'uid', 'ruid', 'svuid'), values))
                # ucomm reports a short kernel command name, not argv or an
                # executable identity. It may be truncated or misleading and
                # can never qualify a process for the custody producer below.
                name = cells[6].strip() if len(cells) == 7 else b''
                require(len(name) <= 4096, 'census_command_name_bound')
                observed.update(kernelCommandName=name.decode('ascii')
                    if re.fullmatch(rb'[A-Za-z0-9_.+-]{1,64}', name) else None,
                    kernelCommandNameBytes=len(name), kernelCommandNameSHA256=hashlib.sha256(name).hexdigest())
                matching.append(observed)
        save(self.public / (label + '-census.json'), {'records': matching, 'supplementalOnly': True,
             'kernelCommandNamesAreDiagnosticOnly': True,
             'doesNotProveDescendantsRetired': True, 'logFact': file_fact(Path(row['stdout']), 4 * 1024 * 1024, empty=True)})
        require(self.os_custody is not None, 'unexpected_account_process_unproved')
        # These phases precede actual installer/product execution in this real
        # guardian. No caller-supplied boolean enables a product-time ending.
        preproduct = (self.results['stage'] in PREPRODUCT_STAGES
                      and not self.results['caseObservations'])
        dispositions = self.os_custody.classify(matching, preproduct)
        require(len(dispositions) == len(matching)
                and {item['pid'] for item in dispositions} == {item['pid'] for item in matching},
                'unexpected_account_process_unproved')
        save(self.public / (label + '-os-custody.json'), {
            'records': matching, 'dispositions': dispositions,
            'scope': 'hosted-test-environment-os-services-only',
            'productDescendantJoin': False, 'accountEmptyClaimed': False})

    def create_account(self):
        self.results['stage'] = 'account-setup'
        require(not self.account, 'account_already_created')
        name = 'latq' + secrets.token_hex(8)
        try:
            pwd.getpwnam(name)
            raise CustodyFailure('account_name_collision')
        except KeyError:
            pass
        for _ in range(256):
            uid = 60000 + secrets.randbelow(20000)
            try:
                pwd.getpwuid(uid)
            except KeyError:
                break
        else:
            raise CustodyFailure('no_unused_account_uid')
        gid = grp.getgrnam('staff').gr_gid
        require(gid > 0, 'no_nonadmin_primary_group')
        home = Path('/Users') / name
        require(not os.path.lexists(home) and identity(Path('/Users'))['uid'] == 0
                and not identity(Path('/Users'))['mode'] & 0o022, 'account_home_namespace_unavailable')
        self.account = {'name': name, 'uid': uid, 'gid': gid, 'home': str(home),
                        'generatedUID': None, 'created': False, 'retained': True}
        save(self.public / 'account-reservation.json', self.account)
        fields = [('UniqueID', str(uid)), ('PrimaryGroupID', str(gid)), ('NFSHomeDirectory', str(home)),
                  ('UserShell', '/usr/bin/false'), ('RealName', 'Lattice hosted qualification'),
                  ('IsHidden', '1'), ('AuthenticationAuthority', ';DisabledUser;')]
        self.admin_run('account-create', ['/usr/bin/dscl', '.', '-create', '/Users/' + name])
        self.account['created'] = True
        for number, (key, value) in enumerate(fields):
            self.admin_run('account-field-' + str(number), ['/usr/bin/dscl', '.', '-create', '/Users/' + name, key, value])
        actual = pwd.getpwnam(name)
        require(actual.pw_uid == uid and actual.pw_gid == gid and actual.pw_dir == str(home)
                and actual.pw_shell == '/usr/bin/false' and name not in grp.getgrnam('admin').gr_mem,
                'dedicated_account_binding_unproved')
        row = self.admin_run('account-identity', ['/usr/bin/dscl', '.', '-read', '/Users/' + name, 'GeneratedUID'])
        # Directory Services assigns this identity. Never replace it with a
        # caller-generated value, and never infer it from record creation alone.
        actual_guid = bounded_regular(Path(row['stdout']), 4096).decode('ascii')
        matched = re.fullmatch(r'GeneratedUID: ([0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12})\n?', actual_guid)
        require(matched is not None, 'directory_service_identity_unproved')
        generated_uid = matched.group(1)
        require(uuid.UUID(generated_uid).int != 0 and str(uuid.UUID(generated_uid)).upper() == generated_uid,
                'directory_service_identity_unproved')
        self.account['generatedUID'] = generated_uid
        directory(home, uid=uid, gid=gid)
        directory(home / 'localdev', uid=uid, gid=gid)
        self.workspace = directory(home / 'localdev/managed-startup', uid=uid, gid=gid)
        self.uid, self.gid, self.home = uid, gid, home
        self.receipts = directory(self.workspace / 'managed-startup-receipts', uid=uid, gid=gid)
        self.raw = directory(self.workspace / 'private', uid=uid, gid=gid)
        self.tmp = directory(self.workspace / 'tmp', uid=uid, gid=gid)
        cache = directory(self.workspace / 'cache', uid=uid, gid=gid)
        # Redirect actual default caches in this new account into localdev.
        library = directory(home / 'Library', uid=uid, gid=gid)
        caches = directory(library / 'Caches', uid=uid, gid=gid)
        (caches / 'org.swift.swiftpm').symlink_to(cache / 'swiftpm')
        os.lchown(caches / 'org.swift.swiftpm', uid, gid)
        (home / '.cache').symlink_to(cache)
        os.lchown(home / '.cache', uid, gid)
        self.env = {**self.root_env, 'HOME': str(home), 'USER': name, 'LOGNAME': name,
            'TMPDIR': str(self.tmp), 'XDG_CACHE_HOME': str(cache),
            'CLANG_MODULE_CACHE_PATH': str(cache / 'clang'), 'SWIFTPM_MODULECACHE_OVERRIDE': str(cache / 'swiftpm-modules'),
            'GITHUB_ACTIONS': 'true', 'RUNNER_ENVIRONMENT': 'github-hosted', 'ENGRAM_MANAGED_HOSTED_GATE': '1',
            'ENGRAM_MANAGED_UID': str(uid), 'ENGRAM_MANAGED_SOURCE': self.expected}
        self.product = CommandOwner(self.stop, self.receipts, self.raw)
        self.account.update(homeIdentity=identity(home), workspaceIdentity=identity(self.workspace))
        save(self.public / 'actual-account.json', self.account)
        self.census('initial')

    def negative_preflight(self, role, *, cwd, env, uid=None, gid=None):
        require(role in ('root', 'dedicated') and role not in self.negative_owners,
                'negative_preflight_role_reused')
        self.budget(10)
        receipts = directory(self.control / ('negative-' + role + '-receipts'))
        private = directory(self.control / ('negative-' + role + '-private'))
        owner = CommandOwner(self.stop, receipts, private)
        self.negative_owners[role] = owner  # Retain before any actual spawn.
        observation = {'schemaVersion': 1, 'kind': 'actual-nonzero-owner-preflight', 'role': role,
                       'qualified': False, 'primaryFailure': None, 'exception': None,
                       'descendantOrInstallationAuthority': False}
        self.negative_observations[role] = observation
        failure = None
        try:
            qualify_nonzero_owner(owner, cwd=cwd, env=env, uid=uid, gid=gid)
            observation['qualified'] = True
        except BaseException as caught:
            failure = caught
            observation['primaryFailure'] = str(caught) if isinstance(caught, CustodyFailure) else 'negative_preflight_exception'
            observation['exception'] = exception_fact(caught)
        observation.update(ownerFailed=owner.failed, actualCommands=[value.row for value in owner.owners])
        try:
            save(self.public / (role + '-negative-owner-preflight.json'), observation)
        except BaseException:
            self.results['cleanupFailures'].append('negative_preflight_receipt_unproved')
            if failure is None:
                failure = CustodyFailure('negative_preflight_receipt_unproved')
        if failure is not None:
            raise failure

    def checkout(self):
        self.results['stage'] = 'source-checkout'
        sources = [('engram', str(self.source), self.expected, None),
                   ('lattice', 'https://github.com/jsflax/Lattice.git', SDK, SDK_TREE),
                   ('LatticeCore', 'https://github.com/jsflax/LatticeCore.git', CORE, CORE_TREE)]
        for leaf, url, commit, tree in sources:
            target = self.workspace / leaf
            # Root owns this fresh checkout while root Git populates it. Only
            # the completed exact checkout transfers to the dedicated account.
            directory(target)
            self.admin_run('init-' + leaf, ['git', '-c', 'core.hooksPath=/dev/null', 'init', str(target)])
            self.admin_run('fetch-' + leaf, ['git', '-c', 'core.hooksPath=/dev/null', '-C', str(target),
                'fetch', '--no-tags', '--depth=1', url, commit], seconds=600)
            self.admin_run('checkout-' + leaf, ['git', '-c', 'core.hooksPath=/dev/null', '-C', str(target),
                'checkout', '--detach', commit])
            row = self.admin_run('identity-' + leaf, ['git', '-c', 'core.fsmonitor=false', '-C', str(target),
                                                      'rev-parse', 'HEAD', 'HEAD^{tree}'])
            actual = bounded_regular(Path(row['stdout']), 1024).decode().splitlines()
            require(actual[0] == commit and (tree is None or actual[1] == tree), 'source_checkout_identity')
            count = 0
            for parent, directories, files in os.walk(target, followlinks=False):
                for name in [*directories, *files]:
                    count += 1
                    require(count <= 200000, 'checkout_file_count')
                    os.chown(Path(parent) / name, self.uid, self.gid, follow_symlinks=False)
            os.chown(target, self.uid, self.gid)
            save(self.public / ('source-' + leaf + '.json'), {'commit': commit, 'tree': actual[1], 'trackedCheckout': str(target)})
        self.engram, self.sdk, self.core = (self.workspace / n for n in ('engram', 'lattice', 'LatticeCore'))
        # These two actual isolated children run under the dedicated credentials.
        self.budget(30)
        save(self.public / 'dedicated-owner-preflight.json', qualify_owner(self.product, cwd=self.engram,
             env=self.env, uid=self.uid, gid=self.gid))
        self.negative_preflight('dedicated', cwd=self.engram, env=self.env, uid=self.uid, gid=self.gid)
        self.census('preflight')

    def artifact(self, operation, seconds=120):
        return self.product_run('artifact-' + operation, [sys.executable, '-S', '-B',
            str(self.engram / 'scripts/managed_startup_artifact_ops.py'), operation], seconds=seconds)

    def build_and_install(self):
        self.results['stage'] = 'resolve'
        self.artifact('prepare-lock')
        self.product_run('swift-version', ['swift', '--version'])
        self.product_run('sparkle-download', ['/usr/bin/curl', '--fail', '--silent', '--show-error', '--location',
            '--output', str(self.workspace / ZIP), SPARKLE], seconds=300)
        data = bounded_regular(self.workspace / ZIP, 64 * 1024 * 1024)
        self.mirror = Mirror(data)
        self.env['ENGRAM_MANAGED_MIRROR_PORT'] = str(self.mirror.port)
        save(self.public / 'sparkle-transport.json', {'artifactSHA256': hashlib.sha256(data).hexdigest(),
            'bytes': len(data), 'loopbackPort': self.mirror.port, 'trustConfigurationChanged': False})
        self.product_run('sparkle-mirror', ['swift', 'package', 'config', 'set-mirror', '--original', SPARKLE,
            '--mirror', 'http://127.0.0.1:' + str(self.mirror.port) + '/' + ZIP])
        self.product_run('resolve-versioned', ['swift', 'package', 'resolve', '--force-resolved-versions'],
                         seconds=900, credentials=True)
        self.product_run('edit-sdk', ['swift', 'package', 'edit', 'lattice', '--path', str(self.sdk)])
        self.product_run('edit-core', ['swift', 'package', 'edit', 'LatticeCore', '--path', str(self.core)])
        self.product_run('effective-graph-before', ['swift', 'package', 'show-dependencies', '--format', 'json'],
            stdout=self.receipts / 'effective-graph-before.json', separate=True, credentials=True)
        self.artifact('verify-graph', seconds=300)
        self.results['stage'] = 'build-memory'
        self.product_run('build-memory', ['swift', 'build', '--force-resolved-versions', '-c', 'release',
            '--product', 'Engram', '-j', '2', '-v'], seconds=5400, stdout=self.raw / 'managed-startup-build.log')
        self.product_run('effective-graph-after', ['swift', 'package', 'show-dependencies', '--format', 'json'],
            stdout=self.receipts / 'effective-graph-after.json', separate=True)
        self.artifact('restore-public')
        self.mirror.close()
        self.mirror = None
        self.results['stage'] = 'sign-and-package'
        self.artifact('package-memory')
        cli = self.workspace / 'package/cli'
        self.product_run('sign-memory', ['/usr/bin/codesign', '--force', '--sign', '-', str(cli / 'memory')])
        self.product_run('verify-memory-signature', ['/usr/bin/codesign', '--verify', '--strict', str(cli / 'memory')])
        self.artifact('prove-private-build')
        self.product_run('build-launcher', [sys.executable, '-S', '-B',
            str(self.engram / 'scripts/build_installation_launcher.py'), '--private-managed-qualification',
            '--final-cli-directory', str(cli), '--build-directory', str(self.workspace / 'launcher-build')], seconds=900)
        self.product_run('sign-launcher', ['/usr/bin/codesign', '--force', '--sign', '-', str(cli / 'memory-installation-launcher')])
        self.product_run('verify-launcher-signature', ['/usr/bin/codesign', '--verify', '--strict', str(cli / 'memory-installation-launcher')])
        self.product_run('final-provenance', [sys.executable, '-S', '-B',
            str(self.engram / 'scripts/finalize_installation_provenance.py'), '--final-cli-directory', str(cli),
            '--build-inputs', str(self.workspace / 'launcher-build/build-inputs.json')])
        names = ('main.cpp', 'owned_launch.cpp', 'installation_manifest.cpp', 'installation_cohort.cpp',
                 'installation_files.cpp', 'installation_seed.cpp', 'store_admission.cpp',
                 'engram_product_installer.cpp', 'normal_startup.cpp')
        self.unstamped = self.workspace / 'unstamped-launcher'
        self.product_run('build-unstamped', ['xcrun', '--sdk', 'macosx', 'clang++', '-std=c++20', '-O2', '-pthread',
            *[str(self.core / 'Tools/LatticeInstallationLauncher' / name) for name in names], '-o', str(self.unstamped), '-v'],
            seconds=600)
        self.artifact('prove-unstamped')
        self.results['stage'] = 'actual-installer'
        self.artifact('install-package')
        self.product_run('publish-installed-root', [sys.executable, '-S', '-B',
            str(self.engram / 'scripts/publish_managed_installation_root.py')], seconds=60)
        self.artifact('prove-final')

    def cases(self):
        self.results['stage'] = 'actual-product-cases'
        evidence = directory(self.workspace / 'actual-cases', uid=self.uid, gid=self.gid)
        self.env.update(ENGRAM_INSTALLATION_CLI=str(self.home / '.claude/bin'),
            ENGRAM_INSTALLATION_EVIDENCE=str(evidence), ENGRAM_INSTALLATION_SOURCE=self.expected,
            ENGRAM_INSTALLATION_CORE=CORE, ENGRAM_UNSTAMPED_INSTALLER=str(self.unstamped))
        for number, name in enumerate(CASES):
            path = evidence / ('case-' + str(number) + '.json')
            label = 'case-' + str(number)
            case = None
            # Preserve every first observation. A failed case prevents more work;
            # later missing cases remain unobserved, never inferred as passing.
            try:
                command = self.product_run(label, [sys.executable, '-S', '-B',
                    str(self.engram / 'scripts/managed_startup_case_observer.py'), name, str(path)], seconds=120)
            finally:
                if path.exists():
                    case = load(path, 256 * 1024)
                    self.results['caseObservations'].append(case)
                    save(self.public / ('case-' + str(number) + '.json'), case)
            require(case is not None and case['case'] == name and case['qualified'] and case['custodyQualified']
                    and case['testOutcomes'] == ['passed'] and not case['observationError'], 'actual_case_unqualified')
            require(all(row['parentPID'] == command['pid'] and row['actualWaitObserved']
                and row['groupAbsentAfterReap'] for row in case['fixtureProcesses']), 'case_parent_custody_mismatch')
            require(len(case['fixtureProcesses']) == len(CASES[name][0])
                    and [row['role'] for row in case['nativeGates']] == list(CASES[name][1]), 'case_role_inventory_mismatch')
        require(len(self.results['caseObservations']) == 13, 'incomplete_original_cases')
        self.results['all13OriginalMethodsPassed'] = True

    def export_receipts(self):
        # Export a fixed list of bounded reconstructed metadata only. Every
        # missing receipt stays missing and is named in the final observation.
        sources = []
        if self.product is not None:
            for phase in ('prepare-lock', 'verify-graph', 'restore-public', 'package-memory',
                          'prove-private-build', 'prove-unstamped', 'install-package', 'prove-final'):
                sources.append((phase + '.json', self.receipts / (phase + '.json')))
            sources.extend((('launcher-build-inputs.json', self.workspace / 'launcher-build/build-inputs.json'),
                ('launcher-compiler-inputs.json', self.workspace / 'launcher-build/private-launcher-compiler-inputs.json'),
                ('final-provenance.json', self.workspace / 'package/cli/engram-installation-provenance.json')))
        self.results['metadataReceipts'] = {}
        self.results['missingMetadataReceipts'] = []
        for name, source in sources:
            if not source.exists():
                self.results['missingMetadataReceipts'].append(name)
                continue
            # These schemas are written only by the fixed audited packaging
            # helpers. Raw stdout/logs and dependency credentials are excluded.
            value = load(source)
            require(type(value) is dict, 'artifact_metadata_not_object')
            save(self.public / name, value)
            self.results['metadataReceipts'][name] = file_fact(self.public / name, 16 * 1024 * 1024)

    def finish(self):
        # Root retains every actual owner, and outputs their primary facts even
        # when a per-command receipt or later cleanup could not be saved.
        if self.mirror is not None:
            try:
                self.mirror.close()
            except BaseException:
                self.results['cleanupFailures'].append('mirror_thread_unproved')
        try:
            self.export_receipts()
        except BaseException:
            self.results['cleanupFailures'].append('metadata_export_unproved')
        self.results['rootCommands'] = [owner.row for owner in self.admin.owners]
        self.results['productCommands'] = [] if self.product is None else [owner.row for owner in self.product.owners]
        self.results['negativeOwnerPreflights'] = self.negative_observations
        self.results['osServiceCustody'] = None if self.os_custody is None else self.os_custody.receipt()
        self.results['account'] = self.account
        self.results['trustCleanup'] = {'adHocSigningOnly': True, 'keychainImported': False,
            'trustOverrideInstalled': False, 'certificateSearchListChanged': False}
        # Keep the disabled UID and all private files reserved through artifact
        # collection. VM teardown is containment, not a reported process reap.
        self.results['accountRetainedUntilVMTeardown'] = self.account is not None
        self.results['accountDeletionUsedAsClosureProof'] = False
        self.results['unknownLineageProofClaimed'] = False
        if time.monotonic() >= self.end and self.results['primaryFailure'] is None:
            self.results['primaryFailure'] = 'guardian_deadline'
        self.results['overallDeadlineSeconds'] = BUDGET_SECONDS
        self.results['qualified'] = (self.results['all13OriginalMethodsPassed']
            and set(self.negative_observations) == {'root', 'dedicated'}
            and all(row['qualified'] and row['ownerFailed'] for row in self.negative_observations.values())
            and self.results['primaryFailure'] is None and not self.results['cleanupFailures']
            and not self.results.get('missingMetadataReceipts', ['unobserved'])
            and not self.stop.requested and all(row['leaderReaped'] and row['groupGone'] and row['success']
                for row in self.results['rootCommands'] + self.results['productCommands']))
        save(self.public / 'result.json', self.results)
        # Only bounded JSON observations are exported; raw logs/transcripts,
        # tokens, private dependency configuration and product payloads stay out.
        for path in self.public.iterdir():
            require(path.is_file() and not path.is_symlink() and path.suffix == '.json', 'unexpected_public_artifact')
            os.chown(path, self.runner_uid, -1)
        self.stop.restore()

    def run(self):
        try:
            self.budget(30)
            save(self.public / 'root-owner-preflight.json', qualify_owner(self.admin, cwd=self.control, env=self.root_env))
            self.negative_preflight('root', cwd=self.control, env=self.root_env)
            # Root positive/negative owner preflights must precede every helper
            # compile. Retain the new custody owner before its first effect.
            self.os_custody = HostedOSCustody(self)
            self.os_custody.prepare()
            self.create_account()
            self.checkout()
            self.build_and_install()
            self.cases()
            self.census('final')
        except BaseException as failure:
            self.results['primaryFailure'] = str(failure) if isinstance(failure, CustodyFailure) else 'guardian_stage_exception'
        finally:
            self.finish()
        return 0 if self.results['qualified'] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path, required=True)
    parser.add_argument('--control-root', type=Path, required=True)
    parser.add_argument('--engram-commit', required=True)
    args = parser.parse_args()
    require(sys.platform == 'darwin' and sys.flags.isolated and sys.flags.no_site and os.geteuid() == 0
        and os.environ.get('GITHUB_ACTIONS') == 'true' and os.environ.get('RUNNER_ENVIRONMENT') == 'github-hosted'
        and os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch' and os.environ.get('GITHUB_RUN_ATTEMPT') == '1'
        and os.environ.get('ENGRAM_MANAGED_HOSTED_GATE') == '1'
        and os.environ.get('GITHUB_REPOSITORY', '').lower() == 'jsflax/engram'
        and os.environ.get('ENGRAM_WORKFLOW_SHA') == args.engram_commit
        and os.environ.get('GITHUB_SHA') == args.engram_commit and re.fullmatch(r'[0-9a-f]{40}', args.engram_commit),
        'exact_first_hosted_dispatch_required')
    require(signal.getsignal(signal.SIGCHLD) == signal.SIG_DFL, 'default_sigchld_required')
    runner_uid = int(os.environ['SUDO_UID'])
    require(runner_uid > 0, 'real_runner_account_required')
    runner_home = Path(pwd.getpwuid(runner_uid).pw_dir)
    source = args.source_root
    control = args.control_root
    require(source.is_absolute() and source.resolve(strict=True) == source
        and source.is_relative_to(runner_home / 'localdev') and source.name == 'engram'
        and control.is_absolute() and control.parent.resolve(strict=True) == control.parent
        and control.is_relative_to(runner_home / 'localdev') and not control.exists(), 'fresh_localdev_roots_required')
    directory(control)
    return Guardian(source, control, args.engram_commit, runner_uid).run()


if __name__ == '__main__':
    sys.exit(main())
