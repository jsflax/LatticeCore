#!/usr/bin/env python3
"""Dormant Core maintenance release protocol. Operational CLI is code-disabled.

Pure validators establish consistency of authenticated inputs; they do not
authenticate an arbitrary JSON producer or establish that a native test ran.
Activation requires independent review of workflow execution and source policy.
No function in this module dispatches, tags, pushes or publishes a release.
"""
import argparse
import copy
import datetime
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import selectors
import subprocess
import sys
import time
import zipfile
import xml.etree.ElementTree as ET

import maintenance_profile as m
import release_train as r

OPERATIONS_ENABLED = False
CONTRACT_PATH = 'release-train/maintenance-gates.json'
GATES = ('core-linux', 'core-macos', 'capi-linux', 'capi-macos')
JOB_NAMES = {'core-linux': 'test-linux / build-and-test', 'core-macos': 'test-macos / build-and-test',
             'capi-linux': 'test-capi / capi-linux', 'capi-macos': 'test-capi / capi-macos'}
SHA256 = re.compile(r'[0-9a-f]{64}')
MAX_FILE = 64 * 1024 * 1024
MAX_TOTAL = 512 * 1024 * 1024
MAX_JSON = 4 * 1024 * 1024
SOURCE_REVIEW_STATE = 'maintenance-source-reviewed-not-release-admitted'


def require(condition, message):
    r.require(condition, message)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def sha256(value):
    return hashlib.sha256(value).hexdigest()


def digest(value):
    return sha256(encoded(value))


def parse_json(raw):
    require(type(raw) is bytes and len(raw) <= MAX_JSON, 'JSON exceeds size bound')
    return json.loads(raw, object_pairs_hook=m._unique_object,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite JSON')))


def keys(value, expected, label):
    require(type(value) is dict and set(value) == set(expected), f'{label}: exact keys required')


def text(value, label):
    require(type(value) is str and 0 < len(value) <= 4096
            and not any(ord(c) < 32 for c in value), f'{label}: nonempty text required')
    return value


def integer(value, label, minimum=0):
    require(type(value) is int and value >= minimum, f'{label}: integer required')
    return value


def full_sha(value, label):
    require(type(value) is str and r.SHA.fullmatch(value), f'{label}: full commit/tree SHA required')


def hash_value(value, label):
    require(type(value) is str and SHA256.fullmatch(value), f'{label}: SHA256 required')


def timestamp(value):
    text(value, 'timestamp')
    try:
        parsed = datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        raise ValueError('invalid timestamp') from None
    require(parsed.tzinfo is not None, 'timestamp requires timezone')
    return parsed


def _relative(value):
    text(value, 'artifact path')
    path = PurePosixPath(value)
    require(not path.is_absolute() and '..' not in path.parts and str(path) == value
            and value != '.' and '\\' not in value, 'artifact path must be normalized relative path')
    return path


def read_artifact(root, relative, limit=MAX_FILE):
    root = Path(root).absolute()
    require(root.is_dir() and not root.is_symlink() and root == root.resolve(),
            'artifact root must be a real canonical directory')
    path = root / _relative(relative)
    for parent in (path, *path.parents):
        require(not parent.is_symlink(), 'artifact symlink is forbidden')
        if parent == root:
            break
    require(path.is_file() and path.resolve().is_relative_to(root), 'artifact missing or outside root')
    before = path.stat()
    require(0 < before.st_size <= limit, 'artifact empty or exceeds bound')
    raw = path.read_bytes()
    after = path.stat()
    require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            and len(raw) == before.st_size, 'artifact changed while read')
    return raw


def _test_ids(values):
    require(type(values) is list and 0 < len(values) <= 100000, 'nonempty bounded test inventory required')
    for value in values:
        text(value, 'test ID')
        require('.' in value and not any(c.isspace() for c in value), 'invalid test ID')
        require(not re.search(r'(^|[./])DISABLED_', value), 'disabled test is not reconciled')
    require(len(set(values)) == len(values), 'duplicate expected test ID')
    return sorted(values)


def validate_contract(contract, source):
    keys(contract, {'schemaVersion', 'source', 'gates', 'highFd'}, 'gate contract')
    require(type(contract['schemaVersion']) is int and contract['schemaVersion'] == 1, 'gate schema unsupported')
    require(contract['source'] == {'sha': source['sha'], 'tree': source['tree']}, 'gate inventory source mismatch')
    keys(contract['gates'], GATES, 'native gates')
    for name, gate in contract['gates'].items():
        capi = name.startswith('capi-')
        keys(gate, {'platform', 'suites', 'symbols'} if capi else {'platform', 'suites'}, 'gate')
        require(gate['platform'] == ('Linux' if name.endswith('linux') else 'macOS'), 'gate platform mismatch')
        keys(gate['suites'], ('swiftpm', 'cmake') if capi else ('core',), 'suite inventory')
        for tests in gate['suites'].values():
            _test_ids(tests)
        if capi:
            symbols = gate['symbols']
            require(type(symbols) is list and symbols and len(set(symbols)) == len(symbols), 'symbol inventory required')
            require(all(type(s) is str and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', s) for s in symbols), 'invalid symbol')
            require(set(gate['suites']['swiftpm']) == set(gate['suites']['cmake']), 'C API discovery surfaces differ')
    high = contract['highFd']
    keys(high, {'attestationSHA256', 'owner', 'review', 'runId', 'runAttempt', 'artifactId',
                'artifactName', 'archiveSHA256', 'attestationPath', 'shutdownLimitMilliseconds'}, 'high descriptor approval')
    hash_value(high['attestationSHA256'], 'approved high descriptor attestation')
    require(high['owner'] == 'jsflax', 'independent high descriptor owner required')
    text(high['review'], 'high descriptor review')
    require(type(high['runId']) is str and re.fullmatch(r'[1-9][0-9]*', high['runId']), 'high descriptor run ID required')
    integer(high['runAttempt'], 'high descriptor run attempt', 1)
    integer(high['artifactId'], 'high descriptor artifact ID', 1)
    text(high['artifactName'], 'high descriptor artifact name')
    hash_value(high['archiveSHA256'], 'high descriptor archive')
    _relative(high['attestationPath'])
    integer(high['shutdownLimitMilliseconds'], 'high descriptor shutdown limit', 1)
    require(high['shutdownLimitMilliseconds'] <= 60000, 'shutdown bound exceeds one minute')
    return contract


def github_context(context, control_sha, *, run=None, platform=None):
    for name, expected in [('GITHUB_ACTIONS', 'true'), ('GITHUB_EVENT_NAME', 'workflow_dispatch'),
                           ('GITHUB_REF', 'refs/heads/main'), ('GITHUB_REPOSITORY', m.REPOSITORY),
                           ('GITHUB_SHA', control_sha)]:
        require(context.get(name) == expected, f'wrong GitHub context: {name}')
    run_id, attempt = context.get('GITHUB_RUN_ID'), context.get('GITHUB_RUN_ATTEMPT')
    require(type(run_id) is str and re.fullmatch(r'[1-9][0-9]*', run_id), 'run ID required')
    require(type(attempt) is str and re.fullmatch(r'[1-9][0-9]*', attempt), 'run attempt required')
    actual = {'id': run_id, 'attempt': int(attempt)}
    if run is not None:
        require(actual == run, 'run/attempt mismatch')
    if platform is not None:
        require(context.get('RUNNER_OS') == platform, 'runner platform mismatch')
        text(context.get('GITHUB_JOB'), 'GitHub job')
    return actual


def _admission_identity(review, *, strict=True):
    require(type(review) is dict and type(review.get('schemaVersion')) is int
            and review.get('schemaVersion') == 1, 'source admission schema required')
    if strict:
        expected = {'schemaVersion', 'profile', 'repository', 'version', 'tag', 'channel', 'sourceAdmission',
                    'dispatchAdmitted', 'publicationAdmitted', 'control', 'profileSHA256', 'source', 'base',
                    'reviewedBackport', 'sourceDiff', 'metadataDiff', 'remoteInventory', 'checkedAt'}
        require(set(review) in (expected, expected | {'state'}), 'unexpected source admission fields')
        require('state' not in review or review['state'] == SOURCE_REVIEW_STATE, 'unexpected source admission state')
        inventory = review['remoteInventory']
        keys(inventory, {'tagCount', 'releaseCount', 'lineTags'}, 'source admission inventory')
        integer(inventory['tagCount'], 'remote tag count', 1)
        integer(inventory['releaseCount'], 'remote release count')
        require(type(inventory['lineTags']) is list and inventory['lineTags']
                and all(type(tag) is str and r.Version(tag).core[:2] == (1, 4) for tag in inventory['lineTags'])
                and len(inventory['lineTags']) == len(set(inventory['lineTags'])), 'invalid maintenance line inventory')
    require(review.get('sourceAdmission') is True and review.get('dispatchAdmitted') is False
            and review.get('publicationAdmitted') is False, 'source review is not release admission')
    for key, value in [('repository', m.REPOSITORY), ('profile', m.PROFILE), ('version', m.VERSION),
                       ('tag', m.VERSION), ('channel', 'stable')]:
        require(review.get(key) == value, f'source review {key} mismatch')
    source, control = review.get('source'), review.get('control')
    keys(source, {'sha', 'tree', 'branch'}, 'source')
    keys(control, {'sha', 'tree', 'branch', 'files'}, 'control')
    for item in (source, control):
        full_sha(item['sha'], 'source/control commit')
        full_sha(item['tree'], 'source/control tree')
    require(source['branch'] == m.SOURCE_BRANCH and control['branch'] == 'main', 'source/control branch mismatch')
    require(type(control['files']) is dict and CONTRACT_PATH in control['files']
            and set(m.REQUIRED_CONTROL_FILES).issubset(control['files']), 'registered control files missing')
    for path, value in control['files'].items():
        _relative(path)
        hash_value(value, 'control file hash')
    hash_value(review.get('profileSHA256'), 'profile hash')
    require(review.get('base') == {'tag': '1.4.2', 'sha': m.BASE_SHA, 'tree': m.BASE_TREE}, 'base identity mismatch')
    require(review.get('reviewedBackport') == {'sha': m.BACKPORT_SHA, 'tree': m.BACKPORT_TREE}, 'backport identity mismatch')
    m._diff_schema(review.get('metadataDiff'), 'metadata diff')
    m._diff_schema(review.get('sourceDiff'), 'source diff')
    require({x['path'] for x in review['metadataDiff']} == m.METADATA_PATHS, 'metadata membership mismatch')
    require({x['path'] for x in review['sourceDiff']} == m.METADATA_PATHS | m.BACKPORT_PATHS, 'full diff membership mismatch')
    timestamp(review.get('checkedAt'))
    return {name: copy.deepcopy(review[name]) for name in (
        'repository', 'profile', 'version', 'tag', 'channel', 'source', 'control',
        'profileSHA256', 'base', 'reviewedBackport', 'sourceDiff', 'metadataDiff')}


def make_candidate(source_review, gate_contract_bytes, context):
    identity = _admission_identity(source_review)
    contract = parse_json(gate_contract_bytes)
    validate_contract(contract, identity['source'])
    require(sha256(gate_contract_bytes) == identity['control']['files'][CONTRACT_PATH], 'gate contract is not control-registered')
    identity['gateContractSHA256'] = sha256(gate_contract_bytes)
    identity['gateContractCanonicalSHA256'] = digest(contract)
    result = {'schemaVersion': 2, 'kind': 'maintenance-release-candidate', 'identity': identity,
              'candidateDigest': digest(identity), 'gateContract': contract,
              'run': github_context(context, identity['control']['sha']),
              'sourceReviewedAt': source_review['checkedAt'], 'publicationAdmitted': False}
    validate_candidate(result)
    return result


def validate_candidate(candidate):
    keys(candidate, {'schemaVersion', 'kind', 'identity', 'candidateDigest', 'gateContract', 'run',
                     'sourceReviewedAt', 'publicationAdmitted'}, 'candidate')
    require(type(candidate['schemaVersion']) is int and candidate['schemaVersion'] == 2
            and candidate['kind'] == 'maintenance-release-candidate'
            and candidate['publicationAdmitted'] is False, 'candidate is not publication admission')
    identity = candidate['identity']
    require(type(identity) is dict, 'candidate identity required')
    review = dict(identity, schemaVersion=1, sourceAdmission=True, dispatchAdmitted=False,
                  publicationAdmitted=False, checkedAt=candidate['sourceReviewedAt'])
    base_identity = _admission_identity(review, strict=False)
    keys(identity, set(base_identity) | {'gateContractSHA256', 'gateContractCanonicalSHA256'}, 'candidate identity')
    require(identity['gateContractSHA256'] == identity['control']['files'][CONTRACT_PATH], 'gate contract registration mismatch')
    require(identity['gateContractCanonicalSHA256'] == digest(candidate['gateContract']), 'gate contract content changed')
    validate_contract(candidate['gateContract'], identity['source'])
    require(candidate['candidateDigest'] == digest(identity), 'candidate digest mismatch')
    keys(candidate['run'], {'id', 'attempt'}, 'candidate run')
    require(type(candidate['run']['id']) is str and re.fullmatch(r'[1-9][0-9]*', candidate['run']['id']), 'invalid candidate run')
    integer(candidate['run']['attempt'], 'candidate attempt', 1)
    return candidate


def binding(candidate):
    validate_candidate(candidate)
    value = {name: copy.deepcopy(candidate['identity'][name]) for name in
             ('repository', 'profile', 'version', 'source', 'control', 'profileSHA256')}
    return dict(value, candidateDigest=candidate['candidateDigest'], run=copy.deepcopy(candidate['run']))


def discovered_tests(raw):
    require(len(raw) <= 16 * 1024 * 1024, 'discovery exceeds bound')
    suite, tests = None, []
    for original in raw.decode('utf-8').splitlines():
        line = original.split('#', 1)[0].rstrip()
        if not line or line.startswith('Running main() from '):
            continue
        if not line[0].isspace():
            require(line.endswith('.') and not any(c.isspace() for c in line), 'invalid discovery suite')
            suite = line
        else:
            name = line.strip()
            require(suite is not None and name and not any(c.isspace() for c in name), 'invalid discovery case')
            tests.append(suite + name)
    return _test_ids(tests)


def executed_tests(raw):
    require(len(raw) <= 16 * 1024 * 1024 and b'<!DOCTYPE' not in raw.upper()
            and b'<!ENTITY' not in raw.upper(), 'unsupported XML declaration or bound')
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        raise ValueError('invalid test XML') from None
    require(root.tag in ('testsuites', 'testsuite'), 'GTest XML root required')
    suites = list(root) if root.tag == 'testsuites' else [root]
    require(suites and all(s.tag == 'testsuite' for s in suites), 'unexpected XML suite structure')
    result = []
    for suite in suites:
        cases = [item for item in suite if item.tag == 'testcase']
        require(cases and suite.get('tests') == str(len(cases)), 'suite count mismatch or zero tests')
        for name in ('failures', 'errors', 'disabled', 'skipped'):
            require(suite.get(name, '0') == '0', 'failed/skipped/disabled suite')
        for case in cases:
            require(case.get('status') == 'run' and case.get('result') == 'completed', 'test did not complete')
            require(not any(item.tag in ('failure', 'error', 'skipped') for item in case.iter()), 'test issue present')
            require(case.get('classname') == suite.get('name'), 'test class/suite mismatch')
            result.append(text(case.get('classname'), 'test class') + '.' + text(case.get('name'), 'test case'))
    require(len(root.findall('.//testcase')) == len(result) if root.tag == 'testsuites'
            else len(root.findall('testcase')) == len(result), 'unaccounted test cases')
    require(root.get('tests') == str(len(result)), 'XML total count mismatch')
    for name in ('failures', 'errors', 'disabled', 'skipped'):
        require(root.get(name, '0') == '0', 'XML contains nonpassing totals')
    return _test_ids(result)


def _commands(gate):
    if gate.startswith('core-'):
        return {'core'}
    required = {'swiftpm', 'cmake-build', 'cmake', 'symbols', 'c11'}
    return required | ({'all-target'} if gate.endswith('linux') else set())


def _symbols(raw):
    names = [line.strip() for line in raw.decode('utf-8').splitlines()
             if line.strip() and not line.lstrip().startswith('#')]
    require(names and len(names) == len(set(names))
            and all(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', name) for name in names), 'invalid/duplicate symbols')
    return sorted(names)


def make_gate_receipt(candidate, gate, artifact_root, manifest, context, *, read_file=None):
    validate_candidate(candidate)
    require(gate in GATES, 'unknown native gate')
    contract = candidate['gateContract']['gates'][gate]
    read_file = (lambda path: read_artifact(artifact_root, path)) if read_file is None else read_file
    github_context(context, candidate['identity']['control']['sha'], run=candidate['run'], platform=contract['platform'])
    keys(manifest, {'schemaVersion', 'gate', 'suites', 'commands', 'artifacts'}, 'gate manifest')
    require(type(manifest['schemaVersion']) is int and manifest['schemaVersion'] == 1
            and manifest['gate'] == gate, 'gate manifest identity mismatch')
    keys(manifest['suites'], contract['suites'], 'observed suites')
    keys(manifest['commands'], _commands(gate), 'native commands')
    require(type(manifest['artifacts']) is dict and 'toolchain' in manifest['artifacts'], 'toolchain artifact required')
    paths, suites = set(manifest['artifacts'].values()), {}
    for name, value in manifest['suites'].items():
        keys(value, {'xml', 'discovery'}, 'suite artifacts')
        paths.update(value.values())
        discovery = discovered_tests(read_file(value['discovery']))
        executed = executed_tests(read_file(value['xml']))
        require(discovery == executed == _test_ids(contract['suites'][name]), 'discovered/executed/approved membership mismatch')
        suites[name] = {'tests': executed, 'count': len(executed), **value}
    for name, command in manifest['commands'].items():
        keys(command, {'argv', 'exitCode', 'timedOut', 'signal', 'log'}, 'command receipt')
        require(type(command['argv']) is list and command['argv'], 'command argv required')
        for arg in command['argv']:
            text(arg, 'command argument')
        require(type(command['exitCode']) is int and command['exitCode'] == 0
                and command['timedOut'] is False and command['signal'] is None, 'native command failed or incomplete')
        paths.add(command['log'])
    if gate.startswith('capi-'):
        for name in ('symbols-declared', 'symbols-exported'):
            require(name in manifest['artifacts'], 'C API symbol evidence missing')
            require(_symbols(read_file(manifest['artifacts'][name])) == sorted(contract['symbols']), 'C API symbols changed')
    require(0 < len(paths) <= 64, 'artifact membership bound exceeded')
    artifacts, total = {}, 0
    for path in sorted(paths):
        _relative(path)
        raw = read_file(path)
        require(type(raw) is bytes and 0 < len(raw) <= MAX_FILE, 'artifact bytes invalid or excessive')
        total += len(raw)
        artifacts[path] = {'sha256': sha256(raw), 'bytes': len(raw)}
    require(total <= MAX_TOTAL, 'artifact total exceeds bound')
    receipt = {'schemaVersion': 2, 'kind': 'maintenance-native-gate', 'binding': binding(candidate),
               'gate': gate, 'platform': contract['platform'], 'job': context['GITHUB_JOB'],
               'result': 'passed', 'suites': suites, 'manifest': copy.deepcopy(manifest), 'artifacts': artifacts}
    receipt['receiptDigest'] = digest(receipt)
    return receipt


def verify_gate_receipt(candidate, receipt, artifact_root, *, files=None):
    keys(receipt, {'schemaVersion', 'kind', 'binding', 'gate', 'platform', 'job', 'result', 'suites',
                   'manifest', 'artifacts', 'receiptDigest'}, 'native receipt')
    require(receipt['binding'] == binding(candidate), 'native receipt source/control/run mismatch')
    payload = {k: v for k, v in receipt.items() if k != 'receiptDigest'}
    require(receipt['receiptDigest'] == digest(payload), 'native receipt digest mismatch')
    context = {'GITHUB_ACTIONS': 'true', 'GITHUB_EVENT_NAME': 'workflow_dispatch', 'GITHUB_REF': 'refs/heads/main',
               'GITHUB_REPOSITORY': m.REPOSITORY, 'GITHUB_SHA': candidate['identity']['control']['sha'],
               'GITHUB_RUN_ID': candidate['run']['id'], 'GITHUB_RUN_ATTEMPT': str(candidate['run']['attempt']),
               'RUNNER_OS': receipt['platform'], 'GITHUB_JOB': receipt['job']}
    rebuilt = make_gate_receipt(candidate, receipt['gate'], artifact_root, receipt['manifest'], context,
                                read_file=files.__getitem__ if files is not None else None)
    require(rebuilt == receipt, 'native receipt artifact/content mismatch')
    return receipt['receiptDigest']


def verify_high_fd(candidate, attestation_bytes, artifact_root, *, files=None):
    approved = candidate['gateContract']['highFd']
    require(sha256(attestation_bytes) == approved['attestationSHA256'], 'high descriptor attestation not independently approved')
    value = parse_json(attestation_bytes)
    keys(value, {'schemaVersion', 'kind', 'repository', 'source', 'owner', 'review', 'run', 'build', 'proof', 'artifacts'}, 'high descriptor attestation')
    require(type(value['schemaVersion']) is int and value['schemaVersion'] == 1
            and value['kind'] == 'maintenance-high-fd-attestation' and value['repository'] == m.REPOSITORY,
            'high descriptor attestation kind mismatch')
    require(value['source'] == candidate['gateContract']['source'], 'high descriptor final source mismatch')
    require(value['owner'] == approved['owner'] and value['review'] == approved['review'], 'high descriptor owner/review mismatch')
    require(value['run'] == {'id': approved['runId'], 'attempt': approved['runAttempt']}, 'high descriptor run mismatch')
    keys(value['build'], {'artifact', 'sha256', 'source'}, 'high descriptor build')
    require(value['build']['source'] == value['source'], 'high descriptor build source mismatch')
    hash_value(value['build']['sha256'], 'high descriptor build hash')
    proof = value['proof']
    keys(proof, {'fdSetSize', 'watchedFd', 'wakeReadFd', 'wakeWriteFd', 'notificationDelivered',
                 'shutdownExitCode', 'timedOut', 'shutdownElapsedMilliseconds', 'shutdownLimitMilliseconds'}, 'high descriptor proof')
    threshold = integer(proof['fdSetSize'], 'FD_SETSIZE', 1)
    for name in ('watchedFd', 'wakeReadFd', 'wakeWriteFd'):
        integer(proof[name], name, threshold)
    require(proof['notificationDelivered'] is True and type(proof['shutdownExitCode']) is int
            and proof['shutdownExitCode'] == 0 and proof['timedOut'] is False, 'high descriptor proof incomplete')
    integer(proof['shutdownElapsedMilliseconds'], 'shutdown elapsed milliseconds')
    require(proof['shutdownLimitMilliseconds'] == approved['shutdownLimitMilliseconds']
            and proof['shutdownElapsedMilliseconds'] <= proof['shutdownLimitMilliseconds'], 'shutdown exceeded approved bound')
    require(type(value['artifacts']) is dict and 0 < len(value['artifacts']) <= 64, 'high descriptor artifacts required')
    total = 0
    for path, item in value['artifacts'].items():
        keys(item, {'sha256', 'bytes'}, 'attested artifact')
        _relative(path)
        raw = files[path] if files is not None else read_artifact(artifact_root, path)
        require(type(raw) is bytes and 0 < len(raw) <= MAX_FILE, 'attested artifact bytes invalid')
        total += len(raw)
        require(item == {'sha256': sha256(raw), 'bytes': len(raw)}, 'high descriptor artifact mismatch')
    require(total <= MAX_TOTAL, 'high descriptor artifacts exceed bound')
    require(value['build']['artifact'] in value['artifacts']
            and value['artifacts'][value['build']['artifact']]['sha256'] == value['build']['sha256'], 'attested executable missing or changed')
    return approved['attestationSHA256']


def aggregate_receipts(candidate, receipts, artifact_roots, high_fd_bytes, high_fd_root, context):
    github_context(context, candidate['identity']['control']['sha'], run=candidate['run'])
    require(type(receipts) is list and len(receipts) == len(GATES), 'exactly four native gate receipts required')
    keys(artifact_roots, GATES, 'native artifact roots')
    by_gate = {}
    for receipt in receipts:
        gate = receipt.get('gate')
        require(gate in GATES and gate not in by_gate, 'duplicate or unknown native gate')
        by_gate[gate] = verify_gate_receipt(candidate, receipt, artifact_roots[gate])
    require(set(by_gate) == set(GATES), 'native gate missing')
    high_digest = verify_high_fd(candidate, high_fd_bytes, high_fd_root)
    result = {'schemaVersion': 2, 'kind': 'maintenance-release-receipt', 'binding': binding(candidate),
              'state': 'validated-source-and-native-evidence', 'gateReceiptDigests': by_gate,
              'highFdAttestationSHA256': high_digest, 'publicationAdmitted': False}
    result['receiptDigest'] = digest(result)
    return result


def retry_identity(candidate):
    return {key: copy.deepcopy(binding(candidate)[key]) for key in
            ('repository', 'profile', 'version', 'source', 'control', 'candidateDigest')}


def run_title(candidate):
    return (f'Release {m.PROFILE} {m.VERSION} control {candidate["identity"]["control"]["sha"]} '
            f'product {candidate["identity"]["source"]["sha"]}')


def _run_identity(candidate, observed, run_id, attempt):
    require(type(observed) is dict and observed.get('id') == int(run_id)
            and observed.get('run_attempt') == attempt
            and observed.get('head_sha') == candidate['identity']['control']['sha']
            and observed.get('event') == 'workflow_dispatch' and observed.get('head_branch') == 'main'
            and observed.get('path') == '.github/workflows/release.yml'
            and observed.get('display_title') == run_title(candidate)
            and observed.get('repository', {}).get('full_name') == m.REPOSITORY,
            'GitHub release workflow/run/control/product identity mismatch')


def authenticated_attempt_inventory(candidate):
    validate_candidate(candidate)
    endpoint = (f'repos/{m.REPOSITORY}/actions/workflows/release.yml/runs'
                f'?head_sha={candidate["identity"]["control"]["sha"]}')
    runs = _api_pages(endpoint, 'workflow_runs')
    attempts = []
    for run in runs:
        if run.get('display_title') != run_title(candidate):
            continue
        run_id, count = str(run['id']), integer(run.get('run_attempt'), 'run attempt count', 1)
        require(count <= 1000, 'attempt count exceeds bound')
        for attempt in range(1, count + 1):
            observed = r.gh_api(f'repos/{m.REPOSITORY}/actions/runs/{run_id}/attempts/{attempt}')
            _run_identity(candidate, observed, run_id, attempt)
            attempts.append({'identity': retry_identity(candidate), 'runId': run_id,
                             'runAttempt': attempt, 'runNumber': observed['run_number'],
                             'status': observed['status'], 'conclusion': observed['conclusion']})
    result = {'complete': True, 'runs': attempts}
    reconcile_attempts(candidate, result)
    return result


def require_fresh_attempt(candidate, inventory):
    reconcile_attempts(candidate, inventory)
    matching = [row for row in inventory['runs'] if row['identity'] == retry_identity(candidate)]
    require(candidate['run']['attempt'] == 1 and len(matching) == 1
            and matching[0]['runId'] == candidate['run']['id'] and matching[0]['runAttempt'] == 1,
            'unchanged retry/duplicate attempt requires a separately reviewed exception; none is registered')


def reconcile_attempts(candidate, attempts):
    require(type(attempts) is dict and attempts.get('complete') is True, 'complete authenticated attempt inventory required')
    keys(attempts, {'complete', 'runs'}, 'attempt inventory')
    require(type(attempts['runs']) is list and len(attempts['runs']) <= 100000, 'bounded attempt list required')
    relevant, seen = [], set()
    for row in attempts['runs']:
        keys(row, {'identity', 'runId', 'runAttempt', 'runNumber', 'status', 'conclusion'}, 'prior attempt')
        if row['identity'] != retry_identity(candidate):
            continue
        text(row['runId'], 'prior run')
        require(re.fullmatch(r'[1-9][0-9]*', row['runId']), 'invalid prior run ID')
        integer(row['runAttempt'], 'prior attempt', 1)
        integer(row['runNumber'], 'prior run number', 1)
        key = (row['runId'], row['runAttempt'])
        require(key not in seen, 'duplicate prior attempt')
        seen.add(key)
        require(row['status'] in ('queued', 'in_progress', 'completed'), 'unknown run status')
        require(row['conclusion'] in (None, 'success', 'failure', 'cancelled', 'timed_out', 'action_required', 'neutral', 'skipped', 'stale', 'startup_failure'), 'unknown run conclusion')
        require((row['status'] == 'completed') == (row['conclusion'] is not None), 'inconsistent run terminal state')
        relevant.append(row)
    if not relevant:
        return {'state': 'no-prior-attempt-observed', 'dispatchAdmitted': False, 'automaticRetry': False}
    latest = max(relevant, key=lambda row: (row['runNumber'], row['runAttempt']))
    return {'state': 'existing-attempt-requires-reconciliation', 'attempt': copy.deepcopy(latest),
            'dispatchAdmitted': False, 'automaticRetry': False}


def _latest(value):
    keys(value, {'id', 'tag', 'sourceSha'}, 'latest release')
    integer(value['id'], 'latest release ID', 1)
    text(value['tag'], 'latest tag')
    full_sha(value['sourceSha'], 'latest source')
    require(r.Version(value['tag']).core[0] >= 2, 'latest release must remain on modern line')


def validate_aggregate(candidate, receipt):
    keys(receipt, {'schemaVersion', 'kind', 'binding', 'state', 'gateReceiptDigests', 'highFdAttestationSHA256',
                   'publicationAdmitted', 'receiptDigest'}, 'aggregate receipt')
    require(receipt['schemaVersion'] == 2 and receipt['kind'] == 'maintenance-release-receipt'
            and receipt['binding'] == binding(candidate)
            and receipt['state'] == 'validated-source-and-native-evidence'
            and receipt['publicationAdmitted'] is False, 'aggregate identity/state mismatch')
    keys(receipt['gateReceiptDigests'], GATES, 'aggregate gates')
    for value in receipt['gateReceiptDigests'].values():
        hash_value(value, 'gate digest')
    require(receipt['highFdAttestationSHA256'] == candidate['gateContract']['highFd']['attestationSHA256'], 'aggregate high descriptor mismatch')
    require(receipt['receiptDigest'] == digest({k: v for k, v in receipt.items() if k != 'receiptDigest'}), 'aggregate digest mismatch')


def publication_plan(candidate, receipt_bytes, remote_state, notes_file, receipt_file, authenticated_gates, notes_bytes):
    validate_candidate(candidate)
    receipt = parse_json(receipt_bytes)
    validate_aggregate(candidate, receipt)
    keys(authenticated_gates, {'binding', 'gates', 'highFd', 'checkedAt'}, 'authenticated CI evidence')
    require(authenticated_gates['binding'] == binding(candidate), 'CI evidence identity mismatch')
    keys(authenticated_gates['gates'], GATES, 'authenticated gate jobs')
    for gate, observed in authenticated_gates['gates'].items():
        require(observed.get('receiptDigest') == receipt['gateReceiptDigests'][gate], 'CI archive receipt does not match aggregate')
    require(authenticated_gates['highFd'].get('attestationSHA256') == receipt['highFdAttestationSHA256'], 'CI high descriptor mismatch')
    timestamp(authenticated_gates['checkedAt'])
    keys(remote_state, {'repository', 'tag', 'tagExists', 'releaseExists', 'latest', 'observedAt'}, 'prepublication state')
    require(remote_state['repository'] == m.REPOSITORY and remote_state['tag'] == m.VERSION
            and remote_state['tagExists'] is False and remote_state['releaseExists'] is False,
            'existing or partial publication cannot be overwritten')
    timestamp(remote_state['observedAt'])
    _latest(remote_state['latest'])
    for path in (notes_file, receipt_file):
        require(Path(path).is_absolute(), 'publication artifact paths must be absolute')
        text(str(path), 'publication path')
    require(Path(receipt_file).name == 'release-receipt.json', 'release receipt asset must use canonical name')
    require(type(notes_bytes) is bytes and 0 < len(notes_bytes) <= MAX_JSON and notes_bytes.strip(), 'nonempty bounded notes required')
    source = candidate['identity']['source']['sha']
    value = {'schemaVersion': 2, 'kind': 'maintenance-publication-plan', 'binding': binding(candidate),
             'tag': m.VERSION, 'sourceSha': source, 'makeLatest': False, 'latestBefore': copy.deepcopy(remote_state['latest']),
             'notesFile': str(notes_file), 'releaseReceiptFile': str(receipt_file),
             'notesSHA256': sha256(notes_bytes), 'notesBytes': len(notes_bytes),
             'releaseReceiptSHA256': sha256(receipt_bytes), 'releaseReceiptBytes': len(receipt_bytes),
             'gateReceiptDigests': receipt['gateReceiptDigests'], 'highFdAttestationSHA256': receipt['highFdAttestationSHA256'],
             'authenticatedCI': copy.deepcopy(authenticated_gates),
             'createRefArgs': ['gh', 'api', '--method', 'POST', f'repos/{m.REPOSITORY}/git/refs',
                               '-f', 'ref=refs/tags/' + m.VERSION, '-f', 'sha=' + source],
             'releaseArgs': ['gh', 'release', 'create', m.VERSION, str(receipt_file), '--repo', m.REPOSITORY,
                             '--verify-tag', '--target', source, '--title', m.VERSION,
                             '--notes-file', str(notes_file), '--latest=false'],
             'execute': False}
    value['planDigest'] = digest(value)
    return value


def verify_publication(plan, observed):
    require(type(plan) is dict and plan.get('schemaVersion') == 2 and plan.get('kind') == 'maintenance-publication-plan'
            and plan.get('makeLatest') is False and plan.get('execute') is False, 'publication plan required')
    require(plan.get('planDigest') == digest({k: v for k, v in plan.items() if k != 'planDigest'}), 'publication plan changed')
    keys(observed, {'repository', 'tag', 'tagExists', 'releaseExists', 'latest', 'observedAt', 'sourceSha', 'release'}, 'published state')
    timestamp(observed['observedAt'])
    require(observed['repository'] == m.REPOSITORY and observed['tag'] == plan['tag']
            and observed['sourceSha'] == plan['sourceSha'] and observed['tagExists'] is True
            and observed['releaseExists'] is True, 'published tag/source identity mismatch')
    require(observed['latest'] == plan['latestBefore'], 'maintenance release displaced latest')
    release = observed['release']
    keys(release, {'id', 'tag', 'draft', 'prerelease', 'assets'}, 'published release')
    integer(release['id'], 'published release ID', 1)
    require(release['tag'] == plan['tag'] and release['draft'] is False and release['prerelease'] is False,
            'published release is not stable and public')
    require(type(release['assets']) is list and len(release['assets']) == 1, 'unexpected release asset membership')
    require(release['assets'][0] == {'name': 'release-receipt.json', 'sha256': plan['releaseReceiptSHA256'],
                                     'bytes': plan['releaseReceiptBytes']}, 'published receipt bytes changed')
    return {'state': 'maintenance-publication-verified', 'binding': plan['binding'], 'releaseId': release['id'],
            'planDigest': plan['planDigest'], 'latestPreserved': True,
            'downstreamAdoptionAdmitted': False, 'remainingGate': 'independent full release workflow completion verification'}


def verify_source(candidate, control_root, product_root, context):
    validate_candidate(candidate)
    control = Path(control_root).resolve()
    require(control == Path(__file__).resolve().parent.parent, 'control must be this installed checkout')
    profile = m.load_profile(control)
    m.validate_profile(profile, require_enabled=True)
    product = Path(product_root).resolve()
    require(control != product and not control.is_relative_to(product)
            and not product.is_relative_to(control), 'source roots must be distinct and non-nested')
    identity = candidate['identity']
    require(m._checkout(control, identity['control']['sha'], 'control') == identity['control']['tree'], 'control tree drift')
    require(m._checkout(product, identity['source']['sha'], 'product') == identity['source']['tree'], 'product tree drift')
    require(profile['registration']['sourceSha'] == identity['source']['sha']
            and profile['registration']['sourceTree'] == identity['source']['tree'], 'profile source registration drift')
    require(r.digest(m._tracked_file(control, m.PROFILE_PATH)) == identity['profileSHA256'], 'profile bytes changed')
    actual_files = {name: r.digest(m._tracked_file(control, name)) for name in identity['control']['files']}
    require(actual_files == identity['control']['files'] == profile['controlRegistration']['files'], 'registered control bytes changed')
    github_context(context, identity['control']['sha'], run=candidate['run'])
    return {'state': 'maintenance-source-rechecked', 'binding': binding(candidate)}


def _api_pages(endpoint, member):
    result, seen, total = [], set(), None
    for page in range(1, 1001):
        separator = '&' if '?' in endpoint else '?'
        data = r.gh_api(f'{endpoint}{separator}per_page=100&page={page}')
        require(type(data) is dict and type(data.get(member)) is list
                and len(data[member]) <= 100, 'malformed CI inventory page')
        integer(data.get('total_count'), 'CI inventory total')
        if total is None:
            total = data['total_count']
        require(total == data['total_count'] and total <= 100000, 'CI inventory changed or exceeds bound')
        for item in data[member]:
            require(type(item) is dict, 'malformed CI inventory item')
            integer(item.get('id'), 'CI inventory ID', 1)
            require(item['id'] not in seen, 'repeated CI inventory item/page')
            seen.add(item['id'])
            result.append(item)
        if len(data[member]) < 100:
            require(len(result) == total, 'incomplete CI inventory')
            return result
    raise ValueError('CI pagination limit exceeded')


def _archive_bytes(artifact):
    integer(artifact.get('size_in_bytes'), 'archive bytes', 1)
    require(artifact['size_in_bytes'] <= MAX_TOTAL, 'archive exceeds bound')
    limit = artifact['size_in_bytes']
    chunks, size, deadline = [], 0, time.monotonic() + 120
    command = ['gh', 'api', f'repos/{m.REPOSITORY}/actions/artifacts/{artifact["id"]}/zip']
    # Bound binary bytes and time while streaming; never extract an archive.
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL) as process:
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                while True:
                    remaining = deadline - time.monotonic()
                    require(remaining > 0 and selector.select(remaining), 'archive download timeout')
                    raw = os.read(process.stdout.fileno(), min(65536, limit + 1 - size))
                    if not raw:
                        break
                    chunks.append(raw)
                    size += len(raw)
                    require(size <= limit, 'archive download exceeds authenticated size')
            require(process.wait(timeout=max(0.01, deadline - time.monotonic())) == 0,
                    'archive download failed')
        except BaseException:
            process.kill()
            process.wait(timeout=10)
            raise
    require(size == limit, 'archive byte count mismatch')
    return b''.join(chunks)


def _zip_files(raw):
    require(type(raw) is bytes and len(raw) <= MAX_TOTAL, 'archive exceeds bound')
    result, total = {}, 0
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            require(0 < len(archive.infolist()) <= 256, 'archive entry count exceeds bound')
            seen = set()
            for item in archive.infolist():
                name = item.filename.rstrip('/') if item.is_dir() else item.filename
                _relative(name)
                require(name not in seen, 'duplicate archive member')
                seen.add(name)
                kind = (item.external_attr >> 16) & 0o170000
                require(kind in ((0, 0o040000) if item.is_dir() else (0, 0o100000)), 'archive symlink/special member forbidden')
                require(not item.flag_bits & 1, 'encrypted archive forbidden')
                if item.is_dir():
                    continue
                require(0 < item.file_size <= MAX_FILE, 'archive member empty or exceeds bound')
                total += item.file_size
                require(total <= MAX_TOTAL, 'archive expansion exceeds bound')
                result[item.filename] = archive.read(item)
    except (zipfile.BadZipFile, RuntimeError, OSError):
        raise ValueError('invalid immutable artifact archive') from None
    return result


def _artifact_metadata(artifact, name, run, expected_sha=None):
    require(artifact.get('name') == name and artifact.get('expired') is False, 'artifact name/expiration mismatch')
    integer(artifact.get('id'), 'artifact ID', 1)
    integer(artifact.get('size_in_bytes'), 'artifact size', 1)
    require(artifact['size_in_bytes'] <= MAX_TOTAL, 'artifact archive exceeds bound')
    require(type(artifact.get('digest')) is str and artifact['digest'].startswith('sha256:'), 'immutable artifact digest required')
    hash_value(artifact['digest'][7:], 'artifact digest')
    producer = artifact.get('workflow_run')
    require(type(producer) is dict and producer.get('id') == int(run['id']), 'artifact producer run mismatch')
    if expected_sha is not None:
        require(producer.get('head_sha') == expected_sha and producer.get('head_branch') == 'main', 'artifact producer source mismatch')
    timestamp(artifact.get('created_at'))
    return artifact


def authenticate_ci(candidate, aggregate, api=None, download=None):
    """Authenticate gate archives via GitHub before issuing a publication plan.

    api/download are dependency-injection seams for synthetic tests only. The
    CLI supplies no overrides and uses authenticated same-repository endpoints.
    """
    validate_candidate(candidate)
    validate_aggregate(candidate, aggregate)
    api = r.gh_api if api is None else api
    download = _archive_bytes if download is None else download
    run = candidate['run']
    base = f'repos/{m.REPOSITORY}/actions'
    observed_run = api(f'{base}/runs/{run["id"]}/attempts/{run["attempt"]}')
    _run_identity(candidate, observed_run, run['id'], run['attempt'])
    require(observed_run.get('status') == 'in_progress' or
            (observed_run.get('status') == 'completed' and observed_run.get('conclusion') == 'success'),
            'release run is not active or successful')
    attempt_started = timestamp(observed_run.get('run_started_at'))
    # Pagination uses the same injected API in tests, without changing globals.
    def pages(endpoint, member):
        result, seen, total = [], set(), None
        for page in range(1, 1001):
            value = api(f'{endpoint}?per_page=100&page={page}')
            require(type(value) is dict and type(value.get(member)) is list and len(value[member]) <= 100, 'malformed CI page')
            integer(value.get('total_count'), 'CI total')
            total = value['total_count'] if total is None else total
            require(total == value['total_count'] and total <= 100000, 'CI inventory changed or excessive')
            for row in value[member]:
                integer(row.get('id'), 'CI row ID', 1)
                require(row['id'] not in seen, 'duplicate CI row')
                seen.add(row['id']); result.append(row)
            if len(value[member]) < 100:
                require(len(result) == total, 'incomplete CI inventory')
                return result
        raise ValueError('CI page bound exceeded')
    jobs = pages(f'{base}/runs/{run["id"]}/attempts/{run["attempt"]}/jobs', 'jobs')
    artifacts = pages(f'{base}/runs/{run["id"]}/artifacts', 'artifacts')
    evidence = {}
    for gate in GATES:
        matches = [job for job in jobs if job.get('name') == JOB_NAMES[gate]]
        require(len(matches) == 1, 'native job missing or ambiguous')
        job = matches[0]
        require(job.get('status') == 'completed' and job.get('conclusion') == 'success'
                and job.get('run_id') == int(run['id']) and job.get('head_sha') == candidate['identity']['control']['sha'],
                'native job did not complete successfully at control source')
        started, completed = timestamp(job.get('started_at')), timestamp(job.get('completed_at'))
        require(attempt_started <= started <= completed, 'native job predates attempt')
        name = f'maintenance-{gate}-{run["id"]}-{run["attempt"]}'
        found = [item for item in artifacts if item.get('name') == name]
        require(len(found) == 1, 'native artifact missing/ambiguous')
        artifact = _artifact_metadata(found[0], name, run, candidate['identity']['control']['sha'])
        require(started <= timestamp(artifact['created_at']) <= completed, 'artifact not created during authenticated job')
        raw = download(artifact)
        require(len(raw) == artifact['size_in_bytes'] and sha256(raw) == artifact['digest'][7:], 'GitHub native archive bytes mismatch')
        files = _zip_files(raw)
        require('receipt.json' in files, 'native archive receipt missing')
        receipt = parse_json(files['receipt.json'])
        require(receipt.get('gate') == gate and receipt.get('binding') == binding(candidate)
                and receipt.get('receiptDigest') == aggregate['gateReceiptDigests'][gate], 'native archive receipt identity mismatch')
        require(receipt['receiptDigest'] == digest({k: v for k, v in receipt.items() if k != 'receiptDigest'}), 'archived receipt digest mismatch')
        require(type(receipt.get('artifacts')) is dict and set(files) == set(receipt['artifacts']) | {'receipt.json'},
                'unaccounted or missing native archive leaves')
        for path, metadata in receipt['artifacts'].items():
            require(metadata == {'sha256': sha256(files[path]), 'bytes': len(files[path])}, 'native archive leaf changed')
        verify_gate_receipt(candidate, receipt, None, files=files)
        evidence[gate] = {'jobId': job['id'], 'jobName': job['name'], 'artifactId': artifact['id'],
                          'artifactName': name, 'archiveSHA256': sha256(raw), 'archiveBytes': len(raw),
                          'receiptDigest': receipt['receiptDigest']}
    high = candidate['gateContract']['highFd']
    high_run = api(f'{base}/runs/{high["runId"]}/attempts/{high["runAttempt"]}')
    require(high_run.get('id') == int(high['runId']) and high_run.get('run_attempt') == high['runAttempt']
            and high_run.get('repository', {}).get('full_name') == m.REPOSITORY
            and high_run.get('status') == 'completed' and high_run.get('conclusion') == 'success', 'high descriptor run not authenticated successful')
    high_artifact = api(f'{base}/artifacts/{high["artifactId"]}')
    _artifact_metadata(high_artifact, high['artifactName'], {'id': high['runId']})
    require(high_artifact['id'] == high['artifactId'] and high_artifact['digest'] == 'sha256:' + high['archiveSHA256'],
            'high descriptor immutable artifact mismatch')
    require(timestamp(high_run['run_started_at']) <= timestamp(high_artifact['created_at'])
            <= timestamp(high_run['updated_at']), 'high descriptor artifact outside approved attempt')
    raw = download(high_artifact)
    require(len(raw) == high_artifact['size_in_bytes'] and sha256(raw) == high['archiveSHA256'], 'high descriptor archive bytes changed')
    files = _zip_files(raw)
    require(high['attestationPath'] in files and sha256(files[high['attestationPath']]) == high['attestationSHA256'], 'high descriptor attestation bytes changed')
    attestation = parse_json(files[high['attestationPath']])
    require(type(attestation.get('artifacts')) is dict
            and set(files) == set(attestation['artifacts']) | {high['attestationPath']}, 'high descriptor archive inventory incomplete')
    for path, metadata in attestation['artifacts'].items():
        require(metadata == {'sha256': sha256(files[path]), 'bytes': len(files[path])}, 'high descriptor archived leaf changed')
    verify_high_fd(candidate, files[high['attestationPath']], None, files=files)
    return {'binding': binding(candidate), 'gates': evidence,
            'highFd': {'runId': high['runId'], 'runAttempt': high['runAttempt'], 'artifactId': high['artifactId'],
                       'archiveSHA256': high['archiveSHA256'], 'attestationSHA256': high['attestationSHA256']}, 'checkedAt': r.now()}


def capture_state(candidate, phase):
    validate_candidate(candidate)
    latest = r.gh_api(f'repos/{m.REPOSITORY}/releases/latest')
    latest_value = {'id': latest['id'], 'tag': latest['tag_name'],
                    'sourceSha': r.published_tag_commit(m.REPOSITORY, latest['tag_name'])}
    _latest(latest_value)
    value = {'repository': m.REPOSITORY, 'tag': m.VERSION, 'latest': latest_value, 'observedAt': r.now()}
    if phase == 'before':
        m._exact_absence(f'repos/{m.REPOSITORY}/git/ref/tags/{m.VERSION}')
        m._exact_absence(f'repos/{m.REPOSITORY}/releases/tags/{m.VERSION}')
        return dict(value, tagExists=False, releaseExists=False)
    require(phase == 'after', 'invalid capture phase')
    release = r.gh_api(f'repos/{m.REPOSITORY}/releases/tags/{m.VERSION}')
    require(type(release.get('assets')) is list and len(release['assets']) == 1, 'exact release asset set required')
    asset = release['assets'][0]
    require(asset.get('name') == 'release-receipt.json', 'release receipt asset missing')
    integer(asset.get('id'), 'asset ID', 1)
    integer(asset.get('size'), 'asset size', 1)
    require(asset['size'] <= MAX_JSON, 'receipt asset exceeds bound')
    raw = subprocess.check_output(['gh', 'api', f'repos/{m.REPOSITORY}/releases/assets/{asset["id"]}',
                                   '-H', 'Accept: application/octet-stream'], timeout=30)
    require(len(raw) == asset['size'], 'asset download size mismatch')
    return dict(value, tagExists=True, releaseExists=True, sourceSha=r.published_tag_commit(m.REPOSITORY, m.VERSION),
                release={'id': release['id'], 'tag': release['tag_name'], 'draft': release['draft'],
                         'prerelease': release['prerelease'], 'assets': [{'name': asset['name'], 'sha256': sha256(raw), 'bytes': len(raw)}]})


def _load(path):
    path = Path(path).absolute()
    return parse_json(read_artifact(path.parent, path.name, MAX_JSON))


def _outside_output(path, roots):
    output = Path(path).resolve()
    require(all(not output.is_relative_to(Path(root).resolve()) for root in roots), 'output must be outside source checkouts')
    require(not output.exists(), 'output already exists; preserve original evidence')
    return output


def main(argv=None):
    # This check precedes argument parsing, file reads, Git and network calls.
    require(OPERATIONS_ENABLED is True, 'maintenance release operations are code-disabled; reviewed activation required')
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    for command in ('candidate', 'verify-source', 'gate-receipt', 'aggregate-receipts', 'reconcile', 'publication-plan', 'capture-state', 'verify-publication'):
        p = sub.add_parser(command)
        p.add_argument('--output', required=True)
        if command != 'verify-publication':
            p.add_argument('--candidate', required=command != 'candidate')
        if command in ('candidate', 'verify-source', 'gate-receipt', 'aggregate-receipts', 'publication-plan'):
            p.add_argument('--product-root', required=True)
        if command == 'candidate':
            p.add_argument('--source-review', required=True)
            p.add_argument('--gate-contract')
        elif command == 'verify-source':
            p.add_argument('--control-root')
        elif command == 'gate-receipt':
            p.add_argument('--gate', choices=GATES, required=True)
            p.add_argument('--artifact-root', required=True)
            p.add_argument('--gate-manifest', required=True)
        elif command == 'aggregate-receipts':
            p.add_argument('--gate-receipt', action='append', required=True)
            p.add_argument('--gate-artifact-root', action='append', required=True)
            p.add_argument('--high-fd-attestation', required=True)
            p.add_argument('--high-fd-artifact-root', required=True)
        elif command == 'reconcile':
            p.add_argument('--attempts', help='Optional retained inventory; must equal live authenticated reads')
        elif command == 'publication-plan':
            p.add_argument('--release-receipt', required=True)
            p.add_argument('--remote-state', required=True)
            p.add_argument('--notes-file', required=True)
        elif command == 'capture-state':
            p.add_argument('--phase', choices=('before', 'after'), required=True)
        else:
            p.add_argument('--plan', required=True)
            p.add_argument('--observed-state', required=True)
    args = parser.parse_args(argv)
    control = Path(__file__).resolve().parent.parent
    profile = m.load_profile(control)
    m.validate_profile(profile, require_enabled=True)
    product = getattr(args, 'product_root', None)
    output = _outside_output(args.output, [control] + ([product] if product else []))
    context = dict(os.environ)
    if args.command == 'candidate':
        review = _load(args.source_review)
        checked = m.check_candidate(control, Path(product).resolve(), profile, m.VERSION,
                                    review['source']['sha'], review['control']['sha'], context=context)
        require(_admission_identity(review) == _admission_identity(checked), 'supplied source review changed')
        contract_path = control / CONTRACT_PATH
        require(args.gate_contract is None or Path(args.gate_contract).resolve() == contract_path,
                'gate contract must be the control-registered canonical file')
        result = make_candidate(checked, read_artifact(control, CONTRACT_PATH, MAX_JSON), context)
        require_fresh_attempt(result, authenticated_attempt_inventory(result))
    elif args.command == 'verify-publication':
        result = verify_publication(_load(args.plan), _load(args.observed_state))
    else:
        candidate = _load(args.candidate)
        validate_candidate(candidate)
        github_context(context, candidate['identity']['control']['sha'], run=candidate['run'])
        if product:
            verify_source(candidate, getattr(args, 'control_root', None) or control, product, context)
        if args.command == 'verify-source':
            result = {'state': 'maintenance-source-rechecked', 'binding': binding(candidate)}
        elif args.command == 'gate-receipt':
            result = make_gate_receipt(candidate, args.gate, args.artifact_root, _load(args.gate_manifest), context)
        elif args.command == 'aggregate-receipts':
            roots = {}
            for item in args.gate_artifact_root:
                name, sep, path = item.partition('=')
                require(sep and name in GATES and name not in roots and path, 'unique GATE=PATH roots required')
                roots[name] = path
            high = Path(args.high_fd_attestation).absolute()
            result = aggregate_receipts(candidate, [_load(path) for path in args.gate_receipt], roots,
                                        read_artifact(high.parent, high.name, MAX_JSON), args.high_fd_artifact_root, context)
        elif args.command == 'reconcile':
            inventory = authenticated_attempt_inventory(candidate)
            require(args.attempts is None or _load(args.attempts) == inventory, 'retained attempt inventory differs from authenticated reads')
            result = reconcile_attempts(candidate, inventory)
            result['authenticatedInventoryDigest'] = digest(inventory)
        elif args.command == 'capture-state':
            result = capture_state(candidate, args.phase)
        elif args.command == 'publication-plan':
            checked = m.check_candidate(control, Path(product).resolve(), profile, m.VERSION,
                                        candidate['identity']['source']['sha'], candidate['identity']['control']['sha'], context=context)
            stable_identity = _admission_identity(checked)
            require(all(candidate['identity'][key] == value for key, value in stable_identity.items()),
                    'post-gate canonical source admission changed')
            receipt_path = Path(args.release_receipt).absolute()
            notes_path = Path(args.notes_file).absolute()
            notes = read_artifact(notes_path.parent, notes_path.name, MAX_JSON)
            require(notes.strip(), 'nonempty release notes required')
            raw_receipt = read_artifact(receipt_path.parent, receipt_path.name, MAX_JSON)
            require_fresh_attempt(candidate, authenticated_attempt_inventory(candidate))
            authenticated = authenticate_ci(candidate, parse_json(raw_receipt))
            # Artifact downloads can take time. Recheck canonical branches,
            # base lineage and version inventories again after those reads.
            checked_after = m.check_candidate(control, Path(product).resolve(), profile, m.VERSION,
                                              candidate['identity']['source']['sha'], candidate['identity']['control']['sha'], context=context)
            require(_admission_identity(checked_after) == stable_identity,
                    'canonical source admission changed while authenticating gate artifacts')
            fresh_state = capture_state(candidate, 'before')
            prior_state = _load(args.remote_state)
            require(all(prior_state.get(key) == value for key, value in fresh_state.items() if key != 'observedAt'),
                    'prepublication remote state changed')
            result = publication_plan(candidate, raw_receipt, fresh_state, notes_path, receipt_path, authenticated, notes)
    r.write(output, result)
    print(json.dumps(result, indent=2))
    return result


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError) as exc:
        detail = 'required read-only operation failed' if isinstance(exc, subprocess.SubprocessError) else str(exc)
        print('Maintenance release stopped: ' + detail, file=sys.stderr)
        sys.exit(1)
