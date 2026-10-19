"""Synthetic protocol fixtures only: no compiler, process or network execution."""
import contextlib
import copy
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET
import zipfile

with patch.object(sys, 'path', [str(Path(__file__).resolve().parent), *sys.path]):
    import maintenance_release as p


def archive(files):
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_DEFLATED) as bundle:
        for name, raw in sorted(files.items()):
            bundle.writestr(name, raw)
    return output.getvalue()


def xml(tests):
    root = ET.Element('testsuites', tests=str(len(tests)), failures='0', disabled='0', errors='0')
    groups = {}
    for case in tests:
        suite, name = case.rsplit('.', 1)
        groups.setdefault(suite, []).append(name)
    for suite, names in groups.items():
        element = ET.SubElement(root, 'testsuite', name=suite, tests=str(len(names)), failures='0', disabled='0', errors='0')
        for name in names:
            ET.SubElement(element, 'testcase', name=name, classname=suite, status='run', result='completed')
    return ET.tostring(root)


def discovery(tests):
    groups = {}
    for case in tests:
        suite, name = case.rsplit('.', 1)
        groups.setdefault(suite, []).append(name)
    return ''.join(suite + '.\n' + ''.join('  ' + name + '\n' for name in names)
                   for suite, names in groups.items()).encode()


class MaintenanceReleaseTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.context = {'GITHUB_ACTIONS': 'true', 'GITHUB_EVENT_NAME': 'workflow_dispatch',
                        'GITHUB_REF': 'refs/heads/main', 'GITHUB_REPOSITORY': p.m.REPOSITORY,
                        'GITHUB_SHA': 'a' * 40, 'GITHUB_RUN_ID': '101', 'GITHUB_RUN_ATTEMPT': '2'}
        self.source = {'sha': 'b' * 40, 'tree': 'c' * 40}
        self.high_files = {'proof.log': b'SYNTHETIC fixture: no native command was executed.\n',
                           'proof.bin': b'SYNTHETIC non-executable fixture bytes\n'}
        self.high = {'schemaVersion': 1, 'kind': 'maintenance-high-fd-attestation',
                     'repository': p.m.REPOSITORY, 'source': self.source, 'owner': 'jsflax',
                     'review': 'SYNTHETIC independent-owner-review fixture', 'run': {'id': '88', 'attempt': 1},
                     'build': {'artifact': 'proof.bin', 'sha256': p.sha256(self.high_files['proof.bin']), 'source': self.source},
                     'proof': {'fdSetSize': 1024, 'watchedFd': 1025, 'wakeReadFd': 1026, 'wakeWriteFd': 1027,
                               'notificationDelivered': True, 'shutdownExitCode': 0, 'timedOut': False,
                               'shutdownElapsedMilliseconds': 25, 'shutdownLimitMilliseconds': 5000},
                     'artifacts': {name: {'sha256': p.sha256(raw), 'bytes': len(raw)} for name, raw in self.high_files.items()}}
        self.high_bytes = p.encoded(self.high)
        self.high_archive_files = dict(self.high_files, **{'attestation.json': self.high_bytes})
        self.high_archive = archive(self.high_archive_files)
        self.contract = {'schemaVersion': 1, 'source': self.source, 'gates': {},
                         'highFd': {'attestationSHA256': p.sha256(self.high_bytes), 'owner': self.high['owner'],
                                    'review': self.high['review'], 'runId': '88', 'runAttempt': 1, 'artifactId': 999,
                                    'artifactName': 'synthetic-high-fd', 'archiveSHA256': p.sha256(self.high_archive),
                                    'attestationPath': 'attestation.json', 'shutdownLimitMilliseconds': 5000}}
        for gate in p.GATES:
            capi = gate.startswith('capi-')
            self.contract['gates'][gate] = {'platform': 'Linux' if gate.endswith('linux') else 'macOS',
                'suites': {'swiftpm': ['CAPI.One', 'CAPI.Two'], 'cmake': ['CAPI.One', 'CAPI.Two']}
                if capi else {'core': ['Core.One', 'Core.Two']}}
            if capi:
                self.contract['gates'][gate]['symbols'] = ['lattice_one', 'lattice_two']
        self.contract_bytes = p.encoded(self.contract)
        def change(path):
            deleted = path == '.github/workflows/release.yml'
            return {'path': path, 'status': 'D' if deleted else 'M', 'oldMode': '100644', 'oldBlob': 'd' * 40,
                    'newMode': None if deleted else '100644', 'newBlob': None if deleted else 'e' * 40}
        self.review = {'schemaVersion': 1, 'profile': p.m.PROFILE, 'repository': p.m.REPOSITORY,
                       'version': p.m.VERSION, 'tag': p.m.VERSION, 'channel': 'stable',
                       'sourceAdmission': True, 'dispatchAdmitted': False, 'publicationAdmitted': False,
                       'control': {'sha': 'a' * 40, 'tree': 'f' * 40, 'branch': 'main',
                                   'files': {name: '1' * 64 for name in set(p.m.REQUIRED_CONTROL_FILES) | {p.CONTRACT_PATH}}},
                       'profileSHA256': '2' * 64, 'source': dict(self.source, branch=p.m.SOURCE_BRANCH),
                       'base': {'tag': '1.4.2', 'sha': p.m.BASE_SHA, 'tree': p.m.BASE_TREE},
                       'reviewedBackport': {'sha': p.m.BACKPORT_SHA, 'tree': p.m.BACKPORT_TREE},
                       'metadataDiff': [change(name) for name in sorted(p.m.METADATA_PATHS)],
                       'sourceDiff': [change(name) for name in sorted(p.m.METADATA_PATHS | p.m.BACKPORT_PATHS)],
                       'remoteInventory': {'tagCount': 62, 'releaseCount': 46, 'lineTags': ['1.4.2']},
                       'checkedAt': '2026-09-29T12:00:00Z'}
        self.review['control']['files'][p.CONTRACT_PATH] = p.sha256(self.contract_bytes)
        self.candidate = p.make_candidate(self.review, self.contract_bytes, self.context)
        self.high_root = self.root / 'high'; self.high_root.mkdir()
        self.write_files(self.high_root, self.high_files)

    def write_files(self, root, files):
        for name, raw in files.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)

    def gate_fixture(self, gate):
        root = self.root / gate; root.mkdir(exist_ok=True)
        files = {'test-logs/toolchain.txt': b'SYNTHETIC toolchain fixture\n'}
        manifest = {'schemaVersion': 1, 'gate': gate, 'suites': {}, 'commands': {},
                    'artifacts': {'toolchain': 'test-logs/toolchain.txt'}}
        for suite, tests in self.contract['gates'][gate]['suites'].items():
            xml_path, list_path = f'test-logs/{suite}.xml', f'test-logs/{suite}.discovery.txt'
            files[xml_path], files[list_path] = xml(tests), discovery(tests)
            manifest['suites'][suite] = {'xml': xml_path, 'discovery': list_path}
        for stage in p._commands(gate):
            log, script = f'test-logs/{stage}.log', f'scripts/{stage}.sh'
            files[log] = b'SYNTHETIC command result fixture\n'
            files[script] = b'# SYNTHETIC script, not executed\n'
            manifest['commands'][stage] = {'argv': ['bash', '/synthetic/' + script], 'exitCode': 0,
                                           'timedOut': False, 'signal': None, 'log': log}
            manifest['artifacts'][stage + '-script'] = script
        if gate.startswith('capi-'):
            for name in ('symbols-declared', 'symbols-exported'):
                files[name + '.txt'] = b'lattice_one\nlattice_two\n'
                manifest['artifacts'][name] = name + '.txt'
        self.write_files(root, files)
        context = dict(self.context, RUNNER_OS=self.contract['gates'][gate]['platform'],
                       GITHUB_JOB=gate if gate.startswith('capi-') else 'build-and-test')
        receipt = p.make_gate_receipt(self.candidate, gate, root, manifest, context)
        return root, files, manifest, context, receipt

    def all_gates(self):
        receipts, roots, archives = [], {}, {}
        for gate in p.GATES:
            root, files, _, _, receipt = self.gate_fixture(gate)
            roots[gate] = root; receipts.append(receipt)
            archives[gate] = archive(dict(files, **{'receipt.json': p.encoded(receipt)}))
        aggregate = p.aggregate_receipts(self.candidate, receipts, roots, self.high_bytes, self.high_root, self.context)
        return receipts, roots, archives, aggregate

    def ci_fixture(self):
        receipts, roots, archives, aggregate = self.all_gates()
        base = f'repos/{p.m.REPOSITORY}/actions'
        run = {'id': 101, 'run_attempt': 2, 'head_sha': 'a' * 40, 'event': 'workflow_dispatch',
               'head_branch': 'main', 'repository': {'full_name': p.m.REPOSITORY},
               'path': '.github/workflows/release.yml', 'display_title': p.run_title(self.candidate),
               'status': 'in_progress', 'conclusion': None, 'run_started_at': '2026-09-29T12:00:00Z'}
        jobs, artifacts, downloads = [], [], {999: self.high_archive}
        for index, gate in enumerate(p.GATES, 1):
            jobs.append({'id': index, 'name': p.JOB_NAMES[gate], 'run_id': 101, 'head_sha': 'a' * 40,
                         'status': 'completed', 'conclusion': 'success', 'started_at': '2026-09-29T12:01:00Z',
                         'completed_at': '2026-09-29T12:10:00Z'})
            artifact_id = 100 + index; raw = archives[gate]; downloads[artifact_id] = raw
            artifacts.append({'id': artifact_id, 'name': f'maintenance-{gate}-101-2', 'expired': False,
                              'size_in_bytes': len(raw), 'digest': 'sha256:' + p.sha256(raw),
                              'workflow_run': {'id': 101, 'head_sha': 'a' * 40, 'head_branch': 'main'},
                              'created_at': '2026-09-29T12:09:00Z'})
        high_artifact = {'id': 999, 'name': 'synthetic-high-fd', 'expired': False, 'size_in_bytes': len(self.high_archive),
                         'digest': 'sha256:' + p.sha256(self.high_archive), 'workflow_run': {'id': 88},
                         'created_at': '2026-09-29T11:09:00Z'}
        responses = {f'{base}/runs/101/attempts/2': run,
                     f'{base}/runs/101/attempts/2/jobs?per_page=100&page=1': {'total_count': 4, 'jobs': jobs},
                     f'{base}/runs/101/artifacts?per_page=100&page=1': {'total_count': 4, 'artifacts': artifacts},
                     f'{base}/runs/88/attempts/1': {'id': 88, 'run_attempt': 1, 'repository': {'full_name': p.m.REPOSITORY},
                         'status': 'completed', 'conclusion': 'success', 'run_started_at': '2026-09-29T11:00:00Z', 'updated_at': '2026-09-29T11:10:00Z'},
                     f'{base}/artifacts/999': high_artifact}
        return aggregate, responses, downloads

    def test_every_operational_cli_is_disabled_before_io_even_with_environment_flags(self):
        commands = ('candidate', 'verify-source', 'gate-receipt', 'aggregate-receipts', 'reconcile',
                    'publication-plan', 'capture-state', 'verify-publication', '--help')
        for command in commands:
            with self.subTest(command=command), patch.dict(os.environ, {'OPERATIONS_ENABLED': 'true'}), \
                 patch.object(p.m, 'load_profile') as load, patch.object(p.r, 'gh_api') as api, \
                 patch.object(subprocess, 'Popen') as process:
                with self.assertRaisesRegex(ValueError, 'code-disabled'):
                    p.main([command])
                load.assert_not_called(); api.assert_not_called(); process.assert_not_called()
        self.assertIs(p.OPERATIONS_ENABLED, False)

    def test_null_shipped_contract_cannot_create_candidate(self):
        raw = Path(p.__file__).with_name('maintenance-gates.json').read_bytes()
        with self.assertRaises(ValueError):
            p.make_candidate(self.review, raw, self.context)

    def test_candidate_digest_stable_across_run_attempt_but_receipts_bind_attempt(self):
        other = p.make_candidate(self.review, self.contract_bytes, dict(self.context, GITHUB_RUN_ATTEMPT='3'))
        self.assertEqual(other['candidateDigest'], self.candidate['candidateDigest'])
        self.assertNotEqual(p.binding(other), p.binding(self.candidate))
        root, _, _, _, receipt = self.gate_fixture('core-linux')
        with self.assertRaisesRegex(ValueError, 'run mismatch'):
            p.verify_gate_receipt(other, receipt, root)

    def test_candidate_rejects_unregistered_contract_or_wrong_context(self):
        changed = copy.deepcopy(self.contract); changed['gates']['core-linux']['suites']['core'] = ['Core.One']
        with self.assertRaisesRegex(ValueError, 'control-registered'):
            p.make_candidate(self.review, p.encoded(changed), self.context)
        for key, value in [('GITHUB_SHA', 'f' * 40), ('GITHUB_EVENT_NAME', 'push'), ('GITHUB_RUN_ATTEMPT', '0'), ('GITHUB_REPOSITORY', 'other/repo')]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                p.make_candidate(self.review, self.contract_bytes, dict(self.context, **{key: value}))

    def test_candidate_rejects_tampering_and_publication_admission_claim(self):
        for mutate in (lambda c: c['identity']['source'].update(sha='f' * 40),
                       lambda c: c['gateContract']['gates']['core-linux']['suites']['core'].pop(),
                       lambda c: c.update(publicationAdmitted=True)):
            item = copy.deepcopy(self.candidate); mutate(item)
            with self.assertRaises(ValueError): p.validate_candidate(item)
        review = copy.deepcopy(self.review); review['publicationAdmitted'] = True
        with self.assertRaises(ValueError): p.make_candidate(review, self.contract_bytes, self.context)

    def test_discovery_and_xml_exact_membership(self):
        expected = ['Core.One', 'Core.Two']
        self.assertEqual(p.discovered_tests(discovery(expected)), expected)
        self.assertEqual(p.executed_tests(xml(expected)), expected)
        for raw in (b'', b'Core.\n  DISABLED_One\n', b'Core.\n  One\n  One\n', b'invalid output\n'):
            with self.subTest(raw=raw), self.assertRaises(ValueError): p.discovered_tests(raw)
        for raw in (b'<testsuites tests="0"/>', xml(expected).replace(b'result="completed"', b'result="skipped"', 1),
                    xml(expected).replace(b'failures="0"', b'failures="1"', 1),
                    xml(expected).replace(b'name="Two"', b'name="One"'), b'<!DOCTYPE foo><testsuites tests="0"/>'):
            with self.subTest(raw=raw), self.assertRaises(ValueError): p.executed_tests(raw)

    def test_native_gate_rejects_missing_discovered_case_even_if_xml_passes(self):
        root, _, manifest, context, _ = self.gate_fixture('core-linux')
        (root / manifest['suites']['core']['xml']).write_bytes(xml(['Core.One']))
        with self.assertRaisesRegex(ValueError, 'membership mismatch'):
            p.make_gate_receipt(self.candidate, 'core-linux', root, manifest, context)

    def test_native_gate_rejects_stage_failures_missing_stage_and_symbol_drift(self):
        root, _, manifest, context, _ = self.gate_fixture('capi-linux')
        for mutate in (lambda m: m['commands'].pop('all-target'), lambda m: m['commands']['c11'].update(exitCode=1),
                       lambda m: m['commands']['cmake'].update(timedOut=True), lambda m: m['commands']['swiftpm'].update(signal=6)):
            value = copy.deepcopy(manifest); mutate(value)
            with self.assertRaises(ValueError): p.make_gate_receipt(self.candidate, 'capi-linux', root, value, context)
        (root / manifest['artifacts']['symbols-exported']).write_text('lattice_one\n')
        with self.assertRaisesRegex(ValueError, 'symbols changed'):
            p.make_gate_receipt(self.candidate, 'capi-linux', root, manifest, context)

    def test_native_artifact_changes_and_symlink_escape_reject(self):
        root, _, manifest, _, receipt = self.gate_fixture('core-linux')
        log = root / manifest['commands']['core']['log']; log.write_bytes(b'changed\n')
        with self.assertRaises(ValueError): p.verify_gate_receipt(self.candidate, receipt, root)
        log.unlink(); log.symlink_to(self.high_root / 'proof.log')
        with self.assertRaisesRegex(ValueError, 'symlink'): p.read_artifact(root, manifest['commands']['core']['log'])
        with self.assertRaises(ValueError): p.read_artifact(root, '../high/proof.log')

    def test_aggregate_requires_all_unique_exact_gate_receipts(self):
        receipts, roots, _, aggregate = self.all_gates()
        p.validate_aggregate(self.candidate, aggregate)
        for values in (receipts[:-1], [receipts[0], receipts[0], *receipts[2:]]):
            with self.assertRaises(ValueError):
                p.aggregate_receipts(self.candidate, values, roots, self.high_bytes, self.high_root, self.context)

    def test_high_descriptor_requires_approved_digest_final_source_actual_fds_build_and_shutdown(self):
        self.assertEqual(p.verify_high_fd(self.candidate, self.high_bytes, self.high_root), p.sha256(self.high_bytes))
        for mutate in (lambda v: v['proof'].update(watchedFd=5), lambda v: v['proof'].update(shutdownElapsedMilliseconds=5001),
                       lambda v: v['build'].update(sha256='0' * 64), lambda v: v.update(source={'sha': 'd' * 40, 'tree': 'e' * 40})):
            value = copy.deepcopy(self.high); mutate(value); raw = p.encoded(value)
            # A synthetic independent approval for the malformed proof does not
            # bypass semantic checks; production pins remain null.
            candidate = copy.deepcopy(self.candidate)
            candidate['gateContract']['highFd']['attestationSHA256'] = p.sha256(raw)
            with self.assertRaises(ValueError): p.verify_high_fd(candidate, raw, self.high_root)
        with self.assertRaisesRegex(ValueError, 'independently approved'):
            p.verify_high_fd(self.candidate, self.high_bytes + b' ', self.high_root)

    def test_retry_selects_newest_failure_not_old_success_and_never_admits_retry(self):
        identity = p.retry_identity(self.candidate)
        rows = [{'identity': identity, 'runId': '101', 'runAttempt': n, 'runNumber': 3,
                 'status': 'completed', 'conclusion': outcome} for n, outcome in [(1, 'success'), (2, 'failure')]]
        result = p.reconcile_attempts(self.candidate, {'complete': True, 'runs': rows})
        self.assertEqual(result['attempt']['conclusion'], 'failure')
        self.assertFalse(result['automaticRetry']); self.assertFalse(result['dispatchAdmitted'])
        self.assertFalse(p.reconcile_attempts(self.candidate, {'complete': True, 'runs': []})['dispatchAdmitted'])
        with self.assertRaises(ValueError): p.reconcile_attempts(self.candidate, {'complete': False, 'runs': rows})
        with self.assertRaises(ValueError): p.reconcile_attempts(self.candidate, {'complete': True, 'runs': rows + [rows[-1]]})

    def test_github_authentication_accepts_inprogress_release_with_four_success_jobs(self):
        aggregate, responses, downloads = self.ci_fixture()
        result = p.authenticate_ci(self.candidate, aggregate, responses.__getitem__, lambda item: downloads[item['id']])
        self.assertEqual(set(result['gates']), set(p.GATES))
        self.assertEqual(result['highFd']['artifactId'], 999)

    def test_github_authentication_rejects_wrong_run_attempt_failed_missing_job_or_expired_artifact(self):
        aggregate, original, downloads = self.ci_fixture()
        run_key = f'repos/{p.m.REPOSITORY}/actions/runs/101/attempts/2'
        job_key = run_key + '/jobs?per_page=100&page=1'
        artifact_key = f'repos/{p.m.REPOSITORY}/actions/runs/101/artifacts?per_page=100&page=1'
        mutations = [lambda d: d[run_key].update(run_attempt=1), lambda d: d[run_key].update(head_sha='f' * 40),
                     lambda d: d[run_key].update(path='.github/workflows/other.yml'),
                     lambda d: d[run_key].update(display_title='different source'),
                     lambda d: d[job_key]['jobs'][0].update(conclusion='failure'), lambda d: d[job_key]['jobs'].pop(),
                     lambda d: d[artifact_key]['artifacts'][0].update(expired=True),
                     lambda d: d[artifact_key]['artifacts'][0]['workflow_run'].update(id=999),
                     lambda d: d[artifact_key]['artifacts'][0].update(created_at='2026-09-29T10:00:00Z')]
        for mutate in mutations:
            responses = copy.deepcopy(original); mutate(responses)
            with self.assertRaises(ValueError):
                p.authenticate_ci(self.candidate, aggregate, responses.__getitem__, lambda item: downloads[item['id']])

    def test_github_authentication_rejects_archive_tamper_unaccounted_leaf_or_fake_digest(self):
        aggregate, original, downloads = self.ci_fixture()
        key = f'repos/{p.m.REPOSITORY}/actions/runs/101/artifacts?per_page=100&page=1'
        for kind in ('raw', 'unaccounted', 'missing'):
            responses, changed = copy.deepcopy(original), dict(downloads)
            item = responses[key]['artifacts'][0]; ident = item['id']
            if kind == 'raw':
                changed[ident] += b'tamper'
            else:
                files = p._zip_files(changed[ident])
                if kind == 'unaccounted': files['unrecorded.txt'] = b'unrecorded\n'
                else: files.pop('test-logs/toolchain.txt')
                changed[ident] = archive(files)
                item.update(size_in_bytes=len(changed[ident]), digest='sha256:' + p.sha256(changed[ident]))
            with self.assertRaises(ValueError):
                p.authenticate_ci(self.candidate, aggregate, responses.__getitem__, lambda item: changed[item['id']])

    def test_zip_path_traversal_symlink_duplicate_and_empty_members_reject(self):
        for files in ({'../escape': b'a'}, {'/absolute': b'a'}, {'empty': b''}):
            with self.assertRaises(ValueError): p._zip_files(archive(files))
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, 'w') as z:
            info = zipfile.ZipInfo('link'); info.create_system = 3; info.external_attr = 0o120777 << 16
            z.writestr(info, b'/outside')
        with self.assertRaises(ValueError): p._zip_files(stream.getvalue())

    def test_publication_plan_is_immutable_stable_not_latest_and_postcheck_preserves_latest(self):
        aggregate, responses, downloads = self.ci_fixture()
        auth = p.authenticate_ci(self.candidate, aggregate, responses.__getitem__, lambda item: downloads[item['id']])
        remote = {'repository': p.m.REPOSITORY, 'tag': p.m.VERSION, 'tagExists': False, 'releaseExists': False,
                  'latest': {'id': 200, 'tag': '2.0.7', 'sourceSha': 'd' * 40}, 'observedAt': '2026-09-29T12:11:00Z'}
        raw = p.encoded(aggregate)
        plan = p.publication_plan(self.candidate, raw, remote, self.root / 'notes.md', self.root / 'release-receipt.json', auth, b'Synthetic notes\n')
        self.assertIn('--latest=false', plan['releaseArgs']); self.assertNotIn('--prerelease', plan['releaseArgs'])
        self.assertEqual(plan['createRefArgs'], ['gh', 'api', '--method', 'POST', f'repos/{p.m.REPOSITORY}/git/refs',
                                               '-f', 'ref=refs/tags/' + p.m.VERSION, '-f', 'sha=' + self.source['sha']])
        self.assertNotIn('pushArgs', plan); self.assertNotIn('tagArgs', plan); self.assertFalse(plan['execute'])
        self.assertEqual(plan['notesSHA256'], p.sha256(b'Synthetic notes\n')); self.assertEqual(plan['notesBytes'], 16)
        for key in ('tagExists', 'releaseExists'):
            with self.assertRaises(ValueError):
                p.publication_plan(self.candidate, raw, dict(remote, **{key: True}), self.root / 'notes.md', self.root / 'release-receipt.json', auth, b'Synthetic notes\n')
        published = dict(remote, tagExists=True, releaseExists=True, sourceSha=self.source['sha'],
                         release={'id': 201, 'tag': p.m.VERSION, 'draft': False, 'prerelease': False,
                                  'assets': [{'name': 'release-receipt.json', 'sha256': p.sha256(raw), 'bytes': len(raw)}]})
        result = p.verify_publication(plan, published)
        self.assertFalse(result['downstreamAdoptionAdmitted'])
        with self.assertRaisesRegex(ValueError, 'displaced latest'):
            p.verify_publication(plan, dict(published, latest={'id': 201, 'tag': p.m.VERSION, 'sourceSha': self.source['sha']}))

    def test_publication_cli_rechecks_remote_source_after_local_validation_before_ci(self):
        candidate_path = self.root / 'candidate.json'; candidate_path.write_bytes(p.encoded(self.candidate))
        output = self.root / 'plan.json'
        args = ['publication-plan', '--candidate', str(candidate_path), '--product-root', str(self.root / 'product'),
                '--release-receipt', str(self.root / 'release-receipt.json'), '--remote-state', str(self.root / 'remote.json'),
                '--notes-file', str(self.root / 'notes.md'), '--output', str(output)]
        # Opening the code guard here is a process-local mock for a negative CLI
        # fixture. No file/config flag or subprocess is enabled in production.
        with patch.object(p, 'OPERATIONS_ENABLED', True), patch.dict(os.environ, self.context, clear=True), \
             patch.object(p.m, 'load_profile', return_value={}), patch.object(p.m, 'validate_profile'), \
             patch.object(p, 'verify_source') as local, patch.object(p.m, 'check_candidate', side_effect=ValueError('canonical branch moved')) as remote, \
             patch.object(p, 'authenticate_ci') as ci, patch.object(subprocess, 'Popen') as processes:
            with self.assertRaisesRegex(ValueError, 'canonical branch moved'): p.main(args)
            local.assert_called_once(); remote.assert_called_once(); ci.assert_not_called(); processes.assert_not_called()
        self.assertFalse(output.exists())

    def test_fresh_attempt_requires_single_current_attempt_and_rejects_reruns(self):
        current = copy.deepcopy(self.candidate); current['run']['attempt'] = 1
        row = {'identity': p.retry_identity(current), 'runId': '101', 'runAttempt': 1,
               'runNumber': 9, 'status': 'in_progress', 'conclusion': None}
        p.require_fresh_attempt(current, {'complete': True, 'runs': [row]})
        for candidate, rows in ((self.candidate, [row]), (current, []),
                                (current, [row, dict(row, runId='100', status='completed', conclusion='failure')])):
            with self.assertRaisesRegex(ValueError, 'unchanged retry'):
                p.require_fresh_attempt(candidate, {'complete': True, 'runs': rows})

    def test_attempt_inventory_authenticates_every_attempt_against_release_workflow(self):
        candidate = copy.deepcopy(self.candidate); candidate['run']['attempt'] = 1
        endpoint = f'repos/{p.m.REPOSITORY}/actions/workflows/release.yml/runs?head_sha=' + 'a' * 40
        run = {'id': 101, 'run_attempt': 1, 'head_sha': 'a' * 40, 'event': 'workflow_dispatch',
               'head_branch': 'main', 'repository': {'full_name': p.m.REPOSITORY},
               'path': '.github/workflows/release.yml', 'display_title': p.run_title(candidate),
               'run_number': 9, 'status': 'in_progress', 'conclusion': None}
        responses = {endpoint + '&per_page=100&page=1': {'total_count': 1, 'workflow_runs': [run]},
                     f'repos/{p.m.REPOSITORY}/actions/runs/101/attempts/1': run}
        with patch.object(p.r, 'gh_api', side_effect=responses.__getitem__):
            inventory = p.authenticated_attempt_inventory(candidate)
        p.require_fresh_attempt(candidate, inventory)
        responses = copy.deepcopy(responses)
        responses[f'repos/{p.m.REPOSITORY}/actions/runs/101/attempts/1']['path'] = '.github/workflows/unrelated.yml'
        with patch.object(p.r, 'gh_api', side_effect=responses.__getitem__), self.assertRaises(ValueError):
            p.authenticated_attempt_inventory(candidate)


if __name__ == '__main__':
    unittest.main()
