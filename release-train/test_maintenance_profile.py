"""Synthetic Git and mocked API tests only; never dispatch or compile products."""
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import maintenance_profile as m


def disabled():
    return {'schemaVersion': 1, 'profile': m.PROFILE, 'enabled': False,
            'repository': m.REPOSITORY, 'controlBranch': 'main', 'sourceBranch': m.SOURCE_BRANCH,
            'version': m.VERSION, 'base': {'tag': '1.4.2', 'sha': m.BASE_SHA, 'tree': m.BASE_TREE},
            'reviewedBackport': {'sha': m.BACKPORT_SHA, 'tree': m.BACKPORT_TREE},
            'registration': None, 'controlRegistration': None}


def git(root, *args):
    return subprocess.check_output(['git', '-C', str(root), *args], text=True,
                                   stderr=subprocess.PIPE).rstrip('\n')


def put(root, path, text):
    p = root / path
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)


def commit(root, title):
    git(root, 'add', '.')
    git(root, 'commit', '-qm', title)
    return git(root, 'rev-parse', 'HEAD')


def init(root):
    root.mkdir()
    git(root, 'init', '-q', '-b', 'main')
    git(root, 'config', 'user.name', 'Synthetic Test')
    git(root, 'config', 'user.email', 'synthetic@example.invalid')
    git(root, 'config', 'commit.gpgsign', 'false')
    git(root, 'config', 'core.hooksPath', '/dev/null')
    git(root, 'remote', 'add', 'origin', 'https://github.com/jsflax/LatticeCore.git')


class SchemaTests(unittest.TestCase):
    def test_inactive_round_trip(self):
        self.assertEqual(m.validate_profile(disabled()), disabled())

    def test_disabled_rejects_before_git_or_network(self):
        with mock.patch.object(m.rt, 'git') as g, mock.patch.object(m.rt, 'gh_api') as api:
            with self.assertRaisesRegex(ValueError, 'disabled'):
                m.check_candidate('/missing', '/missing', disabled(), '1.4.3', 'x', 'y', context={})
            g.assert_not_called()
            api.assert_not_called()

    def test_unknown_missing_and_wrong_typed_fields_fail(self):
        variants = []
        p = disabled(); p['extra'] = True; variants.append(p)
        p = disabled(); del p['registration']; variants.append(p)
        p = disabled(); p['enabled'] = 1; variants.append(p)
        p = disabled(); p['schemaVersion'] = True; variants.append(p)
        for value in variants:
            with self.subTest(value=value), self.assertRaises(ValueError):
                m.validate_profile(value)

    def test_exact_allowlist_rejects_widening(self):
        for key, value in [('repository', 'other/LatticeCore'), ('controlBranch', 'other'),
                           ('sourceBranch', 'maintenance/1.x'), ('version', '1.4.4'),
                           ('profile', 'maintenance-1')]:
            p = disabled(); p[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                m.validate_profile(p)

    def test_enable_requires_both_registered_reviews(self):
        p = disabled(); p['enabled'] = True
        with self.assertRaisesRegex(ValueError, 'registration and review'):
            m.validate_profile(p)

    def test_base_and_backport_are_immutable(self):
        for key in ['base', 'reviewedBackport']:
            p = disabled(); p[key]['sha'] = 'a' * 40
            with self.subTest(key=key), self.assertRaises(ValueError):
                m.validate_profile(p)

    def test_duplicate_json_keys_fail(self):
        with self.assertRaisesRegex(ValueError, 'duplicate JSON'):
            json.loads('{"enabled": false, "enabled": true}', object_pairs_hook=m._unique_object)

    def test_exact_absence_requires_genuine_404_json(self):
        response = subprocess.CompletedProcess([], 1, 'HTTP/2.0 404 Not Found\r\nContent-Type: application/json\r\n\r\n{"message":"Not Found"}\n', '')
        with mock.patch.object(m.subprocess, 'run', return_value=response) as run:
            m._exact_absence('repos/jsflax/LatticeCore/releases/tags/1.4.3')
            self.assertEqual(run.call_args.kwargs['timeout'], 30)
        variants = [
            subprocess.CompletedProcess([], 0, 'HTTP/2.0 200 OK\n\n{"tag_name":"1.4.3"}', ''),
            subprocess.CompletedProcess([], 1, 'HTTP/2.0 403 Forbidden\n\n{"message":"Not Found"}', ''),
            subprocess.CompletedProcess([], 1, 'HTTP/2.0 404 Not Found\n\nnot-json', ''),
            subprocess.CompletedProcess([], 1, 'HTTP/2.0 404 Not Found\n\n{"message":"wrong"}', ''),
            subprocess.CompletedProcess([], 1, 'HTTP/2.0 404 Not Found\n\n{"message":"wrong","message":"Not Found"}', ''),
            subprocess.CompletedProcess([], 1, '{"message":"Not Found"}', ''),
            subprocess.CompletedProcess([], 0, 'HTTP/2.0 404 Not Found\n\n{"message":"Not Found"}', ''),
        ]
        for response in variants:
            with self.subTest(response=response.stdout), mock.patch.object(m.subprocess, 'run', return_value=response):
                with self.assertRaises(ValueError): m._exact_absence('exact')
        for error in [OSError('cannot launch'), subprocess.TimeoutExpired('gh', 30)]:
            with mock.patch.object(m.subprocess, 'run', side_effect=error):
                with self.assertRaisesRegex(ValueError, 'did not complete'): m._exact_absence('exact')


class CandidateTests(unittest.TestCase):
    def setUp(self):
        # The caller owns scratch placement through TMPDIR on local and CI hosts.
        self.temp = tempfile.TemporaryDirectory(prefix='maintenance-synthetic-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.control, self.product = self.root / 'control', self.root / 'product'
        init(self.control); init(self.product)
        put(self.product, 'CHANGELOG.md', '## [1.4.2]\nPrior release.\n')
        put(self.product, '.github/workflows/release.yml', 'legacy tag workflow\n')
        put(self.product, 'CMakeLists.txt', 'existing target\n')
        put(self.product, 'Sources/LatticeCore/src/cross_process_notifier_linux.cpp', 'select\n')
        self.base = commit(self.product, 'base')
        self.base_tree = git(self.product, 'rev-parse', 'HEAD^{tree}')
        git(self.product, 'switch', '-qc', m.SOURCE_BRANCH)
        put(self.product, 'CMakeLists.txt', 'target with notifier tests\n')
        put(self.product, 'Sources/LatticeCore/src/cross_process_notifier_linux.cpp', 'poll\n')
        put(self.product, 'Tests/LatticeCoreTests/LinuxNotifierTests.cpp', 'four tests\n')
        self.backport = commit(self.product, 'backport')
        self.backport_tree = git(self.product, 'rev-parse', 'HEAD^{tree}')
        put(self.product, 'CHANGELOG.md', '## [1.4.3]\nNotifier fix.\n\n## [1.4.2]\nPrior release.\n')
        (self.product / '.github/workflows/release.yml').unlink()
        self.source = commit(self.product, 'release metadata')
        for name, value in [('BASE_SHA', self.base), ('BASE_TREE', self.base_tree),
                            ('BACKPORT_SHA', self.backport), ('BACKPORT_TREE', self.backport_tree)]:
            patch = mock.patch.object(m, name, value); patch.start(); self.addCleanup(patch.stop)
        self.profile = disabled()
        self.profile['enabled'] = True
        self.profile['registration'] = {
            'sourceSha': self.source, 'sourceTree': git(self.product, 'rev-parse', 'HEAD^{tree}'),
            'owner': 'jsflax', 'review': 'synthetic source review',
            'metadataDiff': m._manifest(self.product, self.backport, self.source),
            'sourceDiff': m._manifest(self.product, self.base, self.source),
        }
        for path in m.REQUIRED_CONTROL_FILES:
            put(self.control, path, 'synthetic committed control: ' + path + '\n')
        self.profile['controlRegistration'] = {
            'owner': 'jsflax', 'review': 'synthetic control review',
            'files': {p: m.rt.digest(self.control / p) for p in m.REQUIRED_CONTROL_FILES},
        }
        self.save_control()
        self.tags = [{'name': '1.4.2', 'commit': {'sha': self.base}},
                     {'name': '2.0.7', 'commit': {'sha': 'a' * 40}}]
        self.releases = []
        self.api_override = {}
        patch = mock.patch.object(m.rt, 'gh_api', side_effect=self.api)
        self.api_mock = patch.start(); self.addCleanup(patch.stop)
        patch = mock.patch.object(m, '_exact_absence', return_value=None)
        self.absence_mock = patch.start(); self.addCleanup(patch.stop)

    def save_control(self):
        put(self.control, m.PROFILE_PATH, json.dumps(self.profile, indent=2) + '\n')
        self.control_sha = commit(self.control, 'control registration')

    def api(self, endpoint):
        if endpoint in self.api_override:
            value = self.api_override[endpoint]
            if isinstance(value, Exception): raise value
            return value
        prefix = f'repos/{m.REPOSITORY}/'
        if endpoint == prefix + 'git/ref/heads/main':
            return {'ref': 'refs/heads/main', 'object': {'type': 'commit', 'sha': self.control_sha}}
        if endpoint == prefix + 'git/ref/heads/' + m.quote(m.SOURCE_BRANCH, safe=''):
            return {'ref': 'refs/heads/' + m.SOURCE_BRANCH, 'object': {'type': 'commit', 'sha': self.source}}
        if endpoint == prefix + 'git/ref/tags/1.4.2':
            return {'ref': 'refs/tags/1.4.2', 'object': {'type': 'commit', 'sha': self.base}}
        for resource, values in [('tags', self.tags), ('releases', self.releases)]:
            start = prefix + resource + '?per_page=100&page='
            if endpoint.startswith(start):
                page = int(endpoint[len(start):]); return values[(page - 1) * 100:page * 100]
        raise AssertionError('unmocked API: ' + endpoint)

    def check(self, **kwargs):
        args = {'context': {}}; args.update(kwargs)
        return m.check_candidate(self.control, self.product, self.profile, m.VERSION,
                                 self.source, self.control_sha, **args)

    def test_registered_candidate_admits_source_only(self):
        result = self.check()
        self.assertTrue(result['sourceAdmission'])
        self.assertFalse(result['dispatchAdmitted'])
        self.assertFalse(result['publicationAdmitted'])
        self.assertEqual(result['source']['sha'], self.source)
        self.assertEqual(result['profileSHA256'], m.rt.digest(self.control / m.PROFILE_PATH))

    def test_missing_owner_or_review_fails_before_api(self):
        for obj, key, bad in [('registration', 'owner', 'other'), ('registration', 'review', ''),
                              ('controlRegistration', 'owner', ''), ('controlRegistration', 'review', ' ' )]:
            p = copy.deepcopy(self.profile); p[obj][key] = bad
            with self.subTest(obj=obj, key=key), self.assertRaises(ValueError):
                m.validate_profile(p, require_enabled=True)
        self.api_mock.assert_not_called()

    def test_unknown_nested_registration_keys_fail(self):
        for key in ['registration', 'controlRegistration', 'base', 'reviewedBackport']:
            p = copy.deepcopy(self.profile); p[key]['extra'] = True
            with self.subTest(key=key), self.assertRaises(ValueError): m.validate_profile(p)

    def test_diff_manifest_must_be_exact_and_ordered(self):
        for mutate in [lambda x: x.reverse(), lambda x: x.append(x[0]),
                       lambda x: x[0].update(newMode='100755'),
                       lambda x: x[0].update(path='../escape'),
                       lambda x: x[0].update(extra=True)]:
            p = copy.deepcopy(self.profile); mutate(p['registration']['sourceDiff'])
            with self.assertRaises(ValueError): m.validate_profile(p)

    def test_source_diff_blob_mismatch_fails(self):
        self.profile['registration']['sourceDiff'][0]['oldBlob'] = 'd' * 40
        self.save_control()
        with self.assertRaisesRegex(ValueError, 'full source diff'): self.check()

    def test_wrong_version_and_source_sha_fail(self):
        for version in ['1.5.0', '2.0.8', '1.4.4', '1.4.3-rc.1', '1.4.3+build']:
            with self.subTest(version=version), self.assertRaises(ValueError):
                m.check_candidate(self.control, self.product, self.profile, version,
                                  self.source, self.control_sha, context={})
        with self.assertRaisesRegex(ValueError, 'unregistered product'):
            m.check_candidate(self.control, self.product, self.profile, m.VERSION,
                              'a' * 40, self.control_sha, context={})

    def test_dirty_and_hidden_index_product_fail(self):
        file = self.product / 'CMakeLists.txt'; file.write_text('dirty\n')
        with self.assertRaisesRegex(ValueError, 'working tree'): self.check()
        git(self.product, 'update-index', '--assume-unchanged', 'CMakeLists.txt')
        with self.assertRaisesRegex(ValueError, 'hidden index flags'): self.check()

    def test_source_and_changelog_hidden_by_either_index_flag_fail(self):
        for path in ['Sources/LatticeCore/src/cross_process_notifier_linux.cpp', 'CHANGELOG.md']:
            for flag in ['assume-unchanged', 'skip-worktree']:
                git(self.product, 'update-index', '--' + flag, path)
                (self.product / path).write_text('altered behind hidden flag\n')
                with self.subTest(path=path, flag=flag), self.assertRaisesRegex(ValueError, 'hidden index flags'):
                    self.check()
                git(self.product, 'update-index', '--no-' + flag, path)
                git(self.product, 'restore', '--', path)

    def test_sparse_checkout_config_fails_for_both_roots(self):
        for root in [self.control, self.product]:
            git(root, 'config', 'core.sparseCheckout', 'true')
            with self.subTest(root=root), self.assertRaisesRegex(ValueError, 'sparse checkout'): self.check()
            git(root, 'config', 'core.sparseCheckout', 'false')

    def test_distinct_checkouts_required(self):
        with self.assertRaisesRegex(ValueError, 'distinct'):
            m.check_candidate(self.control, self.control, self.profile, m.VERSION,
                              self.source, self.control_sha, context={})

    def test_wrong_origin_fails(self):
        git(self.product, 'remote', 'set-url', 'origin', 'https://github.com/other/LatticeCore.git')
        with self.assertRaisesRegex(ValueError, 'canonical origin'): self.check()

    def test_registered_control_inventory_and_bytes_fail_closed(self):
        p = copy.deepcopy(self.profile); del p['controlRegistration']['files']['release-train/release_train.py']
        with self.assertRaisesRegex(ValueError, 'control files'): m.validate_profile(p)
        put(self.control, 'release-train/core_release.py', 'unexpected mutation\n')
        self.control_sha = commit(self.control, 'unexpected control change')
        with self.assertRaisesRegex(ValueError, 'control file changed'): self.check()

    def test_additional_product_child_is_not_registered_metadata_child(self):
        put(self.product, 'unexpected.txt', 'extra\n')
        self.source = commit(self.product, 'unreviewed extra child')
        self.profile['registration']['sourceSha'] = self.source
        self.profile['registration']['sourceTree'] = git(self.product, 'rev-parse', 'HEAD^{tree}')
        self.save_control()
        with self.assertRaisesRegex(ValueError, 'one metadata-only child'): self.check()

    def test_annotated_base_tag_is_peeled(self):
        oid = 'b' * 40
        self.api_override[f'repos/{m.REPOSITORY}/git/ref/tags/1.4.2'] = {
            'ref': 'refs/tags/1.4.2', 'object': {'type': 'tag', 'sha': oid}}
        self.api_override[f'repos/{m.REPOSITORY}/git/tags/{oid}'] = {
            'sha': oid, 'object': {'type': 'commit', 'sha': self.base}}
        self.assertTrue(self.check()['sourceAdmission'])

    def test_moved_and_cyclic_base_tag_fail(self):
        endpoint = f'repos/{m.REPOSITORY}/git/ref/tags/1.4.2'
        self.api_override[endpoint] = {'ref': 'refs/tags/1.4.2', 'object': {'type': 'commit', 'sha': 'a' * 40}}
        with self.assertRaisesRegex(ValueError, 'base tag moved'): self.check()
        oid = 'b' * 40
        self.api_override[endpoint]['object'] = {'type': 'tag', 'sha': oid}
        self.api_override[f'repos/{m.REPOSITORY}/git/tags/{oid}'] = {'sha': oid, 'object': {'type': 'tag', 'sha': oid}}
        with self.assertRaisesRegex(ValueError, 'cyclic'): self.check()

    def test_remote_control_or_product_branch_movement_fails(self):
        for branch in ['main', m.SOURCE_BRANCH]:
            endpoint = f'repos/{m.REPOSITORY}/git/ref/heads/{m.quote(branch, safe="")}'
            self.api_override[endpoint] = {'ref': 'refs/heads/' + branch,
                                           'object': {'type': 'commit', 'sha': 'a' * 40}}
            with self.subTest(branch=branch), self.assertRaisesRegex(ValueError, 'canonical branch changed'):
                self.check()
            self.api_override.clear()

    def test_line_semver_precedence_handles_prerelease_and_build(self):
        for name, allowed in [('1.4.3-rc.1', True), ('1.4.2+old', True), ('2.0.7', True),
                              ('1.5.0-rc.1', True), ('1.4.4-rc.1', False), ('1.4.3+build', False),
                              ('1.4.3', False), ('1.4.99', False)]:
            self.tags = [{'name': '1.4.2', 'commit': {'sha': self.base}},
                         {'name': name, 'commit': {'sha': 'a' * 40}}]
            with self.subTest(name=name):
                if allowed: self.assertTrue(self.check()['sourceAdmission'])
                else:
                    with self.assertRaises(ValueError): self.check()

    def test_paginated_tags_find_later_conflicting_version(self):
        self.tags += [{'name': 'evidence-' + str(i), 'commit': {'sha': 'c' * 40}} for i in range(99)]
        self.tags.append({'name': '1.4.4-rc.1', 'commit': {'sha': 'a' * 40}})
        with self.assertRaisesRegex(ValueError, 'remote 1.4 tag'): self.check()

    def test_paginated_draft_release_blocks_exact_version(self):
        self.releases = [{'id': i + 1, 'tag_name': 'historical-' + str(i), 'draft': False} for i in range(100)]
        self.releases.append({'id': 101, 'tag_name': '1.4.3', 'draft': True})
        with self.assertRaisesRegex(ValueError, 'release/draft'): self.check()

    def test_api_errors_malformed_and_repeated_pages_fail_closed(self):
        endpoint = f'repos/{m.REPOSITORY}/tags?per_page=100&page=1'
        for response in [RuntimeError('network unavailable'), {'message': 'rate limit'},
                         [{'name': '1.4.2'}], [{'name': '1.4.2', 'commit': {'sha': 'bad'}}]]:
            self.api_override[endpoint] = response
            with self.subTest(response=response), self.assertRaises((ValueError, RuntimeError)): self.check()
        repeated = [{'name': str(i), 'commit': {'sha': 'a' * 40}} for i in range(100)]
        self.api_override[endpoint] = repeated
        self.api_override[f'repos/{m.REPOSITORY}/tags?per_page=100&page=2'] = repeated
        with self.assertRaisesRegex(ValueError, 'repeated'): self.check()

    def test_missing_base_from_tag_inventory_fails(self):
        self.tags = []
        with self.assertRaisesRegex(ValueError, 'include base'): self.check()

    def test_lists_missing_exact_existing_resources_do_not_admit(self):
        self.assertTrue(self.check()['sourceAdmission'])
        self.absence_mock.assert_has_calls([
            mock.call(f'repos/{m.REPOSITORY}/git/ref/tags/1.4.3'),
            mock.call(f'repos/{m.REPOSITORY}/releases/tags/1.4.3'),
        ])
        for fail_index in [0, 1]:
            self.absence_mock.side_effect = [ValueError('exists'), None] if fail_index == 0 else [None, ValueError('exists')]
            with self.subTest(fail_index=fail_index), self.assertRaisesRegex(ValueError, 'exists'):
                self.check()

    def test_github_context_requires_control_dispatch_identity(self):
        env = {'GITHUB_ACTIONS': 'true', 'GITHUB_EVENT_NAME': 'workflow_dispatch',
               'GITHUB_REF': 'refs/heads/main', 'GITHUB_REPOSITORY': m.REPOSITORY,
               'GITHUB_SHA': self.control_sha}
        self.assertTrue(self.check(context=env)['sourceAdmission'])
        for key in env:
            bad = dict(env); bad[key] = 'wrong'
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'GitHub context'): self.check(context=bad)
        with self.assertRaisesRegex(ValueError, 'partial GitHub'):
            self.check(context={'GITHUB_SHA': self.control_sha})
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertTrue(self.check(context=None)['sourceAdmission'])


if __name__ == '__main__':
    unittest.main()
