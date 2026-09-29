"""Core wrapper admission tests; mocks and local metadata only, never releases."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


TOOLS = Path(__file__).resolve().parent
with patch.object(sys, 'path', [str(TOOLS), *sys.path]):
    spec = importlib.util.spec_from_file_location('core_release', TOOLS / 'core_release.py')
    c = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(c)


class CoreReleaseTests(unittest.TestCase):
    SOURCE = 'a' * 40
    CONTROL = 'b' * 40
    GENERIC_COMMANDS = (
        'check', 'candidate', 'dispatch', 'receipt', 'workflow-receipt',
        'verify-package', 'notes', 'suggest',
    )

    def source_args(self, product, *extra):
        return [
            '--profile', 'maintenance-1.4', 'maintenance-source-check',
            '--version', '1.4.3', '--expected-sha', self.SOURCE,
            '--control-sha', self.CONTROL, '--product-root', str(product),
            *extra,
        ]

    @contextlib.contextmanager
    def no_processes(self):
        with patch.object(subprocess, 'Popen', side_effect=AssertionError('unexpected subprocess')) as processes:
            yield processes
        processes.assert_not_called()

    def test_default_delegates_every_generic_command_without_rewriting_arguments(self):
        for command in self.GENERIC_COMMANDS:
            with self.subTest(command=command):
                args = [command, '--version', '2.0.8', '--output', 'receipt with spaces.json']
                before = sys.argv
                seen = []
                marker = object()
                def legacy_main():
                    seen.append(list(sys.argv))
                    return marker
                with patch.object(c.r, 'main', side_effect=legacy_main), patch.object(c.m, 'load_profile') as load:
                    self.assertIs(c.main(args), marker)
                self.assertEqual(seen, [[before[0], *args]])
                self.assertIs(sys.argv, before)
                load.assert_not_called()

    def test_explicit_main_profile_forms_and_positions_only_strip_profile(self):
        expected = ['check', '--version', '2.0.8', '--expected-sha', self.SOURCE]
        forms = [
            ['--profile', 'main', *expected],
            [*expected, '--profile=main'],
            ['check', '--profile', 'main', *expected[1:]],
        ]
        for args in forms:
            with self.subTest(args=args):
                seen = []
                with patch.object(c.r, 'main', side_effect=lambda: seen.append(sys.argv[1:])):
                    c.main(args)
                self.assertEqual(seen, [expected])

    def test_default_restores_argv_and_preserves_legacy_exceptions(self):
        for error in (ValueError('legacy validation failed'), SystemExit(2)):
            with self.subTest(error=type(error).__name__):
                before = sys.argv
                with patch.object(c.r, 'main', side_effect=error):
                    with self.assertRaises(type(error)) as raised:
                        c.main(['--profile=main', 'check'])
                self.assertIs(raised.exception, error)
                self.assertIs(sys.argv, before)

    def test_none_argv_uses_process_arguments_and_restores_original_list(self):
        args = ['core_release.py', 'check', '--profile=main', '--version', '2.0.8']
        seen = []
        with patch.object(sys, 'argv', args), patch.object(c.r, 'main', side_effect=lambda: seen.append(list(sys.argv))):
            c.main()
            self.assertIs(sys.argv, args)
        self.assertEqual(seen, [['core_release.py', 'check', '--version', '2.0.8']])

    def test_double_dash_preserves_later_profile_text_for_legacy_parser(self):
        args = ['check', '--', '--profile', 'maintenance-1.4']
        seen = []
        with patch.object(c.r, 'main', side_effect=lambda: seen.append(sys.argv[1:])):
            c.main(args)
        self.assertEqual(seen, [args])

    def test_environment_does_not_choose_a_profile(self):
        with patch.dict(os.environ, {'CORE_RELEASE_PROFILE': 'maintenance-1.4', 'RELEASE_PROFILE': 'maintenance-1.4'}):
            with patch.object(c.r, 'main') as legacy, patch.object(c.m, 'load_profile') as load:
                c.main(['check'])
        legacy.assert_called_once_with()
        load.assert_not_called()

    def test_bad_or_duplicate_profile_fails_before_any_release_work(self):
        bad = [
            ['--profile', 'unknown', 'check'],
            ['--profile=unknown', 'check'],
            ['check', '--profile'],
            ['check', '--profile='],
            ['--profile', 'main', '--profile', 'main', 'check'],
            ['--profile=main', 'check', '--profile=maintenance-1.4'],
            ['--profile', '--version', 'check'],
        ]
        for args in bad:
            with self.subTest(args=args), self.no_processes():
                with patch.object(c.r, 'main') as legacy, patch.object(c.m, 'load_profile') as load, patch.object(c.m, 'check_candidate') as check:
                    with self.assertRaises(ValueError):
                        c.main(args)
                legacy.assert_not_called()
                load.assert_not_called()
                check.assert_not_called()

    def test_all_generic_maintenance_commands_stay_disabled_even_with_enabled_config(self):
        for command in self.GENERIC_COMMANDS:
            for profile_args in (['--profile', 'maintenance-1.4'], ['--profile=maintenance-1.4']):
                with self.subTest(command=command, profile=profile_args), self.no_processes():
                    with patch.object(c.r, 'main') as legacy, patch.object(c.m, 'load_profile', return_value={'enabled': True}) as load, patch.object(c.m, 'check_candidate') as check, patch.object(c.r, 'write') as write:
                        with self.assertRaises(ValueError):
                            c.main([command, *profile_args, '--version', '1.4.3'])
                    legacy.assert_not_called()
                    load.assert_not_called()
                    check.assert_not_called()
                    write.assert_not_called()

    def test_shipped_profile_disables_source_check_before_git_network_or_output(self):
        # Exercise the real shipped profile loader and validator, not a mocked
        # enabled/disabled decision. No product repository is needed to reject it.
        with tempfile.TemporaryDirectory() as directory, self.no_processes():
            product = Path(directory) / 'not-a-repository'
            output = Path(directory) / 'candidate.json'
            with patch.object(c.r, 'main') as legacy, patch.object(c.m, 'check_candidate') as check, patch.object(c.r, 'write') as write:
                with self.assertRaises(ValueError):
                    c.main(self.source_args(product, '--output', str(output)))
            legacy.assert_not_called()
            check.assert_not_called()
            write.assert_not_called()
            self.assertFalse(output.exists())

    def test_source_check_requires_each_explicit_source_argument(self):
        args = self.source_args('/nonexistent-product')
        for flag in ('--version', '--expected-sha', '--control-sha', '--product-root'):
            with self.subTest(flag=flag), self.no_processes():
                malformed = list(args)
                index = malformed.index(flag)
                del malformed[index:index + 2]
                with patch.object(c.m, 'load_profile') as load, patch.object(c.m, 'check_candidate') as check, contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit) as raised:
                        c.main(malformed)
                self.assertEqual(raised.exception.code, 2)
                load.assert_not_called()
                check.assert_not_called()

    def test_source_check_requires_explicit_maintenance_profile(self):
        args = self.source_args('/nonexistent-product')[2:]
        with self.no_processes(), patch.object(c.m, 'load_profile') as load, patch.object(c.m, 'check_candidate') as check, contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                c.main(args)
        self.assertEqual(raised.exception.code, 2)
        load.assert_not_called()
        check.assert_not_called()

    def test_source_check_rejects_generic_packaged_flag(self):
        with self.no_processes(), patch.object(c.m, 'check_candidate') as check, contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                c.main(self.source_args('/nonexistent-product', '--packaged'))
        self.assertEqual(raised.exception.code, 2)
        check.assert_not_called()

    def test_source_check_routes_fixed_control_and_explicit_product_identity(self):
        profile = {'enabled': True, 'fixture': 'source-only'}
        result = {'state': 'publication-eligible', 'source': {'sha': self.SOURCE}, 'control': {'sha': self.CONTROL}, 'dispatchAdmitted': True, 'publicationAdmitted': True}
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as directory, self.no_processes():
            product = Path(directory) / 'product' / '..' / 'product'
            environment = {'WRAPPER_TEST_CONTEXT': 'captured'}
            with patch.dict(os.environ, environment, clear=True), patch.object(c.m, 'load_profile', return_value=profile) as load, patch.object(c.m, 'validate_profile') as validate, patch.object(c.m, 'check_candidate', return_value=result) as check, patch.object(c.r, 'main') as legacy, patch.object(c.r, 'write') as write, contextlib.redirect_stdout(output):
                c.main(self.source_args(product))
            load.assert_called_once_with(TOOLS.parent)
            validate.assert_called_once()
            self.assertIs(validate.call_args.args[0], profile)
            self.assertTrue(validate.call_args.kwargs['require_enabled'])
            check.assert_called_once_with(TOOLS.parent, product.resolve(), profile, '1.4.3', self.SOURCE, self.CONTROL, context=environment)
            legacy.assert_not_called()
            write.assert_not_called()
        emitted = json.loads(output.getvalue())
        self.assertEqual(emitted['state'], 'maintenance-source-reviewed-not-release-admitted')
        self.assertEqual(emitted['source']['sha'], self.SOURCE)
        self.assertEqual(emitted['control']['sha'], self.CONTROL)
        self.assertIs(emitted['dispatchAdmitted'], False)
        self.assertIs(emitted['publicationAdmitted'], False)

    def test_source_check_writes_only_source_review_result_after_success(self):
        profile = {'enabled': True}
        result = {'state': 'validated-and-packaged', 'source': {'sha': self.SOURCE}}
        with tempfile.TemporaryDirectory() as directory, self.no_processes():
            target = Path(directory) / 'source-review.json'
            with patch.object(c.m, 'load_profile', return_value=profile), patch.object(c.m, 'validate_profile'), patch.object(c.m, 'check_candidate', return_value=result), contextlib.redirect_stdout(io.StringIO()):
                c.main(self.source_args(Path(directory) / 'product', '--output', str(target)))
            emitted = json.loads(target.read_text())
        self.assertEqual(emitted['state'], 'maintenance-source-reviewed-not-release-admitted')
        self.assertEqual(emitted['source']['sha'], self.SOURCE)
        self.assertNotIn('workflowRun', emitted)

    def test_source_review_output_cannot_modify_either_checkout(self):
        with tempfile.TemporaryDirectory() as directory, self.no_processes():
            product = Path(directory) / 'product'
            product.mkdir()
            alias = Path(directory) / 'product-alias'
            alias.symlink_to(product, target_is_directory=True)
            forbidden = [
                TOOLS.parent / 'source-review.json', TOOLS.parent,
                product / 'source-review.json', product,
                alias / 'source-review.json',
            ]
            for output in forbidden:
                with self.subTest(output=str(output)), patch.object(c.m, 'load_profile', return_value={'enabled': True}), patch.object(c.m, 'validate_profile'), patch.object(c.m, 'check_candidate', return_value={}), patch.object(c.r, 'write') as write, contextlib.redirect_stdout(io.StringIO()):
                    with self.assertRaisesRegex(ValueError, 'outside both source checkouts'):
                        c.main(self.source_args(product, '--output', str(output)))
                    write.assert_not_called()

    def test_output_write_failure_does_not_print_success(self):
        stdout = io.StringIO()
        with tempfile.TemporaryDirectory() as directory, self.no_processes():
            with patch.object(c.m, 'load_profile', return_value={'enabled': True}), patch.object(c.m, 'validate_profile'), patch.object(c.m, 'check_candidate', return_value={}), patch.object(c.r, 'write', side_effect=OSError('read-only destination')), contextlib.redirect_stdout(stdout):
                with self.assertRaisesRegex(OSError, 'read-only destination'):
                    c.main(self.source_args(Path(directory) / 'product', '--output', str(Path(directory) / 'review.json')))
        self.assertEqual(stdout.getvalue(), '')

    def test_source_check_validation_failure_precedes_candidate_and_output(self):
        with tempfile.TemporaryDirectory() as directory, self.no_processes():
            target = Path(directory) / 'source-review.json'
            with patch.object(c.m, 'load_profile', return_value={'enabled': False}), patch.object(c.m, 'validate_profile', side_effect=ValueError('profile disabled')), patch.object(c.m, 'check_candidate') as check, patch.object(c.r, 'write') as write:
                with self.assertRaisesRegex(ValueError, 'profile disabled'):
                    c.main(self.source_args(directory, '--output', str(target)))
            check.assert_not_called()
            write.assert_not_called()
            self.assertFalse(target.exists())

    def test_source_check_failure_never_writes_or_prints_success(self):
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as directory, self.no_processes():
            target = Path(directory) / 'source-review.json'
            with patch.object(c.m, 'load_profile', return_value={'enabled': True}), patch.object(c.m, 'validate_profile'), patch.object(c.m, 'check_candidate', side_effect=ValueError('wrong source tree')), patch.object(c.r, 'write') as write, contextlib.redirect_stdout(output):
                with self.assertRaisesRegex(ValueError, 'wrong source tree'):
                    c.main(self.source_args(directory, '--output', str(target)))
            write.assert_not_called()
            self.assertFalse(target.exists())
        self.assertEqual(output.getvalue(), '')

    def test_release_workflow_routes_each_release_guard_through_explicit_profile(self):
        workflow = (TOOLS.parent / '.github/workflows/release.yml').read_text()
        calls = [shlex.split(line.strip()) for line in workflow.splitlines()
                 if line.strip().startswith('python3 release-train/')]
        self.assertEqual([call[2] for call in calls], ['check', 'candidate', 'workflow-receipt'])
        for call in calls:
            self.assertEqual(call[1], 'release-train/core_release.py')
            self.assertEqual(call[call.index('--profile') + 1], '$RELEASE_PROFILE')
        self.assertIn("RELEASE_PROFILE: ${{ inputs.profile || 'main' }}", workflow)
        # Dormant source preparation follows the same hard-blocking wrapper
        # preflight; it cannot bypass that guard to reach native jobs.
        self.assertLess(workflow.index('core_release.py check --profile'),
                        workflow.index(' maintenance-source-check '))
        for job in ('test-macos', 'test-linux', 'test-capi'):
            self.assertRegex(workflow, rf'(?m)^  {re.escape(job)}:\n    needs: preflight\n')
        self.assertRegex(workflow, r'(?m)^  release:\n    needs: \[preflight, test-macos, test-linux, test-capi\]\n')
        self.assertNotRegex(workflow, r'(?m)^\s*continue-on-error:\s*true\s*$')


if __name__ == '__main__':
    unittest.main()
