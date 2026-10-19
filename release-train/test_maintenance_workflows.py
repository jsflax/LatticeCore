"""Dormant workflow structure and shell fixtures; never invoke native toolchains."""
import ast
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import sys
import unittest


ROOT = Path(__file__).resolve().parent.parent
NATIVE = ('macos.yml', 'linux.yml', 'capi.yml')
UPLOAD = 'ea165f8d65b6e75b540449e92b4886f43607fa02'
DOWNLOAD = 'd3f86a106a0bac45b974a628896c90dbdf5c8093'


def workflow(name):
    return (ROOT / '.github/workflows' / name).read_text()


def steps(text):
    """Extract this repository's fixed-indentation named step blocks."""
    matches = list(re.finditer(r'^      - name: (.+)$', text, re.M))
    result = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        block = text[match.end() + 1:end]
        # A following reusable-job mapping is outside the step.
        boundary = re.search(r'^  [A-Za-z][A-Za-z0-9_-]*:\s*$', block, re.M)
        if boundary:
            block = block[:boundary.start()]
        result.append((match[1], block))
    return result


def script(block):
    marker = '        run: |\n'
    if marker not in block:
        scalar = re.search(r'^        run: (.+)$', block, re.M)
        return scalar[1] + '\n' if scalar else None
    result = []
    for line in block.split(marker, 1)[1].splitlines():
        if line and not line.startswith('          '):
            break
        result.append(line[10:] if line else '')
    return '\n'.join(result).rstrip() + '\n'


def named(text, title):
    return next(block for name, block in steps(text) if name == title)


class MaintenanceWorkflowTests(unittest.TestCase):
    def test_all_embedded_python_heredocs_parse_without_execution(self):
        checked = 0
        for filename in ('release.yml', *NATIVE):
            for title, block in steps(workflow(filename)):
                body = script(block)
                if body is None:
                    continue
                for match in re.finditer(r"<<'(PY[A-Z]*)'\n(.*?)\n\1(?:\n|$)", body, re.S):
                    with self.subTest(workflow=filename, step=title, delimiter=match[1]):
                        ast.parse(match[2], filename=f'{filename}:{title}')
                        checked += 1
        self.assertGreaterEqual(checked, 15)

    def test_python_helpers_cannot_dirty_source_with_bytecode(self):
        for filename in ('release.yml', *NATIVE):
            text = workflow(filename)
            with self.subTest(workflow=filename):
                global_env = text.split('\nenv:\n', 1)[1].split('\njobs:\n', 1)[0]
                self.assertIn("  PYTHONDONTWRITEBYTECODE: '1'", global_env)

    def test_all_multiline_shell_blocks_parse_without_execution(self):
        checked = 0
        for filename in ('release.yml', *NATIVE):
            for title, block in steps(workflow(filename)):
                source = script(block)
                if source is None:
                    continue
                with self.subTest(workflow=filename, step=title):
                    result = subprocess.run(['bash', '-n'], input=source, text=True, capture_output=True)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    checked += 1
        self.assertGreaterEqual(checked, 35)

    def test_maintenance_preflight_hardblock_precedes_product_fetch(self):
        release = workflow('release.yml')
        first_guard = release.index('core_release.py check --profile "$RELEASE_PROFILE"')
        product = release.index('git init "$CORE_JOB_ROOT/product"')
        self.assertLess(first_guard, product)
        wrapper = (ROOT / 'release-train/core_release.py').read_text()
        self.assertIn("remaining and remaining[0] == 'maintenance-source-check'", wrapper)
        policy = json.loads((ROOT / 'release-train/maintenance-1.4.json').read_text())
        self.assertIs(policy['enabled'], False)
        self.assertIsNone(policy['registration'])
        self.assertIsNone(policy['controlRegistration'])

    def test_default_release_identity_and_dependency_graph_remain(self):
        release = workflow('release.yml')
        self.assertIn("format('Release {0} at {1}', inputs.version || github.ref_name, inputs.expected_sha || github.sha)", release)
        self.assertIn("format('Release maintenance-1.4 {0} control {1} product {2}', inputs.version, github.sha, inputs.expected_sha)", release)
        normal_template = re.search(r"\|\| format\('([^']+)', inputs.version \|\| github.ref_name, inputs.expected_sha \|\| github.sha\)", release)[1]
        for version, ref, requested_sha, event_sha, expected in [
            ('2.0.8', 'main', 'a' * 40, 'b' * 40, 'Release 2.0.8 at ' + 'a' * 40),
            ('', '2.0.8', '', 'b' * 40, 'Release 2.0.8 at ' + 'b' * 40),
        ]:
            self.assertEqual(normal_template.format(version or ref, requested_sha or event_sha), expected)
        self.assertIn('  group: release-publication\n  cancel-in-progress: false', release)
        for job, target in [('test-macos', 'macos.yml'), ('test-linux', 'linux.yml'), ('test-capi', 'capi.yml')]:
            self.assertIn(f'  {job}:\n    needs: preflight\n    uses: ./.github/workflows/{target}', release)
        self.assertIn('needs: [preflight, test-macos, test-linux, test-capi]', release)
        self.assertNotIn('continue-on-error:', release)
        normal = named(release, 'Publish validated source release')
        self.assertIn("inputs.profile != 'maintenance-1.4'", normal)
        self.assertIn('FLAGS=(--latest)', normal)
        self.assertIn('FLAGS=(--prerelease --latest=false)', normal)
        self.assertIn('--verify-tag --target "$EXPECTED_SHA"', normal)

    def test_only_publisher_has_write_permissions_or_write_credentials(self):
        release = workflow('release.yml')
        before, publisher = release.split('\n  release:\n', 1)
        self.assertNotIn('contents: write', before)
        self.assertEqual(publisher.count('contents: write'), 1)
        global_env = release.split('\nenv:\n', 1)[1].split('\njobs:\n', 1)[0]
        self.assertNotIn('GH_TOKEN', global_env)
        for filename in NATIVE:
            text = workflow(filename)
            with self.subTest(workflow=filename):
                self.assertIn('permissions:\n  contents: read', text)
                self.assertNotIn('contents: write', text)
                self.assertNotIn('GH_TOKEN', text)
                self.assertNotIn('git-credential', text)
                self.assertNotIn('github-token:', text)

    def test_reusable_inputs_default_to_event_source_and_control_is_separate(self):
        for filename in NATIVE:
            text = workflow(filename)
            with self.subTest(workflow=filename):
                for field in ('release_profile', 'source_sha', 'source_tree', 'control_sha', 'candidate_sha256'):
                    self.assertIn(f'      {field}:\n        type: string\n        required: false', text)
                self.assertIn("inputs.release_profile == 'maintenance-1.4' && inputs.source_sha || github.sha", text)
                self.assertIn('git -C "$CORE_JOB_ROOT/control" checkout --detach "$CORE_CONTROL_SHA"', text)
                self.assertIn('test "$CORE_CONTROL_SHA" = "$GITHUB_SHA"', text)
                self.assertIn('test "$GITHUB_REF" = refs/heads/main', text)
                self.assertIn('test "$GITHUB_EVENT_NAME" = workflow_dispatch', text)
                self.assertIn('rev-parse HEAD^{tree})" = "$CORE_SOURCE_TREE"', text)
                self.assertNotIn('python3 "$CORE_JOB_ROOT/source/release-train/', text)

    def test_candidate_transport_is_small_hash_bound_and_same_attempt(self):
        for filename in ('release.yml', *NATIVE):
            text = workflow(filename)
            with self.subTest(workflow=filename):
                self.assertIn('maintenance-candidate-${{ github.run_id }}-${{ github.run_attempt }}', text)
                self.assertNotIn('MAINTENANCE_CANDIDATE:', text)
                self.assertNotIn('inputs.candidate }}', text)
                self.assertIn("hashlib.sha256(path.read_bytes()).hexdigest() == os.environ['MAINTENANCE_CANDIDATE_SHA256']", text)
        for filename in NATIVE:
            text = workflow(filename)
            self.assertLess(text.index('Verify maintenance candidate transport'), text.index('      - name: swift' if filename == 'capi.yml' else '      - name: Build and run'))
            verify = named(text, 'Verify maintenance candidate transport and source before native execution')
            self.assertIn('--output "$CORE_JOB_ROOT/maintenance/source-verification.json"', verify)

    def test_artifact_actions_are_pinned_and_uploads_do_not_overwrite(self):
        for filename in ('release.yml', *NATIVE):
            for title, block in steps(workflow(filename)):
                if 'uses: actions/' not in block:
                    continue
                with self.subTest(workflow=filename, step=title):
                    self.assertTrue(f'actions/upload-artifact@{UPLOAD}' in block or f'actions/download-artifact@{DOWNLOAD}' in block)
                    if 'actions/upload-artifact@' in block:
                        self.assertIn('overwrite: false', block)
                        self.assertIn('if-no-files-found: error', block)
                    self.assertIn('maintenance-1.4', block)

    def test_each_native_gate_has_discovery_xml_and_source_bound_receipt(self):
        expected = {'macos.yml': ['core-macos'], 'linux.yml': ['core-linux'], 'capi.yml': ['capi-linux', 'capi-macos']}
        for filename, gates in expected.items():
            text = workflow(filename)
            self.assertGreaterEqual(text.count('--gtest_list_tests'), len(gates))
            self.assertGreaterEqual(text.count('export GTEST_OUTPUT="xml:'), len(gates))
            for gate in gates:
                self.assertIn('--gate ' + gate, text)
                self.assertIn('maintenance-' + gate + '-${{ github.run_id }}-${{ github.run_attempt }}', text)
            for title, block in steps(text):
                if title == 'Emit maintenance source-bound gate receipt':
                    self.assertIn('--product-root "$CORE_JOB_ROOT/source"', block)
                    self.assertIn('--output "$CORE_JOB_ROOT/maintenance/receipt.json"', block)
                    self.assertIn("'discovery':", block)
                    self.assertIn("'xml':", block)
                    self.assertIn("'toolchain':", block)
        capi = workflow('capi.yml')
        for marker in ["'all-target'", "'cmake-build'", "'c11'", "'symbols-declared'", "'symbols-exported'"]:
            self.assertIn(marker, capi)

    def test_stage_failure_never_creates_success_command_metadata(self):
        setup = script(named(workflow('macos.yml'), 'Prepare isolated maintenance control and evidence'))
        helper = setup.split("<<'BASH'\n", 1)[1].split('\nBASH', 1)[0]
        for exit_code in (0, 7):
            with self.subTest(exit_code=exit_code), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for relative in ('maintenance/test-logs', 'maintenance/commands'):
                    (root / relative).mkdir(parents=True)
                env = dict(os.environ, CORE_JOB_ROOT=str(root))
                source = 'set -euo pipefail\n' + helper + f"\nmaintenance_command fixture bash -c 'printf actual-result; exit {exit_code}'\n"
                result = subprocess.run(['bash'], input=source, text=True, capture_output=True, env=env)
                self.assertEqual(result.returncode, exit_code, result.stderr)
                metadata = root / 'maintenance/commands/fixture.json'
                self.assertEqual(metadata.exists(), exit_code == 0)
                self.assertIn('actual-result', (root / 'maintenance/test-logs/fixture.log').read_text())
                if metadata.exists():
                    value = json.loads(metadata.read_text())
                    self.assertEqual(value['exitCode'], 0)
                    self.assertIs(value['timedOut'], False)
                    self.assertIsNone(value['signal'])
                    self.assertEqual(value['argv'], ['bash', '-c', 'printf actual-result; exit 0'])

    def test_actual_workflow_manifest_and_upload_paths_close_over_receipt_inventory(self):
        # Reuse the protocol's explicitly synthetic identities, then execute the
        # real YAML manifest producer. Only Python metadata code runs here.
        import test_maintenance_release as fixtures
        fixture = fixtures.MaintenanceReleaseTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        protocol = fixtures.p
        observed = set()
        for filename in NATIVE:
            blocks = steps(workflow(filename))
            for index, (title, block) in enumerate(blocks):
                if title != 'Emit maintenance source-bound gate receipt':
                    continue
                body = script(block)
                gate = re.search(r'--gate ([-a-z]+)', body)[1]
                observed.add(gate)
                with self.subTest(gate=gate):
                    root = fixture.root / 'workflow-shaped' / gate
                    root.mkdir(parents=True)
                    _, _, seed, context, _ = fixture.gate_fixture(gate)
                    for stage, command in seed['commands'].items():
                        path = root / 'commands' / (stage + '.json')
                        path.parent.mkdir(exist_ok=True)
                        path.write_text(json.dumps(command))
                    producer = body.split("<<'PY'\n", 1)[1].split('\nPY', 1)[0]
                    result = subprocess.run([sys.executable, '-', str(root)], input=producer,
                                            text=True, capture_output=True)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    manifest = json.loads((root / 'gate-manifest.json').read_text())
                    files = {}
                    for suite, paths in manifest['suites'].items():
                        inventory = fixture.contract['gates'][gate]['suites'][suite]
                        files[paths['xml']] = fixtures.xml(inventory)
                        files[paths['discovery']] = fixtures.discovery(inventory)
                    for command in manifest['commands'].values():
                        files[command['log']] = b'SYNTHETIC successful fixture log; no native execution\n'
                    for name, path in manifest['artifacts'].items():
                        files[path] = (b'lattice_one\nlattice_two\n' if name.startswith('symbols-')
                                       else b'SYNTHETIC retained fixture evidence\n')
                    fixture.write_files(root, files)
                    receipt = protocol.make_gate_receipt(fixture.candidate, gate, root, manifest, context)
                    (root / 'receipt.json').write_bytes(protocol.encoded(receipt))
                    # These actual producer scratch leaves must not enter the
                    # authenticated native archive.
                    fixture.write_files(root, {'candidate/candidate.json': b'SYNTHETIC transport only\n',
                                              'source-verification.json': b'SYNTHETIC local check\n'})
                    upload_title, upload = blocks[index + 1]
                    self.assertEqual(upload_title, 'Retain maintenance gate evidence')
                    relatives = re.findall(r'^            \$\{\{ env\.CORE_JOB_ROOT \}\}/maintenance/(.+)$', upload, re.M)
                    self.assertTrue(relatives)
                    leaves = set()
                    for relative in relatives:
                        path = root / relative
                        if path.is_file():
                            leaves.add(relative)
                        elif path.is_dir():
                            leaves.update(str(leaf.relative_to(root)) for leaf in path.rglob('*') if leaf.is_file())
                    self.assertEqual(leaves, set(receipt['artifacts']) | {'receipt.json'})
                    self.assertNotIn('gate-manifest.json', leaves)
                    self.assertNotIn('candidate/candidate.json', leaves)
                    self.assertFalse(any(path.startswith('commands/') for path in leaves))
                    protocol.verify_gate_receipt(fixture.candidate, receipt, root)
        self.assertEqual(observed, set(protocol.GATES))

    def test_default_core_execution_has_one_original_command_and_no_discovery(self):
        for filename in ('macos.yml', 'linux.yml'):
            with self.subTest(workflow=filename), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                env = dict(os.environ, CORE_JOB_ROOT=str(root), CORE_RELEASE_PROFILE='main', CALL_LOG=str(root / 'calls'))
                # A shell function intercepts Swift. No compiler, package resolver
                # or product executable is invoked in this fixture.
                stub = 'swift() { printf "%s\\n" "$*" >> "$CALL_LOG"; }\n'
                body = script(named(workflow(filename), 'Build and run C++ tests'))
                result = subprocess.run(['bash'], input=stub + body, text=True, capture_output=True, env=env)
                self.assertEqual(result.returncode, 0, result.stderr)
                calls = (root / 'calls').read_text().splitlines()
                self.assertEqual(len(calls), 1)
                self.assertTrue(calls[0].startswith('run --package-path ' + str(root / 'source')))
                self.assertTrue(calls[0].endswith(' LatticeCoreTests'))
                self.assertNotIn('--gtest_list_tests', calls[0])
                self.assertFalse((root / 'maintenance').exists())

    def test_default_dependency_failure_cannot_be_masked_by_maintenance_setup(self):
        for filename in ('linux.yml', 'capi.yml'):
            for failed in (False, True):
                with self.subTest(workflow=filename, failed=failed), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    binary = root / 'apt-get'
                    binary.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$CALL_LOG"\n'
                                      'if [ "$1" = update ] && [ "$FAIL_UPDATE" = yes ]; then exit 43; fi\n')
                    binary.chmod(0o755)
                    env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ['PATH'],
                               CALL_LOG=str(root / 'calls'), FAIL_UPDATE='yes' if failed else 'no')
                    block = named(workflow(filename), 'Install dependencies')
                    self.assertNotIn('shell:', block)
                    result = subprocess.run(['sh', '-e'], input=script(block), env=env,
                                            text=True, capture_output=True)
                    self.assertEqual(result.returncode, 43 if failed else 0, result.stderr)
                    calls = (root / 'calls').read_text().splitlines()
                    self.assertEqual(len(calls), 1 if failed else 2)
                    self.assertFalse(any('python3' in call for call in calls))
            tooling = named(workflow(filename), 'Install maintenance receipt tooling')
            self.assertIn("inputs.release_profile == 'maintenance-1.4'", tooling)

    def test_capi_pipeline_preserves_default_shell_but_maintenance_fails_strictly(self):
        text = workflow('capi.yml')
        setup = script(named(text, 'Prepare isolated maintenance control and evidence'))
        helper = setup.split("<<'BASH'\n", 1)[1].split('\nBASH', 1)[0]
        symbol_blocks = [block for title, block in steps(text) if title.startswith('Symbols freeze')]
        self.assertEqual(len(symbol_blocks), 2)
        for shell, block in zip(('sh', 'bash'), symbol_blocks):
            self.assertNotIn('        shell:', block)
            body = script(block)
            self.assertTrue(body.startswith('if [ "$CORE_RELEASE_PROFILE" = maintenance-1.4 ]; then'))
            for profile in ('main', 'maintenance-1.4'):
                with self.subTest(shell=shell, profile=profile), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    for path in ('bin', 'tmp', 'symbols', 'maintenance/scripts', 'maintenance/test-logs',
                                 'maintenance/commands', 'maintenance/symbols', 'source/Sources/LatticeCAPI'):
                        (root / path).mkdir(parents=True, exist_ok=True)
                    # Deliberately reproduce an upstream pipeline failure that
                    # the pre-existing default workflow did not propagate.
                    nm = root / 'bin/nm'
                    nm.write_text('#!/bin/sh\nexit 17\n'); nm.chmod(0o755)
                    (root / 'source/Sources/LatticeCAPI/lattice_capi.symbols').write_text('')
                    (root / 'tmp/maintenance-command.sh').write_text(helper)
                    env = dict(os.environ, CORE_JOB_ROOT=str(root), CORE_RELEASE_PROFILE=profile,
                               PATH=str(root / 'bin') + os.pathsep + os.environ['PATH'])
                    result = subprocess.run([shell, '-e'], input=body, cwd=root / 'source', env=env,
                                            text=True, capture_output=True)
                    self.assertEqual(result.returncode, 0 if profile == 'main' else 17, result.stderr)
                    self.assertFalse((root / 'maintenance/commands/symbols.json').exists())

    def test_publisher_uses_registered_high_fd_transport_and_static_safe_commands(self):
        release = workflow('release.yml')
        self.assertNotIn('high_fd_run_id:\n        description:', release)
        self.assertIn("fd = candidate['gateContract']['highFd']", release)
        high_fd = named(release, 'Download control-registered high-descriptor evidence')
        self.assertIn('repository: jsflax/LatticeCore', high_fd)
        self.assertIn('artifact-ids: ${{ steps.evidence.outputs.high_fd_artifact_id }}', high_fd)
        publisher = named(release, 'Publish maintenance from the validated plan without changing latest')
        self.assertIn('--latest=false', publisher)
        self.assertNotIn('--prerelease', publisher)
        self.assertNotIn('eval ', publisher)
        self.assertNotIn("plan['releaseArgs']", publisher)
        self.assertIn('gh api --method POST repos/jsflax/LatticeCore/git/refs', publisher)
        self.assertIn('-f ref="refs/tags/$VERSION" -f sha="$SOURCE_SHA"', publisher)
        self.assertNotIn('git -C "$CORE_JOB_ROOT/product" tag', publisher)
        self.assertNotIn('git -C "$CORE_JOB_ROOT/product" push', publisher)
        for field in ('planDigest', 'notesSHA256', 'notesBytes', 'releaseReceiptSHA256', 'releaseReceiptBytes'):
            self.assertIn(field, publisher)
        self.assertIn("protocol.binding(candidate)", publisher)
        self.assertIn('protocol.github_context(', publisher)
        self.assertIn('verify-publication', publisher)
        self.assertIn('capture-state', publisher)
        self.assertNotIn('if ! git show-ref', publisher)
        self.assertNotIn('--force', publisher)
        prepare = named(release, 'Reconcile all maintenance evidence and prepare immutable publication plan')
        for gate in ('core-macos', 'core-linux', 'capi-linux', 'capi-macos'):
            self.assertIn(f'--gate-receipt "$CORE_JOB_ROOT/native/{gate}/receipt.json"', prepare)
        self.assertIn('publication-plan', prepare)
        self.assertIn('--product-root "$CORE_JOB_ROOT/product"', prepare)


if __name__ == '__main__':
    unittest.main()
