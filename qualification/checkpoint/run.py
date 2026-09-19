#!/usr/bin/env python3
"""One baseline and one candidate; exact six tests, no retries or performance claim."""
import argparse
import json
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import sys
import xml.etree.ElementTree as ET

from guarded_process import GuardedRunner, Interrupts, digest, error_record, save_json

SHA = re.compile(r'^[0-9a-f]{40}$')
DB_SOURCE = 'Sources/LatticeCore/src/db.cpp'
BASELINE_ASSERTIONS = {
    'BoundedCheckpointOwnsConnectionUntilTimeoutRestoredThenAckPersists': 2,
    'BusyReaderMapsLikePragmaAndRetiresOnlyAfterUnlockAndRestore': 1,
    'OpenCallerTransactionRefusedWithoutCommitRollbackOrTimeoutLeak': 2,
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def clean_process(record, expected_exit):
    cleanup = record.get('cleanup', {})
    return (record.get('started') is True and record.get('exitCode') == expected_exit
            and record.get('primaryError') is None and not record.get('stopReason')
            and not record.get('evidenceErrors') and not record.get('receivedSignals')
            and cleanup.get('leaderReaped') is True and cleanup.get('groupGone') is True
            and not cleanup.get('signals') and not cleanup.get('errors'))


def inspect_xml(path, tests, expected_failures):
    require(path.is_file() and path.stat().st_size <= 4 * 2**20, 'missing/oversized test XML')
    root = ET.fromstring(path.read_bytes())
    require(root.tag == 'testsuites', 'wrong XML root')
    cases = list(root.iter('testcase'))
    require(len(cases) == len(tests), 'wrong case count')
    seen, failures = set(), set()
    for case in cases:
        name = case.get('name')
        require(name in tests and name not in seen, 'unknown/duplicate case')
        require(case.get('classname') == 'CheckpointOwnership', 'wrong test suite')
        require(case.get('status') == 'run' and case.get('result') == 'completed', 'case did not run')
        require(not list(case.iter('skipped')) and not list(case.iter('error')), 'skipped/error case')
        assertions = list(case.iter('failure'))
        if assertions:
            failures.add(name)
            require(name in expected_failures and len(assertions) == BASELINE_ASSERTIONS.get(name),
                    'unexpected failure assertion count')
            for assertion in assertions:
                message = assertion.get('message', '') + '\n' + ''.join(assertion.itertext())
                require(re.search(r'observed\.mutex_try_result\s+Which is: 0', message)
                        and re.search(r'\bmutex_result\s+Which is: 5', message),
                        'baseline failed for a reason other than missing mutex ownership')
        seen.add(name)
    require(seen == set(tests), 'missing test case')
    require(failures == set(expected_failures), 'unexpected passing/failing test set')
    require(root.get('tests') == str(len(tests)) and root.get('failures') == str(len(failures)),
            'XML aggregate mismatch')
    require(root.get('disabled') == '0' and root.get('errors') == '0', 'disabled/error tests')
    return {'tests': sorted(seen), 'failures': sorted(failures),
            'nativePassed': not failures, 'xmlSHA256': digest(path)}


def object_snapshot(build):
    return {str(p.relative_to(build)): digest(p) for p in sorted(build.rglob('*.o')) if p.is_file()}


def compare_objects(before, after, db_object):
    require(before.keys() == after.keys(), 'object graph changed across variants')
    changed = sorted(k for k in before if before[k] != after[k])
    require(changed == [db_object], 'candidate must rebuild exactly the corrected Core object')
    return changed


def compiler_proof(build, source, log):
    database = json.loads((build / 'compile_commands.json').read_text())
    matches = [e for e in database if Path(e['file']).resolve() == (source / DB_SOURCE).resolve()]
    require(len(matches) == 1, 'missing/ambiguous Core db.cpp compiler command')
    entry = matches[0]
    argv = entry.get('arguments') or shlex.split(entry['command'])
    require('-c' in argv and '-o' in argv and '-DSQLITE_CORE' in argv, 'unexpected Core compiler flags')
    require(Path(argv[argv.index('-c') + 1]).resolve() == (source / DB_SOURCE).resolve(), 'wrong compiler input')
    object_arg = argv[argv.index('-o') + 1]
    obj = (Path(entry['directory']) / object_arg).resolve()
    require(obj.is_relative_to(build.resolve()) and obj.is_file(), 'missing/unowned Core object')
    lines = log.read_text(errors='replace').splitlines()
    require(any(str(source / DB_SOURCE) in line and object_arg in line and ' -c ' in line for line in lines),
            'db.cpp not witnessed compiling in this build')
    return {'command': entry, 'sourceSHA256': digest(source / DB_SOURCE),
            'object': str(obj.relative_to(build.resolve())), 'objectSHA256': digest(obj),
            'compileCommandsSHA256': digest(build / 'compile_commands.json'), 'buildLogSHA256': digest(log)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--driver-sha', required=True)
    args = parser.parse_args()
    require(SHA.fullmatch(args.driver_sha), 'invalid driver commit')
    root = args.root.resolve()
    require(root.is_relative_to(Path.home() / 'localdev'), 'root must be under localdev')
    here = Path(__file__).resolve().parent
    driver = here.parents[1]
    require(driver == root / 'driver', 'unexpected driver path')
    receipts = root / 'receipts'
    receipts.mkdir(exist_ok=False)
    config = json.loads((here / 'config.json').read_text())
    require(config['schema'] == 1 and SHA.fullmatch(config['baseCommit']), 'invalid source config')
    limits = config['limits']
    source, build = root / 'source', root / 'build'
    require(not source.exists() and not build.exists(), 'source/build path already exists')
    env = os.environ.copy()
    for name in ('TMPDIR', 'TMP', 'TEMP'):
        env[name] = str(root / 'tmp')
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    env['CLANG_MODULE_CACHE_PATH'] = str(root / 'module-cache')
    results, primary = {}, None
    with Interrupts() as interrupts:
        runner = GuardedRunner(root, receipts, env, interrupts,
            free_floor=limits['freeFloorBytes'], packet_ceiling=limits['packetCeilingBytes'],
            log_ceiling=limits['logCeilingBytes'], overall_seconds=limits['overallSeconds'],
            reserve=limits['finalizationReserveSeconds'])

        def command(label, argv, cwd=driver, timeout=60):
            log = runner.run(label, [str(x) for x in argv], cwd=cwd, timeout=timeout,
                             require_full_timeout=True)
            record = json.loads((receipts / (label + '.json')).read_text())
            require(clean_process(record, 0), label + ' did not settle cleanly')
            return log

        def git_text(label, *argv):
            return command(label, ['git', *argv], cwd=source).read_text().strip()

        def attest(name):
            tree = git_text(name + '-tree', 'write-tree')
            require(tree == config['variants'][name]['tree'], name + ' wrong source tree')
            # Include every tracked/native source input; all build outputs live outside source.
            names = [name for name in git_text(name + '-files', 'ls-files', '-z').split('\0') if name]
            actual = {f: digest(source / f) for f in names}
            require(all(actual.get(f) == h for f, h in config['variants'][name]['files'].items()),
                    name + ' changed postimage')
            status = git_text(name + '-unstaged', 'diff', '--name-only')
            untracked = git_text(name + '-untracked', 'ls-files', '--others', '--exclude-standard')
            require(not status and not untracked, name + ' unsealed source changes')
            save_json(receipts / (name + '-source.json'), {'baseCommit': config['baseCommit'],
                'sourceTree': tree, 'files': actual, 'variant': name,
                'baselineNormalization': config['baselineNormalization'] if name == 'baseline' else None})
            return actual

        try:
            driver_head = command('driver-head', ['git', 'rev-parse', 'HEAD']).read_text().strip()
            require(driver_head == args.driver_sha, 'wrong driver commit')
            driver_status = command('driver-status', ['git', 'status', '--porcelain']).read_text().strip()
            require(not driver_status, 'dirty qualification driver')
            inputs = {str(p.relative_to(driver)): digest(p) for p in sorted(here.rglob('*')) if p.is_file()}
            inputs['.github/workflows/checkpoint-ownership.yml'] = digest(driver / '.github/workflows/checkpoint-ownership.yml')
            for name in inputs:
                destination = receipts / 'input-files' / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(driver / name, destination)
            save_json(receipts / 'binding.json', {'driverCommit': driver_head, 'inputs': inputs,
                'baseCommit': config['baseCommit'], 'platform': platform.platform(),
                'python': platform.python_version(), 'runID': env.get('GITHUB_RUN_ID'),
                'attempt': env.get('GITHUB_RUN_ATTEMPT'), 'limits': limits})
            for variant in ('baseline', 'candidate'):
                require(digest(here / (variant + '.patch')) == config['variants'][variant]['patchSHA256'], 'patch hash mismatch')
            require(digest(here / 'transition.patch') == config['transitionPatchSHA256'], 'transition hash mismatch')
            command('clang-version', ['clang++', '--version'])
            command('cmake-version', ['cmake', '--version'])
            command('init-source', ['git', 'init', source])
            command('fetch-base', ['git', 'fetch', '--depth=1', 'https://github.com/' + config['sourceRepository'] + '.git', config['baseCommit']], cwd=source, timeout=120)
            command('checkout-base', ['git', 'checkout', '--detach', config['baseCommit']], cwd=source)
            command('apply-baseline', ['git', 'apply', '--index', here / 'baseline.patch'], cwd=source)
            baseline_files = attest('baseline')
            command('configure', ['cmake', '-S', here, '-B', build, '-G', 'Unix Makefiles',
                '-DCHECKPOINT_SOURCE_DIR=' + str(source), '-DCMAKE_BUILD_TYPE=Debug',
                '-DCMAKE_EXPORT_COMPILE_COMMANDS=ON', '-DCMAKE_C_COMPILER=clang', '-DCMAKE_CXX_COMPILER=clang++'],
                timeout=limits['configureSeconds'])
            old_objects, old_binary = None, None
            for variant in ('baseline', 'candidate'):
                if variant == 'candidate':
                    command('apply-candidate-transition', ['git', 'apply', '--index', here / 'transition.patch'], cwd=source)
                    candidate_files = attest('candidate')
                    require(baseline_files.keys() == candidate_files.keys(), 'source graph membership changed')
                    require([f for f in baseline_files if baseline_files[f] != candidate_files[f]] == [DB_SOURCE],
                            'variants differ beyond Core db.cpp')
                build_log = command(variant + '-build', ['cmake', '--build', build, '--target',
                    'CheckpointOwnershipQualificationTests', '--parallel', str(limits['parallelBuildJobs']), '--verbose'],
                    timeout=limits[variant + 'BuildSeconds'])
                proof = compiler_proof(build, source, build_log)
                objects = object_snapshot(build)
                binary = build / 'CheckpointOwnershipQualificationTests'
                require(binary.is_file(), 'test executable missing')
                binary_hash = digest(binary)
                if variant == 'candidate':
                    proof['changedObjects'] = compare_objects(old_objects, objects, proof['object'])
                    require(binary_hash != old_binary, 'candidate binary did not change')
                proof.update(binarySHA256=binary_hash, objects=objects)
                save_json(receipts / (variant + '-compiler-proof.json'), proof)
                shutil.copyfile(binary, receipts / (variant + '-test-executable'))
                old_objects, old_binary = objects, binary_hash
                env['LATTICE_TEST_LOG_PATH'] = str(receipts / (variant + '-native.log'))
                xml = receipts / (variant + '.xml')
                expected_failures = config['baselineFailures'] if variant == 'baseline' else []
                expected_exit = 1 if expected_failures else 0
                # Preserve the raw failing baseline command receipt verbatim. Only
                # its exact expected assertions + clean process permit the next run.
                test_exception = None
                try:
                    command(variant + '-tests', [binary,
                        '--gtest_filter=' + ':'.join('CheckpointOwnership.' + t for t in config['tests']),
                        '--gtest_repeat=1', '--gtest_output=xml:' + str(xml)], timeout=limits['testSeconds'])
                except BaseException as error:
                    test_exception = error_record(error)
                record = json.loads((receipts / (variant + '-tests.json')).read_text())
                require(clean_process(record, expected_exit), variant + ' command/cleanup mismatch')
                result = inspect_xml(xml, config['tests'], expected_failures)
                require(digest(binary) == binary_hash, variant + ' test executable mutated during run')
                result.update(commandSHA256=digest(receipts / (variant + '-tests.json')),
                              binarySHA256=binary_hash, commandException=test_exception,
                              expectedFailureObserved=variant == 'baseline')
                save_json(receipts / (variant + '-qualification.json'), result)
                results[variant] = result
                # Detect any native source mutations before transitioning/reusing objects.
                final = {f: digest(source / f) for f in baseline_files}
                expected = baseline_files if variant == 'baseline' else candidate_files
                require(final == expected, variant + ' source changed during build/test')
            require(not interrupts.received, 'runner interrupted')
        except BaseException as error:
            primary = error_record(error)
        finally:
            with interrupts.hold():
                save_json(receipts / 'RESULT.json', {'qualified': primary is None,
                    'primaryError': primary, 'variants': results, 'driverCommit': args.driver_sha,
                    'receivedSignals': interrupts.received, 'commands': runner.records,
                    'baselineNativePassed': results.get('baseline', {}).get('nativePassed'),
                    'candidateNativePassed': results.get('candidate', {}).get('nativePassed'),
                    'scope': 'focused correctness schedule qualification; not SDK calibration or release gate'})
    if primary:
        print(json.dumps(primary), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
