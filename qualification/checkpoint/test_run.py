"""Pure-file receipt/oracle tests; never spawn tools or load SQLite."""
import json
import tempfile
from pathlib import Path
import unittest
import xml.etree.ElementTree as ET

import run as qualification


class QualificationOracleTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        config = json.loads(Path(__file__).with_name('config.json').read_text())
        self.tests, self.red = config['tests'], config['baselineFailures']

    def xml(self, failures=()):
        root = ET.Element('testsuites', tests='6', failures=str(len(failures)), disabled='0', errors='0')
        suite = ET.SubElement(root, 'testsuite', name='CheckpointOwnership')
        for name in self.tests:
            case = ET.SubElement(suite, 'testcase', name=name, classname='CheckpointOwnership',
                                 status='run', result='completed')
            if name in failures:
                for _ in range(qualification.BASELINE_ASSERTIONS[name]):
                    ET.SubElement(case, 'failure', message='Expected equality of these values:\n'
                        '  observed.mutex_try_result\n    Which is: 0\n  mutex_result\n    Which is: 5')
        return root

    def inspect(self, root, expected=()):
        path = self.root / 'tests.xml'
        path.write_bytes(ET.tostring(root))
        return qualification.inspect_xml(path, self.tests, expected)

    def test_exact_candidate_pass(self):
        self.assertTrue(self.inspect(self.xml())['nativePassed'])

    def test_baseline_failures_stay_failed(self):
        result = self.inspect(self.xml(self.red), self.red)
        self.assertFalse(result['nativePassed'])
        self.assertEqual(result['failures'], sorted(self.red))

    def test_baseline_unexpected_pass_or_candidate_failure_refused(self):
        with self.assertRaises(ValueError): self.inspect(self.xml(), self.red)
        with self.assertRaises(ValueError): self.inspect(self.xml(self.red))

    def test_missing_duplicate_skipped_or_wrong_suite_refused(self):
        for mutation in ('missing', 'duplicate', 'skipped', 'classname', 'status'):
            with self.subTest(mutation=mutation):
                root = self.xml()
                suite = root.find('testsuite')
                cases = list(suite)
                if mutation == 'missing': suite.remove(cases[-1])
                if mutation == 'duplicate': cases[-1].set('name', cases[0].get('name'))
                if mutation == 'skipped': ET.SubElement(cases[0], 'skipped')
                if mutation == 'classname': cases[0].set('classname', 'OtherSuite')
                if mutation == 'status': cases[0].set('status', 'notrun')
                with self.assertRaises(ValueError): self.inspect(root)

    def test_baseline_wrong_cause_or_assertion_count_refused(self):
        for mutation in ('message', 'count'):
            root = self.xml(self.red)
            case = next(c for c in root.iter('testcase') if c.get('name') == self.red[0])
            if mutation == 'message': list(case)[0].set('message', 'fixture timed out')
            else: case.remove(list(case)[0])
            with self.assertRaises(ValueError): self.inspect(root, self.red)

    def test_aggregate_mismatch_refused(self):
        for attribute in ('tests', 'failures', 'disabled', 'errors'):
            root = self.xml()
            root.set(attribute, '99')
            with self.assertRaises(ValueError): self.inspect(root)

    def test_command_requires_clean_ordinary_exit(self):
        record = {'started': True, 'exitCode': 1, 'primaryError': None,
                  'cleanup': {'leaderReaped': True, 'groupGone': True, 'signals': [], 'errors': []}}
        self.assertTrue(qualification.clean_process(record, 1))
        for key, value in [('exitCode', 0), ('stopReason', 'timeout'), ('receivedSignals', ['SIGTERM']),
                           ('evidenceErrors', ['lost log']), ('primaryError', {'message': 'failed'})]:
            changed = {**record, key: value}
            self.assertFalse(qualification.clean_process(changed, 1))
        for key, value in [('leaderReaped', False), ('groupGone', False), ('signals', ['SIGTERM']), ('errors', ['unknown'])]:
            changed = {**record, 'cleanup': {**record['cleanup'], key: value}}
            self.assertFalse(qualification.clean_process(changed, 1))

    def test_incremental_object_binding(self):
        old = {'db.o': 'old', 'stable.o': 'same'}
        self.assertEqual(qualification.compare_objects(old, {**old, 'db.o': 'new'}, 'db.o'), ['db.o'])
        for changed in (old, {'db.o': 'new'}, {'db.o': 'new', 'stable.o': 'changed'}):
            with self.assertRaises(ValueError): qualification.compare_objects(old, changed, 'db.o')

    def test_compile_proof_requires_this_exact_build(self):
        source, build = self.root / 'source', self.root / 'build'
        cpp = source / qualification.DB_SOURCE
        cpp.parent.mkdir(parents=True); cpp.write_text('// source')
        build.mkdir(); (build / 'db.o').write_bytes(b'object')
        command = {'directory': str(build), 'file': str(cpp),
                   'arguments': ['clang++', '-DSQLITE_CORE', '-o', 'db.o', '-c', str(cpp)]}
        (build / 'compile_commands.json').write_text(json.dumps([command]))
        log = self.root / 'build.log'
        log.write_text('clang++ -DSQLITE_CORE -o db.o -c ' + str(cpp) + '\n')
        proof = qualification.compiler_proof(build, source, log)
        self.assertEqual(proof['object'], 'db.o')
        log.write_text('target already up to date\n')
        with self.assertRaises(ValueError): qualification.compiler_proof(build, source, log)


if __name__ == '__main__':
    unittest.main()
