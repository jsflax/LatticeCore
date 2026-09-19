# GuardedRunner extraction: existing reviewed SDK development runner, prefix unchanged.
#!/usr/bin/env python3
"""Run a bounded, exact edited development graph; never release qualification."""
import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import signal
import subprocess
import time


FREE_FLOOR = 12 * 2**30
PACKET_CEILING = 30 * 2**30
LOG_CEILING = 512 * 2**20
OVERALL_SECONDS = 210 * 60
FINALIZATION_RESERVE = 10 * 60
SHA = re.compile(r'^[0-9a-f]{40}$')


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def error_record(error):
    return {'type': type(error).__name__, 'message': str(error)}


def save_json(path, value):
    # Evidence names are one-shot; failed/incomplete writes are not overwritten.
    with path.open('x') as output:
        json.dump(value, output, indent=2, sort_keys=True)
        output.write('\n')


class RunnerInterrupted(Exception):
    pass


class Interrupts:
    def __init__(self):
        self.received = []
        self.deferred = 0
        self.previous = {}

    def handle(self, number, _frame):
        self.received.append(signal.Signals(number).name)
        if not self.deferred and len(self.received) == 1:
            raise RunnerInterrupted(self.received[-1])

    def __enter__(self):
        for number in (signal.SIGTERM, signal.SIGINT):
            self.previous[number] = signal.signal(number, self.handle)
        return self

    def __exit__(self, *_):
        for number, previous in self.previous.items():
            signal.signal(number, previous)

    @contextlib.contextmanager
    def hold(self):
        self.deferred += 1
        try:
            yield
        finally:
            self.deferred -= 1


def allocated(root):
    total = 0
    for directory, _, names in os.walk(root):
        for name in names:
            try:
                total += (Path(directory) / name).lstat().st_blocks * 512
            except FileNotFoundError:
                pass
    return total


class GuardedRunner:
    """Owns only sessions it creates. Limits are sampled, with final checks."""
    def __init__(self, root, receipts, env, interrupts, *, free_floor=FREE_FLOOR,
                 packet_ceiling=PACKET_CEILING, log_ceiling=LOG_CEILING,
                 overall_seconds=OVERALL_SECONDS, reserve=FINALIZATION_RESERVE,
                 poll_seconds=0.5, signal_grace=5):
        self.root, self.receipts, self.env = root, receipts, env
        self.interrupts = interrupts
        self.free_floor, self.packet_ceiling, self.log_ceiling = free_floor, packet_ceiling, log_ceiling
        self.started = time.monotonic()
        self.work_deadline = self.started + overall_seconds - reserve
        self.overall_deadline = self.started + overall_seconds
        self.poll_seconds, self.signal_grace = poll_seconds, signal_grace
        self.records = []

    def measure(self, log):
        return {'freeBytes': shutil.disk_usage(self.root).free,
                'packetBytes': allocated(self.root),
                'logBytes': log.stat().st_size if log.exists() else 0}

    def violation(self, sample):
        if sample['freeBytes'] < self.free_floor:
            return 'disk floor'
        if sample['packetBytes'] > self.packet_ceiling or sample['logBytes'] > self.log_ceiling:
            return 'artifact ceiling'
        return None

    @staticmethod
    def group_state(pid):
        try:
            os.killpg(pid, 0)
            return True, None, 'killpg alive'
        except ProcessLookupError:
            return False, None, 'killpg ESRCH'
        except OSError as error:
            diagnostic = error_record(error)
            # macOS may report EPERM while a just-killed group disappears. A
            # bounded process-table read can prove absence without signalling
            # any other PID/group. Preserve only matching group members.
            try:
                table = subprocess.run(['ps', '-axo', 'pid=,pgid='],
                                       capture_output=True, text=True, timeout=2, check=True)
                rows = [line.split() for line in table.stdout.splitlines() if line.strip()]
                if any(len(row) != 2 or not all(cell.isdigit() for cell in row) for row in rows):
                    raise ValueError('unparseable ps group membership')
                members = [int(row[0]) for row in rows if int(row[1]) == pid]
                diagnostic['ownedGroupMembers'] = members
                return bool(members), diagnostic, 'ps owned-group membership'
            except BaseException as fallback_error:
                diagnostic['fallbackError'] = error_record(fallback_error)
                return None, diagnostic, 'missing group-absence proof'

    def cleanup(self, process):
        proof = {'signals': [], 'errors': [], 'groupGone': False,
                 'leaderReaped': False, 'proof': 'unknown'}
        for number in (signal.SIGTERM, signal.SIGKILL):
            # Reap the leader even while residual children keep the group alive.
            process.poll()
            exists, error, method = self.group_state(process.pid)
            if error:
                proof['errors'].append(error)
            if exists is False:
                proof.update(groupGone=True, proof=method)
                break
            try:
                os.killpg(process.pid, number)
                proof['signals'].append(signal.Signals(number).name)
            except ProcessLookupError:
                # The group may disappear between liveness check and signal.
                pass
            except OSError as error:
                proof['errors'].append(error_record(error))
            until = min(time.monotonic() + self.signal_grace, self.overall_deadline)
            while time.monotonic() < until:
                process.poll()
                exists, error, method = self.group_state(process.pid)
                if error:
                    proof['errors'].append(error)
                    break
                if exists is False:
                    proof.update(groupGone=True, proof=method)
                    break
                time.sleep(min(0.1, max(0, until - time.monotonic())))
            if proof['groupGone']:
                break
        try:
            process.wait(timeout=max(0.01, min(2, self.overall_deadline - time.monotonic())))
            proof['leaderReaped'] = True
        except (subprocess.TimeoutExpired, OSError) as error:
            proof['errors'].append(error_record(error))
        exists, error, method = self.group_state(process.pid)
        if error:
            proof['errors'].append(error)
        proof['groupGone'] = exists is False
        proof['proof'] = method if exists is False else 'missing group-absence proof'
        return proof

    def run(self, label, argv, *, cwd, timeout=3600, require_full_timeout=False):
        if self.interrupts.received:
            raise RunnerInterrupted('runner has already received interruption')
        log = self.receipts / (label + '.log')
        record = {'argv': argv, 'cwd': str(cwd), 'timeoutSeconds': timeout,
                  'started': False, 'freeFloor': self.free_floor,
                  'packetCeiling': self.packet_ceiling, 'logCeiling': self.log_ceiling,
                  'primaryError': None, 'evidenceErrors': []}
        process = None
        output = None
        started = time.monotonic()
        deadline = min(started + timeout, self.work_deadline)
        primary = None
        try:
            sample = self.measure(log)
            record.update(initial=sample, minFreeBytes=sample['freeBytes'], peakPacketBytes=sample['packetBytes'])
            record['stopReason'] = self.violation(sample)
            remaining = self.work_deadline - time.monotonic()
            if remaining <= 0 or (require_full_timeout and remaining < timeout):
                record['stopReason'] = 'overall budget cannot admit unchanged command timeout'
            if record['stopReason']:
                raise RuntimeError(record['stopReason'])
            output = log.open('xb')
            # Defer TERM/INT until Popen returns and ownership is recorded:
            # the OS child can exist before the Python assignment completes.
            with self.interrupts.hold():
                process = subprocess.Popen(argv, cwd=cwd, env=self.env, stdout=output,
                                           stderr=subprocess.STDOUT, start_new_session=True)
                record.update(started=True, pid=process.pid, ownedPGID=process.pid)
            if self.interrupts.received:
                raise RunnerInterrupted('interrupted during owned process launch')
            while process.poll() is None:
                sample = self.measure(log)
                record['minFreeBytes'] = min(record['minFreeBytes'], sample['freeBytes'])
                record['peakPacketBytes'] = max(record['peakPacketBytes'], sample['packetBytes'])
                record['stopReason'] = self.violation(sample)
                if not record['stopReason'] and time.monotonic() >= deadline:
                    record['stopReason'] = 'command timeout' if deadline < self.work_deadline else 'overall work budget'
                if record['stopReason']:
                    raise RuntimeError(record['stopReason'])
                time.sleep(self.poll_seconds)
        except BaseException as error:
            primary = error
            record['primaryError'] = error_record(error)
        finally:
            # A second TERM/INT cannot interrupt cleanup or overwrite the first error.
            with self.interrupts.hold():
                if process is not None:
                    try:
                        record['cleanup'] = self.cleanup(process)
                    except BaseException as error:
                        record['cleanup'] = {'groupGone': False, 'leaderReaped': False,
                                             'proof': 'cleanup raised; proof missing', 'errors': [error_record(error)]}
                    record['exitCode'] = process.returncode
                else:
                    record['cleanup'] = {'groupGone': True, 'leaderReaped': True, 'proof': 'no process started'}
                    record['exitCode'] = None
                if output is not None:
                    try:
                        output.close()
                    except BaseException as error:
                        record['evidenceErrors'].append(error_record(error))
                try:
                    sample = self.measure(log)
                    record['final'] = sample
                    record['minFreeBytes'] = min(record.get('minFreeBytes', sample['freeBytes']), sample['freeBytes'])
                    record['peakPacketBytes'] = max(record.get('peakPacketBytes', 0), sample['packetBytes'])
                    final_violation = self.violation(sample)
                    if not record.get('stopReason') and final_violation:
                        record['stopReason'] = final_violation
                    if not record.get('stopReason') and time.monotonic() >= deadline:
                        record['stopReason'] = 'deadline exceeded before final acceptance'
                    if log.exists():
                        record.update(logSHA256=digest(log), logBytes=log.stat().st_size)
                except BaseException as error:
                    record['evidenceErrors'].append(error_record(error))
                record.update(elapsedSeconds=time.monotonic() - started,
                              receivedSignals=list(self.interrupts.received))
                record['success'] = (record['started'] and record['exitCode'] == 0 and primary is None
                                     and not record.get('stopReason') and not record['evidenceErrors']
                                     and not self.interrupts.received and record['cleanup']['groupGone']
                                     and record['cleanup']['leaderReaped'])
                try:
                    save_json(self.receipts / (label + '.json'), record)
                except BaseException as error:
                    record['success'] = False
                    record['evidenceErrors'].append(error_record(error))
                    print('RECEIPT_WRITE_FAILED', label, json.dumps(record), flush=True)
                self.records.append({'label': label, 'success': record['success']})
        print(label, 'success', record['success'], 'exit', record['exitCode'], flush=True)
        if primary is not None:
            raise primary
        if not record['success']:
            raise RuntimeError('unqualified command: ' + label)
        return log

