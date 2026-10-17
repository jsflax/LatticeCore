#!/usr/bin/env python3
"""Fixed Darwin child exec gate. No fork, manifest, store or authority handling.

The parent's actual pipe closes after its non-reaping kqueue registration. This
process execs the requested fixed command only after that token and EOF, preserving
its retained PID/session/credentials. Gate startup consumes the original budget.
"""
import os
import select
import signal
import sys
import time


def main():
    if not (sys.platform == 'darwin' and sys.flags.isolated and sys.flags.no_site
            and len(sys.argv) >= 5 and sys.argv[1].isdigit() and sys.argv[2].isdigit()
            and sys.argv[3] == '--' and signal.getsignal(signal.SIGCHLD) == signal.SIG_DFL):
        raise RuntimeError('invalid fixed exec gate')
    fd, end = int(sys.argv[1]), int(sys.argv[2])
    if fd < 3 or not 0 < end - time.monotonic_ns() <= 5400 * 1_000_000_000:
        raise RuntimeError('exec gate outside original deadline')
    os.set_inheritable(fd, False)
    os.set_blocking(fd, False)
    token = bytearray()
    try:
        while time.monotonic_ns() < end:
            remaining = (end - time.monotonic_ns()) / 1_000_000_000
            if remaining <= 0:
                break
            try:
                ready, _, _ = select.select([fd], [], [], remaining)
                if not ready:
                    continue
                block = os.read(fd, 5 - len(token))
            except (InterruptedError, BlockingIOError):
                continue
            if not block:
                if token != b'GO01' or time.monotonic_ns() >= end:
                    raise RuntimeError('exec gate not admitted')
                os.close(fd)
                fd = -1
                os.execvpe(sys.argv[4], sys.argv[4:], os.environ)
                raise RuntimeError('exec unexpectedly returned')
            token.extend(block)
            if len(token) > 4:
                raise RuntimeError('exec gate overflow')
        raise RuntimeError('exec gate deadline')
    finally:
        if fd >= 0:
            os.close(fd)


if __name__ == '__main__':
    try:
        main()
    except BaseException:
        os.write(2, b'managed command exec gate refused\n')
        os._exit(74)
