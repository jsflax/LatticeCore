#!/usr/bin/env python3
"""Core-specific, inactive maintenance admission; default releases use protocol v1."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

import maintenance_profile as m
import release_train as r


def select_profile(argv):
    """Strip one explicit profile option without reinterpreting default arguments."""
    remaining = []
    profile = None
    index = 0
    while index < len(argv):
        value = argv[index]
        if value == '--':
            remaining.extend(argv[index:])
            break
        if value == '--profile' or value.startswith('--profile='):
            r.require(profile is None, 'select exactly one release profile')
            if value == '--profile':
                index += 1
                r.require(index < len(argv) and not argv[index].startswith('--'),
                          '--profile requires a value')
                profile = argv[index]
            else:
                profile = value.partition('=')[2]
            r.require(profile in ('main', 'maintenance-1.4'), 'unknown release profile')
        else:
            remaining.append(value)
        index += 1
    return profile or 'main', remaining


def main(argv=None):
    profile_name, remaining = select_profile(list(sys.argv[1:] if argv is None else argv))
    if profile_name == 'main':
        # Keep the vendored protocol and its CLI semantics unchanged. In
        # particular, default dispatch retains its existing retry/run identity.
        original = sys.argv
        try:
            sys.argv = [original[0], *remaining]
            return r.main()
        finally:
            sys.argv = original

    # This is deliberately independent of the JSON enabled field. Inactive
    # source admission must never unlock native jobs or a publication command.
    r.require(remaining and remaining[0] == 'maintenance-source-check',
              'maintenance release commands are disabled; only maintenance-source-check '
              'is implemented, and it cannot admit a build, dispatch or publication')
    parser = argparse.ArgumentParser(description='Read-only maintenance source review; not release admission')
    parser.add_argument('command', choices=['maintenance-source-check'])
    parser.add_argument('--version', required=True)
    parser.add_argument('--expected-sha', required=True, help='Full product commit SHA')
    parser.add_argument('--control-sha', required=True, help='Full canonical main control commit SHA')
    parser.add_argument('--product-root', required=True, help='Separate existing product checkout')
    parser.add_argument('--output', help='Optional source review JSON; never a release receipt')
    args = parser.parse_args(remaining)
    root = Path(__file__).resolve().parent.parent
    profile = m.load_profile(root)
    m.validate_profile(profile, require_enabled=True)
    result = m.check_candidate(root, Path(args.product_root).resolve(), profile,
                               args.version, args.expected_sha, args.control_sha,
                               context=dict(os.environ))
    result.update(state='maintenance-source-reviewed-not-release-admitted',
                  dispatchAdmitted=False, publicationAdmitted=False)
    if args.output:
        output = Path(args.output).resolve()
        for checkout in (root, Path(args.product_root).resolve()):
            r.require(not output.is_relative_to(checkout),
                      'source review output must be outside both source checkouts')
        r.write(output, result)
    print(json.dumps(result, indent=2))
    return None


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as exc:
        # Never surface subprocess stderr or authentication diagnostics.
        detail = 'required git/GitHub operation failed' if isinstance(exc, subprocess.CalledProcessError) else str(exc)
        print('Core release stopped: ' + detail, file=sys.stderr)
        sys.exit(1)
