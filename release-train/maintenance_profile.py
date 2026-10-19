"""One-release, source-admission-only Core 1.4 profile. No release capability.

Only the control checkout supplies this module and the profile. A successful
check is source evidence, never permission to dispatch or publish a release.
"""
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
from urllib.parse import quote

import release_train as rt

PROFILE_PATH = 'release-train/maintenance-1.4.json'
PROFILE = 'maintenance-1.4'
REPOSITORY = 'jsflax/LatticeCore'
SOURCE_BRANCH = 'codex/linux-notifier-1-4-backport-20260929'
VERSION = '1.4.3'
BASE_SHA = '36b828864cbb1543e945898be31589f9c04d6384'
BASE_TREE = 'd7b353a9bb7067ea0250f5e86484ce4406fd6a88'
BACKPORT_SHA = 'bcf43a8cc789d5330a103fcf1e97ebdfbf7e2b84'
BACKPORT_TREE = '915ca2e5f4a4b8f4bd6429f2dcb65feec9c43538'
REQUIRED_CONTROL_FILES = frozenset({
    'release-train/core_release.py', 'release-train/maintenance_profile.py',
    'release-train/maintenance_release.py', 'release-train/maintenance-gates.json',
    'release-train/release_train.py', 'release-train/policy.json',
    '.github/workflows/release.yml', '.github/workflows/macos.yml',
    '.github/workflows/linux.yml', '.github/workflows/capi.yml',
})
METADATA_PATHS = frozenset({'CHANGELOG.md', '.github/workflows/release.yml'})
BACKPORT_PATHS = frozenset({
    'CMakeLists.txt', 'Sources/LatticeCore/src/cross_process_notifier_linux.cpp',
    'Tests/LatticeCoreTests/LinuxNotifierTests.cpp',
})
SHA256 = re.compile(r'[0-9a-f]{64}')
PROFILE_KEYS = frozenset({
    'schemaVersion', 'profile', 'enabled', 'repository', 'controlBranch',
    'sourceBranch', 'version', 'base', 'reviewedBackport', 'registration',
    'controlRegistration',
})
DIFF_KEYS = frozenset({'path', 'status', 'oldMode', 'oldBlob', 'newMode', 'newBlob'})
MAX_API_PAGES = 1000


def _keys(value, keys, label):
    rt.require(type(value) is dict and set(value) == set(keys),
               f'{label}: exact schema keys required')


def _sha(value, label):
    rt.require(type(value) is str and rt.SHA.fullmatch(value), f'{label}: full SHA required')


def _approval(value, label):
    rt.require(value['owner'] == 'jsflax', f'{label}: explicit jsflax owner required')
    review = value['review']
    rt.require(type(review) is str and 1 <= len(review.strip()) <= 2048
               and not any(ord(c) < 32 for c in review),
               f'{label}: explicit review reference required')


def _path(value):
    rt.require(type(value) is str and value and '\\' not in value
               and not any(ord(c) < 32 for c in value), 'invalid manifest path')
    p = PurePosixPath(value)
    rt.require(not p.is_absolute() and '..' not in p.parts and str(p) == value,
               'manifest path must be a normalized relative path')


def _diff_schema(value, label):
    rt.require(type(value) is list and value, f'{label}: nonempty diff required')
    seen = set()
    for row in value:
        _keys(row, DIFF_KEYS, label)
        _path(row['path'])
        rt.require(row['path'] not in seen, f'{label}: duplicate path')
        seen.add(row['path'])
        rt.require(row['status'] in ('A', 'M', 'D'), f'{label}: unsupported change kind')
        for side, absent in [('old', row['status'] == 'A'), ('new', row['status'] == 'D')]:
            mode, blob = row[side + 'Mode'], row[side + 'Blob']
            if absent:
                rt.require(mode is None and blob is None, f'{label}: absent side must be null')
            else:
                rt.require(mode == '100644', f'{label}: regular non-executable files required')
                _sha(blob, label)
    rt.require([r['path'] for r in value] == sorted(seen), f'{label}: paths must be sorted')


def validate_profile(profile, require_enabled=False):
    _keys(profile, PROFILE_KEYS, 'profile')
    rt.require(type(profile['schemaVersion']) is int and profile['schemaVersion'] == 1,
               'unsupported profile schema')
    rt.require(type(profile['enabled']) is bool, 'enabled must be boolean')
    # Reject inactive use before any Git or network operation.
    if require_enabled:
        rt.require(profile['enabled'], 'maintenance profile is disabled')
    for key, expected in [('profile', PROFILE), ('repository', REPOSITORY),
                          ('controlBranch', 'main'), ('sourceBranch', SOURCE_BRANCH),
                          ('version', VERSION)]:
        rt.require(profile[key] == expected, f'profile {key} is not the one-release allowlist value')
    _keys(profile['base'], {'tag', 'sha', 'tree'}, 'base')
    rt.require(profile['base'] == {'tag': '1.4.2', 'sha': BASE_SHA, 'tree': BASE_TREE},
               'base registration changed')
    _keys(profile['reviewedBackport'], {'sha', 'tree'}, 'reviewedBackport')
    rt.require(profile['reviewedBackport'] == {'sha': BACKPORT_SHA, 'tree': BACKPORT_TREE},
               'reviewed backport registration changed')
    reg, control = profile['registration'], profile['controlRegistration']
    if reg is not None:
        _keys(reg, {'sourceSha', 'sourceTree', 'owner', 'review', 'metadataDiff', 'sourceDiff'},
              'registration')
        _approval(reg, 'registration')
        _sha(reg['sourceSha'], 'registered source')
        _sha(reg['sourceTree'], 'registered tree')
        _diff_schema(reg['metadataDiff'], 'metadataDiff')
        _diff_schema(reg['sourceDiff'], 'sourceDiff')
        rt.require({x['path'] for x in reg['metadataDiff']} == METADATA_PATHS,
                   'metadata diff must contain only changelog and legacy workflow removal')
        rt.require({x['path'] for x in reg['sourceDiff']} == METADATA_PATHS | BACKPORT_PATHS,
                   'source diff must contain exactly backport and release metadata')
        metadata = {x['path']: x for x in reg['metadataDiff']}
        rt.require(metadata['CHANGELOG.md']['status'] == 'M'
                   and metadata['.github/workflows/release.yml']['status'] == 'D',
                   'changelog modification and legacy workflow deletion required')
    if control is not None:
        _keys(control, {'owner', 'review', 'files'}, 'controlRegistration')
        _approval(control, 'controlRegistration')
        _keys(control['files'], REQUIRED_CONTROL_FILES, 'control files')
        for value in control['files'].values():
            rt.require(type(value) is str and SHA256.fullmatch(value), 'control file SHA256 required')
    if profile['enabled']:
        rt.require(reg is not None and control is not None,
                   'enabled profile requires exact source/control registration and review')
    return profile


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        rt.require(key not in result, f'duplicate JSON key: {key}')
        result[key] = value
    return result


def _regular(root, relative):
    path = root / relative
    rt.require(path.is_file() and not path.is_symlink(), f'control file is not regular: {relative}')
    rt.require(path.resolve().is_relative_to(root), f'control file escapes checkout: {relative}')
    for ancestor in path.parents:
        if ancestor == root:
            break
        rt.require(not ancestor.is_symlink(), f'symlink ancestor: {relative}')
    return path


def load_profile(control_root):
    root = Path(control_root).resolve()
    path = _regular(root, PROFILE_PATH)
    return validate_profile(json.loads(path.read_text(), object_pairs_hook=_unique_object))


def _context(context, control_sha):
    env = os.environ if context is None else context
    rt.require(hasattr(env, 'get'), 'context must be an environment mapping')
    if env.get('GITHUB_ACTIONS') is not None:
        for key, expected in [('GITHUB_ACTIONS', 'true'), ('GITHUB_EVENT_NAME', 'workflow_dispatch'),
                              ('GITHUB_REF', 'refs/heads/main'), ('GITHUB_REPOSITORY', REPOSITORY),
                              ('GITHUB_SHA', control_sha)]:
            rt.require(env.get(key) == expected, f'unsupported GitHub context: {key}')
    else:
        rt.require(not any(str(k).startswith('GITHUB_') for k in env),
                   'partial GitHub context is not local admission')


def _checkout(root, sha, label):
    rt.require(Path(rt.git('rev-parse', '--show-toplevel', cwd=root)).resolve() == root,
               f'{label}: checkout root required')
    rt.require(rt.git('rev-parse', 'HEAD', cwd=root) == sha, f'{label}: HEAD changed')
    rt.clean(root)
    try:
        sparse = rt.git('config', '--bool', '--get', 'core.sparseCheckout', cwd=root)
    except subprocess.CalledProcessError as error:
        if error.returncode != 1:
            raise
        sparse = 'false'  # Unset is Git's non-sparse default.
    rt.require(sparse == 'false', f'{label}: sparse checkout is not admitted')
    rt.require(all(line.startswith('H ') for line in rt.git('ls-files', '-v', cwd=root).splitlines()),
               f'{label}: hidden index flags are not admitted')
    rt.require(not rt.git('for-each-ref', '--format=%(refname)', 'refs/replace', cwd=root),
               f'{label}: replacement Git objects are not admitted')
    urls = rt.git('remote', 'get-url', '--all', 'origin', cwd=root).splitlines()
    accepted = {f'https://github.com/{REPOSITORY}', f'https://github.com/{REPOSITORY}.git',
                f'git@github.com:{REPOSITORY}', f'git@github.com:{REPOSITORY}.git'}
    rt.require(len(urls) == 1 and urls[0] in accepted, f'{label}: exact canonical origin required')
    return rt.git('rev-parse', 'HEAD^{tree}', cwd=root)


def _tracked_file(root, relative):
    path = _regular(root, relative)
    raw = path.read_bytes()
    blob = hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest()
    expected = f'100644 blob {blob}\t{relative}'
    rt.require(rt.git('ls-tree', 'HEAD', '--', relative, cwd=root) == expected,
               f'control bytes do not match committed regular file: {relative}')
    return path


def _manifest(root, older, newer):
    data = rt.git('diff', '--raw', '--no-abbrev', '--no-renames', '--no-ext-diff', '-z', older, newer, cwd=root)
    fields = data.split('\0')
    if fields[-1] == '':
        fields.pop()
    rt.require(len(fields) % 2 == 0, 'malformed Git diff')
    rows = []
    for i in range(0, len(fields), 2):
        match = re.fullmatch(r':(\d{6}) (\d{6}) ([0-9a-f]{40}) ([0-9a-f]{40}) ([AMD])', fields[i])
        rt.require(match is not None, 'unexpected source change kind')
        old_mode, new_mode, old_blob, new_blob, status = match.groups()
        rows.append({'path': fields[i + 1], 'status': status,
                     'oldMode': None if status == 'A' else old_mode,
                     'oldBlob': None if status == 'A' else old_blob,
                     'newMode': None if status == 'D' else new_mode,
                     'newBlob': None if status == 'D' else new_blob})
    return sorted(rows, key=lambda row: row['path'])


def _pages(resource):
    result, seen = [], set()
    for page in range(1, MAX_API_PAGES + 1):
        batch = rt.gh_api(f'repos/{REPOSITORY}/{resource}?per_page=100&page={page}')
        rt.require(type(batch) is list and len(batch) <= 100, f'{resource}: invalid/incomplete page')
        for item in batch:
            rt.require(type(item) is dict, f'{resource}: malformed item')
            identity = item.get('name') if resource == 'tags' else item.get('id')
            rt.require((type(identity) is str and bool(identity)) if resource == 'tags'
                       else (type(identity) is int and identity > 0), f'{resource}: invalid identity')
            rt.require(identity not in seen, f'{resource}: repeated page/item')
            seen.add(identity)
            if resource == 'tags':
                rt.require(type(item.get('commit')) is dict, 'tag commit missing')
                _sha(item['commit'].get('sha'), 'tag commit')
            else:
                rt.require(type(item.get('tag_name')) is str and bool(item['tag_name'])
                           and type(item.get('draft')) is bool, 'release tag/draft missing')
            result.append(item)
        if len(batch) < 100:
            return result
    raise ValueError(f'{resource}: pagination did not terminate')


def _base_tag():
    ref = rt.gh_api(f'repos/{REPOSITORY}/git/ref/tags/1.4.2')
    rt.require(type(ref) is dict and ref.get('ref') == 'refs/tags/1.4.2', 'exact base tag required')
    obj, seen = ref.get('object'), set()
    for _ in range(32):
        rt.require(type(obj) is dict, 'malformed base tag object')
        _sha(obj.get('sha'), 'base tag object')
        if obj.get('type') == 'commit':
            rt.require(obj['sha'] == BASE_SHA, 'base tag moved')
            return obj['sha']
        rt.require(obj.get('type') == 'tag' and obj['sha'] not in seen, 'invalid/cyclic base tag chain')
        seen.add(obj['sha'])
        annotated = rt.gh_api(f'repos/{REPOSITORY}/git/tags/{obj["sha"]}')
        rt.require(type(annotated) is dict and annotated.get('sha') == obj['sha'],
                   'annotated base tag identity changed')
        obj = annotated.get('object')
    raise ValueError('base tag chain exceeds bound')


def _remote_branch(branch, sha):
    ref = rt.gh_api(f'repos/{REPOSITORY}/git/ref/heads/{quote(branch, safe="")}')
    rt.require(type(ref) is dict and ref.get('ref') == f'refs/heads/{branch}'
               and type(ref.get('object')) is dict and ref['object'].get('type') == 'commit'
               and ref['object'].get('sha') == sha, f'canonical branch changed: {branch}')


def _exact_absence(endpoint):
    """Only an authenticated API 404 with JSON Not Found establishes absence.

    Successful canonical branch/base/list reads precede this probe. gh errors
    without the exact response are never interpreted as missing resources.
    """
    try:
        response = subprocess.run(['gh', 'api', '--include', endpoint],
                                  capture_output=True, text=True, timeout=30, check=False)
    except (OSError, subprocess.TimeoutExpired):
        raise ValueError('exact absence request did not complete') from None
    text = response.stdout.replace('\r\n', '\n')
    rt.require(len(text) <= 1024 * 1024, 'exact absence response exceeds bound')
    parts = text.split('\n\n', 1)
    rt.require(len(parts) == 2, 'exact absence response lacks HTTP headers/body')
    first = parts[0].split('\n')[0]
    status = re.fullmatch(r'HTTP/[0-9.]+ ([0-9]{3})(?: [^\n]*)?', first)
    rt.require(status is not None, 'exact absence response has invalid HTTP status')
    rt.require(status[1] == '404' and response.returncode == 1,
               'exact tag/release exists or its absence is unverified')
    try:
        body = json.loads(parts[1], object_pairs_hook=_unique_object)
    except (ValueError, TypeError):
        raise ValueError('exact absence response has invalid JSON') from None
    rt.require(type(body) is dict and body.get('message') == 'Not Found',
               'exact absence response is not JSON Not Found')


def check_candidate(control_root, product_root, profile, version, expected_sha, control_sha, *, context=None):
    validate_profile(profile, require_enabled=True)
    rt.require(version == VERSION, 'only stable 1.4.3 is allowed')
    _sha(expected_sha, 'expected source')
    _sha(control_sha, 'control SHA')
    _context(context, control_sha)
    control, product = Path(control_root).resolve(), Path(product_root).resolve()
    rt.require(control != product and not control.is_relative_to(product)
               and not product.is_relative_to(control), 'distinct non-nested control/product checkouts required')
    rt.require(profile == load_profile(control), 'profile must match control checkout bytes')
    reg = profile['registration']
    rt.require(expected_sha == reg['sourceSha'], 'unregistered product SHA')
    control_tree = _checkout(control, control_sha, 'control')
    product_tree = _checkout(product, expected_sha, 'product')
    rt.require(product_tree == reg['sourceTree'], 'product tree differs from registration')
    _tracked_file(control, PROFILE_PATH)
    control_files = {p: rt.digest(_tracked_file(control, p)) for p in sorted(REQUIRED_CONTROL_FILES)}
    rt.require(control_files == profile['controlRegistration']['files'], 'registered control file changed')
    rt.require(rt.git('rev-parse', f'{BASE_SHA}^{{tree}}', cwd=product) == BASE_TREE,
               'base tree changed')
    rt.require(rt.git('rev-parse', f'{BACKPORT_SHA}^{{tree}}', cwd=product) == BACKPORT_TREE,
               'reviewed backport tree changed')
    rt.require(rt.git('rev-list', '--parents', '-n', '1', BACKPORT_SHA, cwd=product)
               == f'{BACKPORT_SHA} {BASE_SHA}', 'backport must directly descend from base')
    rt.require(rt.git('rev-list', '--parents', '-n', '1', expected_sha, cwd=product)
               == f'{expected_sha} {BACKPORT_SHA}', 'product must be one metadata-only child of backport')
    metadata = _manifest(product, BACKPORT_SHA, expected_sha)
    source_diff = _manifest(product, BASE_SHA, expected_sha)
    rt.require(metadata == reg['metadataDiff'], 'metadata diff differs from registration')
    rt.require(source_diff == reg['sourceDiff'], 'full source diff differs from registration')
    rt.check_notes(product, {'changelog': 'CHANGELOG.md'}, VERSION)
    rt.require(not (product / '.github/workflows/release.yml').exists(),
               'legacy tag-triggered release workflow must be removed')
    _remote_branch('main', control_sha)
    _remote_branch(SOURCE_BRANCH, expected_sha)
    _base_tag()
    tags = _pages('tags')
    rt.require(any(t['name'] == '1.4.2' for t in tags), 'complete tag listing must include base tag')
    rt.require(not any(t['name'] == VERSION for t in tags), 'exact maintenance tag already exists')
    proposed, line_tags = rt.Version(VERSION), []
    for item in tags:
        try:
            old = rt.Version(item['name'])
        except ValueError:
            continue  # Non-version evidence tags do not set version precedence.
        if old.core[:2] == (1, 4):
            rt.require(proposed > old, f'maintenance version must exceed remote 1.4 tag {old.text}')
            line_tags.append(old.text)
    releases = _pages('releases')
    rt.require(not any(r['tag_name'] == VERSION for r in releases),
               'maintenance release/draft already exists')
    _exact_absence(f'repos/{REPOSITORY}/git/ref/tags/{VERSION}')
    _exact_absence(f'repos/{REPOSITORY}/releases/tags/{VERSION}')
    return {
        'schemaVersion': 1, 'profile': PROFILE, 'repository': REPOSITORY,
        'version': VERSION, 'tag': VERSION, 'channel': 'stable',
        'sourceAdmission': True, 'dispatchAdmitted': False, 'publicationAdmitted': False,
        'control': {'sha': control_sha, 'tree': control_tree, 'branch': 'main', 'files': control_files},
        'profileSHA256': rt.digest(control / PROFILE_PATH),
        'source': {'sha': expected_sha, 'tree': product_tree, 'branch': SOURCE_BRANCH},
        'base': dict(profile['base']), 'reviewedBackport': dict(profile['reviewedBackport']),
        'sourceDiff': source_diff, 'metadataDiff': metadata,
        'remoteInventory': {'tagCount': len(tags), 'releaseCount': len(releases), 'lineTags': sorted(line_tags)},
        'checkedAt': rt.now(),
    }
