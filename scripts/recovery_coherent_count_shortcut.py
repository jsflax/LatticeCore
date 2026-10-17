#!/usr/bin/env python3
"""Separate genuine-old count seeds and six unchanged current migration parents.

This does not waive any failure/coverage refusal in the full Core qualifier.
Only this module's hosted invocation may execute commands; source review imports
and AST checks do not launch anything. Unknown endings never become artifacts
that upload may race with a still-live writer.
"""
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import stat
import sys
import time
import xml.etree.ElementTree as ET
sys.dont_write_bytecode = True
from recovery_coherent_supervisor import file_fact, run_stage, write_json

PLAN_SHA256 = 'dbad81eaf0bfe1630ed94fb7edf511da93dc0327b98176d5d3f1b5111ce50978'
GIB = 1024**3


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def bounded_text(path, limit):
    require(path.stat().st_size <= limit, 'bounded evidence size: '+str(path))
    return path.read_text()


def inventory(body):
    # Same grammar as recovery_coherent_qualification.inventory; no skip waiver.
    names, suite = [], None
    for line in body.splitlines():
        if re.fullmatch(r'[A-Za-z_][A-Za-z_0-9/.]*\.\s*(?:#.*)?', line):
            suite = line.split('#', 1)[0].strip()
        elif suite and line.startswith('  ') and line.strip():
            value = line.split('#', 1)[0].strip()
            require(re.fullmatch(r'[A-Za-z_0-9/]+', value), 'unknown inventory name')
            names.append(suite+value)
    require(names and len(names) == len(set(names)), 'empty/duplicate inventory')
    return names


def dependency_names(body):
    logical = body.replace('\\\n', '')
    target, colon, dependencies = logical.partition(':')
    require(colon and target.strip(), 'dependency rule absent')
    names = shlex.split(dependencies, posix=True)
    require(names and not any(x.endswith(':') for x in names),
            'empty or unsupported multi-rule dependencies')
    return names


def argument_vector(row):
    argv = row.get('arguments') or shlex.split(row['command'], posix=True)
    require(argv and '-c' in argv and '-o' in argv, 'whole compile recipe absent')
    return argv


def semantic_flags(argv):
    # Path relocations, dependency outputs, architecture and SDK are checked
    # independently. Every other code-generation flag must match the old family.
    kept = []
    pairs = {'-I','-isystem','-iquote','-isysroot','-arch','-o','-c','-MF','-MT','-MQ'}
    skip = False
    for word in argv[1:]:
        if skip:
            skip = False
            continue
        if word in pairs:
            skip = True
        elif word in ('-MD','-MMD'):
            continue
        elif word.startswith(('-I','-isystem','-iquote')):
            continue
        else:
            require(word.startswith(('-D','-g','-std=','-mmacosx-version-min=',
                                     '-f','-O')), 'unclassified compile flag: '+word)
            kept.append(word)
    require(not skip, 'truncated compiler option')
    return sorted(kept)


def natural(row):
    return (row.get('knownEnding') is True and row.get('exitCode') == 0
            and row.get('stopReason') is None and row.get('signals') == []
            and row.get('signalErrors') == [] and row.get('groupQueryError') is None)


def xml_population(path, expected, limit):
    tree = ET.fromstring(bounded_text(path, limit))
    cases = list(tree.iter('testcase'))
    names = [x.get('classname','')+'.'+x.get('name','') for x in cases]
    failures = [names[i] for i,x in enumerate(cases)
                if x.find('failure') is not None or x.find('error') is not None]
    skips = [names[i] for i,x in enumerate(cases)
             if x.find('skipped') is not None or x.get('status') != 'run'
             or x.get('result') in ('skipped','suppressed')]
    row = dict(names=names, failures=failures, skips=skips,
               expectedNames=expected, exactPopulation=names == expected)
    aggregate_errors=[(x.tag,k,x.get(k)) for x in tree.iter()
                      if x.tag in ('testsuites','testsuite')
                      for k in ('failures','errors','disabled')
                      if x.get(k) is not None and x.get(k)!='0']
    row['aggregateErrors']=aggregate_errors
    row['accepted'] = bool(names == expected and not failures and not skips
                           and not aggregate_errors and len(names) == len(set(names)))
    return row


class Run:
    def __init__(self, root, plan):
        self.root, self.plan = root, plan
        self.start = time.monotonic()
        self.end = self.start+plan['caps']['ownerSeconds']
        self.work_end = self.end-plan['caps']['endingReserveSeconds']
        root.mkdir(mode=0o700)
        self.out = root/'out'; self.out.mkdir(mode=0o700)
        self.publish = self.out/'upload'; self.publish.mkdir(mode=0o700)
        self.rows, self.errors, self.safety_errors = [], [], []
        self.shared_dependencies = {}
        self.result = dict(status='RUNNING', componentQualified=False,
            fullCoreSuiteAcceptance=False, fullSDKAcceptance=False,
            resourceProfileAcceptance=False, performanceAcceptance=False,
            releaseAcceptance=False, existingFullQualifierCoverageUnchanged=True,
            clocks=dict(ownerStart=self.start, ownerEnd=self.end,
                        workEnd=self.work_end, endingReserveSeconds=20,
                        noRenewal=True), plan=plan)
        for name in ('tmp','work','cache','seeds'):
            (root/name).mkdir(mode=0o700)
        self.env = {k:v for k,v in os.environ.items()
                    if not k.startswith(('GTEST_','LATTICE_','CMAKE_'))
                    and k not in {'CC','CXX','CFLAGS','CXXFLAGS','CPPFLAGS','LDFLAGS'}}
        self.env.update(TMPDIR=str(root/'tmp'), TMP=str(root/'tmp'),
            TEMP=str(root/'tmp'), TEST_TMPDIR=str(root/'tmp'),
            XDG_CACHE_HOME=str(root/'cache'),
            CLANG_MODULE_CACHE_PATH=str(root/'cache/clang'),
            SWIFTPM_MODULECACHE_OVERRIDE=str(root/'cache/swift'),
            PYTHONDONTWRITEBYTECODE='1')
        self.record('INPUT', self.result)

    def time_check(self):
        require(time.monotonic() < self.work_end, 'fixed owner work deadline')

    def record(self, name, value):
        write_json(self.out/(name+'.json'), value)
        write_json(self.publish/(name+'.json'), value)

    def stable(self, path):
        self.time_check()
        require(stat.S_ISREG(path.lstat().st_mode), 'not regular/no-follow: '+str(path))
        fact = file_fact(path)
        self.time_check()
        return fact

    def upload_file(self, path, relative):
        # Only callers with an actual known ending may pass subprocess outputs.
        before = self.stable(path)
        target = self.publish/relative
        require(not target.exists(), 'artifact already published: '+str(target))
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with path.open('rb') as source, target.open('xb') as destination:
            shutil.copyfileobj(source, destination, 1024*1024)
        os.chmod(target, 0o600)
        after = self.stable(path)
        copied = self.stable(target)
        require(before == after and copied['bytes'] == before['bytes']
                and copied['sha256'] == before['sha256'], 'artifact copy drift')
        return dict(original=after, uploaded=copied)

    def stage(self, name, command, cwd, cap, env=None, xml=None):
        require(not any(not x.get('knownEnding') for x in self.rows
                        if not x.get('noChildIssued')), 'prior ending unknown')
        # Full command cap is admitted; the supervisor's at-most20s ending
        # attempts fit the sole reserve INSIDE the fixed300-minute owner.
        require(time.monotonic()+cap <= self.work_end,
                'whole next stage does not fit original owner: '+name)
        require(os.statvfs(self.root).f_bavail*os.statvfs(self.root).f_frsize
                >= self.plan['resources']['minimumFreeBytes'], 'disk floor')
        failure = None
        slot=len(self.rows)
        self.rows.append(dict(stage=name,knownEnding=False,endingReceiptMissing=True))
        try:
            run_stage(self.out, name, list(map(str,command)), cwd, env or self.env,
                      cap, self.plan['resources']['minimumFreeBytes'])
        except BaseException as error:
            failure = error
        receipt = self.out/(name+'.json')
        require(receipt.exists(), 'supervisor failed without ending receipt: '+name)
        row = json.loads(receipt.read_text())
        if failure is not None:
            self.errors.append(dict(firstStageFailure=name,supervisorError=repr(failure),
                                   rawEndingReceipt=str(receipt)))
        self.rows[slot]=row
        self.record(name+'-ENDING', row)
        if row.get('knownEnding'):
            for suffix in ('.stdout','.stderr'):
                self.upload_file(self.out/(name+suffix), 'raw/'+name+suffix)
            if xml is not None and xml.exists():
                # Bind raw first-failure/malformed XML BEFORE parsing or followup.
                fact = self.upload_file(xml, 'raw/'+name+'.xml')
                self.record(name+'-RAW-XML', fact)
        elif xml is not None:
            self.record(name+'-XML-UNPROVED', dict(path=str(xml),
                reason='ending unknown; raw mutable file not hashed/copied'))
        if failure is not None:
            raise failure
        require(natural(row), 'non-natural/unproved stage: '+name)
        return row

    def text_stage(self, name, command, cwd, cap=30):
        self.stage(name, command, cwd, cap)
        return bounded_text(self.out/(name+'.stdout'), 16*1024**2).strip()

    def test(self, name, binary, names, cap, extra_env):
        tmp, work = self.root/(name+'-tmp'), self.root/(name+'-work')
        tmp.mkdir(mode=0o700); work.mkdir(mode=0o700)
        env = dict(self.env, TMPDIR=str(tmp), TMP=str(tmp), TEMP=str(tmp),
                   TEST_TMPDIR=str(tmp), LATTICE_TEST_LOG_PATH=str(self.out/(name+'-native.log')))
        env.update(extra_env)
        xml = self.out/(name+'.xml')
        command = [binary, '--gtest_filter='+':'.join(names), '--gtest_repeat=1',
                   '--gtest_color=no', '--gtest_output=xml:'+str(xml)]
        first = None
        try:
            self.stage(name, command, work, cap, env, xml)
        except BaseException as error:
            first = error
        row = self.rows[-1] if self.rows and self.rows[-1]['stage'] == name else None
        if row and row.get('knownEnding'):
            try:
                parsed = xml_population(xml,names,self.plan['evidenceBounds']['xmlBytes'])
                self.record(name+'-PARSED', parsed)
                if not parsed['accepted'] and first is None:
                    first = RuntimeError(name+' exact XML population/assertions refused')
            except BaseException as error:
                self.record(name+'-XML-ERROR', dict(error=repr(error),
                    primaryStageFailure=repr(first) if first else None))
                if first is None: first = error
            log = self.out/(name+'-native.log')
            if log.exists(): self.upload_file(log, 'raw/'+log.name)
            # This is evidence, never a namespace cleanup or resource-pass rule.
            self.record(name+'-NAMESPACE', dict(tmp=census(tmp),work=census(work)))
        if first is not None: raise first


def census(root, bound=20000):
    rows = []
    def failed(error): raise error
    for directory, dirs, files in os.walk(root,followlinks=False,onerror=failed):
        for name in sorted(dirs+files):
            path = Path(directory)/name; value = path.lstat()
            rows.append(dict(relative=str(path.relative_to(root)),mode=oct(value.st_mode),
                dev=value.st_dev,ino=value.st_ino,uid=value.st_uid,bytes=value.st_size))
            require(len(rows) <= bound, 'supervisor namespace evidence bound')
    return rows


def tracked_manifest(run, source, prefix, expected_sha, expected_tree):
    actual = run.text_stage(prefix+'-IDENTITY',
        ['git','-C',source,'rev-parse','HEAD','HEAD^{tree}'],source).splitlines()
    require(actual == [expected_sha,expected_tree], 'wrong immutable '+prefix+' source')
    dirty = run.text_stage(prefix+'-STATUS',
        ['git','-C',source,'status','--porcelain','--untracked-files=all'],source)
    require(not dirty, 'dirty source: '+prefix)
    run.stage(prefix+'-TRACKED',['git','-C',source,'ls-files','-z'],source,30)
    body = (run.out/(prefix+'-TRACKED.stdout')).read_bytes()
    names = sorted(os.fsdecode(x) for x in body.split(b'\0') if x)
    require(names and len(names) == len(set(names)), 'tracked list empty/duplicate')
    facts = manifest_files(run,source,names)
    run.record(prefix+'-MANIFEST',dict(commit=expected_sha,tree=expected_tree,
                                      names=names,files=facts))
    return facts


def manifest_files(run, source, names):
    rows = []
    for name in names:
        path = source/name
        require(not Path(name).is_absolute() and '..' not in Path(name).parts
                and path.resolve().is_relative_to(source), 'tracked path escapes source')
        rows.append(dict(relative=name, **run.stable(path)))
    return rows


def old_contract(run, old, plan, old_manifest):
    for name,pin in plan['old']['metadata'].items():
        fact = run.stable(old/name)
        require((fact['bytes'],fact['sha256']) == (pin['bytes'],pin['sha256']),
                'wrong published old metadata: '+name)
    for name in plan['old']['metadata']:
        run.upload_file(old/name,'old-source-metadata/'+name)
    mapping = json.loads(bounded_text(old/'SOURCE-MAP.json',4*1024**2))
    expected = {x['relativePath']:x for x in mapping['sourceFiles']}
    actual = {x['relative']:x for x in old_manifest}
    require(len(expected)==560 and set(actual)==set(expected)|set(plan['old']['metadata'])
            and len(actual)==563, 'old canonical563-file recipe mismatch')
    for relative,row in expected.items():
        require((actual[relative]['bytes'],actual[relative]['sha256']) ==
                (row['source']['bytes'],row['source']['sha256']), 'old body mismatch: '+relative)
        require(actual[relative]['mode']=='0o644', 'old materialized logical mode mismatch')
    require(len(mapping['units'])==241 and len(mapping['testSourceOrder'])==148
            and not mapping['objectReuseApproved'], 'old complete source closure required')
    expected_names = json.loads(bounded_text(old/'EXPECTED-HELPER-NAMES.json',4*1024**2))['names']
    require(len(expected_names)==2805 and len(set(expected_names))==2805
            and plan['helperCase'] in expected_names, 'old exact inventory metadata')
    return mapping,expected_names


def artifact_set(run, build):
    names = ['LatticeCoreTests','LatticeCAPITests','libLatticeCAPI.0.1.0.dylib']
    require(all((build/x).is_file() for x in names), 'normal all-target products missing')
    extra = ['LatticeObserverDeliveryBridgeTests','LatticeInstallationLauncher']
    names += [x for x in extra if (build/x).is_file()]
    return {x:run.stable(build/x) for x in names}


def flags_for(plan, target, relative):
    if relative.endswith('RecoveryProducerContinuityTests.cpp') and target=='LatticeCoreTests':
        return sorted(plan['exactHelperFlags'])
    families = plan['originalFlags'][target]
    if target == 'LatticeCoreTests':
        family = families[1] if relative.endswith('.c') else families[0]
    else:
        require(len(families)==1, 'ambiguous old flags')
        family=families[0]
    return sorted(family['flags'])


def compile_closure(run, source, other_source, build, label, cc, cxx, sdk, mapping=None):
    commands_path = build/'compile_commands.json'
    commands = json.loads(bounded_text(commands_path,16*1024**2))
    require(commands, 'empty actual compilation database')
    run.upload_file(commands_path,'build-proof/'+label+'/compile_commands.json')
    verbose = bounded_text(run.out/(label+'-BUILD.stdout'),256*1024**2).splitlines()
    observed = Counter()
    for line in verbose:
        try: tokens=shlex.split(line,posix=True)
        except ValueError: continue
        for index,word in enumerate(tokens):
            if word in (str(cc),str(cxx)) and '-c' in tokens[index:]:
                observed[tuple(tokens[index:])] += 1
                break
    expected = {(u['target'],u['source']):u for u in mapping['units']} if mapping else None
    if expected is not None: require(len(expected)==241 and len(commands)==241,'old241 recipes required')
    units, dependencies, index_by_path, seen_outputs, seen_units = [], [], {}, set(), set()
    for command in commands:
        run.time_check()
        argv=argument_vector(command);directory=Path(command['directory']).resolve()
        require(directory.is_relative_to(build), 'compile cwd outside fresh build')
        require(argv[0] in (str(cc),str(cxx)), 'compiler substitution/launcher')
        actual_source=Path(command['file'])
        if not actual_source.is_absolute(): actual_source=directory/actual_source
        actual_source=actual_source.resolve()
        require(actual_source.is_relative_to(source), 'foreign/current source in '+label)
        relative=str(actual_source.relative_to(source))
        output=Path(argv[argv.index('-o')+1]); output=output if output.is_absolute() else directory/output
        output=output.resolve()
        require(output.is_relative_to(build) and output not in seen_outputs,'object reuse/escape')
        seen_outputs.add(output)
        target=output.relative_to(build).parts[1].removesuffix('.dir')
        origin_target='LatticeCoreTests' if mapping and target=='LatticeCountShortcutSeedHelper' else target
        key=(origin_target,relative)
        require(key not in seen_units, 'duplicate complete TU'); seen_units.add(key)
        require(observed[tuple(argv)]==1, 'actual verbose codegen recipe not observed exactly once: '+relative)
        require('-arch' in argv and argv[argv.index('-arch')+1]=='arm64'
                and '-isysroot' in argv and Path(argv[argv.index('-isysroot')+1]).resolve()==sdk,
                'compiler architecture/SDK mismatch')
        if mapping:
            require(key in expected and semantic_flags(argv)==flags_for(run.plan,origin_target,relative),
                    'old unit or original effective flags mismatch: '+relative)
        dpath=Path(str(output)+'.d');d_fact=run.stable(dpath)
        run.upload_file(dpath,'build-proof/'+label+'/'+str(dpath.relative_to(build)))
        spellings=dependency_names(bounded_text(dpath,4*1024**2))
        dep_indices,project=set(),set()
        for spelling in spellings:
            dep=Path(spelling);dep=dep if dep.is_absolute() else directory/dep;dep=dep.resolve()
            require(not dep.is_relative_to(other_source), 'mixed old/current dependency')
            if dep.is_relative_to(source): project.add(str(dep.relative_to(source)))
            if dep.is_relative_to(Path.home()/'localdev'):
                require(dep.is_relative_to(source) or dep.is_relative_to(build),'foreign localdev dependency')
            if str(dep) not in index_by_path:
                fact=run.stable(dep)
                prior=run.shared_dependencies.get(str(dep))
                require(prior is None or prior==fact, 'shared toolchain/dependency drift between builds')
                run.shared_dependencies[str(dep)]=fact
                index_by_path[str(dep)]=len(dependencies); dependencies.append(fact)
            dep_indices.add(index_by_path[str(dep)])
        require(relative in project, 'actual .d omitted its source')
        if mapping:
            wanted=(mapping['helperProjectDependencies'] if relative.endswith('RecoveryProducerContinuityTests.cpp')
                    and origin_target=='LatticeCoreTests' else expected[key]['oldProjectDependencies'])
            require(project==set(wanted), 'actual old project dependency set mismatch: '+relative)
        units.append(dict(target=target,originalTarget=origin_target,source=relative,
            generatedCompileRecipe=argv,actualVerboseCommandObserved=True,cwd=str(directory),
            sourceFact=run.stable(actual_source),object=run.stable(output),dependencyFile=d_fact,
            dependencyTokenCount=len(spellings),uniqueDependencyIndices=sorted(dep_indices),
            actualProjectDependencies=sorted(project)))
    require(not mapping or seen_units==set(expected), 'old unit closure incomplete')
    receipt=dict(status='FRESH_BUILD_CLOSURE',units=units,dependencies=dependencies,
        compileCommands=run.stable(commands_path),count=len(units),objectReuse=False,
        actualVerboseCommands=True,systemDependenciesBound=True)
    run.record(label+'-COMPILE-CLOSURE',receipt)
    return units


def link_closure(run, source, build, label, units, cxx, mapping=None):
    by_object={u['object']['path']:u for u in units}
    observed=[]
    for line in bounded_text(run.out/(label+'-BUILD.stdout'),256*1024**2).splitlines():
        try: observed.append(shlex.split(line,posix=True))
        except ValueError: pass
    recipes=[]; archives=[]
    for path in sorted((build/'CMakeFiles').glob('*.dir/link.txt')):
        body=bounded_text(path,4*1024**2)
        commands=[shlex.split(x,posix=True) for x in body.splitlines() if x.strip()]
        require(commands,'empty link recipe')
        run.upload_file(path,'build-proof/'+label+'/'+str(path.relative_to(build)))
        rows=[]
        for argv in commands:
            require(sum(tokens[-len(argv):]==argv for tokens in observed)==1,
                    'actual verbose archive/link command not observed exactly once')
            objects=[];linked_archives=[]
            for value in argv:
                if value.endswith(('.o','.a')):
                    actual=Path(value);actual=actual if actual.is_absolute() else build/actual
                    actual=actual.resolve()
                    require(actual.is_relative_to(build),'foreign link input')
                    if value.endswith('.o'):
                        require(str(actual) in by_object,'unproved link object')
                        objects.append(by_object[str(actual)])
                    else: linked_archives.append(run.stable(actual))
            rows.append(dict(argv=argv,objects=[u['object'] for u in objects],
                objectSources=[u['source'] for u in objects],archives=linked_archives))
        recipes.append(dict(recipe=run.stable(path),commands=rows))
        if (len(commands[0])>=3 and Path(commands[0][0]).name in ('ar','llvm-ar','libtool')
                and any(x.endswith('.a') for x in commands[0])):
            archive_candidates=[x for x in commands[0] if x.endswith('.a')]
            require(len(archive_candidates)==1,'archive recipe ambiguous')
            archive=Path(archive_candidates[0]);archive=archive if archive.is_absolute() else build/archive
            # An executable link can contain archives too; only ar/libtool recipes count.
            if Path(commands[0][0]).name not in ('ar','llvm-ar','libtool'): continue
            archive=archive.resolve();name=archive.name
            listed=run.text_stage(label+'-MEMBERS-'+name,['ar','-t',archive],build,30).splitlines()
            want=[Path(x['path']).name for x in rows[0]['objects']]
            indexes={'__.SYMDEF','__.SYMDEF SORTED','__.SYMDEF_64','__.SYMDEF_64 SORTED'}
            metadata=listed[:1] if listed and listed[0] in indexes else []
            members=listed[len(metadata):]
            require(members==want and members,'actual archive objects/order do not match fresh recipe')
            archives.append(dict(artifact=run.stable(archive),rawTable=listed,
                formatIndexMetadata=metadata,members=members,recipe=run.stable(path)))
    require(recipes and archives,'link/archive closure absent')
    if mapping:
        require({x['artifact']['path'].split('/')[-1] for x in archives} ==
            {'libLatticeCore.a','libLatticeSwiftCppBridge.a','libSqliteVec.a','libgtest.a','libgtest_main.a'},
            'old five original archives required')
        entry=next((x for x in recipes if Path(x['recipe']['path']).parent.name=='LatticeCountShortcutSeedHelper.dir'),None)
        require(entry and len(entry['commands'])==1,'old helper actual link recipe absent')
        command=entry['commands'][0]
        require(command['argv'][0]==str(cxx),'old helper linker substituted')
        require(command['objectSources']==mapping['testSourceOrder'], 'old148 whole test link order changed')
        require([Path(x['path']).name for x in command['archives']]==run.plan['originalArchiveOrder'],
                'original old archive/link order changed')
        require(len(command['objects'])==148,'old model registry TU set incomplete')
    run.record(label+'-LINK-CLOSURE',dict(recipes=recipes,archives=archives,
        allInputsFromFreshActualObjects=True,currentOldMixing=False))


def seed_snapshot(run,container,name,phase):
    # No Native I/O. The exact owned process/group MUST already have ended.
    require(container.is_dir() and not container.is_symlink(),'fresh old seed absent')
    require((container/'store.sqlite').is_file(),'seed MAIN absent')
    rows=census(container,run.plan['evidenceBounds']['namespaceEntries'])
    total=0;files=[]
    for row in rows:
        path=container/row['relative'];s=path.lstat()
        require(s.st_uid==os.getuid() and not stat.S_ISLNK(s.st_mode),'seed ownership/symlink')
        if stat.S_ISDIR(s.st_mode): continue
        require(stat.S_ISREG(s.st_mode) and s.st_nlink==1,'unknown seed artifact')
        total+=s.st_size
        require(total<=run.plan['evidenceBounds']['perSeedSnapshotBytes'],'seed evidence byte bound')
        files.append(run.upload_file(path,'seed-snapshots/'+phase+'/'+name+'/'+row['relative']))
    run.record('SEED-'+name+'-'+phase,dict(container=str(container),namespace=rows,
        files=files,bytes=total,processEndingPrecedesSnapshot=True,
        copiedHistoricalSnapshotNotUsedForParents=True))


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--old-source',type=Path,required=True)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--expected-sha',required=True)
    parser.add_argument('--expected-tree',required=True)
    args=parser.parse_args()
    require(all(re.fullmatch('[0-9a-f]{40}',x) for x in (args.expected_sha,args.expected_tree)),
            'exact current commit/tree required')
    require(platform.system()=='Darwin' and platform.machine()=='arm64'
            and os.environ.get('RUNNER_ENVIRONMENT')=='github-hosted','actual hosted macOS/arm64 required')
    localdev=(Path.home()/'localdev').resolve()
    source,old,root=(x.resolve() for x in (args.source,args.old_source,args.root))
    require(all(x.is_relative_to(localdev) for x in (source,old,root)), 'all work under actual HOME/localdev')
    require(not root.exists() and root.parent.is_dir() and source.is_dir() and old.is_dir(), 'fresh owned root required')
    require(not any(a.is_relative_to(b) for a,b in ((source,old),(old,source),(root,source),
        (root,old),(source,root),(old,root))), 'source/output roots must be separate')
    plan_path=Path(__file__).with_name('recovery_coherent_count_shortcut_plan.json')
    require(file_fact(plan_path)['sha256']==PLAN_SHA256,'qualification plan changed')
    plan=json.loads(plan_path.read_text())
    os.umask(0o077) # Future build/tmp/evidence outputs remain in private owned scopes.
    run=Run(root,plan)
    before_current=before_old=artifacts_before=None
    complete=False
    try:
        require(os.statvfs(root).f_bavail*os.statvfs(root).f_frsize >=plan['resources']['initialFreeBytes'],
                'initial17GiB disk admission')
        run.record('PLAN-FILE',file_fact(plan_path))
        run.upload_file(plan_path,'source/recovery_coherent_count_shortcut_plan.json')
        before_current=tracked_manifest(run,source,'CURRENT-SOURCE',args.expected_sha,args.expected_tree)
        before_old=tracked_manifest(run,old,'OLD-SOURCE',plan['old']['commit'],plan['old']['tree'])
        mapping,old_names=old_contract(run,old,plan,before_old)
        parent_path=source/plan['currentParentTU']['relative'];fact=run.stable(parent_path)
        require((fact['bytes'],fact['sha256'])==(plan['currentParentTU']['bytes'],plan['currentParentTU']['sha256']),
                'current original parent wholeTU changed; explicit source review required')
        cc=Path(run.text_stage('FIND-CC',['xcrun','--find','clang'],source))
        cxx=Path(run.text_stage('FIND-CXX',['xcrun','--find','clang++'],source))
        require(cc.is_absolute() and cxx.is_absolute(),'absolute installed compiler paths required')
        sdk=Path(run.text_stage('FIND-SDK',['xcrun','--show-sdk-path'],source)).resolve()
        tools=dict(cc=dict(invocationPath=str(cc),body=run.stable(cc.resolve())),
                   cxx=dict(invocationPath=str(cxx),body=run.stable(cxx.resolve())),sdk=str(sdk))
        for name,command in [('CC-VERSION',[cc,'--version']),('CXX-VERSION',[cxx,'--version']),
            ('XCODE-VERSION',['xcodebuild','-version']),('CMAKE-VERSION',['cmake','--version'])]:
            run.stage(name,command,source,30)
        tools['sdkMetadata']=[run.stable(sdk/n) for n in ('SDKSettings.json','SDKSettings.plist') if (sdk/n).is_file()]
        require(tools['sdkMetadata'],'actual SDK metadata absent')
        run.record('SAME-HOST-TOOLS',dict(**tools,system=platform.platform(),machine=platform.machine(),
            actualHome=str(Path.home()),sameHostForBothFreshBuilds=True))
        build,old_build=root/'current-build',root/'old-build'
        require(not build.exists() and not old_build.exists(),'no inherited build outputs')
        common=['-DCMAKE_BUILD_TYPE=Debug','-DCMAKE_EXPORT_COMPILE_COMMANDS=ON',
            '-DCMAKE_OSX_ARCHITECTURES=arm64','-DCMAKE_OSX_DEPLOYMENT_TARGET=14.0',
            '-DCMAKE_C_COMPILER='+str(cc),'-DCMAKE_CXX_COMPILER='+str(cxx),'-DCMAKE_OSX_SYSROOT='+str(sdk)]
        run.stage('CURRENT-CONFIGURE',['cmake','-G','Unix Makefiles','-S',source,'-B',build,*common],
                  source,plan['caps']['currentConfigure'])
        run.stage('CURRENT-BUILD',['cmake','--build',build,'--parallel','2','--verbose','--','-k'],
                  source,plan['caps']['currentBuild'])
        artifacts_before=artifact_set(run,build);run.record('CURRENT-ARTIFACTS-BEFORE',artifacts_before)
        current_units=compile_closure(run,source,old,build,'CURRENT',cc,cxx,sdk)
        link_closure(run,source,build,'CURRENT',current_units,cxx)
        run.stage('OLD-CONFIGURE',['cmake','-G','Unix Makefiles','-S',old,'-B',old_build,*common],
                  old,plan['caps']['oldConfigure'])
        run.stage('OLD-BUILD',['cmake','--build',old_build,'--parallel','2','--verbose',
            '--target',plan['helperTarget'],'--','-k'],old,plan['caps']['oldBuild'])
        old_binary=old_build/plan['helperTarget'];old_binary_before=run.stable(old_binary)
        old_units=compile_closure(run,old,source,old_build,'OLD',cc,cxx,sdk,mapping)
        link_closure(run,old,old_build,'OLD',old_units,cxx,mapping)
        run.stage('OLD-INVENTORY',[old_binary,'--gtest_list_tests','--gtest_color=no'],old_build,
                  plan['caps']['oldInventory'])
        actual_old=inventory(bounded_text(run.out/'OLD-INVENTORY.stdout',plan['evidenceBounds']['inventoryBytes']))
        require(actual_old==old_names,'actual2805 old whole-image inventory changed')
        current_binary=build/'LatticeCoreTests'
        run.stage('CURRENT-INVENTORY',[current_binary,'--gtest_list_tests','--gtest_color=no'],build,
                  plan['caps']['currentInventory'])
        actual_current=inventory(bounded_text(run.out/'CURRENT-INVENTORY.stdout',plan['evidenceBounds']['inventoryBytes']))
        parents=[x['parent'] for x in plan['seeds']]
        require(all(x in actual_current for x in parents) and plan['helperCase'] not in actual_current,
                'current six original parents absent/old helper define leaked')
        run.record('ACTUAL-INVENTORIES',dict(old=actual_old,current=actual_current,
            oldCount=len(actual_old),currentCount=len(actual_current),sixCurrentParents=parents))
        for row in plan['seeds']:
            container=root/'seeds'/(row['name']+'.lattice-continuous')
            require(not container.exists(),'seed original already exists; no reuse')
            first=None
            try:
                run.test('OLD-SEED-'+row['name'],old_binary,[plan['helperCase']],plan['caps']['seed'],
                    {'LATTICE_COUNT_SHORTCUT_SEED_CONTAINER':str(container)})
            except BaseException as error:
                first=error
            if run.rows and run.rows[-1].get('knownEnding') and container.exists():
                try: seed_snapshot(run,container,row['name'],'before-current')
                except BaseException as error:
                    if first is None: first=error
                    else: run.safety_errors.append('seed snapshot secondary: '+repr(error))
            if first is not None: raise first
        first=None
        try:
            run.test('CURRENT-PARENTS',current_binary,parents,plan['caps']['currentParents'],
                {'LATTICE_COUNT_SHORTCUT_PREDECESSOR_ROOT':str(root/'seeds')})
        except BaseException as error:
            first=error
        if run.rows and run.rows[-1].get('knownEnding'):
            for row in plan['seeds']:
                try: seed_snapshot(run,root/'seeds'/(row['name']+'.lattice-continuous'),row['name'],'after-current')
                except BaseException as error:
                    if first is None: first=error
                    else: run.safety_errors.append('post-parent snapshot secondary: '+repr(error))
        if first is not None: raise first
        require(run.stable(cc.resolve())==tools['cc']['body']
                and run.stable(cxx.resolve())==tools['cxx']['body'],'compiler body drift')
        require(all(run.stable(Path(x['path']))==x for x in tools['sdkMetadata']), 'SDK metadata drift')
        require(run.stable(old_binary)==old_binary_before,'old helper binary drift')
        require(artifact_set(run,build)==artifacts_before,'current built artifact drift')
        complete=True
    except BaseException as error:
        run.errors.append(repr(error))
    finally:
        all_known=all(x.get('knownEnding') or x.get('noChildIssued') for x in run.rows)
        # No process/Native commands follow a failure. Source-body checks are
        # DATA-only and only safe when every issued owned group is gone.
        if all_known:
            for label,path,before in [('CURRENT',source,before_current),('OLD',old,before_old)]:
                if before is None: continue
                try:
                    after=manifest_files(run,path,[x['relative'] for x in before])
                    run.record(label+'-SOURCE-AFTER',after)
                    require(after==before,label+' source body/mode drift')
                except BaseException as error: run.safety_errors.append(repr(error))
        else:
            run.safety_errors.append('unknown ending: no source/namespace/raw-output hash or copy')
        accepted=bool(complete and not run.errors and not run.safety_errors
                      and all_known and all(natural(x) for x in run.rows))
        run.result.update(status='COUNT_COMPONENT_PASS' if accepted else 'REFUSED',
            componentQualified=accepted,errors=run.errors,secondaryEvidenceOrEndingDebt=run.safety_errors,
            stages=run.rows,allIssuedEndingsKnown=all_known,elapsedSeconds=time.monotonic()-run.start,
            originalOwnerEndUnchanged=run.end,originalWorkEndUnchanged=run.work_end,
            noCleanupPerformed=True,rawUnknownEndingArtifactsExcludedFromUpload=True,
            sixOldSeedsAndSixParentsAccepted=accepted,
            knownPriorFailuresNotRelabeled=True)
        run.record('RESULT',run.result)
        print(json.dumps(run.result),flush=True)
    return 0 if run.result['componentQualified'] else 1


if __name__=='__main__':
    raise SystemExit(main())
