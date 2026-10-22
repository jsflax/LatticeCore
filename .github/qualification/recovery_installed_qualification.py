"""Root-reviewed explicit hosted installed scopes; no ordinary/full-goal acceptance."""
from pathlib import Path, PurePosixPath
import argparse, ast, gzip, hashlib, json, os, platform, pwd, re, resource, shutil, signal, stat, subprocess, sys, tarfile, time
import xml.etree.ElementTree as ET

REFERENCE_SHA = '6433e0f58f4e874b2d68e807b2230a7f9bf482bddfa0ee803cf5f5002fb86179'
REFERENCE_BYTES = 875638
SOURCE_MEMBERS = 2002
SOURCE_BYTES = 45290714
HELPER_SHA = 'ce223e15abc09a0b856e2d537d243a0e3cd0fae74022a3a12abfbeeebc48f478'
LIMITS_SHA = '8736ec6c2f3653369170ac9d3a770497ec8d8ae21aabcc975d2b41c5f565958f'
GIB = 1024**3


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2)+'\n')


def file_fact(path):
    path = Path(path)
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode): raise RuntimeError('actual regular evidence/source/image required: '+str(path))
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024*1024), b''): h.update(block)
    after = path.lstat()
    if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_mode)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_mode):
        raise RuntimeError('body/mode/identity changed while observed')
    return dict(path=str(path),bytes=after.st_size,sha256=h.hexdigest(),mode=oct(stat.S_IMODE(after.st_mode)),device=after.st_dev,inode=after.st_ino)


def require_fact(expected):
    actual=file_fact(expected['path'])
    if actual!=expected: raise RuntimeError('retained actual file changed: '+expected['path'])
    return actual


def secure_directory(path, private=False):
    path=Path(path)
    if not path.is_absolute() or path!=Path(os.path.normpath(path)):raise RuntimeError('normalized absolute directory required')
    current=Path('/')
    for component in path.parts[1:]:
        current/=component;s=current.lstat()
        if not stat.S_ISDIR(s.st_mode) or stat.S_ISLNK(s.st_mode) or s.st_uid not in (0,os.geteuid()) or s.st_mode&0o022:
            raise RuntimeError('actual directory ownership/no-follow/mode differs: '+str(current))
    if private and (s.st_uid!=os.geteuid() or stat.S_IMODE(s.st_mode)!=0o700):raise RuntimeError('private current-user directory required')


def decode_bundle(bundle, project, evidence):
    if file_fact(bundle)['bytes']>64*1024*1024:raise RuntimeError('source bundle compressed bound exceeded')
    source=project/'source';source.mkdir(mode=0o700)
    unpacked=project/'source-input.tar';total=0
    with gzip.open(bundle,'rb')as compressed,unpacked.open('xb')as output:
        os.fchmod(output.fileno(),0o600)
        while True:
            block=compressed.read(1024*1024)
            if not block:break
            total+=len(block)
            if total>64*1024*1024:raise RuntimeError('source archive expanded bound exceeded')
            output.write(block)
    with tarfile.open(unpacked,'r:') as archive:
        members=archive.getmembers()
        if len(members)!=SOURCE_MEMBERS+1 or len({m.name for m in members})!=len(members):raise RuntimeError('exact source archive population differs')
        by_name={m.name:m for m in members};ref=by_name.get('REFERENCE.json')
        if ref is None or not ref.isfile() or ref.size!=REFERENCE_BYTES:raise RuntimeError('exact source reference missing')
        raw=archive.extractfile(ref).read(REFERENCE_BYTES+1)
        if hashlib.sha256(raw).hexdigest()!=REFERENCE_SHA:raise RuntimeError('pinned full coherent source reference differs')
        (evidence/'REFERENCE.json').write_bytes(raw);rows=json.loads(raw)
        if len(rows)!=SOURCE_MEMBERS or len({r['logicalPath']for r in rows})!=SOURCE_MEMBERS or sum(r['bytes']for r in rows)!=SOURCE_BYTES:raise RuntimeError('full source reference population differs')
        expected={'REFERENCE.json'}
        for row in rows:
            logical=PurePosixPath(row['logicalPath'])
            if logical.is_absolute() or '..' in logical.parts or not logical.parts or logical.parts[0]not in ('Core','SDK') or str(logical)!=row['logicalPath']:
                raise RuntimeError('logical source member escapes closed Core/SDK image')
            name='source/'+str(logical);expected.add(name);member=by_name.get(name)
            mode=int(row['mode'],8)
            if mode not in (0o444,0o600,0o644,0o755) or member is None or not member.isfile() or member.linkname or member.size!=row['bytes'] or member.mode!=mode:
                raise RuntimeError('exact regular source member/body/mode missing: '+str(logical))
            target=source/str(logical);target.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
            h=hashlib.sha256();remaining=row['bytes'];stream=archive.extractfile(member)
            with target.open('xb')as output:
                while remaining:
                    block=stream.read(min(1024*1024,remaining))
                    if not block:raise RuntimeError('source member truncated')
                    output.write(block);h.update(block);remaining-=len(block)
                if stream.read(1):raise RuntimeError('source member overrun')
            if h.hexdigest()!=row['sha256']:raise RuntimeError('source member body differs: '+str(logical))
            target.chmod(mode)
        if set(by_name)!=expected:raise RuntimeError('source archive extra/missing member')
    return rows


def snapshot_source(rows, project):
    root=project/'source';actual=set()
    for path in root.rglob('*'):
        if path.is_symlink():raise RuntimeError('source census contains symlink')
        if path.is_file():actual.add(str(path.relative_to(root)))
    if actual!={r['logicalPath']for r in rows}:raise RuntimeError('whole actual source census changed')
    facts=[]
    for row in rows:
        f=file_fact(root/row['logicalPath'])
        if (f['bytes'],f['sha256'],f['mode'])!=(row['bytes'],row['sha256'],row['mode']):raise RuntimeError('exact source row differs: '+row['logicalPath'])
        facts.append(f)
    return facts

def group_state(pgid):
    try:
        os.killpg(pgid, 0)
        return dict(absent=False, error=None)
    except ProcessLookupError:
        return dict(absent=True, error=None)
    except OSError as error:
        return dict(absent=None, error=dict(errno=error.errno, message=str(error)))


def run_stage(out, name, command, work, env, cap, floor, growth, log_ceiling, purpose):
    started = time.monotonic()
    row = dict(stage=name, purpose=purpose, command=command, cwd=str(work), capSeconds=cap, signals=[], signalErrors=[],
               stopReason=None, nativeTestBodiesIssued=False, nativeDiscoveryIssued=False,
               globalDescendantEndingNotClaimed=True, leaderReaped=False, processGroupAbsent=None)
    process = None
    disk = os.statvfs(out)
    first_free = disk.f_bavail * disk.f_frsize
    row['minimumFreeBytes'] = first_free
    stdout_path, stderr_path = out/(name+'.stdout'), out/(name+'.stderr')
    with stdout_path.open('xb') as stdout, stderr_path.open('xb') as stderr:
        try:
            if first_free < floor:
                raise RuntimeError('disk admission floor before original child launch')
            process = subprocess.Popen(command, cwd=work, env=env, stdout=stdout, stderr=stderr, start_new_session=True)
            row.update(pid=process.pid, nativeTestBodiesIssued=(purpose in ('installed-role','data-tests')),
                       nativeDiscoveryIssued=('--gtest_list_tests' in command),
                       nativeDiscoveryMayBeIssuedByBuild=(purpose=='build'))
            row.update(pgid=os.getpgid(process.pid),sid=os.getsid(process.pid))
            row['actualOriginalSessionGroup'] = row['pid'] == row['pgid'] == row['sid']
            if not row['actualOriginalSessionGroup']:
                raise RuntimeError('actual original session/group identity differs')
            write_json(out/'CURRENT.json', row)
            while process.poll() is None:
                disk = os.statvfs(out)
                free = disk.f_bavail * disk.f_frsize
                row['minimumFreeBytes'] = min(row['minimumFreeBytes'], free)
                if free < floor:
                    row['stopReason'] = 'disk floor'
                    break
                if first_free-free > growth:
                    row['stopReason'] = 'admitted free-space growth ceiling'
                    break
                if stdout_path.stat().st_size+stderr_path.stat().st_size > log_ceiling:
                    row['stopReason'] = 'retained log growth ceiling'
                    break
                if time.monotonic()-started >= cap:
                    row['stopReason'] = 'original stage deadline'
                    break
                time.sleep(0.25)
        except BaseException as error:
            row['stopReason'] = row['stopReason'] or repr(error)
        finally:
            if process is not None:
                # Match the proven supplied run_stage custody semantics:
                # returncode None means the actual child is still unreaped,
                # reserving its PID even if it exits just before this signal.
                # Once wait/poll reaps it, no further group signal is allowed.
                if row['stopReason'] and row.get('actualOriginalSessionGroup') and process.returncode is None:
                    for sig in (signal.SIGTERM,signal.SIGKILL):
                        if process.returncode is not None:
                            break
                        try:
                            os.killpg(process.pid, sig)
                            row['signals'].append(int(sig))
                        except OSError as error:
                            row['signalErrors'].append(dict(signal=int(sig), errno=error.errno, message=str(error)))
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            pass
                try:
                    row['exitCode'] = process.wait(timeout=5)
                    row['leaderReaped'] = True
                except subprocess.TimeoutExpired:
                    row['exitCode'] = None
                if row['leaderReaped'] and row.get('actualOriginalSessionGroup'):
                    end = time.monotonic()+5
                    state = group_state(row['pgid'])
                    while state['absent'] is False and time.monotonic() < end:
                        time.sleep(0.05)
                        state = group_state(row['pgid'])
                    row['processGroupAbsent'], row['groupQueryError'] = state['absent'], state['error']
                    if state['absent'] is not True:
                        row['stopReason'] = row['stopReason'] or 'original group ending unproved; no post-reap signal'
            else:
                row.update(exitCode=None, noChildIssued=True)
    row['knownEnding'] = bool(row['leaderReaped'] and row.get('actualOriginalSessionGroup') and row['processGroupAbsent'] is True)
    if row['knownEnding'] or process is None:
        disk = os.statvfs(out)
        free = disk.f_bavail * disk.f_frsize
        row['minimumFreeBytes'] = min(row['minimumFreeBytes'],free)
        row['finalFreeBytes'] = free
        if free < floor or first_free-free > growth or stdout_path.stat().st_size+stderr_path.stat().st_size > log_ceiling:
            row['stopReason'] = row['stopReason'] or 'actual final disk/log admission exceeded'
    row['elapsedSeconds'] = time.monotonic()-started
    row['stageSucceeded'] = bool(row['knownEnding'] and row['exitCode'] == 0 and not row['stopReason'])
    row['nativeOwnedOriginalEndingsUnproved'] = process is not None and purpose in ('real-installer','installed-role') and not row['stageSucceeded']
    # A live/unknown group may still own output. Preserve paths without reading,
    # hashing or parsing it; Root must settle that exact original before reuse.
    if (row['knownEnding'] and not row['nativeOwnedOriginalEndingsUnproved']) or process is None:
        row['stdout'], row['stderr'] = file_fact(stdout_path), file_fact(stderr_path)
    write_json(out/(name+'.json'), row)
    return row


def parse_inventory(path):
    if Path(path).stat().st_size > 8*1024*1024:
        raise RuntimeError('complete actual inventory exceeds admitted parser storage; raw output retained')
    cases, suite = [], None
    for raw in Path(path).read_text().splitlines():
        line = raw.split('#', 1)[0].rstrip()
        if not line or line.startswith('Running main() from '):
            continue
        if line.startswith('Note:'):
            raise RuntimeError('unexpected filtered/sharded discovery note')
        if line[:1].isspace():
            name = line.strip()
            if suite is None or not name or re.search(r'\s', name):
                raise RuntimeError('noncanonical runtime test inventory')
            cases.append(dict(suite=suite, test=name, name=suite+'.'+name))
        elif line.endswith('.'):
            suite = line[:-1]
        else:
            raise RuntimeError('unexpected runtime inventory text')
    if not cases or len({r['name'] for r in cases}) != len(cases):
        raise RuntimeError('empty or duplicate runtime test inventory')
    for row in cases:
        row['disabled'] = any(part.startswith('DISABLED_') or '/DISABLED_' in part for part in (row['suite'], row['test']))
    return cases


def check_xml(path, expected):
    result = dict(present=path.exists(), accepted=False, expectedEnabled=len(expected))
    if not result['present']:
        return result
    result['file'] = file_fact(path)
    if result['file']['bytes'] > 64*1024*1024:
        result['parseRefusal'] = 'complete XML exceeds admitted parser storage; full file retained'
        return result
    try:
        tree = ET.parse(path)
        cases = []
        for suite in tree.getroot().iter('testsuite'):
            for case in suite.findall('testcase'):
                name = case.attrib.get('classname', suite.attrib.get('name', ''))+'.'+case.attrib['name']
                cases.append(dict(name=name, status=case.attrib.get('status'), result=case.attrib.get('result'),
                    failed=bool(case.findall('failure') or case.findall('error')), skipped=bool(case.findall('skipped'))))
        ran = [c for c in cases if c['status'] != 'notrun']
        result.update(totalXMLCases=len(cases), ran=len(ran), failed=sum(c['failed'] for c in ran),
                      skipped=sum(c['skipped'] or c['result']=='skipped' for c in ran),
                      notRun=sum(c['status']=='notrun' for c in cases),
                      actualNames=[c['name'] for c in ran], allXMLNames=[c['name'] for c in cases])
        result['accepted'] = bool(expected) and len(cases) == len({c['name'] for c in cases}) and set(result['actualNames']) == set(expected) and not result['failed'] and not result['skipped'] and all(c['status'] in ('run','notrun') for c in cases) and all(c['result']=='completed' for c in ran) and all(int(tree.getroot().attrib.get(key,'0'))==0 for key in ('failures','errors'))
    except BaseException as error:
        result['parseError'] = repr(error)
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project-root',type=Path,required=True)
    parser.add_argument('--batch',choices=['canonical-a','canonical-b','journal','nonempty'],required=True)
    args=parser.parse_args();project=args.project_root
    home=Path(pwd.getpwuid(os.geteuid()).pw_dir);localdev=home/'localdev'
    if not project.is_relative_to(localdev) or project==localdev:raise RuntimeError('project must be below actual pwd-home/localdev')
    secure_directory(home);secure_directory(localdev);secure_directory(project,True)
    evidence=project/'evidence';secure_directory(evidence,True)
    started=time.monotonic();global_end=started+7800
    run=dict(status='PREPARING',batch=args.batch,stages=[],failures=[],actualUid=os.geteuid(),actualHome=str(home),
             projectRoot=str(project),hostSystem=platform.system(),hostMachine=platform.machine(),
             hostMacOS=platform.mac_ver()[0],hostImageOS=os.environ.get('ImageOS'),hostImageVersion=os.environ.get('ImageVersion'),
             recipe=file_fact(Path(__file__)),sourceReferenceSHA256=REFERENCE_SHA,sourceMemberCount=SOURCE_MEMBERS,
             sourceAuthority='EXACT_SOURCE009_EXPERIMENTAL_IMAGE_NOT_A_RELEASE',requestedInstalledScopeAccepted=False,
             ordinaryFullCoreAccepted=False,fullRecoveryAccepted=False,performanceAccepted=False,
             globalDescendantEndingNotClaimed=True,observerLimitsAreFixedQualificationDeclaration=True)
    rows=None;before_source=None;before_images={};immutable=[];active=None;unknown=False;scope_passed=False;data_scope_passed=False
    resource.setrlimit(resource.RLIMIT_CORE,(0,0));sys.dont_write_bytecode=True
    env=dict(os.environ)
    for key in list(env):
        if key.startswith('GTEST_') or key in ('GH_TOKEN','GITHUB_TOKEN'):env.pop(key,None)
    for name in ('tmp','clang-modules','cache','data-config'):
        path=project/name;path.mkdir(mode=0o700,exist_ok=True);secure_directory(path,True)
    env.update(TMPDIR=str(project/'tmp'),TMP=str(project/'tmp'),TEMP=str(project/'tmp'),TEMPDIR=str(project/'tmp'),
               SQLITE_TMPDIR=str(project/'tmp'),CLANG_MODULE_CACHE_PATH=str(project/'clang-modules'),
               XDG_CACHE_HOME=str(project/'cache'),PYTHONDONTWRITEBYTECODE='1',
               LATTICE_TEST_LOG_PATH=str(evidence/'direct-discovery.log'))
    def stage(name,command,cap,purpose,work=None):
        nonlocal unknown
        remaining=global_end-time.monotonic()
        if remaining<=0:raise RuntimeError('global original admission deadline before '+name)
        row=run_stage(evidence,name,command,project if work is None else work,env,
                      min(cap,remaining),12*GIB,16*GIB,GIB,purpose)
        run['stages'].append(row);write_json(evidence/'RUN-PROGRESS.json',run)
        if not row['knownEnding'] and not row.get('noChildIssued'):
            unknown=True;raise RuntimeError('actual original ending unknown: '+name)
        if not row['stageSucceeded']:
            if row['nativeOwnedOriginalEndingsUnproved']:
                # An outer nonzero ending does not certify every Native-owned
                # child/retirement original. Retain raw diagnostics and stop.
                unknown=True
            raise RuntimeError('actual required stage failed: '+name)
        return row
    def output_path(name):
        raw=(evidence/(name+'.stdout')).read_text().strip()
        if not raw or len(raw)>4096 or '\n'in raw or '\r'in raw or '\0'in raw or not Path(raw).is_absolute():
            raise RuntimeError('bounded actual tool path missing: '+name)
        return raw
    def discover(name,command,expected):
        stage(name,command+['--gtest_list_tests','--gtest_color=no'],60,'native-discovery')
        population=parse_inventory(evidence/(name+'.stdout'))
        enabled={row['name']for row in population if not row['disabled']}
        if not set(expected)<=enabled:raise RuntimeError('requested enabled test absent/disabled in actual image')
        write_json(evidence/(name+'-POPULATION.json'),population)
        return dict(total=len(population),enabled=len(enabled),requested=expected)
    try:
        if platform.system()!='Darwin' or platform.machine()not in ('arm64','x86_64'):raise RuntimeError('genuine native Darwin host required')
        bootstrap=json.loads((project/'BOOTSTRAP.json').read_bytes())
        if bootstrap['actualHome']!=str(home) or bootstrap['actualUid']!=os.geteuid() or bootstrap['projectRoot']!=str(project) or bootstrap['batch']!=args.batch:
            raise RuntimeError('actual bootstrap account/project/batch differs')
        run['bootstrap']=file_fact(project/'BOOTSTRAP.json');immutable.append(run['bootstrap'])
        run['workflowInvocation']={key:bootstrap[key]for key in ('repository','commit','runId','runAttempt','job','batch')}
        helper=project/'control/stage_installed_fixture.py';limits=project/'control/observer-limits.json';bundle=project/'source009.tar.gz'
        for path,expected in ((helper,HELPER_SHA),(limits,LIMITS_SHA),(bundle,bootstrap['sourceBundleSHA256'])):
            f=file_fact(path)
            if f['sha256']!=expected:raise RuntimeError('committed control/source input differs')
            immutable.append(f)
        run['inputFiles']=immutable.copy();run['observerLimits']=json.loads(limits.read_bytes())
        rows=decode_bundle(bundle,project,evidence);before_source=snapshot_source(rows,project)
        write_json(evidence/'SOURCE-BEFORE.json',before_source)
        run['completeSourceBefore']=file_fact(evidence/'SOURCE-BEFORE.json');immutable.append(run['completeSourceBefore'])
        for name,command in [('TOOLCHAIN-DEVELOPER',['/usr/bin/xcode-select','-p']),
                             ('TOOLCHAIN-CLANG',['/usr/bin/xcrun','--find','clang']),
                             ('TOOLCHAIN-CLANGXX',['/usr/bin/xcrun','--find','clang++']),
                             ('TOOLCHAIN-SDK',['/usr/bin/xcrun','--sdk','macosx','--show-sdk-path'])]:
            stage(name,command,30,'host-probe')
        clang=output_path('TOOLCHAIN-CLANG');clangxx=output_path('TOOLCHAIN-CLANGXX');sdk=output_path('TOOLCHAIN-SDK')
        cmake=shutil.which('cmake')
        if not cmake or not Path(cmake).is_absolute():raise RuntimeError('actual CMake prerequisite missing')
        stage('TOOLCHAIN-CMAKE-VERSION',[cmake,'--version'],30,'host-probe')
        stage('TOOLCHAIN-CLANG-VERSION',[clang,'--version'],30,'host-probe')
        run['toolchain']=dict(cmake=cmake,cmakeTarget=file_fact(Path(cmake).resolve()),clang=clang,clangxx=clangxx,sdk=sdk,
                              clangTarget=file_fact(Path(clang).resolve()),clangxxTarget=file_fact(Path(clangxx).resolve()),
                              nativeArchitecture=platform.machine(),deploymentTarget='14.0')
        immutable.extend(run['toolchain'][key]for key in ('cmakeTarget','clangTarget','clangxxTarget'))
        build=project/'build';build_started=time.monotonic()
        configure=[cmake,'-G','Unix Makefiles','-S',str(project/'source/Core'),'-B',str(build),'-DCMAKE_BUILD_TYPE=Debug',
                   '-DCMAKE_EXPORT_COMPILE_COMMANDS=ON','-DCMAKE_OSX_ARCHITECTURES='+platform.machine(),'-DCMAKE_OSX_DEPLOYMENT_TARGET=14.0',
                   '-DCMAKE_C_COMPILER='+clang,'-DCMAKE_CXX_COMPILER='+clangxx,'-DCMAKE_OSX_SYSROOT='+sdk,
                   '-DLATTICE_RESOURCE_DURABLE_INITIALIZATION_TESTING=ON','-DLATTICE_RESOURCE_INSTALLED_GTEST_HOST=ON',
                   '-DLATTICE_RESOURCE_CONTINUOUS_FINAL_NONEMPTY_TESTING=ON']
        production=[cmake,'--build',str(build),'--target','LatticeCore','LatticeSwiftCppBridge','LatticeCAPI','SqliteVec','LatticeSQLiteProvider','--parallel','2','--','-k']
        normal_all=[cmake,'--build',str(build),'--parallel','2','--','-k']
        run['buildRecipe']=dict(configure=configure,productionPrerequisite=production,mandatoryNormalALL=normal_all,
                                freshBuildTree=True,cumulativeBuildCapSeconds=7200,globalCapSeconds=7800,normalALLCannotBeReplaced=True)
        write_json(evidence/'BUILD-RECIPE.json',run['buildRecipe'])
        for name,command in [('CONFIGURE',configure),('BUILD-PRODUCTION-PREREQUISITE',production),('BUILD-NORMAL-ALL',normal_all)]:
            remaining=7200-(time.monotonic()-build_started)
            if remaining<=0:raise RuntimeError('fixed cumulative build deadline exhausted')
            stage(name,command,remaining,'build')
        cache=build/'CMakeCache.txt';cache_text=cache.read_text();run['installedCMakeCache']=file_fact(cache);immutable.append(run['installedCMakeCache'])
        for flag in ('LATTICE_RESOURCE_DURABLE_INITIALIZATION_TESTING','LATTICE_RESOURCE_INSTALLED_GTEST_HOST','LATTICE_RESOURCE_CONTINUOUS_FINAL_NONEMPTY_TESTING'):
            if flag+':BOOL=ON'not in cache_text:raise RuntimeError('actual installed profile flag differs: '+flag)
        for original,retained_name in ((cache,'CMakeCache.txt'),(build/'compile_commands.json','compile_commands.json')):
            original_fact=file_fact(original)
            if original_fact['bytes']>16*1024*1024:raise RuntimeError('complete build-input retention bound exceeded')
            retained=evidence/retained_name;retained.write_bytes(original.read_bytes());retained_fact=file_fact(retained)
            require_fact(original_fact)
            if original_fact['sha256']!=retained_fact['sha256']:raise RuntimeError('actual build-input copy differs')
            immutable.extend((original_fact,retained_fact))
        names=['lattice-resource-launcher','LatticeResourceFactoryControls','LatticeDurableInitializationChild','LatticeResourceLegacyWriter']
        if args.batch=='nonempty':names.append('LatticeCoreTests')
        for name in names:
            f=file_fact(build/name)
            if not int(f['mode'],8)&0o111:raise RuntimeError('actual built image not executable')
            before_images[name]=f;immutable.append(f)
        run['actualBuiltImages']=before_images
        pins=project/'data-config/image-pins.json';write_json(pins,{name:f['sha256']for name,f in before_images.items()})
        if hashlib.sha256(helper.read_bytes()).hexdigest()!=HELPER_SHA:raise RuntimeError('fixture helper changed before use')
        tree=ast.parse(helper.read_text());literal={}
        for node in tree.body:
            if isinstance(node,ast.Assign)and len(node.targets)==1 and isinstance(node.targets[0],ast.Name)and node.targets[0].id in ('CANONICAL','NONEMPTY'):
                literal[node.targets[0].id]=ast.literal_eval(node.value)
        if args.batch.startswith('canonical-'):
            selected=literal['CANONICAL'][:3]if args.batch=='canonical-a'else literal['CANONICAL'][3:]
            expected=['InitializationChannel.'+name for name,keys in selected]
            run['actualDiscovery']=discover('CONTROL-DISCOVERY',[str(build/'LatticeResourceFactoryControls')],expected)
        elif args.batch=='nonempty':
            expected=[literal['NONEMPTY']]
            run['actualDiscovery']=discover('CONTROL-DISCOVERY',[str(build/'LatticeCoreTests')],expected)
        else:
            expected=['DurableInitialization.ActualEnrollmentResumesCatalogAckAndOwnedWriteCrashCutsWithoutFreshRelabeling',
                      'DurableInitialization.ActualEnrollmentCommitFileSizeFailureLeavesIntentRetryableWithoutFalseCompletion',
                      'DurableInitialization.ActualEnrollmentMissingAndForgedIntentRefuseBeforeNativeAndPreserveCommittedCatalog',
                      'DurableInitialization.ActualEnrollmentIntentFromAnotherGenuineMemberIsRejectedAsReplay']
            data_expected=['CatalogInitializationRecord.EnrollmentHasDistinctKindsAndBindsActualCatalogAndPolicy',
                           'CatalogInitializationRecord.ReplayedEnrollmentFromDifferentMemberOrInstallationRefuses',
                           'CatalogInitializationRecord.MissingOrNoncanonicalEnrollmentCannotBecomeCatalogFreshness']
            run['actualDataDiscovery']=discover('JOURNAL-DATA-DISCOVERY',[str(build/'LatticeResourceFactoryControls')],data_expected)
            data_xml=evidence/'JOURNAL-DATA.xml'
            stage('JOURNAL-DATA',[str(build/'LatticeResourceFactoryControls'),'--gtest_filter='+':'.join(data_expected),
                  '--gtest_output=xml:'+str(data_xml),'--gtest_color=no','--gtest_repeat=1'],90,'data-tests')
            run['actualDataXML']=check_xml(data_xml,data_expected)
            if not run['actualDataXML']['accepted']:raise RuntimeError('actual journal DATA XML population did not pass completely')
            immutable.append(run['actualDataXML']['file']);data_scope_passed=True
        fixture=project/'fixture'
        base_args=['--fixture-root',str(fixture),'--expected-home',str(home),'--expected-uid',str(os.geteuid()),'--batch',args.batch]
        stage('PACKAGE-DATA',[sys.executable,str(helper),'package']+base_args+['--build-dir',str(build),'--image-pins',str(pins),'--observer-limits',str(limits)],60,'data-tool')
        plan=json.loads((fixture/'PLAN.json').read_bytes());package=fixture/'package';launcher=package/'lattice-resource-launcher'
        run['fixturePlan']=file_fact(fixture/'PLAN.json');immutable.append(run['fixturePlan']);immutable.append(file_fact(package/'resource-package.v1'))
        for image in plan['copiedImages'].values():
            f=file_fact(image['path'])
            if f['sha256']!=image['sha256'] or (f['device'],f['inode'])!=(image['identity']['device'],image['identity']['inode']):raise RuntimeError('actual package image differs')
            immutable.append(f)
        installation=home/'.lattice-resource-installation';active_path=installation/'active.v1'
        if active_path.exists():raise RuntimeError('fresh actual installation required; no reset/retry')
        stage('PREPARE',[str(launcher),'prepare-resource-cold-restart',str(package)],60,'real-installer')
        stage('ACTIVATE',[str(launcher),'activate-staged-resource-cold-restart'],150,'real-installer')
        secure_directory(installation,True);active=file_fact(active_path)
        if active['bytes']>128*1024 or active['mode']!='0o600':raise RuntimeError('actual bounded private activated anchor required')
        (evidence/'ACTIVE-BEFORE.v1').write_bytes(active_path.read_bytes());run['actualActiveBefore']=active
        for name in ('preparation.intent.v1','epoch.v1','restart.v1','retirement.complete.v1'):
            p=installation/name
            if p.exists():
                f=file_fact(p)
                if f['bytes']>128*1024:raise RuntimeError('actual installation evidence exceeds record bound')
                (evidence/('INSTALLER-'+name)).write_bytes(p.read_bytes())
        if args.batch=='journal':
            stage('LOCATOR-DATA',[sys.executable,str(helper),'locators']+base_args+['--active-sha256',active['sha256']],60,'data-tool')
            run['actualLocators']=file_fact(fixture/'LOCATORS.json');immutable.append(run['actualLocators'])
            run['actualDiscovery']=discover('CONTROL-DISCOVERY',[str(launcher),'test-run-installed-controls'],expected)
        role='canonical-controls'if args.batch.startswith('canonical-')else 'journal-parent'if args.batch=='journal'else 'nonempty-control'
        stage('INSTALLED-CONTROLS',[str(launcher),'test-run-installed-role',role],150,'installed-role')
        xml=fixture/'artifacts'/('canonical.xml'if args.batch.startswith('canonical-')else 'journal-parent.xml'if args.batch=='journal'else 'nonempty.xml')
        result=check_xml(xml,expected);run['actualXML']=result
        if 'file'in result:
            raw_xml=xml.read_bytes()
            if hashlib.sha256(raw_xml).hexdigest()!=result['file']['sha256']:raise RuntimeError('actual XML changed during retention')
            retained_xml=evidence/'INSTALLED-CONTROLS.xml';retained_xml.write_bytes(raw_xml)
            immutable.extend((result['file'],file_fact(retained_xml)))
        role_logs=[];retained_log_bytes=0
        for log in sorted((fixture/'artifacts').glob('*.log')):
            size=log.lstat().st_size;retained_log_bytes+=size
            if retained_log_bytes>GIB:raise RuntimeError('complete role log retention bound exceeded')
            before=file_fact(log);destination=evidence/('ROLE-'+log.name);destination.write_bytes(log.read_bytes())
            require_fact(before);after=file_fact(destination)
            if before['sha256']!=after['sha256']:raise RuntimeError('role log retention body differs')
            immutable.extend((before,after));role_logs.append(after)
        run['actualRoleLogs']=role_logs
        if not result['accepted']:raise RuntimeError('actual installed enabled XML population did not pass completely')
        immutable.append(result['file']);scope_passed=True
        run['status']='ACTUAL_INSTALLED_SCOPE_PASSED_PENDING_FINAL_CORRESPONDENCE'
    except BaseException as error:
        run['failures'].append(dict(controller=repr(error)))
        run['status']='ACTUAL_ORIGINAL_ENDING_UNKNOWN_STOPPED'if unknown else 'REFUSED_OR_ACTUAL_FAILURE_RETAINED'
    finally:
        run['allIssuedOriginalGroupsKnownEnded']=all(row['knownEnding']for row in run['stages']if not row.get('noChildIssued'))
        final=False
        if not unknown:
            try:
                for f in immutable:require_fact(f)
                require_fact(run['recipe'])
                if rows is not None:
                    after=snapshot_source(rows,project);write_json(evidence/'SOURCE-AFTER.json',after)
                    run['completeSourceAfter']=file_fact(evidence/'SOURCE-AFTER.json')
                    if before_source!=after:raise RuntimeError('whole source post-ending facts differ')
                if active is not None:
                    require_fact(active);(evidence/'ACTIVE-AFTER.v1').write_bytes(Path(active['path']).read_bytes())
                for row in run['stages']:
                    for key in ('stdout','stderr'):
                        if key in row:require_fact(row[key])
                final=bool(rows is not None and before_source is not None and active is not None and run['allIssuedOriginalGroupsKnownEnded'])
            except BaseException as error:run['failures'].append(dict(postEndingVerification=repr(error)))
        else:run['postEndingVerification']='DEFERRED_UNTIL_ACTUAL_ORIGINAL_ENDING_KNOWN; uploaded raw diagnostics are unqualified'
        run['rawInstalledPopulationPassed']=scope_passed
        run['rawJournalDataPopulationPassed']=data_scope_passed if args.batch=='journal'else None
        run['finalSourceImagePackageAnchorAndEvidenceCorrespondence']=final
        run['journalDataScopeAccepted']=bool(args.batch=='journal'and data_scope_passed and final and not run['failures'])
        run['requestedInstalledScopeAccepted']=bool(scope_passed and (args.batch!='journal'or data_scope_passed)and final and not run['failures'])
        if run['requestedInstalledScopeAccepted']:run['status']='ACTUAL_REQUESTED_INSTALLED_SCOPE_QUALIFIED_NOT_FULL_GOAL'
        elif run['status'].startswith('ACTUAL_INSTALLED_SCOPE_PASSED'):run['status']='ACTUAL_POSTENDING_VERIFICATION_FAILURE_RETAINED'
        run['elapsedSeconds']=time.monotonic()-started
        write_json(evidence/'RESULT.json',run)
        print(json.dumps({key:run[key]for key in ('status','batch','rawInstalledPopulationPassed','allIssuedOriginalGroupsKnownEnded','finalSourceImagePackageAnchorAndEvidenceCorrespondence','requestedInstalledScopeAccepted')}),flush=True)
    return 0 if run['requestedInstalledScopeAccepted']else 1

if __name__=='__main__':sys.exit(main())
