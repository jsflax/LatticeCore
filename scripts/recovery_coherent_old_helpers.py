"""Genuine old-body companion provenance and explicit child-only evidence."""
import json
from collections import Counter
import os
from pathlib import Path
import posixpath
import re
import shlex
import stat
from recovery_coherent_supervisor import file_fact, write_json

OLD_SHA='d16086f31e28f63905ccc0b21863469df5fd09e7'
OLD_TREE='8ea1f0de99d09588250c4932032f745626e69aee'
MAP_SHA='9fbf0fd000a788f423bc7165abffc69e31da54069c1a38e1673653ebfc8d0bd5'
CMAKE_SHA='f9e1413bb84ae5e51566f83a8ed85040bee8fc3bf814108ae8eb1f5a2366ca9d'
MIGRATIONS=[
 'ReceiptDispositionCrash.ActualPreviousWriterV1RefreezesWithoutSendAndReacquiresSourceKnowledge',
 'ReceiptDispositionCrash.ActualPreviousFrozenV1RevalidatesWithoutRestrictedSendBeforeReservedQ',
 'ReceiptDispositionCrash.ActualPreviousPartialV1CancelsLocallyBeforeFreshReservedRevalidation']
HELPERS={
 'OrdinaryOwnedLaunch.ProcessHelper':(['OrdinaryOwnedLaunch.ForkedWrapperRefusesUseAndCloseDoesNotShutdownParentChannel'],1,1),
 'OrdinaryOwnedLaunch.ObservationProcessHelper':(['OrdinaryOwnedLaunch.ForkedNaturalObservationRefusesBeforeInheritedOwnerLocks'],1,1),
 'OrdinaryStoreAdmission.ProcessHelper':(['OrdinaryStoreAdmission.ChildOwnsIndependentHoldUntilActualJoinedCompletion','OrdinaryStoreAdmission.ForkedUseRefusesAndChildCloseDoesNotUnlockParentHold'],2,1),
 'OrdinaryOwnedRestart.ProcessHelper':(['OrdinaryOwnedRestart.ActualNewProcessContinuesSourceAndReceiverThenRejectsStaleMaterialization'],1,1),
 'RecoveryProducerContinuity.ProcessHelper':(['RecoveryProducerContinuity.FreshProcessesPreserveClosedQAfterKnownCommitAndAbruptExit'],2,1),
 'ReceiptDispositionProcess.ProcessHelper':(['ReceiptDispositionCrash.KnownCancelCommitSurvivesAbruptExitAndReopensOnlyUnknownNamespace'],2,1),
 'ManagedTargetReclamation.ConstructorProcessHelper':(['ManagedTargetReclamation.GenuineInstalledConstructorReceipt'],1,1)}


def passed(case):
    return case.get('status')=='run' and case.get('result') not in ('skipped','suppressed') and not case.get('failures') and not case.get('skipped')


def helper_discovery_skip(case):
    phrases={
      'ReceiptDispositionProcess.ProcessHelper':'parent-only receipt crash/reopen helper',
      'RecoveryProducerContinuity.ProcessHelper':'invoked only by owned fresh-process parent',
      'OrdinaryOwnedRestart.ProcessHelper':'Dedicated actual process-restart child; parent receipt is separate'}
    name=case.get('name');items=case.get('skipped',[])
    if name not in HELPERS or case.get('status')!='run' or case.get('failures') or len(items)!=1:return False
    phrase=phrases.get(name,'Requires the real parent-owned process fixture')
    item=items[0];text=str(item.get('text')or'')+' '+str(item.get('attributes',{}))
    return phrase in text


def bounded(path,limit):
    if path.stat().st_size>limit:raise RuntimeError('old companion bounded artifact refusal: '+str(path))
    return path.read_text()


def canonical_sources(source,body):
    """488 original dependency entries collapse to404 same-body destinations."""
    expected={}
    for item in body['sourceFiles']:
        rel=posixpath.normpath(item['relativePath'])
        if rel.startswith('../') or rel.startswith('/') or rel=='.':raise RuntimeError('old dependency escapes source')
        path=source/rel
        if not path.resolve().is_relative_to(source):raise RuntimeError('old source alias escapes source')
        row=file_fact(path);prior=item['source']
        if any(row[k]!=prior[k] for k in ('bytes','sha256')):raise RuntimeError('genuine old body mismatch: '+rel)
        if rel in expected and expected[rel]!=row:raise RuntimeError('old alias body mismatch')
        expected[rel]=row
    if len(body['sourceFiles'])!=488 or len(expected)!=404:raise RuntimeError('old sparse dependency population mismatch')
    return expected



# Recorded actual original93 semantic compile-flag families. Includes inherited
# global settings; no default-visibility/definition drift is silently accepted.
def require_old_flags(argv,target):
    common=['-g','-mmacosx-version-min=14.0']
    if target=='SqliteVec':
        expected=common+['-DSQLITE_CORE','-DSQLITE_VEC_ENABLE_NEON','-DSQLITE_VEC_EXPERIMENTAL_IVF_ENABLE','-DSQLITE_VEC_STATIC','-fPIC','-fvisibility=hidden']
    elif target in ('LatticeCore','LatticeSwiftCppBridge'):
        expected=common+['-std=gnu++20','-fPIC','-fvisibility=hidden','-fvisibility-inlines-hidden']
        if target=='LatticeCore':expected+=['-DSQLITE_CORE']
    elif target in ('gtest','gtest_main'):
        expected=common+['-std=gnu++20','-fPIC']
    elif target=='ReadySupport':
        expected=common+['-std=gnu++20','-fPIE']
    elif target=='ReceiptHelper':
        expected=common+['-std=gnu++20','-fPIE','-DLATTICE_RECEIPT_RETENTION_LEGACY_WRITER=1']
    else:raise RuntimeError('unknown old compiler target')
    actual=[v for v in argv if v.startswith(('-D','-U','-std=','-f','-m','-g','-O'))]
    if sorted(actual)!=sorted(expected):raise RuntimeError('actual original compile flag family differs: '+target)
    return dict(target=target,actual=actual,recordedOriginal=expected)


def build_companion(source,sha,tree,current,root,out,tools,stage,parse_inventory):
    if not OLD_SHA or not OLD_TREE:raise RuntimeError('NEW whole-support immutable source pin is unbound; do not issue OLD')
    if sha!=OLD_SHA or tree!=OLD_TREE:raise RuntimeError('unreviewed old provenance reference')
    def require(name,command,cwd,cap):
        row=stage(name,command,cwd,cap)
        if row.get('exitCode')!=0 or row.get('stopReason') or not row.get('knownEnding'):raise RuntimeError(name+' prerequisite refused')
    for label,args in [('OLD-HEAD',['rev-parse','HEAD']),('OLD-TREE',['rev-parse','HEAD^{tree}']),('OLD-STATUS',['status','--porcelain=v1','--untracked-files=all']),('OLD-FILES',['ls-files','-z'])]:
        require(label,['git','-C',str(source),*args],source,30)
    if (out/'OLD-HEAD.stdout').read_text().strip()!=sha or (out/'OLD-TREE.stdout').read_text().strip()!=tree:raise RuntimeError('immutable old commit/tree mismatch')
    if (out/'OLD-STATUS.stdout').read_bytes():raise RuntimeError('old provenance checkout is dirty')
    if file_fact(source/'SOURCE-MAP.json')['sha256']!=MAP_SHA or file_fact(source/'CMakeLists.txt')['sha256']!=CMAKE_SHA:raise RuntimeError('old reference does not contain exact reviewed recipe')
    body=json.loads(bounded(source/'SOURCE-MAP.json',1024*1024))
    expected=canonical_sources(source,body)
    tracked=sorted(set(os.fsdecode(x)for x in (out/'OLD-FILES.stdout').read_bytes().split(b'\0')if x))
    if set(tracked)!=set(expected)|{'CMakeLists.txt','SOURCE-MAP.json'}:raise RuntimeError('old source tracked population mismatch')
    before=[dict(relative=n,**file_fact(source/n))for n in tracked]
    proof=dict(source=str(source),sha=sha,tree=tree,sourceBefore=before,sourceMap=file_fact(source/'SOURCE-MAP.json'),compilerSDK=tools,
               dependencyEntries=488,canonicalSourceFiles=404,codegenUnits=97,uniqueSourceUnits=96,qualified=False,
               focusedImageOmitsUnrelatedOriginalTestRegistrations=True,wholeReadySupportNames=63,newProvenanceSnapshotNotHistoricalCommit=True)
    write_json(out/'OLD-COMPANION-PROVENANCE.json',proof)
    build=root/'old-build'
    require('OLD-CONFIGURE',['cmake','-G','Unix Makefiles','-S',str(source),'-B',str(build),'-DCMAKE_BUILD_TYPE=Debug',
        '-DCMAKE_EXPORT_COMPILE_COMMANDS=ON','-DCMAKE_OSX_ARCHITECTURES=arm64','-DCMAKE_OSX_DEPLOYMENT_TARGET=14.0',
        '-DCMAKE_C_COMPILER='+tools['cc'],'-DCMAKE_CXX_COMPILER='+tools['cxx'],'-DCMAKE_OSX_SYSROOT='+tools['sdk']],source,120)
    require('OLD-BUILD',['cmake','--build',str(build),'--parallel','2','--target','LatticeReceiptV1Helper','LatticeReceiptV1PartialHelper','--','-k'],source,3600)
    commands=json.loads(bounded(build/'compile_commands.json',4*1024**2));expected_units={x['source']for x in body['productUnits']}|{
        'Tests/LatticeCoreTests/RecoveryReceiptCoverageControllerTests.cpp','Tests/LatticeCoreTests/RecoveryReceiptCoveragePartialControllerTests.cpp',
        'Tests/LatticeCoreTests/CanonicalDurableReadyTests.cpp'}
    support_source='Tests/LatticeCoreTests/CanonicalDurableReadyTests.cpp'
    if len(body.get('supportUnits',[]))!=1 or body['supportUnits'][0]['source']!=support_source:raise RuntimeError('whole old ready-support authority missing')
    actual_units=set();unit_counts=Counter();objects=[];dependencies={}
    product_targets={x['source']:x['target']for x in body['productUnits']}
    for row in commands:
        unit=Path(row['file']).resolve();rel=str(unit.relative_to(source));actual_units.add(rel);unit_counts[rel]+=1
        argv=shlex.split(row['command']);compiler=tools['cc']if unit.suffix=='.c'else tools['cxx']
        if Path(argv[0]).resolve()!=Path(compiler).resolve():raise RuntimeError('old compiler differs from current compiler')
        for flag,value in [('-arch','arm64'),('-isysroot',tools['sdk']),('-o',None)]:
            if flag not in argv or (value is not None and argv[argv.index(flag)+1]!=value):raise RuntimeError('old compiler family flag mismatch: '+flag)
        if '-g'not in argv or '-mmacosx-version-min=14.0'not in argv:raise RuntimeError('old build mode/deployment mismatch')
        if unit.suffix!='.c' and '-std=gnu++20'not in argv:raise RuntimeError('old C++20 extensions contract mismatch')
        helper=rel in {'Tests/LatticeCoreTests/RecoveryReceiptCoverageControllerTests.cpp','Tests/LatticeCoreTests/RecoveryReceiptCoveragePartialControllerTests.cpp'}
        if ('-DLATTICE_RECEIPT_RETENTION_LEGACY_WRITER=1'in argv)!=helper:raise RuntimeError('legacy test guard leaked into product or missing from helper')
        flag_family=require_old_flags(argv,'ReceiptHelper'if helper else 'ReadySupport'if rel==support_source else product_targets.get(rel))
        obj=Path(argv[argv.index('-o')+1]);obj=obj if obj.is_absolute()else Path(row['directory'])/obj;obj=obj.resolve()
        if not obj.is_relative_to(build):raise RuntimeError('old object outside fresh build')
        dep=Path(str(obj)+'.d');text=bounded(dep,2*1024**2).replace('\\\n',' ').split(':',1)[1]
        project=set()
        for word in re.findall(r'(?:\\.|[^\s])+',text):
            path=Path(re.sub(r'\\(.)',r'\1',word));path=path if path.is_absolute()else Path(row['directory'])/path;path=path.resolve()
            if path.is_relative_to(current):raise RuntimeError('CURRENT source in OLD actual dependency')
            if path.is_relative_to(source):
                key=str(path.relative_to(source))
                if key not in expected:raise RuntimeError('unbound old project dependency: '+key)
                project.add(key)
            elif path.is_relative_to(Path.home()/'localdev') and not path.is_relative_to(build):raise RuntimeError('foreign project dependency in old build')
            if str(path)not in dependencies:dependencies[str(path)]=file_fact(path)
        if rel not in project:raise RuntimeError('old actual .d misses its whole TU')
        if rel==support_source and project!=set(body['supportUnits'][0]['oldProjectDependencies']):raise RuntimeError('whole old ready-support actual dependency closure differs')
        objects.append(dict(source=rel,command=argv,flagFamily=flag_family,object=file_fact(obj),dependencyFile=file_fact(dep),projectDependencies=sorted(project)))
    expected_counts=Counter({unit:1 for unit in expected_units});expected_counts[support_source]=2
    if len(commands)!=97 or actual_units!=expected_units or unit_counts!=expected_counts:raise RuntimeError('actual old97 codegen/96source population mismatch')
    expected_support_objects={str(build/'CMakeFiles'/(target+'.dir')/(support_source+'.o')) for target in ['LatticeReceiptV1Helper','LatticeReceiptV1PartialHelper']}
    if {x['object']['path']for x in objects if x['source']==support_source}!=expected_support_objects:raise RuntimeError('whole support must be freshly compiled in each actual helper')
    links=[]
    object_paths={x['object']['path']for x in objects}
    archive_targets=['LatticeCore','LatticeSwiftCppBridge','SqliteVec','gtest','gtest_main']
    archives=[file_fact(build/('lib'+target+'.a'))for target in archive_targets]
    for target in archive_targets+['LatticeReceiptV1Helper','LatticeReceiptV1PartialHelper']:
        recipe=build/'CMakeFiles'/(target+'.dir')/'link.txt'
        text=bounded(recipe,2*1024**2)
        for token in shlex.split(text):
            if not token.endswith(('.o','.a')):continue
            path=Path(token);path=path if path.is_absolute()else build/path;path=path.resolve()
            if not path.is_relative_to(build):raise RuntimeError('foreign object/archive in actual OLD link recipe')
            if token.endswith('.o') and str(path)not in object_paths:raise RuntimeError('unproved old link object')
        if target in ('LatticeReceiptV1Helper','LatticeReceiptV1PartialHelper'):
            actual_link_objects={str((build/word).resolve())for word in shlex.split(text)if word.endswith('.o')}
            helper_source='Tests/LatticeCoreTests/'+('RecoveryReceiptCoverageControllerTests.cpp'if target=='LatticeReceiptV1Helper'else 'RecoveryReceiptCoveragePartialControllerTests.cpp')
            wanted={str(build/'CMakeFiles'/(target+'.dir')/(unit+'.o'))for unit in (helper_source,support_source)}
            if actual_link_objects!=wanted:raise RuntimeError('actual helper link must retain both whole old test TUs')
        links.append(dict(target=target,recipe=file_fact(recipe),text=text))
    proof.update(archives=archives,linkRecipes=links)
    binaries=[];mapping={}
    for target,key in [('LatticeReceiptV1Helper','LATTICE_RECEIPT_V1_HELPER_EXECUTABLE'),('LatticeReceiptV1PartialHelper','LATTICE_RECEIPT_V1_PARTIAL_HELPER_EXECUTABLE')]:
        binary=build/target;require('OLD-INVENTORY-'+target,[str(binary),'--gtest_list_tests','--gtest_color=no'],source,60)
        names=parse_inventory(bounded(out/('OLD-INVENTORY-'+target+'.stdout'),4*1024**2))
        if sorted(names)!=sorted(body['helperInventoriesEach']):raise RuntimeError('old helper actual74 inventory mismatch')
        binaries.append(file_fact(binary));mapping[key]=str(binary)
    after=[dict(relative=n,**file_fact(source/n))for n in tracked]
    if after!=before:raise RuntimeError('old source changed during companion build')
    proof.update(sourceAfter=after,sourceUnchanged=True,actualCompileCommands=file_fact(build/'compile_commands.json'),objects=objects,
                 actualDependencies=list(dependencies.values()),binaries=binaries,actualInventoriesEach=body['helperInventoriesEach'],qualified=True)
    write_json(out/'OLD-COMPANION-PROVENANCE.json',proof)
    return dict(environment=mapping,artifacts=binaries,proof=proof,source=source,tracked=tracked,before=before)


def child_artifacts(case,settled=False):
    """Parent ending permits DATA snapshots; passed actual parents permit settled child facts."""
    rows=[];visited=0
    for directory,dirs,files in os.walk(case,followlinks=False):
        visited+=len(dirs)+len(files)
        if visited>20000:raise RuntimeError('child namespace inventory bound')
        for name in dirs:
            if (Path(directory)/name).is_symlink():raise RuntimeError('child evidence directory symlink')
        for name in files:
            path=Path(directory)/name
            if not (name.endswith('terminal.log') or name.startswith('native.log.')):continue
            mode=path.lstat().st_mode
            if not stat.S_ISREG(mode):raise RuntimeError('child evidence is not a regular file')
            if len(rows)>=512 or path.stat().st_size>64*1024**2:raise RuntimeError('child evidence bound')
            item=dict(relative=str(path.relative_to(case)),snapshotOnly=not settled,bytesObserved=path.stat().st_size)
            if settled:item['physicalFact']=file_fact(path)
            rows.append(item)
    return rows


def child_records(case,artifacts,helper,compiled_source):
    records=[]
    for item in artifacts:
        path=case/item['relative']
        if not path.name.endswith('terminal.log') or item.get('snapshotOnly',True):continue
        text=bounded(path,2*1024**2)
        if 'Note: Google Test filter = '+helper+'\n'not in text:continue
        if 'Running main() from '+str(compiled_source/'Sources/GoogleTest/src/gtest_main.cc')+'\n'not in text:continue
        if not re.search(r'^\[ RUN\s+\] '+re.escape(helper)+r'$',text,re.M):continue
        failed=bool(re.search(r'^\[\s*(?:FAILED|SKIPPED)\s*\]',text,re.M))
        ok=bool(re.search(r'^\[\s*OK\s*\] '+re.escape(helper)+r' \(',text,re.M))
        native=item['relative'][:-len('.terminal.log')]if item['relative'].endswith('.terminal.log')else None
        if helper=='RecoveryProducerContinuity.ProcessHelper':
            match=re.fullmatch(r'(.+)-(seed|reopen)-terminal.log',path.name)
            native='native.log.continuous-'+match[1]+'-'+match[2]if match else None
        native_record=next((x for x in artifacts if x['relative']==native and not x.get('snapshotOnly',True)),None)
        if native_record is None:continue
        records.append(dict(relative=item['relative'],physicalFact=item['physicalFact'],native=native_record['physicalFact'],failed=failed,passed=ok))
    return records


def migration_proof(report,case,artifacts,current,old):
    byname={x['name']:x for x in report.get('cases',[])}
    rows=[];qualified=bool(report.get('qualified')) and all(name in byname and passed(byname[name])for name in MIGRATIONS)
    if not qualified:return dict(qualified=False,parents=MIGRATIONS,children=[],reason='actual migration parents not all qualified; child logs remain snapshots')
    for mode,verify in [('crash','verify'),('frozen','verify-frozen'),('partial','verify-partial')]:
        candidates=child_records(case,artifacts,'ReceiptDispositionProcess.ProcessHelper',old)
        old_rows=[x for x in candidates if x['relative'].endswith('.'+mode+'.native.log.terminal.log') and not x['failed']]
        matches=[]
        for first in old_rows:
            prefix=first['relative'][:-len('.'+mode+'.native.log.terminal.log')]
            second_rel=prefix+'.'+verify+'.native.log.terminal.log'
            second=next((x for x in child_records(case,artifacts,'ReceiptDispositionProcess.ProcessHelper',current)
                         if x['relative']==second_rel and x['passed'] and not x['failed']),None)
            native=[prefix+'.'+m+'.native.log' for m in [mode,verify]]
            if second and all(any(a['relative']==n for a in artifacts)for n in native):matches.append(dict(old=first,current=second,native=native))
        rows.append(dict(oldMode=mode,currentMode=verify,matches=matches));qualified=qualified and len(matches)==1
    return dict(qualified=bool(qualified),parents=MIGRATIONS,children=rows,
                intentionalAbruptExitRequiresParentWaitStatusAndDurableAssertions=True)


def classify_helper_skips(report,case,artifacts,current):
    byname={x['name']:x for x in report.get('cases',[])};classified=[];details=[]
    if not report.get('exactPopulation') or any(not passed(x)for x in report.get('cases',[])if x['name']not in HELPERS):
        return dict(discoveryOnly=[],details=[],passingTestsNotCreated=True,reason='nonhelper full population incomplete; child logs remain snapshots')
    for helper,(parents,minimum,minimum_passes)in HELPERS.items():
        value=byname.get(helper)
        if not value or not helper_discovery_skip(value):continue
        # Only the exact seven helper declarations; no real migration/platform/parameter waiver.
        records=child_records(case,artifacts,helper,current)
        good=[x for x in records if not x['failed']]
        ok=all(p in byname and passed(byname[p])for p in parents) and len(good)>=minimum and sum(x['passed']for x in good)>=minimum_passes
        details.append(dict(helper=helper,parents=parents,childReceipts=records,classified=bool(ok)))
        if ok:classified.append(helper)
    return dict(discoveryOnly=classified,details=details,passingTestsNotCreated=True)
