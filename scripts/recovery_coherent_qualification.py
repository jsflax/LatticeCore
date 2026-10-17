#!/usr/bin/env python3
"""Ordinary fresh Core qualification. All raw failures remain part of the result."""
import argparse
import json
import os
from pathlib import Path
import platform
import re
import signal
import stat
import sys
import time
import xml.etree.ElementTree as ET
sys.dont_write_bytecode = True
from recovery_coherent_supervisor import file_fact, run_stage, write_json
from recovery_coherent_old_helpers import (HELPERS, MIGRATIONS, build_companion, child_artifacts,
    classify_helper_skips, helper_discovery_skip, migration_proof)

GIB = 1024**3
XML_LIMIT = 64*1024**2
INVENTORY_LIMIT = 4*1024**2
CAPS = None


def inventory(body):
    names, suite = [], None
    for line in body.splitlines():
        if re.fullmatch(r'[A-Za-z_][A-Za-z_0-9/.]*\.\s*(?:#.*)?', line):
            suite = line.split('#', 1)[0].strip()
        elif suite and line.startswith('  ') and line.strip():
            value = line.split('#', 1)[0].strip()
            if not re.fullmatch(r'[A-Za-z_0-9/]+', value):
                raise RuntimeError('unrecognized inventory name: '+value)
            names.append(suite+value)
    if not names or len(names) != len(set(names)):
        raise RuntimeError('empty or duplicate test inventory')
    return names


def read_bounded(path, limit):
    if path.stat().st_size > limit:
        raise RuntimeError('bounded artifact limit: '+str(path))
    return path.read_text()


def namespace_entries(root):
    rows = []
    def fail(error):
        raise error
    for directory, dirs, files in os.walk(root, followlinks=False, onerror=fail):
        for name in sorted(dirs+files):
            path = Path(directory)/name
            value = path.lstat()
            rows.append(dict(relative=str(path.relative_to(root)), mode=oct(value.st_mode),
                             uid=value.st_uid,dev=value.st_dev,ino=value.st_ino,bytes=value.st_size))
            if len(rows)>20000:
                raise RuntimeError('namespace evidence bound exceeded')
    return rows


def main():
    global CAPS
    parser = argparse.ArgumentParser()
    parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--expected-sha',required=True)
    parser.add_argument('--expected-tree',required=True)
    parser.add_argument('--old-source',type=Path)
    parser.add_argument('--old-sha')
    parser.add_argument('--old-tree')
    args=parser.parse_args()
    if any((args.old_source,args.old_sha,args.old_tree)) and not all((args.old_source,args.old_sha,args.old_tree)):
        raise RuntimeError('old source requires all exact source/commit/tree inputs')
    if args.old_source and any(not re.fullmatch('[0-9a-f]{40}',v)for v in (args.old_sha,args.old_tree)):
        raise RuntimeError('immutable old commit/tree required; no inferred OLD reference')
    for value in (args.expected_sha,args.expected_tree):
        if not re.fullmatch('[0-9a-f]{40}',value):
            raise RuntimeError('exact immutable commit/tree required')
    source=args.source.resolve();root=args.root.resolve()
    localdev=(Path.home()/'localdev').resolve()
    if not root.is_relative_to(localdev) or not source.is_relative_to(localdev):
        raise RuntimeError('all source and outputs must be under ~/localdev')
    if root.exists() or source.is_relative_to(root):
        raise RuntimeError('fresh output root, separate from source, required')
    old_source=args.old_source.resolve()if args.old_source else None
    if old_source and (not old_source.is_relative_to(localdev) or old_source.is_relative_to(root) or old_source.is_relative_to(source) or source.is_relative_to(old_source)):
        raise RuntimeError('separate explicit localdev old-source checkout required')
    root.mkdir(mode=0o700,parents=False)
    build=root/'build';out=root/'out';out.mkdir(mode=0o700)
    (root/'module-cache').mkdir(mode=0o700)
    started=time.monotonic();work_end=started+320*60-30
    population_path=Path(__file__).with_name('recovery_coherent_populations.json')
    population=json.loads(population_path.read_text());CAPS=population['stageCapsSeconds']
    assert len(population['components'])==106 and len(population['recovery'])==39
    assert len(population['priorActualCapiInventory'])==33
    assert len(population['priorActualCoreInventory'])==2994
    result=dict(source=str(source),expectedSha=args.expected_sha,expectedTree=args.expected_tree,
        populationSource=file_fact(population_path),startedMonotonic=started,workEndMonotonic=work_end,
        stages=[],populations={name:dict(issued=False,qualified=False) for name in ('OLD-MIGRATIONS','FULL-CORE','COMPONENTS','RECOVERY','DELIVERY-COMPONENTS','BRIDGE','CAPI')},errors=[],firstFailure=None,qualified=False,fullCoreQualified=False,
        focusDiagnosticQualified=False,capiQualified=False,profileAcceptance=False,
        performanceAcceptance=False,releaseAcceptance=False,noNamespaceCleanupPerformed=True,
        oldHelperCoverageAvailable=False,countShortcutSeedCoverageAvailable=False)
    env=dict(os.environ)
    for key in list(env):
        if key.startswith('GTEST_'):env.pop(key)
    for key in ('LATTICE_DISABLE_XPROC','LATTICE_RECOVERY_AREA','LATTICE_RECOVERY_MODE',
        'LATTICE_RECOVERY_PEER','LATTICE_COUNT_SHORTCUT_SEED_CONTAINER',
        'LATTICE_RECEIPT_RETENTION_MODE','LATTICE_RECEIPT_RETENTION_AREA',
        'LATTICE_CONTINUITY_PROCESS','LATTICE_OWNER_SCHEMA_PEER',
        'LATTICE_RECEIPT_V1_HELPER_EXECUTABLE','LATTICE_RECEIPT_V1_PARTIAL_HELPER_EXECUTABLE',
        'LATTICE_COUNT_SHORTCUT_PREDECESSOR_ROOT','LATTICE_ORDINARY_LAUNCH_HELPER_DIRECTORY',
        'LATTICE_ORDINARY_LAUNCH_OBSERVATION_HELPER_DIRECTORY','LATTICE_ORDINARY_JOURNAL_HELPER_MODE',
        'LATTICE_ORDINARY_JOURNAL_HELPER_DIRECTORY','LATTICE_RECLAMATION_HELPER_DIRECTORY',
        'LATTICE_OWNED_RESTART_SERVER','LATTICE_OWNED_RESTART_RECEIVER','LATTICE_OWNED_RESTART_SOURCE'):
        env.pop(key,None)
    env.update(CLANG_MODULE_CACHE_PATH=str(root/'module-cache'),
        SWIFT_MODULECACHE_PATH=str(root/'module-cache'),
        SWIFTPM_MODULECACHE_OVERRIDE=str(root/'module-cache'))
    base_tmp=root/'bootstrap-tmp';base_tmp.mkdir(mode=0o700)
    env.update(TMPDIR=str(base_tmp),TMP=str(base_tmp),TEMP=str(base_tmp))
    known=True;before_manifest=None;artifacts_before=None;companion=None
    raw_candidates=set();raw_required=set()

    def refuse(message):
        result['errors'].append(message)
        if result['firstFailure'] is None:result['firstFailure']=message

    def stage(name,command,cwd,cap,stage_env=None):
        nonlocal known
        if not known:raise RuntimeError('previous process group ending unknown; no further job')
        if time.monotonic()+cap+20>work_end:
            raise RuntimeError('full original stage allowance unavailable: '+name)
        failure=None
        raw_candidates.update((out/(name+'.stdout'),out/(name+'.stderr')))
        try:run_stage(out,name,command,cwd,stage_env or env,cap)
        except BaseException as error:failure=repr(error)
        path=out/(name+'.json')
        row=json.loads(path.read_text()) if path.exists() else dict(stage=name,knownEnding=False,
            error='supervisor did not publish final stage record')
        result['stages'].append(row);known=bool(row.get('knownEnding'))
        if not row.get('noChildIssued'):raw_required.update((out/(name+'.stdout'),out/(name+'.stderr')))
        if failure or row.get('exitCode')!=0 or row.get('stopReason') or not known:
            refuse(name+': '+(failure or str(row.get('stopReason') or row.get('exitCode'))))
        return row

    def require_stage(name,command,cwd,cap):
        row=stage(name,command,cwd,cap)
        if row.get('exitCode')!=0 or row.get('stopReason') or not row.get('knownEnding'):
            raise RuntimeError(name+' prerequisite refused')
        return row

    def manifest(names):
        rows=[]
        for name in names:
            path=source/name
            if not path.resolve().is_relative_to(source):
                raise RuntimeError('tracked source escapes checkout: '+name)
            rows.append(dict(relative=name,**file_fact(path)))
        return rows

    def raw_xml(path,ending):
        if not path.exists():return dict(exists=False,settled=ending)
        value=path.lstat()
        if not ending:return dict(exists=True,settled=False,snapshotOnly=True,bytesObserved=value.st_size)
        if not stat.S_ISREG(value.st_mode):raise RuntimeError('XML artifact is not a regular file')
        return dict(exists=True,settled=True,snapshotOnly=False,physicalFact=file_fact(path))

    def run_tests(name,binary,names,cap,unfiltered=False):
        case=out/name.lower();case.mkdir(mode=0o700)
        tmp=case/'tmp';work=case/'work';tmp.mkdir(mode=0o700);work.mkdir(mode=0o700)
        xml=case/'tests.xml';case_env=dict(env)
        case_env.update(TMPDIR=str(tmp),TMP=str(tmp),TEMP=str(tmp),TEST_TMPDIR=str(tmp),
                        LATTICE_TEST_LOG_PATH=str(case/'native.log'))
        command=[str(binary),'--gtest_repeat=1','--gtest_color=no','--gtest_output=xml:'+str(xml)]
        if not unfiltered:command.append('--gtest_filter='+':'.join(names))
        raw_candidates.update((xml,case/'native.log',out/(name+'-RESULT.json')))
        row=stage(name,command,work,cap,case_env)
        if not row.get('noChildIssued'):raw_required.add(xml)
        report=dict(expected=names,unfiltered=unfiltered,issued=True,ending=file_fact(out/(name+'.json'))
            if (out/(name+'.json')).exists() else None,qualified=False)
        # Bind the raw first failure before semantic parsing, including nonzero exits.
        try:
            if row.get('knownEnding'):
                report['childArtifacts']=child_artifacts(case)
            report['rawXml']=raw_xml(xml,row.get('knownEnding',False))
            if row.get('knownEnding') and xml.exists():
                if xml.stat().st_size>XML_LIMIT:raise RuntimeError('XML bounded-size refusal')
                cases=[]
                for case_xml in ET.parse(xml).iter('testcase'):
                    cases.append(dict(name=case_xml.attrib['classname']+'.'+case_xml.attrib['name'],
                        status=case_xml.attrib.get('status'),result=case_xml.attrib.get('result'),
                        failures=[dict(attributes=x.attrib,text=x.text) for x in list(case_xml)
                                  if x.tag in ('failure','error')],
                        skipped=[dict(attributes=x.attrib,text=x.text) for x in case_xml.findall('skipped')]))
                report['cases']=cases
                report['exactPopulation']=sorted(x['name'] for x in cases)==sorted(names)
                passed=lambda x:x['status']=='run' and x['result'] not in ('skipped','suppressed') and not x['failures'] and not x['skipped']
                report.update(passed=sum(passed(x) for x in cases),failed=sum(bool(x['failures']) for x in cases),
                    skippedOrUnexecuted=sum(not passed(x) and not x['failures'] for x in cases))
                report['strictQualified']=bool(row.get('exitCode')==0 and not row.get('stopReason') and
                    row.get('knownEnding') and report['exactPopulation'] and report['passed']==len(names))
                report['qualified']=report['strictQualified']
                child_settled=(name=='OLD-MIGRATIONS' and report['strictQualified']) or (name=='FULL-CORE' and
                    report['exactPopulation'] and all(passed(x) or helper_discovery_skip(x) for x in cases))
                if child_settled:
                    report['childArtifacts']=child_artifacts(case,settled=True)
                    for item in report['childArtifacts']:
                        path=case/item['relative'];raw_candidates.add(path);raw_required.add(path)
                if name=='FULL-CORE':
                    report['helperSkipClassification']=classify_helper_skips(report,case,report.get('childArtifacts',[]),source)
                    classified=report['helperSkipClassification']['discoveryOnly']
                    report['qualified']=bool(row.get('exitCode')==0 and not row.get('stopReason') and row.get('knownEnding') and
                        report['exactPopulation'] and report['passed']+len(classified)==len(names))
        except BaseException as error:
            report['parserError']=repr(error)
        if not report['qualified']:refuse(name+' XML/outcome/population not fully qualified')
        result['populations'][name]=report;write_json(out/(name+'-RESULT.json'),report)
        if not row.get('knownEnding'):raise RuntimeError(name+' unknown ending; no subsequent Native')
        return report['qualified']

    def unchanged_artifacts():
        actual=[file_fact(Path(x['path'])) for x in artifacts_before]
        if actual!=artifacts_before:raise RuntimeError('built artifacts changed before further runtime')
        return actual

    def cancellation(signum,_frame):
        raise RuntimeError('external cancellation signal '+str(signum))
    signal.signal(signal.SIGTERM,cancellation);signal.signal(signal.SIGINT,cancellation)
    try:
        result['host']=dict(system=platform.system(),machine=platform.machine(),python=sys.version)
        if platform.system()!='Darwin' or platform.machine()!='arm64':
            raise RuntimeError('this exact normal Core leg requires macOS arm64; no other platform claim')
        disk=os.statvfs(root)
        if disk.f_bavail*disk.f_frsize<13*GIB:raise RuntimeError('13GiB initial disk admission')
        for name,args_git in [('SOURCE-HEAD',['rev-parse','HEAD']),('SOURCE-TREE',['rev-parse','HEAD^{tree}']),
                              ('SOURCE-STATUS',['status','--porcelain=v1','--untracked-files=all']),
                              ('SOURCE-FILES',['ls-files','-z'])]:
            require_stage(name,['git','-C',str(source),*args_git],source,30)
        if (out/'SOURCE-HEAD.stdout').read_text().strip()!=args.expected_sha:raise RuntimeError('commit mismatch')
        if (out/'SOURCE-TREE.stdout').read_text().strip()!=args.expected_tree:raise RuntimeError('tree mismatch')
        if (out/'SOURCE-STATUS.stdout').read_bytes():raise RuntimeError('immutable checkout is dirty')
        names=sorted(set(os.fsdecode(x) for x in (out/'SOURCE-FILES.stdout').read_bytes().split(b'\0') if x))
        before_manifest=manifest(names);write_json(out/'SOURCE-INPUTS.json',before_manifest)
        markers=[source/'Sources/LatticeCore/include/lattice/detail/observer_delivery_update.hpp',
                 source/'Tests/LatticeCoreTests/ObserverDeliveryBridgeSetterTests.cpp']
        present=[x.exists() for x in markers]
        if any(present) and not all(present):raise RuntimeError('partial delivery component source family')
        delivery=all(present);result['deliveryComponentSelectedFromSource']=delivery
        planned_count=population['plannedDeliveryCoreInventory'] if delivery else population['expectedCoreInventory']
        expected_focus=population['components']+population['recovery']+(population['deliveryCore'] if delivery else [])
        for name,command in [('CLANG',['xcrun','clang','--version']),('XCODE',['xcodebuild','-version']),('CMAKE',['cmake','--version'])]:
            require_stage(name,command,source,30)
        tools={}
        for name,command in [('cc',['xcrun','--find','clang']),('cxx',['xcrun','--find','clang++']),('sdk',['xcrun','--show-sdk-path'])]:
            require_stage('TOOL-'+name,command,source,30)
            value=(out/('TOOL-'+name+'.stdout')).read_text().strip()
            if not Path(value).is_absolute():raise RuntimeError('absolute current tool/SDK required')
            tools[name]=value
        tools['physicalInputs']=[file_fact(Path(tools[k]))for k in ('cc','cxx')]
        settings=next((Path(tools['sdk'])/n for n in ('SDKSettings.json','SDKSettings.plist')if (Path(tools['sdk'])/n).is_file()),None)
        if settings is None:raise RuntimeError('actual selected SDK settings unavailable')
        tools['physicalInputs'].append(file_fact(settings));result['compilerSDK']=tools
        require_stage('CONFIGURE',['cmake','-G','Unix Makefiles','-S',str(source),'-B',str(build),'-DCMAKE_BUILD_TYPE=Debug',
            '-DCMAKE_EXPORT_COMPILE_COMMANDS=ON','-DCMAKE_OSX_ARCHITECTURES=arm64','-DCMAKE_OSX_DEPLOYMENT_TARGET=14.0',
            '-DCMAKE_C_COMPILER='+tools['cc'],'-DCMAKE_CXX_COMPILER='+tools['cxx'],'-DCMAKE_OSX_SYSROOT='+tools['sdk']],source,CAPS['CONFIGURE'])
        require_stage('BUILD',['cmake','--build',str(build),'--parallel','2','--','-k'],source,CAPS['BUILD'])
        artifacts_before=[file_fact(build/'LatticeCoreTests'),file_fact(build/'LatticeCAPITests'),
                          file_fact(build/'libLatticeCAPI.0.1.0.dylib')]
        if delivery:artifacts_before.append(file_fact(build/'LatticeObserverDeliveryBridgeTests'))
        result['artifactsBefore']=artifacts_before
        observed={}
        pairs=[('CORE-INVENTORY','LatticeCoreTests'),('CAPI-INVENTORY','LatticeCAPITests')]
        if delivery:pairs.append(('BRIDGE-INVENTORY','LatticeObserverDeliveryBridgeTests'))
        for label,binary in pairs:
            require_stage(label,[str(build/binary),'--gtest_list_tests','--gtest_color=no'],source,60)
            observed[label]=inventory(read_bounded(out/(label+'.stdout'),INVENTORY_LIMIT))
        write_json(out/'ACTUAL-INVENTORIES.json',observed)
        core_names=observed['CORE-INVENTORY'];capi_names=observed['CAPI-INVENTORY']
        result['plannedCoreCount']=planned_count;result['actualBuiltCoreCount']=len(core_names)
        result['plannedCoreCountMatches']=len(core_names)==planned_count
        if not result['plannedCoreCountMatches']:refuse('planned Core count mismatch (planned is not observed history)')
        if not set(population['priorActualCoreInventory'])<=set(core_names):raise RuntimeError('prior observed Core names missing')
        if not set(expected_focus)<=set(core_names):raise RuntimeError('selected focused Core names missing')
        if sorted(capi_names)!=sorted(population['priorActualCapiInventory']):raise RuntimeError('actual CAPI33 population mismatch')
        if delivery and sorted(observed['BRIDGE-INVENTORY'])!=sorted(population['deliveryBridge']):raise RuntimeError('Bridge7 population mismatch')
        result['oldHelperCoverageAvailable']=False
        result['countShortcutSeedCoverageAvailable']=False
        result['coverageLimitations']=['Six genuine predecessor count-shortcut seeds are not integrated; no full old-family acceptance.']
        if old_source:
            raw_candidates.add(out/'OLD-COMPANION-PROVENANCE.json')
            try:
                companion=build_companion(old_source,args.old_sha,args.old_tree,source,root,out,tools,stage,inventory)
                env.update(companion['environment']);artifacts_before.extend(companion['artifacts'])
                result['oldCompanionBuildVerified']=True
                result['currentCompileCommands']=file_fact(build/'compile_commands.json')
                run_tests('OLD-MIGRATIONS',build/'LatticeCoreTests',MIGRATIONS,900)
                report=result['populations']['OLD-MIGRATIONS'];case=out/'old-migrations'
                proof=migration_proof(report,case,report.get('childArtifacts',[]),source,old_source)
                result['oldMigrationEvidence']=proof
                result['oldHelperCoverageAvailable']=bool(report['qualified'] and proof['qualified'])
                write_json(out/'OLD-MIGRATION-PROOF.json',proof);raw_candidates.add(out/'OLD-MIGRATION-PROOF.json')
            except BaseException as error:
                refuse('old companion/migration prerequisite: '+repr(error))
                if not known:raise
        if not result['oldHelperCoverageAvailable']:
            result['coverageLimitations'].append('Genuine old companion build/source/compiler/inventory and exact current migration parent/child evidence are incomplete.')
        full=run_tests('FULL-CORE',build/'LatticeCoreTests',core_names,CAPS['FULL-CORE'],unfiltered=True)
        result['fullCoreQualified']=full
        unchanged_artifacts()
        if before_manifest!=manifest(names):raise RuntimeError('source changed before focused diagnostic')
        if not full:
            # This is diagnostic coverage of an already-failed family, never a recovery to green.
            result['focusAfterFullFailure']=True
            c=run_tests('COMPONENTS',build/'LatticeCoreTests',population['components'],CAPS['COMPONENTS'])
            r=run_tests('RECOVERY',build/'LatticeCoreTests',population['recovery'],CAPS['RECOVERY'])
            extra=True
            if delivery:extra=run_tests('DELIVERY-COMPONENTS',build/'LatticeCoreTests',population['deliveryCore'],CAPS['DELIVERY-COMPONENTS'])
            result['focusDiagnosticQualified']=c and r and extra
        else:
            result['focusedCoreCoveredByUnfilteredFull']=expected_focus
        if delivery:
            result['bridgeQualified']=run_tests('BRIDGE',build/'LatticeObserverDeliveryBridgeTests',population['deliveryBridge'],CAPS['BRIDGE'],unfiltered=True)
        result['capiQualified']=run_tests('CAPI',build/'LatticeCAPITests',capi_names,CAPS['CAPI'],unfiltered=True)
        result['artifactsAfter']=unchanged_artifacts()
    except BaseException as error:
        refuse(repr(error))
    finally:
        result['allIssuedGroupsKnownEnded']=known and all(x.get('knownEnding') for x in result['stages'])
        if result['allIssuedGroupsKnownEnded']:
            try:
                if companion is not None:
                    after=[dict(relative=n,**file_fact(companion['source']/n))for n in companion['tracked']]
                    result['oldSourceUnchanged']=after==companion['before']
                    if not result['oldSourceUnchanged']:refuse('old companion source changed')
                if before_manifest is not None:
                    after_manifest=manifest(names);write_json(out/'SOURCE-INPUTS-AFTER.json',after_manifest)
                    result['sourceUnchanged']=after_manifest==before_manifest
                    if not result['sourceUnchanged']:refuse('source changed')
                case_roots=[x for x in out.iterdir() if x.is_dir()]
                result['namespaces']={x.name:namespace_entries(x) for x in case_roots}
                # Each case has its two empty scaffolding directories; inspect descendants separately.
                result['namespacesEmpty']=all(not namespace_entries(x/'tmp') and not namespace_entries(x/'work') for x in case_roots)
                if not result['namespacesEmpty']:refuse('retained test namespace entries; no deletion performed')
            except BaseException as error:refuse('ending/source/namespace evidence: '+repr(error))
        else:result['namespaceInspectionDeferredForUnknownEnding']=True
        result['qualified']=bool(not result['errors'] and result['fullCoreQualified'] and result['capiQualified'] and
            result.get('bridgeQualified',True) and result.get('sourceUnchanged') and result.get('namespacesEmpty') and
            result['allIssuedGroupsKnownEnded'] and result.get('plannedCoreCountMatches') and result.get('oldHelperCoverageAvailable'))
        # Capture only fixed named outputs. Unknown ending never walks a live
        # namespace or hashes a file that may still be written.
        artifacts=[]
        for path in sorted(raw_candidates):
            item=dict(relative=str(path.relative_to(out)),snapshotOnly=not result['allIssuedGroupsKnownEnded'])
            try:
                observed_stat=path.lstat();item['exists']=True
                if result['allIssuedGroupsKnownEnded']:
                    item['physicalFact']=file_fact(path)
                else:item.update(bytesObserved=observed_stat.st_size,modeObserved=oct(observed_stat.st_mode))
            except FileNotFoundError:
                item['exists']=False
                if path in raw_required:refuse('required issued raw artifact missing: '+str(path))
            except BaseException as error:
                item['captureError']=repr(error);refuse('raw artifact finalization refused: '+str(path))
            artifacts.append(item)
        result['rawArtifactIndex']=artifacts
        if result.get('coverageLimitations'):
            for limitation in result['coverageLimitations']:refuse(limitation)
        if result['errors']:result['qualified']=False
        result['elapsedSeconds']=time.monotonic()-started
        write_json(out/'RESULT.json',result)
        print(json.dumps(dict(qualified=result['qualified'],firstFailure=result['firstFailure'],
            fullCoreQualified=result['fullCoreQualified'],focusDiagnosticQualified=result['focusDiagnosticQualified'],
            allIssuedGroupsKnownEnded=result['allIssuedGroupsKnownEnded'])),flush=True)
    return 0 if result['qualified'] else 1

if __name__=='__main__':
    raise SystemExit(main())
