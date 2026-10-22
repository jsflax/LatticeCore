"""Explicit ephemeral-CI DATA packaging only; never installs, launches or edits an anchor."""
from pathlib import Path
import argparse, hashlib, json, os, pwd, shutil, stat

LIMIT_FIELDS = {'format','clients','waiters','guaranteedCount','guaranteedBytes','burstCount','burstBytes','events','eventBytes','workspaceBytes','readWorkspaceBytes','readColumns','readRows','admissionWaitMs','readDurationMs','initializationStatements','initializationWorkspaceBytes'}
CANONICAL = [
 ('ActualCanonicalFinalAuditsReadEveryFamilyWithoutSqlWritesOnAllRoles',['READONLY']),
 ('ActualFinalReadFailureInAnyCanonicalFamilyCannotBecomeAbsentOrSeal',['DENY_REQUEST','DENY_RANGE','DENY_PAGE','DENY_DOMAIN','DENY_MEMBER','DENY_WITNESS','DENY_RECEIVER']),
 ('ActualFinalLocalProducerClearOrReplacementInvalidatesItsOwnReceiptBeforeBegin',['CLEAR','REBIND']),
 ('ActualSealedFinalSnapshotCannotPublishAfterItsProducerRegistrationChanges',['SEALED_CLEAR','SEALED_REBIND','SEALED_RAW_GUARD']),
 ('ActualUnrelatedDispatcherSlotReissueDoesNotReplaceProducerFamilyReceipt',['UNRELATED']),
 ('ActualRawProducerGuardReplacementCannotKeepFinalFamilyReceipt',['RAW_GUARD']),
 ('ActualDataOnlyReaderCannotForgeFinalSnapshotOrSelectCanonicalRecipe',['UNISSUED']),
 ('ActualFinishedWriterReceiptCannotCertifyEitherNewPendingReader',['READER']),
 ('ActualReplacementWriterThenReaderPairUsesNewAccountsAfterOriginalDeadline',['REPLACEMENT']),
 ('ActualBaseProviderFamiliesCannotSubstituteMissingContinuousProducerFamily',['BASE_SEVEN'])]
JOURNAL = [('CATALOG_ACK','enroll-crash-catalog-ack'),('PRECOMMIT','enroll-crash-precommit'),('COMMIT','enroll-crash-commit'),('ACK_SYNC','enroll-crash-ack-sync'),('ACK_RENAME','enroll-crash-ack-rename'),('ACK_PARENT','enroll-crash-ack-parent'),('COMMIT_FAILURE','enroll-commit-failure'),('MISSING','enroll-missing-intent'),('FORGED','enroll-forged-intent'),('REPLAY_DONOR','enroll-replay-donor'),('REPLAY','enroll-replayed-intent')]
NONEMPTY = 'RecoveryNonemptyFinalAudit.ActualAuthenticatedInstallRetainsNonemptyCanonicalRowsThroughReadOnlyFinalReaderPair'


def secure(path, directory=True, private=False):
    path = Path(path)
    if not path.is_absolute() or path != Path(os.path.normpath(path)):
        raise RuntimeError('actual normalized absolute path required')
    cursor = Path('/')
    for part in path.parts[1:]:
        cursor /= part
        value = cursor.lstat()
        last = cursor == path
        if stat.S_ISLNK(value.st_mode): raise RuntimeError('fixture path contains a symlink')
        if not last or directory:
            if not stat.S_ISDIR(value.st_mode) or value.st_uid not in (0,os.geteuid()) or value.st_mode&0o022:
                raise RuntimeError('fixture directory owner/mode differs: '+str(cursor))
        elif not stat.S_ISREG(value.st_mode): raise RuntimeError('fixture input must be a regular file')
    value=path.lstat()
    if private and (value.st_uid != os.geteuid() or stat.S_IMODE(value.st_mode) != (0o700 if directory else 0o600)):
        raise RuntimeError('private fixture owner/mode differs: '+str(path))
    return value


def fact(path):
    before=secure(path,False);h=hashlib.sha256()
    with Path(path).open('rb') as source:
        for block in iter(lambda:source.read(1024*1024),b''):h.update(block)
    after=Path(path).lstat()
    if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns):
        raise RuntimeError('actual image/data changed while observed')
    return {'path':str(path),'identity':{'device':after.st_dev,'inode':after.st_ino},'sha256':h.hexdigest(),'bytes':after.st_size}


def read_json(path, limit=128*1024):
    value=secure(path,False)
    if value.st_size<=0 or value.st_size>limit:raise RuntimeError('bounded fixture JSON required')
    return json.loads(Path(path).read_bytes())


def create_dir(path):
    path=Path(path)
    if path.exists():return secure(path,True,True)
    secure(path.parent,True)
    path.mkdir(mode=0o700);os.chmod(path,0o700);return secure(path,True,True)


def exclusive(path, data):
    secure(Path(path).parent,True,True)
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
    try:
        os.fchmod(fd,0o600)
        at=0
        while at<len(data):at+=os.write(fd,data[at:])
        os.fsync(fd)
    finally:os.close(fd)
    fd=os.open(Path(path).parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
    try:os.fsync(fd)
    finally:os.close(fd)


def write_json(path, value):exclusive(path,(json.dumps(value,indent=2)+'\n').encode())


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('mode',choices=['package','locators'])
    p.add_argument('--fixture-root',type=Path,required=True)
    p.add_argument('--expected-home',type=Path,required=True)
    p.add_argument('--expected-uid',type=int,required=True)
    p.add_argument('--batch',choices=['canonical-a','canonical-b','journal','nonempty'],required=True)
    p.add_argument('--build-dir',type=Path)
    p.add_argument('--image-pins',type=Path)
    p.add_argument('--observer-limits',type=Path)
    p.add_argument('--active-sha256')
    a=p.parse_args()
    home=Path(pwd.getpwuid(os.geteuid()).pw_dir)
    localdev=home/'localdev'
    if os.geteuid()!=a.expected_uid or home!=a.expected_home:
        raise RuntimeError('requires the exact actual disposable CI uid and getpwuid home; no HOME override')
    secure(home,True)
    secure(localdev,True)
    if not Path(__file__).resolve().is_relative_to(localdev) or not a.fixture_root.is_relative_to(localdev):
        raise RuntimeError('helper and all project fixture output must remain below actual ~/localdev')
    if a.mode=='package':
        if not a.build_dir or not a.image_pins or not a.observer_limits:raise RuntimeError('actual built image pins and independently selected finite limits are required')
        if any(not path.is_relative_to(localdev) for path in (a.build_dir,a.image_pins,a.observer_limits)):
            raise RuntimeError('build and project input files must remain below actual ~/localdev')
        pins=read_json(a.image_pins);limits=read_json(a.observer_limits)
        if set(limits)!=LIMIT_FIELDS or limits['format']!='lattice-observer-delivery-startup-v1':raise RuntimeError('exact finite delivery declaration required')
        for key in LIMIT_FIELDS-{'format'}:
            if type(limits[key]) is not int or limits[key]<0 or limits[key]>=2**64-1:raise RuntimeError('finite explicit policy required')
        if a.fixture_root.exists():raise RuntimeError('fresh nonexistent fixture output required')
        create_dir(a.fixture_root)
        package=a.fixture_root/'package';work=a.fixture_root/'work';cases=a.fixture_root/'cases';artifacts=a.fixture_root/'artifacts';temporary=a.fixture_root/'tmp'
        for d in (package,work,cases,artifacts,temporary):create_dir(d)
        names={'lattice-resource-launcher':'lattice-resource-launcher','LatticeResourceFactoryControls':'LatticeResourceFactoryControls','LatticeDurableInitializationChild':'LatticeDurableInitializationChild','LatticeResourceLegacyWriter':'LatticeResourceLegacyWriter'}
        if a.batch=='nonempty':names['LatticeCoreTests']='LatticeCoreTests'
        images={};source_images={}
        for label,name in names.items():
            source=a.build_dir/name;before=fact(source)
            if before['sha256']!=pins[label]:raise RuntimeError('Root explicit built image pin differs: '+label)
            target=package/name;shutil.copyfile(source,target);os.chmod(target,0o755)
            after=fact(source);copied=fact(target)
            if before!=after or copied['sha256']!=before['sha256']:raise RuntimeError('copy changed actual image identity/body')
            source_images[label]=before;images[label]={k:copied[k]for k in ('path','identity','sha256')}
        roles=[];containers=[];locators=[]
        def continuous(key):
            relative='.claude/sync/'+a.batch+'-'+key.lower().replace('_','-')+'.lattice-continuous'
            containers.append(relative);return str(home/relative/'store.sqlite')
        def role(name,label,args,env):
            # Immutable DATA selectors only; the original TMPDIR ban remains.
            environment={'TMP':str(temporary),'TEMP':str(temporary),'TEMPDIR':str(temporary),'SQLITE_TMPDIR':str(temporary),
                'LATTICE_TEST_LOG_PATH':str(artifacts/(name+'.log'))}
            if set(environment)&set(env):raise RuntimeError('duplicate fixture environment key')
            environment.update(env)
            roles.append({'name':name,'runtime':'native','image':images[label],'codeImages':[images[label]],'cwd':str(work),'arguments':[images[label]['path']]+args,'environment':[k+'='+v for k,v in environment.items()],'observerDelivery':limits})
        if a.batch.startswith('canonical-'):
            chosen=CANONICAL[:3]if a.batch=='canonical-a'else CANONICAL[3:]
            env={};filters=[]
            for name,keys in chosen:
                filters.append('InitializationChannel.'+name)
                for key in keys:env['LATTICE_RESOURCE_CONTINUOUS_FINAL_'+key+'_PATH']=continuous(key)
            role('canonical-controls','LatticeResourceFactoryControls',['--gtest_filter='+':'.join(filters),'--gtest_output=xml:'+str(artifacts/'canonical.xml'),'--gtest_color=no'],env)
        elif a.batch=='journal':
            parent_env={};donor=cases/'replay_donor'/'member-path.v1'
            for key,scenario in JOURNAL:
                selected=cases/key.lower();create_dir(selected);logical=continuous(key);name='journal-'+key.lower()
                locator=selected/'member-path.v1'
                env={'LATTICE_DURABLE_CASE_DIRECTORY':str(selected),'LATTICE_DURABLE_MEMBER_LOCATOR':str(locator)}
                if key=='REPLAY':env['LATTICE_DURABLE_ENROLLMENT_REPLAY_DONOR_LOCATOR']=str(donor)
                role(name,'LatticeDurableInitializationChild',[scenario,logical],env)
                locators.append({'logicalRoot':str(Path(logical).parent),'locator':str(locator)})
                parent_env['LATTICE_CONTINUOUS_ENROLLMENT_'+key+'_ROLE']=name
            role('journal-parent','lattice-resource-launcher',['test-run-installed-controls','--gtest_filter=DurableInitialization.ActualEnrollment*','--gtest_output=xml:'+str(artifacts/'journal-parent.xml'),'--gtest_color=no'],parent_env)
        else:
            env={'LATTICE_RESOURCE_CONTINUOUS_FINAL_NONEMPTY_CONTROLLER_PATH':continuous('controller'),'LATTICE_RESOURCE_CONTINUOUS_FINAL_NONEMPTY_SOURCE_PATH':str(home/'.claude/sync/store.sqlite')}
            role('nonempty-control','LatticeCoreTests',['--gtest_filter='+NONEMPTY,'--gtest_output=xml:'+str(artifacts/'nonempty.xml'),'--gtest_color=no'],env)
        if len(containers)>12:raise RuntimeError('fixture exceeds actual four ordinary plus twelve continuous root bound')
        body={'format':'lattice-resource-package-v1','host':images['lattice-resource-launcher'],'oldImages':[images['LatticeResourceLegacyWriter']],'roles':roles,'continuousContainers':containers}
        # The actual package parser requires byte equality with nlohmann's
        # ordered-key, compact UTF-8 dump, including the absence of a newline.
        encoded=json.dumps(body,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode('utf-8')
        if len(encoded)>128*1024:raise RuntimeError('fixture exceeds actual package frame bound')
        exclusive(package/'resource-package.v1',encoded)
        write_json(a.fixture_root/'PLAN.json',{'state':'ACTUAL_IMAGE_DATA_STAGED_NOT_INSTALLED','batch':a.batch,'actualHome':str(home),'actualUid':os.geteuid(),'projectRoot':str(localdev),'package':str(package),'sourceImages':source_images,'copiedImages':images,'locators':locators,'nativeIssued':False,'anchorWritten':False,'limitsAreExplicitInputNotRuntimeQualification':True})
        print(str(a.fixture_root/'PLAN.json'));return 0
    if a.batch!='journal':raise RuntimeError('only the journal batch requires member locator DATA')
    if not a.active_sha256:raise RuntimeError('Root must pin actual activated anchor bytes')
    plan=read_json(a.fixture_root/'PLAN.json');active_path=home/'.lattice-resource-installation/active.v1'
    if plan['state']!='ACTUAL_IMAGE_DATA_STAGED_NOT_INSTALLED' or plan['batch']!=a.batch or plan['actualHome']!=str(home) or plan['actualUid']!=os.geteuid() or plan['projectRoot']!=str(localdev) or plan['package']!=str(a.fixture_root/'package'):
        raise RuntimeError('exact staged fixture/account binding differs')
    active_fact=fact(active_path);secure(active_path,False,True)
    if active_fact['sha256']!=a.active_sha256:raise RuntimeError('actual activated anchor differs')
    active=read_json(active_path)
    if active['format']!='lattice-resource-installation-v1' or active['uid']!=os.geteuid() or active['home']!=str(home):raise RuntimeError('actual activated account binding differs')
    if active['host']!=plan['copiedImages']['lattice-resource-launcher']:raise RuntimeError('actual activated Host differs from observed package image')
    slot=hashlib.sha256(b'store.sqlite').hexdigest();observed=[]
    for item in plan['locators']:
        rows=[r for r in active['roots']if r['logicalPath']==item['logicalRoot']]
        if len(rows)!=1:raise RuntimeError('actual activated logical root absent/duplicate')
        row=rows[0];control=Path(row['controlPath']);s=secure(control,True,True)
        if row['control']!={'device':s.st_dev,'inode':s.st_ino}:raise RuntimeError('observed control identity differs from actual anchor DATA')
        if row['rule']!={'names':['store.sqlite'],'suffixes':[]}:raise RuntimeError('actual fixed-leaf root differs')
        member=control/slot;encoded=str(member).encode()
        if len(encoded)>4096:raise RuntimeError('actual selected member locator exceeds child bound')
        destination=Path(item['locator'])
        if not destination.is_relative_to(a.fixture_root/'cases'):raise RuntimeError('locator output escapes owned cases')
        exclusive(destination,encoded)
        observed.append({'locator':fact(destination),'control':{'path':str(control),'identity':row['control']},'memberPathOnly':str(member),'memberIdentityNotInvented':True})
    if fact(active_path)!=active_fact:raise RuntimeError('actual anchor changed while locators were produced')
    write_json(a.fixture_root/'LOCATORS.json',{'state':'ACTIVATED_ROOT_DATA_OBSERVED_LOCATORS_WRITTEN_NOT_AUTHORITY','active':active_fact,'observed':observed,'nativeIssued':False,'anchorWritten':False})
    print(str(a.fixture_root/'LOCATORS.json'));return 0

if __name__=='__main__':raise SystemExit(main())
