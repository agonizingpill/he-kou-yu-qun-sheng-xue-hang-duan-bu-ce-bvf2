"""图层封存收件与离库复算：端到端规则验证（SQLite，不依赖PG/Redis/外部下载件）。

覆盖规则：
1 封存件自带分群规则与回波顺序，离库可凭件复算；
2 群号变化、成员相同照收；
3 摘要一致仍逐点核对，归错群整箱退回并指出航段；
4 旧件缺复算依据 → pending 待补；
5 整件验过才入账，同件重试幂等不重复收件；
6 同航次两份不同图层：先入账算数，另一份 409 冲突。
另含 canonical_json/SHA256 篡改拒收与 FastAPI 路由层 multipart 上传验证。
"""
import os,sys,json,hashlib,tempfile,copy
from datetime import datetime,date
os.environ['DATABASE_URL']='sqlite:///'+tempfile.mkdtemp()+'/intake.db'
os.environ['REDIS_URL']='redis://127.0.0.1:6399/0'  # 收件链路不实际连接
sys.path.insert(0,'/workspace/backend')

from fastapi.testclient import TestClient
from sqlalchemy import select
from src.database import Base,engine,Session
from src import models
from src.models import User,Boat,Cruise,Transect,Echo,SchoolAnalysis,SchoolReview,Net,NetCalibration,BioSample,LayerIntake
from src.algorithm import compute_clustering
from src import release_service,intake_service
from src.auth import password_hash
from src.app import app

PASS=0;FAIL=0;RESULTS=[]
def check(name,cond,extra=''):
    global PASS,FAIL
    RESULTS.append({'name':name,'ok':bool(cond),'extra':str(extra)})
    print(('  ✓ ' if cond else '  ✗ ')+name+('' if not extra else '  → '+str(extra)))
    if cond:PASS+=1
    else:FAIL+=1

with Session() as db:
    Base.metadata.create_all(engine)
    sup=User(username='supervisor',password=password_hash('x'),role='supervisor')
    op=User(username='operator',password=password_hash('x'),role='operator')
    db.add_all([sup,op]);db.flush()
    boat=Boat(code='T1',name='潮研一号');db.add(boat);db.flush()
    # 两个航次：CY-A 正常入账；CY-B 用于同航次冲突
    cruises=[]
    for code in ('CY-A','CY-B'):
        c=Cruise(code=code,boat_id=boat.id,depart=datetime.fromisoformat('2026-11-18T14:15:00+00:00'),arrive=datetime.fromisoformat('2026-11-18T17:00:00+00:00'),purpose='河口鱼群声学航段与补测',status='surveyed')
        db.add(c);db.flush();cruises.append(c)
        net=Net(code='NET-'+code,mesh_mm=10);db.add(net);db.flush()
        cal=NetCalibration(net_id=net.id,measured_mm=10.1,valid_until=date(2026,12,31),certificate='CERT-'+code,passed=True);db.add(cal);db.flush()
        # 两条航段，各 30 个结构清晰的回波点：两簇各12点 + 6噪点
        for tcode,lat0 in (('E1',30.10),('E2',30.11)):
            t=Transect(cruise_id=c.id,code=tcode,start_lon=120.10,start_lat=lat0,end_lon=120.11,end_lat=lat0,length_m=1000.0,probe_khz=120,gain_db=36.0,data_version=1,status='reviewed')
            db.add(t);db.flush()
            pts=[]
            for k in range(12):  # 簇1
                pts.append({'along_m':180+k,'lon':120.1002+k*0.00001,'lat':lat0,'depth_m':7.0+k*0.02,'strength_db':-46.0})
            for k in range(12):  # 簇2
                pts.append({'along_m':510+k,'lon':120.1002+k*0.00001,'lat':lat0,'depth_m':14.0+k*0.02,'strength_db':-39.0})
            for k in range(6):   # 噪点
                pts.append({'along_m':50+k*150.0,'lon':120.1005+k*0.0001,'lat':lat0,'depth_m':20.0+k,'strength_db':-70.0})
            for p in pts:db.add(Echo(transect_id=t.id,source='test',**p))
            db.flush()
            echoes=list(db.scalars(select(Echo).where(Echo.transect_id==t.id).order_by(Echo.id)))
            points=[{'id':e.id,'lon':e.lon,'lat':e.lat,'depth_m':e.depth_m,'strength_db':e.strength_db} for e in echoes]
            assignments,_=compute_clustering(points,1.2,4)
            a=SchoolAnalysis(transect_id=t.id,data_version=1,eps=1.2,min_samples=4,status='reviewed',
                             assignments=assignments,metrics={'noise_count':sum(1 for x in assignments if x['label']==-1)})
            db.add(a);db.flush()
            db.add(SchoolReview(analysis_id=a.id,noise_note='底质干扰造成的孤立回波，现场已核实',decision='同意按当前识别签署'))
            labels={x['label'] for x in assignments if x['label']>=0}
            db.add(BioSample(transect_id=t.id,analysis_id=a.id,calibration_id=cal.id,cluster_label=min(labels),species='模拟鲻鱼',count=12,mean_length_mm=125.0))
    db.commit()

class R:  # 模拟ReleaseInput
    def __init__(self,cruise_id,recipient):self.cruise_id=cruise_id;self.recipient=recipient

def build_file(cruise_id):
    with Session() as db:
        row=release_service.freeze(db,R(cruise_id,'潮汐渔研站资源调查组'),sup)
        return row

print('\n[1] 封存：新快照自带 clustering_rule 与 echo_order')
rel_a=build_file(cruises[0].id)
snap=json.loads(rel_a['canonical_json'])
for layer in snap['layers']:
    check(f"航段{layer['transect']['code']}含分群规则",layer['clustering_rule']['eps']==1.2 and layer['clustering_rule']['min_samples']==4)
    check(f"航段{layer['transect']['code']}含回波顺序且与点数一致",len(layer['echo_order'])==len(layer['echoes'])==30)
raw_a=json.dumps({'snapshot':snap,'canonical_json':rel_a['canonical_json'],'sha256':rel_a['sha256']},ensure_ascii=False)

print('\n[2] 离库复算：不查原航次库，仅凭下载件，整件通过并入账')
with Session() as db:
    row=intake_service.intake(db,raw_a,'CY-A-survey-layer.json',sup)
    check('状态 accepted',row['status']=='accepted',row['status'])
    check('两航段全部 passed',all(x['result']=='passed' for x in row['checks']),row['checks'])
    intake_a_id=row['id']

print('\n[3] 同件重试：幂等，不重复收件')
with Session() as db:
    before=db.scalar(select(__import__('sqlalchemy').func.count()).select_from(LayerIntake))
    try:intake_service.intake(db,raw_a,'CY-A-survey-layer.json',sup);check('重试应被拒',False)
    except Exception as e:check('重试返回409且提示不重复收件',e.status_code==409 and '不重复收件' in e.detail,e.detail)
    after=db.scalar(select(__import__('sqlalchemy').func.count()).select_from(LayerIntake))
    check('收件记录数未增加',before==after==1)

print('\n[4] 群号变了、成员相同：照收')
snap_relabel=copy.deepcopy(snap)
for layer in snap_relabel['layers']:
    # 把所有非噪点群号整体 +9，噪点-1不变
    for a in layer['analysis']['assignments']:
        if a['label']>=0:a['label']+=9
canon=json.dumps(snap_relabel,sort_keys=True,ensure_ascii=False,separators=(',',':'))
raw_relabel=json.dumps({'snapshot':snap_relabel,'canonical_json':canon,'sha256':hashlib.sha256(canon.encode()).hexdigest()},ensure_ascii=False)
with Session() as db:
    # 同航次已有accepted，先验证成员一致逻辑本身（直接调复算），再用CY-B验证可入账
    ok,status,checks,detail=intake_service.verify_snapshot(snap_relabel)
    check('重标号复算通过',ok and status=='accepted',detail)
    check('明细提示忽略群号差异',all('群号差异已忽略' in x['detail'] for x in checks))

print('\n[5] 摘要对得上但归错群：逐点核对失败，整箱退回并指出航段')
snap_bad=copy.deepcopy(snap)
snap_bad['cruise']['code']='CY-RET'  # 独立航次代号，避免与CY-A已入账件冲突
target=snap_bad['layers'][0]  # E1
# 交换两个回波点的群归属（两点都属不同非噪点簇），簇数/噪点数等摘要完全不变
m={a['echo_id']:a for a in target['analysis']['assignments']}
cluster_points=[eid for eid,a in m.items() if a['label']>=0]
labels=sorted({a['label'] for a in m.values() if a['label']>=0})
g1=[eid for eid in cluster_points if m[eid]['label']==labels[0]][:2]
m[g1[0]]['label'],m[g1[1]]['label']=labels[1],labels[1]
canon=json.dumps(snap_bad,sort_keys=True,ensure_ascii=False,separators=(',',':'))
raw_bad=json.dumps({'snapshot':snap_bad,'canonical_json':canon,'sha256':hashlib.sha256(canon.encode()).hexdigest()},ensure_ascii=False)
with Session() as db:
    ok,status,checks,detail=intake_service.verify_snapshot(snap_bad)
    check('复算判 returned',not ok and status=='returned',detail)
    e1=[x for x in checks if x['transect']=='E1'][0];e2=[x for x in checks if x['transect']=='E2'][0]
    check('指出航段E1归错群',e1['result']=='returned' and '归错群' in e1['detail'] and g1[0] in e1['mismatched_echo_ids'],e1)
    check('未动航段E2仍逐点通过',e2['result']=='passed')
    row=intake_service.intake(db,raw_bad,'CY-RET-tampered.json',sup)
    check('退回件登记为 returned 而非入账',row['status']=='returned')

print('\n[6] 旧件缺复算依据：pending 待补，不入账')
snap_old=copy.deepcopy(snap)
snap_old['cruise']['code']='CY-OLD'  # 独立航次代号
for layer in snap_old['layers']:layer.pop('clustering_rule',None);layer.pop('echo_order',None)
canon=json.dumps(snap_old,sort_keys=True,ensure_ascii=False,separators=(',',':'))
raw_old=json.dumps({'snapshot':snap_old,'canonical_json':canon,'sha256':hashlib.sha256(canon.encode()).hexdigest()},ensure_ascii=False)
with Session() as db:
    row=intake_service.intake(db,raw_old,'CY-OLD-legacy.json',sup)
    check('旧件状态 pending',row['status']=='pending',row['detail'])
    check('逐航段标注待补',all(x['result']=='pending' for x in row['checks']))

print('\n[7] 同航次两份不同图层：先验过入账的算数，另一份报冲突')
rel_b=build_file(cruises[1].id)
snap_b=json.loads(rel_b['canonical_json'])
raw_b=json.dumps({'snapshot':snap_b,'canonical_json':rel_b['canonical_json'],'sha256':rel_b['sha256']},ensure_ascii=False)
with Session() as db:
    first=intake_service.intake(db,raw_b,'CY-B-v1.json',sup)
    check('CY-B 首份 accepted',first['status']=='accepted')
    # 制造 CY-B 的不同图层（合法但内容改动：改 recipient 快照文本 → 不同 sha）
    snap_b2=copy.deepcopy(snap_b);snap_b2['cruise']['purpose']=snap_b2['cruise']['purpose']+'（修订）'
    canon2=json.dumps(snap_b2,sort_keys=True,ensure_ascii=False,separators=(',',':'))
    raw_b2=json.dumps({'snapshot':snap_b2,'canonical_json':canon2,'sha256':hashlib.sha256(canon2.encode()).hexdigest()},ensure_ascii=False)
    try:intake_service.intake(db,raw_b2,'CY-B-v2.json',sup);check('第二份应冲突拒收',False)
    except Exception as e:check('第二份返回409冲突',e.status_code==409 and '冲突' in e.detail,e.detail)
    accepted=[r for r in db.scalars(select(LayerIntake).where(LayerIntake.cruise_code=='CY-B'))]
    check('CY-B 仅一份 accepted 且为首件',len([r for r in accepted if r.status=='accepted'])==1 and accepted[0].source_filename=='CY-B-v1.json')

print('\n[8] 校验和/原文篡改：拒绝收件')
with Session() as db:
    tampered=json.loads(raw_a);tampered['sha256']='0'*64
    try:intake_service.intake(db,json.dumps(tampered),'bad-sha.json',sup);check('错误sha应拒收',False)
    except Exception as e:check('sha不符返回400',e.status_code==400 and '校验和不符' in e.detail,e.detail)
    try:intake_service.intake(db,'{not json','broken.json',sup);check('坏JSON应拒收',False)
    except Exception as e:check('坏JSON返回400',e.status_code==400)
    try:intake_service.intake(db,json.dumps({'snapshot':{}}),'nofield.json',sup);check('缺字段应拒收',False)
    except Exception as e:check('缺canonical字段返回400',e.status_code==400)
    # 回波点被删掉关键字段：应判退回而非500
    snap_mal=copy.deepcopy(snap);snap_mal['cruise']['code']='CY-MAL'
    del snap_mal['layers'][0]['echoes'][0]['depth_m']
    canon=json.dumps(snap_mal,sort_keys=True,ensure_ascii=False,separators=(',',':'))
    raw_mal=json.dumps({'snapshot':snap_mal,'canonical_json':canon,'sha256':hashlib.sha256(canon.encode()).hexdigest()},ensure_ascii=False)
    row=intake_service.intake(db,raw_mal,'CY-MAL.json',sup)
    check('缺字段回波判 returned 不报错',row['status']=='returned' and row['checks'][0]['result']=='returned',row['checks'][0])

print('\n[9] 路由层：multipart 上传 + 权限 + 列表')
client=TestClient(app)
tok=client.post('/auth/login',json={'username':'supervisor','password':'x'}).json()['token']
tok_op=client.post('/auth/login',json={'username':'operator','password':'x'}).json()['token']
r=client.post('/intakes',files={'file':('route-CY-A.json',raw_a,'application/json')},headers={'Authorization':'Bearer '+tok})
check('已入账件经路由重试→409',r.status_code==409 and '不重复收件' in r.json()['detail'],r.status_code)
r=client.post('/intakes',files={'file':('route-CY-B2.json',raw_b2,'application/json')},headers={'Authorization':'Bearer '+tok})
check('冲突件经路由→409',r.status_code==409,r.json().get('detail'))
r=client.post('/intakes',files={'file':('x.json',raw_b2,'application/json')},headers={'Authorization':'Bearer '+tok_op})
check('operator 无权收件→403',r.status_code==403,r.status_code)
r=client.get('/intakes',headers={'Authorization':'Bearer '+tok})
check('收件列表可查且含三类状态',r.status_code==200 and {x['status'] for x in r.json()}>={'accepted','returned','pending'},[(x['cruise_code'],x['status']) for x in r.json()])

print(f'\n结果：{PASS} 通过 / {FAIL} 失败')
from pathlib import Path
Path('/workspace/test-artifacts/gb-1037/intake-evidence.json').write_text(json.dumps({'passed':PASS,'failed':FAIL,'checks':RESULTS},ensure_ascii=False,indent=2))
sys.exit(1 if FAIL else 0)
