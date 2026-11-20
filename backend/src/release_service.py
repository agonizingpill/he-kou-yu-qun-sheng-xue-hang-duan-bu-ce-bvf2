import json,hashlib
from sqlalchemy import select
from .models import Cruise,Transect,Echo,BioSample,MissingInterval,SurveyRelease,SchoolReview,NetCalibration,Net
from .common import get,reject,finish,cruise_lock,dump
from .school_service import latest

def freeze(db,data,user):
    c=cruise_lock(db,data.cruise_id);reject(c.status!='surveyed','先完成缺测接续并结束现场调查')
    ts=list(db.scalars(select(Transect).where(Transect.cruise_id==c.id,Transect.status!='cancelled')));reject(not ts,'航次没有有效航段')
    layers=[]
    for t in ts:
        a=latest(db,t.id,True);reject(not a,'航段'+t.code+'的最新群落/噪点尚未复核')
        reject(db.scalar(select(MissingInterval).where(MissingInterval.transect_id==t.id,MissingInterval.status=='open')) is not None,'仍有缺测未完成补航')
        samples=list(db.scalars(select(BioSample).where(BioSample.transect_id==t.id,BioSample.analysis_id==a.id)));reject(not samples,'航段'+t.code+'缺少最新群落对应的生物样')
        echo_rows=list(db.scalars(select(Echo).where(Echo.transect_id==t.id).order_by(Echo.id)))
        layers.append({'transect':dump(t),
            # 下载件自带识别口径与点序，资源组脱离本航次库也可凭件复算。
            'clustering_rule':{'eps':a.eps,'min_samples':a.min_samples,'data_version':a.data_version,'analysis_id':a.id},
            'echo_order':[e.id for e in echo_rows],
            'echoes':[dump(e) for e in echo_rows],
            'analysis':dump(a),'review':dump(db.scalar(select(SchoolReview).where(SchoolReview.analysis_id==a.id))),
            'samples':[dump(s) for s in samples],'calibrations':[dump(get(db,NetCalibration,s.calibration_id)) for s in samples],'nets':[dump(get(db,Net,get(db,NetCalibration,s.calibration_id).net_id)) for s in samples]})
    snap={'cruise':dump(c),'layers':layers,'gaps':[dump(g) for g in db.scalars(select(MissingInterval).join(Transect).where(Transect.cruise_id==c.id))]}
    # JSON列只存可复现的JSON值，时间统一ISO格式。
    snap=json.loads(json.dumps(snap,default=str,ensure_ascii=False));canonical=json.dumps(snap,sort_keys=True,ensure_ascii=False,separators=(',',':'));sha=hashlib.sha256(canonical.encode('utf-8')).hexdigest()
    row=SurveyRelease(cruise_id=c.id,recipient=data.recipient,snapshot=snap,canonical_json=canonical,sha256=sha);db.add(row);c.status='frozen';db.flush();return finish(db,user,'封存调查图层',row)
