import json,hashlib
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from .models import LayerIntake
from .common import finish
from .algorithm import compute_clustering

def _canonical_sha(canonical:str):return hashlib.sha256(canonical.encode('utf-8')).hexdigest()

def parse_package(raw:str):
    """解析下载件并对原始canonical_json的UTF-8字节复算SHA256，不信任浏览器重序列化。"""
    try:doc=json.loads(raw)
    except json.JSONDecodeError:raise HTTPException(400,'不是合法JSON图层文件')
    if not isinstance(doc,dict) or 'canonical_json' not in doc or 'sha256' not in doc:raise HTTPException(400,'缺少canonical_json或sha256字段，非本系统冻结图层')
    canonical=doc['canonical_json']
    if not isinstance(canonical,str):raise HTTPException(400,'canonical_json必须为字符串原文')
    actual=_canonical_sha(canonical)
    if actual!=doc['sha256']:raise HTTPException(400,f'校验和不符：canonical_json UTF-8字节SHA256={actual}，文件声明={doc["sha256"]}')
    try:snap=json.loads(canonical)
    except json.JSONDecodeError:raise HTTPException(400,'canonical_json无法还原为快照对象')
    if snap!=doc.get('snapshot',snap):raise HTTPException(400,'snapshot与canonical_json还原结果不一致，拒绝收件')
    return snap,actual

def _members(assignments):
    """群号无关：按回波成员集合比较，群号变了成员相同视为一致。"""
    groups={}
    for item in assignments:groups.setdefault(item['label'],set()).add(item['echo_id'])
    return {frozenset(ids) for ids in groups.values()}

def verify_snapshot(snap):
    """逐航段复算。返回(是否通过, 状态, 逐点核对明细, 汇总信息)。"""
    cruise_code=(snap.get('cruise') or {}).get('code','?')
    layers=snap.get('layers')
    if not isinstance(layers,list) or not layers:return False,'returned',[],'快照无航段图层'
    checks=[];pending=False;returned=False;total_points=0;total_clusters=0
    for layer in layers:
        code=(layer.get('transect') or {}).get('code','?')
        rule=layer.get('clustering_rule');order=layer.get('echo_order');echoes=layer.get('echoes');stored=((layer.get('analysis') or {}).get('assignments'))
        check={'transect':code,'echo_count':len(echoes) if isinstance(echoes,list) else 0}
        # 旧件缺复算依据：待补，不退件但不入账
        if not rule or not order or not isinstance(order,list) or not echoes or not stored:
            check.update(result='pending',detail='缺分群规则、回波顺序或回波点，旧件无复算依据，待补')
            checks.append(check);pending=True;continue
        by_id={e['id']:e for e in echoes}
        if sorted(order)!=sorted(by_id) or len(order)!=len(set(order)):
            check.update(result='returned',detail='回波顺序与回波点集合不一致，无法按序复算')
            checks.append(check);returned=True;continue
        points=[by_id[i] for i in order]
        missing=[k for k in ('id','lon','lat','depth_m','strength_db') if any(k not in p for p in points)]
        if missing:
            check.update(result='returned',detail='回波点缺复算字段：'+','.join(missing))
            checks.append(check);returned=True;continue
        try:
            re_labels,input_sha=compute_clustering(points,rule['eps'],rule['min_samples'])
        except (KeyError,TypeError,ValueError):
            check.update(result='returned',detail='分群规则参数或回波点数值异常，无法复算')
            checks.append(check);returned=True;continue
        check['input_sha256']=input_sha
        snap_members=_members(stored);recalc_members=_members(re_labels)
        # 摘要先对得上，仍逐点核对成员
        if snap_members==recalc_members:
            check.update(result='passed',detail=f'{len(recalc_members)}个分群成员逐点一致（群号差异已忽略）',cluster_count=len(recalc_members))
            total_points+=len(points);total_clusters+=len(recalc_members);checks.append(check);continue
        # 归错群：找出归群发生变化的回波点并指出航段
        snap_map={a['echo_id']:a['label'] for a in stored};recalc_map={a['echo_id']:a['label'] for a in re_labels}
        mismatched=[eid for eid in sorted(snap_map) if snap_map.get(eid)!=recalc_map.get(eid)]
        check.update(result='returned',detail=f'归错群：{len(mismatched)}个回波点分群与复算不符（首点echo_id={mismatched[0] if mismatched else "-"}）',mismatched_echo_ids=mismatched[:50])
        checks.append(check);returned=True
    if returned:return False,'returned',checks,f'航次{cruise_code}存在归错群航段，整箱退回'
    if pending:return False,'pending',checks,f'航次{cruise_code}旧件缺复算依据，待补后重交'
    return True,'accepted',checks,f'航次{cruise_code}全部{len(checks)}航段复算通过：{total_points}回波点/{total_clusters}分群'

def intake(db,raw:str,filename:str,user):
    snap,file_sha=parse_package(raw)
    cruise_code=(snap.get('cruise') or {}).get('code','?')
    # 幂等：同件重试直接返回既有收件，不重复入账
    prior=db.scalar(select(LayerIntake).where(LayerIntake.file_sha256==file_sha))
    if prior is not None:
        raise HTTPException(409,f'该图层已收件（存根#{prior.id}，状态{prior.status}），重试不重复收件')
    # 同航次两份不同图层：先验过并入账的算数
    accepted=db.scalar(select(LayerIntake).where(LayerIntake.cruise_code==cruise_code,LayerIntake.status=='accepted'))
    if accepted is not None:
        raise HTTPException(409,f'航次{cruise_code}已有入账图层（存根#{accepted.id}，{accepted.file_sha256[:12]}），本件报冲突拒收')
    ok,status,checks,detail=verify_snapshot(snap)
    row=LayerIntake(cruise_code=cruise_code,file_sha256=file_sha,status=status,source_filename=filename[:160],checks=checks,detail=detail[:500])
    db.add(row)
    try:db.flush()
    except IntegrityError:
        db.rollback()
        clash=db.scalar(select(LayerIntake).where(LayerIntake.file_sha256==file_sha)) or db.scalar(select(LayerIntake).where(LayerIntake.cruise_code==cruise_code,LayerIntake.status=='accepted'))
        raise HTTPException(409,f'并发收件冲突：存根#{clash.id if clash else "?"}已先行登记')
    action={'accepted':'图层复算通过并入账','returned':'图层复算退回','pending':'图层缺复算依据待补'}[status]
    return finish(db,user,action,row)
