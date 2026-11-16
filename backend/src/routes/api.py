import os,json
import redis
from fastapi import APIRouter,Depends,HTTPException,Response
from src.config.database import db
from src.models.entities import User,Job,MiningRun,AssociationRule,Kit,KitLedger,Audit
from src.schemas.business import Login,JobInput,RunInput,KitInput,KitEdit
from src.middlewares.auth import current,editor,admin
from src.utils.security import verify,token
from src.services.business import audit,dump
from src.services.jobs import save_job
from src.services.ledger import ensure_ledger,enqueue_rebuild,fail_queue,baseline_snapshot,reconcile_ledger
from src.controllers.business import ensure_run,ensure_rule,ensure_kit
router=APIRouter()
@router.post('/auth/login')
def login(data:Login,s=Depends(db)):
 u=s.query(User).filter_by(email=data.email).first()
 if not u or not verify(data.password,u.password):raise HTTPException(401,'账号或密码错误')
 return {'token':token(u),'user':dump(u)}
@router.get('/me')
def me(u=Depends(current)):return dump(u)
@router.get('/jobs')
def jobs(u=Depends(current),s=Depends(db)):return [dump(r) for r in s.query(Job).order_by(Job.id).all()]
@router.post('/jobs')
def create_job(data:JobInput,u=Depends(editor),s=Depends(db)):
 r=save_job(s,data);audit(s,u,'录入作业 '+r['code']);return r
@router.put('/jobs/{i}')
def edit_job(i:int,data:JobInput,u=Depends(editor),s=Depends(db)):
 r=save_job(s,data,i);audit(s,u,'修订作业 '+str(i));return r
@router.get('/runs')
def runs(u=Depends(current),s=Depends(db)):return [dump(r) for r in s.query(MiningRun).order_by(MiningRun.id.desc()).all()]
@router.post('/runs')
def run(data:RunInput,u=Depends(editor),s=Depends(db)):
 rows=[dump(r) for r in s.query(Job).order_by(Job.id).all() if data.equipment=='全部' or r.equipment==data.equipment]
 r=MiningRun(**data.model_dump(),result={'snapshot':rows});s.add(r);s.commit()
 try:redis.Redis.from_url(os.environ['REDIS_URL']).rpush('affinity:runs',r.id)
 except redis.RedisError:r.state='failed';r.error='队列暂不可用，请恢复后新建挖掘';s.commit();raise HTTPException(503,r.error)
 audit(s,u,'提交挖掘 '+str(r.id));return dump(r)
@router.get('/runs/{i}')
def detail(i:int,u=Depends(current),s=Depends(db)):return dump(ensure_run(s,i))
@router.get('/rules')
def rules(run_id:int,u=Depends(current),s=Depends(db)):
 ensure_run(s,run_id);return [dump(r) for r in s.query(AssociationRule).filter_by(run_id=run_id).order_by(AssociationRule.lift.desc()).all()]
@router.get('/runs/{i}/export')
def export(i:int,u=Depends(current),s=Depends(db)):
 r=ensure_run(s,i)
 if r.state!='done':raise HTTPException(409,'仅成功挖掘可以导出')
 return Response(json.dumps(dump(r),ensure_ascii=False,indent=2),media_type='application/json',headers={'Content-Disposition':f'attachment; filename="parts-affinity-{i}.json"'})
@router.get('/kits')
def kits(u=Depends(current),s=Depends(db)):return [dump(r) for r in s.query(Kit).order_by(Kit.id.desc()).all()]
@router.post('/kits')
def kit(data:KitInput,u=Depends(editor),s=Depends(db)):
 rule=ensure_rule(s,data.rule_id)
 if ensure_run(s,rule.run_id).state!='done':raise HTTPException(409,'来源规则尚未完成')
 r=Kit(**data.model_dump(),parts=sorted(set(rule.antecedent+rule.consequent)));s.add(r);s.commit()
 ensure_ledger(s,r.id)
 if not enqueue_rebuild(r.id):fail_queue(s,r.id)
 audit(s,u,'起草备件包 '+str(r.id));return dump(r)
@router.put('/kits/{i}')
def edit_kit(i:int,data:KitEdit,u=Depends(editor),s=Depends(db)):
 r=ensure_kit(s,i)
 if r.status!='draft':raise HTTPException(409,'已签核备件包不可覆盖，需新建方案')
 r.name=data.name;r.note=data.note;s.commit();audit(s,u,'修改备件包 '+str(i));return dump(r)
@router.post('/kits/{i}/approve')
def approve(i:int,u=Depends(admin),s=Depends(db)):
 r=ensure_kit(s,i)
 if r.status!='draft':raise HTTPException(409,'已签核方案不可重复签核')
 r.status='approved'
 existed=s.query(KitLedger).filter_by(kit_id=i).first()
 l=ensure_ledger(s,i)
 if l and not existed and not enqueue_rebuild(i):fail_queue(s,i)
 if l and not l.baseline:l.baseline=baseline_snapshot(s,r,l)
 s.commit();audit(s,u,'签核备件包 '+str(i));return dump(r)
@router.get('/ledgers')
def ledgers(u=Depends(current),s=Depends(db)):return [dump(r) for r in s.query(KitLedger).order_by(KitLedger.kit_id).all()]
@router.get('/kits/{i}/ledger')
def ledger_detail(i:int,u=Depends(current),s=Depends(db)):
 ensure_kit(s,i)
 l=s.query(KitLedger).filter_by(kit_id=i).first()
 if not l:raise HTTPException(404,'该备件包台账尚未建立')
 return dump(l)
@router.post('/kits/{i}/ledger/rebuild')
def rebuild(i:int,u=Depends(editor),s=Depends(db)):
 ensure_kit(s,i)
 if s.query(KitLedger).filter_by(kit_id=i).first():
  n=s.query(KitLedger).filter(KitLedger.kit_id==i,KitLedger.state.in_(['ready','failed'])).update({'state':'building','error':''},synchronize_session=False)
  s.commit()
  if not n:raise HTTPException(409,'台账重建进行中，请稍候')
 else:
  if not ensure_ledger(s,i):raise HTTPException(409,'来源规则已失效，无法建立台账')
 if not enqueue_rebuild(i):
  fail_queue(s,i);raise HTTPException(503,'队列暂不可用，请恢复后重新发起重建')
 audit(s,u,'发起台账重建 备件包 '+str(i))
 return dump(s.query(KitLedger).filter_by(kit_id=i).first())
@router.get('/kits/{i}/ledger/reconcile')
def reconcile(i:int,u=Depends(current),s=Depends(db)):
 ensure_kit(s,i)
 l=s.query(KitLedger).filter_by(kit_id=i).first()
 if not l:raise HTTPException(404,'该备件包台账尚未建立')
 return reconcile_ledger(s,l)
@router.get('/audits')
def audits(u=Depends(admin),s=Depends(db)):return [dump(r) for r in s.query(Audit).order_by(Audit.id.desc()).limit(100)]
