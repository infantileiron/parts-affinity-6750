import os,hashlib,logging
from datetime import datetime,timezone
import redis
from sqlalchemy import func
from src.config.database import Session
from src.models.entities import Job,Kit,KitLedger,LedgerContrib,AssociationRule,MiningRun
QUEUE='affinity:ledger'
BATCH=25
TOL=1e-12
def now():return datetime.now(timezone.utc).isoformat()
def fingerprint(job):return hashlib.sha1((str(job.equipment)+'|'+','.join(job.parts or [])).encode()).hexdigest()
def contribution(ledger,job):
 """单张作业对台账的贡献：有效作业、前件、后件、共同出现。空事务与范围外设备不计。"""
 parts=set(job.parts or [])
 if not parts or (ledger.equipment!='全部' and job.equipment!=ledger.equipment):return (0,0,0,0)
 ant=set(ledger.antecedent or [])<=parts;cons=set(ledger.consequent or [])<=parts
 return (1,int(ant),int(cons),int(ant and cons))
def recalc(ledger):
 """由计数器重算指标与达标标记，口径与挖掘一致（1e-12容差）。"""
 n=ledger.valid_jobs
 ledger.support=ledger.both_count/n if n else 0
 ledger.confidence=ledger.both_count/ledger.antecedent_count if ledger.antecedent_count else 0
 base=ledger.consequent_count/n if n else 0
 ledger.lift=ledger.confidence/base if base else 0
 ok=n>=2 and ledger.support+TOL>=ledger.min_support and ledger.confidence+TOL>=ledger.min_confidence and ledger.lift+TOL>=ledger.min_lift
 ledger.verdict='ok' if ok else 'below'
 ledger.updated=now()
def apply_job(s,ledger,job):
 """在持有台账行锁的事务内应用一张作业的当前状态。
 撤销旧贡献再计新贡献：按作业版本跳过过期状态，指纹一致时幂等跳过，保证并发重建与修订不漏计、不重复计。"""
 rev=job.revision or 0
 row=s.query(LedgerContrib).filter_by(kit_id=ledger.kit_id,job_id=job.id).first()
 if row and (row.job_revision or 0)>rev:return False
 fp=fingerprint(job);new=contribution(ledger,job)
 if row and row.job_revision==rev and row.fingerprint==fp and (row.in_scope,row.ant,row.cons,row.both)==new:return False
 old=(row.in_scope,row.ant,row.cons,row.both) if row else (0,0,0,0)
 ledger.valid_jobs+=new[0]-old[0];ledger.antecedent_count+=new[1]-old[1]
 ledger.consequent_count+=new[2]-old[2];ledger.both_count+=new[3]-old[3]
 if row:
  row.in_scope,row.ant,row.cons,row.both=new;row.fingerprint=fp;row.job_revision=rev
 else:
  s.add(LedgerContrib(kit_id=ledger.kit_id,job_id=job.id,job_revision=rev,in_scope=new[0],ant=new[1],cons=new[2],both=new[3],fingerprint=fp))
 recalc(ledger)
 return True
def sync_job(s,job):
 """作业录入/修订的增量路径：只动该作业的贡献行与计数器，不全扫历史。须在保存作业的事务内调用。"""
 for ledger in s.query(KitLedger).with_for_update().all():apply_job(s,ledger,job)
def rebuild_ledger(s,kit_id,tick=None):
 """全量重建：分批幂等刷新每张作业的贡献，计数器随批提交持续可读；与并发修订共用apply_job，交错安全。"""
 ledger=s.query(KitLedger).filter_by(kit_id=kit_id).with_for_update().first()
 if not ledger:raise ValueError('台账不存在')
 ledger.state='building';ledger.error='';ledger.processed=0
 ledger.total=s.query(func.count(Job.id)).scalar() or 0
 s.commit();s.expire_all()
 last=0
 while True:
  ids=[r[0] for r in s.query(Job.id).filter(Job.id>last).order_by(Job.id).limit(BATCH).all()]
  if not ids:break
  ledger=s.query(KitLedger).filter_by(kit_id=kit_id).with_for_update().first()
  for jid in ids:
   job=s.get(Job,jid)
   if job:apply_job(s,ledger,job)
  ledger.processed+=len(ids)
  s.commit();s.expire_all()
  last=ids[-1]
  if tick:tick()
 ledger=s.query(KitLedger).filter_by(kit_id=kit_id).with_for_update().first()
 recalc(ledger);ledger.version+=1;ledger.state='ready'
 s.commit()
 return ledger
def reconcile_ledger(s,ledger):
 """逐包对账：台账计数器 vs 贡献行汇总 vs 现场全量重算，同一事务快照内比较。"""
 agg=s.query(func.coalesce(func.sum(LedgerContrib.in_scope),0),func.coalesce(func.sum(LedgerContrib.ant),0),func.coalesce(func.sum(LedgerContrib.cons),0),func.coalesce(func.sum(LedgerContrib.both),0)).filter_by(kit_id=ledger.kit_id).one()
 ev=ea=ec=eb=0
 for job in s.query(Job).order_by(Job.id).all():
  v,a,c,b=contribution(ledger,job);ev+=v;ea+=a;ec+=c;eb+=b
 counters={'valid_jobs':ledger.valid_jobs,'antecedent_count':ledger.antecedent_count,'consequent_count':ledger.consequent_count,'both_count':ledger.both_count}
 contrib={'valid_jobs':int(agg[0]),'antecedent_count':int(agg[1]),'consequent_count':int(agg[2]),'both_count':int(agg[3])}
 recomputed={'valid_jobs':ev,'antecedent_count':ea,'consequent_count':ec,'both_count':eb}
 return {'kit_id':ledger.kit_id,'state':ledger.state,'version':ledger.version,'counters':counters,'contrib_sums':contrib,'recomputed':recomputed,'counters_match_contribs':counters==contrib,'counters_match_recomputed':counters==recomputed,'metrics':{'support':ledger.support,'confidence':ledger.confidence,'lift':ledger.lift,'verdict':ledger.verdict}}
def ensure_ledger(s,kit_id):
 """取台账；不存在则沿来源批次的设备范围与阈值、来源规则的前后件建立。"""
 l=s.query(KitLedger).filter_by(kit_id=kit_id).first()
 if l:return l
 kit=s.get(Kit,kit_id)
 if not kit:return None
 rule=s.get(AssociationRule,kit.rule_id)
 if not rule:return None
 run=s.get(MiningRun,rule.run_id)
 l=KitLedger(kit_id=kit_id,equipment=run.equipment,min_support=run.min_support,min_confidence=run.min_confidence,min_lift=run.min_lift,antecedent=list(rule.antecedent),consequent=list(rule.consequent),state='building',updated=now())
 s.add(l);s.commit()
 return s.query(KitLedger).filter_by(kit_id=kit_id).first()
def enqueue_rebuild(kit_id):
 try:
  redis.Redis.from_url(os.environ['REDIS_URL']).rpush(QUEUE,kit_id);return True
 except redis.RedisError:return False
def fail_queue(s,kit_id):
 l=s.query(KitLedger).filter_by(kit_id=kit_id).first()
 if l:l.state='failed';l.error='队列暂不可用，请恢复后从台账页重新发起重建';s.commit()
def baseline_snapshot(s,kit,ledger):
 """签核时原证据快照：台账可用时取台账，否则取来源规则指标；一经写入不再覆盖。"""
 if ledger and ledger.state=='ready':
  return {'source':'ledger','captured_at':now(),'version':ledger.version,'valid_jobs':ledger.valid_jobs,'antecedent_count':ledger.antecedent_count,'consequent_count':ledger.consequent_count,'both_count':ledger.both_count,'support':ledger.support,'confidence':ledger.confidence,'lift':ledger.lift,'verdict':ledger.verdict}
 rule=s.get(AssociationRule,kit.rule_id)
 return {'source':'rule','captured_at':now(),'run_id':rule.run_id,'count':rule.count,'support':rule.support,'confidence':rule.confidence,'lift':rule.lift}
def ensure_ledgers():
 """启动兜底：为缺少台账的存量备件包建账并入队重建。"""
 with Session() as s:
  new=[]
  for kit in s.query(Kit).all():
   if not s.query(KitLedger).filter_by(kit_id=kit.id).first():
    if ensure_ledger(s,kit.id):new.append(kit.id)
  for kid in new:
   if not enqueue_rebuild(kid):fail_queue(s,kid)
  if new:logging.info('ledgers ensured for kits %s',new)
