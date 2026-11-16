import os,time,json,logging
from pathlib import Path
import redis
from src.config.database import Session
from src.models.entities import MiningRun,AssociationRule,KitLedger
from src.services.analysis import mine
from src.services.ledger import rebuild_ledger,QUEUE as LEDGER_QUEUE
r=redis.Redis.from_url(os.environ['REDIS_URL'])
QUEUES=['affinity:runs',LEDGER_QUEUE]
def heartbeat():
 try:Path('/tmp/heartbeat').touch()
 except OSError:pass
def recover():
 """启动恢复：未完成的挖掘重新入队；重建中断（building）的台账重新入队，旧计数全程可读。"""
 with Session() as s:
  for pending in s.query(MiningRun).filter(MiningRun.state.in_(['queued','running'])).all():pending.state='queued';r.rpush('affinity:runs',pending.id)
  for l in s.query(KitLedger).filter_by(state='building').all():r.rpush(LEDGER_QUEUE,l.kit_id)
  s.commit()
def handle_run(run_id):
 with Session() as s:
  row=s.get(MiningRun,run_id)
  if not row or row.state!='queued':return
  snapshot=row.result['snapshot'];row.state='running';s.commit()
  try:
   result=mine(snapshot,row.min_support,row.min_confidence,row.min_lift)
   s.query(AssociationRule).filter_by(run_id=row.id).delete()
   for rule in result['rules']:s.add(AssociationRule(run_id=row.id,**rule))
   row.result={**result,'snapshot':snapshot};row.state='done'
   Path('/artifacts/'+str(row.id)+'.json').write_text(json.dumps(row.result,ensure_ascii=False))
  except Exception as e:row.state='failed';row.error=str(e)
  s.commit()
def handle_ledger(kit_id):
 with Session() as s:
  try:rebuild_ledger(s,kit_id,tick=heartbeat)
  except Exception as e:
   s.rollback()
   l=s.query(KitLedger).filter_by(kit_id=kit_id).first()
   if l:l.state='failed';l.error=str(e)[:500];s.commit()
   logging.exception('ledger rebuild failed kit=%s',kit_id)
def process(queue,key):
 if queue=='affinity:runs':handle_run(int(key))
 elif queue==LEDGER_QUEUE:handle_ledger(int(key))
def poll_once():
 item=r.blpop(QUEUES,timeout=3)
 if not item:return False
 queue=item[0].decode() if isinstance(item[0],bytes) else item[0]
 process(queue,item[1])
 return True
def drain():
 """处理两个队列中当前全部任务（供验证脚本驱动）。"""
 n=0
 for q in QUEUES:
  while True:
   item=r.lpop(q)
   if item is None:break
   process(q,item);n+=1
 return n
if __name__=='__main__':
 recover()
 while True:
  heartbeat()
  try:poll_once()
  except Exception:logging.exception('worker');time.sleep(2)
