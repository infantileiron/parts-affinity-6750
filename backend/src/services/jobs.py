from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException
from src.models.entities import Job
from src.services.business import dump
from src.services.ledger import sync_job
def save_job(s,data,i=None):
 r=s.get(Job,i) if i else Job()
 if not r:raise HTTPException(404,'维保作业不存在')
 for k,v in data.model_dump().items():setattr(r,k,v)
 r.revision=(r.revision or 0)+1
 s.add(r)
 try:
  s.flush()
  sync_job(s,r)
  s.commit()
 except IntegrityError:s.rollback();raise HTTPException(409,'作业编号重复')
 return dump(r)
