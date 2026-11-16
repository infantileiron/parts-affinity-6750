from sqlalchemy import Column,Integer,String,Float,JSON,Text,ForeignKey,UniqueConstraint
from src.config.database import Base
class User(Base):
 __tablename__='users'
 id=Column(Integer,primary_key=True);email=Column(String(100),unique=True,nullable=False);password=Column(String(200));role=Column(String(20))
class Job(Base):
 __tablename__='jobs'
 id=Column(Integer,primary_key=True);code=Column(String(60),unique=True,nullable=False);equipment=Column(String(40));parts=Column(JSON);note=Column(Text,default='');revision=Column(Integer,default=0)
class MiningRun(Base):
 __tablename__='mining_runs'
 id=Column(Integer,primary_key=True);name=Column(String(100));equipment=Column(String(40));min_support=Column(Float);min_confidence=Column(Float);min_lift=Column(Float);state=Column(String(20),default='queued');result=Column(JSON,nullable=True);error=Column(Text,default='')
class AssociationRule(Base):
 __tablename__='association_rules'
 id=Column(Integer,primary_key=True);run_id=Column(Integer,ForeignKey('mining_runs.id'));signature=Column(String(200));antecedent=Column(JSON);consequent=Column(JSON);support=Column(Float);confidence=Column(Float);lift=Column(Float);count=Column(Integer)
 __table_args__=(UniqueConstraint('run_id','signature'),)
class Kit(Base):
 __tablename__='kits'
 id=Column(Integer,primary_key=True);name=Column(String(100));rule_id=Column(Integer,ForeignKey('association_rules.id'));parts=Column(JSON);note=Column(Text);status=Column(String(20),default='draft')
class Audit(Base):
 __tablename__='audits'
 id=Column(Integer,primary_key=True);actor=Column(String(100));action=Column(String(200));created=Column(String(40))
class KitLedger(Base):
 __tablename__='kit_ledgers'
 id=Column(Integer,primary_key=True)
 kit_id=Column(Integer,ForeignKey('kits.id'),unique=True,nullable=False)
 equipment=Column(String(40))
 min_support=Column(Float);min_confidence=Column(Float);min_lift=Column(Float)
 antecedent=Column(JSON);consequent=Column(JSON)
 state=Column(String(20),default='building')
 valid_jobs=Column(Integer,default=0)
 antecedent_count=Column(Integer,default=0)
 consequent_count=Column(Integer,default=0)
 both_count=Column(Integer,default=0)
 support=Column(Float,default=0);confidence=Column(Float,default=0);lift=Column(Float,default=0)
 verdict=Column(String(20),default='below')
 baseline=Column(JSON,nullable=True)
 version=Column(Integer,default=0)
 processed=Column(Integer,default=0);total=Column(Integer,default=0)
 error=Column(Text,default='')
 updated=Column(String(40),default='')
class LedgerContrib(Base):
 __tablename__='ledger_contribs'
 id=Column(Integer,primary_key=True)
 kit_id=Column(Integer,ForeignKey('kits.id'),nullable=False)
 job_id=Column(Integer,ForeignKey('jobs.id'),nullable=False)
 job_revision=Column(Integer,default=0)
 in_scope=Column(Integer,default=0);ant=Column(Integer,default=0);cons=Column(Integer,default=0);both=Column(Integer,default=0)
 fingerprint=Column(String(64))
 __table_args__=(UniqueConstraint('kit_id','job_id'),)
