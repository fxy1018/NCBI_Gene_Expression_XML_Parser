'''
Created on Aug 16, 2017

@author: XFan
'''
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, Numeric, String, Text, DateTime, Boolean, ForeignKey, Float
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from binstar_client.requests_ext import NullAuth


# Return engine instances to create tables. 
def createEngine(user, password, ip, database):
    query = 'mysql+pymysql://' + user + ':' + str(password) + '@' + str(ip) + '/' + database
    try:
        engine = create_engine(query)
    except sqlalchemy.exc.DatabaseError:
        print("Can't connect mysql.")
    return engine

#create Base object
Base = declarative_base()

class Species(Base):
    __tablename__ = 'species'
    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False, unique=True)

class Project(Base):
    __tablename__ = 'project'
    id = Column(Integer, primary_key=True)
    project = Column(String(50), nullable=False, unique=True)
    description = Column(String(300))
    species_id = Column(Integer, ForeignKey('species.id'))
  
class Tissue(Base):
    __tablename__ = 'tissue'
  
    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False, unique=True)
  
class Sample(Base):
    __tablename__ = 'sample'
        
    id = Column(Integer, primary_key=True)
    sample = Column(String(50), nullable=False)
    description = Column(String(100))
    tax_id = Column(Integer) 
    project_id = Column(Integer, ForeignKey('project.id'))
    sra_id = Column(String(100))
    tissue_id = Column(Integer, ForeignKey('tissue.id'))
    exp_Mcount = Column(Integer)

class Expression(Base):
    #raw count gene expression table
    __tablename__ = 'expression'
    
    id = Column(Integer, primary_key=True)
    description = Column(String(100))
    gene = Column(Integer, nullable = False)
    project_id = Column(Integer, ForeignKey('project.id'), nullable=False)
    sample_id = Column(Integer, ForeignKey('sample.id'), nullable=False)
    exp_total = Column(Integer)
    full_rpkm = Column(Float) 
    exp_rpkm = Column(Float)
    
    
class ExpressionTissue(Base):
    #raw count gene expression table
    __tablename__ = 'expression_tissue'
    
    id = Column(Integer, primary_key=True)
    description = Column(String(100))
    gene = Column(Integer, nullable = False)
    tissue_id = Column(Integer, ForeignKey('tissue.id'), nullable=False)
    full_rpkm = Column(Float) 
    exp_rpkm = Column(Float)
    var = Column(Float)
    project_id = Column(Integer, ForeignKey('project.id'), nullable=False)

    
    
####################################################
if __name__ == "__main__":
    local = ['root', '1234', 'localhost', 'ncbi_gene_expression']
    serve = ['ironwood', 'irtest', '172.20.203.118:3306', 'ncbi_gene_expression']
    serve_local = ['ironwood', 'irtest', 'localhost', 'ncbi_gene_expression']
    engine = createEngine(*local)
    Base.metadata.create_all(engine)

