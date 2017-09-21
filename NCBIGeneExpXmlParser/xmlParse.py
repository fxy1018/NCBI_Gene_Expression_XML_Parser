from __future__ import generators
from bs4 import BeautifulSoup
import pymysql
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, Numeric, String, Text, DateTime, Boolean, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy import func
from datetime import datetime
import sys
import os.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from NCBIGeneExpXmlParser.createTable import *
import pandas as pd
import re
from collections import defaultdict
from fileParser.fileParser import *


################### Insertion functions start ##########################
class Doc(object):
    @staticmethod
    def factory(type, **kwargs):
        if type == "sample":
            return(SampleObj(**kwargs))
        if type == "summary":
            return(SummaryObj(**kwargs))
        if type == "expression":
            return(ExpressionObj(**kwargs))
        if type == "expressionTissue":
            return(ExpressionTissueObj(**kwargs))
    
        # Return session instances. 
    def createSession(self, user, password, ip, database):
        query = 'mysql+pymysql://' + user + ':' + str(password) + '@' + str(ip) + '/' + database
        try:
            engine = create_engine(query)
        except sqlalchemy.exc.DatabaseError:
            print("Can't connect mysql.")
        
        Session = sessionmaker(bind=engine)
        session = Session()
        return session
      
    def insertDoc(self):
        pass


class SampleObj(Doc):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
    
    def insertDoc(self, server):
        #input: conditions dict, experiment name as key, list of conditions as value
        # example: conditions = {'IWP0004JJ':['s1.sequencing.txt.gz', 's2.sequencing.txt.gz']}
        session = self.createSession(*server)
        tissueId = session.query(Tissue.id).filter(Tissue.name == self.source_name).scalar()
        projectId = session.query(Project.id).filter(Project.project == self.project_desc).scalar()
        sample = Sample(sample = self.sample_id, description = self.id, tax_id = self.taxid,
                        sra_id = self.sra_id, tissue_id = tissueId,
                        exp_Mcount = self.exp_Mcount, project_id = projectId)
        
        session.add(sample)
        session.commit()
    
class SummaryObj(Doc):
    def __init__(self,**kwargs):
        self.__dict__.update(kwargs)
    
    def insertDoc(self,server):
        pass 
        
class ExpressionObj(Doc):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
    
    def insertDoc(self, server):
        session = self.createSession(*server)
        sampleId = session.query(Sample.id).filter(Sample.sample == self.sample_id).scalar()
        projectId = session.query(Project.id).filter(Project.project == self.project_desc).scalar()
        expression = Expression(description = self.id, gene = self.gene, exp_total = self.exp_total,
                            full_rpkm = self.full_rpkm, exp_rpkm = self.exp_rpkm,
                            project_id = projectId, sample_id = sampleId)
        
        session.add(expression)
        session.commit()
              
class ExpressionTissueObj(Doc):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        
    def insertDoc(self, server):
        session = self.createSession(*server)
        tissueId = session.query(Tissue.id).filter(Tissue.name == self.source_name).scalar()
        projectId = session.query(Project.id).filter(Project.project == self.project_desc).scalar()
        expressionTissue = ExpressionTissue(description = self.id, gene = self.gene, var = self.var,
                            full_rpkm = self.full_rpkm, exp_rpkm = self.exp_rpkm,
                            project_id = projectId, tissue_id = tissueId)
        
        session.add(expressionTissue)
        session.commit()

def createSession(user, password, ip, database):
        query = 'mysql+pymysql://' + user + ':' + str(password) + '@' + str(ip) + '/' + database
        try:
            engine = create_engine(query)
        except sqlalchemy.exc.DatabaseError:
            print("Can't connect mysql.")
        
        Session = sessionmaker(bind=engine)
        session = Session()
        return session

def insertSpecies(species, server):
    #inpust: a list of insert species
    session = createSession(*server)
    for s in species:
        isExist = session.query(Species.id).filter(Species.name == s).scalar()
        if not isExist:
            spec = Species(name = s)
            session.add(spec)
        else:
            continue
    session.commit()

def insertProject(projects,server):
    #input: a dict containing all experiment information
    #example: exp = {}
    session = createSession(*server)
    for p in projects:
        isExist = session.query(Project.id).filter(Project.project == p['project_desc']).scalar()
        if not isExist:
            speciesId = session.query(Species.id).filter(Species.name == p['species']).scalar()
            project = Project(project = p['project_desc'], description = p['description'],
                                    species_id = speciesId)
            session.add(project)
        else:
            continue
    session.commit()

def insertTissues(tissues):
    #input: a list of insert tissues
    session = createSession(*server)
    for t in tissues:
        isExist = session.query(Tissue.id).filter(Tissue.name == t).scalar()
        if not isExist:
            tiss = Tissue(name = t)
            session.add(tiss)
        else:
            continue
    session.commit()

def parseDoc(doc, server):
#     print(d)
#     print(dir(d))
    fs = doc.findAll("field")
    attrs = [f.get('name') for f in fs]
    data = [f.get_text() for f in fs]
    content_pair = dict(zip(attrs, data))
    if "is_sample" in content_pair:
        obj = Doc.factory("sample",**content_pair)
    elif "entropy" in content_pair:
        obj = Doc.factory("summary",**content_pair)
    elif "var" in content_pair:
        obj = Doc.factory("expressionTissue",**content_pair)
    else:    
        obj = Doc.factory("expression",**content_pair)
    
    obj.insertDoc(server)
    print("------------------------")

if __name__=="__main__":
    local = ['root', '1234', 'localhost', 'ncbi_gene_expression']
    server = local
    
    #insert species 
    species = ['Homo sapiens', 'Mus musculus', 'Rattus norvegicus']
    insertSpecies(species, server)
 
    #insert project
    projects = [{"project_desc": "PRJEB4337" , "species": "Homo sapiens", "description": "HPA RNA-seq normal tissues"},
                {"project_desc": "PRJNA280600" , "species": "Homo sapiens", "description": "RNA sequencing of total RNA from 20 human tissues"},
                {"project_desc": "PRJEB2445" , "species": "Homo sapiens", "description": "Illumina bodyMap2 transcriptome"},
                {"project_desc": "PRJNA238328" , "species": "Rattus norvegicus", "description": "Rnor_6.0_106_expression"}]
    insertProject(projects,server)
        
    infile = open('test.xml', 'r')
    contents = infile.read()
    
    #get all tissues 
    tissues = set(re.findall("<field name=\"source_name\">(.+)</field>", contents))
    insertTissues(tissues)

    soup = BeautifulSoup(contents, 'lxml')
    docs = soup.findAll('doc')
    
    for doc in docs:
        parseDoc(doc, server)
