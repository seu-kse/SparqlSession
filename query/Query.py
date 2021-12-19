import sys
import os
o_path = os.getcwd()
sys.path.append(o_path)

from parser_spl.proSPARQL_rdflib import parseStr, GetInfo, GetQueryForm, proQueryForm
from parser_spl.proSPARQL_rdflib import GetSPOfromParseTree, GetProjectionNum, GetBGPNum
from join.structure_utils import get_structure_info
import numpy as np

def GetResultSizeOrRuntime(idx, query2factor, key, dbpedia=False):
    queries = query2factor['query'].tolist()
    queryi = idx

    if queryi not in queries:
        return False, -1
    else:
        factori = query2factor.loc[query2factor['query']==queryi]
        if not dbpedia:
            return True, factori[key].tolist()[0]
        else:
            if factori[key].tolist()[0] == 'None':
                return False, -1
            else:
                return True, float(factori[key].tolist()[0])

def proStruc(nodes):
    node = []
    type_ = []
    join_degree = []
    for op, nodesi in nodes.items():
        for spo, struc_info in nodesi.items():
            if struc_info['type'] != 'no':
                type_.append(struc_info['type'])
                join_degree.append(struc_info['join_degree'])
                node.append(spo)
    return node, type_, join_degree

class Query(object):

    def __init__(self, idx, text):
        self.idx = idx
        self.text = text
        self.parseTree = None
        self.triples = None
        self.graph = None
        self.bind = None
        self.values = None
        self.service = None
        self.filter_info = None
        self.queryForm = ''
        self.NoParseError = None
        self.resultSize = -1
        self.projectionNum = -1
        self.termSet = None
        self.starNum = -1
        self.sinkNum = -1
        self.pathNum = -1
        self.hybridNum = -1
        self.tpNum = -1
        self.maxJoinDegree = -1
        self.meanJoinDegree = -1
        self.minJoinDegree = -1
        self.BGPNum = -1
        self.runtime = -1
        self.nodes = None
        self.join = None
        self.featureVec = None
    
    def fill(self, query2factor, dbpedia):
        self.parseTree = parseStr(self.text)
        self.queryForm = GetQueryForm(self.parseTree)
        self.triples, self.graph, self.bind, self.values, self.service, self.filter_info = proQueryForm(self.parseTree, additional=False)
        self.NoParseError, self.resultSize = GetResultSizeOrRuntime(self.idx, query2factor, 'resultSize', dbpedia=dbpedia)
        _, self.runtime = GetResultSizeOrRuntime(self.idx, query2factor, 'runtime', dbpedia=dbpedia)
        # runtime from ms -> s
        if not dbpedia:
            self.runtime = float(self.runtime) / 1000
        self.termSet = GetSPOfromParseTree(self.parseTree)
        if self.queryForm == 'SelectQuery':
            self.projectionNum = GetProjectionNum(self.parseTree, self.termSet)
        else:
            self.projectionNum = 0
        self.nodes = get_structure_info(self.triples)
        self.BGPNum = GetBGPNum(self.parseTree)
        count_tp = 0
        for op, tp in self.triples.items():
            count_tp += len(tp)
        self.tpNum = count_tp
        node, type_, join_degree = proStruc(self.nodes)
        self.sinkNum = type_.count('sink')
        self.pathNum = type_.count('path')
        self.hybridNum = type_.count('hybrid')
        self.starNum = type_.count('star')
        self.maxJoinDegree = np.array(join_degree).max() if len(join_degree) != 0 else 0
        self.minJoinDegree = np.array(join_degree).min() if len(join_degree) != 0 else 0
        self.meanJoinDegree = np.array(join_degree).mean() if len(join_degree) != 0 else 0
        self.join = {'node': node, 'type': type_, 'join_degree': join_degree}
        self.featureVec = np.array([self.tpNum, self.BGPNum, self.projectionNum, self.sinkNum,
                                    self.starNum, self.hybridNum, self.pathNum, self.maxJoinDegree,
                                    self.minJoinDegree, self.meanJoinDegree])
    
    def print_query(self):
        for name,value in vars(self).items():
            if name == 'nodes' or name == 'parseTree':
                continue
            print('%s=%s'%(name,value))