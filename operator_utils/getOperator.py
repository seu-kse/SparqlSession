import sys
import os
o_path = os.getcwd()
sys.path.append(o_path)


from file_utils.readfile import read_valuedSession
from file_utils.read_exported_data import sessions2Query2Text, readExportedData
from file_utils.writefile import write_pkl
from rdflib.plugins.sparql import prepareQuery
from rdflib.plugins.sparql.sparql import Query
from rdflib.plugins.sparql.algebra import translate, inner_traverse, inner_simplifyFilters
from rdflib.plugins.sparql import parser
from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.algebra import pprintAlgebra
from rdflib.plugins.sparql.parserutils import plist
from rdflib.plugins.sparql.algebra import translatePrologue
from rdflib.plugins.sparql.algebra import traverse
from rdflib.plugins.sparql.algebra import translatePName
from rdflib.plugins.sparql.algebra import translatePath
from rdflib.paths import Path, MulPath, SequencePath, AlternativePath
from rdflib.paths import InvPath, NegatedPath
from rdflib.term import Literal, URIRef, BNode, Variable
from rdflib.plugins.sparql.parserutils import CompValue
from pyparsing import ParseResults
from rdflib import RDF, RDFS, OWL, XSD
from rdflib import Namespace
from tqdm import tqdm
import functools

def parse_spl(query):
    pq = parser.parseQuery(query)    

    # using function in rdflib to absolutize/resolve prefixes
    # may cause prefix error
    prologue = translatePrologue(pq[0], None)
    # using function in rdflib to simplify filter
    inner_traverse(pq[1], inner_simplifyFilters)    
    pq[1] = traverse(
        pq[1], visitPost=functools.partial(translatePName, prologue=prologue))
    # using function in rdflib to translate path
    if 'where' in pq[1].keys():
        pq[1]['where'] = traverse(pq[1]['where'], visitPost=translatePath)
    return pq

def GetFeatureDBpedia(pq):
    node2visit = [pq]
    res = []
    while len(node2visit) != 0:
        current = node2visit.pop(0)
        if isinstance(current, ParseResults):
            for i in current:
                node2visit.insert(0, i)
        elif isinstance(current, list):
            for i in current:
                node2visit.insert(0, i)
        elif isinstance(current, CompValue) or isinstance(current, dict):
            res.append(current.name)
            for i, j in current.items():
                res.append(i)
                node2visit.insert(0, j)
        elif isinstance(current, (Literal, URIRef, BNode, Variable, str)):
            continue
        elif isinstance(current, (Path, MulPath, SequencePath, AlternativePath)):
            res.append(type(current))
        else:
            print(type(current))
    return res

def GetTexts(valuedSession, name):     
    conn = GetConn(name)         
    # get query texts
    queries = []
    for sessi in valuedSession:
        sess = sessi['session']
        queries.extend(sess['query'].tolist())
    queries = list(set(queries))

    texts = GetTextFromQueryID(conn, queries)
    query2text = {}
    for i, query in enumerate(queries):
        query2text[query] = texts[i][0]
    
    return query2text

def getQueryFeature(data, name, dbpedia=False, output_dir='results/operator'):

    feature_file = os.path.join(output_dir, f'{name}_feature.pkl')

    query2text = sessions2Query2Text(data)

    idx2query = {}
    for sessi in data:
        sess = sessi['queries']
        for idxi in range(len(sess)):
            index = sess[idxi]['index_in_file']
            if index not in idx2query.keys():
                idx2query[index] = query2text[index]

    query2feature = {}
    for idx, query in idx2query.items():
        try:
            pq = parse_spl(query)
        except:
            continue
        res = GetFeatureDBpedia(pq)
        query2feature[idx] = res
    
    write_pkl(feature_file, query2feature)

def GetFeatureFromText(text):
    try:
        pq = parse_spl(text)
    except:
        return False, 0
    res = GetFeatureDBpedia(pq)
    return True, res

if __name__ == "__main__":
    
    
    data_source =  ['dbpedia.3.5.1.log',
                    'access.log-20151025',
                    'access.log-20151124',
                    'access.log-20151126',
                    'access.log-20151213',
                    'access.log-20151230',
                    'access.log-20160117',
                    'access.log-20160212',
                    'access.log-20160222',
                    'access.log-20160301',
                    'access.log-20160303',
                    'access.log-20160304',
                    'access.log-20160314',
                    'access.log-20160411']
    
    data_source += ['ncbigene','ndc','orphanet','sgd','sider','swdf','affymetrix','goa', 'linkedgeodata']

    for i, name in tqdm(enumerate(data_source), leave=False, total=len(data_source)):
        dbpedia = True if i <= 13 else False
        data = readExportedData('docs/exportSession', name)
        getQueryFeature(data['sessions'], name, dbpedia=dbpedia)