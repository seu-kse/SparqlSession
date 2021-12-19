import sys
import os
o_path = os.getcwd()
sys.path.append(o_path)

from rdflib.plugins.sparql import prepareQuery
from rdflib.plugins.sparql.sparql import Query
from rdflib.plugins.sparql import parser
from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.algebra import pprintAlgebra
from rdflib.plugins.sparql.algebra import translate, inner_traverse, inner_simplifyFilters
from rdflib.plugins.sparql.parserutils import plist
from rdflib.plugins.sparql.algebra import translatePrologue
from rdflib.plugins.sparql.algebra import traverse
from rdflib.plugins.sparql.algebra import translatePName
from rdflib.plugins.sparql.algebra import translatePath
from rdflib.paths import Path, MulPath, SequencePath, AlternativePath
from rdflib.paths import InvPath, NegatedPath
from rdflib.term import Literal, URIRef, BNode, Variable
from rdflib import RDF, RDFS, OWL, XSD
from rdflib import Namespace
import functools
import rdflib
from tqdm import tqdm
from parser_spl.utils import GetAndRemovePart, clean, match

def GetQueryForm(pq):
    """
    return the query form. ask, construct, select, describe

    input: 
        pq, pyparsing.ParseResults. such as pq = parser.parseQuery(query)
    
    output:
        str 
    """
    return pq[1].name

def GetPrefix(pq):
    """
    return a dict of prefix.
    such as "PREFIX a : <https://www.baidu.com/a#>" generate -> {'a': rdf.term.Namespace(https://www.baidu.com/a#)}

    input: 
        pq, pyparsing.ParseResults. such as pq = parser.parseQuery(query)

    output:
        dict, str -> Namespace
    """
    default_prefix = [RDF, RDFS, OWL, XSD]
    upper_names = ['RDF', 'RDFS', 'OWL', 'XSD']
    lower_names = [i.lower() for i in upper_names]
    prefix = {}
    for i in pq[0]:
        if i.name == 'PrefixDecl':
            if 'prefix' in i.keys():
                prefix[i['prefix']] = Namespace(i['iri'])
            else:
                prefix[':'] = Namespace(i['iri'])
    for i in range(len(default_prefix)):
        if upper_names[i] not in prefix.keys():
            prefix[upper_names[i]] = Namespace(default_prefix[i])
        if lower_names[i] not in prefix.keys():
            prefix[lower_names[i]] = Namespace(default_prefix[i])
    return prefix

def proLoop(i, triples, graph, bind, values, service, filter_info, name2func, name2keyword, name2block, name):
    """
    process the parsing.Parsults, proLoop means that after some process, it's a iterator value.

    input:
        i: OrderedDict
        triples: dict. key means where this triple from, such as 'optional'
        graph: list, store the 'term' info 
        bind: list, store the 'Bind' info 
        values, list, store the 'InlineData' info
        name2func, dict, keyword in OrderedDict 2 function
        name2keyword, dict, keyword in OrderedDict 2 key in next dict, store in a dict to process.
        name2block, dict, keyword in OrderedDict 2 block name, aim to generate key in triples
        name, key used in triples
    
    output:
        the same as input
    """

    keyword = name2keyword[i.name]
    if keyword not in i.keys():
        return triples, graph, bind, values, service, filter_info
    ite = i[keyword]
    if type(ite) != list and type(ite) != plist:
        ite = [ite]
    for xi in ite:
        #try:
        namexi = GetName(name, name2block, xi.name)
        triples, graph, bind, values, service, filter_info = name2func[xi.name](xi, triples, graph, bind, values, service, 
                                                             filter_info, name2func, name2keyword, name2block, namexi)
        #except:
            #from ipdb import set_trace; set_trace()
    return triples, graph, bind, values, service, filter_info

def proGraphSER(i, triples, graph, bind, values, service, filter_info, name2func, name2keyword, name2block, name):
    """
    process graph and service

    input:
        the same as proLoop
    
    output:
        the same as proLoop
    """
    if i.name == 'GraphGraphPattern':
        graph.append(i['term'])
    if i.name == 'ServiceGraphPattern':
        service.append(i['term'])
    ite = i['graph']
    if type(ite) != list and type(ite) != plist:
        ite = [ite]
    for xi in ite:
        namexi = GetName(name, name2block, xi.name)
        triples, graph, bind, values, service, filter_info = name2func[xi.name](xi, triples, graph, bind, values, service, 
                                                             filter_info, name2func, name2keyword, name2block, namexi)
    return triples, graph, bind, values, service, filter_info

def proBindValues(i, triples, graph, bind, values, service, filter_info, name2func, name2keyword, name2block, name):
    """
    process bing and values.

    input and output are the same as proLoop.
    """
    if i.name == 'Bind':
        bind.append(i)
    if i.name == 'InlineData':
        values.append(i)
    return triples, graph, bind, values, service, filter_info

def proTriples(i, triples, graph, bind, values, service, filter_info, name2func, name2keyword, name2block, name):
    """
    process TripleBlock

    input and output are the same as proLoop.
    """
    for t in i['triples']:
        if name == '':
            name = 'main'
        if name not in triples.keys():
            triples[name] = []
        if len(t) % 3 != 0:
            from ipdb import set_trace; set_trace()
        for index in range(int(len(t)/3)):
            triples[name].append(t[index*3:(index+1)*3])

    return triples, graph, bind, values, service, filter_info

def proFilter(i, triples, graph, bind, values, service, filter_info, name2func, name2keyword, name2block, name):
    if name == '':
        name = 'main'
    if name not in filter_info.keys():
        filter_info[name] = []
    filter_info[name].append(i)
    return triples, graph, bind, values, service, filter_info    

def GetName(name, name2block, namei):
    """
    change name information, when mention the important keyword in name2block.keys(), add keyword to name

    input:
        name: str, the original name, the same as proLoop
        name2block, dict, the same as proLoop
        namei: str, the keyword we are currently processing 

    Notice:
        GroupOrUnionGraphPattern may be confused, it may not represent 'Union'
    """
    if namei not in name2block.keys():
        return name
    name = name + name2block[namei]
    return name

def GetWhereBlockInfo(whereblock, name2func, name2keyword, name2block, triples):
    """
    the main function to process a pyparsing.ParseResults

    input:
        whereblock: pq[1].where
        name2func, name2keyword, name2block are the same as proLoop

    output:
        the same as proLoop function
    """
    name2func, name2keyword, name2block = GetReadyForName2Dict()

    bind = []
    values = []
    graph = []
    service = []
    filter_info = {}
    name = '' 
    key_func = whereblock.name
    if key_func in name2func.keys():
        funci = name2func[whereblock.name]
    else:
        return triples, graph, bind, values, service, filter_info
    try:
        triples, graph, bind, values, service, filter_info = funci(whereblock, triples, graph, bind, values, service,
                                                                filter_info, name2func, name2keyword, name2block, name)
    except:
        from ipdb import set_trace; set_trace()
    return triples, graph, bind, values, service, filter_info

def GetReadyForName2Dict():
    func = [proLoop, proGraphSER, proBindValues, proTriples, proFilter]
    name2func = {'GroupGraphPatternSub': func[0], 'TriplesBlock': func[3], 'Bind': func[2], 'InlineData': func[2],
                'GraphGraphPattern': func[1], 'GroupOrUnionGraphPattern': func[0], 'OptionalGraphPattern': func[0],
                'MinusGraphPattern': func[0], 'SubSelect': func[0], 'ServiceGraphPattern': func[1], 'Filter': func[4]}
    name2keyword = {'GroupGraphPatternSub': 'part', 'OptionalGraphPattern': 'graph', 'GroupOrUnionGraphPattern': 'graph', 
                    'MinusGraphPattern': 'graph', 'SubSelect': 'where'}
    name2block = {'OptionalGraphPattern': 'Optional', 'GroupOrUnionGraphPattern': 'Union', 'MinusGraphPattern': 'Minus',
                   'SubSelect': 'Sub', 'GroupGraphPatternSub': 'Main', 'GraphGraphPattern': 'Graph', 'ServiceGraphPattern': 'Service'}
    return name2func, name2keyword, name2block

def GetFilter(query):
    """
    get filter of query, just original string from SPARQL query.

    input:
        query: str, SPARQL string
    
    output:
        filter_info: list
    """
    filter_info = []
    # only need to match keyword "filter"
    # because filter and filter exists' left and right are different
    keyword = ['filter', 'FILTER']
    left = ['(', '{']
    right = [')', '}']
    for i in range(len(keyword)):
        # print("i")
        # print(i)
        for j in range(len(left)):
            # print("j")
            # print(j)
            f, _ = GetAndRemovePart(keyword[i], left[j], right[j], query)
            if len(f) != 0:
                for fi in f:
                    filter_info.append(fi)
    return filter_info

def proQueryForm(pq, additional=True):
    """
    additional -> return additional describe variable?
    """
    form = GetQueryForm(pq)
    triples = {}
    filter_info = {}
    bind = []
    values = []
    graph = []
    service = []
    describe_var = []
    name2func, name2keyword, name2block = GetReadyForName2Dict()
    if form == 'ConstructQuery':
        if 'template' in pq[1].keys():
            triples['template'] = []
            for t in pq[1]['template']:
                if len(t)%3 != 0:
                    from ipdb import set_trace; set_trace()
                for index in range(int(len(t)/3)):
                    triples['template'].append(t[index*3:(index+1)*3])
    if form == 'DescribeQuery':
        if 'var' in pq[1].keys():
            for ti in pq[1]['var']:
                describe_var.append(ti)
    if 'where' in pq[1].keys():
        triples, graph, bind, values, service, filter_info = GetWhereBlockInfo(pq[1]['where'], name2func, name2keyword,
                                                    name2block, triples)
    if form == 'DescribeQuery' and additional:
        return triples, graph, bind, values, service, filter_info, describe_var
    else:
        return triples, graph, bind, values, service, filter_info

        
def GetSOFromTriple(triples, getBlankNode=False):
    """
    get subject and object from triples.

    input:
        triples, dict, result from GetWhereBlockInfo function

    output: 
        SO, unique list
    """
    notBnode = [rdflib.term.Literal, rdflib.term.URIRef, rdflib.term.Variable]
    SO = []
    for i, t in triples.items():
        # print(t)
        for ti in t:
            # print(ti)
            if type(ti[0]) in notBnode:
                SO.append(ti[0])
            elif type(ti[0]) != rdflib.term.BNode:
                print(type(ti[0]))
            if type(ti[2]) in notBnode:
                SO.append(ti[2])
            elif type(ti[2]) != rdflib.term.BNode:
                print(type(ti[2]))
            elif getBlankNode and isinstance(ti[0], rdflib.term.BNode):
                SO.append(ti[0])
            elif getBlankNode and isinstance(ti[2], rdflib.term.BNode):
                SO.append(ti[2])
    SO = list(set(SO))
    return SO

def GetPFromTriple(triples, getBlankNode=False):
    """
    get predicate from triples.

    input:
        triples, dict, result from GetWhereBlockInfo function

    output: 
        P, unique list
    """
    P = []
    notBnode = [rdflib.term.Literal, rdflib.term.URIRef, rdflib.term.Variable]
    types = [MulPath, SequencePath, AlternativePath, InvPath, NegatedPath]
    for i, t in triples.items():
        for ti in t:
            if type(ti[1]) in notBnode:
                P.append(ti[1])
            elif type(ti[1]) in types:
                res = proPropertyPath(ti[1])
                for resi in res:
                    P.append(resi)
            elif getBlankNode and isinstance(ti[1], rdflib.term.BNode):
                    P.append(ti[1])
            else:
                print(type(ti[1]))
    P = list(set(P))
    return P

def proPropertyPath(p):
    types = [MulPath, SequencePath, AlternativePath, InvPath, NegatedPath]
    notBnode = [rdflib.term.Literal, rdflib.term.URIRef, rdflib.term.Variable]
    # keyword = [path, args, args, arg, args]
    res = []
    if type(p) in notBnode:
        res.append(p)
    if type(p) in types:
        index = types.index(type(p))
        if index == 0:
            temp = p.path
        elif index == 3:
            temp = p.arg
        else:
            temp = p.args
        if index != 0 and index != 3:
            for pi in temp:
                pi_res = proPropertyPath(pi)
                for pj in pi_res:
                    res.append(pj)
        else:
            res.append(temp)
    return list(set(res))

def GetSharedSPO(triples1, triples2):
    """
    using the results from GetP/SOFromTriples, get the shared SPO

    input:
        triples: the same as GetPFromTriples
    
    output:
        a list with shared SPO
    """
    SO1 = GetSOFromTriple(triples1)
    P1 = GetPFromTriple(triples1)
    SO2 = GetSOFromTriple(triples2)
    P2 = GetPFromTriple(triples2)
    spo1 = SO1 + P1
    spo2 = SO2 + P2
    shared = [xi for xi in spo1 if xi in spo2]
    return shared

def GetSPOfromQuery(q, getBlankNode=False):
    pq = parser.parseQuery(q)  
    form = GetQueryForm(pq)

    # using function in rdflib to absolutize/resolve prefixes
    prologue = translatePrologue(pq[0], None)
    pq[1] = traverse(
        pq[1], visitPost=functools.partial(translatePName, prologue=prologue))

    # using function in rdflib to simplify filter
    inner_traverse(pq[1], inner_simplifyFilters)

    # using function in rdflib to translate path
    if 'where' in pq[1].keys():
        pq[1]['where'] = traverse(pq[1]['where'], visitPost=translatePath)

    if form == 'DescribeQuery':
        triples, graph, bind, values, service, filter_info, describe_var = proQueryForm(pq)
    else:
        triples, graph, bind, values, service, filter_info = proQueryForm(pq)
    
    so = GetSOFromTriple(triples, getBlankNode=getBlankNode)
    p = GetPFromTriple(triples, getBlankNode=getBlankNode)

    if form == 'DescribeQuery':
        so = so + describe_var
    return list(set(so + p))


def GetSPOfromParseTree(pq, getBlankNode=False):
    form = GetQueryForm(pq)
    if form == 'DescribeQuery':
        triples, graph, bind, values, service, filter_info, describe_var = proQueryForm(pq)
    else:
        triples, graph, bind, values, service, filter_info = proQueryForm(pq)
    
    so = GetSOFromTriple(triples, getBlankNode=getBlankNode)
    p = GetPFromTriple(triples, getBlankNode=getBlankNode)

    if form == 'DescribeQuery':
        so = so + describe_var
    return list(set(so + p))


def GetSPO_rdflib(q, pattern, predicate=True):
    """
    get spo list.

    input:
        q1, one raw in DataFrame, keys: ip, time, query
        pattern: result from GetPattern function

    output:
        a list of SPO
    """
    fo = open('error_query_now.txt', 'w')

    pq = parser.parseQuery(q['query'])
    # write the query cannot be processed now.
    try:
        fo.write(q['query'])
        fo.write('\n')
        fo.write(str(pq))
        fo.write('\n')
    except:
        pass
    
    form = GetQueryForm(pq)

    # using function in rdflib to absolutize/resolve prefixes
    prologue = translatePrologue(pq[0], None)
    pq[1] = traverse(
        pq[1], visitPost=functools.partial(translatePName, prologue=prologue))

    # using function in rdflib to translate path
    if 'where' in pq[1].keys():
        pq[1]['where'] = traverse(pq[1]['where'], visitPost=translatePath)

    # write the query cannot be processed now.
    try:
        fo.write(str(pq))
        fo.write('\n')
    except:
        pass

    if form == 'DescribeQuery':
        triples, graph, bind, values, service, filter_info, describe_var = proQueryForm(pq)
    else:
        triples, graph, bind, values, service, filter_info = proQueryForm(pq)
    
    so = GetSOFromTriple(triples)
    if predicate:
        p = GetPFromTriple(triples)
    else:
        p = []

    if form == 'DescribeQuery':
        so = so + describe_var
    
    fo.close()
    return list(set(so + p))

def hasParseError_rdflib(q, parse_path=True):
    """
    if has parse error, return true.

    input:
        q, the same as GetSPO_rdflib, one row from DataFrame with key: query
    output:
        boolean
    """
    try:
        # from ipdb import set_trace; set_trace()
        pq = parser.parseQuery(q['query'])    

        if parse_path:
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

        return False

    except:
        #from ipdb import set_trace; set_trace()
        return True

def parseStr(query):
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


def GetInfo(query):
    pq = parser.parseQuery(query)
    prologue = translatePrologue(pq[0], None)
    pq[1] = traverse(
        pq[1], visitPost=functools.partial(translatePName, prologue=prologue))

    # using function in rdflib to simplify filter
    inner_traverse(pq[1], inner_simplifyFilters)
    
    # using function in rdflib to translate path
    if 'where' in pq[1].keys():
        pq[1]['where'] = traverse(pq[1]['where'], visitPost=translatePath)

    info = proQueryForm(pq, additional=False)
    return info

def GetProjectionNum(parsetree, termSet):
    if 'projection' in parsetree[1].keys():
        return len(parsetree[1]['projection'])
    else:
        count = 0
        for i in termSet:
            if isinstance(i, Variable):
                count += 1
        return count

def GetBGPNum(parsetree):
    return str(parsetree[1]).count('TriplesBlock_')

if __name__ == "__main__":
    data_source = ['service_logs_bio2rdf.txt', 'SWDF_sample.csv', 'DBpedia.csv', 'LGD_sample.csv']

    for i, data in tqdm(enumerate(data_source), leave=False):
        print('Reading ' + data + ' file ...')
        if i==0:
            df = read_file()
        # else:
            # df = readLSQ_csv(data)
        sum_ = 0
        error_with_parse_path = 0
        error_without_parse_path = 0
        for ith in tqdm(range(len(df))):
            sum_ += 1
            if hasParseError_rdflib(df.iloc[ith], parse_path=False):
                error_without_parse_path += 1
                error_with_parse_path += 1
            elif hasParseError_rdflib(df.iloc[ith]):
                # from ipdb import set_trace; set_trace()
                error_with_parse_path += 1
        print(data + ' parse error count: ')
        print('error with parse path: ' + str(error_with_parse_path))
        print('error without parse path ' + str(error_without_parse_path))