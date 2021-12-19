from agraph_utils import GetConn, query_agraph
from franz.openrdf.model.literal import Literal
import numpy as np

def URILibrary():
    lib = {}
    # -----------------Query Form--------------------
    lib['Ask'] = 'http://spinrdf.org/sp#Ask'
    lib['Select'] = 'http://spinrdf.org/sp#Select'
    lib['Construct'] = 'http://spinrdf.org/sp#Construct'
    lib['Describe'] = 'http://spinrdf.org/sp#Describe'

    # -----------------Operator-----------------------
    lib['Optional'] = 'http://spinrdf.org/sp#Optional'
    lib['Minus'] = 'http://spinrdf.org/sp#Minus'
    lib['Union'] = 'http://spinrdf.org/sp#Union'
    lib['Service'] = 'http://spinrdf.org/sp#Service'
    lib['NamedGraph'] = 'http://spinrdf.org/sp#NamedGraph'
    lib['SubQuery'] = 'http://spinrdf.org/sp#SubQuery'
    lib['Values'] = 'http://spinrdf.org/sp#Values'
    lib['Bind'] = 'http://spinrdf.org/sp#Bind'
    lib['Filter'] = 'http://spinrdf.org/sp#Filter'
    lib['NotExists'] = 'http://spinrdf.org/sp#NotExists'
    lib['Exists'] = 'http://spinrdf.org/sp#Exists'

    # ------------property path---------------------
    lib['TriplePath'] = 'http://spinrdf.org/sp#TriplePath'
    lib['AltPath'] = 'http://spinrdf.org/sp#AltPath'
    lib['ModPath'] = 'http://spinrdf.org/sp#ModPath'
    lib['SeqPath'] = 'http://spinrdf.org/sp#SeqPath'
    lib['ReversePath'] = 'http://spinrdf.org/sp#ReversePath'

    # ----------property path features-----------------
    # TriplePath
    lib['path'] = 'http://spinrdf.org/sp#path'

    # AltPath SeqPath
    lib['path1'] = 'http://spinrdf.org/sp#path1'
    lib['path2'] = 'http://spinrdf.org/sp#path2'

    # ReversePath ModPath
    lib['subPath'] = 'http://spinrdf.org/sp#subPath'

    # ModPath
    lib['modMax'] = 'http://spinrdf.org/sp#modMax'
    lib['modMin'] = 'http://spinrdf.org/sp#modMin'


    # ------------Operator Feature--------------------
    # union minus optional graph service
    lib['elements'] = 'http://spinrdf.org/sp#elements'
    # graph
    lib['graphNameNode'] = 'http://spinrdf.org/sp#graphNameNode'
    # values
    lib['varNames'] = 'http://spinrdf.org/sp#varNames'
    lib['bindings'] = 'http://spinrdf.org/sp#bindings'
    # bind
    lib['variable'] = 'http://spinrdf.org/sp#variable'
    # filter bind
    lib['expression'] = 'http://spinrdf.org/sp#expression'
    # service
    lib['serviceURI'] = 'http://spinrdf.org/sp#serviceURI'
    # subQuery
    lib['query'] = 'http://spinrdf.org/sp#query'

    # ---------------Query Form Feature----------------
    lib['resultNodes'] = 'http://spinrdf.org/sp#resultNodes'

    # ------------------utils-----------------------
    lib['type'] = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
    lib['nil'] = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#nil'
    lib['first'] = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#first'
    lib['rest'] = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#rest'
    lib['where'] = 'http://spinrdf.org/sp#where'
    lib['args'] = 'spinrdf.org/sp#arg'

    # ----------------Structure  Features-------------------
    lib['hasSF'] = 'http://lsq.aksw.org/vocab#hasStructuralFeatures'

    # -------------------Spin---------------------------
    lib['hasSpin'] = 'http://lsq.aksw.org/vocab#hasSpin'

    # --------------------Triples--------------------- 
    lib['hasTP'] = 'http://lsq.aksw.org/vocab#hasTP'
    lib['s'] = 'http://spinrdf.org/sp#subject'
    lib['p'] = 'http://spinrdf.org/sp#predicate'
    lib['o'] = 'http://spinrdf.org/sp#object'
    lib['label'] = 'http://www.w3.org/2000/01/rdf-schema#label'
    lib['var_name'] = 'http://spinrdf.org/sp#varName'
    return lib

def QueryFormLib(conn):
    form = {}
    lib = URILibrary()
    form['Ask'] = conn.createURI(lib['Ask'])
    form['Select'] = conn.createURI(lib['Select'])
    form['Construct'] = conn.createURI(lib['Construct'])
    form['Describe'] = conn.createURI(lib['Describe'])
    return form

def getQueryForm(q, conn, error_fo, addResultNodes=False):
    # q -> spin -> type
    lib = URILibrary()
    q_id = conn.createURI(q)
    hasSpin = conn.createURI(lib['hasSpin'])
    type_ = conn.createURI(lib['type'])
    resultNodes = conn.createURI(lib['resultNodes'])
    statements = conn.getStatements(q_id, hasSpin, None)
    form = QueryFormLib(conn)

    with statements:
        if len(statements) != 1:
            # q -> spin : has more than 1 spin
            # from ipdb import set_trace; set_trace()
            error_fo.write('<%s><sep>has more than 1 spin.\n' % q)
            return None
        for statement in statements:
            spin = statement.getObject()
            types = conn.getStatements(spin, type_, None)
            if len(types) != 1:
                # spin -> type : has more than 1 type
                # from ipdb import set_trace; set_trace()
                error_fo.write('<%s><sep>has more than 1 type(select, describe, ask, construct).\n' % q)
                return None
            with types:
                for t in types:
                    query_form = t.getObject()

            # get query form
            if query_form == form['Ask']:
                res = 'Ask'
            elif query_form == form['Select']:       
                res = 'Select'
            elif query_form == form['Construct']:
                res = 'Construct'
            elif query_form == form['Describe']:
                res = 'Describe'
            else:
                # new query form?
                # from ipdb import set_trace; set_trace()
                error_fo.write('<%s><sep>has different query forms.\n' % q)
                return None

            # if query form == 'Describe', then add resultNodes
            if res == 'Describe' and addResultNodes:
                res_nodes = conn.getStatements(spin, resultNodes, None)
                if len(res_nodes) > 1:
                    # spin -> resultNodes : has more than 1 
                    # from ipdb import set_trace; set_trace()
                    error_fo.write('<%s><sep>has more than 1 resultNodes.\n' % q)
                    return None
                # if equals to 0, then return
                if len(res_nodes) != 0:
                    with res_nodes:
                        for r_node in res_nodes:
                            root = r_node.getObject()
                            nodes = []
                            nodes = traverse_node(root, conn, nodes, error_fo, q)
                    return res, nodes
    return res

    

def getTerms(q, conn, error_fo):
    """
    get all the terms in a query.
    including terms (literals, variables, URI terms) in BGPs and Construct template, Describe resultNodes.

    input:
        q, query_id in lsq dataset, string, such as 'http://lsq.aksw.org/res/q-96b029d3'.
        conn, connect object in agraph server.
        error_fo, write error file.
    output:
        a list of terms.
            URI term : return in format like <URI>, string.
            variables: 
            variables : return in format like "subject", string.
    """
    term = []
    lib = URILibrary()
    q_id = conn.createURI(q)
    hasSF = conn.createURI(lib['hasSF'])
    hasTP = conn.createURI(lib['hasTP'])
    s = conn.createURI(lib['s'])
    p = conn.createURI(lib['p'])
    o = conn.createURI(lib['o'])
    spo = [s, p, o]
    statements = conn.getStatements(q_id, hasSF, None)
    if len(statements) != 1:
        error_fo.write('<%s><sep>has more than 1 structure feature. \n' % q)
        return None
    with statements:
        for statement in statements:
            sf = statement.getObject()
            tps = conn.getStatements(sf, hasTP, None)
            with tps:
                for tp in tps:
                    tp = tp.getObject()
                    for i in spo:
                        terms_ = conn.getStatements(tp, i, None)
                        with terms_:
                            for term_ in terms_:
                                term_ = term_.getObject()
                                # judge term type
                                temp = judgeTermType(q, term_, conn, error_fo)
                                if temp == None:
                                    return None
                                if isinstance(temp, list):
                                    for ti in temp:
                                        if ti not in term:
                                            term.append(ti)
                                else:
                                    if temp not in term:
                                        term.append(temp)
  
    temp = getQueryForm(q, conn, error_fo, addResultNodes=True)
    if temp == None:
        return None
    if len(temp) == 2:
        for t in temp[1]:
            if t not in term:
                term.append(t)

    # if query form == 'Describe', add resultNodes
    
    return list(set(term))


def judgeTermType(q, term, conn, error_fo):
    # Literal
    if isinstance(term, Literal):
        return str(term)
    lib = URILibrary()
    var_name = conn.createURI(lib['var_name']) 
    type_ = conn.createURI(lib['type'])
    AltPath = conn.createURI(lib['AltPath'])
    ModPath = conn.createURI(lib['ModPath'])
    SeqPath = conn.createURI(lib['SeqPath'])
    ReversePath = conn.createURI(lib['ReversePath'])

    # AltPath SeqPath
    path2 = conn.createURI(lib['path2'])
    path1 = conn.createURI(lib['path1'])

    # ReversePath ModPath
    subPath = conn.createURI(lib['subPath'])

    statements = conn.getStatements(term, None, None)
    # URI term
    if len(statements) == 0:
        return str(term)
    else:
        p = []
        o = []
        with statements:
            for statement in statements:
                p.append(statement.getPredicate())
                o.append(statement.getObject())
        # variable
        if var_name in p:
            ith = p.index(var_name)
            return str(o[ith])
        elif type_ in p:
            ith = p.index(type_)
            node_type = o[ith]
            res = []
            if node_type == AltPath or node_type == SeqPath:
                # path 1
                ith = p.index(path1)
                temp = judgeTermType(q, o[ith], conn, error_fo)
                if temp == None:
                    return None
                if isinstance(temp, list):
                    for ti in temp:
                        if ti not in res:
                            res.append(ti)
                else:
                    if temp not in res:
                        res.append(temp)
                # path2
                ith = p.index(path2)
                temp = judgeTermType(q, o[ith], conn, error_fo)
                if temp == None:
                    return None
                if isinstance(temp, list):
                    for ti in temp:
                        if ti not in res:
                            res.append(ti)
                else:
                    if temp not in res:
                        res.append(temp)
                return res
            elif node_type == ReversePath or node_type == ModPath:
                ith = p.index(subPath)
                temp = judgeTermType(q, o[ith], conn, error_fo)
                if temp == None:
                    return None
                if isinstance(temp, list):
                    for ti in temp:
                        if ti not in res:
                            res.append(ti)
                else:
                    if temp not in res:
                        res.append(temp)
                return res
            else:
                error_fo.write('<%s><sep>has other property path.\n' % q)
                return None
                # other property path?
                # from ipdb import set_trace; set_trace()
        else:
            # not literal, variable or URI term 
            # watch p and o.
            # term ?p ?o
            # from ipdb import set_trace; set_trace()
            error_fo.write('<%s><sep>has other term type %s.(not literal, variable or URI)\n' % (q, term))
            return None


def traverse_node(root, conn, nodes, error_fo, q):
    """
    used to collect variables or nodes in resultVariables/resultNodes
    
    input:
        root: spin -> resultNodes/resultVariables (at start)
        conn, agraph connect server
        nodes, list, store collected nodes
    
    output:
        nodes
    """
    lib = URILibrary()
    nil = conn.createURI(lib['nil'])
    rest = conn.createURI(lib['rest'])
    first = conn.createURI(lib['first'])
    if root == nil:
        return nodes
    next_ = hasNext(root, conn)
    if next_ == None:
        temp = judgeTermType(q, root, conn, error_fo)
        if temp == None:
            return None
        if isinstance(temp, list):
            for ti in temp:
                if ti not in nodes:
                    nodes.append(ti)
        else:
            if temp not in nodes:
                nodes.append(temp)
        return nodes
    p = next_[0]
    o = next_[1]
    if first in p:
        ith = p.index(first)
        nodes = traverse_node(o[ith], conn, nodes, error_fo, q)
        if nodes == None:
            return None
        ith = p.index(rest)
        nodes = traverse_node(o[ith], conn, nodes, error_fo, q)
        if nodes == None:
            return None
        return nodes
    else:
        temp = judgeTermType(q, root, conn, error_fo)
        if temp == None:
            return None
        if isinstance(temp, list):
            for ti in temp:
                if ti not in nodes:
                    nodes.append(ti)
        else:
            if temp not in nodes:
                nodes.append(temp)
        return nodes


def hasNext(root, conn):
    """
    test if there is next
    if True, return list of 'p' and 'o'
    else, return None
    """
    statements = conn.getStatements(root, None, None)
    if len(statements) == 0:
        return None
    else:
        p = []
        o = []
        with statements:
            for statement in statements:
                p.append(statement.getPredicate())
                o.append(statement.getObject())
        return p, o

def hasNext_dict(root, conn, error_fo, q):
    """
    test if there is next
    if True, return a dict {'p': 'o'}
    else, return None
    """
    if isinstance(root, str):
        if '<' in root:
            root = conn.createURI(root[1:-1])
        else:
            root = conn.createURI(root)
    statements = conn.getStatements(root, None, None)
    if len(statements) == 0:
        return None
    else:
        res = {}
        with statements:
            for statement in statements:
                p = statement.getPredicate()
                o = statement.getObject()
                if str(p) in res.keys():
                    error_fo.write('<%s><sep>node %s have more than one %s' % (q, root, str(p)))
                    return None
                res[str(p)] = o
        if res == {}:
            res = None
        return res

def getTermFromTriple(triple, conn, error_fo, q):
    """
    when handling property path, terms in property path can only be gotten in spin tree.

    input:
        triple, 
            query_id -> hasSF -> hasTP -> (tp)
            (spin) -> type -> TriplePath
            content in () can be input
    output:
        terms, a list of terms
    """
    lib = URILibrary()
    type_ = conn.createURI(lib['type'])
    label = conn.createURI(lib['label'])
    temp = hasNext(triple, conn)
    res = []
    if temp != None:
        p = temp[0]
        o = temp[1]
        # from ipdb import set_trace; set_trace()
        for i in range(len(p)):
            if p[i] == type_ or p[i] == label:
                continue
            terms = judgeTermType(q, o[i], conn, error_fo)
            if terms == None:
                return None
            elif isinstance(terms, list):
                for termi in terms:
                    if termi not in res:
                        res.append(termi)
            else:
                if terms not in res:
                    res.append(terms)
        try:
            return list(set(res))
        except:
            from ipdb import set_trace; set_trace()
    else:
        error_fo.write('<%s><sep>getTermFromTriple input %s do not have next.\n' % (q, triple))
        return None

def getTriples(tp, conn, error_fo, q):
    """
    from tp(spin node), with 'p' like [label, subject, predicate, object]

    input:
        query_id -> hasSF -> hasTP -> (tp)
        (spin) -> type -> TriplePath
        content in () can be input
    output:
        triple, represented by a list with length of 3
    """
    if isinstance(tp, str):
        if '<' in tp:
            tp = conn.createURI(tp[1:-1])
        else:
            tp = conn.createURI(tp)

    triple = list(np.zeros(3))
    lib = URILibrary()
    s = conn.createURI(lib['s'])
    pp = conn.createURI(lib['p'])
    oo = conn.createURI(lib['o'])
    path = conn.createURI(lib['path'])
    type_ = conn.createURI(lib['type'])
    label = conn.createURI(lib['label'])
    temp = hasNext(tp, conn)
    if temp != None:
        p = temp[0]
        o = temp[1]
        if label in p:
            # s, p, o
            try:
                ith = p.index(s)
                triple[0] = o[ith]
                ith = p.index(pp)
                triple[1] = o[ith]
                ith = p.index(oo)
                triple[2] = o[ith]
            except:
                error_fo.write('<%s><sep>getTriples input %s get [s, p, o] fail. \n' % (q, tp))
                return None
        elif type_ in p:
            # s, path, o
            try:
                # from ipdb import set_trace; set_trace()
                ith = p.index(s)
                triple[0] = o[ith]
                ith = p.index(path)
                triple[1] = o[ith]
                ith = p.index(oo)
                triple[2] = o[ith]
            except:
                error_fo.write('<%s><sep>getTriples input %s get [s, path, o] fail. \n' % (q, str(tp)))
                return None             
    else:
        error_fo.write('<%s><sep>getTriples input %s do not have next. \n' % (q, tp))
        return None       
    return triple


def traverse_triple(root, conn, info, name, error_fo, q, test=False):
    if test:
        print('root: %s' % str(root))
        print(info)
    """
    traverse tree to get triples and basic infos.

    input:
        root: query_id -> hasSpin (at start)
        conn: agraph connect server
        info: list, [triples, filter, bind, service, values, graph, other]
                    [{}, {}, [], [], [], [], {}]
              triples: dict, keys: Optional, Union, Minus, NamedGraph, Main, SubQuery(..+)
                             item: triple pattern id
              filter: dict, keys: Optional, Union, Minus, NamedGraph, Main, SubQuery(..+)
                             item: filter spin id
              other: dict, store modifier such as 'limit'
              bind: list, item -> bind spin id
              values: list, item -> values spin id
              graph: list, item -> graph spin id             
              service, list, item -> service spin id    
        name: str, used to mark which block this triple(filter) belong to.
    output:
        output              
    """
    lib = URILibrary()
    type_ = conn.createURI(lib['type'])
    nil = conn.createURI(lib['nil'])
    rest = conn.createURI(lib['rest'])
    first = conn.createURI(lib['first'])
    Optional = conn.createURI(lib['Optional'])
    Minus = conn.createURI(lib['Minus'])
    Union = conn.createURI(lib['Union'])
    Service = conn.createURI(lib['Service'])
    NamedGraph = conn.createURI(lib['NamedGraph'])
    SubQuery = conn.createURI(lib['SubQuery'])
    Values = conn.createURI(lib['Values'])
    Bind = conn.createURI(lib['Bind'])
    Filter = conn.createURI(lib['Filter'])
    elements = conn.createURI(lib['elements'])
    query = conn.createURI(lib['query'])
    label = conn.createURI(lib['label'])
    Ask = conn.createURI(lib['Ask'])
    Describe = conn.createURI(lib['Describe'])
    Construct = conn.createURI(lib['Construct'])
    Select = conn.createURI(lib['Select'])
    forms = [Ask, Describe, Construct, Select]
    where = conn.createURI(lib['where'])
    TriplePath = conn.createURI(lib['TriplePath'])
    path = conn.createURI(lib['path'])
    NotExists = conn.createURI(lib['NotExists'])
    Exists = conn.createURI(lib['Exists'])

    # from ipdb import set_trace; set_trace()
    if root == nil:
        return info
    next_ = hasNext(root, conn)
    if next_ == None:
        return info
    p = next_[0]
    o = next_[1]
    # spin node, first and rest
    if first in p:
        for pi in p:
            if pi == first or pi == rest:
                ith = p.index(pi)
                info = traverse_triple(o[ith], conn, info, name, error_fo, q)
        return info
    # have type in p
    elif type_ in p:
        ith = p.index(type_)
        node_type = o[ith]
        # just add info then return
        if node_type == Bind:
            info[2].append(str(root))
            return info
        elif node_type == Values:
            info[4].append(str(root))
            return info
        elif node_type == Filter:
            if name == '':
                name = 'Main'
            if name not in info[1].keys():
                info[1][name] = []
            info[1][name].append(str(root))
            return info
        # property path, add to triples
        elif node_type == TriplePath:
            if name == '':
                name = 'Main'
            if name not in info[0].keys():
                info[0][name] = []
            info[0][name].append(str(root))
            return info
        # to corrspoind elements
        elif node_type == Optional:
            name += 'Optional'
            if elements not in p:
                return info
            ith = p.index(elements)
            ele = o[ith]
            info = traverse_triple(ele, conn, info, name, error_fo, q)
            return info
        elif node_type == Minus:
            name += 'Minus'
            if elements not in p:
                return info
            ith = p.index(elements)
            ele = o[ith]
            info = traverse_triple(ele, conn, info, name, error_fo, q)
            return info  
        elif node_type == NotExists:
            name += 'NotExists'
            if elements not in p:
                return info
            ith = p.index(elements)
            ele = o[ith]
            info = traverse_triple(ele, conn, info, name, error_fo, q)
            return info  
        elif node_type == Exists:
            name += 'Exists'
            if elements not in p:
                return info
            ith = p.index(elements)
            ele = o[ith]
            info = traverse_triple(ele, conn, info, name, error_fo, q)
            return info  
        elif node_type == Union:
            name += 'Union'
            if elements not in p:
                return info
            ith = p.index(elements)
            ele = o[ith]
            info = traverse_triple(ele, conn, info, name, error_fo, q)
            return info
        # add info and to correspoind elements
        elif node_type == NamedGraph:
            name += 'Graph'
            info[5].append(str(root))
            if elements not in p:
                return info
            ith = p.index(elements)
            ele = o[ith]
            info = traverse_triple(ele, conn, info, name, error_fo, q)
            return info 
        elif node_type == Service:
            name += 'Service'      
            info[3].append(str(root))
            if elements not in p:
                return info
            ith = p.index(elements)
            ele = o[ith]
            info = traverse_triple(ele, conn, info, name, error_fo, q)
            return info  
        # SubQuery and Query
        elif node_type == SubQuery:
            # subquery 
            name += 'SubQuery'
            if query not in p:
                return info
            ith = p.index(query)
            ele = o[ith]
            info = traverse_triple(ele, conn, info, name, error_fo, q)
            return info
        # construct, select, ask, describe
        elif node_type in forms:
            for pi in p:
                if pi == type_ or pi == where:
                    continue
                if str(pi) not in info[-1].keys():
                    info[-1][str(pi)] = []
                ith = p.index(pi)
                info[-1][str(pi)].append(str(o[ith]))
            if where not in p:
                return info
            ith = p.index(where)
            ele = o[ith]
            info = traverse_triple(ele, conn, info, name, error_fo, q)
            return info   
        else:
            # other information?
            # from ipdb import set_trace; set_trace()
            error_fo.write('<%s><sep>has other information that do not included in traverse_triple function.\n' % (q))
            return None                  
    # triple
    elif label in p:
        if name == '':
            name = 'Main'
        if name not in info[0].keys():
            info[0][name] = []
        info[0][name].append(str(root))
        return info
    else:
        # other information?
        # from ipdb import set_trace; set_trace()
        error_fo.write('<%s><sep>has other information that do not included in traverse_triple function.\n' % (q))
        return None


def queryID2SpinID(queryid):
    """
    query URI and Spin URI(root) has standard format.
        query URI: 'http://lsq.aksw.org/res/q-cbdfb613'
        Spin URI(root): 'http://lsq.aksw.org/res/spin-q-cbdfb613'
    """
    queryid = queryid.split('/')[-1]
    spin_root = 'http://lsq.aksw.org/res/spin-' + queryid
    return spin_root

def getTerms_pt(q, conn, error_fo):
    """
    get query terms with proerty path

    input:
        q, URI, such as 'http://lsq.aksw.org/res/q-cbdfb613'
        conn, agraph connect server
        error_fo, file IO to write error_info
    """
    res = []
    info = [{}, {}, [], [], [], [], {}]
    spin_root = queryID2SpinID(q)
    root = conn.createURI(spin_root)
    name = ''
    info = traverse_triple(root, conn, info, name, error_fo, q)
    if info == None:
        return None
    triples = info[0]
    for key, triple in triples.items():
        for ti in triple:
            terms = getTermFromTriple(ti, conn, error_fo, q)
            if terms == None:
                return None
            for termi in terms:
                if termi not in res:
                    res.append(termi)
    return res

def getTermsfromQ(q, terms_dict):
    """
    q, query id, <URI>
    conn, agraph connect server
    terms_dict, read from %s_terms(_tp).txt
    """
    return terms_dict[q]

def AskHasError(query, conn, reason=False):   
    """
    we consider two kind of errors:
    1. query  hasError ?error exists
    2. one query id to multiple query texts
    3. processing error
    """
    q = query[1:-1]
    q = conn.createURI(q)
    temp = hasNext(q, conn)
    p = temp[0]
    o = temp[1]
    parseError = conn.createURI('http://lsq.aksw.org/vocab#parseError')
    text = conn.createURI('http://lsq.aksw.org/vocab#text')
    processError = conn.createURI('http://lsq.aksw.org/vocab#processingError')
    if parseError in p:
        if reason:
            return True, 'ParseError'
        else:
            return True
    elif processError in p:
        if reason:
            return True, 'ProcessError'
        else:
            return True
    elif p.count(text) != 1:
        if reason:
            return True, 'MoreThan1Texts'
        else:
            return True
    else:
        if reason:
            return False, 'None'
        else:
            return False 

def AskErrorFromFile(query, error_list):
    if query in error_list:
        return True
    else:
        return False

def str2URI(root, conn):
    if isinstance(root, str):
        if '<' in root:
            root = conn.createURI(root[1:-1])
        else:
            root = conn.createURI(root)
    return root

def traverse_triple_noRecursion(root, conn, info, name, error_fo, q, count=0, test=False):
    # print('root: %s' + str(root))
    # print(info)
    """
    traverse tree to get triples and basic infos.

    input:
        root: query_id -> hasSpin (at start)
        conn: agraph connect server
        info: list, [triples, filter, bind, service, values, graph, other]
                    [{}, {}, [], [], [], [], {}]
              triples: dict, keys: Optional, Union, Minus, NamedGraph, Main, SubQuery(..+)
                             item: triple pattern id
              filter: dict, keys: Optional, Union, Minus, NamedGraph, Main, SubQuery(..+)
                             item: filter spin id
              other: dict, store modifier such as 'limit'
              bind: list, item -> bind spin id
              values: list, item -> values spin id
              graph: list, item -> graph spin id             
              service, list, item -> service spin id    
        name: str, used to mark which block this triple(filter) belong to.
    output:
        output              
    """
    lib = URILibrary()
    type_ = conn.createURI(lib['type'])
    nil = conn.createURI(lib['nil'])
    rest = conn.createURI(lib['rest'])
    first = conn.createURI(lib['first'])
    Optional = conn.createURI(lib['Optional'])
    Minus = conn.createURI(lib['Minus'])
    Union = conn.createURI(lib['Union'])
    Service = conn.createURI(lib['Service'])
    NamedGraph = conn.createURI(lib['NamedGraph'])
    SubQuery = conn.createURI(lib['SubQuery'])
    Values = conn.createURI(lib['Values'])
    Bind = conn.createURI(lib['Bind'])
    Filter = conn.createURI(lib['Filter'])
    elements = conn.createURI(lib['elements'])
    query = conn.createURI(lib['query'])
    label = conn.createURI(lib['label'])
    Ask = conn.createURI(lib['Ask'])
    Describe = conn.createURI(lib['Describe'])
    Construct = conn.createURI(lib['Construct'])
    Select = conn.createURI(lib['Select'])
    forms = [Ask, Describe, Construct, Select]
    where = conn.createURI(lib['where'])
    TriplePath = conn.createURI(lib['TriplePath'])
    path = conn.createURI(lib['path'])
    NotExists = conn.createURI(lib['NotExists'])
    Exists = conn.createURI(lib['Exists'])

    # from ipdb import set_trace; set_trace()
    nodes2visit = [root]
    visited = []
    while len(nodes2visit) != 0:
        if test:
            print(nodes2visit)
        count += 1
        if count > 150:
            print('to many times!')
            raise RecursionError
        current_node = nodes2visit.pop(0)
        if current_node in visited:
            # print('visit a node has already visited, may have inner loop!')
            raise RecursionError
        visited.append(current_node)
        current_node = str2URI(current_node, conn)
        # do something 
        if current_node == nil:
            continue
        next_ = hasNext(current_node, conn)
        if next_ == None:
            continue
        p = next_[0]
        o = next_[1]
        # if test:
        #    from ipdb import set_trace; set_trace()
        # spin node, first and rest
        if first in p:
            for pi in p:
                if pi == first or pi == rest:
                    ith = p.index(pi)
                    nodes2visit.insert(0, o[ith])
            continue
        # have type in p
        elif type_ in p:
            ith = p.index(type_)
            node_type = o[ith]
            # just add info then return
            if node_type == Bind:
                # from ipdb import set_trace; set_trace()
                info[2].append(str(current_node))
                continue
            elif node_type == Values:
                info[4].append(str(current_node))
                continue
            elif node_type == Filter:
                if name == '':
                    name = 'Main'
                if name not in info[1].keys():
                    info[1][name] = []
                info[1][name].append(str(current_node))
                continue
            # property path, add to triples
            elif node_type == TriplePath:
                if name == '':
                    name = 'Main'
                if name not in info[0].keys():
                    info[0][name] = []
                info[0][name].append(str(current_node))
                continue
            # to corrspoind elements
            elif node_type == Optional:
                name += 'Optional'
                if elements not in p:
                    continue
                ith = p.index(elements)
                ele = o[ith]
                nodes2visit.insert(0, ele)
                continue
            elif node_type == NotExists:
                name += 'NotExists'
                if elements not in p:
                    continue
                ith = p.index(elements)
                ele = o[ith]
                nodes2visit.insert(0, ele)
                continue
            elif node_type == Exists:
                name += 'Exists'
                if elements not in p:
                    continue
                ith = p.index(elements)
                ele = o[ith]
                nodes2visit.insert(0, ele)
                continue
            elif node_type == Minus:
                name += 'Minus'
                if elements not in p:
                    continue
                ith = p.index(elements)
                ele = o[ith]
                nodes2visit.insert(0, ele)
                continue
            elif node_type == Union:
                name += 'Union'
                if elements not in p:
                    continue
                ith = p.index(elements)
                ele = o[ith]
                nodes2visit.insert(0, ele)
                continue
            # add info and to correspoind elements
            elif node_type == NamedGraph:
                name += 'Graph'
                info[5].append(str(current_node))
                if elements not in p:
                    continue
                ith = p.index(elements)
                ele = o[ith]
                nodes2visit.insert(0, ele)
                continue
            elif node_type == Service:
                name += 'Service'      
                info[3].append(str(current_node))
                if elements not in p:
                    continue
                ith = p.index(elements)
                ele = o[ith]
                nodes2visit.insert(0, ele)
                continue  
            # SubQuery and Query
            elif node_type == SubQuery:
                # subquery 
                name += 'SubQuery'
                if query not in p:
                    continue
                ith = p.index(query)
                ele = o[ith]
                nodes2visit.insert(0, ele)
                continue
            # construct, select, ask, describe
            elif node_type in forms:
                for pi in p:
                    if pi == type_ or pi == where:
                        continue
                    if str(pi) not in info[-1].keys():
                        info[-1][str(pi)] = []
                    ith = p.index(pi)
                    info[-1][str(pi)].append(str(o[ith]))
                if where not in p:
                    continue
                ith = p.index(where)
                ele = o[ith]
                nodes2visit.insert(0, ele)
                continue
            else:
                error_fo.write('%s<sep>has other information that do not included in traverse_triple function.\n' % (q))
                return None                
        # triple
        elif label in p:
            if name == '':
                name = 'Main'
            if name not in info[0].keys():
                info[0][name] = []
            info[0][name].append(str(current_node))
            continue
        else:
            # other information?
            # from ipdb import set_trace; set_trace()
            error_fo.write('%s<sep>has other information that do not included in traverse_triple function.\n' % (q))
            return None
    return info