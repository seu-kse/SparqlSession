import sys
import os
o_path = os.getcwd()
sys.path.append(o_path)

from parser_spl.utils import GetAndRemovePart, GetProcessStr, GetPattern, Clean
from parser_spl.utils import add_list,  add_dict

def GetTriples(query, op, left, right, ith):
    """
    from a clean block in SPARQL (remove optional, minus, union, filter, values, bind).
    from a clean sub block in SPARQL (optional, minus and union block without filter)

    input: 
        query: str, the clean block or sub block
        op: str, operator block, for optional, minus, graph and union, the op is the upper case of there operators.
                                 for clean main block, the op is None
        left and right: str, for optional, minus and Union, left is "{", right is "}"
                             for clean main block, the op is the same as optional ..
        ith, str, can be any thing which can represents the query we currently processing, used for print error.

    output:
        triples: list of list, for each list in list has a len of 3, represent s, p, o in a triple respectively.
        so, the unique list of s and o
        p, unique list of p.

    Notice: this function is originally used for the stardard BGP pattern processed by java. If used for the raw query, 
            some error may occur.
    """
    #from ipdb import set_trace; set_trace()
    so = []
    p = []
    if op != None:
        query = re.sub(op, '', query)
    if left != None and right != None:
        p_s = GetProcessStr(left, right, query)
    else:
        query = Clean(query)
        p_s = [query]
        
    triples = []
    for query in p_s:
        #from ipdb import set_trace; set_trace()
        if query == '':
            continue
        query = query.split(' . ')   
        for i in query:
            if ';' not in i:
                i = re.sub('\s*$', '', i)
                i = re.sub('^\s', '', i)
                temp = i.split(' ')
                if len(temp) > 3:
                    temp_s = ''
                    for ti in temp[2:]:
                        temp_s += ti + ' '
                    temp[2] = temp_s.strip()
                    temp = temp[:3]
                if len(temp) != 3:
                    #from ipdb import set_trace; set_trace()
                    flag = 0
                    for xi in temp:
                        if 'SELECT' in xi or 'CONSTRUCT' in xi:
                            flag = 1
                    if flag:
                        continue
                    else:
                        print(str(ith) + ' : Get Triples Error, assume has len of 3 but got ' + str(len(temp)))
                        return [], [], []
                triples.append(temp)
            else:
                i = re.sub('\s*$', '', i)
                i = re.sub('^\s', '', i)
                i = i.split(';')
                s = ''
                for j, k in enumerate(i):
                    if j == 0:
                        s = k.split(' ')[0]
                        temp = k.split(' ')
                        if len(temp) > 3:
                            temp_s = ''
                            for ti in temp[2:]:
                                temp_s += ti + ' '
                            temp[2] = temp_s.strip()
                            temp = temp[:3]
                        if len(temp) != 3:
                            #from ipdb import set_trace; set_trace()
                            flag = 0
                            for xi in temp:
                                if 'SELECT' in xi or 'CONSTRUCT' in xi:
                                    flag = 1
                            if flag:
                                continue
                            else:
                                print(str(ith) + ' : Get Triples Error, assume has len of 3 but got ' + str(len(temp)))
                                return [], [], []
                        triples.append([s, temp[1], temp[2]])
                    else:
                        k = re.sub('^\s', '', k)
                        temp = k.split(' ')
                        if len(temp) > 2:
                            temp_s = ''
                            for ti in temp[1:]:
                                temp_s += ti + ' '
                            temp[1] = temp_s.strip()
                            temp = temp[:2]
                        if len(temp) != 2:
                            # from ipdb import set_trace; set_trace()
                            print(str(ith) + ' : Get Triples Error, assume has len of 2 but got ' + str(len(temp)))
                            return [], [], []
                        triples.append([s, temp[0], temp[1]])  
    for t in triples:
        so.append(t[0])
        so.append(t[2])
        p.append(t[1])
    so = list(set(so))
    p = list(set(p))
    return triples, so, p
        

def HandleSubquery(query_pattern, pattern, ith):
    """
    process subquery to stardard format.

    input:
        query_pattern: query pattern with subquery
        pattern: result from GetPattern function
        ith: the same as GetTriples function

    output:
        filter_all: dict, for each key means the source of the filter, such as "optional"
        triples_all: list, a list of triples, each triple represented by a list
        so_all: list, the subject and object in triples, which is unique
        p_all: list, the predicate in triples, which is unique
        values_all, the list of values, for each item in this list is the original str in SPARQL
        bind_all: the list of bind, for each item in this list is the original str in SPARQL
        remove: str without subquery
    """
    res, remove = GetAndRemovePart('WHERE', '{', '}', query_pattern)
    values_all = {}
    triples_all = []
    so_all = []
    p_all = []
    filter_all = {}
    bind_all = {}
    for i in res:
        i = i.replace('WHERE', '')
        filter_info, triples, so, p, values, bind = GetCleanBGP(i, pattern, ith)
        filter_all = add_dict(filter_all, filter_info)
        triples_all = add_list(triples_all, triples, False, op='SUB')
        so_all = add_list(so_all, so, True)
        p_all = add_list(p_all, p, True)
        values_all = add_dict(values_all, values)
        bind_all = add_dict(bind_all, bind)
    return filter_all, triples_all, so_all, p_all, values_all, bind_all, remove


def HandleOptUniGra(query_pattern, op, ith):
    """
    handle Optional, union, graph and minus block, get filter block first, and then get triples in this block.

    input:
        query_pattern, str, query pattern processed in java, represent in the DataFrame 'pattern' column
        op, str, oprator, used for GetAndRemove function.
        ith, the same as GetTriples
    
    output:
        filter_info, list of filter in this block.
        triples, the same as GetTriples
        remove, query string with out "op" block.
    """
    # op, string, higher case
    left = '{'
    right = '}'
    so = []
    p = []
    # get and remove the main part of union/graph/optional
    res, remove = GetAndRemovePart(op, left, right, query_pattern)
    # then remove the inner filter and get triple
    filter_info = []
    triples = []
    for i in res:
        filter_temp, res_remove_filter = GetAndRemovePart('FILTER', '(', ')', i)
        #from ipdb import set_trace; set_trace()
        for f in filter_temp:
            filter_info.append(f)
        triple, so_, p_ = GetTriples(res_remove_filter, op, '{', '}', ith)
        for t in triple:
            t[1] = op + '<sep>' + t[1]
            triples.append(t) 
        for soi in so_:
            so.append(soi)
        for pi in p_:
            p.append(pi)
    return filter_info, triples, remove, so, p

def HandleFilValBind(query_pattern, op):
    """
    handle filter, value and bind.
    get the filter value and bind block in query_pattern.

    input:
        the same as HandleOptUniGra function.

    output:
        res, list of str, which contains the original str of this block.
        remove, the query pattern after removing this block.

    Notice:
        when using for filter, should remove the filter in optional, graph, minus and union block.
    """
    left = '('
    right = ')'
    if op == 'VALUES':
        left = '{'
        right = '}'
    res, remove = GetAndRemovePart(op, left, right, query_pattern)
    return res, remove

# get clean bgp, which contains bgps, optional, minus, union, subquery, graph
def GetCleanBGP(query_pattern, pattern, ith):
    """
    get all the info about BGP in one SPARQL query.

    input:
        query_pattern, DataFrame 'pattern' column.
        pattern: result from GetPattern
        ith:, the same as GetTriples
    
    output:
        filter_info: dict, all the filter infos.
                     keys: subquery -> dict, with optional ....
                           optional -> list
                           union -> list
                           graph -> list
                           filter -> list, main filter not in other block.
        values: dict, keys: main -> list
                            subquery -> list
        bind: dict, keys: main -> list
                          sunquery -> list
        triples: list, p has the marker such as "OPT<sep>?p"
        so, list, unique
        p, list, unique

    """
    filter_info = {}
    triples = []
    values = {}
    so = []
    p = []
    bind = {}
    query_pattern = re.sub(pattern['service'], '', query_pattern)
    filter_info['subquery'], triple_sub, so_sub, p_sub, values['subquery'], bind['subquery'], query_pattern = HandleSubquery(query_pattern, pattern, ith)
                                                        # filter(list), triple, query, so, p
    filter_info['optional'], triple_o, query_pattern, so_o, p_o = HandleOptUniGra(query_pattern, 'OPTIONAL', ith) 
    filter_info['union'], triple_u, query_pattern, so_u, p_u = HandleOptUniGra(query_pattern, 'UNION', ith) # filter, triple, query, so, p
    filter_info['graph'], triple_g, query_pattern, so_g, p_g = HandleOptUniGra(query_pattern, 'GRAPH', ith) # filter, graph, query, so, p
    filter_info['filter'], query_pattern = HandleFilValBind(query_pattern, 'FILTER')  # filter, query
    values['main'], query_pattern = HandleFilValBind(query_pattern, 'VALUES')  # values, query 
    bind['main'], query_pattern = HandleFilValBind(query_pattern, 'BIND')
    triple_main, so_main, p_main = GetTriples(query_pattern, None, '{', '}', ith)
    # if contain subquery, remove 'SELECT * LIMIT5' in triple_main
    if len(triple_sub) != 0:
        for xi in triple_main:
            if 'SELECT' in xi[0] or 'CONSTRUCT' in xi[0]:
                triple_main.remove(xi)
                so_main.remove(xi[0])
                so_main.remove(xi[2])
                p_main.remove(xi[1])
    for t in [triple_o, triple_u, triple_g, triple_main, triple_sub]:
        for ti in t:
            triples.append(ti)
    for so_ in [so_o, so_u, so_g, so_main, so_sub]:
        for soi in so_:
            so.append(soi)
    for p_ in [p_o, p_u, p_g, p_main, p_sub]:
        for pi in p_:
            p.append(pi)
    so = list(set(so))
    p = list(set(p))
    return filter_info, triples, so, p, values, bind

def GetSPO_string(q1, pattern):
    """
    get spo list.

    input:
        q1, one raw in DataFrame, keys: ip, time, query, type, pattern
        pattern: result from GetPattern function
    """
    filter_info1, triples1, so1, p1, values1, bind1 = GetCleanBGP(q1['pattern'], pattern, q1['ip'])
    return so1 + p1

def hasParseError_string(q):
    """
    if has parse error, return true.

    input:
        q, the same as GetSPO_string 
    output:
        boolean
    """
    if q['type'] == 0:
        return True
    else: 
        return False