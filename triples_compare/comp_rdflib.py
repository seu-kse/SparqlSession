import sys
import os
o_path = os.getcwd()
sys.path.append(o_path)

from datetime import datetime, timedelta
from agraph_utils import GetConn, GetTextFromQueryID
from parser_spl.proSPARQL_rdflib import GetInfo
from file_utils.readfile import read_valuedSession, read_pkl
from file_utils.writefile import write_pkl
from file_utils.read_exported_data import readExportedData, sessions2Query2Text
from structure.structure_utils import structure_analysis, get_structure_info
from scipy.optimize import linear_sum_assignment
from rdflib.plugins.sparql.parserutils import CompValue
from rdflib.term import Literal, URIRef, BNode, Variable
# from similarity.comp_onto import GetTypesFromTerms, checkEmpty
import argparse
import copy
from tqdm import tqdm
import numpy as np

def compDict(t1, t2, keyword):
    """
    compare triples dict in two queries.
    t1, t2 here means triples dict in 'info' returned by traverse_triple function.
    t1 comes from queries executed earlier than t2.
    we measure how t2 has changed based on t1.

    input:
        t1 -> dict
        t2 -> dict
        conn -> agraph connect server

    return:
        dict, {
            'block_name': { -> how triples inside 'block_name' has changed. block_name means t1(t2) keys.
                'add': list -> [triple-node, ...]
                'delete': list -> [triple-node, ...]
                'new': bool, -> is this block new?
                'old': bool  -> is this block deleted based on t1?
                'change': list of dict -> {'triple1': URI, 'triple2': URI, 
                                        'change': 's/p/o', 'ori': URI, 'now': URI}
            }
            'block_name': ...
        }
    """
    res = {}
    keys1 = list(t1.keys())
    keys2 = list(t2.keys())
    share = [x for x in keys1 if x in keys2]
    keys = list(set(keys1 + keys2))
    for key in keys:
        if key not in share:
            if key in keys1:
                res[key] = {'old': t1[key]}
            else:
                res[key] = {'new': t2[key]}
        else:
            res[key] = {}
            res[key] = comp_single(t1[key], t2[key], keyword)
    return res

def compDictAsList(t1, t2, keyword):
    pass

def comp_BGP(bgp1, bgp2):
    return compDict(bgp1, bgp2, keyword='triple')

def comp_Fil(f1, f2):
    return compDict(f1, f2, keyword='filter')

def comp_triples(t1, t2, path=None):
    """
    res -> 
    {
        add -> {
            edge -> uri
            node -> ...
        }
        delete -> {
            edge -> uri
            node -> ...
        }
        change -> {
            change -> name
            ori -> uri/var
            now -> uri/var
        }
    }
    """
    res = {
        'add': [],
        'delete': [],
        'change': []
    }
    diff = 0
    same = 0
    name = {0:'subject', 1:'predicate', 2:'object'}
    # from ipdb import set_trace; set_trace()
    for i in range(3):
        if t1[i] != t2[i]:
            res['change'].append({'change': name[i], 'ori': t1[i], 'now': t2[i]})
            diff += 1
        else:
            same += 1
    return res, diff, same

def comp_filter(f1, f2, path=None):
    entity_type = [Literal, URIRef, BNode, Variable, str]
    diff = 0
    same = 0
    res = {
        'add': [],
        'delete': [],
        'change': []
    }
    node2visit1 = [f1]
    node2visit2 = [f2]
    paths = [path]
    while len(node2visit1) != 0 and len(node2visit2) != 0:
        node1 = node2visit1.pop(0)
        node2 = node2visit2.pop(0)
        path = paths.pop(0)
        type1 = type(node1)
        type2 = type(node2)

        if type1 in entity_type or type2 in entity_type:
            if node1 == node2:
                same += 1
            else:
                res['change'].append({'change': path, 'ori': node1, 'now': node2})
                diff += 1
            continue
        if type1 != type2:
            res['change'].append({'change': path, 'ori': node1, 'now': node2})
            diff += 1
            continue
        try:
            if str(node1) == str(node2):
                same += 1
                continue
        except:
            pass
        if isinstance(node1, list):
            for i in node1:
                node2visit1.insert(0, i)
            for i in node2:
                node2visit2.insert(0, i)
                paths.insert(0, path)
            continue
        elif isinstance(node2, CompValue):
            name1 = node1.name
            name2 = node2.name
            if name1 != name2:
                res['change'].append({'change': path, 'ori': node1, 'now': node2})
                diff += 1
            else:
                path = name1
                keys1 = node1.keys()
                keys2 = node2.keys()
                share_keys = [x for x in keys1 if x in keys2]
                not_1 = [x for x in keys1 if x not in share_keys]
                not_2 = [x for x in keys2 if x not in share_keys]
                for ki in share_keys:
                    node2visit1.insert(0, node1[ki])
                    node2visit2.insert(0, node2[ki])
                    paths.insert(0, f'{name1}_{ki}')
                    same += 1
                for ki in not_1:
                    res['change'].append({'delete': {'edge': ki, 'node': node1[ki]}})
                    diff += 1 
                for ki in not_2:
                    res['change'].append({'add': {'edge': ki, 'node': node2[ki]}})
                    diff += 1
            continue
        
    return res, diff, same

                    

def comp_single(t1, t2, keyword, debug=False):
    # -----------compare triples-------------
    # t1, t2 -> list of triples come from one operator
    if keyword == 'triple':
        comp_func = comp_triples
    elif keyword == 'filter':
        comp_func = comp_filter

    cost_matrix = np.zeros((len(t1), len(t2)))
    same_matrix = np.zeros((len(t1), len(t2)))
    match_info = list(np.zeros(len(t1)))

    for i, t1_ in enumerate(t1):
        match_info[i] = list(np.zeros(len(t2)))
        for j, t2_ in enumerate(t2):
            match_info[i][j] = {}
            match_info[i][j]['change'], diff, same = comp_func(t1_, t2_, path=keyword)
            # from ipdb import set_trace; set_trace()
            cost_matrix[i][j] = diff
            same_matrix[i][j] = same

    # match in a minimal cost
    match_x_list, match_y_list = linear_sum_assignment(cost_matrix)
    add_ = []
    delete_ = []
    change = []
    x = len(match_x_list)
    y = len(match_y_list)
    if x != len(t1):
        delete_list = [k for k in range(len(t1)) if k not in match_x_list]
        for di in delete_list:
            delete_.append(t1[di])
    if y != len(t2):
        add_list = [k for k in range(len(t2)) if k not in match_y_list]
        for ai in add_list:
            add_.append(t2[ai])   
              
    match_x = []
    match_y = []
    for i, xi in enumerate(match_x_list):
        yi = match_y_list[i]
        # do not match
        if same_matrix[xi][yi] == 0:
            delete_.append(t1[xi])
            add_.append(t2[yi])
        elif cost_matrix[xi][yi] > 0:
            # from ipdb import set_trace; set_trace()
            change.append(match_info[xi][yi])
            if debug:
                match_x.append(xi)
                match_y.append(yi)

    if debug:
        return {'add': add_, 'delete': delete_, 'change': change}, match_x, match_y, match_info
    return {'add': add_, 'delete': delete_, 'change': change}


def GetCompInfo(name, structure=False, dbpedia=False, data_dir='docs/exportSession', out_dir='results/triples'):
    """
    mask -> data source id

    output:
        only consider changes on triples and filter
        res -> {
            session_id -> index in valuedSession
            query1 -> lsq query id for query1
            query2 -> lsq query id for query2
            time_span -> period between query1 and query2
            comp_info -> {
                Triple -> xxx
                Filter -> xxx
            }
        }
        keys_dict -> [ {
                "Triple keys count"
                key1: xxx
                key2: xxx
            }{
                "Filter keys count"
                ...
            }
        ]
    """

    error_fo = open(os.path.join(out_dir, '%s_error.txt' % name), 'w')

    # valuedSession = read_valuedSession(name, filter_users=False, dbpedia=dbpedia)
    valuedSession = readExportedData(data_dir, name)
    valuedSession = valuedSession['sessions']

    # --------update 20200304----------
    # count the number of keys appearing
    triple_keys = {}
    filter_keys = {}
    keys_dict = [triple_keys, filter_keys]
    ith_key = [0, -1]
    # ------------end------------------

    # --------update 20200304----------
    # count where the changes happen 
    star = [0,0,0] 
    sink = [0,0,0]
    path = [0,0]
    hybrid = [0,0,0,0,0]
    count_struc = {'star':star, 'sink':sink, 'hybrid':hybrid, 'path':path}
    change_count = 0
    # -------------end-----------------

    # get query texts
    query2text = sessions2Query2Text(valuedSession)

    pair_count = 0
    error_count = 0

    res = []
    error_sess = 0
    for index, sess in enumerate(valuedSession):
        # DataFrame -> 'query', 'time'
        session = sess['queries']
        session_len = sess['session_length']
        flag = 0
        for ith in range(session_len-1):
            pair_count += 1
            query1 = session[ith]['index_in_file']
            query2 = session[ith+1]['index_in_file']

            temp = {}
            temp['session_id'] = index

            temp['query1'] = session[ith]['index_in_file']
            temp['query2'] = session[ith+1]['index_in_file']
            text1 = query2text[query1]
            text2 = query2text[query2]

            temp['time_span'] = session[ith+1]['time_stamp'] - session[ith]['time_stamp']
            if temp['time_span'] < timedelta(seconds=0):
                print('%d time span < 0' % index)
                from ipdb import set_trace; set_trace()
            try:
                info1 = GetInfo(text1)
                info2 = GetInfo(text2)
            except:
                flag = 1
                error_count += 1
                error_fo.write(f'{query1}<sep>{query2}\n')
                continue

            # ------------------update 20200304---------------------
            # after getting info, count the number of keys appearing
            for i in range(len(ith_key)):
                keys = list(info1[ith_key[i]].keys())
                keys.extend(list(info2[ith_key[i]].keys()))
                keys = list(set(keys))
                for ki in keys:
                    if ki not in keys_dict[i].keys():
                        keys_dict[i][ki] = 0
                    keys_dict[i][ki] += 1
            # ---------------------end------------------------------

            comp_info = {}
            comp_info['Triple'] = comp_BGP(info1[0], info2[0])
            comp_info['Filter'] = comp_Fil(info1[-1], info2[-1])
            temp['comp_info'] = comp_info
            res.append(temp)

            # --------------update 20200304----------------------
            # count structure change
            if structure:
                count_struc, change_count = structure_analysis(info1[0], comp_info['Triple'], 
                                            res=count_struc, change_count=change_count)
            # --------------------end----------------------------
            
        if flag:
            error_count += 1
    if structure:
        return count_struc, change_count
    return res, keys_dict


def countKeys_all(keys_dict_list, data_source):
    """
    generate keys count for all the data_source.

    input:
        keys_dict_list, a list of keys_dict -> return from GetCompInfo
    
    output:
        dict, {
            data_sourcei:{
                [{triple operators count}, {filter}]
            }
        }
    """
    res = {}
    res_all = [{}, {}]
    for i, temp in enumerate(keys_dict_list):
        res[data_source[i]] = temp
        for ith, ti in enumerate(temp):
            for keyi, count in ti.items():
                if keyi not in res_all[ith].keys():
                    res_all[ith][keyi] = 0
                res_all[ith][keyi] += count
    res['all'] = res_all
    return res

def compDiffChangeType(triple_change, res=None, debug=False, ontology=False):
    interestedExample = [(URIRef, Variable), (Variable, Variable), (URIRef, URIRef), (Variable, URIRef)]
    if res == None:
        res = {}
    for block, block_change in triple_change.items():
        if 'change' in block_change.keys() and len(block_change['change']) != 0:                  
            for block_changei in block_change['change']:
                temp = block_changei['change']
                if 'change' in temp.keys():
                    for rewrite in temp['change']:
                        if 'change' in rewrite.keys():
                            # keys -> change, ori, now
                            type1 = type(rewrite['ori'])
                            type2 = type(rewrite['now'])
                            # -------------------------debug-----------------------
                            if debug:
                                if (type1, type2) in interestedExample:
                                    print((type1, type2))
                                    from ipdb import set_trace; set_trace()
                            # --------------------------end--------------------------
                            key = (type1, type2)
                            if ontology:
                                if key == (URIRef, URIRef):
                                    nodes = read_pkl('docs/dbpedia_ontology.pkl')
                                    type1_ = GetTypesFromTerms([rewrite['ori']], nodes)
                                    type2_ = GetTypesFromTerms([rewrite['now']], nodes)
                                    if not checkEmpty(type1_) and not checkEmpty(type2_):
                                        if nodes[type2_[0]].depth > nodes[type1_[0]].depth:
                                            sign = '+' 
                                        elif nodes[type2_[0]].depth < nodes[type1_[0]].depth:
                                            sign = '-'
                                        elif nodes[type2_[0]].depth == nodes[type1_[0]].depth:
                                            sign = '=' 
                                        key = (type1, type2, sign)
                            
                            if key not in res.keys():
                                res[key] = 0
                            res[key] += 1
    return res


def GetCompInfo_struc_once(name, dbpedia=False):

    def init():
        res = {'out_node': 0, 'in_node': 0,
               'out_edge': 0, 'in_edge': 0}
        return res
    
    def all_init():
        res = {}
        for keyi in ['star', 'sink', 'path', 'hybrid']:
            res[keyi] = init()
        return res


    error_fo = open('docs/compare/%s_error.txt' % name, 'w')

    if not dbpedia:
        conn = GetConn(name)
    valuedSession = read_valuedSession(name, filter_users=False, dbpedia=dbpedia)


    count_struc = all_init()
    change_count = {'star': 0, 'sink': 0, 'hybrid': 0, 'path': 0, 'no': 0}

    # get query texts
    queries = []
    for sessi in valuedSession:
        sess = sessi['session']
        queries.extend(sess['query'].tolist())
    queries_unique = copy.deepcopy(list(set(queries)))

    if not dbpedia:
        texts = GetTextFromQueryID(conn, queries_unique)
        query2text = {}
        for i, query in enumerate(queries_unique):
            query2text[query] = texts[i][0]

    for queryi in queries:
        if not dbpedia:
            texti = query2text[queryi]
        else:
            texti = queryi

        try:
            infoi = GetInfo(texti)
        except:
            continue
        
        nodes = get_structure_info(infoi[0])

        for blocki, nodesi in nodes.items():
            for nodei, topo in nodesi.items():
                change_count[topo['type']] += 1
                if topo['type'] != 'no':
                    for keyi in ['in_node', 'in_edge', 'out_node', 'out_edge']:
                        count_struc[topo['type']][keyi] += len(topo[keyi])

    
    return change_count, count_struc
