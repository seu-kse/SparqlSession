import sys
import os
o_path = os.getcwd()
sys.path.append(o_path)

from datetime import datetime, timedelta
from parser_spl.proSPARQL_rdflib import GetInfo
from file_utils.read_exported_data import read_pkl
from file_utils.writefile import write_pkl
from scipy.optimize import linear_sum_assignment
from rdflib.plugins.sparql.parserutils import CompValue
from rdflib.term import Literal, URIRef, BNode, Variable
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
