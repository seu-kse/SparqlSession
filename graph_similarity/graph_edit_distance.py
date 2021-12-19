import sys
import os
o_path = os.getcwd()
sys.path.append(o_path)

import networkx as nx
from datetime import timedelta
from file_utils.writefile import write_pkl
from file_utils.read_exported_data import readExportedData
from SPARQL_compare.comp_rdflib import GetInfo
from SPARQL_compare.comp_rdflib import comp_single
from networkx.algorithms.similarity import graph_edit_distance, optimize_graph_edit_distance
from rdflib.term import Literal, URIRef, BNode
from Levenshtein import distance
from tqdm import tqdm
import rdflib
import copy
import argparse

def createGraph(triples):
    DG = nx.DiGraph()
    for triple in triples:
        DG.add_node(triple[0])
        DG.add_node(triple[2])
        DG.add_edge(triple[0], triple[2], object=triple[1])
    return DG

def createHyperGraph(triples):
    DG = nx.DiGraph()
    for triple in triples:
        DG.add_node(triple[0])
        DG.add_node(triple[1])
        DG.add_node(triple[2])
        DG.add_edge(triple[0], triple[1], object=True)
        DG.add_edge(triple[1], triple[2], object=False)
    return DG  

def compGraphSimilarity(t1, t2, hyper=True, debug=False):
    if len(t1) == 0 and len(t2) == 0:
        return {}, 0
    res = 0
    simi = {}
    keys1 = list(t1.keys())
    keys2 = list(t2.keys())
    share = [x for x in keys1 if x in keys2]
    keys = list(set(keys1 + keys2))
    for key in keys:
        if key not in keys1:
            triples1 = []
            triples2 = t2[key]
        if key not in keys2:
            triples2 = []
            triples1 = t1[key]
        if key in share:
            triples1 = t1[key]
            triples2 = t2[key]
        if not hyper:
            dg1 = createGraph(triples1)
            dg2 = createGraph(triples2)
        else:
            dg1 = createHyperGraph(triples1)
            dg2 = createHyperGraph(triples2)    

        nor = max(graph_edit_distance(dg1, nx.DiGraph()), graph_edit_distance(dg2, nx.DiGraph()))
        
        if len(dg1.nodes()) <= 8 or len(dg2.nodes()) <= 8:
            simi[key] = graph_edit_distance(dg1, dg2)
            print('graph edit distance ...')
            if debug:
                print(f'len of nodes1: {len(dg1.nodes())}')
                print(f'len of edges1: {len(dg1.edges())}')
                print(f'len of nodes2: {len(dg2.nodes())}')
                print(f'len of edges2: {len(dg2.edges())}')
        else:
            print('try optimize_graph_edit_distance function ...')
            print(f'len of nodes1: {len(dg1.nodes())}')
            print(f'len of edges1: {len(dg1.edges())}')
            print(f'len of nodes2: {len(dg2.nodes())}')
            print(f'len of edges2: {len(dg2.edges())}')
            ith = 0
            for v in optimize_graph_edit_distance(dg1, dg2):
                ith += 1
                minv = v
                print(f'optimize {ith} times: {v}')
            simi[key] = minv

        simi[key] = simi[key] / nor

    for i, j in simi.items():
        res += j
    res = res/len(simi)
    return simi, res

def compGraphSimilarity_Session(data, begin=0, end=0, hyper=True, first=False):
    """
    compare graph similarity.
    first: False when computing similarity between continuous queries.
           True when computing similarity between current query and the first query.
    """

    if begin != 0 or end != 0:
        sessions = data[begin:end]
    else:
        sessions = data

    simi_dataset = []
    for index, sess in tqdm(enumerate(sessions), total=len(sessions), leave=False):

        simi_session = []
        session_len = sess['session_length']
        
        if first:
            info1 = GetInfo(sess['queries'][0]['query_content'])

        for ith in range(1, session_len): 
            try:
                if not first:
                    info1 = GetInfo(sess['queries'][ith-1]['query_content'])
                info2 = GetInfo(sess['queries'][ith]['query_content'])
            except:
                from ipdb import set_trace; set_trace()
            simi_seperate, simi_all = compGraphSimilarity(info1[0], info2[0], hyper=hyper)
            simi_session.append(simi_all)
        simi_dataset.append({'index': sess['session_id'], 'simi': simi_session})
        
    return simi_dataset

def compGraphSimilarity_Session_First(data, begin=0, end=0, hyper=True):
    """
    compare graph similarity between query and the first query
    """
    return compGraphSimilarity_Session(data, begin, end, hyper, first=True)


if __name__ == "__main__":

    # just list the dbpedia data, all the other dataset can be added to this list. 
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

    parser = argparse.ArgumentParser()
    parser.add_argument("--mask", '-m', type=int, help="choose which file to run")
    parser.add_argument("--hypergraph", '-y', type=bool, default=True, help="use hypergraph or normal graph")
    parser.add_argument("--begin", '-b', type=int, default=0, help="where to begin")
    parser.add_argument("--end", '-e', type=int, default=0, help="where to end")
    parser.add_argument("--data_dir", '-d', type=str, default='exportSession/', help="the directory of data")
    parser.add_argument("--output_dir", '-o', type=str, default='results/', help="output directory")
    args = parser.parse_args()
    i = args.mask
    subdir = 'hypergraph' if args.hypergraph else 'normal_graph'

    output_directory = os.path.join(args.output_dir, subdir)
    if not os.path.exists(output_directory):
        os.mkdir(output_directory)
    
    data = readExportedData(args.data_dir, data_source[args.mask])

    simi_conti = compGraphSimilarity_Session(data['sessions'], begin=args.begin, end=args.end, hyper=args.hypergraph)
    write_pkl(f'{output_directory}/{data_source[i]}_simi_conti.pkl', simi_conti)
    simi_first = compGraphSimilarity_Session_First(data['sessions'], begin=args.begin, end=args.end, hyper=args.hypergraph)
    write_pkl(f'{output_directory}/{data_source[i]}_simi_first.pkl', simi_first)