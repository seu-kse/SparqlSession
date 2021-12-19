import sys
import os
o_path = os.getcwd()
sys.path.append(o_path)

from parser_spl.proSPARQL_rdflib import GetSPOfromQuery
from rdflib.term import Literal, URIRef, BNode
from file_utils.writefile import write_pkl
from file_utils.readfile import GetQuerys
from file_utils.read_exported_data import readExportedData, sessions2Query2Text
from tqdm import tqdm
import numpy as np
import argparse


def getAllVector(data, name, dbpedia=False):
    stop_words = [URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                  URIRef('http://www.w3.org/2000/01/rdf-schema#label')]
    query2vector = {}
    IRI_table = {}
    query2text = sessions2Query2Text(data)
    queries = list(query2text.keys())
    query2spo = {}
    for query, text in tqdm(query2text.items()):
        # print(text)
        try:
            spo = GetSPOfromQuery(text)
        except:
            continue
        query2spo[query] = spo
        for i in spo:
            if isinstance(i, URIRef) and i not in IRI_table.keys() and i not in stop_words:
                IRI_table[i] = len(IRI_table) 
    # print(IRI_table)
    for queryi in tqdm(queries):
        if queryi not in query2spo.keys():
            continue
        text = query2text[queryi]
        spo = query2spo[queryi]
        vec = np.zeros(len(IRI_table))
        for i in spo:
            if isinstance(i, URIRef) and i not in stop_words:
                vec[IRI_table[i]] = 1
        query2vector[queryi] = vec
    # print(query2vector)
    return query2vector, IRI_table


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--mask", '-m', type=int, help="choose which file to run")
    parser.add_argument("--data_dir", '-d', type=str, default='docs/exportSession', help="the directory of data")
    parser.add_argument("--output_dir", '-o', type=str, default='results', help="output directory")
    args = parser.parse_args()

    # repo_names = ['lsr', 'mesh', 'ncbigene', 'ndc', 'omim', 'orphanet', 'sabiork', 'sgd', 'sider', 'swdf',
    #            'affymetrix', 'dbsnp', 'gendr', 'goa', 'linkedgeodata', 'linkedspl', 'dbpedia']
    # data_source = ['lsr', 'mesh', 'ncbigene', 'ndc', 'omim', 'orphanet', 'sabiork', 'sgd', 'sider', 'swdf',
    #            'affymetrix', 'dbsnp', 'gendr', 'goa', 'linkedgeodata', 'linkedspl', 'dbpedia']
    data_source = ['ncbigene','ndc','orphanet','sgd','sider','swdf','affymetrix','goa','linkedgeodata',
            'dbpedia.3.5.1.log','access.log-20151025', 'access.log-20151124','access.log-20151126',
            'access.log-20151213','access.log-20151230','access.log-20160117','access.log-20160212',
            'access.log-20160222','access.log-20160301','access.log-20160303','access.log-20160304',
            'access.log-20160314','access.log-20160411']

    # mask = args.mask
    for mask in range(len(data_source)):
        dbpedia = False if mask <= 8 else True
        data = readExportedData(args.data_dir, data_source[mask])
        query2vector, IRI_table = getAllVector(data['sessions'], data_source[mask], dbpedia=dbpedia)
        write_pkl(f'results/IRI_vector/{data_source[mask]}_IRI_table.pkl', IRI_table)
        write_pkl(f'results/IRI_vector/{data_source[mask]}_Vector.pkl', query2vector)