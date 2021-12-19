import sys
import os
o_path = os.getcwd()
sys.path.append(o_path)

from query.Query import Query
from file_utils.readfile import read_pkl, read_csv
from file_utils.writefile import write_pkl
from file_utils.read_exported_data import sessions2Query2Text, readExportedData
from tqdm import tqdm
import numpy as np
import pyparsing
import argparse

def GetFeatureVectors(data, name, dbpedia=False, debug=False):

    query2text = sessions2Query2Text(data)
    query2factor = read_csv(f'docs/factors/{name}_factor.csv')

    query2vector = {}

    for queryidx, texti in tqdm(query2text.items(), total=len(query2text)):
        queryi = Query(queryidx, texti)
        try:
            queryi.fill(query2factor, dbpedia)
            if queryi.projectionNum == -1:
                from ipdb import set_trace; set_trace()
            query2vector[queryidx] = queryi.featureVec
            if debug:
                queryi.print_query()
                from ipdb import set_trace; set_trace()
        except:
            continue
    return query2vector

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--mask", '-m', type=int, help="choose which file to run")
    parser.add_argument("--data_dir", '-d', type=str, default='docs/exportSession', help="the directory of data")
    parser.add_argument("--output_dir", '-o', type=str, default='ISWC-extension/results', help="output directory")
    args = parser.parse_args()
    mask = args.mask

    data_source = ['ncbigene','ndc','orphanet','sgd','sider','swdf','affymetrix','goa','linkedgeodata',
            'dbpedia.3.5.1.log','access.log-20151025', 'access.log-20151124','access.log-20151126',
            'access.log-20151213','access.log-20151230','access.log-20160117','access.log-20160212',
            'access.log-20160222','access.log-20160301','access.log-20160303','access.log-20160304',
            'access.log-20160314','access.log-20160411']
    
    data = readExportedData(args.data_dir, data_source[args.mask])
    dbpedia = False if args.mask <= 8 else True

    query2vector = GetFeatureVectors(data['sessions'], data_source[args.mask], dbpedia=dbpedia)
    write_pkl(f'results/hyper_featureVector/{data_source[mask]}_Vector.pkl', query2vector)