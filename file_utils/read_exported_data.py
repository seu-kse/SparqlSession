import sys
import os
o_path = os.getcwd()
sys.path.append(o_path)

import pickle

def readExportedData(direc, name):
    data = read_pkl(os.path.join(direc, f'{name}_session_data.json'))
    return data

def sessions2Query2Text(sessions):
    res = {}
    for sessioni in sessions:
        queries = sessioni['queries']
        for queryi in queries:
            if queryi['index_in_file'] not in res.keys():
                res[queryi['index_in_file']] = queryi['query_content']
    return res

def read_pkl(file_name):
    infile = open(file_name,'rb')
    new_dict = pickle.load(infile)
    infile.close()
    return new_dict