import sys
import os
o_path = os.getcwd()
sys.path.append(o_path)

import numpy as np
from file_utils.readfile import read_pkl
from file_utils.writefile import write_pkl
from file_utils.read_exported_data import readExportedData, sessions2Query2Text
from scipy.stats import entropy
from pandas import Series, read_csv
from tqdm import tqdm
import copy
import argparse

def GetText(session, query2text, ith, dbpedia=False):
    if not dbpedia:
        queryi = session.iloc[ith]['query']
    else:
        queryi = session.iloc[ith]['idxInFile']
    texti = query2text[queryi]
    return texti

def GetResultSize(session, query2factor, ith, dbpedia=False):
    queryi = session[ith]['index_in_file']
    queries = query2factor['query'].tolist()
    if queryi not in queries:
        return False, 0
    else:
        factori = query2factor.loc[query2factor['query']==queryi]
        # if len(factori) != 1:
        #    from ipdb import set_trace; set_trace()
        # else:
        if not dbpedia:
            return True, factori['resultSize'].tolist()[0]
        else:
            if factori['resultSize'].tolist()[0] == 'None':
                return False, 0
            else:
                return True, int(factori['resultSize'].tolist()[0]) 

def collectAddOrDelete(comp, keyword='add', res=None):
    if res == None:
        res = 0
    for key, comp_key in comp.items():
        if keyword in comp_key.keys():
            res += len(comp_key[keyword])
    return res

def sign(first, second):
    if first == second:
        return 0
    else:
        return 1 if second-first > 0 else -1


def hiddenState_matrix(mask, data_dir='docs/exportSession', debug=False):
    """
    analyse how result size change influence different changes
    """
    data_source = [ 'ncbigene','ndc','orphanet','sgd','sider','swdf','affymetrix','goa','linkedgeodata',
            'dbpedia.3.5.1.log','access.log-20151025', 'access.log-20151124','access.log-20151126',
            'access.log-20151213','access.log-20151230','access.log-20160117','access.log-20160212',
            'access.log-20160222','access.log-20160301','access.log-20160303','access.log-20160304',
            'access.log-20160314','access.log-20160411']


    res = {-1: 0, 0: 0, 1: 0}
    count = np.zeros((3,3))

    for mask in tqdm(range(len(data_source)), total=len(data_source)):
        dbpedia = False if mask <= 8 else True
        # dbpedia = True
        # valuedSession, query2text = pre(mask)
        # _ , query2text, valuedSession = GetQueryAndText(data_source[mask], dbpedia=dbpedia, ReturnValuedSession=True)
        data = readExportedData(data_dir, data_source[mask])
        sessions = data['sessions']
        query2text = sessions2Query2Text(sessions)
        query2factor = read_csv(f'docs/factors/{data_source[mask]}_factor.csv')

        for index, sess in enumerate(sessions):
            # DataFrame -> 'query', 'time'
            session = sess['queries']
            session_len = sess['session_length']
            first = True
            for ith in range(session_len-2):
                if_first, first_result = GetResultSize(session, query2factor, ith, dbpedia=dbpedia)
                if_second, second_result = GetResultSize(session, query2factor, ith+1, dbpedia=dbpedia)
                if_third, third_result = GetResultSize(session, query2factor, ith+2, dbpedia=dbpedia)
                if if_first and if_second and if_third:
                    delta1 = sign(first_result, second_result)
                    delta2 = sign(second_result, third_result)
                    if first:
                        res[delta1] += 1
                        first = False
                    res[delta2] += 1
                    count[delta1+1][delta2+1] += 1
                else:
                    first = True

    return res, count

def hiddenState_matrix_fromZero(mask, data_dir='docs/exportSession', debug=False):
    """
    analyse how result size change influence different changes
    """
    data_source = [ 'ncbigene','ndc','orphanet','sgd','sider','swdf','affymetrix','goa','linkedgeodata',
            'dbpedia.3.5.1.log','access.log-20151025', 'access.log-20151124','access.log-20151126',
            'access.log-20151213','access.log-20151230','access.log-20160117','access.log-20160212',
            'access.log-20160222','access.log-20160301','access.log-20160303','access.log-20160304',
            'access.log-20160314','access.log-20160411']


    res = {-1: 0, 0: 0, 1: 0}
    count = np.zeros((3,3))

    for mask in tqdm(range(len(data_source)), total=len(data_source)):
        dbpedia = False if mask <= 8 else True
        # dbpedia = True
        # valuedSession, query2text = pre(mask)
        # _ , query2text, valuedSession = GetQueryAndText(data_source[mask], dbpedia=dbpedia, ReturnValuedSession=True)
        data = readExportedData(data_dir, data_source[mask])
        sessions = data['sessions']
        query2text = sessions2Query2Text(sessions)
        query2factor = read_csv(f'docs/factors/{data_source[mask]}_factor.csv')

        for index, sess in enumerate(sessions):
            # DataFrame -> 'query', 'time'
            session = sess['queries']
            session_len = sess['session_length']
            first = True
            store_first = True
            for ith in range(session_len-1):
                if first:
                    first = False
                    if_first = True
                    first_result = 0
                else:
                    if_first, first_result = GetResultSize(session, query2factor, ith-1, dbpedia=dbpedia)
                if_second, second_result = GetResultSize(session, query2factor, ith, dbpedia=dbpedia)
                if_third, third_result = GetResultSize(session, query2factor, ith+1, dbpedia=dbpedia)
                if if_first and if_second and if_third:
                    delta1 = sign(first_result, second_result)
                    delta2 = sign(second_result, third_result)
                    if store_first:
                        res[delta1] += 1
                        store_first = False
                    res[delta2] += 1
                    count[delta1+1][delta2+1] += 1
                else:
                    store_first = True

    return res, count

def hiddenState_seq(mask, data_dir='docs/exportSession', debug=False):
    """
    analyse how result size change influence different changes
    """
    data_source = [ 'ncbigene','ndc','orphanet','sgd','sider','swdf','affymetrix','goa','linkedgeodata',
            'dbpedia.3.5.1.log','access.log-20151025', 'access.log-20151124','access.log-20151126',
            'access.log-20151213','access.log-20151230','access.log-20160117','access.log-20160212',
            'access.log-20160222','access.log-20160301','access.log-20160303','access.log-20160304',
            'access.log-20160314','access.log-20160411']

    seq_x = []
    seq_x_zero = []
    seq_y = []
    seq_y_zero = []

    for mask in tqdm(range(len(data_source)), total=len(data_source)):
        dbpedia = False if mask <= 8 else True
        # dbpedia = True
        # valuedSession, query2text = pre(mask)
        # _ , query2text, valuedSession = GetQueryAndText(data_source[mask], dbpedia=dbpedia, ReturnValuedSession=True)
        data = readExportedData(data_dir, data_source[mask])
        query2text = sessions2Query2Text(sessions)
        query2factor = read_csv(f'docs/factors/{data_source[mask]}_factor.csv')

        for index, sess in enumerate(valuedSession):
            # DataFrame -> 'query', 'time'
            session = sess['queries']
            session_len = sess['session_length']
            first = True
            for ith in range(session_len-1):
                if_first, first_result = GetResultSize(session, query2factor, ith, dbpedia=dbpedia)
                if_second, second_result = GetResultSize(session, query2factor, ith+1, dbpedia=dbpedia)
                if if_first and if_second:
                    delta1 = sign(first_result, second_result)
                    if first_result != 0:
                        seq_x.append(ith)
                        seq_y.append((second_result-first_result)/first_result)
                    else:
                        seq_x_zero.append(ith)
                        seq_y_zero.append(delta1)

    return seq_x, seq_y, seq_x_zero, seq_y_zero


def CountInit(mask, data_dir='docs/exportSession', debug=False):
    """
    analyse how result size change influence different changes
    """
    data_source = [ 'ncbigene','ndc','orphanet','sgd','sider','swdf','affymetrix','goa','linkedgeodata',
            'dbpedia.3.5.1.log','access.log-20151025', 'access.log-20151124','access.log-20151126',
            'access.log-20151213','access.log-20151230','access.log-20160117','access.log-20160212',
            'access.log-20160222','access.log-20160301','access.log-20160303','access.log-20160304',
            'access.log-20160314','access.log-20160411']


    res = {-1: 0, 0: 0, 1: 0, 'all': 0}

    for mask in tqdm(range(len(data_source)), total=len(data_source)):
        dbpedia = False if mask <= 8 else True
        # dbpedia = True
        # valuedSession, query2text = pre(mask)
        # _ , query2text, valuedSession = GetQueryAndText(data_source[mask], dbpedia=dbpedia, ReturnValuedSession=True)
        data = readExportedData(data_dir, data_source[mask])
        query2text = sessions2Query2Text(sessions)
        query2factor = read_csv(f'docs/factors/{data_source[mask]}_factor.csv')
        
        for index, sess in enumerate(sessions):
            # DataFrame -> 'query', 'time'
            session = sess['queries']
            session_len = sess['session_length']
            
            if_first, first_result = GetResultSize(session, query2factor, 0, dbpedia=dbpedia)
            if_second, second_result = GetResultSize(session, query2factor, 1, dbpedia=dbpedia)
            if if_first and if_second:
                delta1 = sign(first_result, second_result)
                res[delta1] += 1
            res['all'] += 1

    return res