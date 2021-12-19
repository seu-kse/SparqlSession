import sys
import os
o_path = os.getcwd()
sys.path.append(o_path)

import numpy as np
from file_utils.readfile import read_pkl
from file_utils.writefile import write_pkl
from file_utils.read_exported_data import readExportedData, sessions2Query2Text
from SPARQL_compare.comp_rdflib import GetInfo
from scipy.stats import entropy
from pandas import Series
from tqdm import tqdm
import copy
import math
import argparse

def removeMinus(a, b):
    a0 = a >= 0
    b0 = b >= 0
    c = np.array([a[i] for i in range(a.shape[0]) if a0[i] and b0[i]])
    d = np.array([b[i] for i in range(b.shape[0]) if a0[i] and b0[i]])
    # from ipdb import set_trace; set_trace()
    return c, d

def kl_divergence(p, q):
    p_nor = np.append(p, np.array((1)))
    q_nor = np.append(q, np.array((1)))

    # p_nor, q_nor = removeMinus(p_nor, q_nor)

    p_nor = p_nor / np.sum(p_nor)
    q_nor = q_nor / np.sum(q_nor)

    res = 0
    for i in range(p_nor.shape[0]):
        if q_nor[i] > 0 and p_nor[i] > 0:
            res += p_nor[i] * np.log(p_nor[i] / q_nor[i])
    if math.isnan(res):
        from ipdb import set_trace; set_trace()
    return res

def cosine_distance(x, y):
    x_nor = np.append(x, np.array((1)))
    y_nor = np.append(y, np.array((1)))

    # x_nor, y_nor = removeMinus(x_nor, y_nor)

    x_nor = x_nor / np.linalg.norm(x_nor)
    y_nor = y_nor / np.linalg.norm(y_nor)
    return np.matmul(x_nor, y_nor.T)

def vector_ana(data, mask, dir_='vector', normalize=False, debug=False):
    # data_source = ['lsr', 'mesh', 'ncbigene', 'ndc', 'omim', 'orphanet', 'sabiork', 'sgd', 'sider', 'swdf',
    #            'affymetrix', 'dbsnp', 'gendr', 'goa', 'linkedgeodata', 'linkedspl', 'dbpedia']
    data_source = ['ncbigene','ndc','orphanet','sgd','sider','swdf','affymetrix','goa','linkedgeodata',
            'dbpedia.3.5.1.log','access.log-20151025', 'access.log-20151124','access.log-20151126',
            'access.log-20151213','access.log-20151230','access.log-20160117','access.log-20160212',
            'access.log-20160222','access.log-20160301','access.log-20160303','access.log-20160304',
            'access.log-20160314','access.log-20160411']
    dbpedia = False if mask <= 8 else True
    query2text = sessions2Query2Text(data)
    
    query2vector = read_pkl(os.path.join(dir_, f'{data_source[mask]}_Vector.pkl'))
    confusionMtrix_dataset = []
    
    for index, sess in tqdm(enumerate(data), total=len(data), leave=True):

        session = sess['queries']
        session_len = sess['session_length']
        flag = 0
        infos = []
        for ith in range(session_len):
            queryi = session[ith]['index_in_file']
            texti = session[ith]['query_content']
            try:
                infoi = GetInfo(texti)
                infos.append(infoi)
            except:
                flag = 1
                break
        if flag:
            continue
        
        if normalize:
            maximum = np.zeros(10)
            for ith1 in range(session_len):
                query1 = session[ith1]['index_in_file']
                vector1 = query2vector[query1]
                for i, num in enumerate(vector1):
                    if num > maximum[i]:
                        maximum[i] = num
                if debug:
                    print(vector1)
            maximum = np.where(maximum==0, 1, maximum)
        if debug:
            print(maximum)
            from ipdb import set_trace; set_trace()

        mat_kl = np.zeros((session_len, session_len))
        mat_cos = np.zeros((session_len, session_len))
        for ith1 in range(session_len):
            for ith2 in range(session_len):
                key = 'index_in_file'
                query1 = session[ith1][key]
                query2 = session[ith2][key]
                vector1 = query2vector[query1]
                vector2 = query2vector[query2]
                if debug:
                    print('before normalize')
                    print(vector1)
                    print(vector2)
                if normalize:
                    vector1 = vector1/maximum
                    vector2 = vector2/maximum
                if debug:
                    print('after')
                    print(vector1)
                    print(vector2)
                    from ipdb import set_trace; set_trace()
                mat_kl[ith1][ith2] = kl_divergence(vector1, vector2)
                mat_cos[ith1][ith2] = cosine_distance(vector1, vector2)   
        confusionMtrix_dataset.append({'index': index, 'mat_kl': mat_kl, 'mat_cos': mat_cos})

    marker = '_normalized' if normalize else ''
    write_pkl(os.path.join(dir_,  f'{data_source[mask]}_confusionMat{marker}.pkl'), confusionMtrix_dataset)          
    return confusionMtrix_dataset

def proConfusionMat(mask, dir_='vector', normalize=False, root='docs'):

    """
    mask, a list of data source index to process
    """

    def init(max_len):
        res_kl = list(np.zeros(max_len))
        for i, _ in enumerate(res_kl):
            res_kl[i] = list(np.zeros(max_len))
            for j in range(max_len):
                res_kl[i][j] = []
        return res_kl
    
    def matToSeries(mat):
        x = len(mat)
        y = len(mat[0])
        for xi in range(x):
            for yi in range(y):
                mat[xi][yi] = Series(mat[xi][yi])
        return mat


    # data_source = ['lsr', 'mesh', 'ncbigene', 'ndc', 'omim', 'orphanet', 'sabiork', 'sgd', 'sider', 'swdf',
    #            'affymetrix', 'dbsnp', 'gendr', 'goa', 'linkedgeodata', 'linkedspl', 'dbpedia']
    data_source = ['ncbigene','ndc','orphanet','sgd','sider','swdf','affymetrix','goa','linkedgeodata',
            'dbpedia.3.5.1.log','access.log-20151025', 'access.log-20151124','access.log-20151126',
            'access.log-20151213','access.log-20151230','access.log-20160117','access.log-20160212',
            'access.log-20160222','access.log-20160301','access.log-20160303','access.log-20160304',
            'access.log-20160314','access.log-20160411']
    mats = []
    max_len = 0
    max_len_all = []
    marker = '_normalized' if normalize else ''
    for i in mask:
        temp = read_pkl(os.path.join(root, dir_, f'{data_source[i]}_confusionMat{marker}.pkl'))
        mats.append(temp)
        max_len_single = 0
        for i in temp:
            session_len = i['mat_cos'].shape[0]
            if session_len > max_len:
                max_len = session_len
            if session_len > max_len_single:
                max_len_single = session_len
        max_len_all.append(max_len_single)

    res_kl_all  = [init(max_len_all[i]) for i in range(len(mask))]
    res_cos_all = [init(max_len_all[i]) for i in range(len(mask))]
    res_kl = init(max_len)
    res_cos = init(max_len)

    for idx, mat in enumerate(mats):
        for sess in mat:
            sess_len = sess['mat_kl'].shape[0]
            for xi in range(sess_len):
                for yi in range(sess_len):
                    res_kl[xi][yi].append(sess['mat_kl'][xi][yi])
                    res_cos[xi][yi].append(sess['mat_cos'][xi][yi])
                    res_kl_all[idx][xi][yi].append(sess['mat_kl'][xi][yi])
                    res_cos_all[idx][xi][yi].append(sess['mat_cos'][xi][yi])

    mat_kl = matToSeries(res_kl)
    mat_cos = matToSeries(res_cos)
    mat_kl_all = [matToSeries(i) for i in res_kl_all]
    mat_cos_all = [matToSeries(i) for i in res_cos_all]
    return mat_kl, mat_cos, mat_kl_all, mat_cos_all

def matToMean(mat):
    res = copy.deepcopy(mat)
    x = len(mat)
    y = len(mat[0])
    for xi in range(x):
        for yi in range(y):
            res[xi][yi] = mat[xi][yi].mean()
    # from ipdb import set_trace; set_trace()
    res = np.asarray(res)
    return res

def prepareBoxData(mat, conti):
    """
    mat, nxn list, each item in list is a Series
    conti, bool, continuous queries or first queries
    """
    if conti:
        res = [np.array(mat[i][i+1].tolist()) for i in range(len(mat)-1)]
        return res
    else:
        res = [np.array(mat[0][i+1].tolist()) for i in range(len(mat)-1)]
        return res

if __name__ == "__main__":
    
    # data_source = ['lsr', 'mesh', 'ncbigene', 'ndc', 'omim', 'orphanet', 'sabiork', 'sgd', 'sider', 'swdf',
    #            'affymetrix', 'dbsnp', 'gendr', 'goa', 'linkedgeodata', 'linkedspl', 'dbpedia']
    data_source = ['ncbigene','ndc','orphanet','sgd','sider','swdf','affymetrix','goa','linkedgeodata',
            'dbpedia.3.5.1.log','access.log-20151025', 'access.log-20151124','access.log-20151126',
            'access.log-20151213','access.log-20151230','access.log-20160117','access.log-20160212',
            'access.log-20160222','access.log-20160301','access.log-20160303','access.log-20160304',
            'access.log-20160314','access.log-20160411']

    parser = argparse.ArgumentParser()
    parser.add_argument("--mask", '-m', type=int, help="choose which file to run")
    parser.add_argument("--normalize", '-n', type=int, default=1, help="True when computing feature vector, false when computing IRI vector")
    parser.add_argument("--data_dir", '-d', type=str, default='docs/exportSession', help="the directory of data")
    parser.add_argument("--output_dir", '-o', type=str, default='results', help="output directory")
    parser.add_argument("--sub_dir", '-s', type=str, default='hyper_featureVector', help="output sub directory")
    args = parser.parse_args()
    mask = args.mask

    norma = True if args.normalize else False
    print(norma)
    for i in range(len(data_source)):
        data = readExportedData(args.data_dir, data_source[i])
        vector_ana(data['sessions'], i, dir_=os.path.join(args.output_dir, args.sub_dir), normalize=norma, debug=False)
