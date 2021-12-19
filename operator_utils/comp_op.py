from agraph_utils import GetQueryUsedFeature, GetConn
from file_utils.writefile import write_pkl, writeList
from file_utils.readfile import read_pkl, read_valuedSession, read_list
from file_utils.read_exported_data import readExportedData, sessions2Query2Text
from pandas import DataFrame
import numpy as np
import os

def GetQueryFeatures(mask):
    """
    get query features for one dataset.
    """

    # data_source = ['lsr', 'mesh', 'ncbigene', 'ndc', 'omim', 'orphanet', 'sabiork', 'sgd', 'sider', 'swdf',
    #            'affymetrix', 'dbsnp', 'gendr', 'goa', 'linkedgeodata', 'linkedspl', 'dbpedia']
    data_source = ['ncbigene','ndc','orphanet','sgd','sider','swdf','affymetrix','goa', 'linkedgeodata']

    feature_file = 'docs/feature/%s_feature.pkl' % data_source[mask]

    conn = GetConn(data_source[mask])
    valuedSession = read_valuedSession(data_source[mask], filter_users=False)

    # get query texts
    queries = []
    for sessi in valuedSession:
        sess = sessi['session']
        queries.extend(sess['query'].tolist())
    queries = list(set(queries))

    query2feature = {}
    for queryi in queries:
        features = GetQueryUsedFeature(conn, queryi)
        if queryi not in query2feature.keys():
            query2feature[queryi] = []
        query2feature[queryi].extend(features)
    
    write_pkl(feature_file, query2feature)

    return query2feature

def OperatorDistribution(data_dir='docs/exportSession', operator_dir='results/operator'):
    """
    generate a table, columns -> different data set
                      index   -> different feature
    for all the dataset.
    """
    # data_source = ['lsr', 'mesh', 'ncbigene', 'ndc', 'omim', 'orphanet', 'sabiork', 'sgd', 'sider', 'swdf',
    #            'affymetrix', 'dbsnp', 'gendr', 'goa', 'linkedgeodata', 'linkedspl', 'dbpedia']
    data_source1 = ['ncbigene','ndc','orphanet','sgd','sider','swdf','affymetrix','goa', 'linkedgeodata']
    data_source2 =  ['dbpedia.3.5.1.log',
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
    data_source = data_source1 + data_source2
    res = {}
    features_all = []
    query2feature = {}
    for namei in data_source:
        query2feature[namei] = read_pkl(os.path.join(operator_dir, '%s_feature.pkl' % namei))
        for queryi, features in query2feature[namei].items():
            features_all.extend(features)
    features_all = list(set(features_all))
    # TODO: simplify URI?
    writeList(features_all, os.path.join(operator_dir, 'operator_list_10_new.txt'))

    for featurei in features_all:
        res[featurei] = list(np.zeros(len(data_source)+1))            
    
    for idx, namei in enumerate(data_source):
        for queryi, features in query2feature[namei].items():
            for featurei in features:
                res[featurei][idx] += 1
                res[featurei][-1] += 1

    if 'all' not in data_source:
        data_source.append('all') 
    df = DataFrame.from_dict(res, orient='index', columns=data_source)
    df = df.sort_values(by='all', ascending=False)
    df.to_csv(os.path.join(operator_dir, 'dataset-operator-count.csv'))
    return df

def compUsedOperator(dbpedia=False, data_dir='docs/exportSession', operator_dir='results/operator'):
    """
    comp used features between two continous queries for all datasets.
    generate a table, columns -> different data set
                      index   -> different feature, <feature>_add, <feature>_delete ...
    for all the dataset.
    """
    # data_source = ['lsr', 'mesh', 'ncbigene', 'ndc', 'omim', 'orphanet', 'sabiork', 'sgd', 'sider', 'swdf',
    #            'affymetrix', 'dbsnp', 'gendr', 'goa', 'linkedgeodata', 'linkedspl', 'dbpedia']
    data_source1 = ['ncbigene','ndc','orphanet','sgd','sider','swdf','affymetrix','goa', 'linkedgeodata']
    data_source2 =  ['dbpedia.3.5.1.log',
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
    data_source = data_source1 + data_source2

    error_fo = open(os.path.join(operator_dir, 'comp_operator_error.txt'), 'w')

    res = {}
    indexes = []
    features_all = read_list(os.path.join(operator_dir, 'operator_list_10_new.txt'))

    for featurei in features_all:
        indexes.extend([f'{featurei}_add', f'{featurei}_delete'])
        res[f'{featurei}_add'] = list(np.zeros(len(data_source)+1))
        res[f'{featurei}_delete'] = list(np.zeros(len(data_source)+1))

    for idx, datai in enumerate(data_source):
        dbpedia = False if idx <= 8 else True
        # valuedSession = read_valuedSession(datai, filter_users=False, dbpedia=dbpedia)
        valuedSession = readExportedData(data_dir, datai)
        valuedSession = valuedSession['sessions']
        query2feature = read_pkl(os.path.join(operator_dir, '%s_feature.pkl' % datai))  

        for index, sess in enumerate(valuedSession):
            session = sess['queries']
            session_len = sess['session_length']
            for ith in range(session_len-1):
                query_key = 'index_in_file'
                query1 = session[ith][query_key]
                query2 = session[ith+1][query_key]
                try:
                    feature1 = query2feature[query1]
                    feature2 = query2feature[query2]
                except:
                    continue
                share = [x for x in feature1 if x in feature2]
                not1 = [x for x in feature2 if x not in feature1]
                not2 = [x for x in feature1 if x not in feature2]
                for fi in not1:
                    res[f'{fi}_add'][idx] += 1
                    res[f'{fi}_add'][-1] += 1
                for fi in not2:
                    res[f'{fi}_delete'][idx] += 1
                    res[f'{fi}_delete'][-1] += 1
    if 'all' not in data_source:
        data_source.append('all')
    df = DataFrame.from_dict(res, orient='index', columns=data_source)
    df = df.sort_values(by='all', ascending=False)
    df.to_csv(os.path.join(operator_dir, 'compUsedOperator_10_new.csv'))
    return df


def compUsedOperatorPair(dbpedia=False, debug=False, data_dir='docs/exportSession', operator_dir='results/operator'):
    """
    comp used features between two continous queries for all datasets.
    generate a table, columns -> different data set
                      index   -> different feature, <feature>_add, <feature>_delete ...
    for all the dataset.
    """
    # data_source = ['lsr', 'mesh', 'ncbigene', 'ndc', 'omim', 'orphanet', 'sabiork', 'sgd', 'sider', 'swdf',
    #            'affymetrix', 'dbsnp', 'gendr', 'goa', 'linkedgeodata', 'linkedspl', 'dbpedia']
    data_source1 = ['ncbigene','ndc','orphanet','sgd','sider','swdf','affymetrix','goa', 'linkedgeodata']
    data_source2 =  ['dbpedia.3.5.1.log',
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

    data_source = data_source2

    error_fo = open(os.path.join(operator_dir, 'comp_operator_error.txt'), 'w')

    res = {}
    indexes = []

    for idx, datai in enumerate(data_source):
        # dbpedia = False if idx <= 8 else True
        dbpedia = True
        # valuedSession = read_valuedSession(datai, filter_users=False, dbpedia=dbpedia)
        valuedSession = readExportedData(data_dir, datai)
        valuedSession = valuedSession['sessions']
        query2feature = read_pkl(os.path.join(operator_dir, '%s_feature.pkl' % datai))

        for index, sess in enumerate(valuedSession):
            # DataFrame -> 'query', 'time'
            session = sess['queries']
            session_len = sess['session_length']
            for ith in range(session_len-1):
                query_key = 'index_in_file'
                query1 = session[ith][query_key]
                query2 = session[ith+1][query_key]
                try:
                    feature1 = query2feature[query1]
                    feature2 = query2feature[query2]
                except:
                    continue
                share = [x for x in feature1 if x in feature2]
                not1 = [x for x in feature2 if x not in feature1]
                not2 = [x for x in feature1 if x not in feature2]
                for fi1 in not1:
                    for fi2 in not2:
                        key = (f'{fi1}_add', f'{fi2}_delete')
                        # --------------------debug------------------------
                        if debug:
                            try:
                                if 'ask' in fi1.lower():
                                    if 'select' in fi2.lower():
                                        print(session.iloc[ith]['query'])
                                        print(session.iloc[ith+1]['query'])
                                        from ipdb import set_trace; set_trace()
                            except:
                                pass
                        # ---------------------end---------------------------

                        if key not in res.keys():
                            res[key] = list(np.zeros(len(data_source)+1))
                        res[key][idx] += 1
                        res[key][-1] += 1

    if 'all' not in data_source:
        data_source.append('all')
    df = DataFrame.from_dict(res, orient='index', columns=data_source)
    df = df.sort_values(by='all', ascending=False)
    file_name = os.path.join(operator_dir, 'compUsedOperatorPair_10_new.csv')
    df.to_csv(file_name)
    return df


def CollectNumOfPairs(dbpedia=False):
    """
    comp used features between two continous queries for all datasets.
    generate a table, columns -> different data set
                      index   -> different feature, <feature>_add, <feature>_delete ...
    for all the dataset.
    """
    # data_source = ['lsr', 'mesh', 'ncbigene', 'ndc', 'omim', 'orphanet', 'sabiork', 'sgd', 'sider', 'swdf',
    #            'affymetrix', 'dbsnp', 'gendr', 'goa', 'linkedgeodata', 'linkedspl', 'dbpedia']
    data_source1 = ['ncbigene','ndc','orphanet','sgd','sider','swdf','affymetrix','goa', 'linkedgeodata']
    data_source2 =  ['dbpedia.3.5.1.log',
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
    data_source = data_source1 + data_source2

    error_fo = open('docs/feature/comp_feature_error.txt', 'w')

    queryForms = ['ask', 'select', 'describe', 'construct']
    Patterns = ['path', 'group', 'optional', 'union', 'graph', 'bind', 'having', 'minus', 'filter', 'agg']
    Modifiers = ['orderby', 'projection', 'distinct', 'reduced', 'offset', 'limit']
    fn = ['fn', 'builtin']
    other = ['values', 'service']

    op_list = [queryForms, Patterns, Modifiers, fn, other]
    count_all = [0, 0, 0, 0, 0]
    count_single = [list(np.zeros(5)) for x in range(len(data_source))]
    pair_count = 0

    for idx, datai in enumerate(data_source):
        dbpedia = False if idx <= 8 else True
        valuedSession = read_valuedSession(datai, filter_users=False, dbpedia=dbpedia)
        query2feature = read_pkl('docs/feature/%s_feature.pkl' % datai)  

        for index, sess in enumerate(valuedSession):
            # DataFrame -> 'query', 'time'
            session = sess['session']
            session_len = len(session)
            for ith in range(session_len-1):
                query_key = 'idxInFile' if dbpedia else 'query'
                query1 = session.iloc[ith][query_key]
                query2 = session.iloc[ith+1][query_key]
                try:
                    feature1 = query2feature[query1]
                    feature2 = query2feature[query2]
                except:
                    continue
                pair_count += 1
                not1 = [x for x in feature2 if x not in feature1]
                not2 = [x for x in feature1 if x not in feature2]
                
                for op_listi, ops in enumerate(op_list):
                    for opi, oneop in enumerate(ops):
                        if_next = False
                        for changeOp in (not1 + not2):
                            if oneop.lower() in str(changeOp).lower():
                                count_all[op_listi] += 1
                                count_single[idx][op_listi] += 1
                                if_next = True
                                break
                        if if_next:
                            break
    return count_all, count_single, pair_count