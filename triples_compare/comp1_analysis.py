import sys
import os
o_path = os.getcwd()
sys.path.append(o_path)

from pandas import DataFrame
from file_utils.readfile import read_valuedSession, read_pkl, read_list

def countKeys(name, fromFile=True):
    """
    from info get operator list.
    
    input:
        name: str, used to locate files which contains some info.
              files we used: <file_name>_info.pkl
                             <file_name>_valuedSession.txt
                             <file_name>_info_error.txt
    
    output:
        keys_dict, a list of dict.
                 [  triple keys count
                     {
                         'operator in triple': int -> times this operator appears
                     }
                     filter ...
                     other ...
                 ]
    """
    valudSession =  read_valuedSession(name)
    query_info = read_pkl(f'docs/info/{name}_info.pkl')
    error_file_name = 'docs/info/%s_info_error.txt' % name
    
    triple_keys = {}
    filter_keys = {}
    others_keys = {}
    keys_dict = [triple_keys, filter_keys, others_keys]
    ith_key = [0, 1, -1]
    
    error_list = list(read_list(error_file_name, sep='<sep>').keys())

    for index, sess in enumerate(valudSession):
        # DataFrame -> 'query', 'time'
        session = sess['session']
        session_len = len(session)
        for ith in range(session_len-1):
            query1 = session.iloc[ith]['query']
            query2 = session.iloc[ith+1]['query']
            if query1 in error_list or query2 in error_list:
                continue
            else:
                query_info1 = query_info[query1]
                query_info2 = query_info[query2]
                # from ipdb import set_trace; set_trace()
                for i in range(len(ith_key)):
                    keys = list(query_info1[ith_key[i]].keys())
                    keys.extend(list(query_info2[ith_key[i]].keys()))
                    keys = list(set(keys))
                    for ki in keys:
                        if ki not in keys_dict[i].keys():
                            keys_dict[i][ki] = 0
                        keys_dict[i][ki] += 1
    return keys_dict

def countKeys_all(data_source):
    """
    generate keys count for all the data_source.

    input:
        data_source, a list of data_sources
    
    output:
        dict, {
            data_sourcei:{
                [{triple operators count}, {filter}, {other}]
            }
        }
    """
    res = {}
    res_all = [{}, {}, {}]
    for i in data_source:
        temp = countKeys(i)
        res[i] = temp
        for ith, ti in enumerate(temp):
            for keyi, count in ti.items():
                if keyi not in res_all[ith].keys():
                    res_all[ith][keyi] = 0
                res_all[ith][keyi] += count
    res['all'] = res_all
    return res

def Generate_csv(keys_count, data_file, file_name, ith=0):
    """
    generate a csv, with index: operator1, operator2 ...
                         columns: data_file1, data_file2 ... all
                         for all the data_sources
                         
    input:
        keys_count: a dict, result from countKeys_all
        data_file: a list of str, data_sources
        file_name: which file to store this csv
        ith: 0 -> use keys_count['data_source'][ith], triples, generate a operator distribution for triple
             1 -> filter
             2 -> other
    """
    if 'all' not in data_file:
        data_file.append('all')
    keys_list = list(keys_count['all'][ith].keys())
    res = []
    for index, ki in enumerate(keys_list):
        res.append([])
        for di in data_file:
            if ki in keys_count[di][ith].keys():
                res[index].append(keys_count[di][ith][ki])
            else:
                res[index].append(0)
    df = DataFrame(res, columns=data_file, index=keys_list)
    try:
        df = df.sort_values(by='all', ascending=False)
    except:
        df = df.sort_values(by='linkedgeodata', ascending=False)
    df.to_csv(file_name)
    return df

def Generate_count_csv(count, file_name):
    # input: count -> comp_count['all']['Triple']
    """
    generate a csv with index -> operators
                        columns -> new, old, add ...
                        for one data source
    input -> 
    {
        operator: {new_count: xx, old_count: xx, add_count:xx, delete_count:xx, change_count:xx, 
                        change_type_count: {'type1': xx ...}}
    }

    output:
        DataFrame
    """
    columns = ['new', 'old', 'add', 'delete', 'change']
    column_key = ['new_count', 'old_count', 'add_count', 'delete_count', 'change_count']
    keys_list = list(count.keys())
    res = {}
    for ki in keys_list:
        res[ki] = []
        for ci in column_key:
            if ci in count[ki].keys():
                res[ki].append(count[ki][ci])
            else:
                res[ki].append(0)
    df = DataFrame.from_dict(res, orient='index', columns=columns)
    df = df.sort_values(by='change', ascending=False)
    df.to_csv(file_name)
    return df

def gene_count_csv_all(data_source_all, comp_count, block):
    """
    generate a csv with index -> operators
                        columns -> new, old, add ...
                        for all the data sources
    input:
        data_source_all: a list of str, data sources
        comp_count: result from compare_count.py
        block: 'Triple', 'Filter' or 'Other'
    
    run this function with three block name will generate all count info. (old, new, add ...)
    """
    if 'all' not in data_source_all:
        data_source_all.append('all')
    for i in data_source_all:
        file_name = 'results/triples_comp_analysis/%s_%s_change_count_10_once.csv' % (i, block)
        df = Generate_count_csv(comp_count[i][block], file_name)

def Generate_changeType_count_csv(count, file_name, columns, column_key, filter_=False):
    # input: count -> comp_count['all']['Triple']  what we need ->  (['Main']['change_type_count'])
    """
    generate change type (arg, func, subject, object ...) change times
    should be very careful when dealing with filter.
    see what type it has first, then adjust code

    input: 
        count, comp_count[file_name][triple/filter]   ->  [operator][change_type_count]

    """
    keys_list = list(count.keys())
    res = {}
    for ki in keys_list:
        res[ki] = []
        for ci in column_key:
            if ci in count[ki]['change_type_count'].keys():
                res[ki].append(count[ki]['change_type_count'][ci])
            else:
                res[ki].append(0)
    df = DataFrame.from_dict(res, orient='index', columns=columns)
    if filter_:
        args = ['arg%d' % (x+1) for x in range(11)]
        args.extend(['first', 'rest', 'subject', 'object'])
        del df['Filter']
        for index, argi in enumerate(args):
            if index == 0:
                df['arg'] = df[argi]
            else:
                df['arg'] += df[argi]
            del df[argi]
        df['func'] = df['type']
        del df['type']
    try:
        df = df.sort_values(by='subject', ascending=False)
    except:
        pass
    df.to_csv(file_name)
    return df

def generate_columns(comp_count, block):
    # columns in comp_count['all']['Triple']['Main']['change_type_count'].keys()
    columns = []
    column_keys = []
    for i,j in comp_count['all'][block].items():
        for k,x in j['change_type_count'].items():
            if k not in column_keys:
                column_keys.append(k)
                if '#' in k:
                    columni = k[1:-1].split('#')[-1]
                elif '/' in k:
                    columni = k[1:-1].split('/')[-1]
                else:
                    columni = k
                columns.append(columni)
    columns.remove('add_count')
    columns.remove('delete_count')
    column_keys.remove('add_count')
    column_keys.remove('delete_count')
    return columns, column_keys

def gene_change_type_count_csv_all(data_source_all, comp_count, block):
    if 'all' not in data_source_all:
        data_source_all.append('all')
    columns, column_keys = generate_columns(comp_count, block)
    for i in data_source_all:
        file_name = 'results/triples_comp_analysis/%s_%s_type_change_count_10.csv' % (i, block)
        df = Generate_changeType_count_csv(comp_count[i][block], file_name, columns, column_keys)