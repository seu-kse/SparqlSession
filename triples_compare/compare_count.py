import sys
import os
o_path = os.getcwd()
sys.path.append(o_path)

from tqdm import tqdm
from file_utils.readfile import read_pkl
from file_utils.writefile import write_pkl

def count_changes(name, dict_key=['Triple', 'Filter'], list_key=[], pkl=None):
    """
    name -> data source name
    count_changes about some operator, is this operator new? or old? 
                                    inside this block, has some triples added? deleted?
                                    in the mappings of these triple, how did these change?
                                    where change and how to change?
    Triple/Filter/Other: {
        block_name: {new_count: xx, old_count: xx, add_count:xx, delete_count:xx, change_count:xx, 
                    change_type_count: {'type1': xx ...}}
    }
    Bind/Values/Graph/Service : {add_count:xx, delete_count:xx, change_count:xx}

    --------------------------update 20200304----------------------------
    original -> read com_info from file
    now -> pass as para directly
    dict_key, list_key -> only consider 'Triple' 'Filter' in dict_keys
    pkl -> pass comp_info 
    ------------------------------end-------------------------------------
    """
    res = {}
    # dict_index = [0, 1, -1]
    # dict_key = ['Triple', 'Filter', 'Other']
    # list_index = [2, 3, 4, 5]
    # list_key = ['Bind', 'Service', 'Values', 'Graph']
    dict_index = range(len(dict_key))
    list_index = range(len(list_key))
    if pkl == None:
        pkl = read_pkl(f'docs/compare/{name}_comp_info.pkl')
    for changei in tqdm(pkl):
        for i, index in enumerate(dict_index):
            if dict_key[i] not in res.keys():
                res[dict_key[i]] = {}
            op = changei['comp_info'][dict_key[i]]
            for block, block_change in op.items():
                if block not in res[dict_key[i]].keys():
                    res[dict_key[i]][block] = {'new_count': 0, 'old_count': 0, 'add_count': 0, 'delete_count': 0,
                                              'change_count': 0, 'change_type_count': {'add_count': 0, 'delete_count': 0}}
                # from ipdb import set_trace; set_trace()
                # --------------------update 20200306----------------------
                # ori: 'old': True or False
                # now: 'old': info about this block
                # change 'if 'old' in block_change.keys() and block_change['old'] == True:'
                # to 'if 'old' in block_change.keys():'
                # -------------------------end-----------------------------
                if 'old' in block_change.keys():
                    res[dict_key[i]][block]['old_count'] += 1
                if 'new' in block_change.keys():
                    res[dict_key[i]][block]['new_count'] += 1
                if 'add' in block_change.keys() and len(block_change['add']) != 0:
                    res[dict_key[i]][block]['add_count'] += len(block_change['add'])
                if 'delete' in block_change.keys() and len(block_change['delete']) != 0:
                    res[dict_key[i]][block]['delete_count'] += len(block_change['delete'])
                if 'change' in block_change.keys() and len(block_change['change']) != 0:
                    # res[dict_key[i]][block]['change_count'] += len(block_change['change'])
                    # ------------------update 20200306------------------
                    # check if change is empty
                    # ------------------------end------------------------
                    
                    for block_changei in block_change['change']:
                        temp = block_changei['change']
                        if 'add' in temp.keys() and len(temp['add']) != 0:
                            res[dict_key[i]][block]['change_type_count']['add_count'] += 1
                        if 'delete' in temp.keys() and len(temp['delete']) != 0:
                            res[dict_key[i]][block]['change_type_count']['delete_count'] += 1
                        if 'change' in temp.keys():
                            for rewrite in temp['change']:
                                if 'change' in rewrite.keys():
                                    if rewrite['change'] not in res[dict_key[i]][block]['change_type_count'].keys():
                                        res[dict_key[i]][block]['change_type_count'][rewrite['change']] = 0
                                    res[dict_key[i]][block]['change_type_count'][rewrite['change']] += 1
                                    # ---------------------update 20200306-----------------
                                    res[dict_key[i]][block]['change_count'] += 1
                                    # ----------------------------end----------------------
                                    
        for i, index in enumerate(list_index):
            # from ipdb import set_trace; set_trace()
            temp = changei['comp_info'][list_key[i]]
            if list_key[i] not in res.keys():
                res[list_key[i]] = {'add_count': 0, 'delete_count': 0, 'change_count': 0}
            if 'add' in temp.keys() and len(temp['add']) != 0:
                res[list_key[i]]['add_count'] += len(temp['add']) 
            if 'delete' in temp.keys() and len(temp['delete']) != 0:
                res[list_key[i]]['delete_count'] += len(temp['delete'])
            if 'change' in temp.keys() and len(temp['change']) != 0:
                res[list_key[i]]['change_count'] += len(temp['change'])     
    return res

def count_changes_all(dict_key=['Triple', 'Filter'], list_key=[], count_changes_list=None, data_source=None):
    """
    add count_changes (return from count_changes) for every data set together.
    count_changes_list -> a list of count_changes for every data set.
    data_source -> a list of str, data set name
    dict_key, list_key -> only consider 'Triple' 'Filter' in dict_keys

    return -> 
        file_name/all -> 
        Triple/Filter/Other: { 
            block_name: {new_count: xx, old_count: xx, add_count:xx, delete_count:xx, change_count:xx, 
                        change_type_count: {'type1': xx ...}}
        }
        Bind/Values/Graph/Service : {add_count:xx, delete_count:xx, change_count:xx}
    """
    res = {}
    res_all = {}
    for idx, i in enumerate(data_source):
        # res[i] = count_changes(i)
        res[i] = count_changes_list[idx]
        # add to res_all
        # for dict
        for key in dict_key:
            if key not in res_all.keys():
                res_all[key] = {}

            temp = res[i][key]

            for block, block_change in temp.items():
                if block not in res_all[key].keys():
                    res_all[key][block] = {'new_count': 0, 'old_count': 0, 'add_count': 0, 'delete_count': 0,
                                            'change_count': 0, 'change_type_count': {'add_count': 0, 'delete_count': 0}}
                ite = ['new_count', 'old_count', 'add_count', 'delete_count',
                      'change_count']
                for itei in ite:
                    res_all[key][block][itei] += block_change[itei]
                # from ipdb import set_trace; set_trace()
                change_type_count = block_change['change_type_count']
                for ii, jj in change_type_count.items():
                    if ii not in res_all[key][block]['change_type_count'].keys():
                        res_all[key][block]['change_type_count'][ii] = 0
                    res_all[key][block]['change_type_count'][ii] += block_change['change_type_count'][ii]
        # list
        for key in list_key:
            from ipdb import set_trace; set_trace()
            temp = res[i][key]
            if key not in res_all.keys():
                res_all[key] = {'add_count': 0, 'delete_count': 0, 'change_count': 0}

            ite = ['add_count', 'delete_count', 'change_count']
            for itei in ite:
                res_all[key][itei] += temp[itei]
    # print(res_all)
    res['all'] = res_all
    return res

if __name__ == "__main__":
    data_source = ['affymetrix', 'dbsnp', 'gendr', 'goa', 'linkedgeodata', 'linkedspl']
    dict_key = ['Triple', 'Filter', 'Other']
    list_key = ['Bind', 'Service', 'Values', 'Graph']
    res = {}
    res_all = {}
    for i in data_source:
        res[i] = count_changes(i)
        # add to res_all
        # for dict
        for key in dict_key:
            if key not in res_all.keys():
                res_all[key] = {}

            temp = res[i][key]

            for block, block_change in temp.items():
                if block not in res_all[key].keys():
                    res_all[key][block] = {'new_count': 0, 'old_count': 0, 'add_count': 0, 'delete_count': 0,
                                            'change_count': 0, 'change_type_count': {'add_count': 0, 'delete_count': 0}}
                ite = ['new_count', 'old_count', 'add_count', 'delete_count',
                      'change_count']
                for itei in ite:
                    res_all[key][block][itei] += block_change[itei]
                # from ipdb import set_trace; set_trace()
                change_type_count = block_change['change_type_count']
                for ii, jj in change_type_count.items():
                    if ii not in res_all[key][block]['change_type_count'].keys():
                        res_all[key][block]['change_type_count'][ii] = 0
                    res_all[key][block]['change_type_count'][ii] += block_change['change_type_count'][ii]
        # list
        for key in list_key:
            from ipdb import set_trace; set_trace()
            temp = res[i][key]
            if key not in res_all.keys():
                res_all[key] = {'add_count': 0, 'delete_count': 0, 'change_count': 0}

            ite = ['add_count', 'delete_count', 'change_count']
            for itei in ite:
                res_all[key][itei] += temp[itei]
    # print(res_all)
    res['all'] = res_all
    write_pkl('docs/compare_count.pkl', res)



def count_changes_block(name, dict_key=['Triple', 'Filter'], list_key=[], pkl=None):
    """
    name -> data source name
    count_changes about some operator, is this operator new? or old? 
                                    inside this block, has some triples added? deleted?
                                    in the mappings of these triple, how did these change?
                                    where change and how to change?
    Triple/Filter/Other: {
        block_name: {new_count: xx, old_count: xx, add_count:xx, delete_count:xx, change_count:xx, 
                    change_type_count: {'type1': xx ...}}
    }
    Bind/Values/Graph/Service : {add_count:xx, delete_count:xx, change_count:xx}

    --------------------------update 20200304----------------------------
    original -> read com_info from file
    now -> pass as para directly
    dict_key, list_key -> only consider 'Triple' 'Filter' in dict_keys
    pkl -> pass comp_info 
    ------------------------------end-------------------------------------
    """
    res = {}
    dict_index = range(len(dict_key))
    list_index = range(len(list_key))
    if pkl == None:
        pkl = read_pkl(f'docs/compare/{name}_comp_info.pkl')
    for changei in tqdm(pkl):
        for i, index in enumerate(dict_index):
            if dict_key[i] not in res.keys():
                res[dict_key[i]] = {}
            op = changei['comp_info'][dict_key[i]]
            for block, block_change in op.items():
                if block not in res[dict_key[i]].keys():
                    res[dict_key[i]][block] = {'new_count': 0, 'old_count': 0, 'add_count': 0, 'delete_count': 0,
                                              'change_count': 0, 'change_type_count': {'add_count': 0, 'delete_count': 0}}

                if 'old' in block_change.keys():
                    res[dict_key[i]][block]['old_count'] += 1
                if 'new' in block_change.keys():
                    res[dict_key[i]][block]['new_count'] += 1
                if 'add' in block_change.keys() and len(block_change['add']) != 0:
                    res[dict_key[i]][block]['add_count'] += 1
                if 'delete' in block_change.keys() and len(block_change['delete']) != 0:
                    res[dict_key[i]][block]['delete_count'] += 1
                if 'change' in block_change.keys() and len(block_change['change']) != 0:
                    res[dict_key[i]][block]['change_count'] += 1
                    
                    for block_changei in block_change['change']:
                        temp = block_changei['change']
                        if 'add' in temp.keys() and len(temp['add']) != 0:
                            res[dict_key[i]][block]['change_type_count']['add_count'] += 1
                        if 'delete' in temp.keys() and len(temp['delete']) != 0:
                            res[dict_key[i]][block]['change_type_count']['delete_count'] += 1
                        if 'change' in temp.keys():
                            for rewrite in temp['change']:
                                if 'change' in rewrite.keys():
                                    if rewrite['change'] not in res[dict_key[i]][block]['change_type_count'].keys():
                                        res[dict_key[i]][block]['change_type_count'][rewrite['change']] = 0
                                    res[dict_key[i]][block]['change_type_count'][rewrite['change']] += 1
                                 
    return res