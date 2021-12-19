import sys
import os
o_path = os.getcwd()
sys.path.append(o_path)

import numpy as np
import pandas as pd
from pandas import DataFrame, read_csv
from file_utils.readfile import read_loop_result_fre, read_freq_file
from file_utils.readfile import read_loop_fre_sess_count, read_freq_sess_count
from file_utils.readfile import readAtypicalUsers

def get_timeSpan_queryNum(agents, data, agent_index, time_index='time'):
    '''
    To know every's agent's time_span and amount of query
    agents: a list of unique agent
    data: data_frame
    agent_index, str, the index of agent in data
    return: time_span, a list of time_span of every agent
    combined, a list of [agent, time_span, querys]
    '''
    time_span_ = list(np.zeros(len(agents)))
    querys_ = list(np.zeros(len(agents)))
    combined = list(np.zeros(len(agents)))
    for i, agent in enumerate(agents):
        timeline = data.loc[data[agent_index] == agent][time_index].tolist()
        querys_[i] = len(data.loc[data[agent_index] == agent])
        timeline.sort()
        time_span_[i] = timeline[-1] - timeline[0]
        combined[i] = [agent, time_span_[i], querys_[i]]
    return time_span_, combined


def CountByThresh(thre, data):
    '''
    count how many by threshold
    thre, a list of thre, such as thre = [1, 10, 20, 50, 100, 200, 500, 1000, 2000, 3000, 4000]
    data, a list of data need to be counted, this list should be sorted
    '''
    k = 0
    data.sort()
    # if thre[-1] != max(data):
    #    amount_query = list(np.zeros(len(thre) + 1))
    # else:
    amount_query = list(np.zeros(len(thre)))
    for i, threi in enumerate(thre):
        start = k
        if k >= len(data):
            break
        while(k<len(data) and data[k]<=threi):
            k += 1
        end = k
        amount_query[i] = end - start
    # if thre[-1] != max(data):
    #    amount_query[-1] = len(data) - k
    return amount_query

def CombinedSortByThresh(thre_x, threBasedOnX, data):
    '''
    first thresh by x, (query times)
    then thresh based on the result of x (time_span for instance)
    data, combined from  get_timeSpan_queryNum (original version)
          ----- changed in 2020.02.11 ---------
    data, DataFrame with keys ['agent', 'time_span', 'querys']
    '''
    # combinedData = DataFrame(data, columns=['ip', 'time_span','querys'])
    combinedData = data
    sorted_df = combinedData.sort_values(by='time_span')
    combinedSort = list(np.zeros(len(threBasedOnX) + 1))
    for i, timei in enumerate(threBasedOnX):
        temp = list(np.zeros(len(thre_x)+1))
        if i != 0:
            filtered = sorted_df.loc[sorted_df['time_span']<=timei]
            filtered = filtered.loc[filtered['time_span']>threBasedOnX[i-1]]
        else:
            filtered = sorted_df.loc[sorted_df['time_span']<=timei]
        filtered.sort_values(by='querys')
        for k, threi in enumerate(thre_x):
            if k == 0:
                temp[k] = len(filtered.loc[filtered['querys']<=threi])
            else:
                temp_k = filtered.loc[filtered['querys']<=threi]
                temp_k = temp_k.loc[temp_k['querys']>thre_x[k-1]]
                temp[k] = len(temp_k)
        temp[k+1] = len(filtered.loc[filtered['querys']>thre_x[k]])
        combinedSort[i] = temp
    temp = list(np.zeros(len(thre_x)+1))
    filtered = sorted_df.loc[sorted_df['time_span']>threBasedOnX[i]]
    for k, threi in enumerate(thre_x):
        if k == 0:
            temp[k] = len(filtered.loc[filtered['querys']<=threi])
        else:
            temp_k = filtered.loc[filtered['querys']<=threi]
            temp_k = temp_k.loc[temp_k['querys']>thre_x[k-1]]
            temp[k] = len(temp_k)
    temp[k+1] = len(filtered.loc[filtered['querys']>thre_x[k]])
    combinedSort[i+1] = temp
    return combinedSort


def GenerateThre(l, num, mark='x', smaller1=0.1, smaller2=0.5, bigger=1.1, stardard=5, kk=1.5):
    """
    generate threshold automatically.

    input:
        list: a list need to split
        num, int, how many [] should split into.
    output:
        thre: a list with threshold
        label: a list with str (which will be useful when plotting)
        mark: str, used when generating label, such as 6<'mark'<=10
        smaller: make [] in small position smaller
        stardard: make thre%5 = 0
    """
    l.sort()
    temp = int((max(l) - min(l))/num)
    thre = []
    thre.append(min(l))
    for i in range(num):
        i += 1
        if i == num:
            thre.append(max(l))
        else:
            if i < num/kk:
                tempi = min(l)+int(i*temp*smaller1)
            elif i < (2.5)*num/kk:
                tempi = min(l)+int(i*temp*smaller2)
            else:
                tempi = min(l)+int(i*temp*bigger)
            tempi = tempi - tempi % stardard
            thre.append(tempi)
    label = []
    label.append(str(thre[0]))
    for i in range(num):
        label.append(str(thre[i])+'<'+mark+'<='+str(thre[i+1]))
    return thre, label

def GetQueryThre(session=False):
    # query_thre = [1,5,10,30,50,80,100,200,500,1000,5000]
    query_thre = [1,5,20,50,80,1000,3000]
    query_label = []
    for i, thre in enumerate(query_thre):
        if i == 0:
            query_label.append('x=%d' % thre)
        else:
            query_label.append('%d<x<=%d' % (query_thre[i-1], thre))
    query_label.append('x>%d' % thre)
    if session:
        return query_thre[1:], query_label[1:]
    return query_thre, query_label

def processLoopType(name):
    """
    process dataFrame in 'docs/loop/<file_name>_loop_type.csv' into format of plot_combined function in drawer.py
    
    input:
        name: data source name, a list of str
    output:
        x_label, combinedSort, column_label
    -------------------------------update 20200225----------------------------
    delete column name 'repetitions'
    ------------------------------------end-----------------------------------
    """
    # keys: len, type
    df = read_csv('docs/apweb_final/loop/%s_loop_type.csv' % name)
    column_label = ['no pattern', 'intra loop pattern', 'sequence of intra loop pattern',
                    'inter loop pattern']
                    #'repetitions']
    query_thre, x_label = GetQueryThre(session=True)
    column_thre = [-1, 0, 1, 2, 3]
    combinedSort = []
    for ci in column_thre:
        # from ipdb import set_trace; set_trace()
        df_type = df.loc[df['type']==ci]
        len_sort = []
        for i, qi in enumerate(query_thre):
            if i==0:
                temp = df_type.loc[df_type['len']<=qi]
            else:
                temp = df_type.loc[df_type['len']<=qi]
                # from ipdb import set_trace; set_trace()
                temp = temp.loc[temp['len']>query_thre[i-1]]
            len_sort.append(len(temp))
        temp = df_type.loc[df_type['len']>qi]
        len_sort.append(len(temp))
        combinedSort.append(len_sort)
    # return combinedSort
    #----------------update 20200225------------------
    # merge the last two types
    for i in range(len(combinedSort[0])):
        combinedSort[0][i] += combinedSort[-1][i]
    combinedSort = combinedSort[:-1] 
    #---------------------end-------------------------

    return x_label, combinedSort, column_label

def CountLoop(name):
    # keys: len, type
    df = read_csv('docs/apweb_final/loop/%s_loop_type.csv' % name)
    query_thre, x_label = GetQueryThre(session=True)
    len_sort = []
    for i, qi in enumerate(query_thre):
        if i==0:
            temp = df.loc[df['len']<=qi]
            len_sort.append(len(temp))
        else:
            temp = df.loc[df['len']<=qi]
            temp = temp.loc[temp['len']>query_thre[i-1]]
            len_sort.append(len(temp))
    len_sort.append(len(df.loc[df['len']>qi]))
    return len_sort

def drawTimeDistributionInOneDay(sess, test=False, res=[], period=None, filter_users=None):
    """
    filter, a list of users. Remove these users.
    """
    # count for every hour 0~23
    agents_mor = {}
    all_ = 0
    if res == []:
        res = list(np.zeros(24))
    for si in sess:
        querys = si['session']
        agent = si['agent']

        # ------remove users with atypical behaviours--------
        if filter_users != None and agent in filter_users:
            continue
        # --------------------------------------------------

        for i in range(len(querys)):
            all_ += 1
            hour = querys.iloc[i]['time'].hour
            res[hour] += 1

            # -----------debug mode--------------------
            if period != None and test == True:
                if hour >= period[0] and hour <= period[1]:
                    if agent not in agents_mor.keys():
                        agents_mor[agent] = 0
                    agents_mor[agent] += 1
            #------------------------------------------
    if test == True:
        return res, agents_mor, all_
    else:
        return res

def getTimeDisWithFreq(name, filter_users=False):
    """
    get loop related info from Valuedsession dir (sess_loop, sess_no_loop)
    get frequency related info from frequency dir (sess_loop + frequency, sess_no_loop)
    filter, bool, whether or not filter atypical users.
    """
    users = None
    if filter_users:
        users = readAtypicalUsers(name)
    sess_loop, sess_no_loop = read_loop_result_fre(name)
    freq_query = read_freq_file(name)
    res_mach = []
    res_human = []
    res_mach = drawTimeDistributionInOneDay(sess_loop, res_mach)
    res_mach = drawTimeDistributionInOneDay(freq_query, res_mach)
    res_human = drawTimeDistributionInOneDay(sess_no_loop, res_human, filter_users=users)
    return res_mach, res_human

def getClaNum(name):
    sess_loop, sess_no_loop = read_loop_result_fre(name)
    freq_query = read_freq_file(name)
    res, _, sess_loop_count = drawTimeDistributionInOneDay(sess_loop, test=True)
    res, _, sess_no_loop_count = drawTimeDistributionInOneDay(sess_no_loop, test=True)
    res, _, freq_count = drawTimeDistributionInOneDay(freq_query, test=True)
    count_mach = sess_loop_count + freq_count
    count_human = sess_no_loop_count
    sessionCountFilterByLoop, session_count_valued = read_loop_fre_sess_count(name)
    sessionCountFilterByFreq = read_freq_sess_count(name)
    return count_mach, count_human, sess_loop_count, freq_count, sessionCountFilterByFreq, sessionCountFilterByLoop, session_count_valued