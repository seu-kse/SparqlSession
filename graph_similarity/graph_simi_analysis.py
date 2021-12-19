import sys
import os
o_path = os.getcwd()
sys.path.append(o_path)

from vis_utils.process_data import CountByThresh
import matplotlib.pyplot as plt
from pandas import DataFrame, Series
import seaborn as sns

def countSame(simi, name='conti'):
    """
    count queries with the same structure.
    simi: -> a list of simi-list for every data source 
    name: -> 'conti' or 'first'
    """
    # data_source = ['lsr', 'mesh', 'ncbigene', 'ndc', 'omim', 'orphanet', 'sabiork', 'sgd', 'sider', 'swdf',
    #            'affymetrix', 'dbsnp', 'gendr', 'goa', 'linkedspl']
    data_source =  ['ncbigene',
                    'ndc',
                    'orphanet',
                    'sgd',
                    'sider',
                    'swdf',
                    'affymetrix',
                    'goa',
                    'linkedgeodata',
                    'access.log-20151124',
                    'access.log-20151213']
    """
    data_source = ['dbpedia.3.5.1.log','access.log-20151025', 'access.log-20151124','access.log-20151126',
            'access.log-20151213','access.log-20151230','access.log-20160117','access.log-20160212',
            'access.log-20160222','access.log-20160301','access.log-20160303','access.log-20160304',
            'access.log-20160314','access.log-20160411']
    """

    thre = [0, 0.2, 0.3, 0.5, 0.8, 1.0]
    label = ['0', '0<x<=0.2', '0.2<x<=0.3', '0.3<x<=0.5', '0.5<x<=0.8', '0.8<x<=1.0']
    simi_all = []
    df = []
    for idx, simi_i in enumerate(simi):
        simi_single = []
        temp = []
        for i in simi_i:
            simi_single.extend(i)
        simi_single.sort()
        count = CountByThresh(thre, simi_single)
        simi_all.extend(simi_single)

        temp.append(data_source[idx])
        temp.extend(count)
        df.append(temp)
    
    simi_all.sort()
    count = CountByThresh(thre, simi_all)
    temp = []
    temp.append('all')
    temp.extend(count)
    df.append(temp)

    columns = []
    columns.append('data source')
    columns.extend(label)
    df = DataFrame(df, columns=columns)
    df.to_csv(f'figs/tree/ontology_{name}.csv', index=False)
    return df
    
def GraphSimiChangeOverTime(simi, name='conti', data_source=None):
    """
    simi: -> a list of simi-list for every data source 
    output, a list of series. Each series store graph similarity in this step.
    """

    def ListToSeries(data):
        # from ipdb import set_trace; set_trace()
        res = []
        for i in data:
            res.append(Series(i))
        return res

    # data_source = ['lsr', 'mesh', 'ncbigene', 'ndc', 'omim', 'orphanet', 'sabiork', 'sgd', 'sider', 'swdf',
    #            'affymetrix', 'dbsnp', 'gendr', 'goa', 'linkedspl']
    # data_source = ['ncbigene','ndc','orphanet','sgd','sider','swdf','affymetrix','goa', 'linkedgeodata']
    """
    data_source =  ['ncbigene',
                    'ndc',
                    'orphanet',
                    'sgd',
                    'sider',
                    'swdf',
                    'affymetrix',
                    'goa',
                    'linkedgeodata',
                    'access.log-20151124',
                    'access.log-20151213']

    data_source = ['dbpedia.3.5.1.log','access.log-20151025', 'access.log-20151124','access.log-20151126',
            'access.log-20151213','access.log-20151230','access.log-20160117','access.log-20160212',
            'access.log-20160222','access.log-20160301','access.log-20160303','access.log-20160304',
            'access.log-20160314','access.log-20160411']
    """
    ans = {}
    res = []
    # for each data set
    for idx, simi_i in enumerate(simi):
        res_single = []
        # for each session
        for simi_sess in simi_i:
            for i, j in enumerate(simi_sess):
                if i >= len(res):
                    res.append([])
                if i >= len(res_single):
                    res_single.append([])
                res[i].append(j)
                res_single[i].append(j)

        ans[data_source[idx]] = ListToSeries(res_single)

    ans['all'] = ListToSeries(res)
    return ans

def plotSimiChangeOverTime(simi, name):
    """
    plot a line chart with three values, mean, max and min
    simi -> return value from GraphSimiChangeOverTime for one dataset
    """
    fig = plt.figure()
    ax = plt.subplot(111)

    plt.xlabel('i', fontsize=15)
    # plt.ylabel('Normalized Graph Edit Distance', fontsize=15)
    plt.ylabel('Normalized Graph Edit Distance', fontsize=15)

    x = [i+1 for i in list(range(len(simi)))]
    y_min = [i.min() for i in simi]
    # from ipdb import set_trace; set_trace()
    y_mean = [j.mean() for j in simi]
    y_max = [k.max() for k in simi]
    y_var = [i.var() for i in simi]

    # plt.plot(x, y_min, label='min')
    plt.plot(x, y_mean, 
            color='red',   
            linewidth=1.0,  
            linestyle='--',
            label='mean'
            )
    plt.plot(x, y_var,
            color='green',
            linewidth=1.0,
            linestyle='-.',
            label='var')
    plt.grid(linestyle='--')
    plt.tight_layout()
    plt.legend(fontsize=15)
    fig.savefig(name, bbox_inches = 'tight')

def plotLineBySeanborn(simi, name):
    res = []
    for i, SimiInSteps in enumerate(simi):
        for j in SimiInSteps:
            res.append([i+1, j])
    df = DataFrame(res, columns=['Step', 'Normalized Graph Edit Distance'])
    sns_plot = sns.relplot(x="Step", y='Normalized Graph Edit Distance', kind="line", data=df)
    plt.grid(linestyle='--')

    x = [i+1 for i in list(range(len(simi)))]
    y_var = [i.var() for i in simi]
    y_mean = [j.mean() for j in simi]

    plt.plot(x, y_mean, 
            color='blue',   
            linewidth=1.0,  
            linestyle='-',
            label='mean'
            )
    plt.plot(x, y_var,
            color='red',
            linewidth=1.0,
            linestyle='-.',
            label='var')
    plt.grid(linestyle='--')
    plt.legend(fontsize=15)

    sns_plot.savefig(name)