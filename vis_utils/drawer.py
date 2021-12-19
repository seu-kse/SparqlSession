import sys
import os
o_path = os.getcwd()
sys.path.append(o_path)

import matplotlib.pyplot as plt
import matplotlib
import numpy as np
from vis_utils.process_data import getTimeDisWithFreq 

def plot_scatter(x_, y_, name, fig_size=(15, 10), mark_every1=30, mark_every2=15, thresh=400, 
                xlab='x', ylab='y', title='title', xlog=True):
    '''
    x axis is for agent rank
    y axis is for time_span/number of querys for every agent
    y: a list of timedelta if for time_span
    y: a list of int if for querys 
    fig_size: tuple, the fig_size of figure
    name: str, the name of saving file
    thresh: int, to see data more clearly, data less than thresh using one mark_every, and data more than thresh using another
    mark_every1/2: int, before/after thresh, using different frequency's mark
    '''

    fig = plt.figure()
    ax = plt.subplot(111)

    # Plot
    ax.scatter(x_, y_, alpha=0.5)
    #plt.title(title, fontsize=20)
    plt.xlabel(xlab, fontsize=25)
    plt.ylabel(ylab, fontsize=25)
    if ylab == 'len2/len_Ori':
        xfontsize = 15
    else:
        xfontsize = 17
    plt.xticks(fontsize=xfontsize)
    plt.yticks(fontsize=18)
    if xlog:
        ax.set_xscale('log')

    for x,y in zip(x_, y_):
        
        if x%mark_every1 != 0 and x <= thresh:
            continue
        elif x > thresh and x%mark_every2 != 0:
            if x != len(x_)-1 and x != len(x_)-10 and x!=len(x_)-15: # the last two has to mark
                continue
        
        label = "{:.2f}".format(y)
        """
        temp = timedelta(seconds=y)
        if temp.days != 0:
            label = str(temp.days) + 'd,' + str(temp.seconds/3600) + 'h'
        else:
            label = str(temp.seconds/3600) + 'h'
        """

        plt.annotate(label, # this is the text
                     (x,y), # this is the point to label
                     textcoords="offset points", # how to position the text
                     xytext=(0,5), # distance from text to points (x,y)
                     ha='center', # horizontal alignment can be left, right or center
                     fontsize=20)
        
    #plt.show()
    fig.savefig(name, bbox_inches = 'tight')

def plot_line(x_, y_, name, mark_every1=30, mark_every2=15, thresh=400, xlab='x', ylab='y', title='title'):
    '''
    x axis is for agent rank
    y axis is for time_span/number of querys for every agent
    y: a list of timedelta if for time_span
    y: a list of int if for querys 
    fig_size: tuple, the fig_size of figure
    name: str, the name of saving file
    thresh: int, to see data more clearly, data less than thresh using one mark_every, and data more than thresh using another
    mark_every1/2: int, before/after thresh, using different frequency's mark
    '''

    fig = plt.figure()
    ax = plt.subplot(111)

    # Plot
    ax.plot(x_, y_, alpha=0.5)
    plt.title(title, fontsize=12)
    plt.xlabel(xlab, fontsize=10)
    plt.ylabel(ylab,fontsize=12)
    # ax.set_xscale('log')

    for x,y in zip(x_, y_):
        
        if x%mark_every1 != 0 and x <= thresh:
            continue
        elif x > thresh and x%mark_every2 != 0:
            if x != len(x_)-1 and x != len(x_)-10 and x!=len(x_)-15: # the last two has to mark
                continue
        
        label = "{:.2f}".format(y)
        """
        temp = timedelta(seconds=y)
        if temp.days != 0:
            label = str(temp.days) + 'd,' + str(temp.seconds/3600) + 'h'
        else:
            label = str(temp.seconds/3600) + 'h'
        """

        plt.annotate(label, # this is the text
                     (x,y), # this is the point to label
                     textcoords="offset points", # how to position the text
                     xytext=(0,5), # distance from text to points (x,y)
                     ha='center') # horizontal alignment can be left, right or center
        
    #plt.show()
    fig.savefig(name)


def plot_bar(x, amount, aim, data_source, name, fig_size=(6.4,4.8), title='distribution of session length',
             xlabel='number of queries for one session', log_=True):
    '''
    x: list of str, x axis, such as 
    x_ = ['<=1day', '1~3days', '3~7days', '1~2weeks', '2~3weeks', 
      '3weeks~1month', '>1month']
    amount, list of int, how many agent is in this []
    aim, int, the same meaning as last one
    data_source, the same as last one
    '''
    x_ = x
    fig = plt.figure(figsize=fig_size)
    ax = plt.subplot(111)
    if data_source != None:
        if not aim:
            title = 'time span for every agent (' + data_source + ')'  
            xlabel = 'time_span' 
        else:
            title = 'number of queries for every agent '+ '(' + data_source + ')'
            xlabel = 'number of querys'
    else:
        title = title
        xlabel = xlabel

    ax.bar(x_, amount)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel('number of session')

    if log_:
        ax.set_yscale('log')

    for x,y in zip(x_,amount):

        label = "{:.2f}".format(y)

        plt.annotate(label, # this is the text
                     (x,y), # this is the point to label
                     textcoords="offset points", # how to position the text
                     xytext=(0,5), # distance from text to points (x,y)
                     ha='center') # horizontal alignment can be left, right or center

    #plt.show()
    fig.savefig(name, dpi=200)

def plot_combined(x, sum_every_column, combinedSort, timelabel, name, fig_size=(6.4,4.8),
                 title='session length distribution', xlab='the number of query for every session', 
                 ylab='number of agents'):
    '''
    combinedSort, a list of list. 
    sum_every_column, a list with len of x_asix, to mark
    '''
    # delta_ records threshold for time span
    # x_query for x axis, x_time for time label, such as x = [1, less than 10, 10~20 ...]
    # thre records threshold for query amount
    colors = ['mediumturquoise', 'plum', 'lightgreen', 'mediumpurple', 'lightpink', 
             'aquamarine', 'cornflowerblue', 'greenyellow', 'sandybrown']
    x_ = x
    fig = plt.figure(figsize=fig_size)
    ax = plt.subplot(111) 
    temp = list(np.zeros(len(x_)))
    for i in range(len(combinedSort)):
        if i == 0:
            plt.bar(x_, combinedSort[i], bottom=temp, label=timelabel[i], color=colors[i])
        else:
            temp = list(np.asarray(combinedSort[i-1])+np.asarray(temp))
            plt.bar(x_, combinedSort[i], bottom=temp, label=timelabel[i], color=colors[i])
                
    # plt.title(title, fontsize=20)
    plt.xlabel(xlab, fontsize=25)
    plt.ylabel(ylab, fontsize=25)
    plt.xticks(fontsize=22, rotation=20)
    plt.yticks(fontsize=22)
    plt.yscale('log')
    ax.set_yscale('log')
    ax.yaxis.grid(True, linestyle='--')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()

    for x,y in zip(x, sum_every_column):

        label = "{:}".format(y)

        plt.annotate(label, # this is the text
                     (x,y), # this is the point to label
                     textcoords="offset points", # how to position the text
                     xytext=(0,1.5), # distance from text to points (x,y)
                     ha='center', # horizontal alignment can be left, right or center
                     fontsize=20)
    plt.legend(fontsize=20, loc=3)
    #plt.show()
    fig.savefig(name, bbox_inches = 'tight')


def plot_combined_apweb(x, sum_every_column, combinedSort, timelabel, name, fig_size=(6.4,4.8),
                 title='session length distribution', xlab='the number of query for every session', 
                 ylab='number of agents'):
    '''
    -------------------update 20200501----------------------
    replot for APWeb camera ready 
    the same as plot_combined, just change parameters
    ------------------------end-----------------------------
    combinedSort, a list of list. 
    sum_every_column, a list with len of x_asix, to mark
    '''
    # delta_ records threshold for time span
    # x_query for x axis, x_time for time label, such as x = [1, less than 10, 10~20 ...]
    # thre records threshold for query amount
    colors = ['mediumturquoise', 'plum', 'lightgreen', 'mediumpurple', 'lightpink', 
             'aquamarine', 'cornflowerblue', 'greenyellow', 'sandybrown']
    x_ = x
    fig = plt.figure(figsize=fig_size)
    ax = plt.subplot(111) 
    temp = list(np.zeros(len(x_)))
    for i in range(len(combinedSort)):
        if i == 0:
            plt.bar(x_, combinedSort[i], bottom=temp, label=timelabel[i], color=colors[i])
        else:
            temp = list(np.asarray(combinedSort[i-1])+np.asarray(temp))
            plt.bar(x_, combinedSort[i], bottom=temp, label=timelabel[i], color=colors[i])
                
    #plt.title(title, fontsize=20)
    plt.xlabel(xlab, fontsize=25)
    plt.ylabel(ylab, fontsize=25)
    plt.xticks(fontsize=22, rotation=25)
    plt.yticks(fontsize=22)
    plt.yscale('log')
    ax.set_yscale('log')
    ax.yaxis.grid(True, linestyle='--')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()

    for x,y in zip(x, sum_every_column):

        label = "{:}".format(y)

        plt.annotate(label, # this is the text
                     (x,y), # this is the point to label
                     textcoords="offset points", # how to position the text
                     xytext=(0,2), # distance from text to points (x,y)
                     ha='center', # horizontal alignment can be left, right or center
                     fontsize=18)
    plt.legend(fontsize=15, loc=3)
    #plt.show()
    fig.savefig(name, bbox_inches = 'tight')

def plotDisInOneDay(name, filter_users=False, dir='disInOneDay'):
    res_mach, res_human = getTimeDisWithFreq(name, filter_users=filter_users)
    x = [x for x in range(24)]
    fig = plt.figure(figsize=(12.8,4.8))
    # human
    ax = plt.subplot(122)
    plt.xlabel('time', fontsize=18)
    plt.ylabel('number of queries', fontsize=14)
    plt.xticks(fontsize=8)
    plt.yticks(fontsize=11)
    plt.title(f'Organic queries distribution', fontsize=15)
    plt.bar(x, res_human)

    # machine
    ax = plt.subplot(121)
    plt.xlabel('time', fontsize=18)
    plt.ylabel('number of queries', fontsize=14)
    plt.xticks(fontsize=13)
    plt.yticks(fontsize=11)
    plt.title(f'Robotic queries distribution', fontsize=15)
    plt.bar(x, res_mach)   

    fig.savefig(f'figs/{dir}/{name}_dis.pdf')


def heatmap(data, row_labels, col_labels, ax=None,
            cbar_kw={}, cbarlabel="", **kwargs):
    """
    Create a heatmap from a numpy array and two lists of labels.

    Parameters
    ----------
    data
        A 2D numpy array of shape (N, M).
    row_labels
        A list or array of length N with the labels for the rows.
    col_labels
        A list or array of length M with the labels for the columns.
    ax
        A `matplotlib.axes.Axes` instance to which the heatmap is plotted.  If
        not provided, use current axes or create a new one.  Optional.
    cbar_kw
        A dictionary with arguments to `matplotlib.Figure.colorbar`.  Optional.
    cbarlabel
        The label for the colorbar.  Optional.
    **kwargs
        All other arguments are forwarded to `imshow`.
    """

    if not ax:
        ax = plt.gca()

    # Plot the heatmap
    im = ax.imshow(data, **kwargs)

    # Create colorbar
    cbar = ax.figure.colorbar(im, ax=ax, **cbar_kw)
    cbar.ax.set_ylabel(cbarlabel, rotation=-90, va="bottom", fontsize=35)
    cbar.ax.tick_params(labelsize=30) 

    # We want to show all ticks...
    ax.set_xticks(np.arange(data.shape[1]))
    ax.set_yticks(np.arange(data.shape[0]))
    # ... and label them with the respective list entries.
    ax.set_xticklabels(col_labels)
    ax.set_yticklabels(row_labels)

    # Let the horizontal axes labeling appear on top.
    ax.tick_params(top=True, bottom=False,
                   labeltop=True, labelbottom=False)

    # Rotate the tick labels and set their alignment.
    plt.setp(ax.get_xticklabels(), rotation=-30, ha="right",
             rotation_mode="anchor")

    # Turn spines off and create white grid.
    for edge, spine in ax.spines.items():
        spine.set_visible(False)

    ax.set_xticks(np.arange(data.shape[1]+1)-.5, minor=True)
    ax.set_yticks(np.arange(data.shape[0]+1)-.5, minor=True)
    ax.grid(which="minor", color="w", linestyle='-', linewidth=3)
    ax.tick_params(which="minor", bottom=False, left=False)

    return im, cbar


def annotate_heatmap(im, data=None, valfmt="{x:.2f}",
                     textcolors=("black", "white"),
                     threshold=None, **textkw):
    """
    A function to annotate a heatmap.

    Parameters
    ----------
    im
        The AxesImage to be labeled.
    data
        Data used to annotate.  If None, the image's data is used.  Optional.
    valfmt
        The format of the annotations inside the heatmap.  This should either
        use the string format method, e.g. "$ {x:.2f}", or be a
        `matplotlib.ticker.Formatter`.  Optional.
    textcolors
        A pair of colors.  The first is used for values below a threshold,
        the second for those above.  Optional.
    threshold
        Value in data units according to which the colors from textcolors are
        applied.  If None (the default) uses the middle of the colormap as
        separation.  Optional.
    **kwargs
        All other arguments are forwarded to each call to `text` used to create
        the text labels.
    """

    if not isinstance(data, (list, np.ndarray)):
        data = im.get_array()

    # Normalize the threshold to the images color range.
    if threshold is not None:
        threshold = im.norm(threshold)
    else:
        threshold = im.norm(data.max())/2.

    # Set default alignment to center, but allow it to be
    # overwritten by textkw.
    kw = dict(horizontalalignment="center",
              verticalalignment="center")
    kw.update(textkw)

    # Get the formatter in case a string is supplied
    if isinstance(valfmt, str):
        valfmt = matplotlib.ticker.StrMethodFormatter(valfmt)

    # Loop over the data and create a `Text` for each "pixel".
    # Change the text's color depending on the data.
    texts = []
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            kw.update(color=textcolors[int(im.norm(data[i, j]) > threshold)])
            text = im.axes.text(j, i, valfmt(data[i, j], None), **kw)
            texts.append(text)

    return texts


def draw_confusionMatrix(mat, name, y_label, cmap="YlGn", annotate=False, figsize=(6.4, 4.8)):
    fig = plt.figure(figsize=figsize)
    ax = plt.subplot(111)

    x = [i+1 for i in range(mat.shape[0])]
    y = [i+1 for i in range(mat.shape[1])]
    im, cbar = heatmap(mat, x, y, ax=ax,
                       cmap=cmap, cbarlabel=y_label)
    if annotate:
        texts = annotate_heatmap(im, valfmt="{x:.2f}", fontsize=30)

    fig.tight_layout()
    plt.show()
    fig.savefig(name)


def PlotBox(all_data, title, name, ylabel, figsize=(6.4, 4.8)):
    # Random test data
    # np.random.seed(19680801)
    # all_data = [np.random.normal(0, std, size=100) for std in range(1, 4)]
    # labels = ['x1', 'x2', 'x3']
    labels = [x+1 for x in range(len(all_data))]

    fig = plt.figure(figsize=figsize)
    ax = plt.subplot(111)

    # rectangular box plot
    bplot = ax.boxplot(all_data,
                        vert=True,  # vertical box alignment
                        patch_artist=True,  # fill with color
                        labels=labels)  # will be used to label x-ticks
    ax.set_title(title)

    # notch shape box plot
    """
    bplot2 = ax2.boxplot(all_data,
                        notch=True,  # notch shape
                        vert=True,  # vertical box alignment
                        patch_artist=True,  # fill with color
                        labels=labels)  # will be used to label x-ticks
    ax2.set_title('Notched box plot')
    """

    # fill with colors
    colors = ['lightblue' for x in range(len(all_data))]
    for patch, color in zip(bplot['boxes'], colors):
        patch.set_facecolor(color)

    # adding horizontal grid lines
    ax.yaxis.grid(True, linestyle='--')
    ax.set_xlabel('Step')
    ax.set_ylabel(ylabel)

    plt.show()
    fig.savefig(name)