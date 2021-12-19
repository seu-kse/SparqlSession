import re
from fuzzywuzzy import fuzz
from tqdm import tqdm

def GetPattern():
    """
    return a pattern dict
    """
    pattern = {}
    temp_re = '[A-Za-z0-9_?:><=".# /|(\')(\(),(\));!(\^)(\-)+!@#$%&*(\[)(\])]'
    # temp_sub = '[A-Za-z0-9_?:><=".# /|(\')(\(),{}(\));!(\^)(\-)+!@#$%&*(\[)(\])]'
    pattern['filter'] = r'FILTER\s*\w*\s*\(('+temp_re+')+\)'
    pattern['filter_not_exists'] = r'FILTER\s+NOT\s+EXISTS\s*\{('+temp_re+')+\}' # can not have {} in set
    pattern['filter_exists'] = r'FILTER\s+EXISTS\s*\{('+temp_re+')+\}'
    pattern['values'] = r'VALUES\s*('+temp_re+')+\s*\{('+temp_re+')+\}'
    pattern['graph'] = r'GRAPH\s*('+temp_re+')\s*\{('+temp_re+')+\}'
    pattern['bind'] = r'BIND\s*\('+temp_re+'+\)'
    pattern['optional'] = r'OPTIONAL\s*\{'+temp_re+'+\}'
    pattern['union'] = r'UNION\s*\{('+temp_re+')+\}'
    # pattern['subquery'] = r'WHERE\s?\{'+temp_sub+'+\}' # the number of {} may be wrong
                                                        # get limit, group by, having first.
    pattern['limit'] = r'LIMIT\s*\d+'
    # no group by, having, minus
    pattern['var'] = r'\?\w+'
    # be careful about relation or entity like 'a', without "<>", if remove all space, can make mistakes when getiing vars
    pattern['entity'] = r'\<\S+\>'
    pattern['service'] = r'SERVICE\s*\<\S+\>'
    pattern['spo'] = r'('+pattern['entity']+'|'+pattern['var']+'|\w*)'
    pattern['triple'] = r'(('+pattern['spo']+')\s+('+pattern['spo']+')\s+('+pattern['spo']+'))'
    return pattern

def GetPart(keyword, left, right, query):
    """
    match the full part whose format is "keyword  \left \right", such as "OPTIONAL { this is the {test} }" 

    input:
        keyword: str, the keyword that part we want to match begin with.
        left: str, shoule be a '(' , '{' or '['
        right, str, should be a ')', '}' or ']'
        query, str, in which str we want to find substring with "kerword \left  \right" 

    output:
        st: list full of int
        end: list full of int
        the number in st and end with the same index are a pair represent the substring we want fo find's start and end index.

    Notice:  string[st[i], end[i]] represent exactly the substring i we want to find.
    """
    keyword = str(keyword)
    query = str(query)
    st = []
    end = []
    a = 0
    while (query.find(keyword, a)) != -1:
        left_count = 0
        right_count = 0
        keyword_s = query.find(keyword, a)
        left_s = query.find(left, keyword_s)
        if left_s == -1:
            break
        for i, s in enumerate(query[left_s:]):
            if s == left:
                left_count += 1
            elif s == right:
                right_count += 1
            if left_count == right_count:
                #from ipdb import set_trace; set_trace()
                break
        st.append(keyword_s)
        end.append(i+1+left_s)
        #print(keyword_s)
        #print(i+1+left_s)
        a = i+left_s+1
    return st, end
    
def GetAndRemovePart(keyword, left, right, query):
    """
    Using the result from the GetPart function, get the substring and the string without substring.

    input:
        the same as GetPart function.
    
    output: 
        res: list with str, the substing we want to get.
        remove: str, query string without substring.
    """
    st, end = GetPart(keyword, left, right, query)
    res = []
    remove = query
    for i in range(len(st)): 
        temp = query[st[i]:end[i]]
        res.append(temp)
        remove = remove.replace(temp, '')
        #from ipdb import set_trace; set_trace()
    return res, remove

def MatchLeftRight(left, right, query):
    """
    find the "{}", "()" or "[]" position in a str 

    input:
        the same as GetPart function
    
    output:
        st, end: the same as GetPart function.
        the difference is the same position in st and end do not represent a pair
    """
    st = []
    end = []
    a = 0
    while (query.find(left, a)) != -1:
        left_count = 0
        right_count = 0
        left_s = query.find(left, a)
        for i, s in enumerate(query[left_s:]):
            if s == left:
                left_count += 1
                st.append(i+1+left_s)
            elif s == right:
                right_count += 1
                end.append(i+1+left_s)
            if left_count == right_count:
                break
        a = i+1+left_s
    return st, end

def GetPair(left, right, query):
    """
    using the result from MatchLeftRight function, generate the pair for every "{}"(or others) start and end.
    We sort the st and end list first, and handle st from the back. for each int in st, it should be pair with the "right" whose position 
    is after this "left" and also be nearest one. 

    input:
        the same as GetPart function

    output:
        pair, list of list. for each list in list, is a list with a len of 2, means [start_index, end_index]
    """
    st, end = MatchLeftRight(left, right, query)
    pair = []
    st.sort()
    end.sort()
    for i in range(len(st)):
        xi = st[len(st)-1-i]
        for yi in end:
            if yi >= xi:
                break
        end.remove(yi)
        pair.append([xi,yi])
    return pair

def Process_pair(pair, query):
    """
    using the result from MatchLeftRight function and GetPair function, get the content within "left" and "right".
    We match the content from inside to outside. 
    For example, query "{test1 {test2}} {test3}", left "{", right "}" should return ["test3", "test2", "test1"]

    input:
        pair: the result from GetPair
        query: the str we want to get content.

    output:
        process_list, list of str. for each str in this list, means the content with in "{}" (or others)
    """
    process_list = []
    # pair has order
    for i in pair:
        xi = i[0]
        yi = i[1]
        s = query[xi:yi]
        s = re.sub('\n', '', s)       
        s = re.sub('{', '', s)
        s = re.sub('}', '', s)
        s = re.sub('\s+', ' ', s)
        s = re.sub(r'^\s*', '', s)
        query = query[:xi] + query[yi:]
        if s != '':
            process_list.append(s)
    return process_list

def GetProcessStr(left, right, query):
    """
    a function find the content within "{}"(or others) from left, right and query

    input:
        the same as GetPair
    
    output:
        process_list: the same as Process_pair
    """
    pair = GetPair(left, right, query)
    process_list = Process_pair(pair, query)
    return process_list

def Clean(s):
    s = re.sub('\n', '', s)
    s = re.sub('\s+', ' ', s)
    s = re.sub(r'\s*\{\s*', '', s)
    s = re.sub(r'\s*\}\s*', '', s)
    s = re.sub(r'^\s*', '', s)
    s = re.sub(r'\s*$', '', s)
    return s

def add_dict(des, source):
    """
    add a dict to another dict.

    input:
        des, destination dict.
        source, source dict.
    
    return:
        des, destination dict adding source dict
    """
    for k in source.keys():
        if k not in des.keys():
            des[k] = []
        for xi in source[k]:
            des[k].append(xi)
    return des


def add_list(des, source, unique, op=None):
    """
    add a list to another list.

    input:
        des, destination list.
        source, source list.
        unique, boolean, if True, return the unique list.
        op, str, when adding triples, add the marker before p. Such as "OPT<sep>?p"
    
    output:
        des, destination list adding source list.
    """
    for xi in source:
        if op != None:
            xi[1] = op + "<sep>" + xi[1]
        des.append(xi)
    if unique:
        des = list(set(des))
    return des

def clean(a):
    a = re.sub('\n', ' ', a)
    a = re.sub('\s+', ' ', a)
    a = re.sub('^\s+', '', a)
    a = re.sub('\s+$', '', a)
    return a

def match(a, b, cleaned=True):
    if not cleaned:
        a = clean(a)
        b = clean(b)
    return fuzz.ratio(a, b)