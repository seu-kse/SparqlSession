

def structure_analysis(triple, changes, res=None, change_count=None):
    """
    analyse node in changes located in which structure in triple.

    triple: dict triple info, {operator, [triple list]}
    changes: dict triple comp in comp_info,{operator, {
        new, bool
        old, bool
        add, triple list
        delete, triple list
        change, change list [{add, detele, change -> change list: [{change->spo, ori, now}, {} ...]}, {} ...]
    }}
    return -> dict
        [star_center, star_neigh_node, star_neigh_edge]
        [sink_center, sink_neigh_node, sink_neigh_edge]
        [path_node, path_edge]
        [hybrid_center, hybrid_in_node, hybrid_out_node, hybrid_in_edge, hybrid_out_edge]
    """
    def init():
        star = [0,0,0]
        sink = [0,0,0]
        path = [0,0]
        hybrid = [0,0,0,0,0]
        res = {'star':star, 'sink':sink, 'hybrid':hybrid, 'path':path}
        return res

    if res == None:
        res = init()
    if change_count == None:
        change_count = 0
    nodes_info = get_structure_info(triple)
    for op, changei in changes.items():
        if 'change' in changei.keys():
            ci = changei['change']
            # list
            for i in ci:
                # dict
                if 'change' in i.keys():
                    # dict
                    kk = i['change']
                    if 'change' in kk.keys():
                        # list
                        jj = kk['change']
                        for node_change in jj:
                            change_count += 1
                            ori = node_change['ori']
                            if op in nodes_info.keys():
                                res = proStrucCount(nodes_info[op], ori, res)
    return res, change_count



def proStrucCount(nodes, ori, res):
    """
    nodes: {node -> {
                type: star, path, hybrid, sink
                out_node: list of out node
                in_node: list of in node
                out_edge: list of out p
                in_edge: list of in p    
                }
            }
    res, dict
        [star_center, star_neigh_node, star_neigh_edge]
        [sink_center, sink_neigh_node, sink_neigh_edge]
        [path_node, path_edge]
        [hybrid_center, hybrid_in_node, hybrid_out_node, hybrid_in_edge, hybrid_out_edge]
    """
    for node, struc_info in nodes.items():
        if struc_info['type'] == 'no':
            continue
        type_ = struc_info['type']
        if node == ori:
            res[type_][0] += 1
        elif type_ == 'star' or type_ == 'sink':
            if ori in struc_info['in_node'] or ori in struc_info['out_node']:
                res[type_][1] += 1
            elif ori in struc_info['in_edge'] or ori in struc_info['out_edge']:
                res[type_][2] += 1
        elif type_ == 'hybrid':
            if ori in struc_info['in_node']:
                res[type_][1] += 1
            elif ori in struc_info['out_node']:
                res[type_][2] += 1
            elif ori in struc_info['in_edge']:
                res[type_][3] += 1
            elif ori in struc_info['out_edge']:
                res[type_][4] += 1
        elif type_ == 'path':
            if ori in struc_info['in_edge'] or ori in struc_info['out_edge']:
                res[type_][1] += 1
    return res


def get_structure_info(triple):
    """
    triple: dict triple info, {operator, [triple list]}

    return:
        triple_struc -> {
            operator {
                node {
                    type: star, path, hybrid, sink
                    out_node: list of out node
                    in_node: list of in node
                    out_edge: list of out p
                    in_edge: list of in p
                }
            }
        } 
    """

    res = {}
    for op, block in triple.items():
        if op not in res.keys():
            res[op] = {}
        nodes = {} 
        for triplei in block:
            for idx, nodei in enumerate(triplei):
                # if idx == 1: continue
                if nodei not in nodes.keys():
                    nodes[nodei] = {'spo_count': [0,0,0], 'out_node': [], 'in_node': [],
                                    'out_edge': [], 'in_edge': [], 'join_degree': 0}
                nodes[nodei]['spo_count'][idx] += 1
                if idx == 0:
                    node_dir = 'out_node'
                    edge_dir = 'out_edge'
                elif idx == 2:
                    node_dir = 'in_node'
                    edge_dir = 'in_edge'
                elif idx == 1:
                    node_dir = 'in_node'
                other_idx = [x for x in list(range(3)) if x != idx]
                if idx != 1:
                    for i in other_idx:
                        if i == 0 or i == 2 and triplei[i] not in nodes[nodei][node_dir]:
                            nodes[nodei][node_dir].append(triplei[i])
                        else:
                            if triplei[i] not in nodes[nodei][edge_dir]:
                                nodes[nodei][edge_dir].append(triplei[i])
                else:
                    if triplei[0] not in nodes[nodei][node_dir]:
                        nodes[nodei][node_dir].append(triplei[0])
                nodes[nodei]['type'] = jugde_struc_type(nodes[nodei], idx)
        res[op] = nodes
    return res
                    

def jugde_struc_type(node, idx):

    spo_count = node['spo_count']

    join = 'no'
    if idx == 0 or idx == 2:
        if spo_count[0] + spo_count[2] <= 1:
            join = 'no'
        elif spo_count[2] == 0:
            if spo_count[0] >= 2:
                join = 'star'
        elif spo_count[0] == 0:
            if spo_count[2] >= 2:
                join = 'sink'
        elif spo_count[0] == spo_count[2] and spo_count[0] == 1:
                join = 'path'
        elif (spo_count[0] >= 1 and spo_count[2] > 1) or (spo_count[2] >= 1 and spo_count[0] > 1):
            join = 'hybrid'
    else:
        if len(node['in_node']) >= 2:
            join = 'sink'
            node['join_degree'] = len(node['in_node'])
    
    if (idx==0 or idx==2) and join!='no':
        node['join_degree'] = spo_count[0] + spo_count[2]
    return join


