from pyspark import SparkContext
from pyspark import SparkConf
import math
from itertools import combinations
import sys
import time
import csv
import random
from functools import reduce
from pyspark import *
import queue


start_time = time.time()

def count_shorest_path(tree_edges):
    result_dict = dict()
    if len(tree_edges)>0:
        l0_edges = tree_edges[0]
        result_dict[l0_edges[0][0]]=1
        for ii in range(0,len(tree_edges)):
            current_level = tree_edges[ii]
            for each_edge in current_level:
                up_node_value = result_dict.get(each_edge[0])
                current_value = result_dict.get(each_edge[1],0)
                result_dict[each_edge[1]]=current_value+up_node_value
    return result_dict



def update_sum(s_dict,tree_edges):
    sp_count_dict = count_shorest_path(tree_edges)

    # reverse list
    node_value_dict = dict()
    all_edges = tree_edges[::-1]
    for i in range(0,len(all_edges)):
        each_level_edges = all_edges[i]

        for each_edge in each_level_edges:
            up_node = each_edge[0]
            bottom_node = each_edge[1]
            # update node value
            bottom_node_value = node_value_dict.get(bottom_node,1)
            up_node_value = node_value_dict.get(up_node,1)
            new_up_node_value = up_node_value+float(bottom_node_value*sp_count_dict.get(up_node)/sp_count_dict.get(bottom_node))
            node_value_dict[up_node] = new_up_node_value
            # update edge value
            sorted_edge = sorted(each_edge)
            previous_edge_value = (s_dict.get(sorted_edge[0])).get(sorted_edge[1])
            new_edge_value = previous_edge_value+float(bottom_node_value*sp_count_dict.get(up_node)/sp_count_dict.get(bottom_node))
            s_dict[sorted_edge[0]][sorted_edge[1]]=new_edge_value

    return s_dict


def get_betweness(node_list,edge_dict_direct,edge_dict_undirect):
    # initialize sum_dict
    sum_dict = dict()
    for (k,l) in edge_dict_direct.items():
        sum_dict[k] = { i:0 for i in l }

    for k in range(0,len(node_list)):
        current_root = node_list[k]
        discovered_nodes = [current_root]
        current_level_nodes = [current_root]
        discovered_edges = []
        next_level_nodes = [current_root]

        while len(next_level_nodes)>0:
            level_edges = []
            next_level_nodes = []
            for each_node in current_level_nodes:
                potential_nodes = edge_dict_undirect.get(each_node)
                for each_potential_node in potential_nodes:
                    if each_potential_node not in discovered_nodes and each_potential_node not in current_level_nodes:
                        level_edges.append((each_node,each_potential_node))
                        next_level_nodes.append(each_potential_node)

            if len(level_edges)>0:
                discovered_edges.append(level_edges)
            next_level_nodes = list(set(next_level_nodes))
            discovered_nodes = discovered_nodes+next_level_nodes
            current_level_nodes = next_level_nodes

        sum_dict = update_sum(sum_dict,discovered_edges)

    # format output
    betweenness_list = []
    for (node1,list_of_node2) in sum_dict.items():
        for (node2,b_value) in list_of_node2.items():
            betweenness_list.append((tuple(sorted((node1,node2))),)+(float(b_value/2),))
    s_betweenness_list = sorted(betweenness_list, key=lambda tup: (-tup[1],tup[0][0],tup[0][1]) )

    return s_betweenness_list



def get_groups(undirect_dict,node_l):
    visited_node = []
    q = queue.Queue()
    result_goups = []
    for each_n in node_l:
        if each_n not in visited_node:
            q.put(each_n)
            single_group = [each_n]
            visited_node.append(each_n)
            while not q.empty():
                current_node = q.get()
                potential_nodes = undirect_dict.get(current_node)
                for each_p_node in potential_nodes:
                    if each_p_node not in visited_node:
                        q.put(each_p_node)
                        single_group.append(each_p_node)
                        visited_node.append(each_p_node)
            result_goups.append(single_group)
    return result_goups


def get_Q(ss,mm,aa,kk):
    sum = 0
    for each_group in ss:
        for index_i in range(0,len(each_group)-1):
            node_i = each_group[index_i]
            for index_j in range(index_i+1,len(each_group)):
                node_j = each_group[index_j]
                a_value = 0
                temp_edges = aa.get(node_i)
                if node_j in temp_edges:
                    a_value = 1
                exp_value = kk.get(node_i)*kk.get(node_j)/(2*mm)
                sum = sum+(a_value-exp_value)
    return float(sum/(2*mm))


def remove_edge(undirect_d,direct_d, b_list):
    highest_Q = b_list[0][1]
    b_list_copy = b_list.copy()
    undirect_d_copy = undirect_d.copy()
    direct_d_copy = direct_d.copy()
    while len(b_list_copy)>0 and b_list_copy[0][1]==highest_Q:
        edge = b_list_copy[0][0]
        node1 = edge[0]
        node2 = edge[1]
        # set node1
        edges1 = undirect_d_copy.get(node1)[:]
        edges1.remove(node2)
        undirect_d_copy[node1] = edges1
        # set node2
        edges2 = undirect_d_copy.get(node2)[:]
        edges2.remove(node1)
        undirect_d_copy[node2] = edges2
        # set direct_d_copy
        sorted_tuple = sorted(edge)
        s_node1 = sorted_tuple[0]
        s_node2 = sorted_tuple[1]
        edges3 = direct_d_copy.get(s_node1)[:]
        edges3.remove(s_node2)
        direct_d_copy[s_node1] = edges3
        # set b_list_copy
        b_list_copy = b_list_copy[1:]
    return (undirect_d_copy,direct_d_copy,b_list_copy)



# --------------- prepare data --------------------

filter_threshold = int(sys.argv[1])
input_file = sys.argv[2]
betweenness_output_file = sys.argv[3]
community_output_file = sys.argv[4]

# produce input matrix
conf = SparkConf().setMaster("local[*]").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf)
data = sc.textFile(input_file)
header = data.first()
raw_data_ukey = data.filter(lambda x: x != header).map(lambda x: x.split(',')).map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).collectAsMap()
#raw_data_ukey = data.filter(lambda x: x != header).map(lambda x: x.split(',')).map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).filter(lambda x: len(x[1])>=filter_threshold).collectAsMap()

all_users = list(raw_data_ukey.keys())
edge_list_undirect = []
edge_list_direct = []
raw_node_list = []
for i in range(0,len(all_users)-1):
    user1 = all_users[i]
    for j in range(i+1,len(all_users)):
        user2 = all_users[j]
        set1 = set(raw_data_ukey.get(user1))
        set2 = set(raw_data_ukey.get(user2))
        if len(set1.intersection(set2))>=filter_threshold:
            edge_list_direct.append(tuple(sorted((user1,user2))))
            edge_list_undirect.append((user1,user2))
            edge_list_undirect.append((user2,user1))
            raw_node_list.append(user1)
            raw_node_list.append(user2)

n_list = sorted(list(set(raw_node_list)))
e_dict_undirect = sc.parallelize(edge_list_undirect).map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).collectAsMap()
e_dict_direct = sc.parallelize(edge_list_direct).map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).collectAsMap()

# ---------------2.1 betweenness calculation--------------------
sorted_betweenness_list = get_betweness(n_list,e_dict_direct,e_dict_undirect)

# print output
f = open(betweenness_output_file, 'w')
for i in sorted_betweenness_list:
    format_i = (i[0],round(i[1],5))
    f.write(str(format_i)[1:-1])
    f.write('\n')
f.close()


# ---------------2.2 betweenness calculation--------------------
# start_time = time.time()
e_dict_direct_copy = e_dict_direct.copy()
e_dict_undirect_copy = e_dict_undirect.copy()
b_list_copy = sorted_betweenness_list.copy()

m = len(sorted_betweenness_list)
A = e_dict_undirect
K = dict()
for (key,value) in e_dict_undirect.items():
    K[key] = len(value)

group_list = []
Q_list = []


while len(b_list_copy)>0:
    S = get_groups(e_dict_undirect_copy,n_list)
    Q = get_Q(S,m,A,K)
    group_list.append(S)
    Q_list.append(Q)
    (e_dict_undirect_copy,e_dict_direct_copy,b_list_copy) = remove_edge(e_dict_undirect_copy,e_dict_direct_copy,b_list_copy)
    b_list_copy = get_betweness(n_list,e_dict_direct_copy,e_dict_undirect_copy)



S = get_groups(e_dict_undirect_copy,n_list)
Q = get_Q(S,m,A,K)
group_list.append(S)
Q_list.append(Q)

max_index = Q_list.index(max(Q_list))
raw_answer = group_list[max_index]
temp_answer = []
for each_s in raw_answer:
    temp_answer.append(sorted(each_s))
final_answer = sorted(temp_answer, key=lambda x: (len(x),x[0]) )


# print output
f = open(community_output_file, 'w')
for i in final_answer:
    f.write(str(i)[1:-1])
    f.write('\n')
f.close()


# print time
print('Duration: ' + str(time.time() - start_time))