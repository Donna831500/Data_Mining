from pyspark import SparkContext
from pyspark import SparkConf
import math
from itertools import combinations
import sys
import csv
import random
from functools import reduce
from pyspark import *
import numpy as np
from sklearn.cluster import KMeans


def split_list(d,n):
    l = list(d.keys())
    random.shuffle(l)
    size_list = []
    result = []
    remainder = len(l)%n
    q = math.floor(len(l)/n)
    for i in range(0,remainder):
        size_list.append(q+1)
    for i in range(0,n-remainder):
        size_list.append(q)
    current_position = 0
    for size in size_list:
        temp = l[current_position:current_position+size]
        partial_result = dict()
        for each_id in temp:
            partial_result[each_id]=d.get(each_id)
        result.append(partial_result)
        current_position = current_position+size
    return result


def get_DS(ks,vs,ls,d):
    dict_size = max(ls)+1
    result_dict = dict()
    for i in range(0,dict_size):
        result_dict[i]=([],[0]*d,[0]*d)
    for i in range(0,len(ls)):
        current_label = ls[i]
        current_tup = result_dict.get(current_label)
        current_dimension = vs[i]
        Current_ids = current_tup[0][:]
        Current_ids.append(ks[i])
        Current_SUM = current_tup[1][:]
        Current_SUMSQ = current_tup[2][:]
        for j in range(0,d):
            Current_SUM[j] = Current_SUM[j]+current_dimension[j]
            Current_SUMSQ[j] = Current_SUMSQ[j]+(current_dimension[j]*current_dimension[j])
        result_dict[current_label] = (Current_ids,Current_SUM,Current_SUMSQ)
    return result_dict


def get_intermediate(cs,rs,ds):
    num_of_discard = 0
    for (k,v) in ds.items():
        num_of_discard = num_of_discard+len(v[0])
    num_of_compression = 0
    for (k,v) in cs.items():
        num_of_compression = num_of_compression+len(v[0])
    return [num_of_discard,len(cs),num_of_compression,len(rs)]


def get_centroid_std(ds,d):
    result_dict = dict()
    for (k,v) in ds.items():
        c_list = []
        std_list = []
        current_N = len(v[0])
        current_SUM = v[1]
        current_SUMSQ = v[2]
        for j in range(0,d):
            centroid = current_SUM[j]/current_N
            c_list.append(centroid)
            std = math.sqrt((current_SUMSQ[j]/current_N)-(centroid**2))
            std_list.append(std)
        result_dict[k]=(c_list,std_list)
    return result_dict


def get_min_hdist_with_label(c_std_data,point,d):
    result_dict = dict()
    for (k,v) in c_std_data.items():
        centroid = v[0]
        std = v[1]
        total = 0
        for j in range(0,d):
            total = total+(((point[j]-centroid[j])/std[j])**2)
        result_dict[k]=math.sqrt(total)
    return min(result_dict.items(), key=lambda x: x[1])


def update(ds,add_to_ds,d,data):
    result_ds = ds.copy()
    for (current_label,ids) in add_to_ds.items():
        current_tup = result_ds.get(current_label)
        Current_ids = current_tup[0][:]
        Current_SUM = current_tup[1][:]
        Current_SUMSQ = current_tup[2][:]
        for id in ids:
            current_dimension = data.get(id)
            Current_ids.append(id)
            for j in range(0,d):
                Current_SUM[j] = Current_SUM[j]+current_dimension[j]
                Current_SUMSQ[j] = Current_SUMSQ[j]+(current_dimension[j]*current_dimension[j])
        result_ds[current_label] = (Current_ids,Current_SUM,Current_SUMSQ)
    return result_ds


def expand_CS(cs,new_cs):
    cs_copy = cs.copy()
    start_label=0
    if len(cs)>0:
        start_label=max(list(cs.keys()))+1
    for (k,v) in new_cs.items():
        cs_copy[start_label]=v
        start_label = start_label+1
    return cs_copy


def hdist_bet_clusters(t1,t2,d):
    t1_c = t1[0]
    t1_std = t1[1]
    t2_c = t2[0]
    t2_std = t2[1]
    total1 = 0
    total2 = 0
    for i in range(0,d):
        total1 = total1+(((t1_c[i]-t2_c[i])/t2_std[i])**2)
        total2 = total2+(((t2_c[i]-t1_c[i])/t1_std[i])**2)
    dist = min(math.sqrt(total1),math.sqrt(total2))
    return dist


def merge_CS(cs,d,thresh):
    cs_copy = cs.copy()
    min_dist=0
    while min_dist<thresh and len(cs_copy)>1:
        cs_c_std = get_centroid_std(cs_copy,d)
        keys = list(cs_c_std.keys())
        pairs = list(combinations(keys,2))
        dist_list = []
        for each_pair in pairs:
            label1 = each_pair[0]
            label2 = each_pair[1]
            current_dist = hdist_bet_clusters(cs_c_std.get(label1),cs_c_std.get(label2),d)
            dist_list.append(current_dist)
        min_dist = min(dist_list)
        if min_dist<thresh:
            index_of_min = dist_list.index(min_dist)
            combine_pair = pairs[index_of_min]
            tup1 = cs_copy.pop(combine_pair[0])
            tup2 = cs_copy.pop(combine_pair[1])
            new_ids = tup1[0]+tup2[0]
            new_SUM = [sum(x) for x in zip(tup1[1], tup2[1])]
            new_SUMSQ = [sum(x) for x in zip(tup1[2], tup2[2])]
            cs_copy[combine_pair[0]]=(new_ids[:],new_SUM[:],new_SUMSQ[:])
    result_cs = dict()
    index = 0
    for (k,v) in cs_copy.items():
        result_cs[index] = v
        index = index+1
    return result_cs


def merge_CS_DS(cs,ds,thresh,d):
    cs_copy = cs.copy()
    ds_copy = ds.copy()

    cs_assign = {'aa':'bb'}
    negative_counter=0
    while(len(cs_assign)!=negative_counter and len(cs_copy)>0):
        ds_c_std = get_centroid_std(ds_copy,d)
        cs_c_std = get_centroid_std(cs_copy,d)
        cs_assign = dict()
        for (cs_k,cs_v) in cs_c_std.items():
            dist_list = []
            for (ds_k,ds_v) in ds_c_std.items():
                current_dist = hdist_bet_clusters(cs_v,ds_v,d)
                dist_list.append(current_dist)
            min_dist = min(dist_list)
            if min_dist<thresh:
                index_of_min = dist_list.index(min_dist)
                cs_assign[cs_k]=index_of_min
            else:
                cs_assign[cs_k]=-1
        negative_counter=0
        for (cs_id,ds_id) in cs_assign.items():
            if ds_id!=-1:
                tup1 = cs_copy.pop(cs_id)
                tup2 = ds_copy.get(ds_id)
                new_ids = tup1[0]+tup2[0]
                new_SUM = [sum(x) for x in zip(tup1[1], tup2[1])]
                new_SUMSQ = [sum(x) for x in zip(tup1[2], tup2[2])]
                ds_copy[ds_id]=(new_ids[:],new_SUM[:],new_SUMSQ[:])
            else:
                negative_counter=negative_counter+1

        temp_cs = dict()
        index = 0
        for (k,v) in cs_copy.items():
            temp_cs[index] = v
            index = index+1
        cs_copy = temp_cs.copy()

    return (ds_copy,cs_copy)


input_file = sys.argv[1]
num_of_cluster = int(sys.argv[2])
output_file = sys.argv[3]

conf = SparkConf().setMaster("local[*]").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf)
data = sc.textFile(input_file)
id_dict = data.map(lambda x: x.split(',')).map(lambda x: (x[0], [float(i) for i in x[2:]])).reduceByKey(lambda x, y: x + y).collectAsMap()

output_internediate_list = []
RS = dict()
CS = dict()

# step 1
split_data = split_list(id_dict,5)
current_data = split_data[0]
keys = list(current_data.keys())
values = list(current_data.values())
d = len(values[0])
# step 2
X = np.array(values)
mul = 8
kmeans = KMeans(n_clusters=mul*num_of_cluster,n_jobs=-1).fit(X)
labels = list(kmeans.labels_)
# step 3 + 4
current_data_copy = current_data.copy()
for i in range(0,mul*num_of_cluster):
    counter = labels.count(i)
    if counter==1:
        index = labels.index(i)
        RS[keys[index]] = values[index]
        current_data_copy.pop(keys[index])
remain_keys = list(current_data_copy.keys())
remain_values = list(current_data_copy.values())
X2 = np.array(remain_values)
kmeans2 = KMeans(n_clusters=num_of_cluster,n_jobs=-1).fit(X2)
labels2 = list(kmeans2.labels_)
# step 5
DS = get_DS(remain_keys,remain_values, labels2,d)
# step 6
keys = list(RS.keys())
values = list(RS.values())
X = np.array(values)
mul = 3
if mul*num_of_cluster<len(values):
    kmeans = KMeans(n_clusters=mul*num_of_cluster,n_jobs=-1).fit(X)
    labels = list(kmeans.labels_)
    CS = get_DS(keys, values, labels, d)
    #res = {k: v for k, v in d.items() if len(v) >= 2}
    rs_temp_dict = {k: v for k, v in CS.items() if len(v[0]) == 1}
    CS = {k: v for k, v in CS.items() if len(v[0]) > 1}
    RS = dict()
    for (k,v) in rs_temp_dict.items():
        RS[v[0][0]]=v[1]
output_internediate_list.append(get_intermediate(CS,RS,DS))


threshold = 2*(math.sqrt(d))
for loop_index in range(1,len(split_data)):
    # step 7
    current_data = split_data[loop_index]
    keys = list(current_data.keys())
    values = list(current_data.values())
    # step 8 + 9 + 10
    DS_c_std = get_centroid_std(DS,d)
    CS_c_std = get_centroid_std(CS,d)

    add_to_DS = dict()
    for k in list(DS.keys()):
        add_to_DS[k]=[]
    add_to_CS = dict()
    for k in list(CS.keys()):
        add_to_CS[k]=[]

    for (k,v) in current_data.items():
        (label_id,min_hdist) = get_min_hdist_with_label(DS_c_std,v,d)
        if min_hdist<threshold: # add to DS
            temp = add_to_DS.get(label_id)[:]
            temp.append(k)
            add_to_DS[label_id]=temp
        elif len(CS_c_std)>0: # add to CS if possible
            (label_id,min_hdist) = get_min_hdist_with_label(CS_c_std,v,d)
            if min_hdist<threshold: # add to CS
                temp = add_to_CS.get(label_id)[:]
                temp.append(k)
                add_to_CS[label_id]=temp
            else: # add to RS
                RS[k]=v
        else: # add to RS
            RS[k]=v

    # update DS + CS
    DS = update(DS,add_to_DS,d,current_data)
    CS = update(CS,add_to_CS,d,current_data)

    # step 11 (repeat step 6)
    #current_data_copy = current_data.copy()
    keys = list(RS.keys())
    values = list(RS.values())
    mul = 3
    if mul*num_of_cluster<len(values):
        X = np.array(values)
        kmeans = KMeans(n_clusters=mul*num_of_cluster,n_jobs=-1).fit(X)
        labels = list(kmeans.labels_)
        new_CS = get_DS(keys, values, labels, d)
        #res = {k: v for k, v in d.items() if len(v) >= 2}
        rs_temp_dict = {k: v for k, v in new_CS.items() if len(v[0]) == 1}
        new_CS = {k: v for k, v in new_CS.items() if len(v[0]) > 1}
        RS = dict()
        for (k,v) in rs_temp_dict.items():
            RS[v[0][0]]=v[1]
        CS = expand_CS(CS,new_CS)

    # step 12
    CS = merge_CS(CS,d,threshold)

    # if last term
    if loop_index==len(split_data)-1:
        (DS,CS) = merge_CS_DS(CS,DS,threshold,d)

    output_internediate_list.append(get_intermediate(CS,RS,DS))


# output
result_list = []
for (label,tup) in DS.items():
    id_list = tup[0]
    for each_id in id_list:
        result_list.append((int(each_id),label))
for (label,tup) in CS.items():
    id_list = tup[0]
    for each_id in id_list:
        result_list.append((int(each_id),-1))
for (each_id_2,tup) in RS.items():
    result_list.append((int(each_id_2),-1))
result_list = sorted(result_list, key=lambda tup: tup[0])

f = open(output_file, 'w')
f.write('The intermediate results:')
f.write('\n')
round_counter = 1
for i in output_internediate_list:
    f.write('Round '+str(round_counter)+': ')
    f.write(str(i)[1:-1])
    f.write('\n')
    round_counter = round_counter+1
f.write('\n')
f.write('The clustering results:')
f.write('\n')
for i in result_list:
    f.write(str(i)[1:-1])
    f.write('\n')
f.close()