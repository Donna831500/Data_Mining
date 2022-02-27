import math
import csv
import sys
import numpy as np
import pandas as pd
import json


import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import *

import os

def my_sort(l):
    temp1 = []
    for each in l:
        temp1.append(sorted(each))
    temp2 = sorted(temp1, key=lambda x: (len(x),x[0]) )
    return temp2


# --------------- prepare data --------------------
filter_threshold = int(sys.argv[1])
input_file = sys.argv[2]
community_output_file = sys.argv[3]

# produce input matrix
conf = SparkConf().setMaster("local[*]").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf)
data = sc.textFile(input_file)
header = data.first()
raw_data_ukey = data.filter(lambda x: x != header).map(lambda x: x.split(',')).map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).collectAsMap()

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

sc.stop()


node_list = []
for each in n_list:
    node_list.append((each,))


spark = SparkSession.builder.appName("h").config("spark.jars.packages", "graphframes:graphframes:0.6.0-spark2.3-s_2.11").getOrCreate()

from graphframes import *

vertices = spark.createDataFrame(node_list, ["id"])
edges = spark.createDataFrame(edge_list_undirect, ["src", "dst"])
g = GraphFrame(vertices, edges)
result = g.labelPropagation(maxIter=5)
temp_result = result.toPandas()
df1 = temp_result.groupby('label')['id'].apply(list).reset_index(name='new')
result_list = df1['new'].tolist()
final_answer = my_sort(result_list)

# print output
f = open(community_output_file, 'w')
for i in final_answer:
    f.write(str(i)[1:-1])
    f.write('\n')
f.close()
