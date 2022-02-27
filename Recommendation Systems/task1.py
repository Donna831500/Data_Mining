from pyspark import SparkContext
from pyspark import SparkConf
import math
from itertools import combinations
import sys
#import time
import csv
import random

#start_time = time.time()


def get_baskets(rows, all_bus):
    temp_result = dict()
    for each_bus in all_bus:
        total = 0
        product = 1
        for each_dict in rows:
            total = total+each_dict.get(each_bus)
            product = product*each_dict.get(each_bus)
        #basket_number = total % 10000
        basket_number = (total+product) % 1000000
        basket_contain = temp_result.get(basket_number, [])
        temp_result[basket_number] = basket_contain + [each_bus]

    # filter (key,value) pair with len(value)==1
    result = dict()
    for (key, value) in temp_result.items():
        if len(value)>1:
            result[key]=value

    return result



def jaccard_similarity(l1, l2):
    intersection = len(list(set(l1).intersection(l2)))
    union = (len(l1) + len(l2)) - intersection
    return float(intersection) / union


def get_valid_pairs(l,data,already_valid_pairs):
    pairs = list(combinations(l, 2))
    result = []
    valid_pairs = []
    for each_pair in pairs:
        temp_pair = tuple(sorted(each_pair))
        if temp_pair not in already_valid_pairs:
            bus1 = data.get(temp_pair[0])
            bus2 = data.get(temp_pair[1])
            sim = jaccard_similarity(bus1, bus2)
            if sim>=0.5:
                valid_pairs.append(temp_pair)
                result.append(temp_pair+(sim,))
    return [result,valid_pairs]


def getHashCol(num_of_user,n,random_state):
    num_repeat = math.ceil(num_of_user/n)
    result = []
    for j in range(0,num_repeat):
        order = list(range(0, n))
        result = result+order
    result = result[0:num_of_user]
    random.Random(random_state).shuffle(result)
    #random.shuffle(result)
    return result



b = 27
r = 2
M = b * r
input_file = sys.argv[1]
output_file = sys.argv[2]

# produce input matrix
conf = SparkConf().setMaster("local[*]").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf)
data = sc.textFile(input_file)
header = data.first()
raw_data_ukey = data.filter(lambda x: x != header).map(lambda x: x.split(',')).map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).collectAsMap()
raw_data_bkey = data.filter(lambda x: x!=header).map(lambda x: x.split(',')).map(lambda x:( x[1],[x[0]] )).reduceByKey(lambda x,y:x+y).collectAsMap()

all_user = sorted(list(raw_data_ukey.keys()))
all_bus = sorted(list(raw_data_bkey.keys()))
num_of_bus = len(all_bus)
num_of_user = len(all_user)

# get signature matrix
Matrix = []
for index in range(0, M):
    M_current_row = dict.fromkeys(all_bus, math.inf)
    hash_col = getHashCol(num_of_user,5000,index)
    inner_index=0
    for (key, value) in raw_data_ukey.items():
        hash_result = hash_col[inner_index]  # hash(key)%101
        inner_index = inner_index+1
        for each_bus in value:
            previous_value = M_current_row.get(each_bus)
            if previous_value > hash_result:
                M_current_row[each_bus] = hash_result
    Matrix.append(M_current_row)




# find candidate
all_baskets = []
for i in range(0, b):
    rows = Matrix[i * r:(i + 1) * r]
    basket = get_baskets(rows,all_bus)
    all_baskets.append(basket)


# find valid pairs

already_valid_pairs = []
final_result = []
for each_basket in all_baskets:
    for (key, value) in each_basket.items():
        [result_row,valid_pairs] = get_valid_pairs(value,raw_data_bkey,already_valid_pairs)
        already_valid_pairs = already_valid_pairs+valid_pairs
        final_result = final_result+result_row



# output
sorted_result = sorted(final_result, key = lambda x: (x[0], x[1]))
with open(output_file,'w', newline='') as temp_file:
    csv_out = csv.writer(temp_file)
    csv_out.writerow(['business_id_1','business_id_2','similarity'])
    for each_row in sorted_result:
        csv_out.writerow(list(each_row))
temp_file.close()


# print time
#print('Duration: ' + str(time.time() - start_time))