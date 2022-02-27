from pyspark import SparkContext
from pyspark import SparkConf
import math
from itertools import combinations
import sys
import time
import csv


start_time = time.time()

global filter_threshold
global support
filter_threshold = int(sys.argv[1])
support = int(sys.argv[2])
input_file = sys.argv[3]
output_file = sys.argv[4]
num_of_p = 2


def split_list(l,n):
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
        result.append(temp)
        current_position = current_position+size
    return result



def getNestBasket(baskets,freq_list):
    freqs = [item for t in freq_list for item in t]
    result = []
    for each_basket in baskets:
        single_basket = []
        for each_item in each_basket:
            if each_item in freqs:
                single_basket.append(each_item)
        result.append(single_basket)
    return result


def getSingle(raw_data, p):
    chunks = split_list(raw_data,p)
    s = math.ceil(support/p)
    count_all_list = []
    candidate_list = []
    for each_chunk in chunks:
        count_all_dict = sc.parallelize(each_chunk).map(lambda x:(x,1)).reduceByKey(lambda x, y: x + y).collectAsMap()
        count_all_list.append(count_all_dict)
        # filter out candidate
        for (key, value) in count_all_dict.items():
            if value>=s:
                candidate_list.append((key,))

    candidate_list = sorted(list(set(candidate_list)))

    freq_list = []
    for each_candidate in candidate_list:
        total = 0
        for each_dict in count_all_list:
            temp = each_dict.get(each_candidate[0])
            if temp!=None:
                total = total+temp
        if total>=support:
            freq_list.append(each_candidate)

    return [candidate_list,freq_list]



def get_possible_combine(freq_list):
    result = []
    if len(freq_list)>0:
        wanted_size = len(freq_list[0])+1
        temp = list(combinations(freq_list, 2))
        for each in temp:
            temp1 = each[0]+each[1]
            nest_tuple = tuple(set(temp1))
            if len(nest_tuple)==wanted_size:
                result.append(tuple(sorted(nest_tuple)))
    return sorted(result)


# basket: list
# possible_combines: list of tuples
# return list of valid tuples
def basket2set(basket, possible_combines):
    wanted_size = len(possible_combines[0])
    result = []
    temp = list(combinations(basket, wanted_size))
    if wanted_size==2:
        for each in temp:
            result.append(tuple(sorted(each)))
    else:
        for each in temp:
            sorted_tuple = tuple(sorted(each))
            if sorted_tuple in possible_combines:
                result.append(sorted_tuple)
    return result



# baskets: list of list
# p: number of partition
# freq_list: list of tuples
def son_algr(baskets,p, freq_list):
    chunks = split_list(baskets,p)
    s = math.ceil(support/p)
    possible_combines = get_possible_combine(freq_list)
    candidate_list = []
    freq_list = []
    if len(possible_combines)>0:
        count_all_list = []
        for each_chunk in chunks:
            count_chunk_dict = dict()
            for each_basket in each_chunk:
                each_basket_combines = basket2set(each_basket, possible_combines)
                for each_tuple in each_basket_combines:
                    v = count_chunk_dict.get(each_tuple,0)
                    count_chunk_dict[each_tuple]=v+1
            #count_chunk_dict = sc.parallelize(all_combines).map(lambda x:(x,1)).reduceByKey(lambda x, y: x + y).collectAsMap()
            count_all_list.append(count_chunk_dict)
            #print(count_chunk_dict)

            # get candidate
            for (key, value) in count_chunk_dict.items():
                if value>=s:
                    candidate_list.append(key)
        candidate_list = sorted(list(set(candidate_list)))

        # count freq items
        for each_candidate in candidate_list:
            total = 0
            for each_dict in count_all_list:
                temp = each_dict.get(each_candidate)
                if temp!=None:
                    total = total+temp
            if total>=support:
                freq_list.append(each_candidate)
    return [candidate_list,freq_list]


# l: list of tuples, each line in output
def get_print_str(l):
    raw_str = str(l)
    result = raw_str[1:-1]
    if len(l[0])==1:
        result = result.replace(',)',')')
        result = result.replace(', ',',')
    return result


conf = SparkConf().setMaster("local[1]").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf)

# preprocess
data = sc.textFile(input_file)
header = data.first()
raw_data = data.filter(lambda x: x!=header).map(lambda x: x.split(',')).map(lambda x:(str(x[0]).replace('"','')+'-'+(str(x[1])).replace('"','').lstrip("0"),x[5].replace('"','').lstrip("0"))).collect()

with open('preprocess_data.csv','w', newline='') as temp_file:
    csv_out = csv.writer(temp_file)
    for each_row in raw_data:
        csv_out.writerow(list(each_row))
temp_file.close()

del raw_data;


# apply algr
pre_data = sc.textFile('preprocess_data.csv')
#basket_dict = pre_data.map(lambda x: x.split(',')).map(lambda x:(x[0],[x[1]])).reduceByKey(lambda x,y:x+y).filter(lambda x: len(x[1])>filter_threshold).collectAsMap()
basket_dict = pre_data.map(lambda x: x.split(',')).map(lambda x:(x[0],[x[1]])).reduceByKey(lambda x,y:x+y).map(lambda x:(x[0],list(set(x[1])))).filter(lambda x: len(x[1])>filter_threshold).collectAsMap()


basket_list = list(basket_dict.values())

c_list = []
f_list = []

# size = 1
flat_basket_list = [item for sublist in basket_list for item in sublist]
singles = getSingle(flat_basket_list, num_of_p)
single_candidate = sorted(singles[0])
single_freq = sorted(singles[1])

nest_basket = getNestBasket(basket_list,single_freq)

freqs = single_freq
candidates = single_candidate
c_list.append(candidates)
f_list.append(freqs)


# size >= 2

while len(freqs)>1:
    [candidates,freqs] = son_algr(nest_basket,num_of_p, freqs)
    c_list.append(candidates)
    f_list.append(freqs)



# print output
f = open(output_file, 'w')
f.write('Candidates:\n')
for i in c_list:
    if len(i)>0:
        f.write(get_print_str(i))
        f.write('\n\n')
f.write('Frequent Itemsets:\n')
for i in f_list:
    if len(i)>0:
        f.write(get_print_str(i))
        f.write('\n\n')
f.close()

# print time
print('Duration: '+str(time.time()-start_time))