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
import binascii
from blackbox import BlackBox


def myhashs(s):
    result = []
    user_int = int(binascii.hexlify(s.encode('utf8')),16)
    hash_function_list = [(1734545623,3475864383,426131),(1734545623,3475864383,434297)]
    for f in hash_function_list:
        index = (f[0]*user_int+f[1])%f[2]%69997
        result.append(index)
    return result


input_file = sys.argv[1]
stream_size = int(sys.argv[2])
num_of_asks = int(sys.argv[3])
output_file = sys.argv[4]

output_list = []

bx = BlackBox()
filter_bit_array = [0] * 69997
previous_user = set()


for ii in range(0,num_of_asks):
    fp=0
    tn=0
    stream_users = bx.ask(input_file,stream_size)
    change_index_list = []
    for each_user in stream_users:
        hash_result = myhashs(each_user)
        shown_before = True
        for each_index in hash_result:
            change_index_list.append(each_index)
            if filter_bit_array[each_index] == 0:
                shown_before = False
        if each_user not in previous_user:
            if not shown_before:
                tn=tn+1
            else:
                fp=fp+1
        previous_user.add(each_user)

    current_fpr = float(fp)/(fp+tn)
    output_list.append((ii,current_fpr))

    change_index_list = list(set(change_index_list))
    for each_i in change_index_list:
        filter_bit_array[each_i] = 1


# output file
with open(output_file,'w', newline='') as temp_file:
    csv_out = csv.writer(temp_file)
    csv_out.writerow(['Time','FPR'])
    for each_row in output_list:
        csv_out.writerow(list(each_row))
temp_file.close()