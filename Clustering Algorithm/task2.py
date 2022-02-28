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


input_file = sys.argv[1]
stream_size = int(sys.argv[2])
num_of_asks = int(sys.argv[3])
output_file = sys.argv[4]

bx = BlackBox()

def myhashs(s):
    result = []
    user_int = int(binascii.hexlify(s.encode('utf8')),16)
    hash_function_list = [(1734545623,3475864383,426131),(1734545623,3475864383,434297),(9287583,8264892,491591)] #,(1734623,3472383,490481)
    for f in hash_function_list:
        index = (f[0]*user_int+f[1])%f[2] #%69997
        result.append(bin(index)[2:])
    return result    


def estimate(h_r_list):
    num_of_hash = len(h_r_list[0])
    max_r_list = [0]*num_of_hash
    for each_h_r in h_r_list:
        for i in range(0,num_of_hash):
            current_str = each_h_r[i]
            current_r = len(current_str) - len(current_str.rstrip('0'))
            if current_r>max_r_list[i]:
                max_r_list[i]=current_r

    sorted_max_r_list = sorted(max_r_list)
    result = int(((2**sorted_max_r_list[0])+(2**sorted_max_r_list[1]))/2)
    return result



output_list = []
for ii in range(0,num_of_asks):
    stream_users = bx.ask(input_file,stream_size)
    hash_result_list = []
    for each_user in stream_users:
        hash_result = myhashs(each_user)
        hash_result_list.append(hash_result)
    predict_value = estimate(hash_result_list)
    output_list.append((ii,len(set(stream_users)),predict_value))


# output file
with open(output_file,'w', newline='') as temp_file:
    csv_out = csv.writer(temp_file)
    csv_out.writerow(['Time','Ground Truth','Estimation'])
    for each_row in output_list:
        csv_out.writerow(list(each_row))
temp_file.close()