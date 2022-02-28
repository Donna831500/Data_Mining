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

random.seed(553)
bx = BlackBox()
fixed_size = 100

output_list = []
sample_list = []

# first 100 users
'''
stream_users = bx.ask(input_file,stream_size)
sample_list = sample_list+stream_users
output_list.append((stream_size,sample_list[0],sample_list[20],sample_list[40],sample_list[60],sample_list[80]))
'''


n = 0
for ii in range(0,num_of_asks):
    stream_users = bx.ask(input_file,stream_size)
    for each_user in stream_users:
        n=n+1
        if n<=fixed_size:
            sample_list.append(each_user)
        else:
            random_value = random.random()
            if random_value<(float(fixed_size)/n): # accept sample
                index = random.randint(0,fixed_size-1)
                sample_list[index] = each_user

        if n % 100 == 0:
            output_list.append((n,sample_list[0],sample_list[20],sample_list[40],sample_list[60],sample_list[80]))


# output file
with open(output_file,'w', newline='') as temp_file:
    csv_out = csv.writer(temp_file)
    csv_out.writerow(['seqnum','0_id','20_id','40_id','60_id','80_id'])
    for each_row in output_list:
        csv_out.writerow(list(each_row))
temp_file.close()