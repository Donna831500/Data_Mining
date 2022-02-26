import time
import sys
import json
from pyspark import SparkContext


input_file = sys.argv[1]
output_file = sys.argv[2]
custom_n_partitions = int(sys.argv[3])

def reminder(x):
    return hash(x)%custom_n_partitions


sc = SparkContext("local[*]",'task2')
rdd=sc.textFile(input_file)


#### default
default_start_time = time.time()

temp1 = rdd.map(lambda x:(  (json.loads(x)).get('business_id'),1 ))
default_sets = temp1.glom().collect()
default_n_items = [len(ele) for ele in default_sets]
default_n_partition = len(default_n_items)
rdd_group_by_business = temp1.reduceByKey(lambda x,y:x+y).collectAsMap()
sorted_business = sorted(rdd_group_by_business.items(), key=lambda item: item[1],reverse=True)[:10]
default_result = [list(ele) for ele in sorted_business]

default_exe_time = time.time()-default_start_time



#### customize
customize_start_time = time.time()

temp2 = rdd.map(lambda x:(  (json.loads(x)).get('business_id'),1 )).partitionBy(custom_n_partitions, reminder)
custom_sets = temp2.glom().collect()
custom_n_items = [len(ele) for ele in custom_sets]
#rdd_group_by_business = temp1.reduceByKey(lambda x,y:x+y).collectAsMap()
#sorted_business = sorted(rdd_group_by_business.items(), key=lambda item: item[1],reverse=True)[:10]
#custom_result = [list(ele) for ele in sorted_business]

custom_exe_time = time.time()-customize_start_time


#### format

output_dict = {'default':{'n_partition':default_n_partition, 'n_items':default_n_items,'exe_time':default_exe_time},'customized':{'n_partition':custom_n_partitions,'n_items':custom_n_items,'exe_time':custom_exe_time}}
with open(output_file,'w') as outFile:
	json.dump(output_dict,outFile)

outFile.close()
