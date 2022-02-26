import sys
import json
from pyspark import SparkContext


input_file = sys.argv[1]
output_file = sys.argv[2]

sc = SparkContext("local[*]",'task1')

rdd=sc.textFile(input_file)


#### part B
B = rdd.filter(lambda x: json.loads(x).get('date')[0:4] == '2018').count()

#### part C
rdd_group_by_user = rdd.map(lambda x:(  (json.loads(x)).get('user_id'),1 )).reduceByKey(lambda x,y:x+y).collectAsMap()
C = len(rdd_group_by_user)

#### part D
sorted_user = sorted(rdd_group_by_user.items(), key=lambda item: (-item[1],item[0]),reverse=False)[:10]
D = [list(ele) for ele in sorted_user]


#### part E
rdd_group_by_business = rdd.map(lambda x:(  (json.loads(x)).get('business_id'),1 )).reduceByKey(lambda x,y:x+y).collectAsMap()
E = len(rdd_group_by_business)


#### part F
sorted_business = sorted(rdd_group_by_business.items(), key=lambda item: (-item[1],item[0]),reverse=False)[:10]
F = [list(ele) for ele in sorted_business]

#### part A
A = sum(rdd_group_by_business.values())


#### output

output_dict = {'n_review':A,'n_review_2018':B,'n_user':C,'top10_user':D,'n_business':E,'top10_business':F}
with open(output_file,'w') as outFile:
	json.dump(output_dict,outFile)

outFile.close()
