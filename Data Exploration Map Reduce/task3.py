import time
import sys
import json
from pyspark import SparkContext

review_file = sys.argv[1]
business_file = sys.argv[2]
output_file_A = sys.argv[3]
output_file_B = sys.argv[4]

sc = SparkContext("local[*]", 'task3')
review_rdd = sc.textFile(review_file)
business_rdd = sc.textFile(business_file)

#### method1
m1_start_time = time.time()

busID_star_dict = {}
busID_num_dict = {}
with open(review_file, encoding="utf8") as f:
    for line in f:
        current_review = json.loads(line)
        busID = current_review.get('business_id')
        star = current_review.get('stars')
        busID_star_dict[busID] = busID_star_dict.get(busID, 0) + star
        busID_num_dict[busID] = busID_num_dict.get(busID, 0) + 1
f.close()

busID_city_dict = {}
with open(business_file, encoding="utf8") as f:
    for line in f:
        current_business = json.loads(line)
        busID = current_business.get('business_id')
        city = current_business.get('city')
        busID_city_dict[busID] = city
f.close()

# calculation
city_total_star = {}
city_total_review = {}
for k in busID_star_dict.keys():
    city = busID_city_dict.get(k)
    star = busID_star_dict.get(k)
    num = busID_num_dict.get(k)
    city_total_star[city] = city_total_star.get(city, 0) + star
    city_total_review[city] = city_total_review.get(city, 0) + num

city_avg_star = {}
for k in city_total_star.keys():
    total = city_total_star.get(k)
    num = city_total_review.get(k)
    avg = total / num
    city_avg_star[k] = avg

# sort
python_result = sorted(city_avg_star.items(), key=lambda item: (-item[1], item[0]), reverse=False)[:10]

m1_exe_time = time.time() - m1_start_time

#### method2
m2_start_time = time.time()

id_star_num = review_rdd.map(
    lambda x: ((json.loads(x)).get('business_id'), [(json.loads(x)).get('stars'), 1])).reduceByKey(
    lambda x, y: [x[0] + y[0], x[1] + y[1]])
id_city = business_rdd.map(lambda x: ((json.loads(x)).get('business_id'), (json.loads(x)).get('city')))
temp = id_city.join(id_star_num)
city_star_num = temp.map(lambda x: (x[1][0], x[1][1])).reduceByKey(
    lambda x, y: [float(x[0]) + float(y[0]), float(x[1]) + float(y[1])])
city_avg = city_star_num.map(lambda x: (x[0], (float(x[1][0]) / float(x[1][1]))))

spark_result = city_avg.takeOrdered(10, lambda x: (-x[1], x[0]))
m2_exe_time = time.time() - m2_start_time

#### output A
with open(output_file_A, 'w') as f:
    f.write('city,stars\n')
    for each_tuple in python_result:
        f.write(each_tuple[0] + ',' + str(each_tuple[1]) + '\n')
f.close()

#### output B
result_exe_time = {'m1': m1_exe_time, 'm2': m2_exe_time}
with open(output_file_B, 'w') as f:
    json.dump(result_exe_time, f)
f.close()
