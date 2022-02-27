from pyspark import SparkContext
from pyspark import SparkConf
import math
#import time
import csv
import sys

#start_time = time.time()

global avg_for_unknown
avg_for_unknown = 3.628701193699879

def get_weight(dict1,dict2):
    users1 = dict1.keys()
    users2 = dict2.keys()
    co_users = list(set(users1).intersection(users2))
    if len(co_users)>1:
        l1 = []
        l2 = []
        for each_user in co_users:
            l1.append(dict1.get(each_user))
            l2.append(dict2.get(each_user))
        avg1 = sum(l1)/len(l1)
        avg2 = sum(l2)/len(l2)
        upper = 0
        lower1 = 0
        lower2 = 0
        for i in range(0,len(co_users)):
            upper = upper+( (l1[i]-avg1)*(l2[i]-avg2) )
            lower1 = lower1+( (l1[i]-avg1)**2 )
            lower2 = lower2+( (l2[i]-avg2)**2 )
        lower = math.sqrt(lower1)*math.sqrt(lower2)
        if lower==0:
            weight = 0
        else:
            weight = upper/lower
        return weight
    else:
        return 0


def add_user_avg_to_result(k,v,u_dict):
    total_avg = 3.8403908794788273
    result_list = []
    for each_k in k:
        reviews_dict = u_dict.get(each_k)
        if reviews_dict is None:    # if this is new user, use avg
            result_list.append((each_k,v,avg_for_unknown))
        else:
            temp = list(reviews_dict.values())
            user_avg = sum(temp)/len(temp)
            wanted = (total_avg+user_avg)/2
            result_list.append((each_k,v,wanted))
    return result_list


def add_minor_to_result(k,v,review_dict):
    total_avg = 3.750487329434698
    review_l = list(review_dict.values())
    p = sum(review_l)/len(review_l)
    wanted = (total_avg+p)/2
    result_list = []
    for each_k in k:
        result_list.append((each_k,v,wanted))
    return result_list


def get_prediction(l,eval_review_dict):
    rate_list = list(eval_review_dict.values())
    rate_avg = sum(rate_list)/len(rate_list)
    if len(l)==0:
        return rate_avg
    else:
        p_upper = 0
        p_lower = 0
        for each_w_r in l:
            weight = each_w_r[0]
            r = each_w_r[1]
            p_upper = p_upper+(weight*r)
            p_lower = p_lower+weight
        p = p_upper/p_lower
        result_combine = (rate_avg*0.7)+(p*0.3)
        return result_combine



train_file = sys.argv[1]
validation_file = sys.argv[2]
output_file = sys.argv[3]


conf = SparkConf().setMaster("local[*]").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf)

data = sc.textFile(train_file)
header = data.first()


train_bkey = data.filter(lambda x: x != header).map(lambda x: x.split(',')).map(lambda x: (x[1],dict(((x[0],float(x[2])),)) ) ).reduceByKey(lambda x, y: {**x, **y}).collectAsMap()
train_bkey_minor = dict()
train_bkey_main = dict()
for (k,v) in train_bkey.items():
    if len(v)>2:
        train_bkey_main[k]=v
    else:
        train_bkey_minor[k]=v



train_ukey = data.filter(lambda x: x != header).map(lambda x: x.split(',')).map(lambda x: (x[0],dict(((x[1],float(x[2])),)) ) ).reduceByKey(lambda x, y: {**x, **y}).collectAsMap()
train_main_buses = sorted(list(train_bkey_main.keys()))
train_minor_buses = sorted(list(train_bkey_minor.keys()))


data1 = sc.textFile(validation_file)
header = data1.first()
val_bkey = data1.filter(lambda x: x!=header).map(lambda x: x.split(',')).map(lambda x: (x[1],[x[0]] ) ).reduceByKey(lambda x,y:x+y).collectAsMap()





result = []
for (eval_bus,eval_users) in val_bkey.items():
    if eval_bus in train_main_buses:    # need compute
        eval_bus_review = train_bkey_main.get(eval_bus)
        for eval_user in eval_users:
            need_compute_buses = train_ukey.get(eval_user)
            if need_compute_buses is None: # if eval_user is a new user
                temp = list(eval_bus_review.values)
                eval_bus_avg = sum(temp)/len(temp)
                result.append((eval_user,eval_bus,eval_bus_avg))
            else: # if eval_user is an old user
                current_w_r_list = []
                for (each_bus,each_rate) in need_compute_buses.items():
                    other_bus_review = train_bkey_main.get(each_bus)
                    if other_bus_review is not None: # if the bus is rated by >2 users,w!=0 (need calculate w)
                        w = get_weight(other_bus_review,eval_bus_review)
                        if w>0:
                            current_w_r_list.append([w,each_rate])
                current_p = get_prediction(current_w_r_list,eval_bus_review)
                result.append((eval_user,eval_bus,current_p))

    elif eval_bus in train_minor_buses: # if the business is rated by 1 or 2 user
        review_dict = train_bkey_minor.get(eval_bus)
        result = result+add_minor_to_result(eval_users,eval_bus,review_dict)
    else:   # if the business is not rated before
        result = result+add_user_avg_to_result(eval_users,eval_bus,train_ukey)





# output
with open(output_file,'w', newline='') as temp_file:
    csv_out = csv.writer(temp_file)
    csv_out.writerow(['user_id','business_id','prediction'])
    for each_row in result:
        csv_out.writerow(list(each_row))
temp_file.close()

