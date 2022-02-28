##########################################
# Method Description:
# My method consist of linear combination of following parts:
#  1. item-based CF: I use the same way in hwk3 to calculate item-based CF prediction
#  2. XGBregressor: I use the same way in hwk3 to calculate XGBregressor prediction, I adjust some parameters to get better result
#  3. mean rate for each business (column name: bus_mean): take from business.json->stars
#  4. review count for each business (column name: bus_review_count): take from business.json->review_count
#  5. average rate of user (column name: user_mean): take from user.json->average_stars
#  6. the number of useful received by user (column name: user_useful): take from user.json->useful
#  7. the weighted rate of business (column name: useful_bus_rate): a review of business will weighted more
#     if it has more 'useful' clicked by other users
#
# I notice that the review count for each business will highly affect the weight of mean rate of business,
# so I seperate them to 6 part based on the review count, then apply linear regression on each part seperately,
# and combine them at the end. I record the coefficients and intersects for each part and apply them to data in order to save execution time.
#
# Error Distribution:
# >=0 and <1: 101943
# >=1 and <2: 33120
# >=2 and <3: 6199
# >=3 and <4: 781
# >=4: 1
#
# RMSE: 0.9805534498420431
#
#
# Execution Time: 530s (roughly on local machine)
#
##########################################

import math
import csv
import sys
import pandas as pd
import xgboost as xgb
import json
from pyspark import SparkContext, SparkConf

import time
start_time = time.time()

path = sys.argv[1]
validation_file = sys.argv[2] #test file name
output_file = sys.argv[3]

business_file = 'business.json'
review_file = 'review_train.json'
user_file = 'user.json'
train_file = 'yelp_train.csv'

conf = SparkConf().setMaster("local[*]").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf)

data = sc.textFile(path+train_file)
header = data.first()
train_bkey = data.filter(lambda x: x != header).map(lambda x: x.split(',')).map(lambda x: (x[1],dict(((x[0],float(x[2])),)) ) ).reduceByKey(lambda x, y: {**x, **y}).collectAsMap()
train_ukey = data.filter(lambda x: x != header).map(lambda x: x.split(',')).map(lambda x: (x[0],dict(((x[1],float(x[2])),)) ) ).reduceByKey(lambda x, y: {**x, **y}).collectAsMap()
data1 = sc.textFile(validation_file)
header = data1.first()
val_bkey = data1.filter(lambda x: x!=header).map(lambda x: x.split(',')).map(lambda x: (x[1],[x[0]] ) ).reduceByKey(lambda x,y:x+y).collectAsMap()


def avoid_extreme2(p):
    if p>5:
        return 5.0
    elif p<1:
        return 1.0
    else:
        return p


def get_weighted_business_rate(path, review_file, base_weight):
    rdd = sc.textFile(path+review_file)
    review_bkey = rdd.map(lambda x: (json.loads(x).get('business_id'), [[json.loads(x).get('stars'),
                                                                         json.loads(x).get('useful')]]) ).reduceByKey(lambda x,y:x+y).collectAsMap()
    weighted_business_rate = dict()
    for (k,v) in review_bkey.items():
        upper = 0
        lower = 0
        for each_couple in v:
            smooth_weight = base_weight+each_couple[1]
            upper = upper+(smooth_weight*each_couple[0])
            lower = lower+smooth_weight
        weighted_business_rate[k]=upper/lower
        #weighted_business_rate[k]=upper
    return weighted_business_rate


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
        N=15
        if len(l)>N:
            l = sorted(l, key = lambda x: x[0], reverse=True)
            l = l[:N]

        for each_w_r in l:
            weight = each_w_r[0]
            r = each_w_r[1]
            p_upper = p_upper+(weight*r)
            p_lower = p_lower+weight
        p = p_upper/p_lower
        result_combine = (rate_avg*0.7)+(p*0.3)
        #result_combine = rate_avg
        return result_combine


def item_base_CF(train_bkey,train_ukey,val_bkey):
    train_bkey_minor = dict()
    train_bkey_main = dict()
    for (k,v) in train_bkey.items():
        if len(v)>2:
            train_bkey_main[k]=v
        else:
            train_bkey_minor[k]=v
    train_main_buses = sorted(list(train_bkey_main.keys()))
    train_minor_buses = sorted(list(train_bkey_minor.keys()))


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
    return result


def xgb_regressor(path,validation_file,business_file,user_file,review_file):
    # load yelp_val_in.csv
    val_df = pd.read_csv(validation_file,usecols = ['user_id','business_id'])

    # load business.json
    rdd = sc.textFile(path+business_file)
    b_rdd = rdd.map(lambda x:(  (json.loads(x)).get('business_id'),(json.loads(x)).get('stars'),(json.loads(x)).get('review_count') )).collect()
    b_df = pd.DataFrame(b_rdd, columns =['business_id', 'b_stars', 'b_review_count'])
    #b_rdd = rdd.map(lambda x:(  (json.loads(x)).get('business_id'),(json.loads(x)).get('stars'),(json.loads(x)).get('review_count') )).collect()
    #b_df = pd.DataFrame(b_rdd, columns =['business_id', 'b_stars', 'b_review_count'])

    # load user.json
    rdd = sc.textFile(path+user_file)
    u_rdd = rdd.map(lambda x:(  (json.loads(x)).get('user_id'),(json.loads(x)).get('review_count'),len((json.loads(x)).get('friends')),len((json.loads(x)).get('elite')),(json.loads(x)).get('average_stars'),(json.loads(x)).get('fans'),(json.loads(x)).get('useful'),(json.loads(x)).get('funny'),(json.loads(x)).get('cool'),(json.loads(x)).get('compliment_hot'),(json.loads(x)).get('compliment_more'),(json.loads(x)).get('compliment_profile'),(json.loads(x)).get('compliment_cute'),(json.loads(x)).get('compliment_list'),(json.loads(x)).get('compliment_note'),(json.loads(x)).get('compliment_plain'),(json.loads(x)).get('compliment_cool'),(json.loads(x)).get('compliment_funny'),(json.loads(x)).get('compliment_writer'),(json.loads(x)).get('compliment_photos') )).collect()
    u_df = pd.DataFrame(u_rdd, columns =['user_id', 'u_review_count','u_friends','u_elite','u_stars','u_fans','useful','funny','cool','compliment_hot','compliment_more','compliment_profile','compliment_cute','compliment_list','compliment_note','compliment_plain','compliment_cool','compliment_funny','compliment_writer','compliment_photos'])
    #u_rdd = rdd.map(lambda x:(  (json.loads(x)).get('user_id'),(json.loads(x)).get('review_count'),(json.loads(x)).get('average_stars'),(json.loads(x)).get('fans'),(json.loads(x)).get('useful'),(json.loads(x)).get('funny'),(json.loads(x)).get('cool'),(json.loads(x)).get('compliment_hot'),(json.loads(x)).get('compliment_more'),(json.loads(x)).get('compliment_profile'),(json.loads(x)).get('compliment_cute'),(json.loads(x)).get('compliment_list'),(json.loads(x)).get('compliment_note'),(json.loads(x)).get('compliment_plain'),(json.loads(x)).get('compliment_cool'),(json.loads(x)).get('compliment_funny'),(json.loads(x)).get('compliment_writer'),(json.loads(x)).get('compliment_photos') )).collect()
    #u_df = pd.DataFrame(u_rdd, columns =['user_id', 'u_review_count','u_stars','u_fans','useful','funny','cool','compliment_hot','compliment_more','compliment_profile','compliment_cute','compliment_list','compliment_note','compliment_plain','compliment_cool','compliment_funny','compliment_writer','compliment_photos'])

    # load review_train.json
    rdd = sc.textFile(path+review_file)
    r_train_rdd = rdd.map(lambda x:(  (json.loads(x)).get('user_id'),(json.loads(x)).get('business_id'),(json.loads(x)).get('stars') )).collect()
    r_train_df = pd.DataFrame(r_train_rdd, columns =['user_id', 'business_id','r_train_stars'])


    temp1 = pd.merge(b_df, r_train_df,  how='right', left_on=['business_id'], right_on = ['business_id'])
    temp2 = pd.merge(u_df, temp1,  how='right', left_on=['user_id'], right_on = ['user_id'])
    train_df = temp2.drop(columns=['user_id', 'business_id'])
    number_of_cols = train_df.iloc[0,:].size
    train_X = train_df.iloc[:, :-1]
    train_Y = train_df.iloc[:, number_of_cols-1]

    # merge to get test_df, test_X, test_Y
    #val_result_file = 'data/yelp_val.csv'
    #val_result_df = pd.read_csv(val_result_file)

    temp3 = pd.merge(b_df, val_df,  how='right', left_on=['business_id'], right_on = ['business_id'])
    temp4 = pd.merge(u_df, temp3,  how='right', left_on=['user_id'], right_on = ['user_id'])

    #temp5 = pd.merge(temp4, val_result_df, how='right', left_on=['user_id','business_id'], right_on = ['user_id','business_id'])
    test_X = temp4.drop(columns=['user_id', 'business_id'])

    xgbr = xgb.XGBRegressor(verbosity=1,n_jobs=-1)
    xgbr.fit(train_X, train_Y)
    ypred = xgbr.predict(test_X)

    output_df = temp4[['user_id', 'business_id']]
    output_df['xgb'] = ypred
    return output_df

# -------------------------------------------item base CF-----------------------------------------------------------
item_cf_result = item_base_CF(train_bkey,train_ukey,val_bkey)

# -------------------------------------------XGB regressor-----------------------------------------------------------
xgb_result = xgb_regressor(path,validation_file,business_file,user_file,review_file)

#-------------------------------------------business mean-----------------------------------------------------------
rdd=sc.textFile(path+business_file)
bus_avg_dict = rdd.map(lambda x: (json.loads(x).get('business_id'), (json.loads(x).get('stars'),json.loads(x).get('review_count')) ) ).collectAsMap()

#-------------------------------------------user mean-----------------------------------------------------------
rdd=sc.textFile(path+user_file)
user_avg_dict = rdd.map(lambda x: ( json.loads(x).get('user_id'), (json.loads(x).get('average_stars'),json.loads(x).get('useful')) )   ).collectAsMap()

#-------------------------------------business weighted mean-----------------------------------------------------------
weighted_business_rate = get_weighted_business_rate(path, review_file, 50)

cf_df = pd.DataFrame(item_cf_result, columns=['user_id', 'business_id', 'item_cf'])
result_summary = pd.merge(xgb_result, cf_df,  how='inner', left_on=['user_id','business_id'], right_on = ['user_id','business_id'])
result_summary['bus_mean'] = result_summary.apply(lambda row: bus_avg_dict.get(row.business_id)[0], axis=1)
result_summary['bus_review_count'] = result_summary.apply(lambda row: bus_avg_dict.get(row.business_id)[1], axis=1)
result_summary['user_mean'] = result_summary.apply(lambda row: user_avg_dict.get(row.user_id)[0], axis=1)
result_summary['user_useful'] = result_summary.apply(lambda row: user_avg_dict.get(row.user_id)[1], axis=1)
result_summary['useful_bus_rate'] = result_summary.apply(lambda row: weighted_business_rate.get(row.business_id,row.bus_mean), axis=1)

test_df0 = result_summary[(result_summary['bus_review_count'] > 1000)]
test_df1 = result_summary[(result_summary['bus_review_count'] > 500) & (result_summary['bus_review_count'] <= 1000)]
test_df2 = result_summary[(result_summary['bus_review_count'] > 300) & (result_summary['bus_review_count'] <= 500)]
test_df3 = result_summary[(result_summary['bus_review_count'] > 200) & (result_summary['bus_review_count'] <= 300)]
test_df4 = result_summary[(result_summary['bus_review_count'] > 100) & (result_summary['bus_review_count'] <= 200)]
test_df5 = result_summary[(result_summary['bus_review_count'] > 50) & (result_summary['bus_review_count'] <= 100)]
test_df6 = result_summary[(result_summary['bus_review_count'] <= 50)]

# 0
cl = [1.0123703642486301, 0.18252010683552686, -0.46955306412661274, -1.9072575112075136e-07, -0.03048162939415905, 1.2341489297089944e-06, 0.5460852094442428]
intercept = -0.9214070031988655
test_df0['pred'] = test_df0.apply(lambda row: avoid_extreme2((row.xgb*cl[0])+(row.item_cf*cl[1])+(row.bus_mean*cl[2])+(row.bus_review_count*cl[3])+(row.user_mean*cl[4])+(row.user_useful*cl[5])+(row.useful_bus_rate*cl[6])+intercept), axis=1)

# 1
cl = [0.9821801572833088, 0.13744591263122427, -0.4416127164959606, 2.8250878953690415e-05, -0.004862677821584241, 9.898227171243772e-07, 0.5499551491083812]
intercept = -0.8754558772218606
test_df1['pred'] = test_df1.apply(lambda row: avoid_extreme2((row.xgb*cl[0])+(row.item_cf*cl[1])+(row.bus_mean*cl[2])+(row.bus_review_count*cl[3])+(row.user_mean*cl[4])+(row.user_useful*cl[5])+(row.useful_bus_rate*cl[6])+intercept), axis=1)

# 2
cl = [1.2474971522665268, 0.18002222647570293, -0.5511828574371705, -2.558978330046043e-05, -0.26053572342799153, 1.6370441588514496e-06, 0.41508294789186195]
intercept = -0.1282068332205495
test_df2['pred'] = test_df2.apply(lambda row: avoid_extreme2((row.xgb*cl[0])+(row.item_cf*cl[1])+(row.bus_mean*cl[2])+(row.bus_review_count*cl[3])+(row.user_mean*cl[4])+(row.user_useful*cl[5])+(row.useful_bus_rate*cl[6])+intercept), axis=1)

# 3
cl = [1.1018204151900475, 0.09460453970558656, -0.34953231980992444, 1.951403252551157e-05, -0.0676463116916083, -8.85940566038572e-07, 0.31919760823617704]
intercept = -0.38665075859578524
test_df3['pred'] = test_df3.apply(lambda row: avoid_extreme2((row.xgb*cl[0])+(row.item_cf*cl[1])+(row.bus_mean*cl[2])+(row.bus_review_count*cl[3])+(row.user_mean*cl[4])+(row.user_useful*cl[5])+(row.useful_bus_rate*cl[6])+intercept), axis=1)

# 4
cl = [1.0240672963963215, 0.07032089106992677, -0.21947008414698171, 0.0003014219338372094, -0.034955979456478395, 1.371658505473904e-06, 0.2126832028130974]
intercept = -0.2481538754536703
test_df4['pred'] = test_df4.apply(lambda row: avoid_extreme2((row.xgb*cl[0])+(row.item_cf*cl[1])+(row.bus_mean*cl[2])+(row.bus_review_count*cl[3])+(row.user_mean*cl[4])+(row.user_useful*cl[5])+(row.useful_bus_rate*cl[6])+intercept), axis=1)

# 5
cl = [1.3178984012453596, 0.06888454732023047, -0.3063144650962218, 0.00037374172839171774, -0.26769548905076496, 2.039982018470344e-06, 0.07720606727758941]
intercept = 0.38934297270842455
test_df5['pred'] = test_df5.apply(lambda row: avoid_extreme2((row.xgb*cl[0])+(row.item_cf*cl[1])+(row.bus_mean*cl[2])+(row.bus_review_count*cl[3])+(row.user_mean*cl[4])+(row.user_useful*cl[5])+(row.useful_bus_rate*cl[6])+intercept), axis=1)

# 6
cl = [1.1740388943157496, 0.048026322353901696, -0.13896054228621074, 0.0005156379324254068, -0.15213131698513693, 8.063232253782647e-07, 0.00948612438010792]
intercept = 0.21134411465007474
test_df6['pred'] = test_df6.apply(lambda row: avoid_extreme2((row.xgb*cl[0])+(row.item_cf*cl[1])+(row.bus_mean*cl[2])+(row.bus_review_count*cl[3])+(row.user_mean*cl[4])+(row.user_useful*cl[5])+(row.useful_bus_rate*cl[6])+intercept), axis=1)


pred_df = pd.concat([ test_df0,test_df1, test_df2, test_df3, test_df4, test_df5, test_df6])
output_df = pred_df[['user_id', 'business_id', 'pred']]
output_tuple = list(output_df.to_records(index=False))
# output
with open(output_file,'w', newline='') as temp_file:
    csv_out = csv.writer(temp_file)
    csv_out.writerow(['user_id','business_id','prediction'])
    for each_row in output_tuple:
        csv_out.writerow(list(each_row))
temp_file.close()

print('Duration:')
print(time.time()-start_time)
