import math
import csv
import sys
import numpy as np
import pandas as pd
import xgboost as xgb
import json
from pyspark import SparkContext, SparkConf

path = sys.argv[1]
validation_file = sys.argv[2]
output_file = sys.argv[3]

business_file = 'business.json'
user_file = 'user.json'
review_train_file = 'review_train.json'

conf = SparkConf().setMaster("local[*]").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf)

# load yelp_val_in.csv
val_df = pd.read_csv(validation_file,usecols = ['user_id','business_id'])

# load business.json
rdd = sc.textFile(path+business_file)
b_rdd = rdd.map(lambda x:(  (json.loads(x)).get('business_id'),(json.loads(x)).get('stars'),(json.loads(x)).get('review_count') )).collect()
b_df = pd.DataFrame(b_rdd, columns =['business_id', 'b_stars', 'b_review_count'])

# load user.json
rdd = sc.textFile(path+user_file)
u_rdd = rdd.map(lambda x:(  (json.loads(x)).get('user_id'),(json.loads(x)).get('review_count'),(json.loads(x)).get('average_stars'),(json.loads(x)).get('fans'),(json.loads(x)).get('useful'),(json.loads(x)).get('funny'),(json.loads(x)).get('cool'),(json.loads(x)).get('compliment_hot'),(json.loads(x)).get('compliment_more'),(json.loads(x)).get('compliment_profile'),(json.loads(x)).get('compliment_cute'),(json.loads(x)).get('compliment_list'),(json.loads(x)).get('compliment_note'),(json.loads(x)).get('compliment_plain'),(json.loads(x)).get('compliment_cool'),(json.loads(x)).get('compliment_funny'),(json.loads(x)).get('compliment_writer'),(json.loads(x)).get('compliment_photos') )).collect()
u_df = pd.DataFrame(u_rdd, columns =['user_id', 'u_review_count','u_stars','u_fans','useful','funny','cool','compliment_hot','compliment_more','compliment_profile','compliment_cute','compliment_list','compliment_note','compliment_plain','compliment_cool','compliment_funny','compliment_writer','compliment_photos'])

# load review_train.json
rdd = sc.textFile(path+review_train_file)
r_train_rdd = rdd.map(lambda x:(  (json.loads(x)).get('user_id'),(json.loads(x)).get('business_id'),(json.loads(x)).get('stars') )).collect()
r_train_df = pd.DataFrame(r_train_rdd, columns =['user_id', 'business_id','r_train_stars'])

# merge to get train_df, train_X, train_Y
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

xgbr = xgb.XGBRegressor(verbosity=2,n_jobs=-1)
xgbr.fit(train_X, train_Y)
ypred = xgbr.predict(test_X)

# output
output_df = temp4[['user_id', 'business_id']]
output_df['prediction'] = ypred

output_df.to_csv(output_file,index=False)