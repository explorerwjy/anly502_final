#!/usr/bin/spark-submit
#
# Find the grown pattern of review number of a business


import sys
from pyspark import SparkContext
import json

# Calculate average stars given a review list
def avg_stars(review_list):
    SUM,AVG,COUNT = 0,0,0
    for review in review_list:
        COUNT += 1
        SUM += int(review['stars'])
    AVG = float(SUM)/COUNT
    return AVG


# Given a business, count the number of reviews by month
def find_pattern_by_business(review_data,business_id):
    review_data = review_data.filter(lambda x: x["business_id"] == business_id) #filter the dataset by certain business 
    review_by_month = review_data.map(lambda x : (x['date'][0:7],x)) #review_data,review


    review_by_month_count = review_by_month.groupByKey()
    count_by_month = review_by_month.map(lambda x: x[0],(len(x[1],avg_stars(x[1]))))#count the number of reviews by each month
    count_by_month = review_by_month.sortByKey(True).collect()
    file_name = business_id+"_review_number_by_month.txt"
    with open(file_name,'w') as fout:
        for (month,number,avg) in count_by_month:
            fout.write("{}\t{}\t{}\n".format(month,number))

def find_pattern(review_data):
    id_list = ['4bEjOyTaDG24SY5TxsaUNQ','zt1TpTuJ6y9n551sw9TaEg','2e2e7WgqU1BnpxmQL5jbfw']
    for business_id in id_list:
        find_pattern_by_business(review_data,business_id)


if __name__ == "__main__":

    business_file =  's3://anly502-yelp/yelp_academic_dataset_business.json'
    user_file =  's3://anly502-yelp/yelp_academic_dataset_user.json'
    review_file =  's3://anly502-yelp/yelp_academic_dataset_review.json'
    tip_file =  's3://anly502-yelp/yelp_academic_dataset_tip.json'
    checkin_file =  's3://anly502-yelp/yelp_academic_dataset_checkin.json'

    sc     = SparkContext( appName="Business_stats" )

    business_data = sc.textFile(business_file).map(lambda x: json.loads(x))
    user_data = sc.textFile(user_file).map(lambda x: json.loads(x))
    review_data = sc.textFile(review_file).map(lambda x: json.loads(x))

    # Procedures starts
    find_pattern(review_data)
    # Procedures ends
    sc.stop()
