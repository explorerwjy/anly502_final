#!/usr/bin/spark-submit
#
# User (elite/non-elite)
# Based on year
# Based on city

import sys
from operator import add
from pyspark import SparkContext
import json

def find_most(my_list):
    #my_list:(city,count)
    most_city = ""
    most_count = 0
    for item in my_list:
        if item[1] > most_count:
            most_count = item[1]
            most_city = item[0]
    return (most_city,most_count)

# Group all user by city they most active
# The city that user write most reviews is the city they most active
def stats_by_city(business_data,user_data,review_data):
    #users = user_data.map(lambda x : (x["user_id"],1))
    reviews = review_data.map(lambda x : (x["business_id"],x["user_id"]))
    business = business_data.map(lambda x: (x["business_id"],x["city"].encode('utf-8').strip()))
    user_city = reviews.join(business).map(lambda x : (x[1][0],x[1][1])) #user_id,city
    user_city_count = user_city.map(lambda x: (x[1]+'&'+x[2],1)) #user_id&city,1
    user_city_count = user_city_count.reduceByKey(lambda x,y: x+y)#user_id_city,count
    user_city_group = user_city_count.map(lambda x: (x[0].split('&')[0], (x[0].split('&')[1], x[1])))#user_id,(city,count)
    user_city_group = user_city_group.groupByKey()
    user_city_group = user_city_group.map(lambda x: (x[0],find_most(x[1])))#user_id,(most_city,most_count)
    city_count = user_city_group.map(lambda x: (x[1][0],1))#city,1
    city_count = city_count.reduceByKey(lambda x,y: x+y)#city,count
    city_count.map(lambda x: (x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0]))
    counts = city_count.collect()

    with open("city_user_number.txt",'w') as fout:
        for (city,count) in counts:
            fout.write("{}\t{}\n".format(city,count))

def stats_by_year(user_data):
    users = user_data.map(lambda x : (x["yelping_since"],1))
    user_reduce = users.reduceByKey(lambda x,y : x+y).sortByKey(True)
    counts = user_reduce.collect()

    with open("user_join_time_line.txt") as fout:
        for (time,count) in counts:
            fout.write("{}\t{}\n".format(time,count))
    return

def 

if __name__ == "__main__":

    business_file =  's3://anly502-yelp/yelp_academic_dataset_business.json'
    user_file =  's3://anly502-yelp/yelp_academic_dataset_user.json'
    review_file =  's3://anly502-yelp/yelp_academic_dataset_review.json'
    tip_file =  's3://anly502-yelp/yelp_academic_dataset_tip.json'
    checkin_file =  's3://anly502-yelp/yelp_academic_dataset_checkin.json'

    sc     = SparkContext( appName="User_stats" )

    business_data = sc.textFile(business_file).map(lambda x: json.loads(x))
    user_data = sc.textFile(user_file).map(lambda x: json.loads(x))
    review_data = sc.textFile(review_file).map(lambda x: json.loads(x))
    
    #call the procedures
    stats_by_city(business_data,user_data,review_data)
    #stats_by_year(user_data)

    sc.stop()
