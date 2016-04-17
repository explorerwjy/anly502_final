#!/usr/bin/spark-submit
#
# Basic statstics on business of yelp dataset
# Based on City
# Based on category
# City vs business number plot group by category

import sys
from operator import add
from pyspark import SparkContext
import json

# Group all business by city they belongs to
def group_by_city(business_data):
    count_by_city = business_data.map(lambda x: (x["city"],1))
    pairs1 = count_by_city.reduceByKey(lambda x,y:x+y)
    pairs2 = pairs1.map(lambda x: (x[1],x[0])).sortByKey(False).map(lambda x: (x[1].encode('utf-8').strip(),x[0]))
    counts = pairs2.collect()

    with open("business_count_by_city.txt",'w') as fout:
        for (city,count) in counts:
            fout.write("{}\t{}\n".format(city,count))

def split_cat(x):
    return x

# For a city, Find the number of business by all possible categories
def group_by_city_cat(business_data,city):
    # Filter the business_data by the city
    city_data = business_data.filter(lambda x: x["city"].encode('utf-8').strip() == city)
    # Get all categories data
    categories = city_data.map(lambda x:(x['business_id'],x['categories'])) #b_id,[cat1,cat2,cat3..catn]
    categories = categories.flatMapValues(split_cat)#b_id,cat1;b_id,cat2;...
    cat_count = categories.map(lambda x:(x[1],1))#cat,1
    cat_count = cat_count.reduceByKey(lambda x,y : x+y)#cat,count
    cat_count = cat_count.map(lambda x: (x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0]))
    counts = cat_count.collect()
    
    file_path = 'city_'+'_'.join(city.split())+'.txt'
    with open(file_path,'w') as fout:
        for (cat,count) in counts:
            fout.write("{}\t{}\n".format(cat,count))

    return
# Find the number of business by all possible categories on top n city
def stats_on_city(business_data,n):
    handle = open("business_count_by_city.txt",'r')
    cities = []
    for i in range(n):
        cities.append(handle.readline().strip().split('\t')[0])
    print cities
    for i in range(n):
        group_by_city_cat(business_data,cities[i])
    return

# Find global number of business on each categories
def stats_on_cat(business_data):
    categories = business_data.map(lambda x:(x['business_id'],x['categories'])) #b_id,[cat1,cat2,cat3..catn]
    categories = categories.flatMapValues(split_cat)#b_id,cat1;b_id,cat2;...
    cat_count = categories.map(lambda x:(x[1],1))#cat,1
    cat_count = cat_count.reduceByKey(lambda x,y : x+y)#cat,count
    cat_count = cat_count.map(lambda x: (x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0]))
    counts = cat_count.collect()
    
    file_path = 'categories_all.txt'
    with open(file_path,'w') as fout:
        for (cat,count) in counts:
            fout.write("{}\t{}\n".format(cat,count))

# Here I define best as with most number of reviews
def best_business_all(business_data,n):
    bus = business_data.map(lambda x: (x,x['review_count']))
    counts = bus.map(lambda x : (x[1],x[0])).sortByKey(False).map(lambda x: (x[1],x[0]))
    counts = counts.map(lambda x: (x[0]["business_id"],x[0]["name"],x[0]["city"],x[0]["categories"][0],x[0]["stars"],x[1])).take(n)
    file_name = "best_"+str(n)+"_business_all.txt"
    with open(file_name,'w') as fout:
        for (bus,name,city,cat,stars,count) in counts:
            fout.write("{}\t{}\t{}\t{}\t{}\t{}\n".format(bus,name,city,cat,stars,count))

# Find best business by given categories
def best_business_cat(business_data,cat,n):
    cat_data = business_data.filter(lambda x: cat in x["categories"])
    bus = cat_data.map(lambda x: (x,x['review_count']))
    counts = bus.map(lambda x : (x[1],x[0])).sortByKey(False).map(lambda x: (x[1],x[0]))
    counts = counts.map(lambda x: (x[0]["business_id"],x[0]["name"],x[0]["city"],x[0]["categories"][0],x[0]["stars"],x[1])).take(n)
    file_name = "best_"+str(n)+"_business_"+cat+".txt"
    with open(file_name,'w') as fout:
        for (bus,name,city,cat,stars,count) in counts:
            fout.write("{}\t{}\t{}\t{}\t{}\t{}\n".format(bus,name,city,cat,stars,count))


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

    #group_by_city(business_data)
    #stats_on_city(business_data,5) 
    #stats_on_cat(business_data)
    #best_business_all(business_data,20)
    best_business_cat(business_data,"Restaurants",20)


    # Procedures ends
    sc.stop()
