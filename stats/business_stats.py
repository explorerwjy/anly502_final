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
def group_by_city(data):
    count_by_city = data.map(lambda x: (x["city"],1))
    pairs1 = count_by_city.reduceByKey(lambda x,y:x+y)
    pairs2 = pairs1.map(lambda x: (x[1],x[0])).sortByKey(False).map(lambda x: (x[1].encode('utf-8').strip(),x[0]))
    top_10_city = pairs2.map(lambda x: x[0].encode('utf-8').strip()).take(10)
    counts = pairs2.collect()

    with open("business_count_by_city.txt",'w') as fout:
        for (city,count) in counts:
            fout.write("{}\t{}\n".format(city,count))
    return top_10_city

def group_by_city_cat(data,city):
    city_data = data.filter(lambda x: x["city"].encode('utf-8').strip() == city)
    #print city_data.map(lambda x: x["city"]).take(10)
    count_by_cat = city_data.map(lambda x: (x["categories"],1))
    
    pairs1 = count_by_cat.reduceByKey(lambda x,y : x+y)
    pairs2 = pairs1.map(lambda x: (x[1],x[0])).sortByKey(False).map(lambda x: (x[1].encode('utf-8').strip(),x[0]))
    counts = pairs2.collect()
    
    file_path = 'city_'+'_'.join(city.split())+'.txt'
    with open(file_path,'w') as fout:
        for (cat,count) in counts:
            fout.write("{}\t{}\n".format(cat,count))

    return
def stats_on_city(data,n):
    handle = open("business_count_by_city.txt",'r')
    cities = []
    for i in range(n):
        cities.append(handle.readline().strip().split('\t')[0])
    print cities
    for i in range(n):
        group_by_city_cat(data,cities[i])
    return

if __name__ == "__main__":

    infile =  's3://anly502-yelp/yelp_academic_dataset_business.json'
    #infile = 'https://s3.amazonaws.com/anly502-yelp/yelp_academic_dataset_business.json'
    sc     = SparkContext( appName="Business_stats" )
    lines = sc.textFile(infile)
    data = lines.map(lambda x: json.loads(x))
    #group_by_city(data)
    stats_on_city(data,1) 

    sc.stop()
