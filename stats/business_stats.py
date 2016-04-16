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
    counts = pairs1.sortByKey(True).collect()

    with open("business_count_by_city",'w') as fout:
        for (city,counts) in counts:
            fout.write("{}\t{}\n".format(data,count))



if __name__ == "__main__":

    infile =  's3://anly502-yelp/yelp_academic_dataset_business.json'
    sc     = SparkContext( appName="Business_stats" )
    lines = sc.textFile(infile)
    data = lines.map(lambda x: json.loads(x))
    print data.take(10)



    sc.stop()
