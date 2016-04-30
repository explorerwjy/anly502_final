#!/usr/bin/spark-submit
#
# Find the grown pattern of review number of a business


import sys
from pyspark import SparkContext
import json
import string
from string import translate

# Calculate average stars given a review list
def avg_stars(review_list):
    SUM,AVG,COUNT = 0,0,0
    for review in review_list:
        COUNT += 1
        SUM += int(review['stars'])
    AVG = float(SUM)/COUNT
    return AVG
# Divide reviews by good(3+) or bad (3-)
def divide_review(review_list):
    good,bad = 0,0
    for review in review_list:
        if int(review['stars']) > 3.0:
            good += 1
        else:
            bad += 1
    return good,bad


# Given a business, count the number of reviews by month
def find_pattern_by_business(review_data,business_id):
    review_data = review_data.filter(lambda x: x["business_id"] == business_id) #filter the dataset by certain business 
    review_by_month = review_data.map(lambda x : (x['date'][0:7],x)) #review_data,review


    review_by_month_count = review_by_month.groupByKey()
    count_by_month = review_by_month_count.map(lambda x: (x[0],(len(x[1]),divide_review(x[1]),avg_stars(x[1]))))#count the number of reviews by each month
    count_by_month = count_by_month.sortByKey(True).collect()
    print count_by_month[0:10]
    file_name = business_id+"_review_number_by_month.txt"
    with open(file_name,'w') as fout:
        for (month,val) in count_by_month:
            val1,val2,val3 = val
            val21,val22 = val2
            #month,total,good,bad,stars
            fout.write("{}\t{}\t{}\t{}\t{}\n".format(month,val1,val21,val22,val3))

def find_pattern(review_data):
    id_list = ['4bEjOyTaDG24SY5TxsaUNQ','zt1TpTuJ6y9n551sw9TaEg','2e2e7WgqU1BnpxmQL5jbfw']
    for business_id in id_list:
        find_pattern_by_business(review_data,business_id)

def split_len_word(line,length):
    remove_punctuation_map = dict((ord(char), None) for char in string.punctuation)
    line = line.translate(remove_punctuation_map).lower()
    word_list = line.split(" ")
    res = []
    for i in xrange(0,len(word_list)-length+1):
        res.append(" ".join(word_list[i:i+length]))
    return res

# Find which words occurs most in high stars review
def find_best_words(review_data,n,length):
    review_data = review_data.filter(lambda x: float(x['stars']) >= 4.0)
    review_text = review_data.map(lambda x: x['text'])
    words = review_text.flatMap(lambda x:split_len_word(x,length))
    result = words.map(lambda x:(x,1)).reduceByKey(lambda x,y :x+y)
    total = result.map(lambda x:("Total",x[1])).reduceByKey(lambda x,y:x+y)
    result = result.map(lambda x:(x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0]))
    total = total.collect()
    counts = result.take(n)
    with open("good_words_"+str(length)+".txt",'w') as fout:
        for k,v in total:
            fout.write("{}\t{}\n".format(k,v))
        for k,v in counts:
            fout.write("{}\t{}\n".format(k,v))

def find_worst_words(review_data,n,length):
    review_data = review_data.filter(lambda x: float(x['stars']) <= 2.0)
    review_text = review_data.map(lambda x: x['text'])
    words = review_text.flatMap(lambda x:split_len_word(x,length))
    result = words.map(lambda x:(x,1)).reduceByKey(lambda x,y :x+y)
    total = result.map(lambda x:("Total",x[1])).reduceByKey(lambda x,y:x+y)
    result = result.map(lambda x:(x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0]))
    total = total.collect()
    counts = result.take(n)
    with open("bad_words_"+str(length)+".txt",'w') as fout:
        for k,v in total:
            fout.write("{}\t{}\n".format(k,v))
        for k,v in counts:
            fout.write("{}\t{}\n".format(k,v))
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
    
    #find_pattern(review_data)

    find_best_words(review_data,1000,3)
    
    find_worst_words(review_data,1000,3)
    
    # Procedures ends
    sc.stop()
