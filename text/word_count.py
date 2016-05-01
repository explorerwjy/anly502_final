#!/usr/bin/spark-submit
#
# Find the grown pattern of review number of a business

import sys
from pyspark import SparkContext
import json
import string
from string import translate


###########################################################################################
# Construct a word list of positive words and negetive words
###########################################################################################
def split_len_word(line,length,remove_punctuation_map):
    line = line.translate(remove_punctuation_map).lower()
    word_list = line.split()
    res = []
    for i in xrange(0,len(word_list)-length+1):
        res.append(" ".join(word_list[i:i+length]))
    return res

# Find which words occurs most in high stars review
def find_best_words(review_data,n,length,cat):
    review_data = review_data.filter(lambda x: float(x['stars']) >= 4.0)
    review_text = review_data.map(lambda x: x['text'])
    punctuations='!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~'
    remove_punctuation_map = dict((ord(char), None) for char in punctuations)
    words = review_text.flatMap(lambda x:split_len_word(x,length,remove_punctuation_map))
    result = words.map(lambda x:(x,1)).reduceByKey(lambda x,y :x+y)
    total = result.map(lambda x:("Total",x[1])).reduceByKey(lambda x,y:x+y)
    result = result.map(lambda x:(x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0]))
    total = total.collect()
    counts = result.take(n)
    with open(cat+"_good_words_"+str(length)+".txt",'w') as fout:
        for k,v in total:
            fout.write("{}\t{}\n".format(k.encode('utf-8'),v))
        for k,v in counts:
            fout.write("{}\t{}\n".format(k.encode('utf-8'),v))

def find_worst_words(review_data,n,length,cat):
    review_data = review_data.filter(lambda x: float(x['stars']) <= 2.0)
    review_text = review_data.map(lambda x: x['text'])
    punctuations='!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~'
    remove_punctuation_map = dict((ord(char), None) for char in punctuations)
    words = review_text.flatMap(lambda x:split_len_word(x,length,remove_punctuation_map))
    result = words.map(lambda x:(x,1)).reduceByKey(lambda x,y :x+y)
    total = result.map(lambda x:("Total",x[1])).reduceByKey(lambda x,y:x+y)
    result = result.map(lambda x:(x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0]))
    total = total.collect()
    counts = result.take(n)
    with open(cat+"_bad_words_"+str(length)+".txt",'w') as fout:
        for k,v in total:
            fout.write("{}\t{}\n".format(k.encode('utf-8'),v))
        for k,v in counts:
            fout.write("{}\t{}\n".format(k.encode('utf-8'),v))

def wordcount(review_data,n,length,cat):
    if cat != 'All':
        review_data = review_data.filter(lambda x: cat in x['categories'])
    find_best_words(review_data,n,length,cat)
    find_worst_words(review_data,n,length,cat)

###########################################################################################
# Construct a word list of positive words and negetive words
# # stars in a Yelp review on # positive words, # negative words, and # words in review
###########################################################################################
def text_to_point(x,pos_dic,neg_dic,remove_punctuation_map):
    line = x["text"].translate(remove_punctuation_map).lower()
    words = line.split()
    pos_words_num = 0
    neg_words_num = 0
    total_words_num = len(words)
    review_star = x['stars']
    for word in words:
        if word in pos_dic:
            pos_words_num += 1
        if word in neg_dic:
            neg_words_num += 1
    return (review_star,pos_words_num,neg_words_num,total_words_num)
    

def wordmap(review_data):
    pos_dic_file =  'positive-words.txt'
    neg_dic_file =  'negative-words.txt'
    #construct the dictionary
    pos_dic = {}
    neg_dic = {}
    with open(pos_dic_file,'r') as fin:
        for line in fin:
            word = line.strip()
            pos_dic[word] = 1
        fin.close()
    with open(neg_dic_file,'r') as fin:
        for line in fin:
            word = line.strip()
            neg_dic[word] = 1
        fin.close()
    punctuations='!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~'
    remove_punctuation_map = dict((ord(char), None) for char in punctuations)
    data = review_data.map(lambda x:text_to_point(x,pos_dic,neg_dic,remove_punctuation_map))
    data = data.collect()
    with open("regression_review_text",'w') as fout:
        for v1,v2,v3,v4 in data:
            fout.write("{}\t{}\t{}\t{}\n".format(v1,v2,v3,v4))


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
    cat = "Restaurants"
    cat = "All"
    wordcount(review_data,2000,1,cat)
    #wordmap(review_data)


    
    # Procedures ends
    sc.stop()
