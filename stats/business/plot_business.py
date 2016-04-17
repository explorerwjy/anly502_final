#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
#
# A little program to make a plot of the yelp business statistics
# See http://matplotlib.org/examples/api/barchart_demo.html for worked example

import numpy as np
import matplotlib.pyplot as plt
import unicodedata

def business_count_by_city(n):
    citys = []
    numbers = []
    i = 0
    for line in open("business_count_by_city.txt"):
        vals = line.strip().split("\t")
        (city,count) = unicodedata.normalize('NFKD', vals[0].decode('utf-8')).encode('ascii','ignore'),vals[1]
        citys.append(city)
        
        numbers.append(int(count))
        i+=1
        if i > n:
            break
    print citys
    count = len(citys)         # number of bar groups to plot
    ind   = np.arange(count)    # The X locations for the groups
    width = 0.35                # the width of each bar
    fig, ax = plt.subplots()
    print("ind=",ind)
    print("number=",numbers)
    rects = ax.bar(ind, numbers, width, color='y')
    
    # add some text
    ax.set_ylabel("Number of business")
    ax.set_title("Number of business for each city")
    ax.set_xticks(ind + width)   # where the tics go
    ax.set_xticklabels(citys)  # the are strings
    #ax.legend((rects[0]), ("number"))

    # Rotate the labels ( you need to rotate them separately)
    for label in ax.get_xticklabels():
        label.set_rotation(30)
    
    #plt.show()
    # Saving the figure causes all of the plot commands to be executed
    plt.savefig("business_count_by_city.pdf")

def categories_all(n):
    cats = []
    numbers = []
    i = 0
    for line in open("categories_all.txt"):
        vals = line.strip().split("\t")
        (cat,count) = unicodedata.normalize('NFKD', vals[0].decode('utf-8')).encode('ascii','ignore'),vals[1]
        cats.append(cat)
        
        numbers.append(int(count))
        i+=1
        if i > n:
            break
    print cats
    count = len(cats)         # number of bar groups to plot
    ind   = np.arange(count)    # The X locations for the groups
    width = 0.35                # the width of each bar
    fig, ax = plt.subplots()
    print("ind=",ind)
    print("number=",numbers)
    rects = ax.bar(ind, numbers, width, color='y')
    
    # add some text
    ax.set_ylabel("Number of business")
    ax.set_title("Number of business for each categories")
    ax.set_xticks(ind + width)   # where the tics go
    ax.set_xticklabels(cats)  # the are strings
    #ax.legend((rects[0]), ("number"))

    # Rotate the labels ( you need to rotate them separately)
    for label in ax.get_xticklabels():
        label.set_rotation(30)
    
    #plt.show()
    # Saving the figure causes all of the plot commands to be executed
    plt.savefig("categories_all.pdf")

def city_(city,n):
    cats = []
    numbers = []
    i = 0
    for line in open("city_"+city+".txt"):
        vals = line.strip().split("\t")
        (cat,count) = unicodedata.normalize('NFKD', vals[0].decode('utf-8')).encode('ascii','ignore'),vals[1]
        cats.append(cat)
        
        numbers.append(int(count))
        i+=1
        if i > n:
            break
    print cats
    count = len(cats)         # number of bar groups to plot
    ind   = np.arange(count)    # The X locations for the groups
    width = 0.35                # the width of each bar
    fig, ax = plt.subplots()
    print("ind=",ind)
    print("number=",numbers)
    rects = ax.bar(ind, numbers, width, color='y')
    
    # add some text
    ax.set_ylabel("Number of business")
    ax.set_title("Number of business for each categories")
    ax.set_xticks(ind + width)   # where the tics go
    ax.set_xticklabels(cats)  # the are strings
    #ax.legend((rects[0]), ("number"))

    # Rotate the labels ( you need to rotate them separately)
    for label in ax.get_xticklabels():
        label.set_rotation(30)
    
    #plt.show()
    # Saving the figure causes all of the plot commands to be executed
    plt.savefig("city_"+city+".pdf")

def plot_by_city():
    city_list = ['Charlotte','Las_Vegas','Montréal'.decode('utf-8'),'Phoenix','Scottsdale']
    for city in city_list:
        city_(city,10)

def main():
    business_count_by_city(10)
    categories_all(10)
    plot_by_city()


if __name__=="__main__":
    main()

