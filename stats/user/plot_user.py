#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
#
# A little program to make a plot of the yelp business statistics
# See http://matplotlib.org/examples/api/barchart_demo.html for worked example

import numpy as np
import matplotlib.pyplot as plt
import unicodedata

def Top_N_city_with_most_users(n):
    citys = []
    numbers = []
    i = 0
    for line in open("city_user_number.txt"):
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
    
    ax.set_ylabel("Number of Active Users")
    ax.set_title("Top_"+str(n)+"_city_with_most_users")
    ax.set_xticks(ind + width)   # where the tics go
    ax.set_xticklabels(citys)  # the are strings

    for label in ax.get_xticklabels():
        label.set_rotation(30)

    plt.savefig("city_user_number.pdf")

def elite_number_by_year():
    citys = []
    numbers = []
    for line in open("elite_number_by_year.txt"):
        vals = line.strip().split("\t")
        (city,count) = unicodedata.normalize('NFKD', vals[0].decode('utf-8')).encode('ascii','ignore'),vals[1]
        citys.append(city)
        
        numbers.append(int(count))

    print citys
    count = len(citys)         # number of bar groups to plot
    ind   = np.arange(count)    # The X locations for the groups
    width = 0.35                # the width of each bar
    fig, ax = plt.subplots()
    print("ind=",ind)
    print("number=",numbers)
    rects = ax.bar(ind, numbers, width, color='y')
    
    ax.set_ylabel("Number of Elite Users")
    ax.set_title("elite_number_by_year")
    ax.set_xticks(ind + width)   # where the tics go
    ax.set_xticklabels(citys)  # the are strings

    for label in ax.get_xticklabels():
        label.set_rotation(30)

    plt.savefig("elite_number_by_year.pdf")

def join_time_line_by_year():
    years = {}
    for line in open("user_join_time_line.txt"):
        vals = line.strip().split("\t")
        year,count = vals[0][0:4],vals[1]
        if year not in years:
            years[year] = [count]
        else:
            years[year].append(count)
    for year,count in years.items():
        years[year] = sum(map(int,count))
    sort_dis = sorted(years.items(),key=lambda p:p[0])
    citys = []
    numbers = []
    for a,b in sort_dis[:-1]:
        citys.append(a)
        numbers.append(b)

    count = len(citys)         # number of bar groups to plot
    ind   = np.arange(count)    # The X locations for the groups
    width = 0.35                # the width of each bar
    fig, ax = plt.subplots()
    print("ind=",ind)
    print("number=",numbers)
    rects = ax.bar(ind, numbers, width, color='y')
    
    ax.set_ylabel("Number of new joined Users")
    ax.set_title("Number of new joined Users of each year")
    ax.set_xticks(ind + width)   # where the tics go
    ax.set_xticklabels(citys)  # the are strings

    for label in ax.get_xticklabels():
        label.set_rotation(30)

    plt.savefig("join_time_line_by_year.pdf")

def num_of_user_by_year():
    years = {}
    for line in open("user_join_time_line.txt"):
        vals = line.strip().split("\t")
        year,count = vals[0][0:4],vals[1]
        if year not in years:
            years[year] = [count]
        else:
            years[year].append(count)
    for year,count in years.items():
        years[year] = sum(map(int,count))
    sort_dis = sorted(years.items(),key=lambda p:p[0])
    citys = []
    numbers = []
    for a,b in sort_dis[:-1]:
        citys.append(a)
        numbers.append(b)

    for i in range(1,len(citys)):
        numbers[i] = numbers[i] + numbers[i-1]


    count = len(citys)         # number of bar groups to plot
    ind   = np.arange(count)    # The X locations for the groups
    width = 0.35                # the width of each bar
    fig, ax = plt.subplots()
    print("ind=",ind)
    print("number=",numbers)
    rects = ax.bar(ind, numbers, width, color='y')
    
    ax.set_ylabel("Number of Users")
    ax.set_title("num_of_user_by_year")
    ax.set_xticks(ind + width)   # where the tics go
    ax.set_xticklabels(citys)  # the are strings

    for label in ax.get_xticklabels():
        label.set_rotation(30)

    plt.savefig("num_of_user_by_year.pdf")



def main():
    #Top_N_city_with_most_users(10)
    #elite_number_by_year()
    #join_time_line_by_year()
    num_of_user_by_year()

if __name__=="__main__":
    main()

