#!/usr/bin/python

from random import shuffle
from sys import argv

def make_v4(file_name):
    hand = open(file_name,'r')
    out_file = open(file_name+'_v4','w')
    classes = {}
    total = 0
    zero,one = [],[]
    for l in hand:
        l = l.strip()
        label = l.split()[0]
        value = " ".join(l.split()[1:])
        #value = '1:' + value[0] + ' 2:' + value[1] + ' 3:' + value[2]
        if label == '1':
            one.append((label,value))
        elif label == '0':
            zero.append((label,value))
    length = len(zero)
    shuffle(one)
    one = one[0:length]
    print len(one),len(zero)
    for k,v in one :
        out_file.write(k+" "+v+'\n')
    for k,v in zero:
        out_file.write(k+" "+v+"\n")
    



def count_percent(file_name):
    hand = open(file_name,'r')
    out_file = open('format_'+file_name,'w')
    classes = {}
    total = 0
    for l in hand:
        l = l.strip()
        label = l.split()[0]
        if label not in classes:
            classes[label] = 1
        else:
            classes[label] += 1
        total += 1
    for k,v in classes.items():
        print k+" take a percent of:",float(v)/total

def label_value(file_name):
    hand = open(file_name,'r')
    out_file = open('format_'+file_name,'w')
    for l in hand:
        l = l.strip()
        label = l.split()[0]
        value = l.split()[1:]
        value = '1:' + value[0] + ' 2:' + value[1] + ' 3:' + value[2]
        out_file.write(label+' '+value+'\n')

def multi_bin(file_name):
    hand = open(file_name,'r')
    out_file = open('bin_'+file_name,'w')
    for l in hand:
        l = l.strip()
        label = int(l.split()[0])
        if label >= 4:
            label = '1'
        else:
            label = '0'
        value = l.split()[1:]
        value = '1:' + value[0] + ' 2:' + value[1] + ' 3:' + value[2]
        out_file.write(label+' '+value+'\n')

def main():
    #count_percent(argv[1])
    #label_value(argv[1])
    #multi_bin(argv[1])
    make_v4(argv[1])

if __name__=="__main__":
    main()
