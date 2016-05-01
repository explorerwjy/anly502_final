#!/usr/bin/python
from sys import argv

def del_same(input1,input2,output1,output2):
    file1 = open(input1,'r')
    file2 = open(input2,'r')
    dic1 = {}
    dic2 = {}
    for line in file1:
        k = line.strip().split('\t')[:-1]
        v = line.strip().split('\t')[-1]
        dic1[" ".join(k)] = v
    for line in file2:
        k = line.strip().split('\t')[:-1]
        v = line.strip().split('\t')[-1]
        dic2[" ".join(k)] = v

    #remove same same words 
    dic1_new = {}
    dic2_new = {}

    for k,v in dic1.items():
            if k not in dic2:
                    dic1_new[k] = v

    for k,v in dic2.items():
            if k not in dic1:
                    dic2_new[k] = v

    with open(output1,'w') as fout1:
            list1 = sorted(dic1_new.items(),key=lambda p:p[1])
            for k,v in list1:
                    fout1.write("{}:{}\n".format(k,v))
    with open(output2,'w') as fout1:
            list2 = sorted(dic2_new.items(),key=lambda p:p[1])
            for k,v in list2:
                    fout1.write("{}:{}\n".format(k,v))

def main():
    input1 = argv[1]
    input2 = argv[2]
    output1 = "filtered_"+input1
    output2 = "filtered_"+input2
    del_same(input1,input2,output1,output2)

if __name__=="__main__":
    main()
    
