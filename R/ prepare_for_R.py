def dataClean(data):
    dat = open(data,'r')
    lines = dat.readlines()
    linesClean = [i.strip().split("\t") for i in lines] 
    Dec15x = [i for i in linesClean if  i[1] in ['2015-09','2015-10','2015-11']]
    Dec15y = [i for i in linesClean if  i[1] in '2015-12']


    with open('Dec15x','w') as fout:
        for i in Dec15x:
            fout.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7]))
    with open('Dec15y','w') as fout:
        for i in Dec15y:
            fout.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7]))

            

if __name__=="__main__":
    
    dataClean('traning_data_v1_makeup.txt')
#    print dataClean('sample1.txt')
