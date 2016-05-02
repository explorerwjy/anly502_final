
from pyspark import SparkContext


def my_pos_round(x):
    star,pos,neg,total = x.strip().split()
    if int(total) == 0:
        print x
    else:
        pos_portion = round(float(pos)/int(total),5)
        #neg_portion = round(float(neg)/total,6)
        return pos_portion,star

def my_reduce(x):
    #x[0] = key
    #x[1] = stars
    stars = {'1':0,'2':0,'3':0,'4':0,'5':0}
    for i in x[1]:
        stars[i] += 1
    one = float(stars['1'])/len(x[1])
    two = float(stars['2'])/len(x[1])
    three = float(stars['3'])/len(x[1])
    four = float(stars['4'])/len(x[1])
    five = float(stars['5'])/len(x[1])
    return x[0],one,two,three,four,five

def main(sc):
    count_file =  's3://anly502-yelp/regression_review_text_v3'
    data = sc.textFile(count_file)
    print data.take(10)
    data = data.map(lambda x:my_pos_round(x)).filter(lambda x:x!=None)
    data = data.groupByKey()
    data = data.map(lambda x:my_reduce(x))
    haha = data.collect()
    with open('hahahaha.txt','w') as fout:
        for a,b,c,d,e,f in haha:
            fout.write("{}\t{}\t{}\t{}\t{}\t{}\n".format(a,b,c,d,e,f))

    

if __name__ == "__main__":

    sc     = SparkContext( appName="Business_stats" )

    main(sc)

    sc.stop()
