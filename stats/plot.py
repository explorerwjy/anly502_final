import numpy as np
import matplotlib.pyplot as plt

if __name__ == "__main__":

    xx = []
    yy = []
    for line in open("city.txt"):
        vals = line.split('\t')
        x = vals[0]
        y = vals[1]
        xx.append(x)
        yy.append(float(y))
        
        
    count = len(xx)
    ind = np.arange(count)
    width = 0.035
    fig, ax = plt.subplots()
    print("ind=", ind)
    print("yy=",yy)
    rects1 = ax.bar(ind,yy,width, color = 'r')
    
    # add text
    
    ax.set_ylabel('Average characters in reviews')
    ax.set_title('Average characters in reviews per city')
    ax.set_xticks(ind)
    ax.set_xticklabels(xx)
    
    for label in ax.get_xticklabels():
        label.set_rotation(60)
        
        plt.savefig('city.pdf')
