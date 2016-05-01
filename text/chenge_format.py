from sys import argv

def label_value(file_name):
    hand = open(file_name,'r')
    out_file = open('format_'+file_name,'w')
    for l in hand:
        l = l.strip()
        label = l.split()[0]
        value = l.split()[1:]
        value = '1:' + value[0] + ' 2:' + value[1] + ' 3:' + value[2]
        out_file.write(label+' '+value+'\n')

def main():
    label_value(argv[1])

if __name__=="__main__":
    main()
