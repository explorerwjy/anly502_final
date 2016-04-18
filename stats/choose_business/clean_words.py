

if __name__=="__main__":
	file1 = open("good_words.txt",'r')
	file2 = open("bad_words.txt",'r')
	dic1 = {}
	dic2 = {}
	for line in file1:
		k,v = line.strip().split('\t')
		dic1[k] = v
	for line in file2:
		k,v = line.strip().split('\t')
		dic2[k] = v

	#remove same same words 
	dic1_new = {}
	dic2_new = {}

	for k,v in dic1.items():
		if k not in dic2:
			dic1_new[k] = v

	for k,v in dic2.items():
		if k not in dic1:
			dic2_new[k] = v

	with open("filtered_good_words.txt",'w') as fout1:
		list1 = sorted(dic1_new.items(),key=lambda p:p[1])
		for k,v in list1:
			fout1.write("{}\t{}\n".format(k,v))
	with open("filtered_bad_words.txt",'w') as fout1:
		list2 = sorted(dic2_new.items(),key=lambda p:p[1])
		for k,v in list2:
			fout1.write("{}\t{}\n".format(k,v))