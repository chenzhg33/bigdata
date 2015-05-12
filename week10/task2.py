#! /usr/bin/python
from pyspark import SparkConf, SparkContext

sconf = SparkConf().setMaster("local").setAppName("Correlated Pages")
sc = SparkContext(conf = sconf)

lines = sc.textFile("test")

def mapper(line):
	datas = line.strip().split(" ")
	code = datas[1]
	title = datas[2]
	hits = datas[3]
	return [(code + " " + title, int(hits))]

def reducer(x, y):
	return x + y

def mapper2(x):
	return (x[1], x[0])

top10 = []
for i in range(0, 10):
	top10.append((100000000, ""))
def insert(item):
	index = 0
	for i in range(0, 10):
		index = i
		if top10[i][0] > item[0]:
			break
	for i in range(1, 10 - index):
		top10[10-i] = top10[10-i-1]
	top10[index] = item

f = open("result", "w")
f.write("project_code1 page_title1 visit_count1 project_code2 page_title2 visit_count2\n")
pre = None
for result in lines.flatMap(mapper).reduceByKey(reducer).map(mapper2).sortByKey(False).collect():
	if pre == None:
		pre = result
		continue
	if pre[0] > result[0]:
		insert((pre[0] - result[0], pre[1] + " " + str(pre[0]) + " " + result[1] + " " + str(result[0])))
	elif pre[0] == result[0]:
		if cmp(result[1].split(" ")[1], pre[1].split(" ")[1]) > 0:
			insert((0, result[1] + " " + str(result[0]) + " " + pre[1] + " " + str(pre[0])))
		else:
			insert((0, pre[1] + " " + str(pre[0]) + " " + result[1] + " " + str(result[0])))
	else:
			insert((result[0] - pre[0], result[1] + " " + str(result[0]) + " " + pre[1] + " " + str(pre[0])))
	pre = result

for i in range(0, 10):
	f.write(top10[i][1] + "\n")
f.close()
