#! /usr/bin/python
from pyspark import SparkConf, SparkContext

sconf = SparkConf().setMaster("local").setAppName("Purchase")
sc = SparkContext(conf = sconf)

lines = sc.textFile("access_log2")

def mapper(line):
	start_index = line.find("\"GET")
	if start_index == -1:
		start_index = line.find("\"POST")
	if start_index == -1:
		return
	end_index = line.find(" HTTP/")
	if end_index == -1:
		return
	url = line[start_index + 5:end_index]
	items = line.split()
	code = items[len(items) - 2]
	if code.startswith("4") or code.startswith("5"):
		return [(url, (0, 1))]
	else:
		return [(url, (1, 0))]

def reducer(x, y):
	return (x[0] + y[0], x[1] + y[1])

f = open("task3_result", "w")
for result in lines.flatMap(mapper).reduceByKey(reducer).collect():
	f.write(result[0] + " " + str(float(result[1][1]) / (result[1][0] + result[1][1])) + "\n")
f.close()
