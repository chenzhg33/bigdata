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
	return [(url, 1)]

def reducer(x, y):
	return int(x) + int(y)

f = open("task2_result", "w")
for result in lines.flatMap(mapper).reduceByKey(reducer).collect():
	f.write(result[0] + " " + str(result[1]) + "\n")
f.close()
