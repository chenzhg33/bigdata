#! /usr/bin/python
from pyspark import SparkConf, SparkContext

sconf = SparkConf().setMaster("local").setAppName("Frequent visit")
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

f = open("result", "w")
f.write("project_code page_title visit_count\n")
for result in lines.flatMap(mapper).reduceByKey(reducer).map(mapper2).sortByKey(False).take(10):
	f.write(result[1] + " " + str(result[0]) + "\n")
f.close()
