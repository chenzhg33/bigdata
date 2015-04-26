#! /usr/bin/python
from pyspark import SparkConf, SparkContext

sconf = SparkConf().setMaster("local").setAppName("Purchase")
sc = SparkContext(conf = sconf)

lines = sc.textFile("purchases2.txt")

def mapper(line):
	items = line.strip().split("\t")
	if len(items) == 6:
		return [(items[2], items[4])]

def reducer(x, y):
	return float(x) + float(y)

f = open("result", "w")
for result in lines.flatMap(mapper).reduceByKey(reducer).collect():
	f.write(result[0] + " " + str(result[1]) + "\n")
f.close()
