#! /usr/bin/python
#from pyspark import SparkConf, SparkContext

#sconf = SparkConf().setMaster("local").setAppName("Frequent visit")
#sc = SparkContext(conf = sconf)

#lines = sc.textFile("test")

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

def main(rdds):
	rdd = rdds[0]
	for item in range(1, len(rdds)):
		rdd.union(item)
	for result in rdd.flatMap(mapper).reduceByKey(reducer).map(mapper2).sortByKey(False).take(10):
		print (result[1] + "\t" + str(result[0]))
