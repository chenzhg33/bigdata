#! /usr/bin/python

import sys

que_dic = {}

for line in sys.stdin:
	type, user_id, que_id = line.strip().split("\t")
	if que_id not in que_dic:
		que_dic[que_id] = []
	que_dic[que_id].append(user_id)

for qid in que_dic:
	print "{0}\t{1}".format(qid, que_dic[qid])


