#! /usr/bin/python

import sys

que_len = {}
ans_len = {}
ans_num = {}
id_list = []

for line in sys.stdin:
	type, id, length = line.split()
	if id not in id_list:
		id_list.append(id)
		que_len[id] = 0
		ans_len[id] = 0
		ans_num[id] = 0
	if cmp(type, "1") == 0:
		que_len[id] = length
	else:
		ans_len[id] += int(length)
		ans_num[id] += 1

for id in id_list:
	if ans_num[id] == 0:
		print "{0}\t{1}\t{2}".format(id, que_len[id], 0)
	else:
		print "{0}\t{1}\t{2}".format(id, que_len[id], float(ans_len[id]) / ans_num[id])


