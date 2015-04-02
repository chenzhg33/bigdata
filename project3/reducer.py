#! /usr/bin/python

import sys

tag_dic = {}
tag_list = []

for line in sys.stdin:
	line = line.strip()
	if line in tag_dic:
		tag_dic[line] += 1
	else:
		tag_dic[line] = 1
	
for key in tag_dic.keys():
	tag_list.append((key, tag_dic[key]))

length = len(tag_list)

for i in range(0, 10):
	index = i
	for j in range(i + 1, length):
		if tag_list[index][1] < tag_list[j][1]:
			index = j
		elif tag_list[index][1] == tag_list[j][1]:
			if cmp(tag_list[index][0], tag_list[j][0]) > 0:
				index = j
	print "{0}\t{1}".format(tag_list[index][0], tag_list[index][1])
	tag_list[index] = tag_list[i]

