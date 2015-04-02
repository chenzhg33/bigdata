#! /usr/bin/python

import sys

user_dic = {}
time_dic = {}

for line in sys.stdin:
	user_id, time = line.split("\t")
	if user_id in user_dic:
		if time in user_dic[user_id]:
			user_dic[user_id][time] += 1
			if user_dic[user_id][time] > user_dic[user_id]["max"]:
				user_dic[user_id]["max"] = user_dic[user_id][time]
		else:
			user_dic[user_id][time] = 1
	else:
		user_dic[user_id] = {time:1, "max":1}

for user in user_dic:
	mx = user_dic[user]["max"]
	for time in user_dic[user]:
		if cmp(time, "max") == 0:
			continue
		if user_dic[user][time] ==  mx:
			print user, time
