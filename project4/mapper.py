#! /usr/bin/bash
import sys
import re

user_dic = {}
time_dic = {}

#answer = re.compile("\\t(answer|question)\\t")
answer = re.compile("\\t\"(answer|question)\"\\t")
p3 = re.compile(".*\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.*\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.*")
content = ""
pid = re.compile("\\b\\d+\\b")
first = True

for line in sys.stdin:
	if first:
		first = False
		continue
	content += line
	if p3.search(content) == None:
		continue
	##print content, "-----------------------------------------------"
	type = answer.search(content)
	if type == None:
		content = ""
		continue

	que_id = content.split()[0].strip("\"")
	if type.group().find("question") == -1:
		que_id = pid.search(content, type.start(1)).group()
		type = 0
	else:
		type = 1

	user_id = content.split("\t")[3].strip("\"")

	print "{0}\t{1}\t{2}".format(type, user_id, que_id)
	content = ""
	#print "-----------------------------------------------"
