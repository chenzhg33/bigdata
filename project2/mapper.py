#! /usr/bin/bash
import sys
import re

user_dic = {}
time_dic = {}

answer = re.compile("\\t\"(answer)\"\\t")
question = re.compile("\\t\"question\"\\t")
p1 = re.compile("<p>([^\t]*)</p>")
p3 = re.compile(".*\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.*\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.*")
content = ""
ctmp = ""
pid = re.compile("\\b\\d+\\b")
first = True

for line in sys.stdin:
	if first:
		first = False
		continue
	ctmp += line
	if p3.search(ctmp) == None:
		continue
	content = ctmp
	ctmp = ""
	#print content, "-----------------------------------------------"
	type = answer.search(content)
	if type == None:
		type = question.search(content)
		if type == None:
			continue

	id = content.split()[0].strip("\"")
	if type.group().find("answer") != -1:
		id = pid.search(content, type.start(1)).group()
		type = 0
	else:
		type = 1
	m1 = p1.search(content)
	if m1 == None:
		#print content, "---------------------------------------------------------------"
		continue
	length = len(p1.search(content).group())
	length = len(content.split("\t")[4])
	print "{0}\t{1}\t{2}".format(type, id, length)
	#print "-----------------------------------------------"
