#! /usr/bin/python
import sys
import re


p1 = re.compile("(\\d{9})")
p2 = re.compile("\\d{4}-\\d{2}-\\d{2} (\\d{2}:\\d{2}:\\d{2})")
p3 = re.compile(".*\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.*\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.*")
content = ""
p4 = re.compile("\\d{9}.*\\d{9}")

for line in sys.stdin:
	content += line
	if p3.search(content) == None:
		continue
	m1 = p1.search(content)
	m2 = p2.search(content)
	content = ""
	if m1 == None or m2 == None:
		continue
	user_id = m1.group(1)
	time = m2.group(1).split(" ")[-1].split(":")[0]
	print "{0}\t{1}".format(user_id, time)
