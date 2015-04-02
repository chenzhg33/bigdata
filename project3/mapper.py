#! /usr/bin/bash

import sys
import re

question = re.compile("\\t\"question\"\\t")
p1 = re.compile("<p>([^\t]*)</p>")
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

	if question.search(content) == None:
		content = ""
		continue

	#print content, "----------------------------------------------"
	strs = content.split("\t")[2].strip("\"").split(" ")
	for tag in strs:
		print tag
	content = ""
