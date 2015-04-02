#! /usr/bin/python
import sys
import re


for line in sys.stdin:
	index = line.find("\"GET")
	if index == -1:
		index = line.find("\"POST")
	if index == -1:
		continue
	pattern = re.compile(".*\\.(xml|txt|npg|ico|jpg|png|gif|jpeg|js|css|swf).*")
	if pattern.match(line):
		continue
	end_index = line.find(" HTTP/")
	if end_index == -1:
		continue
	url = line[index + 5:end_index]
	ip = line.split()[0]
	print "{0}\t{1}".format(url, ip)
