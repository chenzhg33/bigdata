#! /usr/bin/python
import sys

lens = 0
num = 0

for line in sys.stdin:
		lens += int(line)
		num += 1
if num != 0:
	print float(lens) / num
