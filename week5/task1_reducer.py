#! /usr/bin/python
import sys

oldkey = None
lens = 0
num = 0

for line in sys.stdin:
		thiskey, len = line.strip().split("\t")

		if oldkey and oldkey != thiskey:
				print oldkey, float(lens) / num
				lens = 0
				num = 0
		oldkey = thiskey
		lens += int(len)
		num += 1

if oldkey != None:
		print oldkey, float(lens) / num
