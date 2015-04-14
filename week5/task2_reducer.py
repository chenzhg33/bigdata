#! /usr/bin/python
import sys

oldkey = None
id_list = []

for line in sys.stdin:
		data = line.strip().split("\t")
		if len(data) != 2:
				continue
		thiskey, id = data

		if oldkey and oldkey != thiskey:
				print oldkey, id_list
				id_list = []
		oldkey = thiskey
		id_list.append(id)

if oldkey != None:
		print oldkey, id_list
