#!/usr/bin/python
import sys
 
#id title   tagnames    author_id   body    node_type   parent_id   abs_parent_id   added_at    score   state_string    last_edited_id  last_activity_by_id last_activity_at    active_revision_id  extra   extra_ref_id    extra_count marked
 
 
item_count = None
curr_line = None
 
def dequote(t):
		if t.startswith("\"") and t.endswith("\""):
				return t[1:-1]
		else:
				return t
 
def map_a_record(curr_line):
		items = curr_line.replace("\n"," ").split("\t")
		if not len(items)==item_count:
				return

		node_type = dequote(items[5])
		assert(node_type in ["question", "answer", "comment"])
		type = "q"
		if node_type == "answer":
				type = "a"
		elif node_type == "comment":
				type = "c"

		print "{0}\t{1}".format(type, len(dequote(items[4])))

for line in sys.stdin:
		if item_count==None:
				item_count=len(line.split("\t"))
				continue
     
		items = line.split("\t")
		if len(items)>4 and dequote(items[0]).isdigit() and dequote(items[3]).isdigit():
				# A new record
				if curr_line != None:
						map_a_record(curr_line)
				curr_line = line
		else:
				curr_line += line
 
if curr_line != None:
		map_a_record(curr_line)
