#! /usr/bin/python
import sys

common_words = ["the","be","to","of","and","a","in","that","have","I","it","for","not","on","with","he","as","you","do","at","this","but","his","by","from","they","we","say","her","she","or","an","will","my","one","all","would","there","their","what","so","up","out","if","about","who","get","which","go","me","when","make","can","like","time","no","just","him","know","take","person","into","year","your","good","some","could","them","see","other","than","then","now","look","only","come","its","over","think","also","back","after","use","two","how","our","work","first","well","way","even","new","want","because","any","these","give","day","most","us"];

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
		id = dequote(items[0])
		body = dequote(items[4])
		if body == "":
				return
		origin = body
		body = body.replace("<p>", "").replace("</p>", "").replace("<em>", "").replace("</em>", "").replace(".", "").replace(",", "").replace("\"", "").replace("?", "").replace("(", "").replace(")", "").replace(":", "").replace("[", "").replace("]", "").replace("<br>", "").replace("<strong>", "").replace("</strong>", "")
		words = body.strip().split(" ")
		for word in words:
				if word not in common_words:
						print "{0}\t{1}".format(word, id + ":" + str(origin.find(word)))


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
