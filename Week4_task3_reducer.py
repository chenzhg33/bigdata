#! /usr/bin/python
import sys

url_dic = {}
re_dic = {}

for line in sys.stdin:
	url, ip = line.split("\t")
	if url in url_dic:
		if ip in url_dic[url]:
			url_dic[url][ip] += 1
		else:
			url_dic[url][ip] = 1
	else:
		url_dic[url] = {}

for url1 in url_dic.keys():
	re_dic[url1] = {}
	for url2 in url_dic.keys():
		if cmp(url1, url2) == 0:
			continue
		ip_cnt = 0
		total = 0
		for ip1 in url_dic[url1].keys():
			total += url_dic[url1][ip1]
			if url_dic[url2].has_key(ip1):
				ip_cnt += url_dic[url1][ip1]
		if total != 0:
			re_dic[url1][url2] = float(ip_cnt) / total
		else:
			re_dic[url1][url2] = float(0)

for key in re_dic.keys():
	print key, re_dic[key]

