#coding:UTF-8
'''
Created on 2016年3月25日

@author: Ivy

准备数据

'''
import time
import happybase
# 连接hbase数据库
conn = happybase.Connection('192.168.168.41')
conn.open()
table=conn.table('NEWS_Content')
import sys,os
reload(sys)
sys.setdefaultencoding('utf8')
# f = open('E:\\news\cctv_military.txt', 'r')
f = open('/usr/jincan/cctv_military_Ori.txt', 'r')
i = 0
for line in f.readlines():
    i += 1
    try:
        json = eval(line)
        # print type(json)
    except Exception:
        pass
    if json.has_key("title"):
        title = json["title"].decode('raw_unicode_escape')
        # print 'jj',title
    else:
        pass
    if json.has_key("url"):
        url = json["url"].decode('raw_unicode_escape')
        # print url
    else:
        pass
    if json.has_key("time"):
        time_news = time.localtime(json["time"]/float(1000))
        time_news = time.strftime('%Y-%m-%d %H:%M:%S',time_news)
        # print type(time_news),time_news
    else:
        pass
    if json.has_key("content"):
        content = json["content"].decode('raw_unicode_escape')
        # print content
    else:
        pass
    print i
    row_key = str(i)+ '-cntvNews-20160325'
    table.put(row_key, {'base_info:abstract': 'none', \
    'base_info:author' : 'none', 'base_info:datetime' : time_news, 'base_info:from'\
    : 'cntv_news', 'base_info:title':title, 'base_info:type': 'none', 'base_info:website' : url})
    table.put(row_key,{'body:content' :content})