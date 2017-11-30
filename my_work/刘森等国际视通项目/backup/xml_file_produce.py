#coding:UTF-8
'''
Created on 2017-05-10
@author: Ivy(jincan@ctvit.com.cn)
将xml网页转为xml文件
测试集不需要用那么大数据量，可限制条数获取（本例60000行），但注意tags should be closed properly,结尾加上</resultset>
'''

import urllib2
url = 'http://l3-pv-dl.news.cctvplus.com/gds05062.xml'
resp = urllib2.urlopen(url)
f = open('E://gds2017.xml','w')
lines = ""
for x in range(6000):
    lines += resp.readline().decode('utf-8')

# print(lines)
f.write(lines.encode('utf-8'))
f.close()
print "file is ready."