#coding:UTF-8
import happybase

import sys,os
reload(sys)
sys.setdefaultencoding('utf8')

# 连接hbase数据库
conn = happybase.Connection('192.168.168.41')
conn.open()
table=conn.table('NEWS_Content')
for key,data in table.scan(limit = 10, batch_size = 10):
    title = data['base_info:title']
    print title
    date = data['base_info:datetime']
    print date
    url = data['base_info:website']
    print url
    content = data['body:content']
    print content
