#coding=UTF-8
'''
__author__ = 'Ivy'
created on 2016.3.1
'''

import sys

#reload(sys)
#sys.setdefaultencoding('utf-8')
import happybase
from collections import OrderedDict
conn = happybase.Connection('192.168.168.41')
conn.open()
print conn.tables()
table=conn.table('commentTable')
row = table.row('row1')
print row['testColumn:date']
ind = 0
for key,data in table.scan():
    ind += 1
    print '1',key,data
    print '2',data['testColumn:date']
    print '3',data['testColumn:weiboId']
print "total_rows" ,ind
rows = table.rows(['row1', 'row2'])
for key, data in rows:
    print 'hey',key, data


rows_as_dict = dict(table.rows(['row1', 'row2']))
print rows_as_dict
for key,data in rows_as_dict.iteritems():
    print 'hoho',key,data

# 不清楚这个order是什么
rows_as_ordered_dict = OrderedDict(table.rows(['row1', 'row2']))
for key,data in rows_as_ordered_dict.iteritems():
    print 'haha',key,data

# 指定columns,并且timestamp使其有效
row = table.row('row1', columns=['testColumn:date', 'testColumn:weiboId'],include_timestamp=True)
print row['testColumn:date']
# result ('2016-02-29', 1456739009066)

# row = table.row('row1', timestamp=123456789)


values = table.cells('row1', 'testColumn:date', versions=2)
for value in values:
    print "Cell data: %s" % value
   # result Cell data: 2016-02-29

# 指定开始行
b = table.batch()
for key, data in table.scan(row_start='row2'):
    print 'row2 begin',key, data
    b.put(key,data)
b.send()
for i,j in b:
    print i,j