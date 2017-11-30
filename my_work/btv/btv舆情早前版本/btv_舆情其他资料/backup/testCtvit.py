#coding:UTF-8
'''
Created on 2016年3月16日

@author: Ivy

test
'''
import sys,os
import happybase

reload(sys)
sys.setdefaultencoding('utf8')

def dataRetrieving(hbaseIP):
    # 连接hbase数据库
    conn = happybase.Connection(hbaseIP)
    conn.open()
    # 热点新闻元数据
    table=conn.table('NEWS_Content')
    # 读取十条数据
    for key,data in table.scan( limit = 10, batch_size = 10):
        # 新闻标题
        title = data['base_info:title']
        print "title:", title
        # 来源
        source = data['base_info:from']
        print "source:", source
        # 内容
        content = data['body:content']
        print "content:", content


if __name__=='__main__':
    commentTest = dataRetrieving(hbaseIP = '192.168.168.41')


