#coding:UTF-8
'''
Created on 2014年3月13日

@author: hao
'''

import MySQLdb
import pymongo
import sys
reload(sys)
sys.setdefaultencoding('utf8')

def addTVTable(mongodbIP, mysqlhostIP,mysqlUserName = 'root', mysqlPassword = '123456', dbname = 'cctvTimer'):
    mongoConn = pymongo.Connection(host = mongodbIP, port = 27017)
    # 
    newmongoCollect = mongoConn.cctv_db_20140311.newsFrom
    
    # 连接数据库
    sqlconn=MySQLdb.connect(host=mysqlhostIP,user=mysqlUserName,passwd=mysqlPassword,db = dbname,charset='utf8')
    print '连接成功'
    sqlcursor = sqlconn.cursor()
    sqlcursor.execute('''DROP TABLE IF EXISTs mediaTable;''')
    print '删库成功'
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS mediaTable(countIndex bigint(64) primary key, mediaName varchar(1024), 
                        mediaCaptureCount bigint(32)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'
    
    listCursor = newmongoCollect.find().batch_size(30)
    mediaList = {}
    # 存储数据
    for doc in listCursor:
        mediaList = doc
    
    mediaName = mediaList.keys()
    mediaCaptureCounts = mediaList.values()
    for i in range(len(mediaName)-1):
        tempData=[]
        tempData.append(i+1)
        tempData.append(mediaName[i])
        tempData.append(mediaCaptureCounts[i])
        temp = tuple(tempData)
        sqlcursor.execute('''insert into mediaTable(countIndex, mediaName, mediaCaptureCount) values (%s, %s, %s)''', temp)
        sqlconn.commit() 
    
    sqlconn.close()
        
if __name__=='__main__':
    weiboTest = addTVTable(mongodbIP = '10.3.3.220', mysqlhostIP = '10.3.3.220', dbname = 'cctvTimer')
    