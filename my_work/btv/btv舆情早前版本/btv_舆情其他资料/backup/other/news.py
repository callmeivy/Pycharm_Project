#coding: UTF-8
'''
Created on Feb 14, 2014

@author: administrator
'''
import MySQLdb
import time
import os,sys
import pymongo
    
# 调用mysql
sqlconn=MySQLdb.connect(host='10.3.3.220',user='root',passwd='123456',db = 'cctv', charset='utf8')
sqlcursor = sqlconn.cursor()
sqlcursor.execute("CREATE TABLE IF NOT EXISTS news(newsindex bigint(64) primary key,  newsnumber int(4), recorddate varchar(128)) DEFAULT CHARSET=utf8;")

conn = pymongo.Connection('localhost', 27017);
db = conn.cctv
dbCollection = db.news

date = []
dateData = []
dateCount = {}
out = dbCollection.distinct('date')
for doc in out:
    tempdate = doc[:8]
    if tempdate not in date:
        date.append(tempdate)

for i in date:
    dateCount['date'] = i
    dateCount['count'] = 0
    dateData.append(dateCount)
    dateCount = {}

cursor = dbCollection.find()
for doc in cursor:
    dateIndex = date.index(doc['date'][:8])
    dateData[dateIndex]['count'] = dateData[dateIndex]['count']+1
 
print dateData
i=0
for doc in dateData:
    i+=1 
#     print doc['count']
    temp = []
    temp.append((i,doc['count'],doc['date']))
#     temp.append(doc['count'])
#     temp.append(doc['date'])
    sqlcursor.executemany("insert into news(newsindex, newsnumber, recorddate) values (%s, %s, %s)",temp)  
    sqlconn.commit()
    
conn.close()
sqlconn.close()
