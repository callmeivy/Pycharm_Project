#coding:UTF-8
'''
Created on 7 Mar 2014
共插入14560条
@author: Administrator
'''
import sys,os
from sys import path
path.append(path[0]+'/tools/')

import MySQLdb
import pymongo
import jieba
import jieba.analyse
import time
import datetime

from emtionProcess import emotionProcess

def addMediaCommentTable(mongodbIP,mysqlhostIP,timeRangeBeginning, timeRangeEnding, mysqlUserName = 'root', mysqlPassword = '123456', dbname = 'cctvTimer'):
    
    mongoConn = pymongo.Connection(host=mongodbIP, port=27017)
    #
    mongoCollect = mongoConn.cctv_db_20140311.news
    newmongoCollect = mongoConn.cctv_db_20140311.newsFrom
    
    # 连接数据库
    sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname,charset='utf8')
    print '连接成功'
    sqlcursor = sqlConn.cursor()
#     sqlcursor.execute('''DROP TABLE IF EXISTs mediaCommentTable;''')
    # print '删库成功'
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS mediaCommentTable(countIndex bigint(64) primary key, mediaName varchar(1024), 
                        sentimentKeywords varchar(1024), sentiment varchar(16), sentimentScore int (16), title varchar(1024), content LONGTEXT,
                        replyTime varchar(128)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'
    
    listCursor = newmongoCollect.find().batch_size(30)
    mediaList = {}
    
    emoPro = emotionProcess()
    # 读出媒体
    for doc in listCursor:
        mediaList = doc
        
    # 存储数据
    mediaData = []
    tempData = []
    
    # 计数
    sqlcursor.execute('select count(*) from mediaCommentTable;')
    totalCount = sqlcursor.fetchall()
    totalCount = list(list(totalCount)[0])[0]
    count = 0
    printCount = totalCount
    
    for media in mediaList.keys():
        certainCursor = mongoCollect.find({'date':{'$gte':timeRangeBeginning}}).batch_size(30)
        for doc in certainCursor:
#            if doc['date']>timeRangeEnding:
#                continue
            count+=1
            printCount+=1
        #     countIndex
            tempData.append(printCount)
            
    #         mediaName
            tempData.append(media)
             
    #         内容处理
            para = doc['content']
            wordSentence = ''.join(jieba.analyse.extract_tags(para, 40))        
            (emotionsWord,emotionsScore) = emoPro.processSentence(wordSentence)
    #         sentimentKeywords
            emotionsWord = ','.join(emotionsWord)
            tempData.append(emotionsWord)
    #         sentiment
            if emotionsScore>0:
                tempData.append('正面')
            elif emotionsScore==0:
                tempData.append('中立')
            else:
                tempData.append('负面')
#             sentimentScore
            tempData.append(emotionsScore)
    #         title
            tempData.append(doc['title'])
    #         内容
            tempData.append(doc['content'])
            #     createdTime
            
            tempData.append(doc['date'][:8])
            mediaData.append(tuple(tempData))
            tempData = []
            if count>=20:
                sqlcursor.executemany('''insert into mediaCommentTable(countIndex, mediaName, sentimentKeywords, sentiment, sentimentScore, title, content, replyTime) values (%s, %s, %s, %s, %s, %s, %s, %s)''',mediaData)
                sqlConn.commit() 
                mediaData = []
                count = 0
                print '插入'+str(printCount)+'个'
                
    sqlcursor.executemany('''insert into mediaCommentTable(countIndex, mediaName, 
                        sentimentKeywords, sentiment, sentimentScore, title, content, replyTime) values (%s, %s, %s, %s, %s, %s, %s, %s)''',mediaData)
    sqlConn.commit() 
    mongoConn.close()

    
if __name__=='__main__':    
    previousDate = time.strptime(str(datetime.date.today() - datetime.timedelta(days=1000)), '%Y-%m-%d')
    timeBegin = datetime.datetime(previousDate[0], previousDate[1], previousDate[2], 0, 0, 0, 0)
    nowDate = time.strptime(str(datetime.date.today()), '%Y-%m-%d')
    
    beginTime = time.strftime("%Y%m%d%H%M%S", previousDate)
    endTime = time.strftime("%Y%m%d%H%M%S", nowDate)
    
    commentTest = addMediaCommentTable(mongodbIP = '10.3.3.220', mysqlhostIP = '10.3.3.220', timeRangeBeginning = beginTime, timeRangeEnding = endTime, dbname = 'cctvTimer')
    
