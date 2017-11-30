#coding:UTF-8
'''
Created on 2014年3月10日

@author: hao
'''
import sys,os
from sys import path
path.append(path[0]+'/tools/')
import pymongo
import MySQLdb
import jieba
import jieba.analyse
import time
import datetime
# 工具类
from remove import removeIrrelevant
from emtionProcess import emotionProcess
reload(sys)
sys.setdefaultencoding('utf8')

def addTVSeriesTable(mongodbIP,mysqlhostIP,timeRangeBeginning, mysqlUserName = 'root', mysqlPassword = '123456', dbname = 'cctvTimer'):
    
    rmIrr = removeIrrelevant()
    emProcess = emotionProcess()
    # 数据库mysql
    sqlConn=MySQLdb.connect(host = mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword,db = dbname,charset='utf8')
    sqlcursor = sqlConn.cursor()
    
    # sqlcursor.execute('''DROP TABLE IF EXISTs TVSeriesTable;''')
    # print '删库成功'
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS TVSeriesTable(countIndex bigint(64) primary key, weiboId bigint(64),
                        TvSeriesName varchar(64), userId bigint(64), weiboContent varchar(1024), contentWords varchar(1024),
                        weiboKeywords varchar(1024), sentiment varchar(64), sentimentScore int(16), sentimentKeywords varchar(1024), 
                        createdTime varchar(128)) DEFAULT CHARSET=utf8;''')   
    print '新建库成功'
    
    # mongoDB数据库
    mongoConn = pymongo.Connection(host = mongodbIP, port = 27017)
    # 查询某条微博的index
    mongoCollection = mongoConn.weibo_db_cctv.weibo
    mongoCursor = mongoCollection.find().batch_size(30).skip(2495500)
    
# 读停用词
    path = os.path.abspath(os.path.dirname(sys.argv[0]))
    dicFile = open(path+'/tools/NTUSD_simplified/stopwords.txt','r')
    stopwords = dicFile.readlines()
    stopwordList = []
    stopwordList.append(' ')
    for stopword in stopwords:
        temp = stopword.strip().replace('\r\n','').decode('utf8')
        stopwordList.append(temp)
    dicFile.close() 
    
    # 存储数据
    tvseriesData = []
    tempData = []
    # 计数
    sqlcursor.execute('select count(*) from TVSeriesTable;')
    totalCount = sqlcursor.fetchall()
    totalCount = list(list(totalCount)[0])[0]
    
    count = 0
    printCount = totalCount
    
    for tvWeibo in mongoCursor:
        count+=1
        printCount+=1
    #     countIndex
        tempData.append(printCount)
    #     weiboId
        tempData.append(tvWeibo['wid'])
    #     TvSeriesName
        tempData.append(tvWeibo['keyword'])
    #     userId
        tempData.append(tvWeibo['uid'])
    #     weiboContent
        tempData.append(tvWeibo['content'])
    #     contentWords
        tempcut_out = jieba.cut(rmIrr.removeEverything(tvWeibo['content']))
        cut_out = []
        for i in tempcut_out:
            if i not in stopwordList:
                cut_out.append(i)
        tempData.append(','.join(cut_out))
    #     weiboKeywords
        temp = jieba.analyse.extract_tags(rmIrr.removeEverything(tvWeibo['content']),5)
        weiboKeywords = ','.join(temp)
        tempData.append(weiboKeywords)
    #     sentiment
        (emotionsWord,emotionsScore) = emProcess.processSentence(rmIrr.removeEverythingButEmotion(tvWeibo['content']))
        if emotionsScore>0:
            tempData.append('正面')
        elif emotionsScore==0:
            tempData.append('中立')
        else:
            tempData.append('负面')
#         sentimentScore   
        tempData.append(emotionsScore)    
    #     sentimentKeywords    
        emotionsWord = ','.join(emotionsWord)
        tempData.append(emotionsWord)
    #     createdTime
        createdTime = time.ctime(tvWeibo['time'])
        tempData.append(createdTime)
    #     执行插入
        tvseriesData.append(tuple(tempData))
        tempData = []
        if count>=100:
            sqlcursor.executemany('''insert into TVSeriesTable(countIndex, weiboId, TvSeriesName, userId, weiboContent, contentWords,
                        weiboKeywords, sentiment, sentimentScore, sentimentKeywords, createdTime) 
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',tvseriesData)
            sqlConn.commit() 
            tvseriesData = []
            count = 0
            print '插入'+str(printCount)+'个'
    sqlcursor.executemany('''insert into TVSeriesTable(countIndex, weiboId, TvSeriesName, userId, weiboContent, contentWords,
                    weiboKeywords, sentiment, sentimentScore, sentimentKeywords, createdTime) 
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',tvseriesData)
    sqlConn.commit() 
    mongoConn.close()
    sqlConn.close()

   
if __name__=='__main__':
    timeBegin = datetime.datetime.now() - datetime.timedelta(days=2)
    weiboTest = addTVSeriesTable(mongodbIP = '10.3.3.220', mysqlhostIP = '10.3.3.220', timeRangeBeginning = timeBegin, dbname = 'cctvTimer')
    




