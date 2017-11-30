#coding:UTF-8
'''
Created on 2014年3月18日

@author: hao
'''
import os, sys
from sys import path
import MySQLdb
import jieba.analyse
import time, datetime
# 工具类
reload(sys)
sys.setdefaultencoding('utf8')

def getTrendWord(mysqlhostIP,timeRangeBeginning, timeRangeEnding, mysqlUserName = 'root', mysqlPassword = '123456', dbname = 'cctvTimer'):
    sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()
#     建库
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS trendWordTable(countIndex bigint(64) primary key, weiboKeywords varchar(1024), 
    startTimestamp bigint(64), startTime varchar(32), endTimeStamp bigint(64), endTime varchar(32)) DEFAULT CHARSET=utf8;''')                    
    print '新建库成功'
    
#     计算时间
#     startTime = 0
#     endTime = 0
    startTime = timeRangeBeginning
    endTime = timeRangeEnding
#     try:
#         startTime = time.strptime(timeRangeBeginning, '%Y-%m-%d')
#     except:
#         startTime = time.strptime('2013-12-20', '%Y-%m-%d')
#     
#     try:
#         endTime = time.strptime(timeRangeEnding, '%Y-%m-%d')
#     except:
#         endTime = time.strptime('2014-02-01', '%Y-%m-%d')
    startTimeStamp = int(time.mktime(startTime))    
    endTimeStamp = int(time.mktime(endTime))
#     计算总天数
    dayCount = (endTimeStamp - startTimeStamp)/86400
    # 计数
    sqlcursor.execute('select count(*) from trendWordTable;')
    totalCount = sqlcursor.fetchall()
    totalCount = list(list(totalCount)[0])[0]
    
    printCount = totalCount
    for i in range(int(dayCount)):
#         查询mysql
        weiboKeyWord = {}
        trendData = []
        printCount += 1
        sqlcursor.execute("select * from twitterTable where timestamp>= "+ str(startTimeStamp)+" and timestamp< "+ str(startTimeStamp+86400)+";")
        bufferTemp = sqlcursor.fetchall()
        certainDayWeiboCount = len(bufferTemp)
#         主键
        trendData.append(printCount)
#         关键词
        if certainDayWeiboCount == 0:
            trendData.append('')
        else:
            for i in bufferTemp:
                wordList = i[3].split(',')
                for word in wordList:
                    if word in weiboKeyWord:
                        weiboKeyWord[word] += 1
                    else:
                        weiboKeyWord[word] = 1
            output = sorted(weiboKeyWord.items(), key=lambda d: d[1], reverse = True)
    #         提取前五个
            words = []
            for keyword in output[:5]:
                words.append(keyword[0])
            trendData.append(','.join(words))
        #     startTimeStamp
        trendData.append(startTimeStamp)
        #     startTime
        trendData.append(time.ctime(startTimeStamp))
        
        startTimeStamp+=86400
#         endTimeStamp
        trendData.append(startTimeStamp)
        #     endTime
        trendData.append(time.ctime(startTimeStamp))
        print trendData
        sqlcursor.execute('''insert into trendWordTable(countIndex, weiboKeywords, startTimestamp, startTime, endTimeStamp, endTime) 
                            values (%s, %s, %s, %s, %s, %s)''',tuple(trendData))
        sqlConn.commit() 
        

if __name__=='__main__':
    previousDate = time.strptime(str(datetime.date.today() - datetime.timedelta(days=7)), '%Y-%m-%d')
    timeBegin = datetime.datetime(previousDate[0], previousDate[1], previousDate[2], 0, 0, 0, 0)
    
    nowDate = time.strptime(str(datetime.date.today()), '%Y-%m-%d')
    endTime = time.strftime('%Y%m%d%H%M%S', nowDate)
    
    mediaCommentTime = time.strftime("%Y%m%d%H%M%S", previousDate)
    getTrendWord(mysqlhostIP = '10.3.3.220', timeRangeBeginning = previousDate, timeRangeEnding = nowDate)



