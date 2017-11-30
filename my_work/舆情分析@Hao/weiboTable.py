#coding:UTF-8
'''
Created on 2014年3月4日

@author: hao
'''
import os, sys
from sys import path
path.append(path[0]+'/tools/')
import MySQLdb
import pymongo
import jieba.analyse
import time
import datetime
# 工具类
from remove import removeIrrelevant
reload(sys)
sys.setdefaultencoding('utf8')

rmIrr = removeIrrelevant()

def addTwitterTable(mongodbIP,mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '123456', dbname = 'cctvTimer'):
    
    sqlConn=MySQLdb.connect(host=mysqlhostIP,user=mysqlUserName,passwd=mysqlPassword,db = dbname,charset='utf8')
    sqlcursor = sqlConn.cursor()
    
    # sqlcursor.execute('''DROP TABLE IF EXISTs twitterTable;''')
    
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS twitterTable(countIndex bigint(64) primary key, weiboId bigint(64), programId int(32),
                        weiboKeywords varchar(1024), commentNum int(64), repostNum int(64), neutralNum int(64), positiveNum int(64), 
                        negativeNum int(64), commentKeywords varchar(1024), positiveSentimentKeywords varchar(1024), negativeSentimentKeywords varchar(1024), 
                        createdTime varchar(128), weiboContent varchar(1024), timestamp bigint(64), totalShown bigint(64)) DEFAULT CHARSET=utf8;''')
    #                         
    print '新建库成功'
    
    mongoConn = pymongo.Connection(host=mongodbIP, port = 27017)
    # check time
    mongoCursor = mongoConn.weibo.timestamp.find({'type':'weibo'}).limit(1)
    timeRangeBeginning = datetime.datetime.now() - datetime.timedelta(days=9999)
    for i in mongoCursor:
        timeRangeBeginning = i['time']
    newTimestamp = timeRangeBeginning
    # 查询某条微博的index
    mongoCursor = mongoConn.weibo.weibo.find({'task_time':{'$gt':timeRangeBeginning}}).sort('task_time').batch_size(30)
#     mongoCursor = mongoConn.weibo.weibo.find({'weibo_id':'3597503377427455'}).batch_size(30)
    
    # 读停用词
    path = os.path.abspath(os.path.dirname(sys.argv[0]))  
    
    dicFile = open(path+'/tools/NTUSD_simplified/stopwords.txt','r')
    stopwords = dicFile.readlines()
    stopwordList = []
    for stopword in stopwords:
        temp = stopword.strip().replace('\r\n','').decode('utf8')
        stopwordList.append(temp)
    dicFile.close() 
    
    # 路径
    path = os.path.abspath(os.path.dirname(sys.argv[0]))+'/source/lanmuid.txt'
    
    programFile = open(path,'r')
    programNames = programFile.readlines()
    count = 0
    programIdList = {}
    for line in programNames:
        count+=1
        line = line.strip()
        line = line.replace("\n","").decode('utf8')
        line = line.split(',')[0]
        
        programIdList[line] = count
    # 参数
    count = 0
    # 计数
    sqlcursor.execute('select count(*) from twitterTable;')
    totalCount = sqlcursor.fetchall()
    totalCount = list(list(totalCount)[0])[0]
    printCount = totalCount
    
    repostsData =[]
    tempData = []
    
    for weibo in mongoCursor:
#         if weibo['task_time']>timeRangeBeginning+ datetime.timedelta(days=1):
#             continue
        name = weibo['user_screen_name']
        if name not in programIdList:
            continue
        count+=1
        printCount+=1
        #主键
        countIndex = printCount
        tempData.append(countIndex)
        # 微博id
        weiboId = weibo['weibo_id']
        tempData.append(weiboId)
#         programId
        tempData.append(programIdList[name])
        # 微博内容关键词
        temp = jieba.analyse.extract_tags(rmIrr.removeEverything(weibo['weibo_text']),5)
        temp = [word for word in temp if not word.encode('utf8').isdigit() and not word.encode('utf8').isalpha()]
        
        tempData.append(','.join(temp))
        #查询mysql回复
    #     回复数
        sqlcursor.execute("select * from commentTable where weiboId="+str(weiboId))
        bufferTemp = sqlcursor.fetchall()
        commentNum = len(bufferTemp)
        tempData.append(commentNum)
    #     转发数
        sqlcursor.execute("select * from repostsTable where weiboId="+str(weiboId))
        bufferTemp = sqlcursor.fetchall()
        repostNum = len(bufferTemp)
        tempData.append(repostNum)
    #     中立数
        sqlcursor.execute("select * from commentTable where weiboId="+str(weiboId)+ " AND sentiment = '中立'")
        bufferTemp = sqlcursor.fetchall()
        neutralNum = len(bufferTemp)
        tempData.append(neutralNum)
    #     正面数
        sqlcursor.execute("select * from commentTable where weiboId="+str(weiboId)+ " AND sentiment = '正面'")
        bufferTemp = sqlcursor.fetchall()
        positiveNum = len(bufferTemp)
        tempData.append(positiveNum)
    #     负面数
        sqlcursor.execute("select * from commentTable where weiboId="+str(weiboId)+ " AND sentiment = '负面'")
        bufferTemp = sqlcursor.fetchall()
        negativeNum = len(bufferTemp)
        tempData.append(negativeNum)
    #     回复关键词
        sqlcursor.execute('select contentKeywords from commentTable where weiboId='+str(weiboId))
        bufferTemp = sqlcursor.fetchall()
        words = {}
        for wordList in bufferTemp:
            wordList = wordList[0].split(',')
            for word in wordList:
                if word in stopwordList:
                    continue
                if word != '' and word != ' ':
                    if word in words:
                        words[word] += 1
                    else:
                        words[word] = 1
        commentKeywordsList = sorted(words.items(), key=lambda d: d[1], reverse = True)
        commentKeywords = ""
        for word in commentKeywordsList[:20]:
            commentKeywords += word[0]
            commentKeywords += ','
        commentKeywords = commentKeywords[:len(commentKeywords)-1]
        contentList = commentKeywords.split(',')
        temp = [word for word in contentList if word not in stopwordList and not word.encode('utf8').isdigit() and not word.encode('utf8').isalpha()]
        
        tempData.append(','.join(temp))
        
    #     正面情感句
        sqlcursor.execute('select * from commentTable where weiboId='+str(weiboId)+ " AND sentiment = '正面'")
        bufferTemp = sqlcursor.fetchall()
        sentences = {}
        fansName = []
        for comments in bufferTemp:
            if comments[9] not in fansName:
                fansName.append(comments[9])
                sentences[comments[4]] = int(comments[8])
            
        outSentence = sorted(sentences.items(), key=lambda d: d[1], reverse = True)
        outputSentence = ''
        for tempSentence in outSentence[:3]:
            outputSentence += tempSentence[0]
            outputSentence += '<br/>'
        
        tempData.append(outputSentence[:-5])
        
        #     负面情感关键词
        sqlcursor.execute('select * from commentTable where weiboId='+str(weiboId)+ " AND sentiment = '负面'")
        bufferTemp = sqlcursor.fetchall()
        sentences = {}
        fansName = []
        for comments in bufferTemp:
            if comments[9] not in fansName:
                fansName.append(comments[9])
                sentences[comments[4]] = int(comments[8])
            
        outSentence = sorted(sentences.items(), key=lambda d: d[1], reverse = True)
        outputSentence = ''
        for tempSentence in outSentence[:3]:
            outputSentence += tempSentence[0]
            outputSentence += '<br/>'
        
        tempData.append(outputSentence[:-5])
        
    #     创建时间
        hh = time.strptime(str(weibo['weibo_created_at']), '%Y-%m-%d %H:%M:%S')
        weiboTime = time.strftime("%a %b %d %H:%M:%S %Y", hh)
        tempData.append(weiboTime)
#         原始微博内容
        tempData.append(weibo['weibo_text'])
#             timestamp
        tt = time.mktime(time.strptime(str(weibo['weibo_created_at']), '%Y-%m-%d %H:%M:%S'))
        tempData.append(int(tt))
        
        newTimestamp = weibo['task_time']
#         总曝光
        sqlcursor.execute("select totalShownCount from repostsTable where weiboId="+str(weiboId))
        bufferTemp = sqlcursor.fetchall()
        shownCount = weibo['followers_count']
        for i in bufferTemp:
            shownCount+=i[0]
        tempData.append(shownCount)
#          综合
        repostsData.append(tuple(tempData))
        tempData = []
#         插入数据
        if count>=1:
            sqlcursor.executemany('''insert into twitterTable(countIndex, weiboId, programId, weiboKeywords, commentNum, repostNum, neutralNum, 
                                positiveNum, negativeNum, commentKeywords, positiveSentimentKeywords, negativeSentimentKeywords, createdTime, weiboContent, timestamp
                                , totalShown) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',repostsData)
            sqlConn.commit() 
            repostsData = []
            count = 0
            print '插入'+str(printCount)+'个'
    
    sqlcursor.executemany('''insert into twitterTable(countIndex, weiboId, programId, weiboKeywords, commentNum, repostNum, neutralNum, 
                                    positiveNum, negativeNum, commentKeywords, positiveSentimentKeywords, negativeSentimentKeywords, createdTime, weiboContent, timestamp 
                                    , totalShown) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',repostsData)
    sqlConn.commit() 
    mongoConn.close()
    mongoConn.weibo.timestamp.update({'type':'weibo'},{'$set':{'time':newTimestamp}})
    sqlConn.close()
    
if __name__=='__main__':
#     timeBegin = datetime.datetime.now() - datetime.timedelta(days=3)
    weiboTest = addTwitterTable(mongodbIP = '10.3.3.220', mysqlhostIP = '10.3.3.220', dbname = 'cctvTimer')
    
    
    
    
    
    
