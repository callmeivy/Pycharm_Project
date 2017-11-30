#coding:UTF-8
'''
Created on 2014年3月24日

@author: hao
'''
import sys,os
from sys import path
path.append('tools/')
path.append(path[0]+'/tools')
import MySQLdb
import pymongo
from emtionProcess import emotionProcess
# 工具类
from remove import removeIrrelevant
from spammerDetection import spammerdetect
import jieba
import datetime
import time

reload(sys)
sys.setdefaultencoding('utf8')

def addCommentTable(mongodbIP,mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '123456', dbname = 'cctvTimer'):

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
    
    # 分词
    jieba.initialize()
    
    # 连接数据库
    sqlConn=MySQLdb.connect(host=mysqlhostIP,user=mysqlUserName,passwd=mysqlPassword,db = dbname,charset='utf8')
    sqlcursor = sqlConn.cursor()
    # 删库
    # sqlcursor.execute('''DROP TABLE IF EXISTs commentTable;''')
    # print '删库成功'
    
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS commentTable(countIndex bigint(64) primary key, commentId bigint(64), weiboId bigint(64), userId bigint(64), comment varchar(1024), 
                    sentimentKeywords varchar(128), contentKeywords varchar(1024), sentiment varchar(16), sentimentScore int(16), userName varchar(64), userSex varchar(16),
                    userLocation varchar(64), userFollowerCount int(64), userFriendCount int(64), userStatusCount int(64), userType varchar(32), 
                    spammerJudge varchar(16), replyTime varchar(128)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'
    
    # 连接mongoDB数据库
    mongoConn = pymongo.Connection(host = mongodbIP, port = 27017)
    # check time
    mongoCursor = mongoConn.weibo.timestamp.find({'type':'comment'}).limit(1)
    timeRangeBeginning = datetime.datetime.now() - datetime.timedelta(days=9999)
#     print timeRangeBeginning
#     a=dict()
#     a['type']='comment'
#     a['time']=timeRangeBeginning
#     mongoConn.weibo.timestamp.insert(a)
#     a=dict()
#     a['type']='repost'
#     a['time']=timeRangeBeginning
#     mongoConn.weibo.timestamp.insert(a)
#     a=dict()
#     a['type']='weibo'
#     a['time']=timeRangeBeginning
#     mongoConn.weibo.timestamp.insert(a)
    for i in mongoCursor:
        timeRangeBeginning = i['time']
    newTimestamp = timeRangeBeginning
    # 查询某条微博的回复
    mongoCursor = mongoConn.weibo.comment.find({'task_time':{'$gt':timeRangeBeginning}}).sort('task_time').batch_size(30)
    print '查询mongoDB成功'
    # 计数
    sqlcursor.execute('select count(*) from commentTable;')
    totalCount = sqlcursor.fetchall()
    totalCount = list(list(totalCount)[0])[0]
    
    # 存储评论数据
    commentsData = []
    tempData = []
    # 处理情感
    emProcess = emotionProcess()
    rmIrr = removeIrrelevant()
    spamDet = spammerdetect()
    
    emotionsWord = []
    emotionsScore = 0
    count = 0
    printCount = totalCount
    # 处理每一条
    # try:
    for comment in mongoCursor:
#        if comment['task_time']>timeRangeBeginning+ datetime.timedelta(days=1):
#            continue
        count += 1
        printCount+=1
        tempData.append(printCount)
    #     评论id
        tempData.append(comment['comment_id'])
    #     微博id
        tempData.append(comment['weibo_id'])
    #     用户id
        tempData.append(comment['comment_user_id'])
    #     评论内容
        tempData.append(comment['comment_text'])
    #     情感关键词
        (emotionsWord,emotionsScore) = emProcess.processSentence(rmIrr.removeEverythingButEmotion(comment['comment_text']))
        emotionsWord = ','.join(emotionsWord)
        tempData.append(emotionsWord)
    #     print comment['mid']
    #     print comment['status']['mid']
    #     print comment['user']['id']
    #     print comment['text']
        # 内容分词
        tempcut_out = jieba.cut(rmIrr.removeEverything(comment['comment_text']))
        cut_out = []
        for i in tempcut_out:
            if i not in stopwordList:
                cut_out.append(i)
        tempData.append(','.join(cut_out))
    #     倾向性判断
        if emotionsScore>0:
            tempData.append('正面')
        elif emotionsScore==0:
            tempData.append('中立')
        else:
            tempData.append('负面')
#         sentimentScore
        tempData.append(emotionsScore)
    #     用户昵称
        tempData.append(comment['comment_user_name'])
    #     用户性别
        tempData.append(comment['comment_gender'])
    #     用户地域信息
        tempData.append(comment['comment_location'])
    #     用户粉丝数
        tempData.append(comment['comment_followers_count'])
    #     用户关注数
        tempData.append(comment['comment_friends_count'])
    #     用户微博数
        tempData.append(comment['comment_statuses_count'])
    #     用户类型
        if (comment['comment_verified_type']==-1):
            tempData.append('普通用户')
        elif (comment['comment_verified_type']==220) or (comment['comment_verified_type']==200):
            tempData.append('微博达人')
        elif (comment['comment_verified_type']==0):
            tempData.append('个人认证')
        else:
            tempData.append('企业认证')
    # 是否水军
        userInfo = {}
        userInfo['statuses_count'] = comment['comment_statuses_count']
        userInfo['followers_count'] = comment['comment_followers_count']
        userInfo['friends_count'] = comment['comment_friends_count']
        userInfo['bi_followers_count'] = comment['comment_bi_followers_count']
        userInfo['domain'] = comment['comment_user_domain']
        userInfo['url'] = comment['comment_url']
        userInfo['description'] = comment['comment_description']
        userInfo['location'] = comment['comment_location']
        userInfo['verified'] = comment['comment_verified']
        userInfo['verified_type'] = comment['comment_verified_type']
        
        newTimestamp = comment['task_time']
        
        spamScore = spamDet.detectSpammer(userInfo)
        if spamScore>0:
            tempData.append("正常")
        else:
            tempData.append("水军")
    #     回复时间
        hh = time.strptime(str(comment['comment_created_at']), '%Y-%m-%d %H:%M:%S')
        commentTime = time.strftime("%a %b %d %H:%M:%S %Y", hh) 
        
        tempData.append(commentTime)
    #     转换为元组
        commentsData.append(tuple(tempData))
        tempData = []
        if count>=10:
            sqlcursor.executemany('''insert into commentTable(countIndex, commentId, weiboId, userId, comment, sentimentKeywords, contentKeywords, sentiment, sentimentScore, userName, 
                        userSex,userLocation, userFollowerCount, userFriendCount, userStatusCount, userType,spammerJudge, replyTime) 
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',commentsData)
            sqlConn.commit() 
            commentsData = []
            count = 0
            print '插入'+str(printCount)+'个'
    # # except:
    # #      print tempData
    sqlcursor.executemany('''insert into commentTable(countIndex, commentId, weiboId, userId, comment, sentimentKeywords, contentKeywords, sentiment, sentimentScore, userName, userSex,
                        userLocation, userFollowerCount, userFriendCount, userStatusCount, userType,spammerJudge, replyTime) 
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',commentsData)  
    sqlConn.commit()
    sqlConn.close()
    mongoConn.weibo.timestamp.update({'type':'comment'},{'$set':{'time':newTimestamp}})
    mongoConn.close()
    
if __name__=='__main__':
#     timeBegin = datetime.datetime.now() - datetime.timedelta(days=1)
#     2014-03-25 14:28:34.827378
    commentTest = addCommentTable(mongodbIP = '10.3.3.220', mysqlhostIP = '10.3.3.220', dbname = 'cctvTimer')
#     print CommentTests
    
    
