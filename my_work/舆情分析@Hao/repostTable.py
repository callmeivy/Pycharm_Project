#coding:UTF-8
'''
Created on 2014年3月4日

@author: hao
'''
import sys
import time
from sys import path
path.append(path[0]+'/tools/')
import MySQLdb
import pymongo
import datetime
# from snownlp import SnowNLP
# 工具类
from spammerDetection import spammerdetect
# from emtionProcess import *

reload(sys)
sys.setdefaultencoding('utf8')

def addRepostsTable(mongodbIP,mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '123456', dbname = 'cctvTimer'):
    # 连接数据库
    sqlConn=MySQLdb.connect(host=mysqlhostIP,user=mysqlUserName,passwd=mysqlPassword,db = dbname,charset='utf8')
    sqlcursor = sqlConn.cursor()
    # sqlcursor.execute('''DROP TABLE IF EXISTs repostsTable;''')
    # print '删库成功'
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS repostsTable(countIndex bigint(64) primary key, repostId bigint(64), weiboId bigint(64), userId bigint(64), userName varchar(64),
                        beFollwedName varchar(64), userFollowerCount int(64), userFriendCount int(64), userStatusCount int(64),repost varchar(1024), reRepostCount int(64), repostTime varchar(128),
                        totalShownCount int(64), userType varchar(32), spammerJudge varchar(16)) DEFAULT CHARSET=utf8;''')
                           
    print '新建库成功'
    # 连接mongoDB数据库
    mongoConn = pymongo.Connection(host = mongodbIP, port = 27017)
        
    # check time
    mongoCursor = mongoConn.weibo.timestamp.find({'type':'repost'}).limit(1)
    timeRangeBeginning = datetime.datetime.now() - datetime.timedelta(days=9999)
    for i in mongoCursor:
        timeRangeBeginning = i['time']
    newTimestamp = timeRangeBeginning
    # 查询某条微博的转发   
    mongoCursor = mongoConn.weibo.repost.find({'task_time':{'$gt':timeRangeBeginning}}).sort('task_time').batch_size(30)
    print '查询mongoDB成功'
    
    # 计数
    sqlcursor.execute('select count(*) from repostsTable;')
    totalCount = sqlcursor.fetchall()
    totalCount = list(list(totalCount)[0])[0]
    
    # 存储评论数据
    repostsData = []
    tempData = []
    # 处理
    spamDet = spammerdetect()
    count = 0
    printCount = totalCount
    # 缓冲关注者数据
    weiboId = 0
    followerContributes = {}
    
    # 处理每一条
    # try:
    for repost in mongoCursor:
#        if repost['task_time']>timeRangeBeginning+ datetime.timedelta(days=1):
#            continue
        count += 1
        printCount+=1
    #    recursiveid    
        tempData.append(printCount)
    #     repostid
        tempData.append(repost['repost_id'])
    #     微博id
        tempData.append(repost['weibo_id'])
        if repost['weibo_id']!=weiboId:
            weiboId = repost['weibo_id']
            followerContributes.clear()
    #     用户id
        tempData.append(repost['repost_user_id'])
    #     用户昵称
        tempData.append(repost['repost_user_name'])
        
    #     被转发者
        doc = repost['repost_text']
            
        if '//@' in doc:
            index1 = doc.find('//@')
            tempPart = doc[index1+3:]
            index2 = tempPart.find(':')
    #         报错
            if index2<=0:    
                index2 = tempPart.find('：')
                if index2<=0:
#                     print "!!!!!!!!!!!!error"
#                     print "只有转发符号没有冒号"
#                     print repost['repost_text']
                    if repost['user_screen_name'] in followerContributes:
                        if repost['repost_user_screen_name'] in followerContributes:
                            followerContributes[repost['user_screen_name']] += followerContributes[repost['repost_user_screen_name']]
                        followerContributes[repost['user_screen_name']] += repost['repost_followers_count']
                    else:
                        followerContributes[repost['user_screen_name']] = repost['repost_followers_count']
                        if repost['repost_user_screen_name'] in followerContributes:
                            followerContributes[repost['user_screen_name']] += followerContributes[repost['repost_user_screen_name']]
                    tempData.append(repost['user_screen_name'])
            #             转发者是tempPart[:index2]
                else:
                    if tempPart[:index2] in followerContributes:
                        if repost['repost_user_screen_name'] in followerContributes:
                            followerContributes[tempPart[:index2]] += followerContributes[repost['repost_user_screen_name']]
                        followerContributes[tempPart[:index2]] += repost['repost_followers_count']                
                    else:
                        followerContributes[tempPart[:index2]] = repost['repost_followers_count']
                        if repost['repost_user_screen_name'] in followerContributes:
                            followerContributes[tempPart[:index2]] += followerContributes[repost['repost_user_screen_name']]
                    tempData.append(tempPart[:index2])
    #             转发者是tempPart[:index2]
            else:
                if tempPart[:index2] in followerContributes:
                    if repost['repost_user_screen_name'] in followerContributes:
                        followerContributes[tempPart[:index2]] += followerContributes[repost['repost_user_screen_name']]
                    followerContributes[tempPart[:index2]] += repost['repost_followers_count']                
                else:
                    followerContributes[tempPart[:index2]] = repost['repost_followers_count']
                    if repost['repost_user_screen_name'] in followerContributes:
                        followerContributes[tempPart[:index2]] += followerContributes[repost['repost_user_screen_name']]
                tempData.append(tempPart[:index2])
    #     转发者是repost['retweeted_status']['user']['name']
        else:
            if repost['user_screen_name'] in followerContributes:
                if repost['repost_user_screen_name'] in followerContributes:
                    followerContributes[repost['user_screen_name']] += followerContributes[repost['repost_user_screen_name']]
                followerContributes[repost['user_screen_name']] += repost['repost_followers_count']
            else:
                followerContributes[repost['user_screen_name']] = repost['repost_followers_count']
                if repost['repost_user_screen_name'] in followerContributes:
                    followerContributes[repost['user_screen_name']] += followerContributes[repost['repost_user_screen_name']]
            tempData.append(repost['user_screen_name'])
                    
    #     用户粉丝数
        tempData.append(repost['repost_followers_count'])
    #     用户关注数
        tempData.append(repost['repost_friends_count'])
    #     用户微博数
        tempData.append(repost['repost_statuses_count'])
    #     转发内容
        tempData.append(repost['repost_text'])
    #     被转发数
        tempData.append(repost['repost_repost_count'])
    #     转发时间
        hh = time.strptime(str(repost['repost_created_at']), '%Y-%m-%d %H:%M:%S')
        repostTime = time.strftime("%a %b %d %H:%M:%S %Y", hh) 
        tempData.append(repostTime)
    #     总曝光量
        if repost['repost_user_screen_name'] in followerContributes:
            tempTotal = repost['repost_followers_count']+followerContributes[repost['repost_user_screen_name']]
        else:
            tempTotal = repost['repost_followers_count']
        tempData.append(tempTotal)
    #     用户类型
        if (repost['repost_verified_type']==-1):
            tempData.append('普通用户')
        elif (repost['repost_verified_type']==220) or (repost['repost_verified_type']==200):
            tempData.append('微博达人')
        elif (repost['repost_verified_type']==0):
            tempData.append('个人认证')
        else:
            tempData.append('企业认证')
    #     是否水军
        userInfo = {}
        userInfo['statuses_count'] = repost['repost_statuses_count']
        userInfo['followers_count'] = repost['repost_followers_count']
        userInfo['friends_count'] = repost['repost_friends_count']
        userInfo['bi_followers_count'] = repost['repost_bi_followers_count']
        userInfo['domain'] = repost['repost_user_domain']
        userInfo['url'] = repost['repost_url']
        userInfo['description'] = repost['repost_description']
        userInfo['location'] = repost['repost_location']
        userInfo['verified'] = repost['repost_verified']
        userInfo['verified_type'] = repost['repost_verified_type']
        spamScore = spamDet.detectSpammer(userInfo)
        if spamScore>0:
            tempData.append("正常")
        else:
            tempData.append("水军")
    #     转换为元组
        repostsData.append(tuple(tempData))
        newTimestamp = repost['task_time']
        tempData = []
        if count>=100:
            sqlcursor.executemany('''insert into repostsTable(countIndex, repostId, weiboId, userId, userName, beFollwedName, userFollowerCount, userFriendCount, 
            userStatusCount, repost, reRepostCount, repostTime, totalShownCount, userType, spammerJudge) 
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',repostsData)
            sqlConn.commit() 
            repostsData = []
            count = 0
            print '插入'+str(printCount)+'个'
    # except:
    #      print tempData
    sqlcursor.executemany('''insert into repostsTable(countIndex, repostId, weiboId, userId, userName, beFollwedName, userFollowerCount, userFriendCount, 
                userStatusCount, repost, reRepostCount, repostTime, totalShownCount, userType, spammerJudge) 
                            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',repostsData)
    sqlConn.commit()
    sqlConn.close()
    mongoConn.weibo.timestamp.update({'type':'repost'},{'$set':{'time':newTimestamp}})
    mongoConn.close()
    
        
if __name__=='__main__':
    
    todayDate = time.strptime(str(datetime.date.today() - datetime.timedelta(days=1)), '%Y-%m-%d')
#     print todayDate[0]
#     print todayDate[1]
#     print todayDate[2]
    timeBegin = datetime.datetime(todayDate[0], todayDate[1], todayDate[2], 0, 0, 0, 0)
    weiboTest = addRepostsTable(mongodbIP = '10.3.3.220', mysqlhostIP = '10.3.3.220', dbname = 'cctvTimer')
    
    
    
    
    
