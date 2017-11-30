#coding:UTF-8
'''
Created on Jan 27, 2014

@author: administrator
'''
import pymongo
import time
import datetime
import MySQLdb
import os,sys
reload(sys)
sys.setdefaultencoding('utf8')

def mysqlImportTodayFans(mongodbIP,mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '123456', dbname = 'cctvTimer'):
    conn = pymongo.Connection(host = mongodbIP, port=27017);
    db = conn.cctv
    dbCollection = db.weibo
    # 读入栏目名
    lanmu=[]
    path = os.path.abspath(os.path.dirname(sys.argv[0]))+'/source/lanmuid.txt'
    
    programFile = open(path,'r')
    programNames = programFile.readlines()
    count = 0
    for line in programNames:
        count+=1
        line = line.strip()
        line = line.replace("\n","").decode('utf8')
        lanmu.append(line.split(',')[0])
    # 调用mysql
    sqlconn=MySQLdb.connect(host=mysqlhostIP,user=mysqlUserName,passwd=mysqlPassword,db = dbname,charset='utf8')
    sqlcursor = sqlconn.cursor()
    
    #根据栏目名称插入
    lanmuNum = len(lanmu)
    for i in range(lanmuNum):
        lanmuName = lanmu[i]
        fensiNum = []
        todayData = []
    # 微博index从100000000起  （100000000，200000000...
    # 得到mysql中最后时刻的粉丝数
        sqlcursor.execute("select max(weiboindex) from fans where lanmu='"+lanmuName+"'")
        bufferTemp = sqlcursor.fetchall()
        weiboindex = bufferTemp[0][0]
        sqlcursor.execute("select * from fans where weiboindex="+str(weiboindex))
        bufferTemp = sqlcursor.fetchall() 
        fansNumberUntilYesterday = bufferTemp[0][2]
#         print "fansNumberUntilYesterday"+str(fansNumberUntilYesterday)
#         print "weiboindex"+str(weiboindex)
    #     weiboindex = 0
    #     fansNumberUntilYesterday=100
        
    # 提取前半个小时的微博
        todayStartTime = time.mktime(time.localtime())#-time.altzone
        yesterdayStartTime = todayStartTime - 1800#-time.altzone # 3600*24-time.altzone
#         yesterdayStartTime-=86400*16
#         todayStartTime-=86400*16
        cursor = dbCollection.find({"user.name":lanmuName,"created_at":{'$gte':yesterdayStartTime,'$lt':todayStartTime}})
        for doc in cursor:
            fensiNum.append((doc['user']['followers_count'], doc['created_at']))
#             fensiNum[doc['user']['followers_count']] = doc['created_at']
        fensiNum.sort(key=lambda x:x[1])
#         yesterdayWeibo = sorted(fensiNum.items(), lambda x,y:cmp(x[1], y[1]))
#         print fensiNum
        numberOfWeibo = len(fensiNum)
        if numberOfWeibo==0:
            tempStartTime = yesterdayStartTime
            for j in range(int((todayStartTime - yesterdayStartTime)/60)):
                weiboindex+=1            
                todayData.append((weiboindex, lanmuName.encode('utf-8'), fansNumberUntilYesterday, time.ctime(tempStartTime)))
                tempStartTime+=60
        else:
            tempStartTime = yesterdayStartTime
            tempFanNum = fansNumberUntilYesterday
            for j in range(numberOfWeibo):
                tempTime = fensiNum[j][1]
                tempTime -= (tempTime - tempStartTime)%60
                tempMininus = (tempTime - tempStartTime)/60
                for t in range(int(tempMininus)):
                    weiboindex+=1
                    todayData.append((weiboindex, lanmuName.encode('utf-8'), tempFanNum, time.ctime(tempStartTime)))
                    tempStartTime+=60
                tempStartTime = tempTime
                tempFanNum = fensiNum[j][0]
            for j in range(int(todayStartTime - tempStartTime)/60):
                weiboindex+=1
                todayData.append((weiboindex, lanmuName.encode('utf-8'), tempFanNum, time.ctime(tempStartTime)))
                tempStartTime+=60
                
        sqlcursor.executemany("insert into fans(weiboindex, lanmu, fannumber, recordtime) values (%s, %s,%s, %s)",todayData)  
        sqlconn.commit()
    
    conn.close()
    sqlconn.close()
    
def mysqlImportTodayWeibos(mongodbIP,mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '123456', dbname = 'cctvTimer'):
    conn = pymongo.Connection(host = mongodbIP, port=27017);
    db = conn.cctv
    dbCollection = db.weibo
    # 读入栏目名
    fp = open('/usr/pythonApp/staticsWeibo/source/38lanmu.txt','r')
    lanmu=[]
    for doc in fp:
        doc = doc.strip()
        doc=doc.replace("\n","")
        lanmu.append(doc)
    fp.close()
    # 调用mysql
    sqlconn=MySQLdb.connect(host=mysqlhostIP,user=mysqlUserName,passwd=mysqlPassword,db = dbname, charset='utf8')
    sqlcursor = sqlconn.cursor()
    
    #根据栏目名称插入
    lanmuNum = len(lanmu)
    for i in range(lanmuNum):
        lanmuName = lanmu[i]
        weiboNum = []
        todayData = []
    # 微博index从100000000起  （100000000，200000000...
    # 得到mysql中最后时刻的粉丝数
        sqlcursor.execute("select max(weiboindex) from weibo where lanmu='"+lanmuName+"'")
        bufferTemp = sqlcursor.fetchall()
        weiboindex = bufferTemp[0][0]
        sqlcursor.execute("select * from weibo where weiboindex="+str(weiboindex))
        bufferTemp = sqlcursor.fetchall() 
        weiboNumberUntilYesterday = bufferTemp[0][3]
        
    # 提取前一天的微博
        todayStartTime = time.mktime(time.localtime())#-time.altzone
        yesterdayStartTime = todayStartTime - 1800#-time.altzone # 3600*24-time.altzone
#         yesterdayStartTime-=86400*16
#         todayStartTime-=86400*16
        cursor = dbCollection.find({"user.name":lanmuName,"created_at":{'$gte':yesterdayStartTime,'$lt':todayStartTime}})
        for doc in cursor:
            weiboNum.append((doc['user']['statuses_count'], doc['created_at']))
        weiboNum.sort(key=lambda x:x[1])
        numberOfWeibo = len(weiboNum)
        if numberOfWeibo==0:
            tempStartTime = yesterdayStartTime   
            tempWeiboNum = weiboNumberUntilYesterday         
            for j in range(int((todayStartTime - yesterdayStartTime)/60)):
                weiboindex+=1            
                todayData.append((weiboindex, lanmuName.encode('utf-8'), 0, tempWeiboNum, time.ctime(tempStartTime)))
                tempStartTime+=60
        else:
            tempStartTime = yesterdayStartTime
            tempWeiboNum = weiboNumberUntilYesterday
            for j in range(numberOfWeibo):
                tempTime = weiboNum[j][1]
                tempTimeshift = tempTime - tempStartTime
                if tempTimeshift<60:
                    tempInfo = todayData.pop()
                    instantNumber = tempInfo[2]+1
                    instantTime = tempInfo[4]
                    todayData.append((weiboindex, lanmuName.encode('utf-8'), instantNumber, weiboNum[j][0], instantTime))
                else:
                    tempTime -= tempTimeshift%60
                    tempMininus = (tempTime - tempStartTime)/60
                    for t in range(int(tempMininus)):
                        if t==int(tempMininus)-1:
                            weiboindex+=1
                            todayData.append((weiboindex, lanmuName.encode('utf-8'), 1, weiboNum[j][0], time.ctime(tempStartTime)))
                            tempStartTime+=60
                        else:
                            weiboindex+=1
                            todayData.append((weiboindex, lanmuName.encode('utf-8'), 0, tempWeiboNum, time.ctime(tempStartTime)))
                            tempStartTime+=60
                    tempStartTime = tempTime
                tempWeiboNum = weiboNum[j][0]
            for j in range(int(todayStartTime - tempStartTime)/60):
                weiboindex+=1
                todayData.append((weiboindex, lanmuName.encode('utf-8'), 0, tempWeiboNum, time.ctime(tempStartTime)))
                tempStartTime+=60
                
        sqlcursor.executemany("insert into weibo(weiboindex, lanmu, postweibo, weibonumber, recordtime) values (%s, %s, %s, %s, %s)",todayData)   
        sqlconn.commit()
    
    conn.close()
    sqlconn.close()    


if __name__=="__main__":
    a=time.time()
    mysqlImportTodayFans(mongodbIP = '10.3.3.220', mysqlhostIP = '10.3.3.220')
    mysqlImportTodayWeibos(mongodbIP = '10.3.3.220', mysqlhostIP = '10.3.3.220')
    print time.time()-a
