#coding: UTF-8
'''
Created on Jan 16, 2014
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!改成每分钟发几条微博
@author: administrator
'''
import MySQLdb
import time
import os,sys
import pymongo

reload(sys)
sys.setdefaultencoding('utf8')
    
def startMysqlImportFans(sinceDate, endDate):
    # 调用mysql
    sqlconn=MySQLdb.connect(host='localhost',user='root',passwd='123456',db = 'cctv',charset="utf8")
    sqlcursor = sqlconn.cursor()
    sqlcursor.execute("CREATE TABLE IF NOT EXISTS fans(weiboindex bigint(64) primary key, lanmu varchar(128), fannumber int(4), recordtime varchar(128)) DEFAULT CHARSET=utf8;")
    
    conn = pymongo.Connection('localhost', 27017);
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
    
    # 微博index从100000000起  （100000000，200000000...）
    #根据栏目名称插入
    lanmuNum = len(lanmu)
    for i in range(lanmuNum):
        lanmuName = lanmu[i]
        fensiNum = []
        todayData = []
        weiboindex = (i+1)*100000000
        fansNumberUntilYesterday = 0
        
    # 提取所有的微博
        todayStartTime = time.mktime(time.strptime(endDate,"%Y-%m-%d"))#-time.altzone        
        yesterdayStartTime = time.mktime(time.strptime(sinceDate,"%Y-%m-%d"))
        
        cursor = dbCollection.find({"user.name":lanmuName,"created_at":{'$gte':yesterdayStartTime,'$lt':todayStartTime}})
        for doc in cursor:
            fensiNum.append((doc['user']['followers_count'], doc['created_at']))
#             fensiNum[doc['user']['followers_count']] = doc['created_at']
        fensiNum.sort(key=lambda x:x[1])
#         yesterdayWeibo = sorted(fensiNum.items(), lambda x,y:cmp(x[1], y[1]))
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
    
def startMysqlImportWeibos(sinceDate, endDate):
    # 调用mysql
    sqlconn=MySQLdb.connect(host='localhost',user='root',passwd='123456',db = 'cctv',charset = 'utf8')
    sqlcursor = sqlconn.cursor()
    sqlcursor.execute("CREATE TABLE IF NOT EXISTS weibo(weiboindex bigint(64) primary key, lanmu varchar(128), postweibo int(4), weibonumber int(4), recordtime varchar(128)) DEFAULT CHARSET=utf8;")
    
    conn = pymongo.Connection('localhost', 27017);
    db = conn.cctv
    dbCollection = db.weibo
    # 读入栏目名
#    fp = open(str(os.path.dirname(sys.argv[0]))+'/source/38lanmu.txt','r')
    fp = open('/usr/pythonApp/staticsWeibo/source/38lanmu.txt','r')

    lanmu=[]
    for doc in fp:
        doc = doc.strip()
        doc=doc.replace("\n","")
        lanmu.append(doc)
    fp.close()
    
    # 微博index从100000000起  （100000000，200000000...）
    #根据栏目名称插入
    lanmuNum = len(lanmu)
    for i in range(lanmuNum):
        lanmuName = lanmu[i]
        weiboNum = []
        todayData = []
        weiboindex = (i+1)*100000000
        weiboNumberUntilYesterday = 0
        
        todayStartTime = time.mktime(time.strptime(endDate,"%Y-%m-%d"))#-time.altzone        
        yesterdayStartTime = time.mktime(time.strptime(sinceDate,"%Y-%m-%d"))
#         找到该栏目名昨天创建的所有微博
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
#             对于每一条微博
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
    
if __name__ == "__main__":
    a=time.time()
    startMysqlImportFans("2013-12-25", "2014-2-7")
    startMysqlImportWeibos("2013-12-25", "2014-2-7")
    print time.time()-a
