#coding:UTF-8
'''
Created on 2014年3月12日

@author: hao
'''
import os, sys
import MySQLdb
reload(sys)

sys.setdefaultencoding('utf8')

def addTVTable(mysqlhostIP,mysqlUserName = 'root', mysqlPassword = '123456', dbname = 'cctvTimer'):
    sqlconn=MySQLdb.connect(host=mysqlhostIP,user=mysqlUserName,passwd=mysqlPassword,db = dbname,charset='utf8')
    sqlcursor = sqlconn.cursor()
    print '建库ok'
    sqlcursor.execute("CREATE TABLE IF NOT EXISTS TVTable(programId int(64) primary key, programName varchar(128)) DEFAULT CHARSET=utf8;")
    
    count=0
    tempData = []
    count +=1
    tempData.append((count,'毛泽东'))
    count +=1
    tempData.append((count,'一代枭雄'))
    count +=1
    tempData.append((count,'土地公土地婆'))
    count +=1
    tempData.append((count,'大掌门'))
    count +=1
    tempData.append((count,'天龙八部'))
    count +=1
    tempData.append((count,'封神英雄榜'))
    count +=1
    tempData.append((count,'老妈的三国时代'))
    count +=1
    tempData.append((count,'舞乐传奇'))
    #     sqlcursor.executemany("insert into programTable(programId, programName) values(%s %s)", tempData)
    sqlcursor.executemany("insert into TVTable(programId, programName) values (%s, %s)", tempData)  
    sqlconn.commit()
    sqlconn.close()
    

if __name__=='__main__':
    addTVTable(mysqlhostIP = '10.3.3.220')


