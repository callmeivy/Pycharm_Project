#coding:UTF-8
'''
Created on 2014年3月4日

@author: hao
'''
import os, sys
import MySQLdb
reload(sys)

sys.setdefaultencoding('utf8')
def addTVTable(mysqlhostIP,mysqlUserName = 'root', mysqlPassword = '123456', dbname = 'cctvTimer'):   
    # 路径
    path = os.path.abspath(os.path.dirname(sys.argv[0]))+'/source/lanmuid.txt'
    
    programFile = open(path,'r')
    programNames = programFile.readlines()
    count = 0
    programIdList = {}
        
    sqlconn=MySQLdb.connect(host=mysqlhostIP,user=mysqlUserName,passwd=mysqlPassword,db = dbname,charset='utf8')
    sqlcursor = sqlconn.cursor()
    programFile.close()
    
    sqlcursor.execute('''DROP TABLE IF EXISTs programTable;''')
    sqlcursor.execute("CREATE TABLE IF NOT EXISTS programTable(programId int(64) primary key, programName varchar(128), is_checked int(11)) DEFAULT CHARSET=utf8;")
        
    count=0
    tempData = []
    
    for line in programNames:
        count+=1
        line = line.strip()
        line = line.replace("\n","").decode('utf8')
        line = line.split(',')[0]
        
        programIdList[line] = count
        tempData.append((count,line,0))
    
    sqlcursor.executemany("insert into programTable(programId, programName, is_checked) values (%s, %s, %s)", tempData)  
    sqlconn.commit()
    sqlconn.close()
    
if __name__=='__main__':
    addTVTable(mysqlhostIP = '10.3.3.220')
    
    