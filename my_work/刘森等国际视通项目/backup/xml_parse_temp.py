#coding:UTF-8
'''
Created on 2017年5月10日

@author: Ivy
xml文件解析
'''
import xml.etree.ElementTree as ET
import MySQLdb
import time
from StringIO import StringIO
def xml_to_mysql(mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'weibo'):
    sqlConn = MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db=dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS cctv_news_content(pk bigint NOT NULL PRIMARY KEY
    AUTO_INCREMENT, MID varchar(10), DCID bigint(10), Language varchar(5), PubTitle varchar(500),
    Summary varchar(500), Shotlist varchar(500),Storyline text,Restrictions varchar(50),
    Source varchar(50), ScriptCreateTime varchar(20),ScriptIssTime varchar(20),lastmodifytime varchar(20),
    videoDuration varchar(10),cid varchar(10),tname varchar(200),key_words varchar(200),related_news varchar(200),tname_by_classifier varchar(200), updated_datetime datetime) DEFAULT CHARSET=utf8;''')
    print '新建库成功'
    now = int(time.time())
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    print otherStyleTime
    tree = ET.parse('E://gds20170516.xml')
    root = tree.getroot()
    PubTitle =""
    for item in root:
        tempData = list()
        for ref in item:
            if ref.attrib.values()[0] == 'MID':
                MID = ref.text
            if ref.attrib.values()[0] == 'PubTitle':
                PubTitle = ref.text
            sqlcursor.execute('''update cctv_news_content set MID = %s where PubTitle = %s''', (MID,PubTitle))
            sqlConn.commit()


    sqlConn.close()

if __name__ == '__main__':
    cctv_news_content = xml_to_mysql(mysqlhostIP='192.168.168.105', dbname='weibo')


