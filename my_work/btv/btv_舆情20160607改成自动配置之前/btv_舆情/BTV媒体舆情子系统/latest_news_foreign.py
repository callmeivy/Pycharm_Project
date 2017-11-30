#coding:UTF-8
'''
Created on 2016年3月4日

@author: Ivy
最新新闻
结果对应sql表是newest_news
英文日期应该转化为2016-03-04的格式
4月5日补充，
加上日期这一维度
'''
import sys,os
from sys import path
path.append('tools/')
path.append(path[0]+'/tools')
import MySQLdb
# 工具类
import jieba
import datetime
import time
import json
import base64
from requests_kerberos import HTTPKerberosAuth, OPTIONAL

reload(sys)
sys.setdefaultencoding('utf8')
import requests

def issuccessful(request):
    if 200 <= request.status_code and request.status_code <= 299:
        return True
    else:
        return False


def addCommentTable(baseurl,mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv_v2'):
    # 连接数据库
    sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()
    # 这里的date修改为varchar格式了
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS newest_news(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, news_title varchar(500), source varchar(500), date varchar(10), news_content varchar(500), region varchar(500),
                    url varchar(500)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'

    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    # 表名
    tablename = "DATA:NEWS_Content"
    r = requests.get(baseurl + "/" + tablename + "/*",  auth=kerberos_auth, headers = {"Accept" : "application/json"})
    if issuccessful(r) == False:
        print "Could not get messages from HBase. Text was:\n" + r.text
    # quit()
    bleats = json.loads(r.text)

    # 存储评论数据
    commentsData = []
    tempData = []
    inter = 1
    now = int(time.time())-86400*inter
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    print otherStyleTime
    emotionsWord = []
    emotionsScore = 0

    for row in bleats['Row']:
        title = ''
        source = ''
        content =''
        url = ''
        columnname = ''
        lennnn = 0
        flag = False
        for cell in row['Cell']:
            columnname = base64.b64decode(cell['column'])
            value = cell['$']
            if value == None:
                print 'none'
                continue
            if columnname == "base_info:datetime":
                date_time = base64.b64decode(value)

            if columnname == "base_info:news_from":
                source = base64.b64decode(value)
                if 'Jane' in source and len(source)>=10:
                    flag=True
                else:
                    break
            if columnname == "base_info:title":
                title = base64.b64decode(value)
                # if '中新' in title:
                #     flag = False
                #     break
            if columnname == "base_info:website":
                url = base64.b64decode(value)
            if columnname == "body:content":
                content = base64.b64decode(value)
        if flag:
        # 处理每一条
            # 新闻标题
            tempData.append(title)
            # 新闻来源
            tempData.append(source)
        #     日期时间
        #     print 'dd',date_time
            tempData.append(date_time)
            # 新闻正文
            tempData.append(content)
            # 国内还是国外
            # if source == 'Janes':
            #     tempData.append('foreign')
            # else:
            tempData.append('foreign')
            # url
            tempData.append(url)
            # program_id
            tempData.append('-3413556768156676966')
        #     转换为元组

            sqlcursor.execute('''insert into newest_news(news_title, source, date, news_content, region, url, program_id)
                        values (%s, %s, %s, %s, %s, %s, %s)''',tempData)
            sqlConn.commit()
            tempData = []
    sqlConn.close()


if __name__=='__main__':
    commentTest = addCommentTable(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', dbname = 'btv_v2')

    
