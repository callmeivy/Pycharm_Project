#coding:UTF-8
'''
Created on 2016年3月4日

@author: Ivy
竞争分析中的竞争栏目相关新闻
结果对应sql表是related_news
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

def issuccessful(request):
    if 200 <= request.status_code and request.status_code <= 299:
        return True
    else:
        return False


def addCommentTable(baseurl,mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv'):
    # 连接数据库
    sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS latest_news(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, news_title varchar(500), source varchar(500), date date, news_content varchar(500), region varchar(500),
                    url varchar(500)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'
    
    # 连接hbase数据库
    # 存储评论数据
    commentsData = []
    tempData = []
    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    # 表名
    # table=conn.table('news_foreign')
    tablename = "DATA:WEIBO_POST_chunwan"
    r = requests.get(baseurl + "/" + tablename + "/*",  auth=kerberos_auth, headers = {"Accept" : "application/json"})
    if issuccessful(r) == False:
        print "Could not get messages from HBase. Text was:\n" + r.text
    # quit()
    bleats = json.loads(r.text)

    # 处理情感

    # commentTable需要有program标识
    emotionsWord = []
    emotionsScore = 0
    count = 0
    printCount = 0

    # 时间属性
    # inter为0，即为当日
    inter = 0
    now = int(time.time())-86400*inter
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    print otherStyleTime

    # row_prefix, limit可以限定次数
    # bleats is json file
    for row in bleats['Row']:
        for cell in row['Cell']:
            columnname = base64.b64decode(cell['column'])
            value = cell['$']
            if value == None:
                print 'none'
                continue
            if columnname == "'base_info:cdate'":
                cdate = base64.b64decode(value)




        date_time = data['testColumn:date']
        if date_time == otherStyleTime:
            content = data['testColumn:body']
            source = data['testColumn:source']
            title = data['testColumn:title']
            url = data['testColumn:url']
            count += 1
            printCount+=1
        # 处理每一条
            # 新闻标题
            tempData.append(title)
            # 新闻来源
            tempData.append(source)
        #     日期时间
            tempData.append(date_time)
            # 新闻正文
            tempData.append(content)
            # 国内还是国外
            tempData.append('foreign')
            # url
            tempData.append(url)
        #     转换为元组
            commentsData.append(tuple(tempData))
            tempData = []
            if count>=10:
                sqlcursor.executemany('''insert into latest_news(news_title, source, date, news_content, region, url)
                            values (%s, %s, %s, %s, %s, %s)''',commentsData)
                sqlConn.commit()
                commentsData = []
                count = 0
                print '插入'+str(printCount)+'个'
    # # except:
    # #      print tempData
    sqlcursor.executemany('''insert into latest_news(news_title, source, date, news_content, region, url)
                        values (%s, %s, %s, %s, %s, %s)''',commentsData)
    sqlConn.commit()
    sqlConn.close()


if __name__=='__main__':
#     timeBegin = datetime.datetime.now() - datetime.timedelta(days=1)
#     2014-03-25 14:28:34.827378
    commentTest = addCommentTable(hbaseIP = '192.168.168.41', mysqlhostIP = '10.3.3.182', dbname = 'btv')
#     print CommentTests
    
    
