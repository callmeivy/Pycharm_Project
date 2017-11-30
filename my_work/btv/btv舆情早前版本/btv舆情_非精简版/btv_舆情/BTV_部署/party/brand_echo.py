#coding:UTF-8
'''
Created on 2016年3月27日
@author: Ivy

'''
import sys,os
import MySQLdb
import time
from collections import Counter
reload(sys)
sys.setdefaultencoding('utf8')
import datetime
import requests
import json
import base64
from requests_kerberos import HTTPKerberosAuth, OPTIONAL

def issuccessful(request):
    if 200 <= request.status_code and request.status_code <= 299:
        return True
    else:
        return False
def mentioned_trend(baseurl,mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv'):
    # 连接数据库
    sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS gala_brand_echo(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, sponsor varchar(20), echo double, date date, program_id varchar(50), program varchar(50)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'
    # 存储评论数据

    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    tablename = "DATA:WEIBO_POST_Keywords"
    r = requests.get(baseurl + "/" + tablename + "/*",  auth=kerberos_auth, headers = {"Accept" : "application/json"})
    if issuccessful(r) == False:
        print "Could not get messages from HBase. Text was:\n" + r.text
    quit()
    bleats = json.loads(r.text)

    count=0
    # 时间属性
    # inter为0，即为当日
    inter = 0
    now = int(time.time())-86400*inter
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    print otherStyleTime
    sqlcursor.execute('''SELECT sum(mentioned) from gala_mentioned_trend where date = '%s';''', otherStyleTime)
    bufferTemp = sqlcursor.fetchone()
    # “春晚”总共被提及的次数
    chunwan_times = bufferTemp[0]
    sqlcursor.execute('''SELECT DISTINCT(sponsor) from gala_sponsor''')
    bufferTemp = sqlcursor.fetchone()
    sponsor_name = bufferTemp[0]
    count = 0
    tempData = list()
    for row in bleats['Row']:
        count+=1
        message = ''
        lineNumber = ''
        username = ''
        for cell in row['Cell']:
            columnname = base64.b64decode(cell['column'])
            value = cell['$']
            if value == None:
                print 'none'
                continue
            if columnname == "base_info:match":
                column = base64.b64decode(value)
                if column == "春晚":
                    if columnname == "base_info:text":
                        content = base64.b64decode(value)
                    if columnname == "base_info:cdate":
                        cdate = base64.b64decode(value)
                        cdate = cdate.split(' ')[0]
                    # 日期筛选
                    if cdate == otherStyleTime:
                        if sponsor_name in content:
                            count += 1
    # 赞助商及春晚同时被提及的次数
    both_mentioned = count
    both_mentioned_probability = float(count)/float(chunwan_times)
    # sponsor
    tempData.append(sponsor_name)
    # echo
    tempData.append(both_mentioned_probability)
    # date
    tempData.append(cdate)
    # program_id
    tempData.append('100')
    # program
    tempData.append('2016年北京卫视春节联欢晚会')
    sqlcursor.execute('''insert into gala_brand_echo(sponsor, echo, date, program_id, program) values (%s, %s, %s, %s, %s)''',tempData)
    sqlConn.commit()
    tempData = []


    sqlConn.close()


if __name__=='__main__':
    commentTest = mentioned_trend(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', dbname = 'btv')


