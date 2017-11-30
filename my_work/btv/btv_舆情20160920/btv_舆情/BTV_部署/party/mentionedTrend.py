#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2016年3月26日
@author: Ivy
提及量趋势(天)

'''
import sys,os
import MySQLdb
reload(sys)
sys.setdefaultencoding('utf8')
import requests
import json
import time
import base64
from requests_kerberos import HTTPKerberosAuth, OPTIONAL

__author__ = "@Ivy"
__email__ = "jincan@ctvit.com.cn"

def issuccessful(request):
    if 200 <= request.status_code and request.status_code <= 299:
        return True
    else:
        return False

def mentioned_trend(baseurl,mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv'):
    # 连接数据库
    sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS gala_mentioned_trend(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, mentioned bigint(50), date Date, program_id varchar(50), program varchar(50)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'

    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    # 表名
    tablename = "DATA:WEIBO_POST_Keywords"
    r = requests.get(baseurl + "/" + tablename + "/*",  auth=kerberos_auth, headers = {"Accept" : "application/json"})
    if issuccessful(r) == False:
        print "Could not get messages from HBase. Text was:\n" + r.text
    quit()
    bleats = json.loads(r.text)

    tempData = []
    date_mentioned_dict = dict()
    # 时间属性
    # inter为0，即为当日
    inter = 0
    now = int(time.time())-86400*inter
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    print otherStyleTime
    count_times = 0
    # bleats is json file
    for row in bleats['Row']:
        for cell in row['Cell']:
            columnname = base64.b64decode(cell['column'])
            value = cell['$']
            if value == None:
                print 'none'
                continue
            if columnname == "base_info:match":
                column = base64.b64decode(value)
            if column == "春晚":
                if columnname == "'base_info:cdate'":
                    cdate = base64.b64decode(value)
                    cdate = cdate.split(' ')[0]
                    if cdate == otherStyleTime:
                        count_times += 1
    date_mentioned_dict[otherStyleTime] = count_times
    for i,j in date_mentioned_dict.iteritems():
        print i,j
        # 提及量
        print 'quatity',j
        tempData.append(int(j))
        print 'date',i
        # 日期
        tempData.append(str(i))
        # program_id
        tempData.append('12345')
        # 节目名称
        tempData.append('2016年北京卫视春节联欢晚会')
        sqlcursor.execute('''insert into gala_mentioned_trend(mentioned, date, program_id, program) values (%s, %s, %s, %s)''',tempData)
        sqlConn.commit()
        tempData = []
    sqlConn.close()


if __name__=='__main__':
    commentTest = mentioned_trend(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', dbname = 'btv')

    
