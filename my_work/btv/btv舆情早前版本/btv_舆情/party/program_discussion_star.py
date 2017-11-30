#coding:UTF-8
'''
Created on 2016年3月27日
@author: Ivy
晚会具体节目分析
'''
import sys,os
import MySQLdb
import time
from collections import Counter
reload(sys)
sys.setdefaultencoding('utf8')
import datetime
import json
import base64
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import requests

def issuccessful(request):
    if 200 <= request.status_code and request.status_code <= 299:
        return True
    else:
        return False


def mentioned_trend(baseurl,mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv_v2'):
    # 连接数据库
    sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS gala_attention_degree(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, type varchar(10), content varchar(200), attention_degree bigint(50), date date, program_id varchar(50), program varchar(50)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'

    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    # 表名
    tablename = "DATA:WEIBO_POST_Keywords"
    r = requests.get(baseurl + "/" + tablename + "/*",  auth=kerberos_auth, headers = {"Accept" : "application/json"})
    if issuccessful(r) == False:
        print "Could not get messages from HBase. Text was:\n" + r.text
    # quit()
    bleats = json.loads(r.text)
    inter = 87
    now = int(time.time())-86400*inter
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    print 'otherStyleTime', otherStyleTime
    tempData = list()
     # 明星话题
    sqlcursor.execute('''SELECT name FROM gala_celebrity;''')
    bufferTemp = sqlcursor.fetchall()
    star_count_dict = dict()
    for one_star in bufferTemp:
        one_star = one_star[0]
        # one_star = one_star[0].encode('utf8')
        # print one_star

        count = 0
        # bleats is json file
        for row in bleats['Row']:
            flag = True
            for cell in row['Cell']:
                columnname = base64.b64decode(cell['column'])
                value = cell['$']
                if value == None:
                    print 'none'
                    continue
                if columnname == "base_info:match":
                    column = base64.b64decode(value)
                    if ("北京卫视春晚"  not in column) and ("北京台的春晚" not in column) and ("BTV春晚" not in column) and ("BTV春晚" not in column) and ("bTV春晚" not in column):
                        flag = False
                        break

                if columnname == "base_info:cdate":
                    date_created = base64.b64decode(value)
                    date_created = date_created.split('T')[0]
                    if date_created != otherStyleTime:
                        flag = False
                        break
                if columnname == "base_info:text":
                    content = base64.b64decode(value)
                    # print content
                    if one_star  not in content:
                        flag = False
                        break
            if flag:
                count += 1
        # type
        tempData.append('明星话题')
        # content
        tempData.append(one_star)
        # attention_degree
        tempData.append(count)
        # date
        tempData.append(otherStyleTime)
        # program_id
        tempData.append('100')
        # program
        tempData.append('2016年北京卫视春节联欢晚会')
        sqlcursor.execute('''insert into gala_attention_degree(type, content, attention_degree, date, program_id, program) values (%s, %s, %s, %s, %s, %s)''',tempData)
        sqlConn.commit()
        tempData = []




    sqlConn.close()


if __name__=='__main__':
    commentTest = mentioned_trend(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', dbname = 'btv_v2')


