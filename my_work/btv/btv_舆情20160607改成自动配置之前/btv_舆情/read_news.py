#coding=utf-8

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
import requests
import re
def issuccessful(request):
    if 200 <= request.status_code and request.status_code <= 299:
        return True
    else:
        return False
def mentioned_trend(baseurl,mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv_v2'):
    list_key_words = list()
    box_from = list()
    inter = 1
    now = int(time.time())-86400*inter
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    print otherStyleTime
    # 存储评论数据
    # 连接数据库
    print(base64.b64decode(b'Q29weXJpZ2h0IChjKSAyMDEyIERvdWN1YmUgSW5jLiBBbGwgcmlnaHRzIHJlc2VydmVkLg==').decode())
    sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS key_kk(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, keywords varchar(50)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'
    os.popen('kinit -k -t /home/ctvit/ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    tablename = "DATA:NEWS_Content"
    r = requests.get(baseurl + "/" + tablename + "/*",  auth=kerberos_auth, headers = {"Accept" : "application/json"})
    if issuccessful(r) == False:
        print "Could not get messages from HBase. Text was:\n" + r.text
    # quit()
    bleats = json.loads(r.text)
    box = list()
    ind =0
    box = list()
    for row in bleats['Row']:
        # print 000
        # count+=1
        message = ''
        lineNumber = ''
        username = ''
        type_0 = ''
        title = ''
        flag = False
        flag_t = False
        flag_type = False
        for cell in row['Cell']:
            columnname = base64.b64decode(cell['column'])

            value = cell['$']
            if value == None:
                print 'none'
                continue
        #     if columnname == "base_info:news_from":
        #         from_f = base64.b64decode(value)
        #         from_f = re.sub('[^a-zA-Z]','',from_f)
        #         # print 'hh',from_f,len(from_f)
        #         if len(from_f) > 1:
        #             if ('Jane' not in from_f) and ():
        #                 if from_f not in box_from:
        #                     box_from.append(from_f)
        # for i in box_from:
        #     print i

                # if 'Jane' not in from_f:
                #     flag = False
                #     break
            # if columnname == "base_info:type":
            #     type_0 = base64.b64decode(value)
        #         if '军事' in type_0:
        #             flag_t = True
        #         else:
        #             break
        #
        #         # if 'anes' not in from_f:
        #         #     flag = False
        #         #     break
            if columnname == "base_info:title":
                title = base64.b64decode(value)
        #         # print 'title',title
        #         if '5月大税改' not in title:
        #             flag = False
        #             break
            if columnname == "base_info:datetime":
                datetime = base64.b64decode(value)
                transform_date_time = datetime.split(' ')[0]
                # print 'hhh',transform_date_time,otherStyleTime, otherStyleTime
                # if transform_date_time >= otherStyleTime:
                if '5月26日' in transform_date_time:
                    flag = True
                    break
        #
            if columnname == "base_info:website":
                url = base64.b64decode(value)

            if columnname == "base_info:type":
                type_0 = base64.b64decode(value)
                print 'type is blank?',type_0,len(type_0),type_0[0:1]
                # if len(type_0) < 1:
                #     print 'type is blank'
                # 不能直接'军事'in type,
                # if (type_0 == '军事') or (type_0 == '新浪军事') or (type_0 == '搜狐军事') or (type_0 == '滚动军事') or (type_0 == '凤凰军事') or (type_0 == '军事要闻'):
                # # if (type_0 == '军事'):
                #     flag_type = True
                # else:
                #     break
        #
        #
        #
        # if flag and flag_t:
        #     rowKey = base64.b64decode(row['key'])
        #     print '00', type_0,rowKey
        #
        #     print title


            #     ind += 1
            # if columnname == "base_info:type":
            #     type = base64.b64decode(value)
            #     print 'type',type

    #             if key_word not in list_key_words:
    #                 list_key_words.append(key_word)
    #   if flag and flag_type:
        if flag:
            print 1111
            # print transform_date_time,datetime,otherStyleTime, len(datetime),title,url,type_0
            # print 'datetime',datetime, type(datetime),len(str(datetime)),str(datetime)[11:19]
    # tempData = []
    # for i in list_key_words:
    #     print 'key',i
    #     tempData.append(str(i))
    #     sqlcursor.execute('''insert into key_kk(keywords) values (%s)''',tempData)
    #     sqlConn.commit()
    #     tempData = []
    sqlConn.close()
        # print "key_words", i
if __name__=='__main__':
    commentTest = mentioned_trend(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', dbname = 'btv_v2')
