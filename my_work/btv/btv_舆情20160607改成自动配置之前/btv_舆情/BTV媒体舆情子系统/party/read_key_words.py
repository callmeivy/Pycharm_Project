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
def issuccessful(request):
    if 200 <= request.status_code and request.status_code <= 299:
        return True
    else:
        return False
def mentioned_trend(baseurl,mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv_v2'):
    list_key_words = list()
    # 存储评论数据
    # 连接数据库
    print(base64.b64decode(b'Q29weXJpZ2h0IChjKSAyMDEyIERvdWN1YmUgSW5jLiBBbGwgcmlnaHRzIHJlc2VydmVkLg==').decode())
    sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS key_kk(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, keywords varchar(50)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'
    os.popen('kinit -k -t /home/ctvit/ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    tablename = "DATA:WEIBO_POST_Keywords"
    r = requests.get(baseurl + "/" + tablename + "/*",  auth=kerberos_auth, headers = {"Accept" : "application/json"})
    if issuccessful(r) == False:
        print "Could not get messages from HBase. Text was:\n" + r.text
    # quit()
    bleats = json.loads(r.text)
    box = list()
    for row in bleats['Row']:
        # print 000
        # count+=1
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
                key_word = base64.b64decode(value)
            if columnname == "base_info:hot":
                hot = base64.b64decode(value)
                print 'hot',hot
            #     if ("北京卫视春晚"  not in key_word) and ("北京台的春晚" not in key_word) and ("BTV春晚" not in key_word) and ("BTV春晚" not in key_word) and ("bTV春晚" not in key_word):
            #         break
            # if columnname == "base_info:cdate":
            #     cdate = base64.b64decode(value)
            #     cdate = cdate.split('T')[0]
            #     print 'date',cdate
            #     if cdate not in box:
            #         box.append(cdate)

    # for i in box:
    #     print i

                # print 'ppp',type(key_word)
                # print '11',key_word
    #             if key_word not in list_key_words:
    #                 list_key_words.append(key_word)
    #
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
