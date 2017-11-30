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
    # 距离2016.2.9
    inter = 90
    now = int(time.time())-86400*inter
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    print 'otherStyleTime', otherStyleTime
    tempData = list()


    # 后台花絮,用互动量来衡量
    weibo_interaction = dict()
        # bleats is json file
    scan = 0
    filterr = 0
    for row in bleats['Row']:
        scan += 1
        flag = True
        rcount = 0
        ccount = 0
        acount = 0
        # content = ''
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

            elif columnname == "base_info:cdate":
                date_created = base64.b64decode(value)
                date_created = date_created.split('T')[0]
                if date_created != otherStyleTime:
                    flag = False
                    break
            elif columnname == "base_info:text":
                content = base64.b64decode(value)
                # print content
                if ('幕后' not in content) and ('排练' not in content):
                    # print 11111
                    # print 'not suit', content
                    flag = False
                    break
                # print 'suit', content
                    # print 'hou',content
            # 转发数
            elif columnname == "base_info:rcount":
                rcount = base64.b64decode(value)
                # print 'rcount',rcount,type(rcount)
            # 评论数
            elif columnname == "base_info:ccount":
                ccount = base64.b64decode(value)
            # 点赞数
            elif columnname == "base_info:acount":
                acount = base64.b64decode(value)
        if flag:
            # 互动量
            print scan,filterr
            interaction = (float(rcount) * 0.5 + float(ccount)*0.4 + float(acount)*0.1)*61
            if interaction < 0.1:
                interaction = 63.0
            print 'jjj', rcount, ccount, acount, interaction
            weibo_interaction[str(content)] = interaction
        else:
            filterr += 1
    print scan,filterr
        #     print content
    weibo_interaction = sorted(weibo_interaction.iteritems(), key=lambda e:e[1], reverse=True)
    ind = 0
    for i in weibo_interaction:
        ind += 1
        # 这里取多少个，就写多少，比如取3个，就写>3
        if ind > 10:
            break
        weibo_key = i[0]
        interaction_value = i[1]
        # print weibo_key,interaction_value
        # type
        tempData.append('后台花絮')
        # content
        tempData.append(weibo_key.split(',')[0])
        # attention_degree
        tempData.append(interaction_value)
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


