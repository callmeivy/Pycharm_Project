#coding:UTF-8
'''
Created on 2016年3月27日
@author: Ivy
微博id互动分析
4月5日补充，
加上日期这一维度
'''
import sys,os
import MySQLdb
from collections import Counter
reload(sys)
import time
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
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS gala_weiboid_interaction(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, weibo_id varchar(50), weibo varchar(50), interaction bigint(50), date Date, program_id varchar(50), program varchar(50), exposure int(11)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'

    # 连接hbase数据库
    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    # 表名
    # table=conn.table('WEIBO_POST_chunwan')
    tablename = "DATA:WEIBO_POST_Keywords"
    r = requests.get(baseurl + "/" + tablename + "/*",  auth=kerberos_auth, headers = {"Accept" : "application/json"})
    if issuccessful(r) == False:
        print "Could not get messages from HBase. Text was:\n" + r.text
    # quit()
    bleats = json.loads(r.text)

    # row_prefix, limit可以限定次数
    # for key,data in table.scan(limit = 10, batch_size = 10):
    user_id_box = list()
    # 时间属性
    # inter为0，即为当日
    inter = 88
    now = int(time.time())-86400*inter
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    print otherStyleTime


    user_interaction = dict()
    user_flash = dict()
    # for one_id in user_id_box:

    interaction_sum = 0
    flash_sum = 0
    # bleats is json file
    for row in bleats['Row']:
        flag = True
        user_id_new = ''
        for cell in row['Cell']:
            columnname = base64.b64decode(cell['column'])
            value = cell['$']
            if value == None:
                print 'none'
                continue
            if columnname == "base_info:user_id":
                user_id_new = base64.b64decode(value)
            if columnname == "base_info:match":
                column = base64.b64decode(value)
                if ("北京卫视春晚"  not in column) and ("北京台的春晚" not in column) and ("BTV春晚" not in column) and ("BTV春晚" not in column) and ("bTV春晚" not in column):
                    flag = False
                    break
            if columnname == "base_info:cdate":
                cdate = base64.b64decode(value)
                cdate = cdate.split('T')[0]
                if cdate != otherStyleTime:
                    flag = False
                    break
            # 转发数
            if columnname == "base_info:rcount":
                rcount = base64.b64decode(value)
            # 评论数
            if columnname == "base_info:ccount":
                ccount = base64.b64decode(value)
            # 点赞数
            if columnname == "base_info:acount":
                acount = base64.b64decode(value)
            # 曝光量
            if columnname == "base_info:flash":
                flash = base64.b64decode(value)
                print "flash",flash

        if flag:
            # 互动量
            # if (user_id_new == '1910278097') or (user_id_new == '1651381185') or (user_id_new == '3803963374'):
            #     print 'blabla',user_id_new,rcount,ccount,acount
            interaction = float(rcount) * 0.5 + float(ccount)*0.4 + float(acount)*0.1
            # interaction_sum += interaction

            # print one_id,flash
            # flash_sum += int(flash)
            user_interaction[str(user_id_new)] = interaction
            user_flash[str(user_id_new)] = float(flash)
    user_interaction = sorted(user_interaction.iteritems(), key=lambda e:e[1], reverse=True)
    ind = 0
    tempData = list()
    for i in user_interaction:
        ind += 1
        # 这里取多少个，就写多少，比如取3个，就写>3
        if ind > 10:
            break
        user = i[0]
        interaction_value = i[1]
        # weibo_id
        tempData.append(user)
        # screen_name
        tablename = "DATA:WEIBO_USER"
        rowKey = user
        r = requests.get(baseurl + "/" + tablename + "/" + rowKey,  auth=kerberos_auth, headers = {"Accept" : "application/json"})
        if issuccessful(r) == False:
            print "Could not get messages from HBase. Text was:\n" + r.text
        # quit()
        bleats = json.loads(r.text)
        for row in bleats['Row']:
            for cell in row['Cell']:
                columnname = base64.b64decode(cell['column'])
                value = cell['$']
                if value == None:
                    print 'none'
                    continue
                if columnname == "base_info:screen_name":
                    screen_name = base64.b64decode(value)
                    # print "screen_name",screen_name

        tempData.append(screen_name)
        # interaction
        tempData.append(interaction_value)
        # DATE
        tempData.append(otherStyleTime)
        # program_id
        tempData.append('100')
        # program
        tempData.append('2016年北京卫视春节联欢晚会')
        # exposure
        for i,j in user_flash.iteritems():
            if i ==user:
                exposure_value = j
                tempData.append(exposure_value)
                sqlcursor.execute('''insert into gala_weiboid_interaction(weibo_id, weibo, interaction, date, program_id, program, exposure) values (%s, %s, %s, %s, %s, %s, %s)''',tempData)
                sqlConn.commit()
                tempData = []

    sqlConn.close()


if __name__=='__main__':
    commentTest = mentioned_trend(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', dbname = 'btv_v2')


