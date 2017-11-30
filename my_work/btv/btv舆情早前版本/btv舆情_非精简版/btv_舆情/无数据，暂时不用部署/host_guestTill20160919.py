#coding:UTF-8
'''
Created on 2016年3月7日

@author: Ivy
主持嘉宾热度，并不是基本信息
结果对应sql表是host_popularity
3月25日补充，
基本信息部分因为没有数据，先把我们的sql表复制过去
4月6日补充
host_popularity显示主持人基本信息和当天热度（用update而不是insert），top_host_trend是热度趋势
目前这个代码不涉及基本信息的抓取
'''
import sys,os
from sys import path
path.append('tools/')
path.append(path[0]+'/tools')
import MySQLdb
# 工具类
import jieba
import datetime
# from datetime import *
import time
import json
import base64
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import requests
reload(sys)
sys.setdefaultencoding('utf8')

def issuccessful(request):
    if 200 <= request.status_code and request.status_code <= 299:
        return True
    else:
        return False


def addhost_guest(baseurl,mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv_v2'):

    # 连接数据库
    sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()

    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS host_popularity(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, host varchar(20), date date, popularity bigint(20), rank bigint(20),
                    pic varchar(1000), created_date datetime, weibo varchar(200), description varchar(300)) DEFAULT CHARSET=utf8;''')
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS top_host_trend(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, host varchar(20), date date, popularity bigint(20), created_date datetime, program_id varchar(50)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'
    program = '军情解码'

    now_new = datetime.datetime.now()
    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    # today = time.strftime("%Y-%m-%d")
    # sqlcursor.execute("""update host_popularity set created_date = %s""",(now_new,))
    # sqlcursor.execute("""update host_popularity set date = %s""",(today,))
    # sqlcursor.execute('''SELECT program_id from competition_analysis where program = %s''', (program,))
    # bufferTemp = sqlcursor.fetchone()
    # program_id = bufferTemp[0]
    # 时间属性
    # inter为0，即为当日
    inter = 1
    now = int(time.time())-86400*inter
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    print otherStyleTime
    # 首先读取索引表
    tablename_index = "DATA:WEIBO_POST_Keywords_INDEX"
    prefix = otherStyleTime + "*"
    # print baseurl + "/" + tablename_index + "/" + prefix
    r_index = requests.get(baseurl + "/" + tablename_index + "/" + prefix,  auth=kerberos_auth, headers = {"Accept" : "application/json"})
    if issuccessful(r_index) == False:
        print "Could not get messages from HBase. Text was:\n" + r_index.text
    # print base64.b64decode(r.text)
    # print r_index.text
    bleats_0 = json.loads(r_index.text)
    row_key_name_list = list()
    for row in bleats_0['Row']:
        row_key = base64.b64decode(row['key'])
        row_key_name = row_key.split("-")[3]
        # print row_key, row_key_name
        row_key_name_list.append(row_key_name)
    print len(row_key_name_list)
    sqlcursor.execute("""delete from top_host_trend where date = %s""", (otherStyleTime,))
    sqlcursor.execute("""delete from top_military_expert_trend where date = %s""", (otherStyleTime,))
    sqlcursor.execute("SELECT DISTINCT(program_name) from ini_app_program_source_rel")
    bufferTemp = sqlcursor.fetchall()
    for one_program in bufferTemp:
        one_program = one_program[0].encode('utf8')
        sqlcursor.execute("""select source_path from ini_app_program_source_rel where app_name = '主持人分析' and program_name = %s""",(one_program,))
        source_path = sqlcursor.fetchone()
        try:
            tablename = source_path[0]
        except:
            print 'error data'
            continue
        print one_program
        sqlcursor.execute('''SELECT program_id from ini_app_program_source_rel where program_name = %s limit 1;''', (one_program,))
        bufferTemp = sqlcursor.fetchone()
        program_id = bufferTemp[0]
        # 主持人嘉宾分开，以下是主持人
        sqlcursor.execute("""select distinct(app_name) from ini_app_program_source_rel where app_id = '1' and program_name = %s""",(one_program,))
        bufferTemp = sqlcursor.fetchall()
        sqlcursor.execute("""select distinct(app_name) from ini_app_program_source_rel where app_id = '2' and program_name = %s""",(one_program,))
        bufferTemp_jiabing = sqlcursor.fetchall()
        for one_host_guest in bufferTemp:
            one_host_guest = one_host_guest[0]
            print "host", one_host_guest
            if one_host_guest == '主持人分析':
                continue
            tempData = []
            count = 0
            for single_rowkey in row_key_name_list:
                r = requests.get(baseurl + "/" + tablename + "/" + single_rowkey,  auth=kerberos_auth, headers = {"Accept" : "application/json"})
                if issuccessful(r) == False:
                    print "Could not get messages from HBase. Text was:\n" + r.text
                # print r.text
                bleats = json.loads(r.text)
                for row in bleats['Row']:
                    flag = True
                    for cell in row['Cell']:
                        columnname = base64.b64decode(cell['column'])
                        value = cell['$']
                        if value == None:
                            print 'none'
                            continue
                        if columnname == "base_info:cdate":
                            cdate = base64.b64decode(value)
                            cdate = cdate.split('T')[0]
                            # if cdate != otherStyleTime:
                            #     flag = False
                            #     break
                        if columnname == "base_info:match":
                            column = base64.b64decode(value)
                            if (one_host_guest not in column):
                                flag = False
                                break
                    if flag:
                        # print "host", one_program, single_rowkey, one_host_guest, cdate
                        count += 1
            # print "host", one_program, single_rowkey, one_host_guest, cdate,count
            count = (count+5)*6
            print "count", count
            tempData.append(one_host_guest)
            tempData.append(otherStyleTime)
            # popularity
            tempData.append(count)
            # 插表的时间
            tempData.append(now_new)
            # program_id
            tempData.append(program_id)

            sqlcursor.execute('''insert into top_host_trend(host, date, popularity, created_date, program_id)
                        values (%s, %s, %s, %s, %s)''',tempData)
            sqlcursor.execute("""UPDATE host_popularity set date = %s, popularity = %s, created_date = %s where host = %s""",(otherStyleTime, count, now_new, one_host_guest))
            sqlConn.commit()
            tempData = []
        #
        # # 以下是嘉宾
        for one_guest in bufferTemp_jiabing:
            one_guest = one_guest[0]
            if one_guest == '嘉宾分析':
                continue
            tempData = []
            count = 0
            for single_rowkey in row_key_name_list:
                r = requests.get(baseurl + "/" + tablename + "/" + single_rowkey,  auth=kerberos_auth, headers = {"Accept" : "application/json"})
                if issuccessful(r) == False:
                    print "Could not get messages from HBase. Text was:\n" + r.text
                # print r.text
                bleats = json.loads(r.text)
                for row in bleats['Row']:
                    flag = True
                    for cell in row['Cell']:
                        columnname = base64.b64decode(cell['column'])
                        value = cell['$']
                        if value == None:
                            print 'none'
                            continue
                        if columnname == "base_info:cdate":
                            cdate = base64.b64decode(value)
                            cdate = cdate.split('T')[0]
                            # if cdate != otherStyleTime:
                            #     flag = False
                            #     break
                        if columnname == "base_info:match":
                            column = base64.b64decode(value)
                            if (one_guest not in column):
                                flag = False
                                break
                    if flag:
                        # print "guest", one_program, single_rowkey, one_guest, cdate
                        count += 1
            # print "guest", one_program, single_rowkey, one_guest, cdate, count
            count = (count+5)*6
            tempData.append(one_guest)
            tempData.append(otherStyleTime)
            # popularity
            tempData.append(count)
            # 插表的时间
            tempData.append(now_new)
            # program_id
            tempData.append(program_id)

            sqlcursor.execute('''insert into top_military_expert_trend(guest, date, popularity, created_date, program_id)
                        values (%s, %s, %s, %s, %s)''',tempData)
            sqlcursor.execute("""UPDATE military_expert_popularity_degree set date = %s, popularity = %s, create_date = %s where guest = %s""",(otherStyleTime, count, now_new, one_guest))
            sqlConn.commit()
            tempData = []


    sqlConn.close()




if __name__=='__main__':
    commentTest = addhost_guest(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', dbname = 'btv_v2')


