#coding:UTF-8
'''
Created on 2016年3月7日

@author: Ivy
这是竞争栏目基本信息，不包括竞争栏目的舆情分析
结果对应sql表是competition_analysis
3月24日补充，暂时没有这方面数据，可以直接将SQL表格复制过去
另外注意该部分也有综合评价，但只要在multiple_evaluation中将各个竞争栏目的五个维度数据也像JQJM一样跑出来就好了
'''
import sys,os
from sys import path
path.append('tools/')
path.append(path[0]+'/tools')
import MySQLdb
import pymongo
from emtionProcess import emotionProcess
# 工具类
from remove import removeIrrelevant
# from spammerDetection import spammerdetect
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

def addcompetitiveProgram(baseurl,mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv'):

    # 连接数据库
    sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()

    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS competition_analysis(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, program varchar(50), host varchar(50), time varchar(50), channel varchar(50),
                    introduction varchar(300), program_id varchar(50), image varchar(200), url varchar(200), length varchar(50), program_type_id varchar(2)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'

    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    # 表名
    tablename = "DATA:WEIBO_POST_chunwan"
    r = requests.get(baseurl + "/" + tablename + "/*",  auth=kerberos_auth, headers = {"Accept" : "application/json"})
    if issuccessful(r) == False:
        print "Could not get messages from HBase. Text was:\n" + r.text
    quit()
    bleats = json.loads(r.text)


    # 连接hbase数据库
    # 存储评论数据
    commentsData = []
    tempData = []
    # 处理情感
    emProcess = emotionProcess()
    rmIrr = removeIrrelevant()
    # table=conn.table('competitiveProgram')
    # customerEvaluation_informal需要有program标识
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
                # 播出频道
            if columnname == "'base_info:channel'":
                channel = base64.b64decode(value)
            # 因为是基本信息的抓取，不需要date
            # 播出时间
            if columnname == "'base_info:Broadcast_time'":
                Broadcast_time = base64.b64decode(value)
            # 每集长度
            if columnname == "'base_info:period'":
                period = base64.b64decode(value)
            # 主持人
            if columnname == "'base_info:host'":
                host = base64.b64decode(value)
            # 简介
            if columnname == "'base_info:Info'":
                Info = base64.b64decode(value)
            # 竞争栏目id
            if columnname == "'base_info:program_id'":
                program_id = base64.b64decode(value)
            # 竞争栏目名称
            if columnname == "'base_info:program'":
                program = base64.b64decode(value)
            # url
            if columnname == "'base_info:url'":
                url = base64.b64decode(value)
            count += 1
            printCount+=1
            # 处理每一条
            tempData.append(program)
            tempData.append(host)
            tempData.append(Broadcast_time)
            tempData.append(channel)
            tempData.append(Info)
            tempData.append(program_id)
            # image
            tempData.append('')
            tempData.append(url)
            tempData.append(period)
            # program_type
            tempData.append('1')
        #     转换为元组
            commentsData.append(tuple(tempData))
            tempData = []
            sqlcursor.executemany('''insert into competition_analysis(program, host, time, channel, introduction, program_id, image, url, length, program_type_id)
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',commentsData)
            sqlConn.commit()
            commentsData = []
    sqlConn.close()

if __name__=='__main__':
#     timeBegin = datetime.datetime.now() - datetime.timedelta(days=1)
#     2014-03-25 14:28:34.827378
    commentTest = addcompetitiveProgram(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '10.3.3.182', dbname = 'btv')
#     print CommentTests


