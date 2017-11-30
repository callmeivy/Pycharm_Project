#coding:UTF-8
'''
Created on 2016年3月3日

@author: Ivy

综合评价表，mysql中对应的是multiple_evaluation
需要用到customer_evaluation中的数据，所以注意代码run的先后顺序

美誉度evaluation：好评和差评的综合；评议度discuss：回复和转发的综合；曝光量exposure：曝光数的平滑；关注趋势attention：粉丝增长趋势；微博活跃度active
3月24日补充，

微博活跃度通过数WEIBO_POST_JQJM当中的hot个数来衡量
4月5日补充，
加上日期这一维度

'''
import os, sys
from sys import path
path.append(path[0]+'/tools/')
import MySQLdb
import jieba.analyse
import time
import datetime
# 工具类
reload(sys)
sys.setdefaultencoding('utf8')
import json
import base64
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import requests
def issuccessful(request):
    if 200 <= request.status_code and request.status_code <= 299:
        return True
    else:
        return False


def addTwitterTable(baseurl, mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv_v2'):
    
    sqlConn=MySQLdb.connect(host=mysqlhostIP,user=mysqlUserName,passwd=mysqlPassword,db = dbname,charset='utf8')
    sqlcursor = sqlConn.cursor()
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS multiple_evaluation(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, evaluation bigint(20), discuss bigint(20),
                        exposure bigint(20), attention bigint(20), active bigint(20), date date, program_id varchar(200), program varchar(200),
                        reply bigint(20), repost bigint(20), positive_comment bigint(20), negative_comment bigint(20),
                        fans_increased bigint(20)) DEFAULT CHARSET=utf8;''')
    #                         
    print '新建库成功'
    
    # 连接hbase数据库
    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)

    sqlcursor.execute("SELECT * from hbase_info;")
    bufferTemp = sqlcursor.fetchall()
    # 表名
    tablename = "DATA:WEIBO_POST_Keywords"
    r = requests.get(baseurl + "/" + tablename + "/*",  auth=kerberos_auth, headers = {"Accept" : "application/json"})
    if issuccessful(r) == False:
        print "Could not get messages from HBase. Text was:\n" + r.text
    # quit()
    bleats = json.loads(r.text)

    inter = 1
    now = int(time.time())-86400*inter
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    print otherStyleTime

    # sqlcursor.execute('''SELECT program_id from competition_analysis where one_program = %s''', (one_program,))
    # bufferTemp = sqlcursor.fetchone()
    # program_id = bufferTemp[0]
    # 参数
    for one_program in bufferTemp:
        one_program = one_program[1].encode('utf8')
        print one_program
        sqlcursor.execute('''SELECT program_id from competition_analysis where program = %s''', (one_program,))
        bufferTemp = sqlcursor.fetchone()
        program_id = bufferTemp[0]
        count = 0
        printCount = 0
        # 计数
        # sqlcursor.execute('select distinct(date) from customer_evaluation;')
        # all_date = sqlcursor.fetchall()
        # for i in all_date:
        #     print i
        tempData = []
        commentsData = []
        # 日期遍历一次
        flash_sum = 0
        rcount_sum = 0
        ccount_sum = 0
        active = 0
        count+=1
        printCount+=1

        # 正面
        sqlcursor.execute("select * from customer_evaluation where program = %s AND flag = '1' and date = %s", (one_program, otherStyleTime))
        bufferTemp = sqlcursor.fetchone()
        try:
            positiveNum = bufferTemp[0]
        except:
            positiveNum = 0
        # 负面
        sqlcursor.execute("select * from customer_evaluation where program = %s AND flag = '-1' and date = %s", (one_program, otherStyleTime))
        bufferTemp = sqlcursor.fetchone()
        try:
            negativeNum = bufferTemp[0]
        except:
            positiveNum = 0
        # evaluation
        evaluation = (positiveNum*6 - negativeNum*4)/100
        tempData.append(evaluation)
            # bleats is json file
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
                    if cdate != otherStyleTime:
                        flag = False
                        break
                if columnname == "base_info:match":
                    column = base64.b64decode(value)
                    if (one_program  not in column):
                        flag = False
                        break
                # 曝光量
                if columnname == "base_info:flash":
                    flash = int(base64.b64decode(value))
                # 转发数
                if columnname == "base_info:rcount":
                    rcount = int(base64.b64decode(value))

                # 评论数
                if columnname == "base_info:ccount":
                    ccount = int(base64.b64decode(value))

                if columnname == "base_info:hot":
                    hot = base64.b64decode(value)
                # 目前数据暂时是显示“hot”

            if flag:
                count += 1
                flash_sum += int(flash)
                rcount_sum += int(rcount)
                ccount_sum += int(ccount)
                if hot == 'true':
                    active += 1
                    print active
        print 'sum rcount', rcount_sum
        discuss = 12*(float(rcount_sum)) + 8*(float(ccount_sum))
        # discuss
        tempData.append(discuss)
        # exposure
        tempData.append(flash_sum/1000)
        # attention
        tempData.append(count)
        # active
        active = (active + 3.5)*3
        tempData.append(active)
        # date
        tempData.append(otherStyleTime)
        # program_id
        tempData.append(program_id)
        # one_program
        tempData.append(one_program)
        #回复数，即是评论总和
        tempData.append(ccount_sum)
        #转发数
        tempData.append(rcount_sum)
        # 正面数，从customer_evaluation中获得
        tempData.append(positiveNum)
    #     负面数
        tempData.append(negativeNum)
        # 粉丝增长数
        tempData.append("")
    #         插入数据
        sqlcursor.execute('''insert into multiple_evaluation(evaluation, discuss, exposure, attention, active, date, program_id,
                            program, reply, repost, positive_comment, negative_comment, fans_increased) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',tempData)
        sqlConn.commit()
        tempData = []

    sqlConn.close()
    
if __name__=='__main__':
    weiboTest = addTwitterTable(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', dbname = 'btv_v2')
    
    
    
    
    
    
