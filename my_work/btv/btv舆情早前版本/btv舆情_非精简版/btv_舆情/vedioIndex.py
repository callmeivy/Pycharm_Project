#coding:UTF-8
'''
Created on 2016年9月27日

@author: Ivy

'''
import sys,os
from sys import path
path.append('tools/')
path.append(path[0]+'/tools')
import MySQLdb
from emtionProcess import emotionProcess
# 工具类
from remove import removeIrrelevant
# from spammerDetection import spammerdetect
import time
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

def addcustomerEvaluation_informal(baseurl,mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv_v2'):

    # 读停用词
    path = os.path.abspath(os.path.dirname(sys.argv[0]))
    dicFile = open(path+'/tools/NTUSD_simplified/stopwords.txt','r')
    stopwords = dicFile.readlines()
    stopwordList = []
    stopwordList.append(' ')
    for stopword in stopwords:
        temp = stopword.strip().replace('\r\n','').decode('utf8')
        stopwordList.append(temp)
    dicFile.close() 

    # 连接数据库
    sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()

    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS vedio_index(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, total_index bigint(20), play_count bigint(20), comment_count bigint(20), date Date,
                    program_id varchar(200), program varchar(200)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'
    # 时间属性
    # inter为0，即为当日
    # 库中是2.29
    inter = 1
    now = int(time.time())-86400*inter
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    print otherStyleTime
    # **********new by Ivy**********************************************
    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    tablename_index = "DATA:VIDEO_ZONGYI_INDEX"
    prefix = otherStyleTime + "*"
    # print baseurl + "/" + tablename_index + "/" + prefix
    r_index = requests.get(baseurl + "/" + tablename_index + "/" + prefix,  auth=kerberos_auth, headers = {"Accept" : "application/json"})
    if issuccessful(r_index) == False:
        print "Could not get messages from HBase table VIDEO_ZONGYI_INDEX. Text was:\n" + r_index.text
    # print base64.b64decode(r.text)
    # print r_index.text
    bleats_0 = json.loads(r_index.text)
    row_key_name_list = list()
    for row in bleats_0['Row']:
        row_key = base64.b64decode(row['key'])
        row_key_name = row_key.split("-")[3]
        if row_key_name not in row_key_name_list:
            row_key_name_list.append(row_key_name)
    print len(row_key_name_list)
    # **********new by Ivy**********************************************
    emProcess = emotionProcess()
    rmIrr = removeIrrelevant()
    sqlcursor.execute("SELECT program_name from ini_app_program_source_rel where program_name = '跨界喜剧王' limit 1;")
    bufferTemp = sqlcursor.fetchall()
    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    for one_program in bufferTemp:
        commentsData = []
        tempData = []
        play_count = 0
        comment_count = 0
        play_count_box = list()
        comment_count_box = list()
        one_program = one_program[0].encode('utf8')
        if one_program == '中国好歌曲第三季':
            otherStyleTime = '2015-10-26'
        tablename = "DATA:VIDEO_ZONGYI"
        print one_program
        sqlcursor.execute('''SELECT program_id from ini_app_program_source_rel where program_name = %s limit 1;''', (one_program,))
        bufferTemp = sqlcursor.fetchone()
        program_id = bufferTemp[0]
        # **********new by Ivy**********************************************
        for single_rowkey in row_key_name_list:
            r = requests.get(baseurl + "/" + tablename + "/" + single_rowkey + "*",  auth=kerberos_auth, headers = {"Accept" : "application/json"})
            if issuccessful(r) == False:
                print "Could not get messages from HBase. Text was:\n" + r.text
            # print r.text
            try:
                bleats = json.loads(r.text)
            except:
                pass
        # **********new by Ivy**********************************************
            # bleats is json file
            vedio_name = ""
            for row in bleats['Row']:
                flag = True
                for cell in row['Cell']:
                    columnname = base64.b64decode(cell['column'])
                    value = cell['$']
                    if value == None:
                        print 'none'
                        continue
                    if columnname == "base_info:column_name":
                        column = base64.b64decode(value)
                        if (one_program  not in column):
                            flag = False
                            break
                    if columnname == "base_info:play_count":
                        play_count = base64.b64decode(value)
                        play_count = int(play_count)
                    if columnname == "base_info:comment_count":
                        comment_count = base64.b64decode(value)
                        comment_count = int(comment_count)
                    if columnname == "base_info:name":
                        vedio_name = base64.b64decode(value)
                if flag:
                    play_count_box.append(play_count)
                    comment_count_box.append(comment_count)
        # 播放指数
        total_play_count =  sum(play_count_box)
        total_comment_count =  sum(comment_count_box)
        total_index = total_play_count/10000000 *2 + total_comment_count/100
        print "total_index", total_index
        tempData.append(total_index)
        # 播放次数
        tempData.append(total_play_count)
        # 评论次数
        tempData.append(total_comment_count)
    #     日期时间
        otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
        tempData.append(otherStyleTime)
        # 栏目id
        tempData.append(program_id)
        # 栏目名称
        tempData.append(one_program)
    #     转换为元组
        sqlcursor.execute('''insert into vedio_index(total_index, play_count, comment_count, date, program_id, program)
                    values (%s, %s, %s, %s, %s, %s)''',tempData)
        sqlConn.commit()
        tempData = []
    sqlConn.close()


if __name__=='__main__':
    commentTest = addcustomerEvaluation_informal(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', dbname = 'btv_v2')

    
