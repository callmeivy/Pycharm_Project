#coding:UTF-8
'''
Created on 2016年3月2日

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

    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS media_evaluation(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, flag int(1), evaluation bigint(20), content varchar(200), date Date,
                    program_id varchar(200), program varchar(200)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'
    # 时间属性
    # inter为0，即为当日
    # 库中是2.29
    inter = 2
    now = int(time.time())-86400*inter
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    print otherStyleTime
    # **********new by Ivy**********************************************
    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    # **********new by Ivy**********************************************
    # 存储评论数据
    # 处理情感
    emProcess = emotionProcess()
    rmIrr = removeIrrelevant()
    sqlcursor.execute("SELECT DISTINCT(program_name) from ini_app_program_source_rel")
    # sqlcursor.execute("SELECT program_name from ini_app_program_source_rel where program_name = '中国好歌曲第三季' limit 1;")
    bufferTemp = sqlcursor.fetchall()
    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    # tablename = "DATA:WEIBO_POST_Keywords"
    # sqlcursor.execute('''select source_path from ini_app_program_source_rel where program_name = '军情解码' and app_name = '观众口碑';''')
    tibeid_name = dict()
    tiba_id_box = list()
    for one_program in bufferTemp:
        commentsData = []
        tempData = []
        one_program = one_program[0].encode('utf8')
        # print one_program
        emotionsWord = []
        emotionsScore = 0
        count = 0
        printCount = 0
        # **********new by Ivy**********************************************
        tablename = "DATA:TIEBA_SITE"
        prefix_0 = otherStyleTime + "*"
        r = requests.get(baseurl + "/" + tablename + "/" + prefix_0,  auth=kerberos_auth, headers = {"Accept" : "application/json"})
        if issuccessful(r) == False:
            print "Could not get messages from HBase. Text was:\n" + r.text
        bleats = json.loads(r.text)
        # **********new by Ivy**********************************************
        for row in bleats['Row']:
            flag = True
            row_key = base64.b64decode(row['key'])
            tiba_id = row_key.split("-")[3]
            for cell in row['Cell']:
                columnname = base64.b64decode(cell['column'])
                value = cell['$']
                if value == None:
                    print 'none'
                    continue
                if columnname == "base_info:name":
                    tieba_name = base64.b64decode(value)
                    if (one_program  not in tieba_name):
                        flag = False
                        break
            if flag:
                # print "jjj", tiba_id,tieba_name
                tibeid_name[tiba_id] = tieba_name
                if tiba_id not in tiba_id_box:
                    tiba_id_box.append(tiba_id_box)
    print "len(tiba_id_box)", len(tiba_id_box)
    tablename = "DATA:TIEBA_SITE_Reply"
    for one_tieba_id in tiba_id_box:
        for i,j in tibeid_name.iteritems():
            if one_tieba_id == i:
                one_program = j
        prefix_1 = otherStyleTime + "-" + str(i) + "*"
        r = requests.get(baseurl + "/" + tablename + "/" + prefix_1,  auth=kerberos_auth, headers = {"Accept" : "application/json"})
        if issuccessful(r) == False:
            print "Could not get messages from HBase. Text was:\n" + r.text
        bleats = json.loads(r.text)
        sqlcursor.execute('''SELECT program_id from ini_app_program_source_rel where program_name = %s limit 1;''', (one_program,))
        bufferTemp = sqlcursor.fetchone()
        program_id = bufferTemp[0]
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
                if columnname == "base_info:content":
                    content = base64.b64decode(value)
                    # print 'hh', content
            if flag:
                # print one_tieba_id, content
            # 暂时没有program_id
            # 处理每一条
            #     情感关键词
                (emotionsWord,emotionsScore) = emProcess.processSentence(rmIrr.removeEverythingButEmotion(content))
            #     倾向性判断flag:1是正面，0是中性，-1是负面

            #     情感极性判断，这里我限制了更严格的条件
                if emotionsScore>0:
                    tempData.append('1')
                elif emotionsScore==0:
                    tempData.append('0')
                elif emotionsScore<0:
                    tempData.append('-1')
        #         情感得分sentimentScore
                tempData.append(emotionsScore)
                # 评论内容
                tempData.append(content)
            #     日期时间
                otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
                tempData.append(otherStyleTime)
                # 栏目id
                tempData.append(program_id)
                # 栏目名称
                tempData.append(one_program)
            #     转换为元组

                sqlcursor.execute('''insert into media_evaluation(flag, evaluation, content, date, program_id, program)
                            values (%s, %s, %s, %s, %s, %s)''',tempData)
                sqlConn.commit()
                tempData = []
    sqlConn.close()


if __name__=='__main__':
    commentTest = addcustomerEvaluation_informal(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', dbname = 'btv_v2')

    
