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
    inter = 1
    now = int(time.time())-86400*inter
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    print otherStyleTime
    # 存储评论数据
    # 处理情感
    emProcess = emotionProcess()
    rmIrr = removeIrrelevant()
    # 首先获取栏目,注意栏目及相关hbase存储信息有更新，请在hbase_info做同步
    # print 'SELECT DISTINCT(program) from hbase_info where source = %s' %source_1
    sqlcursor.execute("SELECT DISTINCT(program_name) from ini_app_program_source_rel")
    bufferTemp = sqlcursor.fetchall()
    for one_program in bufferTemp:
        commentsData = []
        tempData = []
        one_program = one_program[0].encode('utf8')
        print one_program
        emotionsWord = []
        emotionsScore = 0
        count = 0
        printCount = 0
        sqlcursor.execute("""select source_path from ini_app_program_source_rel where app_name = '最新新闻' and program_name = %s""",(one_program,))
        source_path = sqlcursor.fetchone()
        tablename = source_path[0]
        print "source_path", source_path
        os.popen('kinit -k -t ctvit.keytab ctvit')
        kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
        r = requests.get(baseurl + "/" + tablename + "/*",  auth=kerberos_auth, headers = {"Accept" : "application/json"})
        if issuccessful(r) == False:
            print "Could not get messages from HBase. Text was:\n" + r.text
        # quit()
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
                if columnname == "base_info:match":
                    column = base64.b64decode(value)
                    print 'column',column
                    if (one_program  not in column):
                        print 444444
                        flag = False
                        break
                if columnname == "base_info:cdate":
                    cdate = base64.b64decode(value)
                    cdate = cdate.split('T')[0]
                    print '22', cdate
                    if cdate != otherStyleTime:
                        print 55555
                        flag = False
                        break
                if columnname == "base_info:text":
                    content = base64.b64decode(value)
                    # print "content",content
            if flag:
                print 111111111111
                print column
            # 暂时没有program_id
            # program_id = data['base_info:program_id']
                count += 1
                printCount+=1
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

    
