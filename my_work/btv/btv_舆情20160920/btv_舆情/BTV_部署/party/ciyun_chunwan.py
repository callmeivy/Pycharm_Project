#coding: UTF-8
'''
by Ivy
created on 2016年3月26日

'''
import sys
reload(sys)
import MySQLdb
import os
import time
from sys import path
sys.setdefaultencoding('utf8')
from collections import Counter
import datetime
import json
import base64
from requests_kerberos import HTTPKerberosAuth, OPTIONAL


def issuccessful(request):
    if 200 <= request.status_code and request.status_code <= 299:
        return True
    else:
        return False


# number是限定词云个数
def addhighFreqencyWords(baseurl,mysqlhostIP, programid, number,mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv'):
    # 晚会名称
    program = '2016年北京卫视春节联欢晚会'
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
    sqlConn=MySQLdb.connect(host=mysqlhostIP,user=mysqlUserName,passwd=mysqlPassword,db = dbname,charset='utf8')
    sqlcursor = sqlConn.cursor()
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS gala_ci_yun_chunwan(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, word varchar(50), freqency bigint(50), date Date,
                    program_id varchar(200), program varchar(200)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'


    # 读取微博正文内容

    # 存储评论数据
    commentsData = []
    tempData = []
    # 
    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    # 表名
    tablename = "DATA:WEIBO_POST_Keywords"
    r = requests.get(baseurl + "/" + tablename + "/*",  auth=kerberos_auth, headers = {"Accept" : "application/json"})
    if issuccessful(r) == False:
        print "Could not get messages from HBase. Text was:\n" + r.text
    quit()
    bleats = json.loads(r.text)

    sqlcursor.execute("SELECT DISTINCT(program) from hbase_info;")
    bufferTemp = sqlcursor.fetchall()

    count = 0
    printCount = 0
    Corpus_cut = list()
    body_content_box = list()
    # 时间属性
    # inter为0，即为当日
    inter = 0
    now = int(time.time())-86400*inter
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    print otherStyleTime

    for one_program in bufferTemp:
        one_program = one_program[0].encode('utf8')
        print type(one_program),one_program
        emotionsWord = []
        emotionsScore = 0
        count = 0
        printCount = 0
        sqlcursor.execute('''SELECT program_id from competition_analysis where program = %s''', (one_program,))
        bufferTemp = sqlcursor.fetchone()
        program_id = bufferTemp[0]
        print program_id

        # bleats is json file
        for row in bleats['Row']:
            for cell in row['Cell']:
                columnname = base64.b64decode(cell['column'])
                value = cell['$']
                if value == None:
                    print 'none'
                    continue
                if columnname == "base_info:match":
                    column = base64.b64decode(value)
                if column == one_program:
                    if columnname == "'base_info:cdate'":
                        date_created = base64.b64decode(value)

                        if date_created == otherStyleTime:
                            if columnname == "'base_info:text'":
                                body_content = base64.b64decode(value)
                                body_content_box.append(body_content)

    # 将空格以及停用词去除
        Corpus_cut_new = list()
        for i in Corpus_cut:
            if i !=' ':
                if i not in stopwordList:
            #         print 111
                    Corpus_cut_new.append(i)
        Corpus_cut = Corpus_cut_new

    # 词频及排序
        kandian_listcount = dict(Counter(Corpus_cut))
        kandian_listcount = sorted(kandian_listcount.iteritems(), key=lambda e:e[1], reverse=True)
        ind = 0
        rank = 0
        for item in kandian_listcount:
            count += 1
            printCount+=1

            ind += 1
            if ind > int(number):
                break
            rank += 1
            try:
                word = item[0].strip().encode('utf-8')
                print 'word',word
                how_many = item[1]
                print how_many
            except:
                pass
            # 高频词
            tempData.append(word)
            # 词频
            tempData.append(how_many)
            # 数据指向的日期
            tempData.append(date_created)
            # 栏目id
            tempData.append('12345')
            # 栏目名称
            tempData.append(program)
            commentsData.append(tuple(tempData))
            tempData = []
            sqlcursor.execute('''insert into gala_ci_yun_chunwan(word, freqency, date, program_id, program)
                        values (%s, %s, %s, %s, %s)''',commentsData)
            sqlConn.commit()
            commentsData = []
    sqlConn.close()



if __name__=='__main__':
    commentTest = addhighFreqencyWords(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', programid = '100', number = '10')
    # number：展示多少个热词