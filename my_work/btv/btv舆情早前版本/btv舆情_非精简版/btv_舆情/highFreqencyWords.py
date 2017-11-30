#coding: UTF-8
'''
by Ivy
created on 2016年3月2日
高频评论词分析，抓取评论内容，做词云
high-freqency words
结果对应sql表是ci_yun_comment
3月24日补充，
以“JQJM”为关键词的微博原贴代替，按理应该是底下的评论
3月25日补充，
其他竞争栏目的词云也是从这里出
4月5日补充，
加上日期这一维度，并将其扩充到竞争栏目，新增维护hbase先关信息，
详细请看hbase_info
'''
import sys
reload(sys)
import MySQLdb
import os
from sys import path
sys.setdefaultencoding('utf8')
path.append('tools/')
path.append(path[0]+'/tools')
import jieba
from collections import Counter
import datetime
import time
import json
import base64
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import requests

def issuccessful(request):
    if 200 <= request.status_code and request.status_code <= 299:
        return True
    else:
        return False


# number是限定词云个数
def addhighFreqencyWords(baseurl,mysqlhostIP, number,mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv_v2'):
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
    sqlConn=MySQLdb.connect(host=mysqlhostIP,user=mysqlUserName,passwd=mysqlPassword,db = dbname,charset='utf8')
    sqlcursor = sqlConn.cursor()
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS ci_yun_comment(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, key_words varchar(200), frequency bigint(20), related_info varchar(200), date Date,
                    created_date datetime, rank bigint(20), program_id varchar(200), program varchar(200)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'
    # 读取微博正文内容
    # 连接hbase数据库
    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    # 时间属性
    # inter为0，即为当日
    inter = 1
    now = int(time.time())-86400*inter
    print 'day', now
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    print otherStyleTime
# **********new by Ivy**********************************************
    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
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
    # **********new by Ivy**********************************************
    # sqlcursor.execute("SELECT * from ini_app_program_source_rel;")gala_celebrity
    sqlcursor.execute("SELECT DISTINCT(program_name) from ini_app_program_source_rel")
    # sqlcursor.execute("SELECT DISTINCT(name) from gala_celebrity")
    bufferTemp = sqlcursor.fetchall()
    # 存储评论数据
    commentsData = []
    tempData = []
    for one_program in bufferTemp:
        body_content_box = list()
        Corpus_cut = list()
        one_program = one_program[0]
        # print 'sssss', one_program
        # one_program = one_program[0].encode('utf8')
        # print one_program
        # tablename = "DATA:WEIBO_POST_Keywords"
        sqlcursor.execute("""select source_path from ini_app_program_source_rel where app_name = '高频评论词' and program_name = %s""",(one_program,))
        source_path = sqlcursor.fetchone()
        tablename = source_path[0]
        # print one_program,"source_path", source_path
        print one_program
        sqlcursor.execute('''SELECT program_id from ini_app_program_source_rel where program_name = %s limit 1;''', (one_program,))
        bufferTemp = sqlcursor.fetchone()
        program_id = bufferTemp[0]
        # **********new by Ivy**********************************************
        for single_rowkey in row_key_name_list:
            r = requests.get(baseurl + "/" + tablename + "/" + single_rowkey,  auth=kerberos_auth, headers = {"Accept" : "application/json"})
            if issuccessful(r) == False:
                print "Could not get messages from HBase. Text was:\n" + r.text
            # print r.text
            bleats = json.loads(r.text)
        # **********new by Ivy**********************************************
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
                        if (one_program not in column):
                            flag = False
                            break
                    if columnname == "base_info:cdate":
                        cdate = base64.b64decode(value)
                        cdate = cdate.split('T')[0]
                        # if one_program == str(one_program):
                        #     if cdate != otherStyleTime:
                        #         flag = False
                        #         break
                    if columnname == "base_info:text":
                        body_content = base64.b64decode(value)
                if flag:
                    # print one_program, program_id, body_content
                    body_content_box.append(body_content)
                    Corpus_cut = Corpus_cut + list(jieba.cut(body_content,cut_all = False))
    # 将空格以及停用词去除
        Corpus_cut_new = list()
        for i in Corpus_cut:
            if (i !=' ') and len(i) > 1:
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
            ind += 1
            if ind > int(number):
                break
            rank += 1
            try:
                word = item[0].strip().encode('utf-8')

                how_many = item[1]
                # 以下两条都满足的情况下
                if (ind > 5) and (how_many == 1):
                    break
                # print 'word',word,how_many
            except:
                pass
            # 高频词
            tempData.append(word)
            # 词频
            tempData.append(how_many)
            # 排序
            tempData.append(rank)
            # 高频词所在的句子
            for one in body_content_box:
                if word in one:
                    tempData.append(one)
                    break;
            # 数据指向的日期
            tempData.append(otherStyleTime)
            # 插表的时间
            now = datetime.datetime.now()
            tempData.append(now)
            # 栏目id
            tempData.append(program_id)
            # 栏目名称
            tempData.append(one_program)

            sqlcursor.execute('''insert into ci_yun_comment(key_words, frequency, rank, related_info, date, created_date, program_id, program) values (%s, %s, %s, %s, %s, %s, %s, %s)''', tempData)
            sqlConn.commit()
            tempData = []
    sqlConn.close()



if __name__=='__main__':
    commentTest = addhighFreqencyWords(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', number = '20')
    # number：展示多少个热词