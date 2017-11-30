#coding: UTF-8
'''
by Ivy
created on 2016年3月2日
看点分析，抓取官微正文帖子，做词云
high-freqency words
结果对应sql表是ci_yun_kandian
3月24日补充
没有官微的数据
4月5日补充，
加上日期这一维度
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


def issuccessful(request):
    if 200 <= request.status_code and request.status_code <= 299:
        return True
    else:
        return False

# number是限定词云个数
def addkandianTable(baseurl,mysqlhostIP, programid, number,mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv'):
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
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS kandianTable(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, key_words varchar(200), frequency bigint(20), related_info varchar(200), date Date,
                    created_date datetime, rank bigint(20), program_id varchar(200), program varchar(200)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'

    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    # 表名
    tablename = "DATA:WEIBO_POST_chunwan"
    # hbase表格修改-revise
    # table=conn.table('weiboBodyTable')
    r = requests.get(baseurl + "/" + tablename + "/*",  auth=kerberos_auth, headers = {"Accept" : "application/json"})
    if issuccessful(r) == False:
        print "Could not get messages from HBase. Text was:\n" + r.text
    # quit()
    bleats = json.loads(r.text)
    # 读取微博正文内容

    # 存储评论数据
    commentsData = []
    tempData = []


    # 时间属性
    # inter为0，即为当日
    inter = 0
    now = int(time.time())-86400*inter
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    print otherStyleTime

    emotionsWord = []
    emotionsScore = 0
    count = 0
    printCount = 0
    Corpus_cut = list()
    body_content_box = list()
# bleats is json file
    for row in bleats['Row']:
        for cell in row['Cell']:
            columnname = base64.b64decode(cell['column'])
            value = cell['$']
            if value == None:
                print 'none'
                continue
            if columnname == "'base_info:cdate'":
                date_created = base64.b64decode(value)

            date_created= data['testColumn:date']
            if date_created == otherStyleTime:
                body_content = data['testColumn:body_content']
                # body_content = '菲律宾曾在1999年派一艘军舰坐滩我黄岩岛，之后在我强大压力被迫拖走。这艘军舰的舷号是多少？A: 57号 B：507号。答对问题的观众有机会获得张召忠教授亲笔签名的新书《史说岛争》，共5本，快快抢答吧'
                print 'yaya',type(body_content),body_content
                # body_content_box存储评论内容
                body_content_box.append(body_content)
                program_id = data['testColumn:program_id']
                # 用program_id做删选
                if program_id == programid:
                    program = data['testColumn:program']
                    Corpus_cut = Corpus_cut + list(jieba.cut(body_content,cut_all = False))
        except:
            pass
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
        # 高频词所在的句子
        for one in body_content_box:
            if word in one:
                tempData.append(one)
        # 数据指向的日期
        tempData.append(date_created)
        # 插表的时间
        now = datetime.datetime.now()
        tempData.append(now)
        # 排序
        tempData.append(rank)
        # 栏目id
        tempData.append(program_id)
        # 栏目名称
        tempData.append(program)
        commentsData.append(tuple(tempData))
        tempData = []
        if count>=10:
            sqlcursor.executemany('''insert into kandianTable(key_words, frequency, related_info, date, created_date, rank, program_id, program)
                        values (%s, %s, %s, %s, %s, %s, %s, %s)''',commentsData)
            sqlConn.commit()
            commentsData = []
            count = 0
            print '插入'+str(printCount)+'个'
        # # except:
        # #      print tempData
    sqlcursor.executemany('''insert into kandianTable(key_words, frequency, related_info, date, created_date, rank, program_id, program)
                        values (%s, %s, %s, %s, %s, %s, %s, %s)''',commentsData)
    sqlConn.commit()
    sqlConn.close()







if __name__=='__main__':
    commentTest = addkandianTable(hbaseIP = '192.168.168.41', mysqlhostIP = '10.3.3.182', programid = '100', number = '10')