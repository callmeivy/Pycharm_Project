#coding:UTF-8
'''
Created on 2016年3月26日
@author: Ivy
地域互动分析
4月5日补充，
加上日期这一维度
'''
import sys,os
import MySQLdb
import jieba
import time
from collections import Counter
reload(sys)
sys.setdefaultencoding('utf8')
import datetime
import json
import base64
from requests_kerberos import HTTPKerberosAuth, OPTIONAL

def issuccessful(request):
    if 200 <= request.status_code and request.status_code <= 299:
        return True
    else:
        return False

def mentioned_trend(baseurl,mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv'):
    # 分词
    jieba.initialize()
    # 连接数据库
    sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS gala_region_interaction(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, region varchar(50), interaction bigint(50), date Date, program_id varchar(50), program varchar(50)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'

    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    # 表名
    tablename = "DATA:WEIBO_POST_Keywords"
    r = requests.get(baseurl + "/" + tablename + "/*",  auth=kerberos_auth, headers = {"Accept" : "application/json"})
    if issuccessful(r) == False:
        print "Could not get messages from HBase. Text was:\n" + r.text
    quit()
    bleats = json.loads(r.text)

    # 存储评论数据
    tempData = []
    count = 0
    printCount = 0
    # row_prefix, limit可以限定次数
    # for key,data in table.scan(limit = 10, batch_size = 10):
    region_box = list()
    date_mentioned_dict = dict()
    # 时间属性
    # inter为0，即为当日
    inter = 0
    now = int(time.time())-86400*inter
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    print otherStyleTime

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
            if column == "春晚":
                if columnname == "'base_info:cdate'":
                    cdate = base64.b64decode(value)
                    if cdate == otherStyleTime:
                        if columnname == "'base_info:geo'":
                            city_mentioned = base64.b64decode(value)
    region_box_count = dict(Counter(region_box))
    region_box_count = sorted(region_box_count.iteritems(), key=lambda e:e[1], reverse=True)
    for i in region_box_count:
        city = i[0]
        how_many_times = i[1]
        # 地域
        tempData.append(city)
        # 出现次数
        tempData.append(how_many_times)
        # 日期，这是插表的时间
        now = datetime.datetime.now()
        tempData.append(now)
        # program_id
        tempData.append('12345')
        # 节目名称
        tempData.append('2016年北京卫视春节联欢晚会')
        sqlcursor.execute('''insert into gala_region_interaction(region, interaction, date, program_id, program) values (%s, %s, %s, %s, %s)''',tempData)
        sqlConn.commit()
        tempData = []
    sqlConn.close()


if __name__=='__main__':
    commentTest = mentioned_trend(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', dbname = 'btv')


