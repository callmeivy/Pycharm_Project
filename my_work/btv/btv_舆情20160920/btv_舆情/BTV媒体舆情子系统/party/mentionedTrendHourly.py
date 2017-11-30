#coding:UTF-8
'''
Created on 2016年3月26日
@author: Ivy
提及量趋势(小时)
'''
import sys,os
import MySQLdb
from collections import Counter
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


def mentioned_trend(baseurl,mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv_v2'):
    # 晚会日期,注意格式啊，2016-02-09?
    gala_date = '2016-02-08'
    # 分词
    # 连接数据库
    sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS gala_mentioned_trend_hourly(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, mentioned bigint(50), date Date, starting_hour int(10), program_id varchar(50), program varchar(50)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'

    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    # 表名
    tablename = "DATA:WEIBO_POST_Keywords"
    r = requests.get(baseurl + "/" + tablename + "/*",  auth=kerberos_auth, headers = {"Accept" : "application/json"})
    if issuccessful(r) == False:
        print "Could not get messages from HBase. Text was:\n" + r.text
    # quit()
    bleats = json.loads(r.text)

    
    # 存储评论数据
    tempData = []
    count = 0
    printCount = 0
    # row_prefix, limit可以限定次数
    # for key,data in table.scan(limit = 10, batch_size = 10):
    date_collection = list()
    date_mentioned_dict = dict()
    hour_box = list()
        # bleats is json file
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
            if ("北京卫视春晚"  not in column) and ("北京台的春晚" not in column) and ("BTV春晚" not in column) and ("BTV春晚" not in column) and ("bTV春晚" not in column):
                    flag = False
                    break
            if columnname == "base_info:cdate":
                cdate = base64.b64decode(value)
                date_date = cdate.split('T')[0]
                print 'kk',date_date
                # 筛选晚会当天记录
                if date_date != gala_date:
                    flag = False
                    break
        if flag:
            hour = cdate.split(' ')[1].split(':')[0]
            hour_box.append(hour)
    hour_box_count = dict(Counter(hour_box))
    hour_box_count = sorted(hour_box_count.iteritems(), key=lambda e:e[1], reverse=True)
    for i in hour_box_count:

        starting_hour = i[0]
        print starting_hour
        how_many_times = i[1]
        print how_many_times
        # 提及量
        tempData.append(how_many_times)
        # 日期
        tempData.append(str(gala_date))
        # starting_hour
        tempData.append(starting_hour)
        # program_id
        tempData.append('100')
        # 节目名称
        tempData.append('2016年北京卫视春节联欢晚会')
        sqlcursor.execute('''insert into gala_mentioned_trend_hourly(mentioned, date, starting_hour, program_id, program) values (%s, %s, %s, %s, %s)''',tempData)
        sqlConn.commit()
        tempData = []
    sqlConn.close()


if __name__=='__main__':
    commentTest = mentioned_trend(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', dbname = 'btv_v2')

    
