#coding:UTF-8
'''
Created on 2016年3月4日

@author: Ivy
最新新闻
结果对应sql表是newest_news
英文日期应该转化为2016-03-04的格式
4月5日补充，
加上日期这一维度
'''
import sys,os
from sys import path
path.append('tools/')
path.append(path[0]+'/tools')
import MySQLdb
# 工具类
import jieba
import datetime
import time
import json
import base64
#from requests_kerberos import HTTPKerberosAuth, OPTIONAL

reload(sys)
sys.setdefaultencoding('utf8')
import requests

def issuccessful(request):
    if 200 <= request.status_code and request.status_code <= 299:
        return True
    else:
        return False


def addCommentTable(baseurl,mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv_v2'):
    # 连接数据库
    sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()
    # 这里的date修改为varchar格式了
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS newest_news(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, news_title varchar(500), source varchar(500), date varchar(10), news_content varchar(500), region varchar(500),
                    url varchar(500)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'
    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    inter = 10
    now = int(time.time())-86400*inter
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    print otherStyleTime
    sqlcursor.execute("SELECT DISTINCT(program_name) from ini_app_program_source_rel")
    bufferTemp = sqlcursor.fetchall()
    for one_program in bufferTemp:
        tempData = []
        title_box = list()
        one_program = one_program[0]
        sqlcursor.execute("""select source_path from ini_app_program_source_rel where app_name = '最新新闻' and program_name = %s""",(one_program,))
        source_path = sqlcursor.fetchone()
        tablename = source_path[0]
        print "source_path", source_path
        r = requests.get(baseurl + "/" + tablename + "/*",  auth=kerberos_auth, headers = {"Accept" : "application/json"})
        if issuccessful(r) == False:
            print "Could not get messages from HBase. Text was:\n" + r.text
        # quit()
        bleats = json.loads(r.text)
        for row in bleats['Row']:
            title = ''
            source = ''
            content =''
            url = ''
            columnname = ''
            lennnn = 0
            flag_date = False
            flag_from = False
            flag_title = False
            for cell in row['Cell']:
                columnname = base64.b64decode(cell['column'])
                value = cell['$']
                if value == None:
                    print 'none'
                    continue
                if columnname == "base_info:datetime":
                    date_time = base64.b64decode(value)
                    # print 'b4', date_time
                    if ('年' in date_time):
                        date_time = date_time.replace('年','-')
                    if ('月' in date_time):
                        date_time =date_time.replace('月','-')
                    if ('日' in date_time):
                        date_time =date_time.replace('日',' ')
                    if 'Janurary' in date_time:
                        if ',' in date_time:
                            year = date_time.split(',')[1]
                            date = (date_time.split(',')[0]).split(' ')[1]
                            if len(date) > 1:
                                date_time = year+'-01-'+ date + ' 08:00:00'
                            else:
                                date_time = year+'-01-0'+ date + ' 08:00:00'
                        else:
                            date_time = date_time[-4:] +'-01-' + date_time[0:2]+' 08:00:00'
                        # print 'af', date_time
                    if 'February' in date_time:
                        if ',' in date_time:
                            year = date_time.split(',')[1]
                            date = (date_time.split(',')[0]).split(' ')[1]
                            if len(date) > 1:
                                date_time = year+'-02-'+ date + ' 08:00:00'
                            else:
                                date_time = year+'-02-0'+ date + ' 08:00:00'
                        else:
                            date_time = date_time[-4:] +'-02-' + date_time[0:2]+' 08:00:00'
                        # print 'af', date_time
                    if 'March' in date_time:
                        if ',' in date_time:
                            year = date_time.split(',')[1]
                            date = (date_time.split(',')[0]).split(' ')[1]
                            if len(date) > 1:
                                date_time = year+'-03-'+ date + ' 08:00:00'
                            else:
                                date_time = year+'-03-0'+ date + ' 08:00:00'
                        else:
                            date_time = date_time[-4:] +'-03-' + date_time[0:2]+' 08:00:00'
                        # print 'ma', date_time
                        # print 'af', date_time
                    if 'April' in date_time:
                        if ',' in date_time:
                            year = date_time.split(',')[1]
                            date = (date_time.split(',')[0]).split(' ')[1]
                            if len(date) > 1:
                                date_time = year+'-04-'+ date + ' 08:00:00'
                            else:
                                date_time = year+'-04-0'+ date + ' 08:00:00'
                        else:
                            date_time = date_time[-4:] +'-04-' + date_time[0:2]+' 08:00:00'
                        # print 'af', date_time
                    if 'May' in date_time:
                        if ',' in date_time:
                            year = date_time.split(',')[1]
                            date = (date_time.split(',')[0]).split(' ')[1]
                            if len(date) > 1:
                                date_time = year+'-05-'+ date + ' 08:00:00'
                            else:
                                date_time = year+'-05-0'+ date + ' 08:00:00'
                            date_time = date_time[1:20]
                            # print 'may', len(date_time),date_time
                        else:
                            date_time = date_time[-4:] +'-05-' + date_time[0:2]+' 08:00:00'
                        # print 'af', date_time
                    if 'June' in date_time:
                        if ',' in date_time:
                            year = date_time.split(',')[1]
                            date = (date_time.split(',')[0]).split(' ')[1]
                            if len(date) > 1:
                                date_time = year+'-06-'+ date + ' 08:00:00'
                            else:
                                date_time = year+'-06-0'+ date + ' 08:00:00'
                        else:
                            date_time = date_time[-4:] +'-06-' + date_time[0:2]+' 08:00:00'
                        # print 'af', date_time
                    if 'July' in date_time:
                        if ',' in date_time:
                            year = date_time.split(',')[1]
                            date = (date_time.split(',')[0]).split(' ')[1]
                            if len(date) > 1:
                                date_time = year+'-07-'+ date + ' 08:00:00'
                            else:
                                date_time = year+'-07-0'+ date + ' 08:00:00'
                        else:
                            date_time = date_time[-4:] +'-07-' + date_time[0:2]+' 08:00:00'
                        # print 'af', date_time
                    if 'August' in date_time:
                        if ',' in date_time:
                            year = date_time.split(',')[1]
                            date = (date_time.split(',')[0]).split(' ')[1]
                            if len(date) > 1:
                                date_time = year+'-08-'+ date + ' 08:00:00'
                            else:
                                date_time = year+'-08-0'+ date + ' 08:00:00'
                        else:
                            date_time = date_time[-4:] +'-08-' + date_time[0:2]+' 08:00:00'
                        # print 'af', date_time
                    if 'September' in date_time:
                        if ',' in date_time:
                            year = date_time.split(',')[1]
                            date = (date_time.split(',')[0]).split(' ')[1]
                            if len(date) > 1:
                                date_time = year+'-09-'+ date + ' 08:00:00'
                            else:
                                date_time = year+'-09-0'+ date + ' 08:00:00'
                        else:
                            date_time = date_time[-4:] +'-09-' + date_time[0:2]+' 08:00:00'
                        # print 'af', date_time
                    if 'October' in date_time:
                        if ',' in date_time:
                            year = date_time.split(',')[1]
                            date = (date_time.split(',')[0]).split(' ')[1]
                            if len(date) > 1:
                                date_time = year+'-10-'+ date + ' 08:00:00'
                            else:
                                date_time = year+'-10-0'+ date + ' 08:00:00'
                        else:
                            date_time = date_time[-4:] +'-10-' + date_time[0:2]+' 08:00:00'
                        # print 'af', date_time
                    if 'November' in date_time:
                        if ',' in date_time:
                            year = date_time.split(',')[1]
                            date = (date_time.split(',')[0]).split(' ')[1]
                            if len(date) > 1:
                                date_time = year+'-11-'+ date + ' 08:00:00'
                            else:
                                date_time = year+'-11-0'+ date + ' 08:00:00'
                        else:
                            date_time = date_time[-4:] +'-11-' + date_time[0:2]+' 08:00:00'
                        # print 'af', date_time
                    if 'December' in date_time:
                        if ',' in date_time:
                            year = date_time.split(',')[1]
                            date = (date_time.split(',')[0]).split(' ')[1]
                            if len(date) > 1:
                                date_time = year+'-12-'+ date + ' 08:00:00'
                            else:
                                date_time = year+'-12-0'+ date + ' 08:00:00'
                        else:
                            date_time = date_time[-4:] +'-12-' + date_time[0:2]+' 08:00:00'
                    # print 'af', date_time
                    if len(date_time) == 16 :
                        date_time = date_time + ':00'
                    if (len(date_time) == 19):
                        date_time = date_time[0:16] + ':00'
                    if (date_time[0:10] >= otherStyleTime) and (len(date_time) == 19):
                        flag_date = True
                        # print 'hh', date_time,len(date_time)
                    else:
                        break

                if columnname == "base_info:news_from":
                    source = base64.b64decode(value)
                    if (('Jane' in source) or ('SinoDefence' in source) or ('DefenceTalk' in source)) :
                        flag_from=True
                    else:
                        break
                if columnname == "base_info:title":
                    title = base64.b64decode(value)
                    # print 'title',len(title), title
                    if len(title) > 3 and (title not in title_box):
                        # print 11111111111
                        title_box.append(title)
                        flag_title=True
                    else:
                        break
                if columnname == "base_info:website":
                    url = base64.b64decode(value)
                if columnname == "body:content":
                    content = base64.b64decode(value)
                if columnname == "base_info:type":
                    type = base64.b64decode(value)

            if flag_date and flag_from and flag_title:
            # 处理每一条
                # 新闻标题
                print title,type,source
                print 'tttttype', type
    sqlConn.close()


if __name__=='__main__':
    #commentTest = addCommentTable(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', dbname = 'btv_v2')
    #esql = """ sdf----%s----%s""", ('sddd', 'ddd')
    #print esql
    testnum = "234"
    print testnum,len(testnum),"\n"
    testnum = 234
    print testnum,len(testnum),"\n"


