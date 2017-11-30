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
from requests_kerberos import HTTPKerberosAuth, OPTIONAL

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
    sqlcursor.execute('''delete from newest_news where region = 'domestic';''')
    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    # 表名
    commentsData = []
    tempData = []
    inter = 5
    now = int(time.time())-86400*inter
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    print otherStyleTime
    sqlcursor.execute("SELECT DISTINCT(program_name) from ini_app_program_source_rel")
    bufferTemp = sqlcursor.fetchall()
    for one_program in bufferTemp:
        one_program = one_program[0]
        # tablename = "DATA:NEWS_Content"
        sqlcursor.execute("""select source_path from ini_app_program_source_rel where app_name = '最新新闻' and program_name = %s""",(one_program,))
        source_path = sqlcursor.fetchone()
        try:
            tablename = source_path[0]
        except:
            print one_program,'error data'
            continue
        print one_program,"source_path", source_path
        r = requests.get(baseurl + "/" + tablename + "/*",  auth=kerberos_auth, headers = {"Accept" : "application/json"})
        if issuccessful(r) == False:
            print "Could not get messages from HBase. Text was:\n" + r.text
        # quit()
        bleats = json.loads(r.text)
        sqlcursor.execute('''SELECT program_id from ini_app_program_source_rel where program_name = %s limit 1;''', (one_program,))
        bufferTemp = sqlcursor.fetchone()
        program_id = bufferTemp[0]
        sqlcursor.execute("SELECT program_type_name from ini_app_program_source_rel where program_name = %s limit 1;", (one_program,))
        bufferTemp = sqlcursor.fetchone()
        program_type_name = bufferTemp[0]
        print 'kk', one_program, program_type_name
        for row in bleats['Row']:
            title = ''
            source = ''
            content =''
            url = ''
            flag_type = False
            flag_title = False
            flag_date = False
            for cell in row['Cell']:
                columnname = base64.b64decode(cell['column'])
                value = cell['$']
                if value == None:
                    print 'none'
                    continue
                if columnname == "base_info:datetime":
                    date_time = base64.b64decode(value)
                    if " 2016" in date_time:
                        date_time = date_time[3:]
                    if ('年' in date_time):
                        date_time = date_time.replace('年','-')
                    if ('月' in date_time):
                        date_time =date_time.replace('月','-')
                    if ('日' in date_time):
                        date_time =date_time.replace('日',' ')
                    if len(date_time) == 16:
                        date_time = date_time + ':00'
                    if len(date_time) == 18:
                        date_time = date_time[:10] + ' ' + date_time[10:19]
                    if len(date_time) == 10:
                        date_time = date_time + ' 08:00:00'
                    if len(date_time) == 8:
                        continue
                    if len(date_time) == 22:
                        date_time = str(date_time)[0:10] + ' ' + str(date_time)[11:19]
                    # print "date_time", date_time
                    transform_date_time = date_time.split(' ')[0]
                    if transform_date_time >= otherStyleTime:
                        flag_date = True
                    else:
                        break
                if columnname == "base_info:type":
                    type_0 = base64.b64decode(value)
                    # 这里一定要用==,不能用in
                    if program_type_name in type_0:
                    # if (type_0 == '军事') or (type_0 == '新浪军事') or (type_0 == '搜狐军事') or (type_0 == '滚动军事') or (type_0 == '凤凰军事') or (type_0 == '军事要闻'):
                        flag_type = True
                    else:
                        break
                if columnname == "body:content":
                    content = base64.b64decode(value)
                if columnname == "base_info:news_from":
                    source = base64.b64decode(value)

                if columnname == "base_info:title":
                    title = base64.b64decode(value)
                if columnname == "base_info:website":
                    url = base64.b64decode(value)
            if flag_type and flag_date:
                # print 'title' ,title, url
            # 处理每一条
                # 新闻标题
                tempData.append(title)
                # 新闻来源
                tempData.append(source)
            #     日期时间
            #     print 'dd',date_time
                tempData.append(date_time)
                # 新闻正文
                tempData.append(content)
                # 国内还是国外
                # if source == 'Janes':
                #     tempData.append('foreign')
                # else:
                tempData.append('domestic')
                # url
                tempData.append(url)
                # program_id
                tempData.append(program_id)
            #     转换为元组
            #     sqlcursor.execute('''delete from newest_news where region = 'domestic';''')
                sqlcursor.execute('''insert into newest_news (news_title, source, date, news_content, region, url, program_id)
                            values (%s, %s, %s, %s, %s, %s, %s)''',tempData)
                sqlConn.commit()
                tempData = []
    sqlConn.close()


if __name__=='__main__':
    commentTest = addCommentTable(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', dbname = 'btv_v2')

    
