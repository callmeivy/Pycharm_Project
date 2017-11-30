#coding:UTF-8
'''
Created on 2016年3月30日
@author: Ivy
关注度分析
'''
import sys,os
from sys import path
path.append('tools/')
path.append(path[0]+'/tools')
import MySQLdb
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
    # 连接数据库
    sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS gala_attention_degree(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, type varchar(10), content varchar(200), attention_degree bigint(50), date date, program_id varchar(50), program varchar(50)) DEFAULT CHARSET=utf8;''')
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

     # 明星话题
    sqlcursor.execute('''SELECT name FROM gala_celebrity;''')
    bufferTemp = sqlcursor.fetchall()
    star_count_dict = dict()
    for one_star in bufferTemp:
        one_star = one_star[0]
        print type(one_star)

        count = 0
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
                    if columnname == "'base_info:text'":
                        content = base64.b64decode(value)
                        print content
                        if one_star in content:
                            if columnname == "'base_info:cdate'":
                                date_created = base64.b64decode(value)
                                print 'll',one_star
                                count += 1
        # type
        tempData.append('明星话题')
        # content
        tempData.append(one_star)
        # attention_degree
        tempData.append(count)
        # date
        tempData.append(date_created)
        # program_id
        tempData.append('100')
        # program
        tempData.append('2016年北京卫视春节联欢晚会')
        sqlcursor.execute('''insert into gala_attention_degree(type, content, attention_degree, date, program_id, program) values (%s, %s, %s, %s, %s, %s)''',tempData)
        sqlConn.commit()
        tempData = []


    # 后台花絮,用互动量来衡量
    weibo_interaction = dict()
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
                if columnname == "'base_info:text'":
                    content = base64.b64decode(value)
                    print type(content)
                    # if ('花絮' in content) and (one_star in content):
                    if '花絮' in content:
                        # 转发数
                        if columnname == "'base_info:rcount'":
                            rcount = base64.b64decode(value)
                        # 评论数
                        if columnname == "'base_info:ccount'":
                            ccount = base64.b64decode(value)
                        # 点赞数
                        if columnname == "'base_info:acount'":
                            acount = base64.b64decode(value)
                        # 互动量
                        interaction = float(rcount) * 0.5 + float(ccount)*0.4 + float(acount)*0.1
                        if columnname == "'base_info:cdate'":
                            date_created = base64.b64decode(value)
                    weibo_interaction[str(content)] = interaction
    weibo_interaction = sorted(weibo_interaction.iteritems(), key=lambda e:e[1], reverse=True)
    ind = 0
    for i in weibo_interaction:
        ind += 1
        # 这里取多少个，就写多少，比如取3个，就写>3
        if ind > 10:
            break
        weibo_key = i[0]
        interaction_value = i[1]
        print weibo_key,interaction_value
        # type
        tempData.append('后台花絮')
        # content
        tempData.append(content)
        # attention_degree
        tempData.append(interaction_value)
        # date
        tempData.append(date_created)
        # program_id
        tempData.append('100')
        # program
        tempData.append('2016年北京卫视春节联欢晚会')
        sqlcursor.execute('''insert into gala_attention_degree(type, content, attention_degree, date, program_id, program) values (%s, %s, %s, %s, %s, %s)''',tempData)
        sqlConn.commit()
        tempData = []


    # 晚会宣传,同上
    weibo_interaction = dict()
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
                if columnname == "'base_info:text'":
                    content = base64.b64decode(value)
                    print type(content)
                    # if ('花絮' in content) and (one_star in content):
                    if '宣传' in content:
                        # 转发数
                        if columnname == "'base_info:rcount'":
                            rcount = base64.b64decode(value)
                        # 评论数
                        if columnname == "'base_info:ccount'":
                            ccount = base64.b64decode(value)
                        # 点赞数
                        if columnname == "'base_info:acount'":
                            acount = base64.b64decode(value)
                        # 互动量
                        interaction = float(rcount) * 0.5 + float(ccount)*0.4 + float(acount)*0.1
                        if columnname == "'base_info:cdate'":
                            date_created = base64.b64decode(value)
                    weibo_interaction[str(content)] = interaction
    weibo_interaction = sorted(weibo_interaction.iteritems(), key=lambda e:e[1], reverse=True)
    ind = 0
    for i in weibo_interaction:
        ind += 1
        # 这里取多少个，就写多少，比如取3个，就写>3
        if ind > 10:
            break
        weibo_key = i[0]
        interaction_value = i[1]
        print weibo_key,interaction_value
        # type
        tempData.append('晚会宣传')
        # content
        tempData.append(content)
        # attention_degree
        tempData.append(interaction_value)
        # date
        tempData.append(date_created)
        # program_id
        tempData.append('100')
        # program
        tempData.append('2016年北京卫视春节联欢晚会')
        sqlcursor.execute('''insert into gala_attention_degree(type, content, attention_degree, date, program_id, program) values (%s, %s, %s, %s, %s, %s)''',tempData)
        sqlConn.commit()
        tempData = []


    # 节目内容
    sqlcursor.execute('''SELECT item from gala_program_discussion;''')
    bufferTemp = sqlcursor.fetchall()
    for one_item in bufferTemp:
        one_item = one_item[0]
        weibo_interaction = dict()
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
                    if columnname == "'base_info:text'":
                        content = base64.b64decode(value)

                        if one_item in content:
                            # 转发数
                            if columnname == "'base_info:rcount'":
                                rcount = base64.b64decode(value)
                            # 评论数
                            if columnname == "'base_info:ccount'":
                                ccount = base64.b64decode(value)
                            # 点赞数
                            if columnname == "'base_info:acount'":
                                acount = base64.b64decode(value)
                            # 互动量
                            interaction = float(rcount) * 0.5 + float(ccount)*0.4 + float(acount)*0.1
                            if columnname == "'base_info:cdate'":
                                date_created = base64.b64decode(value)
                        weibo_interaction[str(content)] = interaction
    weibo_interaction = sorted(weibo_interaction.iteritems(), key=lambda e:e[1], reverse=True)
    ind = 0
    for i in weibo_interaction:
        ind += 1
        # 这里取多少个，就写多少，比如取3个，就写>3
        if ind > 10:
            break
        weibo_key = i[0]
        interaction_value = i[1]
        print weibo_key,interaction_value
        # type
        tempData.append('节目内容')
        # content
        tempData.append(content)
        # attention_degree
        tempData.append(interaction_value)
        # date
        tempData.append(date_created)
        # program_id
        tempData.append('100')
        # program
        tempData.append('2016年北京卫视春节联欢晚会')
        sqlcursor.execute('''insert into gala_attention_degree(type, content, attention_degree, date, program_id, program) values (%s, %s, %s, %s, %s, %s)''',tempData)
        sqlConn.commit()
        tempData = []

    sqlConn.close()


if __name__=='__main__':
    commentTest = mentioned_trend(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', dbname = 'btv')


