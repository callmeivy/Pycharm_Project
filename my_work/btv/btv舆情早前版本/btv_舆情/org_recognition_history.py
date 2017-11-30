#coding:UTF-8
'''
Created on 2017年4月17日

@author: Ivy
机构名识别
'''
import MySQLdb
import jieba.posseg as pseg
import time

def org_recognition(mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv_v2'):
    # 连接数据库
    sqlConn = MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db=dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()

    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS org_recognition(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, content varchar(200), organization varchar(50), date Date,
                        program_id varchar(200), program varchar(200)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'
    # 每天处理昨日内容
    # inter = 346，2017-4-17，2016-05-06
    inter = 1
    now = int(time.time()) - 86400 * inter
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    print otherStyleTime
    sqlcursor.execute('''Delete from org_recognition where date = %s;''',(otherStyleTime,))
    sqlcursor.execute('''INSERT INTO org_recognition(content, date, program_id, program) select content, date, program_id, program from customer_evaluation;''')
    sqlConn.commit()
    sqlcursor.execute('''select content from org_recognition;''')
    bufferTemp = sqlcursor.fetchall()
    for one in bufferTemp:
        org_box = list()
        words = pseg.cut(one[0])
        for word in words:
            if word.flag =='nt':
                org_box.append(word.word)
                # print word.word,word.flag
        orgnization = ",".join(org_box)
        sqlcursor.execute('''Update org_recognition set organization=%s where content like %s''', (orgnization, one[0]))
        sqlConn.commit()

if __name__ == '__main__':
    # commentTest = org_recognition(mysqlhostIP='172.28.34.16', dbname='btv_v2')
    commentTest = org_recognition(mysqlhostIP='192.168.168.105', dbname='btv_v2')