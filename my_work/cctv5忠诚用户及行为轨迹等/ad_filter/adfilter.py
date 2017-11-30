#coding:UTF-8
'''
Created on 2017年4月11日

@author: Ivy

'''
import sys,os
from sys import path
import MySQLdb
import time
reload(sys)
sys.setdefaultencoding('utf8')
from collections import Counter

def ad_filter(mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'weibo'):
    now = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(int(time.time())))
    print now
    f = open('E:\ctvit\lab\Project\myfile.txt', 'w')
    # 连接数据库
    sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()
    # print '新建库成功'
    # sqlcursor.execute("select distinct(user_name) from cctv_user_comment_default where commenttime between 1490112000 and 1490198399 group by user_name")
    sqlcursor.execute("select distinct(user_name) from cctv_user_comment_default group by user_name")
    user_list = list(sqlcursor.fetchall())
    # user_list = ["玩味","群6691402","可可","怎言笑","来要我"]
    # user_list = ["来要我"]
    for one_user in user_list:
        one_user = one_user[0]
        sqlcursor.execute("""select user_name, comment_content from cctv_user_comment_default where user_name like %s""",(one_user,))
        result = sqlcursor.fetchall()
        count = 0
        comment_box = list()
        user_box = list()
        for user, comment in result:
            # print user, comment
            # user_box.append(user)
            # if user in user_box:
            count += 1
            comment_box.append(comment)
        duplicated_comment = Counter(comment_box).most_common()
        # print "error", duplicated_comment
        try:
            if count == 1:
                duplicated_rate = 0
            elif duplicated_comment[0][1] == 1:
                duplicated_rate = 0
            else:
                duplicated_rate = round(float(duplicated_comment[0][1])/float(count),2)
            # print user, count, duplicated_rate
            line = str(user.encode('utf-8')) + " " + str(count) + " " + str(duplicated_rate)
            f.write("%s\n" %(line))  # python will convert \n to os.linesep
        except:
            print 'error'
    print "done!"
    f.close()

if __name__ == '__main__':
    cctv5Test = ad_filter(mysqlhostIP='192.168.168.105', dbname='weibo')