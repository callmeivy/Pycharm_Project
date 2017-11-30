#coding:UTF-8
'''
Created on 2016年9月27日

@author: Ivy

'''
import sys,os
from sys import path
path.append('tools/')
path.append(path[0]+'/tools')
import MySQLdb
import time
from impala.dbapi import connect
reload(sys)
sys.setdefaultencoding('utf8')
import json
import base64

def behavior(mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'weibo'):
    now = int(time.time())
    timeArray = time.localtime(now)
    # otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    print otherStyleTime
    # 连接数据库
    sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()
    sqlcursor.execute('''create table IF NOT EXISTS loyal_customer(common_device_id varchar(50), date_sction varchar(20), update_datetime datetime);''')
    # sqlcursor.execute('''CREATE TABLE IF NOT EXISTS vedio_index(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, total_index bigint(20), play_count bigint(20), comment_count bigint(20), date Date,
    #                 program_id varchar(200), program varchar(200)) DEFAULT CHARSET=utf8;''')
    # print '新建库成功'
    # sqlcursor.execute("select substring(page_start_time,12,2) from cctv5_app_behavior limit 2;")
    # **********每个小时区间的duration总和**********
    # sqlcursor.execute("select substring(page_start_time,12,2), sum(page_duration)/60000 from cctv5_app_behavior group by substring(page_start_time,12,2);;")
    # **********每个小时区间的独立用户数**********
    # sqlcursor.execute("select substring(page_start_time,12,2), count(distinct(common_device_id)) from cctv5_app_behavior group by substring(page_start_time,12,2);")
    # **********看直播的用户数**********
    # sqlcursor.execute("select count(distinct(common_device_id)) from cctv5_app_behavior where substring(page_content_id,1,3)='Oly';")

    # **********看点播的用户数**********
    # sqlcursor.execute("select count(distinct(common_device_id)) from cctv5_app_behavior where substring(page_content_id,1,3)='VID';")

    # **********当日多少独立用户**********
    # sqlcursor.execute("select count(distinct(common_device_id))from cctv5_app_behavior;")
    # 不太对sqlcursor.execute("select distinct(common_device_id) from cctv5_app_behavior inner join (select distinct(common_device_id) from cctv5_app_behavior where (substring(page_content_id,1,3)='Oly')) t0 using (common_device_id) inner join (select distinct(common_device_id) from cctv5_app_behavior where (substring(page_content_id,1,3)='VID')) t1 using (common_device_id);")
    # **********看文章的用户数**********
    # sqlcursor.execute("select count(distinct(common_device_id)) from cctv5_app_behavior where substring(page_content_id,1,3)='ART';")
    #
    #
    #
    # live_device = list()
    # video_device = list()
    # sqlcursor.execute("select distinct(common_device_id) from cctv5_app_behavior where substring(page_content_id,1,3)='Oly';")
    # oly_device = sqlcursor.fetchall()
    # for one in oly_device:
    #     # print one[0]
    #     if one[0] not in live_device:
    #         live_device.append(one[0])
    # live_device = set(live_device)
    # # print "live",len(live_device)
    #
    # sqlcursor.execute("select distinct(common_device_id) from cctv5_app_behavior where substring(page_content_id,1,3)='VID';")
    # vid_device = sqlcursor.fetchall()
    # for single in vid_device:
    #     if single[0] not in video_device:
    #         video_device.append(single[0])
    # video_device = set(video_device)
    # # print "VOD",len(video_device)
    # intersection = live_device & video_device
    # union = live_device | video_device
    # print "intersection", len(intersection)
    # print "union",len(union)

    conn = connect(host = '192.168.168.43', port=21050)
    cur = conn.cursor()
    all_users = list()
    cur.execute('SELECT distinct(common_device_id) FROM cctv5_app_behavior limit 1;')
    all_device = cur.fetchall()
    # for row in cur:
    #     # print row[0]
    #     all_users.append(row[0])
    # user_number = len(all_users)
    # 318644
    # for date in range(16,19):
    #     print date--------16,17,18
    # **********以下array就是list**********
    array = list()
    loyal_customer = list()
    tempData = list()
    for single_user in all_device:
        for date in range(16,23):
            # print "date",date
            # cur.execute("""SELECT count(*) FROM cctv5_app_behavior where cast(substring(page_start_time,9,2) AS INTEGER)=%s and common_device_id = %s""", (date, single_user[0]))
            # test
            cur.execute("""SELECT count(*) FROM cctv5_app_behavior where cast(substring(page_start_time,9,2) AS INTEGER)=%s and common_device_id = '869288021734042';""", (date, ))
            # 16号52
            times_count = cur.fetchall()
            for row in times_count:
                # print row[0]
                if row[0]!= 0:
                    array.append(1)
                else:
                    array.append(0)
        # print array,sum(array)
        if sum(array) > 5:
            tempData.append(single_user[0])
            tempData.append('2016.11.16-2016.11.22')
            tempData.append(otherStyleTime)
            sqlcursor.execute('''insert into loyal_customer(common_device_id, date_sction, update_datetime)
                                values (%s, %s, %s)''',tempData)
            sqlConn.commit()
            tempData = []


























    sqlConn.close()


if __name__=='__main__':
    cctv5Test = behavior(mysqlhostIP = '192.168.168.105', dbname = 'weibo')

    
