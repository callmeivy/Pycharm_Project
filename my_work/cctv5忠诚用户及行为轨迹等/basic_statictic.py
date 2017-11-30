#coding:UTF-8
'''
Created on 2016年12月13日

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
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS vedio_index(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, total_index bigint(20), play_count bigint(20), comment_count bigint(20), date Date,
                    program_id varchar(200), program varchar(200)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'
    sqlcursor.execute("select substring(page_start_time,12,2) from cctv5_app_behavior limit 2;")
    # **********每个小时区间的duration总和**********
    sqlcursor.execute("select substring(page_start_time,12,2), sum(page_duration)/60000 from cctv5_app_behavior group by substring(page_start_time,12,2);;")
    # **********每个小时区间的独立用户数**********
    sqlcursor.execute("select substring(page_start_time,12,2), count(distinct(common_device_id)) from cctv5_app_behavior group by substring(page_start_time,12,2);")
    # **********看直播的用户数**********
    sqlcursor.execute("select count(distinct(common_device_id)) from cctv5_app_behavior where substring(page_content_id,1,3)='Oly';")

    # **********看点播的用户数**********
    sqlcursor.execute("select count(distinct(common_device_id)) from cctv5_app_behavior where substring(page_content_id,1,3)='VID';")

    # **********当日多少独立用户**********
    sqlcursor.execute("select count(distinct(common_device_id))from cctv5_app_behavior;")
    # 不太对sqlcursor.execute("select distinct(common_device_id) from cctv5_app_behavior inner join (select distinct(common_device_id) from cctv5_app_behavior where (substring(page_content_id,1,3)='Oly')) t0 using (common_device_id) inner join (select distinct(common_device_id) from cctv5_app_behavior where (substring(page_content_id,1,3)='VID')) t1 using (common_device_id);")
    # **********看文章的用户数**********
    sqlcursor.execute("select count(distinct(common_device_id)) from cctv5_app_behavior where substring(page_content_id,1,3)='ART';")



    live_device = list()
    video_device = list()
    sqlcursor.execute("select distinct(common_device_id) from cctv5_app_behavior where substring(page_content_id,1,3)='Oly';")
    oly_device = sqlcursor.fetchall()
    for one in oly_device:
        # print one[0]
        if one[0] not in live_device:
            live_device.append(one[0])
    live_device = set(live_device)
    # print "live",len(live_device)

    sqlcursor.execute("select distinct(common_device_id) from cctv5_app_behavior where substring(page_content_id,1,3)='VID';")
    vid_device = sqlcursor.fetchall()
    for single in vid_device:
        if single[0] not in video_device:
            video_device.append(single[0])
    video_device = set(video_device)
    # print "VOD",len(video_device)
    intersection = live_device & video_device
    union = live_device | video_device
    print "intersection", len(intersection)
    print "union",len(union)



























    sqlConn.close()


if __name__=='__main__':
    cctv5Test = behavior(mysqlhostIP = '192.168.168.105', dbname = 'weibo')

    
