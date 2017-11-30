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
    # sqlcursor.execute("DELETE from cctv5_app_behavior_loyal where common_device_id not in (SELECT DISTINCT(common_device_id) from loyal_customer)")
    # **********每个小时区间的duration总和**********
    sqlcursor.execute("select substring(page_start_time,12,2), sum(page_duration)/60000 from cctv5_app_behavior_loyal where common_device_id in (select distinct(common_device_id) from loyal_customer) group by substring(page_start_time,12,2);;")
    # **********每个小时区间的忠诚用户数**********
    sqlcursor.execute("select substring(page_start_time,12,2), count(distinct(common_device_id)) from cctv5_app_behavior_loyal group by substring(page_start_time,12,2);")

    sqlcursor.execute("SELECT * from metadata_video as a INNER JOIN cctv5_app_behavior_loyal as b on b.page_content_id = a.id")

    sqlcursor.execute("select s.tag, count(DISTINCT(s.tag)) from (SELECT * from metadata_video as a INNER JOIN cctv5_app_behavior_loyal as b on b.page_content_id = a.id) as s GROUP BY s.tag")
    #有过点播行为的忠诚用户数
    sqlcursor.execute("select count(*) from cctv5_app_behavior_loyal where substring(page_content_id,1,3)='VID';")
    #有过直播行为的忠诚用户数
    sqlcursor.execute("select count(*) from cctv5_app_behavior_loyal where substring(page_content_id,1,3)='Oly';")
    # 24504
    # select DISTINCT(page_content_id) from cctv5_app_behavior_loyal where substring(page_content_id,1,3)='VID' and page_content_id not in (SELECT DISTINCT(id) from metadata_video);
    # 忠诚用户的兴趣，tag,每个tag有多少忠诚用户看,每个tag被看多少次
    sqlcursor.execute("SELECT s.tag, count(DISTINCT(s.common_device_id)) as c, count(*) as d from (SELECT * from metadata_video as a INNER JOIN cctv5_app_behavior_loyal as b on b.page_content_id = a.id) s GROUP BY s.tag ORDER BY c desc limit 40;")
    # 接上，针对全体用户
    sqlcursor.execute("SELECT s.tag, count(DISTINCT(s.common_device_id)) as c, count(*) as d from (SELECT * from metadata_video as a INNER JOIN cctv5_app_behavior as b on b.page_content_id = a.id) s GROUP BY s.tag ORDER BY c desc limit 40;")
    # 有过点播行为的忠诚用户为：2321
    sqlcursor.execute("SELECT count(DISTINCT(s.common_device_id)) as c from (SELECT * from metadata_video as a INNER JOIN cctv5_app_behavior_loyal as b on b.page_content_id = a.id) s;")
    # 看哪个内容的不重复忠诚用户独立用户数最多
    sqlcursor.execute("SELECT page_content_name, count(DISTINCT(common_device_id)) from cctv5_app_behavior_loyal GROUP BY page_content_id ORDER BY count(DISTINCT(common_device_id)) desc limit 10;")
    # 看哪个内容的被忠诚用户看的次数最多
    sqlcursor.execute("SELECT page_content_name, count(page_content_name) from cctv5_app_behavior_loyal GROUP BY page_content_id ORDER BY count(DISTINCT(common_device_id)) desc limit 20;")
     # 看哪个内容的不重复独立用户数最多
    sqlcursor.execute("SELECT page_content_name, count(DISTINCT(common_device_id)) from cctv5_app_behavior GROUP BY page_content_id ORDER BY count(DISTINCT(common_device_id)) desc limit 10;;")
    # 看哪个内容的被看的次数最多
    sqlcursor.execute("SELECT page_content_name, count(page_content_name) from cctv5_app_behavior GROUP BY page_content_id ORDER BY count(DISTINCT(common_device_id)) desc limit 20;")
# 11.16忠诚用户数
    sqlcursor.execute("SELECT count(DISTINCT(common_device_id)) from cctv5_app_behavior_loyal;")
    # 忠诚用户点播数
    sqlcursor.execute("select count(*) from cctv5_app_behavior_loyal where substring(page_content_id,1,3)='VID';")
# 全体用户点播数
    sqlcursor.execute("select count(*) from cctv5_app_behavior where substring(page_content_id,1,3)='VID';")
    # 153646
# 忠诚用户直播数
    sqlcursor.execute("select count(*) from cctv5_app_behavior_loyal where substring(page_content_id,1,3)='Oly';")
# 全体用户直播数
    sqlcursor.execute("select count(*) from cctv5_app_behavior where substring(page_content_id,1,3)='Oly';")
    sqlcursor.execute("select common_manufacturer, count(DISTINCT(common_device_id)) as a from cctv5_app_behavior_loyal GROUP BY common_manufacturer ORDER BY a desc limit 5;;")
# 行为最多的几个忠诚用户
    sqlcursor.execute("select common_device_id, count(*) from cctv5_app_behavior_loyal GROUP BY common_device_id ORDER BY count(*) desc limit 4;")

    sqlcursor.execute("select distinct(substring(page_start_time,1,10)) from cctv5_app_behavior where cast(substring(page_start_time,1,4) AS INTEGER)= 2016 and cast(substring(page_start_time,6,2) AS INTEGER)= 11;")

    sqlConn.close()
if __name__=='__main__':
    cctv5Test = behavior(mysqlhostIP = '192.168.168.105', dbname = 'weibo')

    
