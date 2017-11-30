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
    sqlcursor.execute('''create table IF NOT EXISTS loyal_customer(common_device_id varchar(50), times bigint, date_sction varchar(30), update_datetime datetime);''')
    conn = connect(host = '192.168.168.43', port=21050)
    cur = conn.cursor()
    loyal_customer = list()
    one_day_device = list()
    all_device = list()
    tempData = list()
    loyal_customer_6 = list()
    loyal_customer_7 = list()
    loyal_customer_2 = list()
    loyal_customer_1 = list()
    # 7天中至少出现5次，只要把7天各自的不重复list合并成一个list，然后count各个元素，大于5的挑出来
    for i in range(16,23):
        cur.execute("""SELECT distinct(common_device_id) FROM cctv5_app_behavior where cast(substring(page_start_time,9,2) AS INTEGER)=%s""", (i,))
        # test
        # cur.execute("""SELECT distinct(common_device_id) FROM cctv5_app_behavior where cast(substring(page_start_time,9,2) AS INTEGER)=%s limit 3""", (i,))
        one_day_device = list(cur.fetchall())
        # print i, one_day_device
        all_device.extend(one_day_device)
        one_day_device = list()
    # print all_device
    all_device_set = set(all_device)
    print len(all_device_set)
    # 117696用户
    for element in all_device_set:
        times = all_device.count(element)
        if times == 6:
            loyal_customer_6.append(element)
        elif times == 7:
            loyal_customer_7.append(element)

    print "6", len(loyal_customer_6)
    print "7", len(loyal_customer_7)
    sqlConn.close()


if __name__=='__main__':
    cctv5Test = behavior(mysqlhostIP = '192.168.168.105', dbname = 'weibo')

    
