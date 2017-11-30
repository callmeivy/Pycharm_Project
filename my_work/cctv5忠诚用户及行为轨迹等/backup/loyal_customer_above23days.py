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
    otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    monthly_period = list()
    # for inter in range(17,47):
    #     now = int(time.time())-86400*inter
    #     timeArray = time.localtime(now)
    #     # otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    #     otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    #     today = time.strftime("%Y-%m-%d", timeArray)
    #     print today
    #     monthly_period.append(today)
    # print monthly_period
    monthly_period = ['2016-12-03', '2016-12-02', '2016-12-01', '2016-12-07', '2016-12-06', '2016-11-28', '2016-11-27', '2016-11-26', '2016-11-25', '2016-11-24', '2016-11-23', '2016-11-22', '2016-11-21', '2016-11-20', '2016-11-19', '2016-11-18', '2016-11-17', '2016-11-16', '2016-11-15', '2016-11-14', '2016-11-13', '2016-11-12', '2016-11-11', '2016-11-10', '2016-11-09', '2016-11-08', '2016-11-07', '2016-11-06', '2016-11-05', '2016-11-04']
    # monthly_period = ['2016-12-03']
    # 连接数据库
    # sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    # sqlcursor = sqlConn.cursor()
    conn = connect(host = '192.168.168.43', port=21050)
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS default.loyal_customer(common_device_id string, times int, date_sction string, update_datetime string);''')
    loyal_customer = list()
    one_day_device = list()
    all_device = list()
    tempData = list()
    loyal_customer_4 = list()
    loyal_customer_3 = list()
    loyal_customer_2 = list()
    loyal_customer_1 = list()
    # 30天中至少出现23次，只要把30天各自的不重复list合并成一个list，然后count各个元素，大于5的挑出来
    for i in monthly_period:
        cur.execute("""SELECT distinct(common_device_id) FROM cctv5_app_behavior where (substring(page_start_time,1,10))=%s""", (i,))
        # test
        # cur.execute("""SELECT distinct(common_device_id) FROM cctv5_app_behavior where cast(substring(page_start_time,9,2) AS INTEGER)=%s limit 3""", (i,))
        one_day_device = list(cur.fetchall())
        # print i, one_day_device
        all_device.extend(one_day_device)
        one_day_device = list()
    # print all_device
    all_device_set = set(all_device)
    print len(all_device_set)
    # 405413用户
    for element in all_device_set:
        times = all_device.count(element)
        if times >= 25:
            # print element,times
            loyal_customer.append(element)
            tempData.append(element[0])
            tempData.append(times)
            tempData.append('2016.11.04-2016.12.03')
            tempData.append(otherStyleTime)
            try:
                cur.execute('''insert into loyal_customer(common_device_id, times, date_sction, update_datetime)
                                    values (%s, %s, %s, %s)''',tempData)
            except:
                pass
            tempData = []
            # 花了14分钟,5天的也会插入进去
    print len(loyal_customer)

    cur.close()


if __name__=='__main__':
    cctv5Test = behavior(mysqlhostIP = '192.168.168.105', dbname = 'weibo')

    
