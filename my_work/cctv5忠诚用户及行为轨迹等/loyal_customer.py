#coding:UTF-8
'''
Created on 2016年12月13日
统计各个天数（1-30）的用户登录人数,并将10天以上的忠诚用户插入sql

updated on 2017.1.16 将原来的loyal_customer_every_days.py合并过来了
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
from itertools import groupby


def getKey(item):
    return item[1]

def behavior(mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'weibo'):

    now = int(time.time())
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    print otherStyleTime
    monthly_period = list()
    conn = connect(host = '192.168.168.43', port=21050)
    cur = conn.cursor()
    loyal_customer = list()
    one_day_device = list()
    all_device = list()
    tempData = list()
    loyal_customer_4 = list()
    loyal_customer_3 = list()
    loyal_customer_2 = list()
    loyal_customer_1 = list()
    element_count = dict()
    all_couple_list = list()
    couple = list()
    #/ 30天中至少出现10天，只要把30天各自的不重复list合并成一个list，然后count各个元素，大于10的挑出来
    start_date = '2016-12-01'
    end_date = '2016-12-31'
    #/ days是根据你输入的end_date的日期末尾两位来算的
    days = int(end_date[len(end_date)-2:len(end_date)])
    period = start_date+'to'+end_date
    # / 测试数据是否齐全
    cur.execute(
        """SELECT distinct((substring(page_page_start_time,1,10))) FROM cctv5_app_behavior where (substring(page_page_start_time,1,10)) between %s and %s""",
        (start_date, end_date))
    all_days = list(cur.fetchall())
    print all_days
    print '本月导入的数据天数', len(all_days)


    #/ test
    # for i in range(1, 3):
    for i in range(1,days+1):
        if i <10:
            whole_d = start_date[0:9]+str(i)
        else:
            whole_d = start_date[0:8] + str(i)
        cur.execute(
            """SELECT distinct(common_device_id) FROM cctv5_app_behavior where (substring(page_page_start_time,1,10))=%s""",
            (whole_d,))
        # test
        # cur.execute(
        #     """SELECT distinct(common_device_id) FROM cctv5_app_behavior where (substring(page_page_start_time,1,10))=%s limit 3""",
        #     (whole_d,))
        one_day_device = list(cur.fetchall())
        # print i, one_day_device
        all_device.extend(one_day_device)
        one_day_device = list()
        # print all_device
    all_device_set = set(all_device)
    print '独立用户数', len(all_device_set)

    for element in all_device_set:
        times = all_device.count(element)
        couple.append(element[0])
        couple.append(str(times))
        couple = tuple(couple)
        all_couple_list.append(couple)
        couple = list()
    # print all_couple_list
    sorted_input = sorted(all_couple_list, key = getKey)
    # print "sorted_input", sorted_input
    result = []

    sqlConn = MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db=dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS loyal_customer(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT,\
        common_device_id varchar(50), times bigint(20), date_section varchar(50), update_datetime varchar(20)) DEFAULT CHARSET=utf8;''')
    result_d = dict()
    for key, valuesiter in groupby(sorted_input, key=getKey):
        #/ key(登录天数）, v[0]（id）, v[1]:1 A100004C2F7B07 1
        item = list()
        for v in valuesiter:
            item.append(v[0])
            if int(key) >= 1:
                tempData.append(v[0])
                tempData.append(int(v[1]))
                tempData.append(str(period))
                tempData.append(otherStyleTime)
                try:
                    sqlcursor.execute('''insert into loyal_customer(common_device_id, times, date_section, update_datetime)
                                                    values (%s, %s, %s, %s)''', tempData)
                    sqlConn.commit()
                except:
                    pass
                tempData = []

        result_d[key] = len(item)
        result.append(result_d)
    print "各登录天数人数：", result

    cur.close()
    sqlConn.close()


if __name__=='__main__':

    cctv5Test = behavior(mysqlhostIP = '192.168.168.105', dbname = 'weibo')