#coding:UTF-8
'''
Created on 2017年1月11日
将所有忠诚用户的访问路径进行统计,debug版本
@author: Ivy

'''
import sys,os
import re
from sys import path
path.append('tools/')
path.append(path[0]+'/tools')
import MySQLdb
import time
from impala.dbapi import connect
reload(sys)
sys.setdefaultencoding('utf8')
from operator import itemgetter
from itertools import groupby

#/ 返回list中某元素的各index
def find_all_index(lst, a):
    return [i for i, x in enumerate(lst) if x[0]==a]

#/ 删除list中前后相同的元素
already = list()
differ_list = list()
def continual_duplicate(lis):
    del_index = list()
    remainder_list = list()
    differ_list = list()
    for i in range(len(lis)):
        if i < len(lis)-1:
            if lis[i] == lis[i+1]:
                differ = '0'
                differ_list.append(str(differ))
            else:
                differ = '1'
                differ_list.append(str(differ))
    del_index = find_all_index(differ_list, '0')
    remainder_list = [i for j, i in enumerate(lis) if j not in del_index]

    return remainder_list



def getKey(item):
    return item[1]

def simplify(string_str):
    string_str = string_str.replace('(', ' ')
    string_str = string_str.replace(',)', ' ')
    page_cur_page_list = ['activity.StartUpActivity','index.IndexActivity',\
                          'live.LiveActivity','live.VideoLiveDetailActivity',\
                          'player.FullScreenActivity','interact.InteractActivity', \
                          'activity.CardGroupsActivity','news.NewsDetailActivity', \
                          'vod.VideoDetailActivity', 'user.MyActivity' , 'user.ShareInviteActivity',\
                          'VideoViewController', 'MainViewController']
    replacement_code = 0
    for element in page_cur_page_list:
        string_str = string_str.replace(element, str(replacement_code))
        replacement_code += 1
    return string_str


def visit_path(mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'weibo'):
    now = int(time.time())
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    print otherStyleTime
    # monthly_period = ['2016-12-03', '2016-12-02']
    conn = connect(host='192.168.168.43', port=21050)
    cur = conn.cursor()
    #/ 忠诚用户列表获取
    cur.execute('''select common_device_id from loyal_customer;''')
    loyal_customer = list(cur.fetchall())
    # [('BF0A3ABD-261E-4CA1-B1F7-6562799B030C',), ('357507051134206',)]
    # [('BF0A3ABD-261E-4CA1-B1F7-6562799B030C',), ('357507051134206',)]
    # loyal_customer = [('BF0A3ABD-261E-4CA1-B1F7-6562799B030C',), ('357507051134206',)]
    # loyal_customer = [('357507051134206',)]
    all_path = list()
    #/ 一个id所有天所有行为在一个list
    for one_customer in loyal_customer:
        result = list()
        cur.execute("""select page_cur_page from cctv5_app_behavior where common_device_id = %s\
            order by page_page_start_time""", (one_customer[0],))
        # / test某id一个月的数据
        result = list(cur.fetchall())
        #/ 删除连续相同元素
        result = continual_duplicate(result)
        #/ 返回'activity.StartUpActivity'所在的所有index
        slice_index = find_all_index(result, 'activity.StartUpActivity')

        #/根据'activity.StartUpActivity'所在index，将list做切片处理，切好的list相当于用户一次完整的访问，每条path
        #/都做计数处理
        if len(slice_index) == 0:
            all_path.append(tuple(result))
        else:
            for i in range(len(slice_index)):
                if i < len(slice_index)-1:
                    all_path.append(tuple(result[slice_index[i]:slice_index[i+1]]))
                else:
                    all_path.append(tuple(result[(slice_index[i]):]))
        all_path_set = set(all_path)

    couple = list()
    all_couple_list = list()
    for element in all_path_set:
        times = all_path.count(element)
        couple.append(element)
        couple.append(str(times))
        couple = tuple(couple)
        all_couple_list.append(couple)
        couple = list()
    sorted_input = sorted(all_couple_list, key=getKey)

    path_time_result = []
    tempData = list()

    sqlConn = MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db=dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()
    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS path_statistics(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT,\
    path text, times bigint(20), date_section varchar(50), update_datetime varchar(20)) DEFAULT CHARSET=utf8;''')
    for key, valuesiter in groupby(sorted_input, key=getKey):
        #/ 出现次数过滤器
        if int(key) > 0:
            for v in valuesiter:
                path_0 = simplify(v[0].__str__())
                tempData.append(path_0)
                tempData.append(v[1])
                tempData.append('2016.11.04-2016.12.03')
                tempData.append(otherStyleTime)
                tempData = tuple(tempData)
                try:
                    sqlcursor.execute('''insert into path_statistics(path, times, date_section, update_datetime)
                                                    values (%s, %s, %s, %s)''', tempData)
                    sqlConn.commit()
                    tempData = []

                except:
                    pass
                tempData = []
    cur.close()
    sqlConn.close()




if __name__=='__main__':
    cctv5Test = visit_path(mysqlhostIP = '192.168.168.105', dbname = 'weibo')