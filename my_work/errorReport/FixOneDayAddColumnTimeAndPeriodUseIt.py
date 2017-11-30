#coding:UTF-8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import MySQLdb
import time
import datetime
import os

if os.path.exists(r'/NewDataUpdatingIsReady.txt'):
    os.path.remove(r'/NewDataUpdatingIsReady.txt')
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
# mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="123456",db="ire_gehua", charset='utf8')
print "connected"
mysqlcursor = mysqlconn.cursor()


# ####更新errorexport
mysqlcursor.execute('''update errorexport set period = ((datetime-16*60*60*1000) mod (1000*60*60*24)) div(30*60*1000),datestyle = FROM_UNIXTIME((datetime/1000), '%Y-%m-%d' ) where period is null;''' )
#
# ####更新vodcount
mysqlcursor.execute('''update vodcount set period = ((date-16*60*60*1000) mod (1000*60*60*24)) div(30*60*1000),datestyle = FROM_UNIXTIME((date/1000), '%Y-%m-%d' ) where period is null;''' )
#
# #####更新temp



#######Creat vodsum
mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS vodsum(
    pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, cdn VARCHAR(10), period VARCHAR(10), datestyle VARCHAR(20), vodSum VARCHAR(20)) charset=utf8
    ''' )

###先清空所有数据
mysqlcursor.execute('''DELETE from vodsum''')

mysqlcursor.execute('''select distinct(datestyle)from errorexport order by datestyle desc limit 2,28;''')
# mysqlcursor.execute('''select distinct(datestyle)from errorexport order by datestyle desc;''')
# mysqlcursor.execute('''select datestyle,datetime from errorexport where datetime >= %s''',('1413907200000'))
datestyle_dis=mysqlcursor.fetchall()

rowNumber=2
cdnList=['CDN_A','CDN_B','CDN_C','CDN_D','CDN_E','CDN_F','CDN_G','CDN_H']


for one_datestyle_dis in datestyle_dis:
    print "one_datestyle_dis",one_datestyle_dis
    period_list=range(0,48)
    for one_period in period_list:
        for onecdnList in cdnList:
        #######pls use the "sum" function in mysql
            mysqlcursor.execute('''select sum(count) from vodcount where cdn=%s and period=%s and datestyle=%s ''',(onecdnList,one_period,one_datestyle_dis[0]))
            result_count=mysqlcursor.fetchall()
            for one_result_count in result_count:
                #####should define this new var temp_sum
                temp_sum=one_result_count[0]
                if temp_sum is None:
                    temp_sum=0

                #####Add Column VodSum to errorexport
                mysqlcursor.execute("insert into vodsum(cdn, period, datestyle, vodSum) values (%s, %s, %s, %s)" , (onecdnList,one_period,one_datestyle_dis[0],temp_sum))




f = open(r'/tmp/ErrorReportPro/errorReport/NewDataUpdatingIsReady.txt', 'w')
f.close()

print "end"
mysqlconn.close()