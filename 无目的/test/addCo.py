#coding:UTF-8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import MySQLdb
import time
import datetime
import os
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
# mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="123456",db="ire_gehua", charset='utf8')
print "connected"
mysqlcursor = mysqlconn.cursor()

#####add column
# mysqlcursor.execute('''alter table errorexport add column period VARCHAR(20)''')
# mysqlcursor.execute('''alter table errorexport add column datestyle Date''')

# today = datetime.date.today()
# oneday = datetime.timedelta(days=1)
# yesterday = today - oneday
# # yesterday = today
# yes_str=str(yesterday)+''+"00:00:00"
# timeArray = time.strptime(yes_str, "%Y-%m-%d%H:%M:%S")
# timeStamp = str(int(time.mktime(timeArray))*1000)
# # print yesterday
# # exit(0)
# mysqlcursor.execute('update errorexport set period = ((datetime-16*60*60*1000) mod (1000*60*60*24)) div(30*60*1000),datestyle = FROM_UNIXTIME((datetime/1000), '%Y-%m-%d' ); ')
#
# #
#
#
#
#
# # ######DO NOT DELETE BELOW!
# # ######add column
# print "Start The VodCount"
# # mysqlcursor.execute('''alter table vodcount add column datestyle Date''')
# mysqlcursor.execute('''select date from vodcount;''')
# print 2
# datetime=mysqlcursor.fetchall()
# print 3
# for one_date in datetime:
#     date_whole=time.localtime(one_date[0]/1000)
#     date_whole_style=time.strftime("%Y-%m-%d %H:%M:%S", date_whole)
#     date_date=str(date_whole_style)[0:10]
#     date_time=str(date_whole_style)[11:16]
#     date_time_h=str(date_time)[0:2]
#     date_time_m=str(date_time)[3:5]
#     time_convert=int(date_time_h)*60+int(date_time_m)
#     # print "time_conver1",time_convert
#     mysqlcursor.execute('''update vodcount set datestyle= %s where date= %s''',(date_date,one_date[0]))
#     print 4
# # #
# #
# # # ######Add Column#############################
# print "connected"
# # mysqlcursor.execute('''alter table vodcount add column period VARCHAR(20)''')
# mysqlcursor.execute('''select date,datestyle from vodcount;''')
# result_both=mysqlcursor.fetchall()
# print 5
# for one_result_both in result_both:
#
#
#     timeArray = time.strptime(str(one_result_both[1]), "%Y-%m-%d")
#     timeStamp = int(time.mktime(timeArray))
#     timeStamp=timeStamp*1000
#     distance=one_result_both[0]-timeStamp
#     period=distance/1000/60/30
#     mysqlcursor.execute('''update vodcount set period= %s where date= %s''',(period,one_result_both[0]))
#     print 6




#######Creat New Table
mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS vodsum(
    pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, cdn VARCHAR(10), period VARCHAR(10), datestyle VARCHAR(20), vodSum VARCHAR(20)) charset=utf8
    ''' )

###只插入昨天的数据
mysqlcursor.execute('''select distinct(datestyle)from errorexport order by datestyle desc;''')
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

################Insert temp##################################

# mysqlcursor.execute('''Delete from temp''')
######更新昨天数据
mysqlcursor.execute('''insert into temp (select cdn,ip,ngid,freq,LEFT(errorcode, 4), sum(count) as count,now() as date from errorexport where errorcode like '%5225%' and datetime>=UNIX_TIMESTAMP(date_sub(curdate(),interval 1 day))*1000 group by cdn,ngid,freq,ip,LEFT(errorcode, 4));''')
mysqlcursor.execute('''insert into temp (select cdn,ip,ngid,freq,errorcode, sum(count) as count,now() as date from errorexport where errorcode not like '%5225%' and datetime>=UNIX_TIMESTAMP(date_sub(curdate(),interval 1 day))*1000 group by cdn,ngid,freq,ip,errorcode)
''')

#
# mysqlcursor.execute('''insert into temp (select cdn,ip,ngid,freq,errorcode, sum(count) as count, datestyle from errorexport where errorcode not like '%5225%' group by cdn,ngid,freq,ip,errorcode);''')
# mysqlcursor.execute('''insert into temp (select cdn,ip,ngid,freq,LEFT(errorcode, 4), sum(count) as count,datestyle from errorexport where errorcode like '%5225%' group by cdn,ngid,freq,ip,LEFT(errorcode, 4))''')






if os.path.exists(r'/NewDataUpdatingIsReady.txt'):
    os.path.remove(r'/NewDataUpdatingIsReady.txt')
f = open(r'/tmp/ErrorReportPro/errorReport/NewDataUpdatingIsReady.txt', 'w')
f.close()

print "end"
mysqlconn.close()