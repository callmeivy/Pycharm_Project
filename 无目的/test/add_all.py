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

today = datetime.date.today()
oneday = datetime.timedelta(days=1)
yesterday = today - oneday
# yesterday = today
yes_str=str(yesterday)+''+"00:00:00"
timeArray = time.strptime(yes_str, "%Y-%m-%d%H:%M:%S")
timeStamp = str(int(time.mktime(timeArray))*1000)
# print yesterday
# exit(0)
mysqlcursor.execute('select datetime from errorexport ;')
print 2
datetime=mysqlcursor.fetchall()
print 3
m=0
for one_date in datetime:
    m+=1
    date_whole=time.localtime(one_date[0]/1000)
    date_whole_style=time.strftime("%Y-%m-%d %H:%M:%S", date_whole)
    date_date=str(date_whole_style)[0:10]
    date_time=str(date_whole_style)[11:16]
    date_time_h=str(date_time)[0:2]
    date_time_m=str(date_time)[3:5]
    time_convert=int(date_time_h)*60+int(date_time_m)
    mysqlcursor.execute('''update errorexport set datestyle= %s where datetime= %s''',(date_date,one_date[0]))

#
    # 00:00-00:30
    if time_convert>= 0 and time_convert<30:
        period=0
    # 00:30-01:00
    if time_convert>= 30 and time_convert<60:
        period=1
    # 01:00-01:30
    if time_convert>= 60 and time_convert<90:
        period=2
    # 01:30-02:00
    if time_convert>= 90 and time_convert<120:
        period=3
    #02:00-02:30
    if time_convert>= 120 and time_convert<150:
        period=4
    # 02:30-03:00
    if time_convert>= 150 and time_convert<180:
        period=5
    #03:00-03:30
    if time_convert>= 180 and time_convert<210:
        period=6
    # 03:30-04:00
    if time_convert>= 210 and time_convert<240:
        period=7
    # 04:00-04:30
    if time_convert>= 240 and time_convert<270:
        period=8
    # 04:30-05:00
    if time_convert>= 270 and time_convert<300:
        period=9
    # 05:00-05:30
    if time_convert>= 300 and time_convert<330:
        period=10
    #05:30-06:00
    if time_convert>= 330 and time_convert<360:
        period=11
    # 06:00-06:30
    if time_convert>= 360 and time_convert<390:
        period=12
    #06:30-07:00
    if time_convert>= 390 and time_convert<420:
        period=13
    # 07:00-07:30
    if time_convert>= 420 and time_convert<450:
        period=14
    # 07:30-08:00
    if time_convert>= 450 and time_convert<480:
        period=15
    # 08:00-08:30
    if time_convert>= 480 and time_convert<510:
        period=16
    # 08:30-09:00
    if time_convert>= 510 and time_convert<540:
        period=17
    # 09:00-09:30
    if time_convert>= 540 and time_convert<570:
        period=18
    # 09:30-10:00
    if time_convert>= 570 and time_convert<600:
        period=19
    #10:00-10:30
    if time_convert>= 600 and time_convert<630:
        period=20
    # 10:30-11:00
    if time_convert>= 630 and time_convert<660:
        period=21
    #11:00-11:30
    if time_convert>= 660 and time_convert<690:
        period=22
    # 11:30-12:00
    if time_convert>= 690 and time_convert<720:
        period=23
    # 12:00-12:30
    if time_convert>= 720 and time_convert<750:
        period=24
    # 12:30-13:00
    if time_convert>= 750 and time_convert<780:
        period=25
    # 13:00-13:30
    if time_convert>= 780 and time_convert<810:
        period=26
    #13:30-14:00
    if time_convert>= 810 and time_convert<840:
        period=27
    # 14:00-14:30
    if time_convert>= 840 and time_convert<870:
        period=28
    #14:30-15:00
    if time_convert>= 870 and time_convert<900:
        period=29
    # 15:00-15:30
    if time_convert>= 900 and time_convert<930:
        period=30
    # 15:30-16:00
    if time_convert>= 930 and time_convert<960:
        period=31
    # 16:00-16:30
    if time_convert>= 960 and time_convert<990:
        period=32
    # 16:30-17:00
    if time_convert>= 990 and time_convert<1020:
        period=33
    # 17:00-17:30
    if time_convert>= 1020 and time_convert<1050:
        period=34
    #17:30-18:00
    if time_convert>= 1050 and time_convert<1080:
        period=35
    # 18:00-18:30
    if time_convert>= 1080 and time_convert<1110:
        period=36
    #18:30-19:00
    if time_convert>= 1110 and time_convert<1140:
        period=37
    # 19:00-19:30
    if time_convert>= 1140 and time_convert<1170:
        period=38
    # 19:30-20:00
    if time_convert>= 1170 and time_convert<1200:
        period=39
    # 20:00-20:30
    if time_convert>= 1200 and time_convert<1230:
        period=40
    #20:30-21:00
    if time_convert>= 1230 and time_convert<1260:
        period=41
    # 21:00-21:30
    if time_convert>= 1260 and time_convert<1290:
        period=42
    #21:30-22:00
    if time_convert>= 1290 and time_convert<1320:
        period=43
    # 22:00-22:30
    if time_convert>= 1320 and time_convert<1350:
        period=44
    # 22:30-23:00
    if time_convert>= 1350 and time_convert<1380:
        period=45
    # 23:00-23:30
    if time_convert>= 1380 and time_convert<1410:
        period=46
    # 23:30-24:00
    if time_convert>= 1410 and time_convert<1440:
        period=47
    mysqlcursor.execute('''update errorexport set period= %s where datetime= %s''',(period,one_date[0]))
    print 4
#
#
#




# ######DO NOT DELETE BELOW!
# ######add column
print "Start The VodCount"
# mysqlcursor.execute('''alter table vodcount add column datestyle Date''')
mysqlcursor.execute('''select date from vodcount;''')
print 2
datetime=mysqlcursor.fetchall()
print 3
for one_date in datetime:
    date_whole=time.localtime(one_date[0]/1000)
    date_whole_style=time.strftime("%Y-%m-%d %H:%M:%S", date_whole)
    date_date=str(date_whole_style)[0:10]
    date_time=str(date_whole_style)[11:16]
    date_time_h=str(date_time)[0:2]
    date_time_m=str(date_time)[3:5]
    time_convert=int(date_time_h)*60+int(date_time_m)
    # print "time_conver1",time_convert
    mysqlcursor.execute('''update vodcount set datestyle= %s where date= %s''',(date_date,one_date[0]))
    print 4
# #
#
# # ######Add Column#############################
print "connected"
# mysqlcursor.execute('''alter table vodcount add column period VARCHAR(20)''')
mysqlcursor.execute('''select date,datestyle from vodcount;''')
result_both=mysqlcursor.fetchall()
print 5
for one_result_both in result_both:


    timeArray = time.strptime(str(one_result_both[1]), "%Y-%m-%d")
    timeStamp = int(time.mktime(timeArray))
    timeStamp=timeStamp*1000
    distance=one_result_both[0]-timeStamp
    period=distance/1000/60/30
    mysqlcursor.execute('''update vodcount set period= %s where date= %s''',(period,one_result_both[0]))
    print 6




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
# mysqlcursor.execute('''insert into temp (select cdn,ip,ngid,freq,LEFT(errorcode, 4), sum(count) as count,now() as date from errorexport where errorcode like '%5225%' and datetime>=UNIX_TIMESTAMP(date_sub(curdate(),interval 1 day))*1000 group by cdn,ngid,freq,ip,LEFT(errorcode, 4));''')
# mysqlcursor.execute('''insert into temp (select cdn,ip,ngid,freq,errorcode, sum(count) as count,now() as date from errorexport where errorcode not like '%5225%' and datetime>=UNIX_TIMESTAMP(date_sub(curdate(),interval 1 day))*1000 group by cdn,ngid,freq,ip,errorcode)
# # ''')


mysqlcursor.execute('''insert into temp (select cdn,ip,ngid,freq,errorcode, sum(count) as count, datestyle from errorexport where errorcode not like '%5225%' group by cdn,ngid,freq,ip,errorcode);''')
mysqlcursor.execute('''insert into temp (select cdn,ip,ngid,freq,LEFT(errorcode, 4), sum(count) as count,datestyle from errorexport where errorcode like '%5225%' group by cdn,ngid,freq,ip,LEFT(errorcode, 4))''')






# if os.path.exists(r'/NewDataUpdatingIsReady.txt'):
#     os.path.remove(r'/NewDataUpdatingIsReady.txt')
# f = open(r'/tmp/ErrorReportPro/errorReport/NewDataUpdatingIsReady.txt', 'w')
# f.close()

print "end"
mysqlconn.close()