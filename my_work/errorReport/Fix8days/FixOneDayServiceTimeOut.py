#coding:UTF-8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import MySQLdb
import xlwt
import time
import datetime
import os
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/')


if os.path.exists(r'/tmp/ErrorReportPro/errorReport/ServiceTimeOutIsReady.txt'):
    os.remove(r'/tmp/ErrorReportPro/errorReport/ServiceTimeOutIsReady.txt')
w=xlwt.Workbook(encoding = 'utf-8',style_compression=2)
ws=w.add_sheet('sheet 1',cell_overwrite_ok=True)
borders = xlwt.Borders()
borders.left = 1
borders.right = 1
borders.top = 1
borders.bottom = 1
borders.bottom_colour=0x3A
style = xlwt.XFStyle()
style.borders = borders
alignment = xlwt.Alignment()
alignment.horz = xlwt.Alignment.HORZ_CENTER
alignment.vert = xlwt.Alignment.VERT_CENTER
style = xlwt.XFStyle()
style.borders = borders
style.alignment = alignment
ws.col(0).width = 3000
ws.col(1).width = 8000
ws.col(2).width = 8000
ws.write(0,0,'日期',style)
ws.write(0,1,'接口名称',style)
ws.write(0,2,'接口返回时间超过10秒以上的次数',style)
ws.write(0,3,'调用总数',style)
ws.write(0,4,'比例',style)
ws.panes_frozen= True
ws.horz_split_pos= 1

mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
mysqlcursor = mysqlconn.cursor()
date_merge={}
date_end={}
date_start={}
sum=0
# 如果要跑昨天的代码，改这里%%%%%%%%%%%%%%%%%%%%%%%%%
for num in range(9,37):
    mysqlcursor.execute('''select date,count(*) from servicetimeoutcount where date=DATE(DATE_SUB(NOW(), INTERVAL %s DAY)) group by date order by date desc, serviceName asc'''%(str(num)))
    result_merge=mysqlcursor.fetchall()
    for one_result_merge in result_merge:
        date_merge[one_result_merge[0]]=one_result_merge[1]
        sum=sum+int(one_result_merge[1])
        date_end[one_result_merge[0]]=sum
        date_start[one_result_merge[0]]=date_end[one_result_merge[0]]-date_merge[one_result_merge[0]]
    print date_merge
    print date_end
    print date_start


rowNumber=1
# 如果要跑昨天的代码，改这里%%%%%%%%%%%%%%%%%%%%%%%%%
for num in range(9,37):
    mysqlcursor.execute('''select date,serviceName,count,total from servicetimeoutcount where date=DATE(DATE_SUB(NOW(), INTERVAL %s DAY)) order by date desc, serviceName asc '''%(str(num)))
    result=mysqlcursor.fetchall()

    for oneresult in result:
        borders = xlwt.Borders()
        borders.left = 1
        borders.right = 1
        borders.top = 1
        borders.bottom = 1
        borders.bottom_colour=0x3A
        style = xlwt.XFStyle()
        style.borders = borders
        alignment = xlwt.Alignment()
        alignment.horz = xlwt.Alignment.HORZ_CENTER
        alignment.vert = xlwt.Alignment.VERT_CENTER
        style = xlwt.XFStyle()
        style.borders = borders
        style.alignment = alignment
        ws.write(rowNumber,0,str(oneresult[0]),style)
        ws.write(rowNumber,1,str(oneresult[1]),style)
        ws.write(rowNumber,2,str(oneresult[2]),style)
        ws.write(rowNumber,3,str(oneresult[3]),style)
        ws.write(rowNumber,4,format(float(oneresult[2])/float(oneresult[3]),'.2%'),style)

        rowNumber+=1
        # print "S",date_start[oneresult[0]]

##limit 28
# 如果要跑昨天的代码，改这里%%%%%%%%%%%%%%%%%%%%%%%%%
for num in range(9,37):
    mysqlcursor.execute('''select distinct (date) from servicetimeoutcount  where date=DATE(DATE_SUB(NOW(), INTERVAL %s DAY)) order by date desc, serviceName asc'''%(str(num)))
    dis_date=mysqlcursor.fetchall()
    for one_dis_date in dis_date:
        borders = xlwt.Borders()
        borders.left = 1
        borders.right = 1
        borders.top = 1
        borders.bottom = 1
        borders.bottom_colour=0x3A
        style = xlwt.XFStyle()
        style.borders = borders
        alignment = xlwt.Alignment()
        alignment.horz = xlwt.Alignment.HORZ_CENTER
        alignment.vert = xlwt.Alignment.VERT_CENTER
        style = xlwt.XFStyle()
        style.borders = borders
        style.alignment = alignment
        print 'S',date_start[one_dis_date[0]]+1
        print 'E',date_end[one_dis_date[0]]
        ws.write_merge(int(date_start[one_dis_date[0]])+1,int(date_end[one_dis_date[0]]),0,0,str(one_dis_date[0]),style)





    w.save(r'/tmp/ErrorReportPro/errorReport/report/ServiceTimeOut.xls')

# w.save(r'E:\ServiceTimeOut.xls')

f = open(r'/tmp/ErrorReportPro/errorReport/ServiceTimeOutIsReady.txt', 'w')
f.close()

mysqlconn.close()