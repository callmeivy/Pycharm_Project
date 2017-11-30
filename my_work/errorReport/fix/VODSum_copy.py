#coding:UTF-8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import MySQLdb
import time
import xlwt
w=xlwt.Workbook(encoding = 'utf-8')

#########date
ws=w.add_sheet('date',cell_overwrite_ok=True)
borders = xlwt.Borders()
borders.left = 1
borders.right = 1
borders.top = 1
borders.bottom = 1
borders.bottom_colour=0x3A
style = xlwt.XFStyle()
style.borders = borders
ws.write_merge(0,0,0,3,'VOD数据统计',style)
ws.write(1,0,'统计说明',style)
ws.write(2,0,'统计方法',style)
ws.write(3,0,'日期',style)
ws.write(3,1,'应用服务器（IP地址）',style)
ws.write_merge(1,1,1,3,'',style)
ws.write_merge(2,2,1,3,'',style)
ws.write_merge(3,3,2,3,'点流次数（单位：次）',style)
# mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="123456",db="ire_gehua", charset='utf8')
print "connected"
mysqlcursor = mysqlconn.cursor()


# #######DO NOT DELETE BELOW!
# ######add column
mysqlcursor.execute('''alter table vodcount add column datestyle Date''')
mysqlcursor.execute('''select date from vodcount''')
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


mysqlcursor.execute('''SELECT datestyle from vodcount where DATE_ADD(datestyle,INTERVAL "28" DAY)>now() order by datestyle asc''')
# mysqlcursor.execute('''select distinct(datestyle)from vodcount order by datestyle asc''')
datestyle_dis=mysqlcursor.fetchall()
dateList=list()
for one_date in datestyle_dis:

    if one_date[0] not in dateList:
        dateList.append(one_date[0])
print "list",dateList
rowNumber=4
r=0
tomcatList=[1,2,3,4]
for one_temp_date in dateList:
    print "one_temp_date",one_temp_date
    # mysqlcursor.execute('''select distinct(tomcat)from vodcount where datestyle=%s order by tomcat asc''',(one_temp_date))
    # tomcat_dis=mysqlcursor.fetchall()



    if one_temp_date is not None:

        sum_sum=0
        for onetomcat in tomcatList:
            sum=0
            #could use the sum(count)
            mysqlcursor.execute('''select count from vodcount where tomcat=%s and datestyle=%s''',(onetomcat,one_temp_date))
            count_temp=mysqlcursor.fetchall()
            for one_count_temp in count_temp:
                if one_count_temp is None:
                    count_temp[0]=0
                sum=sum+one_count_temp[0]
            sum_sum=sum_sum+sum
            borders = xlwt.Borders()
            borders.left = 1
            borders.right = 1
            borders.top = 1
            borders.bottom = 1
            borders.bottom_colour=0x3A
            style = xlwt.XFStyle()
            style.borders = borders
            ws.write(rowNumber,0,str(one_temp_date),style)
            if onetomcat==1:
                onetomcat="vsp-outlet-1"
            if onetomcat==2:
                onetomcat="vsp-outlet-2"
            if onetomcat==3:
                onetomcat="vsp-outlet-3"
            if onetomcat==4:
                onetomcat="vsp-outlet-4"


            ws.write(rowNumber,1,str(onetomcat),style)
            print 'sum',sum
            ws.write(rowNumber,2,str(sum),style)
            rowNumber+=1
        borders = xlwt.Borders()
        borders.left = 1
        borders.right = 1
        borders.top = 1
        borders.bottom = 1
        borders.bottom_colour=0x3A
        style = xlwt.XFStyle()
        style.borders = borders
        # ws.write(rowNumber-1,3,str(sum_sum),style)
        ws.write_merge(4+r*4,7+r*4,0,0,str(one_temp_date),style)
        print sum_sum
        ws.write_merge(4+r*4,7+r*4,3,3,str(sum_sum),style)
        r+=1

w.save(r'E:\baobiao\filedata\file\VODSum-GH.xls')




mysqlconn.close()