#coding:UTF-8
#encoding:UTF-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb
import time
import xlwt
import os
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/')
w=xlwt.Workbook(encoding = 'utf-8',style_compression=2)
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
alignment = xlwt.Alignment()
alignment.horz = xlwt.Alignment.HORZ_CENTER
alignment.vert = xlwt.Alignment.VERT_CENTER
style = xlwt.XFStyle()
style.borders = borders
style.alignment = alignment
ws.col(0).width = 3000
ws.col(1).width = 3000
ws.write_merge(0,1,0,0,'统计日期',style)
ws.write_merge(0,1,1,1,'详细时间段',style)
ws.write_merge(0,0,2,8,'错误代码',style)
ws.write_merge(0,0,10,17,'错误率=错误数/总点流量',style)
ws.write_merge(0,1,18,18,'总错误量',style)
ws.write_merge(0,1,19,19,'总点流量',style)
ws.write(1,2,'01_000',style)
ws.write(1,3,'01_300',style)
ws.write(1,4,'02_200',style)
ws.write(1,5,'02_300',style)
ws.write(1,6,'5203',style)
ws.write(1,7,'5206',style)
ws.write(1,8,'5225',style)
ws.write(1,10,'01_000',style)
ws.write(1,11,'01_300',style)
ws.write(1,12,'02_200',style)
ws.write(1,13,'02_300',style)
ws.write(1,14,'5203',style)
ws.write(1,15,'5206',style)
ws.write(1,16,'5225',style)
ws.write(1,17,'总错误率',style)
ws.panes_frozen= True
ws.horz_split_pos= 2



############################A!!!!!!!!!!!!!!!!!!##########################
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_A')
# mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="123456",db="ire_gehua", charset='utf8')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
print "connected"
mysqlcursor = mysqlconn.cursor()
mysqlcursor.execute('''select distinct(datestyle)from errorexport order by datestyle desc limit 1,28''')
datestyle_dis=mysqlcursor.fetchall()



rowNumber=1
errorCodeList=['01_000%','01-300%','02-200%','02-300%','5203%','5206%','5225%']
# errorCodeList=['5206']
r=0
for one_datestyle_dis in datestyle_dis:
    print one_datestyle_dis[0]
    j=1
    for i in range(0,48):
        j=1

        sum=0
        sum_per=0
        mysqlcursor.execute('''select vodSum from vodsum where cdn=%s and period= %s and datestyle=%s''',("CDN_A",i,one_datestyle_dis[0]))
        # mysqlcursor.execute('''select vodSum from errorexport where cdn=%s and period= %s and datestyle=%s limit 1''',("CDN_A",i,one_datestyle_dis[0]))
        VodSum_Result=mysqlcursor.fetchall()
        tmpCount=0

        rowNumber+=1
        for one_VodSum_Result in VodSum_Result:
            tmpCount+=1
            VodSum=one_VodSum_Result[0]
            j=1



            for oneErrorCode in errorCodeList:

                j+=1

                # mysqlcursor.execute('''select sum(count)from errorexport where cdn=%s and errorcode like %s and period= %s and datestyle=%s''',("CDN_A",oneErrorCode, i,one_datestyle_dis[0]))
                mysqlcursor.execute('''select sum(count)from errorexport where cdn=%s and errorcode like %s  and period= %s and datestyle=%s ''',("CDN_A",oneErrorCode, i,one_datestyle_dis[0]))
                result1=mysqlcursor.fetchall()
                for oneresult_count1 in result1:
                    count_temp=oneresult_count1[0]
                    if count_temp is None:
                        count_temp=0
                    sum=sum+count_temp
                    if VodSum is None:
                        count_per=format(0,'.2%')
                    else:
                        if int(VodSum)!=0:
                            count_per=format(float(count_temp)/float(VodSum), '.2%')
                        else:
                            count_per=format(0,'.2%')
                    if VodSum is None:
                        sum_per=format(0,'.2%')
                    else:
                        if int(VodSum)!=0:
                            sum_per=format(float(sum)/float(VodSum), '.2%')
                        else:
                            sum_per=format(0,'.2%')
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
                    if i==0:
                        q='00:00-00:30'
                    if i==1:
                        q='00:30-01:00'
                    if i==2:
                        q='01:00-01:30'
                    if i==3:
                        q='01:30-02:00'
                    if i==4:
                        q='02:00-02:30'
                    if i==5:
                        q='02:30-03:00'
                    if i==6:
                        q='03:00-03:30'
                    if i==7:
                        q='03:30-04:00'
                    if i==8:
                        q='04:00-04:30'
                    if i==9:
                        q='04:30-05:00'
                    if i==10:
                        q='05:00-05:30'
                    if i==11:
                        q='05:30-06:00'
                    if i==12:
                        q='06:00-06:30'
                    if i==13:
                        q='06:30-07:00'
                    if i==14:
                        q='07:00-07:30'
                    if i==15:
                        q='07:30-08:00'
                    if i==16:
                        q='08:00-08:30'
                    if i==17:
                        q='08:30-09:00'
                    if i==18:
                        q='09:00-09:30'
                    if i==19:
                        q='09:30-10:00'
                    if i==20:
                        q='10:00-10:30'
                    if i==21:
                        q='10:30-11:00'
                    if i==22:
                        q='11:00-11:30'
                    if i==23:
                        q='11:30-12:00'
                    if i==24:
                        q='12:00-12:30'
                    if i==25:
                        q='12:30-13:00'
                    if i==26:
                        q='13:00-13:30'
                    if i==27:
                        q='13:30-14:00'
                    if i==28:
                        q='14:00-14:30'
                    if i==29:
                        q='14:30-15:00'
                    if i==30:
                        q='15:00-15:30'
                    if i==31:
                        q='15:30-16:00'
                    if i==32:
                        q='16:00-16:30'
                    if i==33:
                        q='16:30-17:00'
                    if i==34:
                        q='17:00-17:30'
                    if i==35:
                        q='17:30-18:00'
                    if i==36:
                        q='18:00-18:30'
                    if i==37:
                        q='18:30-19:00'
                    if i==38:
                        q='19:00-19:30'
                    if i==39:
                        q='19:30-20:00'
                    if i==40:
                        q='20:00-20:30'
                    if i==41:
                        q='20:30-21:00'
                    if i==42:
                        q='21:00-21:30'
                    if i==43:
                        q='21:30-22:00'
                    if i==44:
                        q='22:00-22:30'
                    if i==45:
                        q='22:30-23:00'
                    if i==46:
                        q='23:00-23:30'
                    if i==47:
                        q='23:30-24:00'
                    ws.write(rowNumber,0,str(one_datestyle_dis[0]),style)
                    ws.write(rowNumber,1,str(q),style)
                    ws.write(rowNumber,j,str(count_temp),style)
                    ws.write(rowNumber,j+8,str(count_per),style)
                    #zong cuo wu lv
                    ws.write(rowNumber,17,str(sum_per),style)
                    #zong cuo wu liang
                    ws.write(rowNumber,18,str(sum),style)

        if tmpCount == 0:
            for j in range(1,8):
                j+=1
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
                ws.write(rowNumber,0,str(one_datestyle_dis[0]),style)
                if i==0:
                    q='00:00-00:30'
                if i==1:
                    q='00:30-01:00'
                if i==2:
                    q='01:00-01:30'
                if i==3:
                    q='01:30-02:00'
                if i==4:
                    q='02:00-02:30'
                if i==5:
                    q='02:30-03:00'
                if i==6:
                    q='03:00-03:30'
                if i==7:
                    q='03:30-04:00'
                if i==8:
                    q='04:00-04:30'
                if i==9:
                    q='04:30-05:00'
                if i==10:
                    q='05:00-05:30'
                if i==11:
                    q='05:30-06:00'
                if i==12:
                    q='06:00-06:30'
                if i==13:
                    q='06:30-07:00'
                if i==14:
                    q='07:00-07:30'
                if i==15:
                    q='07:30-08:00'
                if i==16:
                    q='08:00-08:30'
                if i==17:
                    q='08:30-09:00'
                if i==18:
                    q='09:00-09:30'
                if i==19:
                    q='09:30-10:00'
                if i==20:
                    q='10:00-10:30'
                if i==21:
                    q='10:30-11:00'
                if i==22:
                    q='11:00-11:30'
                if i==23:
                    q='11:30-12:00'
                if i==24:
                    q='12:00-12:30'
                if i==25:
                    q='12:30-13:00'
                if i==26:
                    q='13:00-13:30'
                if i==27:
                    q='13:30-14:00'
                if i==28:
                    q='14:00-14:30'
                if i==29:
                    q='14:30-15:00'
                if i==30:
                    q='15:00-15:30'
                if i==31:
                    q='15:30-16:00'
                if i==32:
                    q='16:00-16:30'
                if i==33:
                    q='16:30-17:00'
                if i==34:
                    q='17:00-17:30'
                if i==35:
                    q='17:30-18:00'
                if i==36:
                    q='18:00-18:30'
                if i==37:
                    q='18:30-19:00'
                if i==38:
                    q='19:00-19:30'
                if i==39:
                    q='19:30-20:00'
                if i==40:
                    q='20:00-20:30'
                if i==41:
                    q='20:30-21:00'
                if i==42:
                    q='21:00-21:30'
                if i==43:
                    q='21:30-22:00'
                if i==44:
                    q='22:00-22:30'
                if i==45:
                    q='22:30-23:00'
                if i==46:
                    q='23:00-23:30'
                if i==47:
                    q='23:30-24:00'

                ws.write(rowNumber,1,str(q),style)
                ws.write(rowNumber,j,str('0'),style)
                ws.write(rowNumber,j+8,str(format(0,'.2%')),style)
                #zong cuo wu lv
                ws.write(rowNumber,17,str(format(0,'.2%')),style)
                #zong cuo wu liang
                ws.write(rowNumber,18,str('0'),style)


                # ws.write(0, 0, '日期',style)
    ws.write_merge(2+r*48,49+r*48,0,0,str(one_datestyle_dis[0]),style)
    r+=1






####should be from errorexport


mysqlcursor.execute('''select distinct(datestyle)from errorexport order by datestyle desc limit 1,28''')
datestyle_dis=mysqlcursor.fetchall()
rowNumber=2

for one_datestyle_dis in datestyle_dis:
    # print one_datestyle_dis[0]
    period_list=range(0,48)
    for one_period in period_list:
        #######pls use the "sum" function in mysql
        mysqlcursor.execute('''select sum(count) from vodcount where cdn=%s and period=%s and datestyle=%s''',("CDN_A",one_period,one_datestyle_dis[0]))
        result_count=mysqlcursor.fetchall()
        for one_result_count in result_count:
            #####should define this new var temp_sum
            temp_sum=one_result_count[0]
            if temp_sum is None:
                temp_sum=0

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
            ws.write(rowNumber,19,str(temp_sum),style)
            rowNumber+=1

#
#
#
#
w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_A/ErrorSum(Hour)-GH-CDN-A.xls')
# w.save(r'E:\ErrorSum(Hour)-GH-CDN-A.xls')
# w.save(r'E:\ErrorSum(Hour)-GH-CDN-A.xls')
mysqlconn.close()




#
# # ##################################B！！！！！！！！！！！！！！！！！！！！！##################
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_B')
# mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="123456",db="ire_gehua", charset='utf8')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
print "connected"
mysqlcursor = mysqlconn.cursor()
mysqlcursor.execute('''select distinct(datestyle)from errorexport order by datestyle desc limit 1,28''')
datestyle_dis=mysqlcursor.fetchall()



rowNumber=1
errorCodeList=['01_000%','01-300%','02-200%','02-300%','5203%','5206%','5225%']
# errorCodeList=['5206']
r=0
for one_datestyle_dis in datestyle_dis:
    j=1
    for i in range(0,48):
        j=1

        sum=0
        sum_per=0
        mysqlcursor.execute('''select vodSum from vodsum where cdn=%s and period= %s and datestyle=%s''',("CDN_B",i,one_datestyle_dis[0]))
        # mysqlcursor.execute('''select vodSum from errorexport where cdn=%s and period= %s and datestyle=%s limit 1''',("CDN_B",i,one_datestyle_dis[0]))
        VodSum_Result=mysqlcursor.fetchall()
        tmpCount=0

        rowNumber+=1
        for one_VodSum_Result in VodSum_Result:
            tmpCount+=1
            VodSum=one_VodSum_Result[0]
            j=1



            for oneErrorCode in errorCodeList:

                j+=1

                # mysqlcursor.execute('''select sum(count)from errorexport where cdn=%s and errorcode like %s and period= %s and datestyle=%s''',("CDN_B",oneErrorCode, i,one_datestyle_dis[0]))
                # mysqlcursor.execute('''select sum(count)from errorexport where cdn=%s and substring(errorcode,1,4)=%s  and period= %s and datestyle=%s''',("CDN_A",oneErrorCode, i,one_datestyle_dis[0]))
                mysqlcursor.execute('''select sum(count)from errorexport where cdn=%s and errorcode like %s  and period= %s and datestyle=%s''',("CDN_A",oneErrorCode, i,one_datestyle_dis[0]))
                result1=mysqlcursor.fetchall()
                for oneresult_count1 in result1:
                    count_temp=oneresult_count1[0]
                    if count_temp is None:
                        count_temp=0
                    sum=sum+count_temp
                    if VodSum is None:
                        count_per=format(0,'.2%')
                    else:
                        if int(VodSum)!=0:
                            count_per=format(float(count_temp)/float(VodSum), '.2%')
                        else:
                            count_per=format(0,'.2%')
                    if VodSum is None:
                        sum_per=format(0,'.2%')
                    else:
                        if int(VodSum)!=0:
                            sum_per=format(float(sum)/float(VodSum), '.2%')
                        else:
                            sum_per=format(0,'.2%')
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
                    if i==0:
                        q='00:00-00:30'
                    if i==1:
                        q='00:30-01:00'
                    if i==2:
                        q='01:00-01:30'
                    if i==3:
                        q='01:30-02:00'
                    if i==4:
                        q='02:00-02:30'
                    if i==5:
                        q='02:30-03:00'
                    if i==6:
                        q='03:00-03:30'
                    if i==7:
                        q='03:30-04:00'
                    if i==8:
                        q='04:00-04:30'
                    if i==9:
                        q='04:30-05:00'
                    if i==10:
                        q='05:00-05:30'
                    if i==11:
                        q='05:30-06:00'
                    if i==12:
                        q='06:00-06:30'
                    if i==13:
                        q='06:30-07:00'
                    if i==14:
                        q='07:00-07:30'
                    if i==15:
                        q='07:30-08:00'
                    if i==16:
                        q='08:00-08:30'
                    if i==17:
                        q='08:30-09:00'
                    if i==18:
                        q='09:00-09:30'
                    if i==19:
                        q='09:30-10:00'
                    if i==20:
                        q='10:00-10:30'
                    if i==21:
                        q='10:30-11:00'
                    if i==22:
                        q='11:00-11:30'
                    if i==23:
                        q='11:30-12:00'
                    if i==24:
                        q='12:00-12:30'
                    if i==25:
                        q='12:30-13:00'
                    if i==26:
                        q='13:00-13:30'
                    if i==27:
                        q='13:30-14:00'
                    if i==28:
                        q='14:00-14:30'
                    if i==29:
                        q='14:30-15:00'
                    if i==30:
                        q='15:00-15:30'
                    if i==31:
                        q='15:30-16:00'
                    if i==32:
                        q='16:00-16:30'
                    if i==33:
                        q='16:30-17:00'
                    if i==34:
                        q='17:00-17:30'
                    if i==35:
                        q='17:30-18:00'
                    if i==36:
                        q='18:00-18:30'
                    if i==37:
                        q='18:30-19:00'
                    if i==38:
                        q='19:00-19:30'
                    if i==39:
                        q='19:30-20:00'
                    if i==40:
                        q='20:00-20:30'
                    if i==41:
                        q='20:30-21:00'
                    if i==42:
                        q='21:00-21:30'
                    if i==43:
                        q='21:30-22:00'
                    if i==44:
                        q='22:00-22:30'
                    if i==45:
                        q='22:30-23:00'
                    if i==46:
                        q='23:00-23:30'
                    if i==47:
                        q='23:30-24:00'
                    ws.write(rowNumber,0,str(one_datestyle_dis[0]),style)
                    ws.write(rowNumber,1,str(q),style)
                    ws.write(rowNumber,j,str(count_temp),style)
                    ws.write(rowNumber,j+8,str(count_per),style)
                    #zong cuo wu lv
                    ws.write(rowNumber,17,str(sum_per),style)
                    #zong cuo wu liang
                    ws.write(rowNumber,18,str(sum),style)

        if tmpCount == 0:
            for j in range(1,8):
                j+=1
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
                ws.write(rowNumber,0,str(one_datestyle_dis[0]),style)
                if i==0:
                    q='00:00-00:30'
                if i==1:
                    q='00:30-01:00'
                if i==2:
                    q='01:00-01:30'
                if i==3:
                    q='01:30-02:00'
                if i==4:
                    q='02:00-02:30'
                if i==5:
                    q='02:30-03:00'
                if i==6:
                    q='03:00-03:30'
                if i==7:
                    q='03:30-04:00'
                if i==8:
                    q='04:00-04:30'
                if i==9:
                    q='04:30-05:00'
                if i==10:
                    q='05:00-05:30'
                if i==11:
                    q='05:30-06:00'
                if i==12:
                    q='06:00-06:30'
                if i==13:
                    q='06:30-07:00'
                if i==14:
                    q='07:00-07:30'
                if i==15:
                    q='07:30-08:00'
                if i==16:
                    q='08:00-08:30'
                if i==17:
                    q='08:30-09:00'
                if i==18:
                    q='09:00-09:30'
                if i==19:
                    q='09:30-10:00'
                if i==20:
                    q='10:00-10:30'
                if i==21:
                    q='10:30-11:00'
                if i==22:
                    q='11:00-11:30'
                if i==23:
                    q='11:30-12:00'
                if i==24:
                    q='12:00-12:30'
                if i==25:
                    q='12:30-13:00'
                if i==26:
                    q='13:00-13:30'
                if i==27:
                    q='13:30-14:00'
                if i==28:
                    q='14:00-14:30'
                if i==29:
                    q='14:30-15:00'
                if i==30:
                    q='15:00-15:30'
                if i==31:
                    q='15:30-16:00'
                if i==32:
                    q='16:00-16:30'
                if i==33:
                    q='16:30-17:00'
                if i==34:
                    q='17:00-17:30'
                if i==35:
                    q='17:30-18:00'
                if i==36:
                    q='18:00-18:30'
                if i==37:
                    q='18:30-19:00'
                if i==38:
                    q='19:00-19:30'
                if i==39:
                    q='19:30-20:00'
                if i==40:
                    q='20:00-20:30'
                if i==41:
                    q='20:30-21:00'
                if i==42:
                    q='21:00-21:30'
                if i==43:
                    q='21:30-22:00'
                if i==44:
                    q='22:00-22:30'
                if i==45:
                    q='22:30-23:00'
                if i==46:
                    q='23:00-23:30'
                if i==47:
                    q='23:30-24:00'

                ws.write(rowNumber,1,str(q),style)
                ws.write(rowNumber,j,str('0'),style)
                ws.write(rowNumber,j+8,str(format(0,'.2%')),style)
                #zong cuo wu lv
                ws.write(rowNumber,17,str(format(0,'.2%')),style)
                #zong cuo wu liang
                ws.write(rowNumber,18,str('0'),style)


                ws.write(0, 0, 'Firstname',style)
    ws.write_merge(2+r*48,49+r*48,0,0,str(one_datestyle_dis[0]),style)
    r+=1






####should be from errorexport


mysqlcursor.execute('''select distinct(datestyle)from errorexport order by datestyle desc limit 1,28''')
datestyle_dis=mysqlcursor.fetchall()
rowNumber=2

for one_datestyle_dis in datestyle_dis:
    # print one_datestyle_dis[0]
    period_list=range(0,48)
    for one_period in period_list:
        #######pls use the "sum" function in mysql
        mysqlcursor.execute('''select sum(count) from vodcount where cdn=%s and period=%s and datestyle=%s''',("CDN_B",one_period,one_datestyle_dis[0]))
        result_count=mysqlcursor.fetchall()
        for one_result_count in result_count:
            #####should define this new var temp_sum
            temp_sum=one_result_count[0]
            if temp_sum is None:
                temp_sum=0

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
            ws.write(rowNumber,19,str(temp_sum),style)
            rowNumber+=1





w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_B/ErrorSum(Hour)-GH-CDN-B.xls')


mysqlconn.close()


#######################C!!!!!!!!!!!!!!!!!!!#####################################
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_C')
# mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="123456",db="ire_gehua", charset='utf8')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
print "connected"
mysqlcursor = mysqlconn.cursor()
mysqlcursor.execute('''select distinct(datestyle)from errorexport order by datestyle desc limit 1,28''')
datestyle_dis=mysqlcursor.fetchall()



rowNumber=1
errorCodeList=['01_000%','01-300%','02-200%','02-300%','5203%','5206%','5225%']
# errorCodeList=['5206']
r=0
for one_datestyle_dis in datestyle_dis:
    j=1
    for i in range(0,48):
        j=1

        sum=0
        sum_per=0
        mysqlcursor.execute('''select vodSum from vodsum where cdn=%s and period= %s and datestyle=%s''',("CDN_C",i,one_datestyle_dis[0]))
        # mysqlcursor.execute('''select vodSum from errorexport where cdn=%s and period= %s and datestyle=%s limit 1''',("CDN_C",i,one_datestyle_dis[0]))
        VodSum_Result=mysqlcursor.fetchall()
        tmpCount=0

        rowNumber+=1
        for one_VodSum_Result in VodSum_Result:
            j=1
            tmpCount+=1
            VodSum=one_VodSum_Result[0]



            for oneErrorCode in errorCodeList:

                j+=1

                # mysqlcursor.execute('''select sum(count)from errorexport where cdn=%s and errorcode like %s and period= %s and datestyle=%s''',("CDN_C",oneErrorCode, i,one_datestyle_dis[0]))
                # mysqlcursor.execute('''select sum(count)from errorexport where cdn=%s and substring(errorcode,1,4)=%s  and period= %s and datestyle=%s''',("CDN_A",oneErrorCode, i,one_datestyle_dis[0]))
                mysqlcursor.execute('''select sum(count)from errorexport where cdn=%s and errorcode like %s  and period= %s and datestyle=%s''',("CDN_A",oneErrorCode, i,one_datestyle_dis[0]))
                result1=mysqlcursor.fetchall()
                for oneresult_count1 in result1:
                    count_temp=oneresult_count1[0]
                    if count_temp is None:
                        count_temp=0
                    sum=sum+count_temp
                    if VodSum is None:
                        count_per=format(0,'.2%')
                    else:
                        if int(VodSum)!=0:
                            count_per=format(float(count_temp)/float(VodSum), '.2%')
                        else:
                            count_per=format(0,'.2%')
                    if VodSum is None:
                        sum_per=format(0,'.2%')
                    else:
                        if int(VodSum)!=0:
                            sum_per=format(float(sum)/float(VodSum), '.2%')
                        else:
                            sum_per=format(0,'.2%')
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
                    if i==0:
                        q='00:00-00:30'
                    if i==1:
                        q='00:30-01:00'
                    if i==2:
                        q='01:00-01:30'
                    if i==3:
                        q='01:30-02:00'
                    if i==4:
                        q='02:00-02:30'
                    if i==5:
                        q='02:30-03:00'
                    if i==6:
                        q='03:00-03:30'
                    if i==7:
                        q='03:30-04:00'
                    if i==8:
                        q='04:00-04:30'
                    if i==9:
                        q='04:30-05:00'
                    if i==10:
                        q='05:00-05:30'
                    if i==11:
                        q='05:30-06:00'
                    if i==12:
                        q='06:00-06:30'
                    if i==13:
                        q='06:30-07:00'
                    if i==14:
                        q='07:00-07:30'
                    if i==15:
                        q='07:30-08:00'
                    if i==16:
                        q='08:00-08:30'
                    if i==17:
                        q='08:30-09:00'
                    if i==18:
                        q='09:00-09:30'
                    if i==19:
                        q='09:30-10:00'
                    if i==20:
                        q='10:00-10:30'
                    if i==21:
                        q='10:30-11:00'
                    if i==22:
                        q='11:00-11:30'
                    if i==23:
                        q='11:30-12:00'
                    if i==24:
                        q='12:00-12:30'
                    if i==25:
                        q='12:30-13:00'
                    if i==26:
                        q='13:00-13:30'
                    if i==27:
                        q='13:30-14:00'
                    if i==28:
                        q='14:00-14:30'
                    if i==29:
                        q='14:30-15:00'
                    if i==30:
                        q='15:00-15:30'
                    if i==31:
                        q='15:30-16:00'
                    if i==32:
                        q='16:00-16:30'
                    if i==33:
                        q='16:30-17:00'
                    if i==34:
                        q='17:00-17:30'
                    if i==35:
                        q='17:30-18:00'
                    if i==36:
                        q='18:00-18:30'
                    if i==37:
                        q='18:30-19:00'
                    if i==38:
                        q='19:00-19:30'
                    if i==39:
                        q='19:30-20:00'
                    if i==40:
                        q='20:00-20:30'
                    if i==41:
                        q='20:30-21:00'
                    if i==42:
                        q='21:00-21:30'
                    if i==43:
                        q='21:30-22:00'
                    if i==44:
                        q='22:00-22:30'
                    if i==45:
                        q='22:30-23:00'
                    if i==46:
                        q='23:00-23:30'
                    if i==47:
                        q='23:30-24:00'
                    ws.write(rowNumber,0,str(one_datestyle_dis[0]),style)
                    ws.write(rowNumber,1,str(q),style)
                    ws.write(rowNumber,j,str(count_temp),style)
                    ws.write(rowNumber,j+8,str(count_per),style)
                    #zong cuo wu lv
                    ws.write(rowNumber,17,str(sum_per),style)
                    #zong cuo wu liang
                    ws.write(rowNumber,18,str(sum),style)

        if tmpCount == 0:
            for j in range(1,8):
                j+=1
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
                ws.write(rowNumber,0,str(one_datestyle_dis[0]),style)
                if i==0:
                    q='00:00-00:30'
                if i==1:
                    q='00:30-01:00'
                if i==2:
                    q='01:00-01:30'
                if i==3:
                    q='01:30-02:00'
                if i==4:
                    q='02:00-02:30'
                if i==5:
                    q='02:30-03:00'
                if i==6:
                    q='03:00-03:30'
                if i==7:
                    q='03:30-04:00'
                if i==8:
                    q='04:00-04:30'
                if i==9:
                    q='04:30-05:00'
                if i==10:
                    q='05:00-05:30'
                if i==11:
                    q='05:30-06:00'
                if i==12:
                    q='06:00-06:30'
                if i==13:
                    q='06:30-07:00'
                if i==14:
                    q='07:00-07:30'
                if i==15:
                    q='07:30-08:00'
                if i==16:
                    q='08:00-08:30'
                if i==17:
                    q='08:30-09:00'
                if i==18:
                    q='09:00-09:30'
                if i==19:
                    q='09:30-10:00'
                if i==20:
                    q='10:00-10:30'
                if i==21:
                    q='10:30-11:00'
                if i==22:
                    q='11:00-11:30'
                if i==23:
                    q='11:30-12:00'
                if i==24:
                    q='12:00-12:30'
                if i==25:
                    q='12:30-13:00'
                if i==26:
                    q='13:00-13:30'
                if i==27:
                    q='13:30-14:00'
                if i==28:
                    q='14:00-14:30'
                if i==29:
                    q='14:30-15:00'
                if i==30:
                    q='15:00-15:30'
                if i==31:
                    q='15:30-16:00'
                if i==32:
                    q='16:00-16:30'
                if i==33:
                    q='16:30-17:00'
                if i==34:
                    q='17:00-17:30'
                if i==35:
                    q='17:30-18:00'
                if i==36:
                    q='18:00-18:30'
                if i==37:
                    q='18:30-19:00'
                if i==38:
                    q='19:00-19:30'
                if i==39:
                    q='19:30-20:00'
                if i==40:
                    q='20:00-20:30'
                if i==41:
                    q='20:30-21:00'
                if i==42:
                    q='21:00-21:30'
                if i==43:
                    q='21:30-22:00'
                if i==44:
                    q='22:00-22:30'
                if i==45:
                    q='22:30-23:00'
                if i==46:
                    q='23:00-23:30'
                if i==47:
                    q='23:30-24:00'

                ws.write(rowNumber,1,str(q),style)
                ws.write(rowNumber,j,str('0'),style)
                ws.write(rowNumber,j+8,str(format(0,'.2%')),style)
                #zong cuo wu lv
                ws.write(rowNumber,17,str(format(0,'.2%')),style)
                #zong cuo wu liang
                ws.write(rowNumber,18,str('0'),style)


                ws.write(0, 0, 'Firstname',style)
    ws.write_merge(2+r*48,49+r*48,0,0,str(one_datestyle_dis[0]),style)
    r+=1






####should be from errorexport


mysqlcursor.execute('''select distinct(datestyle)from errorexport order by datestyle desc limit 1,28''')
datestyle_dis=mysqlcursor.fetchall()
rowNumber=2

for one_datestyle_dis in datestyle_dis:
    # print one_datestyle_dis[0]
    period_list=range(0,48)
    for one_period in period_list:
        #######pls use the "sum" function in mysql
        mysqlcursor.execute('''select sum(count) from vodcount where cdn=%s and period=%s and datestyle=%s''',("CDN_C",one_period,one_datestyle_dis[0]))
        result_count=mysqlcursor.fetchall()
        for one_result_count in result_count:
            #####should define this new var temp_sum
            temp_sum=one_result_count[0]
            if temp_sum is None:
                temp_sum=0

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
            ws.write(rowNumber,19,str(temp_sum),style)
            rowNumber+=1





w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_C/ErrorSum(Hour)-GH-CDN-C.xls')


mysqlconn.close()



########################D!!!!!!!!!!!!!!!!!!!#########################
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_D')
# mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="123456",db="ire_gehua", charset='utf8')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
print "connected"
mysqlcursor = mysqlconn.cursor()
mysqlcursor.execute('''select distinct(datestyle)from errorexport order by datestyle desc limit 1,28''')
datestyle_dis=mysqlcursor.fetchall()



rowNumber=1
errorCodeList=['01_000%','01-300%','02-200%','02-300%','5203%','5206%','5225%']
# errorCodeList=['5206']
r=0
for one_datestyle_dis in datestyle_dis:
    j=1
    for i in range(0,48):
        j=1

        sum=0
        sum_per=0
        mysqlcursor.execute('''select vodSum from vodsum where cdn=%s and period= %s and datestyle=%s''',("CDN_D",i,one_datestyle_dis[0]))
        # mysqlcursor.execute('''select vodSum from errorexport where cdn=%s and period= %s and datestyle=%s limit 1''',("CDN_D",i,one_datestyle_dis[0]))
        VodSum_Result=mysqlcursor.fetchall()
        tmpCount=0

        rowNumber+=1
        for one_VodSum_Result in VodSum_Result:
            j=1
            tmpCount+=1
            VodSum=one_VodSum_Result[0]



            for oneErrorCode in errorCodeList:

                j+=1

                # mysqlcursor.execute('''select sum(count)from errorexport where cdn=%s and errorcode like %s and period= %s and datestyle=%s''',("CDN_D",oneErrorCode, i,one_datestyle_dis[0]))
                # mysqlcursor.execute('''select sum(count)from errorexport where cdn=%s and substring(errorcode,1,4)=%s  and period= %s and datestyle=%s''',("CDN_A",oneErrorCode, i,one_datestyle_dis[0]))
                mysqlcursor.execute('''select sum(count)from errorexport where cdn=%s and errorcode like %s  and period= %s and datestyle=%s''',("CDN_A",oneErrorCode, i,one_datestyle_dis[0]))
                result1=mysqlcursor.fetchall()
                for oneresult_count1 in result1:
                    count_temp=oneresult_count1[0]
                    if count_temp is None:
                        count_temp=0
                    sum=sum+count_temp
                    if VodSum is None:
                        count_per=format(0,'.2%')
                    else:
                        if int(VodSum)!=0:
                            count_per=format(float(count_temp)/float(VodSum), '.2%')
                        else:
                            count_per=format(0,'.2%')
                    if VodSum is None:
                        sum_per=format(0,'.2%')
                    else:
                        if int(VodSum)!=0:
                            sum_per=format(float(sum)/float(VodSum), '.2%')
                        else:
                            sum_per=format(0,'.2%')
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
                    if i==0:
                        q='00:00-00:30'
                    if i==1:
                        q='00:30-01:00'
                    if i==2:
                        q='01:00-01:30'
                    if i==3:
                        q='01:30-02:00'
                    if i==4:
                        q='02:00-02:30'
                    if i==5:
                        q='02:30-03:00'
                    if i==6:
                        q='03:00-03:30'
                    if i==7:
                        q='03:30-04:00'
                    if i==8:
                        q='04:00-04:30'
                    if i==9:
                        q='04:30-05:00'
                    if i==10:
                        q='05:00-05:30'
                    if i==11:
                        q='05:30-06:00'
                    if i==12:
                        q='06:00-06:30'
                    if i==13:
                        q='06:30-07:00'
                    if i==14:
                        q='07:00-07:30'
                    if i==15:
                        q='07:30-08:00'
                    if i==16:
                        q='08:00-08:30'
                    if i==17:
                        q='08:30-09:00'
                    if i==18:
                        q='09:00-09:30'
                    if i==19:
                        q='09:30-10:00'
                    if i==20:
                        q='10:00-10:30'
                    if i==21:
                        q='10:30-11:00'
                    if i==22:
                        q='11:00-11:30'
                    if i==23:
                        q='11:30-12:00'
                    if i==24:
                        q='12:00-12:30'
                    if i==25:
                        q='12:30-13:00'
                    if i==26:
                        q='13:00-13:30'
                    if i==27:
                        q='13:30-14:00'
                    if i==28:
                        q='14:00-14:30'
                    if i==29:
                        q='14:30-15:00'
                    if i==30:
                        q='15:00-15:30'
                    if i==31:
                        q='15:30-16:00'
                    if i==32:
                        q='16:00-16:30'
                    if i==33:
                        q='16:30-17:00'
                    if i==34:
                        q='17:00-17:30'
                    if i==35:
                        q='17:30-18:00'
                    if i==36:
                        q='18:00-18:30'
                    if i==37:
                        q='18:30-19:00'
                    if i==38:
                        q='19:00-19:30'
                    if i==39:
                        q='19:30-20:00'
                    if i==40:
                        q='20:00-20:30'
                    if i==41:
                        q='20:30-21:00'
                    if i==42:
                        q='21:00-21:30'
                    if i==43:
                        q='21:30-22:00'
                    if i==44:
                        q='22:00-22:30'
                    if i==45:
                        q='22:30-23:00'
                    if i==46:
                        q='23:00-23:30'
                    if i==47:
                        q='23:30-24:00'
                    ws.write(rowNumber,0,str(one_datestyle_dis[0]),style)
                    ws.write(rowNumber,1,str(q),style)
                    ws.write(rowNumber,j,str(count_temp),style)
                    ws.write(rowNumber,j+8,str(count_per),style)
                    #zong cuo wu lv
                    ws.write(rowNumber,17,str(sum_per),style)
                    #zong cuo wu liang
                    ws.write(rowNumber,18,str(sum),style)

        if tmpCount == 0:
            for j in range(1,8):
                j+=1
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
                ws.write(rowNumber,0,str(one_datestyle_dis[0]),style)
                if i==0:
                    q='00:00-00:30'
                if i==1:
                    q='00:30-01:00'
                if i==2:
                    q='01:00-01:30'
                if i==3:
                    q='01:30-02:00'
                if i==4:
                    q='02:00-02:30'
                if i==5:
                    q='02:30-03:00'
                if i==6:
                    q='03:00-03:30'
                if i==7:
                    q='03:30-04:00'
                if i==8:
                    q='04:00-04:30'
                if i==9:
                    q='04:30-05:00'
                if i==10:
                    q='05:00-05:30'
                if i==11:
                    q='05:30-06:00'
                if i==12:
                    q='06:00-06:30'
                if i==13:
                    q='06:30-07:00'
                if i==14:
                    q='07:00-07:30'
                if i==15:
                    q='07:30-08:00'
                if i==16:
                    q='08:00-08:30'
                if i==17:
                    q='08:30-09:00'
                if i==18:
                    q='09:00-09:30'
                if i==19:
                    q='09:30-10:00'
                if i==20:
                    q='10:00-10:30'
                if i==21:
                    q='10:30-11:00'
                if i==22:
                    q='11:00-11:30'
                if i==23:
                    q='11:30-12:00'
                if i==24:
                    q='12:00-12:30'
                if i==25:
                    q='12:30-13:00'
                if i==26:
                    q='13:00-13:30'
                if i==27:
                    q='13:30-14:00'
                if i==28:
                    q='14:00-14:30'
                if i==29:
                    q='14:30-15:00'
                if i==30:
                    q='15:00-15:30'
                if i==31:
                    q='15:30-16:00'
                if i==32:
                    q='16:00-16:30'
                if i==33:
                    q='16:30-17:00'
                if i==34:
                    q='17:00-17:30'
                if i==35:
                    q='17:30-18:00'
                if i==36:
                    q='18:00-18:30'
                if i==37:
                    q='18:30-19:00'
                if i==38:
                    q='19:00-19:30'
                if i==39:
                    q='19:30-20:00'
                if i==40:
                    q='20:00-20:30'
                if i==41:
                    q='20:30-21:00'
                if i==42:
                    q='21:00-21:30'
                if i==43:
                    q='21:30-22:00'
                if i==44:
                    q='22:00-22:30'
                if i==45:
                    q='22:30-23:00'
                if i==46:
                    q='23:00-23:30'
                if i==47:
                    q='23:30-24:00'

                ws.write(rowNumber,1,str(q),style)
                ws.write(rowNumber,j,str('0'),style)
                ws.write(rowNumber,j+8,str(format(0,'.2%')),style)
                #zong cuo wu lv
                ws.write(rowNumber,17,str(format(0,'.2%')),style)
                #zong cuo wu liang
                ws.write(rowNumber,18,str('0'),style)


                ws.write(0, 0, 'Firstname',style)
    ws.write_merge(2+r*48,49+r*48,0,0,str(one_datestyle_dis[0]),style)
    r+=1






####should be from errorexport


mysqlcursor.execute('''select distinct(datestyle)from errorexport order by datestyle desc limit 1,28''')
datestyle_dis=mysqlcursor.fetchall()
rowNumber=2

for one_datestyle_dis in datestyle_dis:
    # print one_datestyle_dis[0]
    period_list=range(0,48)
    for one_period in period_list:
        #######pls use the "sum" function in mysql
        mysqlcursor.execute('''select sum(count) from vodcount where cdn=%s and period=%s and datestyle=%s''',("CDN_D",one_period,one_datestyle_dis[0]))
        result_count=mysqlcursor.fetchall()
        for one_result_count in result_count:
            #####should define this new var temp_sum
            temp_sum=one_result_count[0]
            if temp_sum is None:
                temp_sum=0

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
            ws.write(rowNumber,19,str(temp_sum),style)
            rowNumber+=1





w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_D/ErrorSum(Hour)-GH-CDN-D.xls')


mysqlconn.close()



################################E!!!!!!!!!!!!!!!!!!!!!!!!!!!#########################
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_E')
# mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="123456",db="ire_gehua", charset='utf8')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
print "connected"
mysqlcursor = mysqlconn.cursor()
mysqlcursor.execute('''select distinct(datestyle)from errorexport order by datestyle desc limit 1,28''')
datestyle_dis=mysqlcursor.fetchall()



rowNumber=1
errorCodeList=['01_000%','01-300%','02-200%','02-300%','5203%','5206%','5225%']
# errorCodeList=['5206']
r=0
for one_datestyle_dis in datestyle_dis:
    j=1
    for i in range(0,48):
        j=1

        sum=0
        sum_per=0
        mysqlcursor.execute('''select vodSum from vodsum where cdn=%s and period= %s and datestyle=%s''',("CDN_E",i,one_datestyle_dis[0]))
        # mysqlcursor.execute('''select vodSum from errorexport where cdn=%s and period= %s and datestyle=%s limit 1''',("CDN_E",i,one_datestyle_dis[0]))
        VodSum_Result=mysqlcursor.fetchall()
        tmpCount=0

        rowNumber+=1
        for one_VodSum_Result in VodSum_Result:
            j=1
            tmpCount+=1
            VodSum=one_VodSum_Result[0]



            for oneErrorCode in errorCodeList:

                j+=1

                # mysqlcursor.execute('''select sum(count)from errorexport where cdn=%s and errorcode like %s and period= %s and datestyle=%s''',("CDN_E",oneErrorCode, i,one_datestyle_dis[0]))
                mysqlcursor.execute('''select sum(count)from errorexport where cdn=%s and errorcode like %s  and period= %s and datestyle=%s''',("CDN_A",oneErrorCode, i,one_datestyle_dis[0]))
                result1=mysqlcursor.fetchall()
                for oneresult_count1 in result1:
                    count_temp=oneresult_count1[0]
                    if count_temp is None:
                        count_temp=0
                    sum=sum+count_temp
                    if VodSum is None:
                        count_per=format(0,'.2%')
                    else:
                        if int(VodSum)!=0:
                            count_per=format(float(count_temp)/float(VodSum), '.2%')
                        else:
                            count_per=format(0,'.2%')
                    if VodSum is None:
                        sum_per=format(0,'.2%')
                    else:
                        if int(VodSum)!=0:
                            sum_per=format(float(sum)/float(VodSum), '.2%')
                        else:
                            sum_per=format(0,'.2%')
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
                    if i==0:
                        q='00:00-00:30'
                    if i==1:
                        q='00:30-01:00'
                    if i==2:
                        q='01:00-01:30'
                    if i==3:
                        q='01:30-02:00'
                    if i==4:
                        q='02:00-02:30'
                    if i==5:
                        q='02:30-03:00'
                    if i==6:
                        q='03:00-03:30'
                    if i==7:
                        q='03:30-04:00'
                    if i==8:
                        q='04:00-04:30'
                    if i==9:
                        q='04:30-05:00'
                    if i==10:
                        q='05:00-05:30'
                    if i==11:
                        q='05:30-06:00'
                    if i==12:
                        q='06:00-06:30'
                    if i==13:
                        q='06:30-07:00'
                    if i==14:
                        q='07:00-07:30'
                    if i==15:
                        q='07:30-08:00'
                    if i==16:
                        q='08:00-08:30'
                    if i==17:
                        q='08:30-09:00'
                    if i==18:
                        q='09:00-09:30'
                    if i==19:
                        q='09:30-10:00'
                    if i==20:
                        q='10:00-10:30'
                    if i==21:
                        q='10:30-11:00'
                    if i==22:
                        q='11:00-11:30'
                    if i==23:
                        q='11:30-12:00'
                    if i==24:
                        q='12:00-12:30'
                    if i==25:
                        q='12:30-13:00'
                    if i==26:
                        q='13:00-13:30'
                    if i==27:
                        q='13:30-14:00'
                    if i==28:
                        q='14:00-14:30'
                    if i==29:
                        q='14:30-15:00'
                    if i==30:
                        q='15:00-15:30'
                    if i==31:
                        q='15:30-16:00'
                    if i==32:
                        q='16:00-16:30'
                    if i==33:
                        q='16:30-17:00'
                    if i==34:
                        q='17:00-17:30'
                    if i==35:
                        q='17:30-18:00'
                    if i==36:
                        q='18:00-18:30'
                    if i==37:
                        q='18:30-19:00'
                    if i==38:
                        q='19:00-19:30'
                    if i==39:
                        q='19:30-20:00'
                    if i==40:
                        q='20:00-20:30'
                    if i==41:
                        q='20:30-21:00'
                    if i==42:
                        q='21:00-21:30'
                    if i==43:
                        q='21:30-22:00'
                    if i==44:
                        q='22:00-22:30'
                    if i==45:
                        q='22:30-23:00'
                    if i==46:
                        q='23:00-23:30'
                    if i==47:
                        q='23:30-24:00'
                    ws.write(rowNumber,0,str(one_datestyle_dis[0]),style)
                    ws.write(rowNumber,1,str(q),style)
                    ws.write(rowNumber,j,str(count_temp),style)
                    ws.write(rowNumber,j+8,str(count_per),style)
                    #zong cuo wu lv
                    ws.write(rowNumber,17,str(sum_per),style)
                    #zong cuo wu liang
                    ws.write(rowNumber,18,str(sum),style)

        if tmpCount == 0:
            for j in range(1,8):
                j+=1
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
                ws.write(rowNumber,0,str(one_datestyle_dis[0]),style)
                if i==0:
                    q='00:00-00:30'
                if i==1:
                    q='00:30-01:00'
                if i==2:
                    q='01:00-01:30'
                if i==3:
                    q='01:30-02:00'
                if i==4:
                    q='02:00-02:30'
                if i==5:
                    q='02:30-03:00'
                if i==6:
                    q='03:00-03:30'
                if i==7:
                    q='03:30-04:00'
                if i==8:
                    q='04:00-04:30'
                if i==9:
                    q='04:30-05:00'
                if i==10:
                    q='05:00-05:30'
                if i==11:
                    q='05:30-06:00'
                if i==12:
                    q='06:00-06:30'
                if i==13:
                    q='06:30-07:00'
                if i==14:
                    q='07:00-07:30'
                if i==15:
                    q='07:30-08:00'
                if i==16:
                    q='08:00-08:30'
                if i==17:
                    q='08:30-09:00'
                if i==18:
                    q='09:00-09:30'
                if i==19:
                    q='09:30-10:00'
                if i==20:
                    q='10:00-10:30'
                if i==21:
                    q='10:30-11:00'
                if i==22:
                    q='11:00-11:30'
                if i==23:
                    q='11:30-12:00'
                if i==24:
                    q='12:00-12:30'
                if i==25:
                    q='12:30-13:00'
                if i==26:
                    q='13:00-13:30'
                if i==27:
                    q='13:30-14:00'
                if i==28:
                    q='14:00-14:30'
                if i==29:
                    q='14:30-15:00'
                if i==30:
                    q='15:00-15:30'
                if i==31:
                    q='15:30-16:00'
                if i==32:
                    q='16:00-16:30'
                if i==33:
                    q='16:30-17:00'
                if i==34:
                    q='17:00-17:30'
                if i==35:
                    q='17:30-18:00'
                if i==36:
                    q='18:00-18:30'
                if i==37:
                    q='18:30-19:00'
                if i==38:
                    q='19:00-19:30'
                if i==39:
                    q='19:30-20:00'
                if i==40:
                    q='20:00-20:30'
                if i==41:
                    q='20:30-21:00'
                if i==42:
                    q='21:00-21:30'
                if i==43:
                    q='21:30-22:00'
                if i==44:
                    q='22:00-22:30'
                if i==45:
                    q='22:30-23:00'
                if i==46:
                    q='23:00-23:30'
                if i==47:
                    q='23:30-24:00'

                ws.write(rowNumber,1,str(q),style)
                ws.write(rowNumber,j,str('0'),style)
                ws.write(rowNumber,j+8,str(format(0,'.2%')),style)
                #zong cuo wu lv
                ws.write(rowNumber,17,str(format(0,'.2%')),style)
                #zong cuo wu liang
                ws.write(rowNumber,18,str('0'),style)


                ws.write(0, 0, 'Firstname',style)
    ws.write_merge(2+r*48,49+r*48,0,0,str(one_datestyle_dis[0]),style)
    r+=1






####should be from errorexport


mysqlcursor.execute('''select distinct(datestyle)from errorexport order by datestyle desc limit 1,28''')
datestyle_dis=mysqlcursor.fetchall()
rowNumber=2

for one_datestyle_dis in datestyle_dis:
    # print one_datestyle_dis[0]
    period_list=range(0,48)
    for one_period in period_list:
        #######pls use the "sum" function in mysql
        mysqlcursor.execute('''select sum(count) from vodcount where cdn=%s and period=%s and datestyle=%s''',("CDN_E",one_period,one_datestyle_dis[0]))
        result_count=mysqlcursor.fetchall()
        for one_result_count in result_count:
            #####should define this new var temp_sum
            temp_sum=one_result_count[0]
            if temp_sum is None:
                temp_sum=0

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
            ws.write(rowNumber,19,str(temp_sum),style)
            rowNumber+=1





w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_E/ErrorSum(Hour)-GH-CDN-E.xls')


mysqlconn.close()

#####################F!!!!!!!!!!!!!!!!!!!#####################
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_F')
# mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="123456",db="ire_gehua", charset='utf8')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
print "connected"
mysqlcursor = mysqlconn.cursor()
mysqlcursor.execute('''select distinct(datestyle)from errorexport order by datestyle desc limit 1,28''')
datestyle_dis=mysqlcursor.fetchall()



rowNumber=1
errorCodeList=['01_000%','01-300%','02-200%','02-300%','5203%','5206%','5225%']
# errorCodeList=['5206']
r=0
for one_datestyle_dis in datestyle_dis:
    j=1
    for i in range(0,48):
        j=1

        sum=0
        sum_per=0
        mysqlcursor.execute('''select vodSum from vodsum where cdn=%s and period= %s and datestyle=%s''',("CDN_F",i,one_datestyle_dis[0]))
        # mysqlcursor.execute('''select vodSum from errorexport where cdn=%s and period= %s and datestyle=%s limit 1''',("CDN_F",i,one_datestyle_dis[0]))
        VodSum_Result=mysqlcursor.fetchall()
        tmpCount=0

        rowNumber+=1
        for one_VodSum_Result in VodSum_Result:
            j=1
            tmpCount+=1
            VodSum=one_VodSum_Result[0]



            for oneErrorCode in errorCodeList:

                j+=1

                # mysqlcursor.execute('''select sum(count)from errorexport where cdn=%s and errorcode like %s and period= %s and datestyle=%s''',("CDN_F",oneErrorCode, i,one_datestyle_dis[0]))
                mysqlcursor.execute('''select sum(count)from errorexport where cdn=%s and errorcode like %s  and period= %s and datestyle=%s''',("CDN_A",oneErrorCode, i,one_datestyle_dis[0]))
                result1=mysqlcursor.fetchall()
                for oneresult_count1 in result1:
                    count_temp=oneresult_count1[0]
                    if count_temp is None:
                        count_temp=0
                    sum=sum+count_temp
                    if VodSum is None:
                        count_per=format(0,'.2%')
                    else:
                        if int(VodSum)!=0:
                            count_per=format(float(count_temp)/float(VodSum), '.2%')
                        else:
                            count_per=format(0,'.2%')
                    if VodSum is None:
                        sum_per=format(0,'.2%')
                    else:
                        if int(VodSum)!=0:
                            sum_per=format(float(sum)/float(VodSum), '.2%')
                        else:
                            sum_per=format(0,'.2%')
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
                    if i==0:
                        q='00:00-00:30'
                    if i==1:
                        q='00:30-01:00'
                    if i==2:
                        q='01:00-01:30'
                    if i==3:
                        q='01:30-02:00'
                    if i==4:
                        q='02:00-02:30'
                    if i==5:
                        q='02:30-03:00'
                    if i==6:
                        q='03:00-03:30'
                    if i==7:
                        q='03:30-04:00'
                    if i==8:
                        q='04:00-04:30'
                    if i==9:
                        q='04:30-05:00'
                    if i==10:
                        q='05:00-05:30'
                    if i==11:
                        q='05:30-06:00'
                    if i==12:
                        q='06:00-06:30'
                    if i==13:
                        q='06:30-07:00'
                    if i==14:
                        q='07:00-07:30'
                    if i==15:
                        q='07:30-08:00'
                    if i==16:
                        q='08:00-08:30'
                    if i==17:
                        q='08:30-09:00'
                    if i==18:
                        q='09:00-09:30'
                    if i==19:
                        q='09:30-10:00'
                    if i==20:
                        q='10:00-10:30'
                    if i==21:
                        q='10:30-11:00'
                    if i==22:
                        q='11:00-11:30'
                    if i==23:
                        q='11:30-12:00'
                    if i==24:
                        q='12:00-12:30'
                    if i==25:
                        q='12:30-13:00'
                    if i==26:
                        q='13:00-13:30'
                    if i==27:
                        q='13:30-14:00'
                    if i==28:
                        q='14:00-14:30'
                    if i==29:
                        q='14:30-15:00'
                    if i==30:
                        q='15:00-15:30'
                    if i==31:
                        q='15:30-16:00'
                    if i==32:
                        q='16:00-16:30'
                    if i==33:
                        q='16:30-17:00'
                    if i==34:
                        q='17:00-17:30'
                    if i==35:
                        q='17:30-18:00'
                    if i==36:
                        q='18:00-18:30'
                    if i==37:
                        q='18:30-19:00'
                    if i==38:
                        q='19:00-19:30'
                    if i==39:
                        q='19:30-20:00'
                    if i==40:
                        q='20:00-20:30'
                    if i==41:
                        q='20:30-21:00'
                    if i==42:
                        q='21:00-21:30'
                    if i==43:
                        q='21:30-22:00'
                    if i==44:
                        q='22:00-22:30'
                    if i==45:
                        q='22:30-23:00'
                    if i==46:
                        q='23:00-23:30'
                    if i==47:
                        q='23:30-24:00'
                    ws.write(rowNumber,0,str(one_datestyle_dis[0]),style)
                    ws.write(rowNumber,1,str(q),style)
                    ws.write(rowNumber,j,str(count_temp),style)
                    ws.write(rowNumber,j+8,str(count_per),style)
                    #zong cuo wu lv
                    ws.write(rowNumber,17,str(sum_per),style)
                    #zong cuo wu liang
                    ws.write(rowNumber,18,str(sum),style)

        if tmpCount == 0:
            for j in range(1,8):
                j+=1
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
                ws.write(rowNumber,0,str(one_datestyle_dis[0]),style)
                if i==0:
                    q='00:00-00:30'
                if i==1:
                    q='00:30-01:00'
                if i==2:
                    q='01:00-01:30'
                if i==3:
                    q='01:30-02:00'
                if i==4:
                    q='02:00-02:30'
                if i==5:
                    q='02:30-03:00'
                if i==6:
                    q='03:00-03:30'
                if i==7:
                    q='03:30-04:00'
                if i==8:
                    q='04:00-04:30'
                if i==9:
                    q='04:30-05:00'
                if i==10:
                    q='05:00-05:30'
                if i==11:
                    q='05:30-06:00'
                if i==12:
                    q='06:00-06:30'
                if i==13:
                    q='06:30-07:00'
                if i==14:
                    q='07:00-07:30'
                if i==15:
                    q='07:30-08:00'
                if i==16:
                    q='08:00-08:30'
                if i==17:
                    q='08:30-09:00'
                if i==18:
                    q='09:00-09:30'
                if i==19:
                    q='09:30-10:00'
                if i==20:
                    q='10:00-10:30'
                if i==21:
                    q='10:30-11:00'
                if i==22:
                    q='11:00-11:30'
                if i==23:
                    q='11:30-12:00'
                if i==24:
                    q='12:00-12:30'
                if i==25:
                    q='12:30-13:00'
                if i==26:
                    q='13:00-13:30'
                if i==27:
                    q='13:30-14:00'
                if i==28:
                    q='14:00-14:30'
                if i==29:
                    q='14:30-15:00'
                if i==30:
                    q='15:00-15:30'
                if i==31:
                    q='15:30-16:00'
                if i==32:
                    q='16:00-16:30'
                if i==33:
                    q='16:30-17:00'
                if i==34:
                    q='17:00-17:30'
                if i==35:
                    q='17:30-18:00'
                if i==36:
                    q='18:00-18:30'
                if i==37:
                    q='18:30-19:00'
                if i==38:
                    q='19:00-19:30'
                if i==39:
                    q='19:30-20:00'
                if i==40:
                    q='20:00-20:30'
                if i==41:
                    q='20:30-21:00'
                if i==42:
                    q='21:00-21:30'
                if i==43:
                    q='21:30-22:00'
                if i==44:
                    q='22:00-22:30'
                if i==45:
                    q='22:30-23:00'
                if i==46:
                    q='23:00-23:30'
                if i==47:
                    q='23:30-24:00'

                ws.write(rowNumber,1,str(q),style)
                ws.write(rowNumber,j,str('0'),style)
                ws.write(rowNumber,j+8,str(format(0,'.2%')),style)
                #zong cuo wu lv
                ws.write(rowNumber,17,str(format(0,'.2%')),style)
                #zong cuo wu liang
                ws.write(rowNumber,18,str('0'),style)


                ws.write(0, 0, 'Firstname',style)
    ws.write_merge(2+r*48,49+r*48,0,0,str(one_datestyle_dis[0]),style)
    r+=1






####should be from errorexport


mysqlcursor.execute('''select distinct(datestyle)from errorexport order by datestyle desc limit 1,28''')
datestyle_dis=mysqlcursor.fetchall()
rowNumber=2

for one_datestyle_dis in datestyle_dis:
    # print one_datestyle_dis[0]
    period_list=range(0,48)
    for one_period in period_list:
        #######pls use the "sum" function in mysql
        mysqlcursor.execute('''select sum(count) from vodcount where cdn=%s and period=%s and datestyle=%s''',("CDN_F",one_period,one_datestyle_dis[0]))
        result_count=mysqlcursor.fetchall()
        for one_result_count in result_count:
            #####should define this new var temp_sum
            temp_sum=one_result_count[0]
            if temp_sum is None:
                temp_sum=0

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
            ws.write(rowNumber,19,str(temp_sum),style)
            rowNumber+=1





w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_F/ErrorSum(Hour)-GH-CDN-F.xls')


mysqlconn.close()



#############################G!!!!!!!!!!!!!!!!!!!!########################
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_G')
# mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="123456",db="ire_gehua", charset='utf8')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
print "connected"
mysqlcursor = mysqlconn.cursor()
mysqlcursor.execute('''select distinct(datestyle)from errorexport order by datestyle desc limit 1,28''')
datestyle_dis=mysqlcursor.fetchall()



rowNumber=1
errorCodeList=['01_000%','01-300%','02-200%','02-300%','5203%','5206%','5225%']
# errorCodeList=['5206']
r=0
for one_datestyle_dis in datestyle_dis:
    j=1
    for i in range(0,48):
        j=1

        sum=0
        sum_per=0
        mysqlcursor.execute('''select vodSum from vodsum where cdn=%s and period= %s and datestyle=%s''',("CDN_G",i,one_datestyle_dis[0]))
        # mysqlcursor.execute('''select vodSum from errorexport where cdn=%s and period= %s and datestyle=%s limit 1''',("CDN_G",i,one_datestyle_dis[0]))
        VodSum_Result=mysqlcursor.fetchall()
        tmpCount=0

        rowNumber+=1
        for one_VodSum_Result in VodSum_Result:
            j=1
            tmpCount+=1
            VodSum=one_VodSum_Result[0]



            for oneErrorCode in errorCodeList:

                j+=1

                # mysqlcursor.execute('''select sum(count)from errorexport where cdn=%s and errorcode like %s and period= %s and datestyle=%s''',("CDN_G",oneErrorCode, i,one_datestyle_dis[0]))
                mysqlcursor.execute('''select sum(count)from errorexport where cdn=%s and errorcode like %s  and period= %s and datestyle=%s''',("CDN_A",oneErrorCode, i,one_datestyle_dis[0]))
                result1=mysqlcursor.fetchall()
                for oneresult_count1 in result1:
                    count_temp=oneresult_count1[0]
                    if count_temp is None:
                        count_temp=0
                    sum=sum+count_temp
                    if VodSum is None:
                        count_per=format(0,'.2%')
                    else:
                        if int(VodSum)!=0:
                            count_per=format(float(count_temp)/float(VodSum), '.2%')
                        else:
                            count_per=format(0,'.2%')
                    if VodSum is None:
                        sum_per=format(0,'.2%')
                    else:
                        if int(VodSum)!=0:
                            sum_per=format(float(sum)/float(VodSum), '.2%')
                        else:
                            sum_per=format(0,'.2%')
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
                    if i==0:
                        q='00:00-00:30'
                    if i==1:
                        q='00:30-01:00'
                    if i==2:
                        q='01:00-01:30'
                    if i==3:
                        q='01:30-02:00'
                    if i==4:
                        q='02:00-02:30'
                    if i==5:
                        q='02:30-03:00'
                    if i==6:
                        q='03:00-03:30'
                    if i==7:
                        q='03:30-04:00'
                    if i==8:
                        q='04:00-04:30'
                    if i==9:
                        q='04:30-05:00'
                    if i==10:
                        q='05:00-05:30'
                    if i==11:
                        q='05:30-06:00'
                    if i==12:
                        q='06:00-06:30'
                    if i==13:
                        q='06:30-07:00'
                    if i==14:
                        q='07:00-07:30'
                    if i==15:
                        q='07:30-08:00'
                    if i==16:
                        q='08:00-08:30'
                    if i==17:
                        q='08:30-09:00'
                    if i==18:
                        q='09:00-09:30'
                    if i==19:
                        q='09:30-10:00'
                    if i==20:
                        q='10:00-10:30'
                    if i==21:
                        q='10:30-11:00'
                    if i==22:
                        q='11:00-11:30'
                    if i==23:
                        q='11:30-12:00'
                    if i==24:
                        q='12:00-12:30'
                    if i==25:
                        q='12:30-13:00'
                    if i==26:
                        q='13:00-13:30'
                    if i==27:
                        q='13:30-14:00'
                    if i==28:
                        q='14:00-14:30'
                    if i==29:
                        q='14:30-15:00'
                    if i==30:
                        q='15:00-15:30'
                    if i==31:
                        q='15:30-16:00'
                    if i==32:
                        q='16:00-16:30'
                    if i==33:
                        q='16:30-17:00'
                    if i==34:
                        q='17:00-17:30'
                    if i==35:
                        q='17:30-18:00'
                    if i==36:
                        q='18:00-18:30'
                    if i==37:
                        q='18:30-19:00'
                    if i==38:
                        q='19:00-19:30'
                    if i==39:
                        q='19:30-20:00'
                    if i==40:
                        q='20:00-20:30'
                    if i==41:
                        q='20:30-21:00'
                    if i==42:
                        q='21:00-21:30'
                    if i==43:
                        q='21:30-22:00'
                    if i==44:
                        q='22:00-22:30'
                    if i==45:
                        q='22:30-23:00'
                    if i==46:
                        q='23:00-23:30'
                    if i==47:
                        q='23:30-24:00'
                    ws.write(rowNumber,0,str(one_datestyle_dis[0]),style)
                    ws.write(rowNumber,1,str(q),style)
                    ws.write(rowNumber,j,str(count_temp),style)
                    ws.write(rowNumber,j+8,str(count_per),style)
                    #zong cuo wu lv
                    ws.write(rowNumber,17,str(sum_per),style)
                    #zong cuo wu liang
                    ws.write(rowNumber,18,str(sum),style)

        if tmpCount == 0:
            for j in range(1,8):
                j+=1
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
                ws.write(rowNumber,0,str(one_datestyle_dis[0]),style)
                if i==0:
                    q='00:00-00:30'
                if i==1:
                    q='00:30-01:00'
                if i==2:
                    q='01:00-01:30'
                if i==3:
                    q='01:30-02:00'
                if i==4:
                    q='02:00-02:30'
                if i==5:
                    q='02:30-03:00'
                if i==6:
                    q='03:00-03:30'
                if i==7:
                    q='03:30-04:00'
                if i==8:
                    q='04:00-04:30'
                if i==9:
                    q='04:30-05:00'
                if i==10:
                    q='05:00-05:30'
                if i==11:
                    q='05:30-06:00'
                if i==12:
                    q='06:00-06:30'
                if i==13:
                    q='06:30-07:00'
                if i==14:
                    q='07:00-07:30'
                if i==15:
                    q='07:30-08:00'
                if i==16:
                    q='08:00-08:30'
                if i==17:
                    q='08:30-09:00'
                if i==18:
                    q='09:00-09:30'
                if i==19:
                    q='09:30-10:00'
                if i==20:
                    q='10:00-10:30'
                if i==21:
                    q='10:30-11:00'
                if i==22:
                    q='11:00-11:30'
                if i==23:
                    q='11:30-12:00'
                if i==24:
                    q='12:00-12:30'
                if i==25:
                    q='12:30-13:00'
                if i==26:
                    q='13:00-13:30'
                if i==27:
                    q='13:30-14:00'
                if i==28:
                    q='14:00-14:30'
                if i==29:
                    q='14:30-15:00'
                if i==30:
                    q='15:00-15:30'
                if i==31:
                    q='15:30-16:00'
                if i==32:
                    q='16:00-16:30'
                if i==33:
                    q='16:30-17:00'
                if i==34:
                    q='17:00-17:30'
                if i==35:
                    q='17:30-18:00'
                if i==36:
                    q='18:00-18:30'
                if i==37:
                    q='18:30-19:00'
                if i==38:
                    q='19:00-19:30'
                if i==39:
                    q='19:30-20:00'
                if i==40:
                    q='20:00-20:30'
                if i==41:
                    q='20:30-21:00'
                if i==42:
                    q='21:00-21:30'
                if i==43:
                    q='21:30-22:00'
                if i==44:
                    q='22:00-22:30'
                if i==45:
                    q='22:30-23:00'
                if i==46:
                    q='23:00-23:30'
                if i==47:
                    q='23:30-24:00'

                ws.write(rowNumber,1,str(q),style)
                ws.write(rowNumber,j,str('0'),style)
                ws.write(rowNumber,j+8,str(format(0,'.2%')),style)
                #zong cuo wu lv
                ws.write(rowNumber,17,str(format(0,'.2%')),style)
                #zong cuo wu liang
                ws.write(rowNumber,18,str('0'),style)


                ws.write(0, 0, 'Firstname',style)
    ws.write_merge(2+r*48,49+r*48,0,0,str(one_datestyle_dis[0]),style)
    r+=1






####should be from errorexport


mysqlcursor.execute('''select distinct(datestyle)from errorexport order by datestyle desc limit 1,28''')
datestyle_dis=mysqlcursor.fetchall()
rowNumber=2

for one_datestyle_dis in datestyle_dis:
    # print one_datestyle_dis[0]
    period_list=range(0,48)
    for one_period in period_list:
        #######pls use the "sum" function in mysql
        mysqlcursor.execute('''select sum(count) from vodcount where cdn=%s and period=%s and datestyle=%s''',("CDN_G",one_period,one_datestyle_dis[0]))
        result_count=mysqlcursor.fetchall()
        for one_result_count in result_count:
            #####should define this new var temp_sum
            temp_sum=one_result_count[0]
            if temp_sum is None:
                temp_sum=0

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
            ws.write(rowNumber,19,str(temp_sum),style)
            rowNumber+=1





w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_G/ErrorSum(Hour)-GH-CDN-G.xls')


mysqlconn.close()



#########################H!########################
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_H')
# mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="123456",db="ire_gehua", charset='utf8')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
print "connected"
mysqlcursor = mysqlconn.cursor()
mysqlcursor.execute('''select distinct(datestyle)from errorexport order by datestyle desc limit 1,28''')
datestyle_dis=mysqlcursor.fetchall()



rowNumber=1
errorCodeList=['01_000%','01-300%','02-200%','02-300%','5203%','5206%','5225%']
# errorCodeList=['5206']
r=0
for one_datestyle_dis in datestyle_dis:
    j=1
    for i in range(0,48):
        j=1

        sum=0
        sum_per=0
        mysqlcursor.execute('''select vodSum from vodsum where cdn=%s and period= %s and datestyle=%s''',("CDN_H",i,one_datestyle_dis[0]))
        # mysqlcursor.execute('''select vodSum from errorexport where cdn=%s and period= %s and datestyle=%s limit 1''',("CDN_H",i,one_datestyle_dis[0]))
        VodSum_Result=mysqlcursor.fetchall()
        tmpCount=0

        rowNumber+=1
        for one_VodSum_Result in VodSum_Result:
            j=1
            tmpCount+=1
            VodSum=one_VodSum_Result[0]



            for oneErrorCode in errorCodeList:

                j+=1

                # mysqlcursor.execute('''select sum(count)from errorexport where cdn=%s and errorcode like %s and period= %s and datestyle=%s''',("CDN_H",oneErrorCode, i,one_datestyle_dis[0]))
                mysqlcursor.execute('''select sum(count)from errorexport where cdn=%s and errorcode like %s  and period= %s and datestyle=%s''',("CDN_A",oneErrorCode, i,one_datestyle_dis[0]))
                result1=mysqlcursor.fetchall()
                for oneresult_count1 in result1:
                    count_temp=oneresult_count1[0]
                    if count_temp is None:
                        count_temp=0
                    sum=sum+count_temp
                    if VodSum is None:
                        count_per=format(0,'.2%')
                    else:
                        if int(VodSum)!=0:
                            count_per=format(float(count_temp)/float(VodSum), '.2%')
                        else:
                            count_per=format(0,'.2%')
                    if VodSum is None:
                        sum_per=format(0,'.2%')
                    else:
                        if int(VodSum)!=0:
                            sum_per=format(float(sum)/float(VodSum), '.2%')
                        else:
                            sum_per=format(0,'.2%')
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
                    if i==0:
                        q='00:00-00:30'
                    if i==1:
                        q='00:30-01:00'
                    if i==2:
                        q='01:00-01:30'
                    if i==3:
                        q='01:30-02:00'
                    if i==4:
                        q='02:00-02:30'
                    if i==5:
                        q='02:30-03:00'
                    if i==6:
                        q='03:00-03:30'
                    if i==7:
                        q='03:30-04:00'
                    if i==8:
                        q='04:00-04:30'
                    if i==9:
                        q='04:30-05:00'
                    if i==10:
                        q='05:00-05:30'
                    if i==11:
                        q='05:30-06:00'
                    if i==12:
                        q='06:00-06:30'
                    if i==13:
                        q='06:30-07:00'
                    if i==14:
                        q='07:00-07:30'
                    if i==15:
                        q='07:30-08:00'
                    if i==16:
                        q='08:00-08:30'
                    if i==17:
                        q='08:30-09:00'
                    if i==18:
                        q='09:00-09:30'
                    if i==19:
                        q='09:30-10:00'
                    if i==20:
                        q='10:00-10:30'
                    if i==21:
                        q='10:30-11:00'
                    if i==22:
                        q='11:00-11:30'
                    if i==23:
                        q='11:30-12:00'
                    if i==24:
                        q='12:00-12:30'
                    if i==25:
                        q='12:30-13:00'
                    if i==26:
                        q='13:00-13:30'
                    if i==27:
                        q='13:30-14:00'
                    if i==28:
                        q='14:00-14:30'
                    if i==29:
                        q='14:30-15:00'
                    if i==30:
                        q='15:00-15:30'
                    if i==31:
                        q='15:30-16:00'
                    if i==32:
                        q='16:00-16:30'
                    if i==33:
                        q='16:30-17:00'
                    if i==34:
                        q='17:00-17:30'
                    if i==35:
                        q='17:30-18:00'
                    if i==36:
                        q='18:00-18:30'
                    if i==37:
                        q='18:30-19:00'
                    if i==38:
                        q='19:00-19:30'
                    if i==39:
                        q='19:30-20:00'
                    if i==40:
                        q='20:00-20:30'
                    if i==41:
                        q='20:30-21:00'
                    if i==42:
                        q='21:00-21:30'
                    if i==43:
                        q='21:30-22:00'
                    if i==44:
                        q='22:00-22:30'
                    if i==45:
                        q='22:30-23:00'
                    if i==46:
                        q='23:00-23:30'
                    if i==47:
                        q='23:30-24:00'
                    ws.write(rowNumber,0,str(one_datestyle_dis[0]),style)
                    ws.write(rowNumber,1,str(q),style)
                    ws.write(rowNumber,j,str(count_temp),style)
                    ws.write(rowNumber,j+8,str(count_per),style)
                    #zong cuo wu lv
                    ws.write(rowNumber,17,str(sum_per),style)
                    #zong cuo wu liang
                    ws.write(rowNumber,18,str(sum),style)

        if tmpCount == 0:
            for j in range(1,8):
                j+=1
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
                ws.write(rowNumber,0,str(one_datestyle_dis[0]),style)
                if i==0:
                    q='00:00-00:30'
                if i==1:
                    q='00:30-01:00'
                if i==2:
                    q='01:00-01:30'
                if i==3:
                    q='01:30-02:00'
                if i==4:
                    q='02:00-02:30'
                if i==5:
                    q='02:30-03:00'
                if i==6:
                    q='03:00-03:30'
                if i==7:
                    q='03:30-04:00'
                if i==8:
                    q='04:00-04:30'
                if i==9:
                    q='04:30-05:00'
                if i==10:
                    q='05:00-05:30'
                if i==11:
                    q='05:30-06:00'
                if i==12:
                    q='06:00-06:30'
                if i==13:
                    q='06:30-07:00'
                if i==14:
                    q='07:00-07:30'
                if i==15:
                    q='07:30-08:00'
                if i==16:
                    q='08:00-08:30'
                if i==17:
                    q='08:30-09:00'
                if i==18:
                    q='09:00-09:30'
                if i==19:
                    q='09:30-10:00'
                if i==20:
                    q='10:00-10:30'
                if i==21:
                    q='10:30-11:00'
                if i==22:
                    q='11:00-11:30'
                if i==23:
                    q='11:30-12:00'
                if i==24:
                    q='12:00-12:30'
                if i==25:
                    q='12:30-13:00'
                if i==26:
                    q='13:00-13:30'
                if i==27:
                    q='13:30-14:00'
                if i==28:
                    q='14:00-14:30'
                if i==29:
                    q='14:30-15:00'
                if i==30:
                    q='15:00-15:30'
                if i==31:
                    q='15:30-16:00'
                if i==32:
                    q='16:00-16:30'
                if i==33:
                    q='16:30-17:00'
                if i==34:
                    q='17:00-17:30'
                if i==35:
                    q='17:30-18:00'
                if i==36:
                    q='18:00-18:30'
                if i==37:
                    q='18:30-19:00'
                if i==38:
                    q='19:00-19:30'
                if i==39:
                    q='19:30-20:00'
                if i==40:
                    q='20:00-20:30'
                if i==41:
                    q='20:30-21:00'
                if i==42:
                    q='21:00-21:30'
                if i==43:
                    q='21:30-22:00'
                if i==44:
                    q='22:00-22:30'
                if i==45:
                    q='22:30-23:00'
                if i==46:
                    q='23:00-23:30'
                if i==47:
                    q='23:30-24:00'

                ws.write(rowNumber,1,str(q),style)
                ws.write(rowNumber,j,str('0'),style)
                ws.write(rowNumber,j+8,str(format(0,'.2%')),style)
                #zong cuo wu lv
                ws.write(rowNumber,17,str(format(0,'.2%')),style)
                #zong cuo wu liang
                ws.write(rowNumber,18,str('0'),style)


                ws.write(0, 0, 'Firstname',style)
    ws.write_merge(2+r*48,49+r*48,0,0,str(one_datestyle_dis[0]),style)
    r+=1






####should be from errorexport


mysqlcursor.execute('''select distinct(datestyle)from errorexport order by datestyle desc limit 1,28''')
datestyle_dis=mysqlcursor.fetchall()
rowNumber=2

for one_datestyle_dis in datestyle_dis:
    # print one_datestyle_dis[0]
    period_list=range(0,48)
    for one_period in period_list:
        #######pls use the "sum" function in mysql
        mysqlcursor.execute('''select sum(count) from vodcount where cdn=%s and period=%s and datestyle=%s''',("CDN_H",one_period,one_datestyle_dis[0]))
        result_count=mysqlcursor.fetchall()
        for one_result_count in result_count:
            #####should define this new var temp_sum
            temp_sum=one_result_count[0]
            if temp_sum is None:
                temp_sum=0

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
            ws.write(rowNumber,19,str(temp_sum),style)
            rowNumber+=1





w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_H/ErrorSum(Hour)-GH-CDN-H.xls')


if os.path.exists(r'/tmp/ErrorReportPro/errorReport/ErrorSumIsReady.txt'):
    os.remove(r'/tmp/ErrorReportPro/errorReport/ErrorSumIsReady.txt')
f = open(r'/tmp/ErrorReportPro/errorReport/ErrorSumIsReady.txt', 'w')
f.close()
mysqlconn.close()