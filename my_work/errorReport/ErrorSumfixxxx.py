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




############################A!!!!!!!!!!!!!!!!!!##########################
if os.path.exists(r'/tmp/ErrorReportPro/errorReport/ErrorSumIsReady.txt'):
    os.remove(r'/tmp/ErrorReportPro/errorReport/ErrorSumIsReady.txt')
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_A')
# mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="123456",db="ire_gehua", charset='utf8')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
print "A connected"
mysqlcursor = mysqlconn.cursor()


# ##############Caculate new data and insert into mysql
cdn_list=['CDN_A','CDN_B','CDN_C','CDN_D','CDN_E','CDN_F','CDN_G','CDN_H']
errorCodeList=['01-000%','01-300%','02-200%','02-300%','5203%','5206%','5225%']
r=0

# j=1
# inter=1
# def iToq(i):
#     if i==0:
#         q='00:00-00:30'
#     if i==1:
#         q='00:30-01:00'
#     if i==2:
#         q='01:00-01:30'
#     if i==3:
#         q='01:30-02:00'
#     if i==4:
#         q='02:00-02:30'
#     if i==5:
#         q='02:30-03:00'
#     if i==6:
#         q='03:00-03:30'
#     if i==7:
#         q='03:30-04:00'
#     if i==8:
#         q='04:00-04:30'
#     if i==9:
#         q='04:30-05:00'
#     if i==10:
#         q='05:00-05:30'
#     if i==11:
#         q='05:30-06:00'
#     if i==12:
#         q='06:00-06:30'
#     if i==13:
#         q='06:30-07:00'
#     if i==14:
#         q='07:00-07:30'
#     if i==15:
#         q='07:30-08:00'
#     if i==16:
#         q='08:00-08:30'
#     if i==17:
#         q='08:30-09:00'
#     if i==18:
#         q='09:00-09:30'
#     if i==19:
#         q='09:30-10:00'
#     if i==20:
#         q='10:00-10:30'
#     if i==21:
#         q='10:30-11:00'
#     if i==22:
#         q='11:00-11:30'
#     if i==23:
#         q='11:30-12:00'
#     if i==24:
#         q='12:00-12:30'
#     if i==25:
#         q='12:30-13:00'
#     if i==26:
#         q='13:00-13:30'
#     if i==27:
#         q='13:30-14:00'
#     if i==28:
#         q='14:00-14:30'
#     if i==29:
#         q='14:30-15:00'
#     if i==30:
#         q='15:00-15:30'
#     if i==31:
#         q='15:30-16:00'
#     if i==32:
#         q='16:00-16:30'
#     if i==33:
#         q='16:30-17:00'
#     if i==34:
#         q='17:00-17:30'
#     if i==35:
#         q='17:30-18:00'
#     if i==36:
#         q='18:00-18:30'
#     if i==37:
#         q='18:30-19:00'
#     if i==38:
#         q='19:00-19:30'
#     if i==39:
#         q='19:30-20:00'
#     if i==40:
#         q='20:00-20:30'
#     if i==41:
#         q='20:30-21:00'
#     if i==42:
#         q='21:00-21:30'
#     if i==43:
#         q='21:30-22:00'
#     if i==44:
#         q='22:00-22:30'
#     if i==45:
#         q='22:30-23:00'
#     if i==46:
#         q='23:00-23:30'
#     if i==47:
#         q='23:30-24:00'
#     return  q
# for i in range(0,48):
#     # print "another period"
#     # j=1
#     # sum=0
#     # sum_per=0
#
#     # for inter in range(1,1):
#     for area in cdn_list:
#         Column1=list()
#         Column2=list()
#         Column3=list()
#         # j=1
#         sum=0
#         sum_per=0
#
#         mysqlcursor.execute('''select vodSum, datestyle from vodsum where cdn=%s and period= %s and datestyle=DATE(DATE_SUB(NOW(), INTERVAL %s DAY))''',(area,i,inter))
#         VodSum_Result=mysqlcursor.fetchall()
#         tmpCount=0
#
#         # rowNumber+=1
#         for one_VodSum_Result in VodSum_Result:
#             # print 1
#             # j=1
#             tmpCount+=1
#             VodSum=one_VodSum_Result[0]
#             date=one_VodSum_Result[1]
#
#             for oneErrorCode in errorCodeList:
#                 # j+=1
#
#                 mysqlcursor.execute('''select sum(count) from errorexport where cdn=%s and errorcode like %s  and period= %s and datestyle=DATE(DATE_SUB(NOW(), INTERVAL %s DAY))''',(area,oneErrorCode, i,inter))
#                 result1=mysqlcursor.fetchall()
#                 for oneresult_count1 in result1:
#                     # print "another errorcode"
#                     count_temp=oneresult_count1[0]
#                     # print oneresult_count1[1]
#                     if count_temp is None:
#                         count_temp=0
#                     sum=sum+count_temp
#                     if VodSum is None:
#                         count_per=format(0,'.2%')
#                     else:
#                         if int(VodSum)!=0:
#                             count_per=format(float(count_temp)/float(VodSum), '.2%')
#                         else:
#                             count_per=format(0,'.2%')
#                     if VodSum is None:
#                         sum_per=format(0,'.2%')
#                     else:
#                         if int(VodSum)!=0:
#                             sum_per=format(float(sum)/float(VodSum), '.2%')
#                         else:
#                             sum_per=format(0,'.2%')
#                     # Column.append(oneresult_count1[1])
#                     # Column.append(str(q))
#                     Column1.append(str(count_temp))
#                     Column2.append(str(count_per))
#
#             Column2.append(str(sum_per))
#             Column2.append(str(sum))
#             Column2.append(str(VodSum))
#             Column1.insert(0,str(date))
#             Column1.insert(1,str(iToq(i)))
#             Column3=Column1+Column2
#             Column3.append(str(area))
#         print Column3,len(Column3)
#
#
#         mysqlcursor.execute("Insert into errorsum (date, period,01_000_num,01_300_num,02_200_num,02_300_num, 5203_num ,5206_num,5225_num,01_000_r,01_300_r,02_200_r,02_300_r,5203_r,5206_r,5225_r,TotalErrorRate,TotalErrorNum,TotalCount, cdn) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", Column3)
#         mysqlconn.commit()
# #
# #
# #
# #
# # #################################sum
# inter=1
# for area in cdn_list:
# # print "insert into errorsum SELECT date, LEFT(period,2),sum(01_000_num),sum(01_300_num),sum(02_200_num),sum(02_300_num), sum(5203_num),sum(5206_num), sum(5225_num),concat(truncate(sum(01_000_r),2),'%%'),concat(truncate(sum(01_300_r),2),'%%'),concat(truncate(sum(02_200_r),2),'%%'),concat(truncate(sum(02_300_r),2),'%%'),concat(truncate(sum(5203_r),2),'%%'),concat(truncate(sum(5206_r),2),'%%'),concat(truncate(sum(5225_r),2),'%%'),concat(truncate(sum(TotalErrorRate),2),'%%'),sum(TotalErrorNum),sum(TotalCount), cdn from errorsum where cdn= %s and date=DATE(DATE_SUB(NOW(), INTERVAL %s DAY))" %(area,str(inter))
#
#     # for inter in range(1,28):
#     mysqlcursor.execute("insert into errorsum SELECT date, mid(period,3,1),sum(01_000_num),sum(01_300_num),sum(02_200_num),sum(02_300_num), sum(5203_num),sum(5206_num), sum(5225_num),concat(truncate(sum(01_000_r),2),'%%'),concat(truncate(sum(01_300_r),2),'%%'),concat(truncate(sum(02_200_r),2),'%%'),concat(truncate(sum(02_300_r),2),'%%'),concat(truncate(sum(5203_r),2),'%%'),concat(truncate(sum(5206_r),2),'%%'),concat(truncate(sum(5225_r),2),'%%'),concat(truncate(sum(TotalErrorRate),2),'%%'),sum(TotalErrorNum),sum(TotalCount), cdn from errorsum where cdn= '%s' and date=DATE(DATE_SUB(NOW(), INTERVAL %s DAY))" %(str(area),str(inter)))
#     mysqlcursor.execute("update errorsum set 01_000_r=concat(round((01_000_num/TotalCount)*100,2),'%%'),01_300_r=concat(round((01_300_num/TotalCount)*100,2),'%%'),02_200_r=concat(round((02_200_num/TotalCount)*100,2),'%%'),02_300_r=concat(round((02_300_num/TotalCount)*100,2),'%%'),5203_r=concat(round((5203_num/TotalCount)*100,2),'%%'),5206_r=concat(round((5206_num/TotalCount)*100,2),'%%'),5225_r=concat(round((5225_num/TotalCount)*100,2),'%%'), TotalErrorRate=concat(round((TotalErrorNum/TotalCount)*100,2),'%%') where period = ':' and cdn= '%s' and date=DATE(DATE_SUB(NOW(), INTERVAL %s DAY))" %(str(area),str(inter)))
#     mysqlconn.commit()
# # #
# mysqlcursor.execute("UPDATE errorsum set period='错误总数' where period=':';")
# mysqlconn.commit()
# #
#
#




#
# # ############################Insert into Excel!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!######################################
# rowNumber以及新建excel放到第一重循环内
cdn_list=['CDN_A','CDN_B','CDN_C','CDN_D','CDN_E','CDN_F','CDN_G','CDN_H']

# cdn_list=['CDN_H']
# mu=0
for one_cdn_list in cdn_list:
    rowNumber=2
    # mu+=1

    w=xlwt.Workbook(encoding = 'utf-8',style_compression=2)
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
    print one_cdn_list[4:5]
    se=0
    for num in range(1,29):
        # if mu==2:
        #     ir=0
        mysqlcursor.execute('''select * from errorsum where date=DATE(DATE_SUB(NOW(), INTERVAL %s DAY)) and cdn=%s ORDER BY period asc''',(num,one_cdn_list))
        result=mysqlcursor.fetchall()
        y=0
        for one_result in result:
            y=1
            for column in range(1,9):
                ws.write(rowNumber,column,str(one_result[column]),style)

            for column in range(10,20):
                ws.write(rowNumber,column,str(one_result[column-1]),style)

            rowNumber+=1
        if y==1:
            se+=1
            ws.write_merge(2+49*(se-1),50+49*(se-1),0,0,str(one_result[0]),style)
    w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_'+one_cdn_list[4:5]+'/ErrorSum(Hour)-GH-CDN-'+one_cdn_list[4:5]+'.xls')
    # w.save(r'E:\ErrorSum(Hour)-GH-CDN-'+one_cdn_list[4:5]+'.xls')
#

f = open(r'/tmp/ErrorReportPro/errorReport/ErrorSumIsReady.txt', 'w')
f.close()


mysqlconn.close()




