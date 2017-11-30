#coding:UTF-8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import MySQLdb
import xlwt
import time
import datetime
import os
import xlrd
from xlutils.copy import copy
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/')
# w=xlwt.Workbook(encoding = 'utf-8',style_compression=2)
import collections

if os.path.exists(r'/01-300IsReady.txt'):
    os.path.remove(r'/01-300IsReady.txt')

m=0
cdn_list=['CDN_A','CDN_B','CDN_C','CDN_D','CDN_E','CDN_F','CDN_G','CDN_H']
# cdn_list=['CDN_A','CDN_B']
# errorCodeList=['01-300','5225','01-300','02-300']
# 如果要跑昨天的代码，改这里%%%%%%%%%%%%%%%%%%%%%%%%%
inter=4
today = datetime.date.today()
# 如果要跑昨天的代码，改这里%%%%%%%%%%%%%%%%%%%%%%%%%
interday = datetime.timedelta(days=32)
day_29_before = today - interday
#rowNumber以及新建excel,打开旧的excel放到第一重循环内
for area in cdn_list:
    print area[4:5]

    oldWb = xlrd.open_workbook(r'/tmp/ErrorReportPro/errorReport/report/CDN_'+area[4:5]+'/GH-CDN-'+area[4:5]+'-NGid01300.xls',formatting_info=True)
    print "0"
    w=xlwt.Workbook(encoding = 'utf-8',style_compression=2)
    w = copy(oldWb)
    print "2"
    # for sheet in w.sheets():


        # w._Workbook__worksheets = [ worksheet for worksheet in w._Workbook__worksheets if worksheet.name == sheet.name ]
    print "3"
    w._Workbook__worksheets = [ worksheet for worksheet in w._Workbook__worksheets if worksheet.name != str(day_29_before) ]
    sys.path.append('..')
    sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_'+area[4:5])
    mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
    # mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="123456",db="ire_gehua", charset='utf8')
    print 'connect'
    mysqlcursor = mysqlconn.cursor()
    # mysqlcursor.execute('''SELECT date from temp where DATE_ADD(date,INTERVAL "28 1:0:0" DAY_SECOND)>now() and cdn=%s and errorcode like %s order by date desc''',('CDN_A','01-300%'))
    # mysqlcursor.execute('''SELECT DISTINCT(date) from temp where date=DATE(DATE_SUB(NOW(), INTERVAL %s DAY))''',(inter))
    #
    # dateList=mysqlcursor.fetchall()
    # # dateList=list()
    rowNumber=1


    # for one_temp_date in dateList:
    #     if one_temp_date[0] is not None:
    #         print one_temp_date[0]
    m=1
    today = datetime.date.today()
    # 如果要跑昨天的代码，改这里%%%%%%%%%%%%%%%%%%%%%%%%%
    oneday = datetime.timedelta(days=4)
    yesterday = today - oneday
    print yesterday
    ws=w.add_sheet(str(yesterday).decode('utf-8'),cell_overwrite_ok=True)
    font = xlwt.Font() # Create the Font
    font.name = 'Times New Roman'
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
    ws.write(0,0,'日期'.decode('utf-8'),style)
    ws.write(0,1,'NGID'.decode('utf-8'),style)
    ws.write(0,2,'FREQ'.decode('utf-8'),style)
    ws.write(0,3,'错误数量'.decode('utf-8'),style)
    ws.write(0,4,'错误总和'.decode('utf-8'),style)
    ws.panes_frozen= True
    ws.horz_split_pos= 1
    # for area in cdn_list:
    #     print 'CDN_A'[4:5]

    mysqlcursor.execute('''select distinct(ngid) from temp where cdn=%s and errorcode = %s and date=DATE(DATE_SUB(NOW(), INTERVAL %s DAY))''',(area,'01-300',inter))
    dis_ngid=mysqlcursor.fetchall()
    rowNumber=1

    ngid_dic={}
    ngid_box=[]
    d=0


    for one_dis_ngid in dis_ngid:
        sum=0
        row_count=0
        sum_temp=0
        d+=1
        ####notice!!!one_dis_ngid[0]
        mysqlcursor.execute('''select DISTINCT count,freq,date from temp where cdn=%s and errorcode = %s and ngid=%s and date=DATE(DATE_SUB(NOW(), INTERVAL %s DAY)) ''',(area,'01-300',one_dis_ngid[0],inter))
        result=mysqlcursor.fetchall()
        count_temp=0
        sum_row=0
        for one_result in result:
            #####notice!!!one_count_result[0]
            sum=sum+int(one_result[0])

            count_temp+=1
            font = xlwt.Font() # Create the Font
            font.name = 'Times New Roman'
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
            # sum_row=sum_row+ngid_dic[oneresult[0]]
            ws.write(rowNumber,2,one_result[1],style)
            ws.write(rowNumber,3,one_result[0],style)
            ws.write(rowNumber,0,one_result[2],style)
            ws.write(rowNumber,1,one_dis_ngid[0],style)

            rowNumber+=1
            row_count+=1

        sum_temp=sum+sum_temp
        ws.write_merge(1+rowNumber-1-row_count,rowNumber-1,1,1,one_dis_ngid[0],style)
        ws.write_merge(1+rowNumber-1-row_count,rowNumber-1,4,4,sum_temp,style)





    if d!=0:
        ws.write_merge(1,rowNumber-1,0,0,str(one_result[2]).decode('utf-8'),style)
####should have one tab space below, or there will be error of out of range
    append_index = len(w._Workbook__worksheets)-1
    w.set_active_sheet(append_index)
    w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_'+area[4:5]+'/GH-CDN-'+area[4:5]+'-NGid01300.xls')
            # w.save(r'E:\GH-CDN-'+area[4:5]+'-NGid01-300new.xls')
            # w.save(r'E:\try'+area[4:5]+'.xls')
    if m==0:
        ws=w.add_sheet('sheet9',cell_overwrite_ok=True)
        font = xlwt.Font() # Create the Font
        font.name = 'Times New Roman'
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
        ws.write(0,0,'日期'.decode('utf-8'),style)
        ws.write(0,1,'NGID'.decode('utf-8'),style)
        ws.write(0,2,'FREQ'.decode('utf-8'),style)
        ws.write(0,3,'错误数量'.decode('utf-8'),style)
        ws.write(0,4,'错误总和'.decode('utf-8'),style)
        ws.panes_frozen= True
        ws.horz_split_pos= 1
        # w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_A/GH-CDN-A-NGid01-300.xls')
        append_index = len(w._Workbook__worksheets)-1
        w.set_active_sheet(append_index)
        w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_'+area[4:5]+'/GH-CDN-'+area[4:5]+'-NGid01300.xls')

f = open(r'/tmp/ErrorReportPro/errorReport/01-300IsReady.txt', 'w')
f.close()

mysqlconn.close()