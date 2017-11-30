#coding:UTF-8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import MySQLdb
import xlwt
import os
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/')
import collections
w=xlwt.Workbook(encoding = 'utf-8',style_compression=2)
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_A')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
print 'connect'

mysqlcursor = mysqlconn.cursor()

mysqlcursor.execute('''SELECT DISTINCT(date) from temp ORDER BY date desc limit 1,28''')

dateList=mysqlcursor.fetchall()
m=0
for one_temp_date in dateList:
    if one_temp_date[0] is not None:
        print one_temp_date[0]
m=1
ws=w.add_sheet('date',cell_overwrite_ok=True)
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
ws.write(0,0,'日期',style)
ws.write(0,1,'NGID',style)
ws.write(0,2,'FREQ',style)
ws.write(0,3,'错误数量',style)
ws.write(0,4,'错误总和',style)
ws.panes_frozen= True
ws.horz_split_pos= 1
sum=0

mysqlcursor.execute('select date, ngid, freq, count, sum_count from gh_ngid where cdn=%s and errorcode = %s and date=DATE(DATE_SUB(NOW(), INTERVAL %s DAY))',('CDN_A','5206','1'))
all_result=mysqlcursor.fetchall()
rowNumber=1
for one_all_result in all_result:
    ws.write(rowNumber,0,str(one_all_result[0]),style)
    ws.write(rowNumber,1,one_all_result[1],style)
    ws.write(rowNumber,2,one_all_result[2],style)
    ws.write(rowNumber,3,one_all_result[3],style)
    ws.write(rowNumber,4,one_all_result[4],style)
    rowNumber+=1
    mysqlcursor.execute('select distinct count(freq),ngid from gh_ngid where cdn=%s and errorcode = %s and date=DATE(DATE_SUB(NOW(), INTERVAL %s DAY)) and ngid= %s',('CDN_A','5206','1',one_all_result[1]))
    count_freq=mysqlcursor.fetchone()
    print count_freq[0],'ngid',count_freq[1],count_freq[2]
    # ws.write_merge(rowNumber-sum,rowNumber-1,1,1,one_all_result[1],style)
    # ws.write_merge(rowNumber-sum,rowNumber-1,4,4,one_all_result[4],style)



w.save(r'E:\5206.xls')