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
mysqlcursor.execute('''Delete from errorsum where date = '2015-02-12';''')
mysqlconn.commit()

mysqlconn.close()