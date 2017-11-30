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
from xlutils import copy
from xlutils.copy import copy
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/')


oldWb = xlrd.open_workbook(r'E:\GH-CDN-A-NGid5206.xls',formatting_info=True)
for sheet in oldWb.sheets():
    w = copy(oldWb)
    # w._Workbook__worksheets = [ worksheet for worksheet in w._Workbook__worksheets if worksheet.name == sheet.name ]
    w._Workbook__worksheets = [ worksheet for worksheet in w._Workbook__worksheets if worksheet.name != '2014-10-15' ]

w.save('{}_workbook.xls'.format(sheet.name))
# oldWb.save(r'E:\GH-CDN-A-NGid5206newnew.xls')
