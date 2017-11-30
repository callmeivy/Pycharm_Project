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


oldWb = xlrd.open_workbook(r'E:\GH-CDN-E-NGid5206new.xls',formatting_info=True)
w = copy(oldWb)
append_index = len(w._Workbook__worksheets)-3
w.set_active_sheet(append_index)

w.save(r'E:\GH-CDN-E-NGid5206newnew.xls')

