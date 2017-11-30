#coding:UTF-8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import MySQLdb
import xlwt
import os
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/')
w=xlwt.Workbook(encoding = 'utf-8',style_compression=2)
import collections
w=xlwt.Workbook(encoding = 'utf-8',style_compression=2)
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_A')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
# mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="123456",db="ire_gehua", charset='utf8')
print 'connect'
mysqlcursor = mysqlconn.cursor()
# mysqlcursor.execute('''SELECT date from temp where DATE_ADD(date,INTERVAL "28 1:0:0" DAY_SECOND)>now() and cdn=%s and errorcode like %s order by date desc''',('CDN_A','5206%'))
mysqlcursor.execute('''SELECT DISTINCT(date) from temp ORDER BY date asc limit 0,21''')

dateList=mysqlcursor.fetchall()
# dateList=list()
m=0
# for one_date in dis_date:
#
#     date_whole=one_date[0]
#     date=str(one_date[0])[0:10]
#     if date not in dateList:
#         dateList.append(date)
for one_temp_date in dateList:
    if one_temp_date[0] is not None:
        print one_temp_date[0]
        m=1
        ws=w.add_sheet(str(one_temp_date[0]),cell_overwrite_ok=True)
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

        mysqlcursor.execute('''select distinct(ngid) from temp where cdn=%s and errorcode like %s and substring(date,1,10)=%s''',('CDN_A','5206%',one_temp_date[0]))
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
            mysqlcursor.execute('''select DISTINCT count,freq from temp where cdn=%s and errorcode like %s and ngid=%s and substring(date,1,10)=%s ''',('CDN_A','5206%',one_dis_ngid[0],one_temp_date[0]))
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
                ws.write(rowNumber,0,str(one_temp_date[0]),style)
                ws.write(rowNumber,1,one_dis_ngid[0],style)

                rowNumber+=1
                row_count+=1

            sum_temp=sum+sum_temp
            ws.write_merge(1+rowNumber-1-row_count,rowNumber-1,1,1,one_dis_ngid[0],style)
            ws.write_merge(1+rowNumber-1-row_count,rowNumber-1,4,4,sum_temp,style)





        if d!=0:
            ws.write_merge(1,rowNumber-1,0,0,str(one_temp_date[0]),style)
    ####should have one tab space below, or there will be error of out of range
        w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_A/GH-CDN-A-NGid5206.xls')
        # w.save(r'E:\GH-CDN-A-NGid5206.xls')
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
        ws.write(0,0,'日期',style)
        ws.write(0,1,'NGID',style)
        ws.write(0,2,'FREQ',style)
        ws.write(0,3,'错误数量',style)
        ws.write(0,4,'错误总和',style)
        ws.panes_frozen= True
        ws.horz_split_pos= 1
        w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_A/GH-CDN-A-NGid5206.xls')
        # w.save(r'E:\GH-CDN-A-NGid5206.xls')
mysqlconn.close()



# # # # ############################B!!!!!!!!!!!!!!!!!!!!###################
w=xlwt.Workbook(encoding = 'utf-8',style_compression=2)
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_B')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
# mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="123456",db="ire_gehua", charset='utf8')
print 'connect'
mysqlcursor = mysqlconn.cursor()
# mysqlcursor.execute('''SELECT date from temp where DATE_ADD(date,INTERVAL "28 1:0:0" DAY_SECOND)>now() and cdn=%s and errorcode like %s order by date desc''',('CDN_B','5206%'))
mysqlcursor.execute('''SELECT DISTINCT(date) from temp ORDER BY date asc limit 0,21''')

dateList=mysqlcursor.fetchall()
# dateList=list()
m=0
# for one_date in dis_date:
#
#     date_whole=one_date[0]
#     date=str(one_date[0])[0:10]
#     if date not in dateList:
#         dateList.append(date)
for one_temp_date in dateList:
    if one_temp_date[0] is not None:
        print one_temp_date[0]
        m=1
        ws=w.add_sheet(str(one_temp_date[0]),cell_overwrite_ok=True)
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

        mysqlcursor.execute('''select distinct(ngid) from temp where cdn=%s and errorcode like %s and substring(date,1,10)=%s''',('CDN_B','5206%',one_temp_date[0]))
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
            mysqlcursor.execute('''select DISTINCT count,freq from temp where cdn=%s and errorcode like %s and ngid=%s and substring(date,1,10)=%s''',('CDN_B','5206%',one_dis_ngid[0],one_temp_date[0]))
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
                ws.write(rowNumber,0,str(one_temp_date[0]),style)
                ws.write(rowNumber,1,one_dis_ngid[0],style)

                rowNumber+=1
                row_count+=1

            sum_temp=sum+sum_temp
            ws.write_merge(1+rowNumber-1-row_count,rowNumber-1,1,1,one_dis_ngid[0],style)
            ws.write_merge(1+rowNumber-1-row_count,rowNumber-1,4,4,sum_temp,style)





        if d!=0:
            ws.write_merge(1,rowNumber-1,0,0,str(one_temp_date[0]),style)
    ####should have one tab space below, or there will be error of out of range
        w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_B/GH-CDN-B-NGid5206.xls')
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
        ws.write(0,0,'日期',style)
        ws.write(0,1,'NGID',style)
        ws.write(0,2,'FREQ',style)
        ws.write(0,3,'错误数量',style)
        ws.write(0,4,'错误总和',style)
        ws.panes_frozen= True
        ws.horz_split_pos= 1
        w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_B/GH-CDN-B-NGid5206.xls')

mysqlconn.close()
#
# # #
# # #
# # #
# # # ####################################C!!!!!!!!!!!!!!!!!!!##########################
# # sys.path.append('..')
# # sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_C')
w=xlwt.Workbook(encoding = 'utf-8',style_compression=2)
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_C')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
# mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="123456",db="ire_gehua", charset='utf8')
print 'connect'
mysqlcursor = mysqlconn.cursor()
# mysqlcursor.execute('''SELECT date from temp where DATE_ADD(date,INTERVAL "28 1:0:0" DAY_SECOND)>now() and cdn=%s and errorcode like %s order by date desc''',('CDN_C','5206%'))
mysqlcursor.execute('''SELECT DISTINCT(date) from temp ORDER BY date asc limit 0,21''')

dateList=mysqlcursor.fetchall()
# dateList=list()
m=0
# for one_date in dis_date:
#
#     date_whole=one_date[0]
#     date=str(one_date[0])[0:10]
#     if date not in dateList:
#         dateList.append(date)
for one_temp_date in dateList:
    if one_temp_date[0] is not None:
        print one_temp_date[0]
        m=1
        ws=w.add_sheet(str(one_temp_date[0]),cell_overwrite_ok=True)
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

        mysqlcursor.execute('''select distinct(ngid) from temp where cdn=%s and errorcode like %s and substring(date,1,10)=%s''',('CDN_C','5206%',one_temp_date[0]))
        dis_ngid=mysqlcursor.fetchall()

        rowNumber=1
        ngid_dic={}
        ngid_box=[]
        d=0




        for one_dis_ngid in dis_ngid:
            d+=1
            sum=0
            row_count=0
            sum_temp=0
            ####notice!!!one_dis_ngid[0]
            #####temp有可能重复插入，所以limit 1
            mysqlcursor.execute('''select DISTINCT count,freq from temp where cdn=%s and errorcode like %s and ngid=%s and substring(date,1,10)=%s''',('CDN_C','5206%',one_dis_ngid[0],one_temp_date[0]))
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
                ws.write(rowNumber,0,str(one_temp_date[0]),style)
                ws.write(rowNumber,1,one_dis_ngid[0],style)

                rowNumber+=1
                row_count+=1

            sum_temp=sum+sum_temp
            ws.write_merge(1+rowNumber-1-row_count,rowNumber-1,1,1,one_dis_ngid[0],style)
            ws.write_merge(1+rowNumber-1-row_count,rowNumber-1,4,4,sum_temp,style)





        if d!=0:
            ws.write_merge(1,rowNumber-1,0,0,str(one_temp_date[0]),style)
    ####should have one tab space below, or there will be error of out of range
        w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_C/GH-CDN-C-NGid5206.xls')
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
        ws.write(0,0,'日期',style)
        ws.write(0,1,'NGID',style)
        ws.write(0,2,'FREQ',style)
        ws.write(0,3,'错误数量',style)
        ws.write(0,4,'错误总和',style)
        ws.panes_frozen= True
        ws.horz_split_pos= 1
        w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_C/GH-CDN-C-NGid5206.xls')

mysqlconn.close()
#
# # #
# # #
# # # #####################D!!!!!!!!!!!!!!!!!!##########################
# # sys.path.append('..')
# # sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_D')
w=xlwt.Workbook(encoding = 'utf-8',style_compression=2)
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_D')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
# mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="123456",db="ire_gehua", charset='utf8')
print 'connect'
mysqlcursor = mysqlconn.cursor()
# mysqlcursor.execute('''SELECT date from temp where DATE_ADD(date,INTERVAL "28 1:0:0" DAY_SECOND)>now() and cdn=%s and errorcode like %s order by date desc''',('CDN_D','5206%'))
mysqlcursor.execute('''SELECT DISTINCT(date) from temp ORDER BY date asc limit 0,21''')

dateList=mysqlcursor.fetchall()
# dateList=list()
m=0
# for one_date in dis_date:
#
#     date_whole=one_date[0]
#     date=str(one_date[0])[0:10]
#     if date not in dateList:
#         dateList.append(date)
for one_temp_date in dateList:
    if one_temp_date[0] is not None:
        print one_temp_date[0]
        m=1
        ws=w.add_sheet(str(one_temp_date[0]),cell_overwrite_ok=True)
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

        mysqlcursor.execute('''select distinct(ngid) from temp where cdn=%s and errorcode like %s and substring(date,1,10)=%s''',('CDN_D','5206%',one_temp_date[0]))
        dis_ngid=mysqlcursor.fetchall()

        rowNumber=1
        ngid_dic={}
        ngid_box=[]
        d=0




        for one_dis_ngid in dis_ngid:
            d+=1
            sum=0
            row_count=0
            sum_temp=0
            ####notice!!!one_dis_ngid[0]
            mysqlcursor.execute('''select DISTINCT count,freq from temp where cdn=%s and errorcode like %s and ngid=%s and substring(date,1,10)=%s ''',('CDN_D','5206%',one_dis_ngid[0],one_temp_date[0]))
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
                ws.write(rowNumber,0,str(one_temp_date[0]),style)
                ws.write(rowNumber,1,one_dis_ngid[0],style)

                rowNumber+=1
                row_count+=1

            sum_temp=sum+sum_temp
            ws.write_merge(1+rowNumber-1-row_count,rowNumber-1,1,1,one_dis_ngid[0],style)
            ws.write_merge(1+rowNumber-1-row_count,rowNumber-1,4,4,sum_temp,style)





        if d!=0:
            ws.write_merge(1,rowNumber-1,0,0,str(one_temp_date[0]),style)
    ####should have one tab space below, or there will be error of out of range
        w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_D/GH-CDN-D-NGid5206.xls')
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
        ws.write(0,0,'日期',style)
        ws.write(0,1,'NGID',style)
        ws.write(0,2,'FREQ',style)
        ws.write(0,3,'错误数量',style)
        ws.write(0,4,'错误总和',style)
        ws.panes_frozen= True
        ws.horz_split_pos= 1
        w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_D/GH-CDN-D-NGid5206.xls')

mysqlconn.close()
#
# # #
# # #
# # #
# # # ############################E!!!!!!!!!!!!!!!!!!!!!!!!!############################
# # sys.path.append('..')
# # sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_E')
w=xlwt.Workbook(encoding = 'utf-8',style_compression=2)
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_E')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
# mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="123456",db="ire_gehua", charset='utf8')
print 'connect'
mysqlcursor = mysqlconn.cursor()
# mysqlcursor.execute('''SELECT date from temp where DATE_ADD(date,INTERVAL "28 1:0:0" DAY_SECOND)>now() and cdn=%s and errorcode like %s order by date desc''',('CDN_E','5206%'))
mysqlcursor.execute('''SELECT DISTINCT(date) from temp ORDER BY date asc limit 0,21''')

dateList=mysqlcursor.fetchall()
# dateList=list()
m=0
# for one_date in dis_date:
#
#     date_whole=one_date[0]
#     date=str(one_date[0])[0:10]
#     if date not in dateList:
#         dateList.append(date)
for one_temp_date in dateList:
    if one_temp_date[0] is not None:
        print one_temp_date[0]
        m=1
        ws=w.add_sheet(str(one_temp_date[0]),cell_overwrite_ok=True)
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

        mysqlcursor.execute('''select distinct(ngid) from temp where cdn=%s and errorcode like %s and substring(date,1,10)=%s''',('CDN_E','5206%',one_temp_date[0]))
        dis_ngid=mysqlcursor.fetchall()

        rowNumber=1
        ngid_dic={}
        ngid_box=[]
        d=0




        for one_dis_ngid in dis_ngid:
            d+=1
            sum=0
            row_count=0
            sum_temp=0
            ####notice!!!one_dis_ngid[0]
            mysqlcursor.execute('''select DISTINCT count,freq from temp where cdn=%s and errorcode like %s and ngid=%s and substring(date,1,10)=%s''',('CDN_E','5206%',one_dis_ngid[0],one_temp_date[0]))
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
                ws.write(rowNumber,0,str(one_temp_date[0]),style)
                ws.write(rowNumber,1,one_dis_ngid[0],style)

                rowNumber+=1
                row_count+=1

            sum_temp=sum+sum_temp
            ws.write_merge(1+rowNumber-1-row_count,rowNumber-1,1,1,one_dis_ngid[0],style)
            ws.write_merge(1+rowNumber-1-row_count,rowNumber-1,4,4,sum_temp,style)





        if d!=0:

            ws.write_merge(1,rowNumber-1,0,0,str(one_temp_date[0]),style)
    ####should have one tab space below, or there will be error of out of range
        w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_E/GH-CDN-E-NGid5206.xls')
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
        ws.write(0,0,'日期',style)
        ws.write(0,1,'NGID',style)
        ws.write(0,2,'FREQ',style)
        ws.write(0,3,'错误数量',style)
        ws.write(0,4,'错误总和',style)
        ws.panes_frozen= True
        ws.horz_split_pos= 1
        w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_E/GH-CDN-E-NGid5206.xls')

mysqlconn.close()
#
#
# # #
# # # ##########################F!!!!!!!!!!!!!!!!!!!##############################
# # sys.path.append('..')
# # sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_F')
w=xlwt.Workbook(encoding = 'utf-8',style_compression=2)
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_F')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
# mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="123456",db="ire_gehua", charset='utf8')
print 'connect'
mysqlcursor = mysqlconn.cursor()
# mysqlcursor.execute('''SELECT date from temp where DATE_ADD(date,INTERVAL "28 1:0:0" DAY_SECOND)>now() and cdn=%s and errorcode like %s order by date desc''',('CDN_F','5206%'))
mysqlcursor.execute('''SELECT DISTINCT(date) from temp ORDER BY date asc limit 0,21''')

dateList=mysqlcursor.fetchall()
# dateList=list()
m=0
# for one_date in dis_date:
#
#     date_whole=one_date[0]
#     date=str(one_date[0])[0:10]
#     if date not in dateList:
#         dateList.append(date)
for one_temp_date in dateList:
    if one_temp_date[0] is not None:
        print one_temp_date[0]
        m=1
        ws=w.add_sheet(str(one_temp_date[0]),cell_overwrite_ok=True)
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

        mysqlcursor.execute('''select distinct(ngid) from temp where cdn=%s and errorcode like %s and substring(date,1,10)=%s''',('CDN_F','5206%',one_temp_date[0]))
        dis_ngid=mysqlcursor.fetchall()

        rowNumber=1
        ngid_dic={}
        ngid_box=[]
        d=0




        for one_dis_ngid in dis_ngid:
            d+=1
            sum=0
            row_count=0
            sum_temp=0
            ####notice!!!one_dis_ngid[0]
            mysqlcursor.execute('''select DISTINCT count,freq from temp where cdn=%s and errorcode like %s and ngid=%s and substring(date,1,10)=%s''',('CDN_F','5206%',one_dis_ngid[0],one_temp_date[0]))
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
                ws.write(rowNumber,0,str(one_temp_date[0]),style)
                ws.write(rowNumber,1,one_dis_ngid[0],style)

                rowNumber+=1
                row_count+=1

            sum_temp=sum+sum_temp
            ws.write_merge(1+rowNumber-1-row_count,rowNumber-1,1,1,one_dis_ngid[0],style)
            ws.write_merge(1+rowNumber-1-row_count,rowNumber-1,4,4,sum_temp,style)





        if d!=0:
            ws.write_merge(1,rowNumber-1,0,0,str(one_temp_date[0]),style)
    ####should have one tab space below, or there will be error of out of range
        w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_F/GH-CDN-F-NGid5206.xls')
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
        ws.write(0,0,'日期',style)
        ws.write(0,1,'NGID',style)
        ws.write(0,2,'FREQ',style)
        ws.write(0,3,'错误数量',style)
        ws.write(0,4,'错误总和',style)
        ws.panes_frozen= True
        ws.horz_split_pos= 1
        w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_F/GH-CDN-F-NGid5206.xls')

mysqlconn.close()
#
# # #
# # #
# # # ###########################G!!!!!!!!!!!!!!!!!!!!!######################
# # sys.path.append('..')
# # sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_G')
w=xlwt.Workbook(encoding = 'utf-8',style_compression=2)
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_G')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
# mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="123456",db="ire_gehua", charset='utf8')
print 'connect'
mysqlcursor = mysqlconn.cursor()
# mysqlcursor.execute('''SELECT date from temp where DATE_ADD(date,INTERVAL "28 1:0:0" DAY_SECOND)>now() and cdn=%s and errorcode like %s order by date desc''',('CDN_G','5206%'))
mysqlcursor.execute('''SELECT DISTINCT(date) from temp ORDER BY date asc limit 0,21''')

dateList=mysqlcursor.fetchall()
# dateList=list()
m=0
# for one_date in dis_date:
#
#     date_whole=one_date[0]
#     date=str(one_date[0])[0:10]
#     if date not in dateList:
#         dateList.append(date)
for one_temp_date in dateList:
    if one_temp_date[0] is not None:
        print one_temp_date[0]
        m=1
        ws=w.add_sheet(str(one_temp_date[0]),cell_overwrite_ok=True)
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

        mysqlcursor.execute('''select distinct(ngid) from temp where cdn=%s and errorcode like %s and substring(date,1,10)=%s''',('CDN_G','5206%',one_temp_date[0]))
        dis_ngid=mysqlcursor.fetchall()

        rowNumber=1
        ngid_dic={}
        ngid_box=[]
        d=0




        for one_dis_ngid in dis_ngid:
            d+=1
            sum=0
            row_count=0
            sum_temp=0
            ####notice!!!one_dis_ngid[0]
            mysqlcursor.execute('''select DISTINCT count,freq from temp where cdn=%s and errorcode like %s and ngid=%s and substring(date,1,10)=%s''',('CDN_G','5206%',one_dis_ngid[0],one_temp_date[0]))
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
                ws.write(rowNumber,0,str(one_temp_date[0]),style)
                ws.write(rowNumber,1,one_dis_ngid[0],style)

                rowNumber+=1
                row_count+=1

            sum_temp=sum+sum_temp
            ws.write_merge(1+rowNumber-1-row_count,rowNumber-1,1,1,one_dis_ngid[0],style)
            ws.write_merge(1+rowNumber-1-row_count,rowNumber-1,4,4,sum_temp,style)





        if d!=0:
            ws.write_merge(1,rowNumber-1,0,0,str(one_temp_date[0]),style)
    ####should have one tab space below, or there will be error of out of range
        w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_G/GH-CDN-G-NGid5206.xls')
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
        ws.write(0,0,'日期',style)
        ws.write(0,1,'NGID',style)
        ws.write(0,2,'FREQ',style)
        ws.write(0,3,'错误数量',style)
        ws.write(0,4,'错误总和',style)
        ws.panes_frozen= True
        ws.horz_split_pos= 1
        w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_G/GH-CDN-G-NGid5206.xls')

mysqlconn.close()
#
# # #
# # #
# # # ##########################H!!!!!!!!!!!!!!!!!!!##############################
w=xlwt.Workbook(encoding = 'utf-8',style_compression=2)
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_H')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
# mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="123456",db="ire_gehua", charset='utf8')
print 'connect'
mysqlcursor = mysqlconn.cursor()
# mysqlcursor.execute('''SELECT date from temp where DATE_ADD(date,INTERVAL "28 1:0:0" DAY_SECOND)>now() and cdn=%s and errorcode like %s order by date desc''',('CDN_H','5206%'))
mysqlcursor.execute('''SELECT DISTINCT(date) from temp ORDER BY date asc limit 0,21''')

dateList=mysqlcursor.fetchall()
# dateList=list()
m=0
# for one_date in dis_date:
#
#     date_whole=one_date[0]
#     date=str(one_date[0])[0:10]
#     if date not in dateList:
#         dateList.append(date)
for one_temp_date in dateList:
    if one_temp_date[0] is not None:
        print one_temp_date[0]
        m=1
        ws=w.add_sheet(str(one_temp_date[0]),cell_overwrite_ok=True)
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

        mysqlcursor.execute('''select distinct(ngid) from temp where cdn=%s and errorcode like %s and substring(date,1,10)=%s''',('CDN_H','5206%',one_temp_date[0]))
        dis_ngid=mysqlcursor.fetchall()

        rowNumber=1
        ngid_dic={}
        ngid_box=[]
        d=0




        for one_dis_ngid in dis_ngid:
            d+=1
            sum=0
            row_count=0
            sum_temp=0
            ####notice!!!one_dis_ngid[0]
            mysqlcursor.execute('''select DISTINCT count,freq from temp where cdn=%s and errorcode like %s and ngid=%s and substring(date,1,10)=%s ''',('CDN_H','5206%',one_dis_ngid[0],one_temp_date[0]))
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
                ws.write(rowNumber,0,str(one_temp_date[0]),style)
                ws.write(rowNumber,1,one_dis_ngid[0],style)

                rowNumber+=1
                row_count+=1

            sum_temp=sum+sum_temp
            ws.write_merge(1+rowNumber-1-row_count,rowNumber-1,1,1,one_dis_ngid[0],style)
            ws.write_merge(1+rowNumber-1-row_count,rowNumber-1,4,4,sum_temp,style)





        if d!=0:
            ws.write_merge(1,rowNumber-1,0,0,str(one_temp_date[0]),style)
    ####should have one tab space below, or there will be error of out of range
        w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_H/GH-CDN-H-NGid5206.xls')
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
        ws.write(0,0,'日期',style)
        ws.write(0,1,'NGID',style)
        ws.write(0,2,'FREQ',style)
        ws.write(0,3,'错误数量',style)
        ws.write(0,4,'错误总和',style)
        ws.panes_frozen= True
        ws.horz_split_pos= 1
        w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_H/GH-CDN-H-NGid5206.xls')

#
#
# #
# #
if os.path.exists(r'/tmp/ErrorReportPro/errorReport/5206IsReady.txt'):
    os.remove(r'/tmp/ErrorReportPro/errorReport/5206IsReady.txt')
f = open(r'/tmp/ErrorReportPro/errorReport/5206IsReady.txt', 'w')
f.close()
mysqlconn.close()