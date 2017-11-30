#coding:UTF-8
# http://blog.sina.com.cn/s/blog_6c07f2b601013wr9.html
import numpy as np
import matplotlib.pyplot as plt
import xlrd
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb
import time
import xlwt
import os
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/')
sys.path.append('/tmp/ErrorReportPro/errorReport/report')
sys.path.append('/tmp/ErrorReportPro/errorReport/new_repo')
sys.path.append('/tmp/ErrorReportPro/errorReport/report/CDN_A')
w=xlwt.Workbook(encoding = 'utf-8',style_compression=2)
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
print "connected"
mysqlcursor = mysqlconn.cursor()

if os.path.exists(r'/tmp/ErrorReportPro/errorReport/GraphIsReady.txt'):
    os.remove(r'/tmp/ErrorReportPro/errorReport/GraphIsReady.txt')

#generate the new excel
# # ############################Insert into Excel!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!######################################
# rowNumber以及新建excel放到第一重循环内
cdn_list=['CDN_A','CDN_B','CDN_C','CDN_D','CDN_E','CDN_F','CDN_G','CDN_H']

# cdn_list=['CDN_H']
# mu=0
for one_cdn_list in cdn_list:
    rowNumber=1
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
    ws.write(0,0,'Date',style)
    ws.write(0,1,'01_000',style)
    ws.write(0,2,'01_300',style)
    ws.write(0,3,'02_200',style)
    ws.write(0,4,'02_300',style)
    ws.write(0,5,'5203',style)
    ws.write(0,6,'5206',style)
    ws.write(0,7,'5225',style)
    ws.write(0,8,'TotalCount',style)
    ws.panes_frozen= True
    ws.horz_split_pos= 1
    print one_cdn_list[4:5]
    # se=0
    for num in range(1,29):
        # if mu==2:
        #     ir=0
        mysqlcursor.execute('''select right(date,5), left(01_000_r,4), left(01_300_r,4), left(02_200_r,4), left(02_300_r,4), left(5203_r,4), left(5206_r,4), left(5225_r,4), TotalCount from errorsum where date=DATE(DATE_SUB(NOW(), INTERVAL %s DAY)) and cdn=%s and period='错误总数' ORDER BY date asc''',(num,one_cdn_list))
        result=mysqlcursor.fetchall()
        y=0
        for one_result in result:
            print one_result
            one_result_new = list()
            for item in one_result:
                if item is None:
                    print 'hahahahahaha'
                    item = '0'
                    print item
                    one_result_new.append(item)
                one_result_new.append(item)
                    # item = '0'
            # if 'None' in one_result:
            #     print '111111'
            #     for item in one_result:
            #         if item == 'None':
            #             item = '0'

        #     y=1
            for column in range(0,9):
                ws.write(rowNumber,column,str(one_result_new[column]),style)
        #
        #
            rowNumber+=1
    # w.save(r'E:\ErrorRateTrend-GH-CDN-'+one_cdn_list[4:5]+'.xls')
    w.save(r'/tmp/ErrorReportPro/errorReport/new_repo/ErrorRateTrend-GH-CDN-'+one_cdn_list[4:5]+'.xls')
mysqlconn.close()






#open excel file and get sheet
cdn_list=['CDN_A','CDN_B','CDN_C','CDN_D','CDN_E','CDN_F','CDN_G','CDN_H']
# cdn_list=['CDN_A']
for one_cdn_list in cdn_list:
    tps1=list()
    tps2=list()
    tps3=list()
    tps4=list()
    tps5=list()
    tps6=list()
    tps7=list()
    print one_cdn_list[4:5]
    myBook = xlrd.open_workbook(r'/tmp/ErrorReportPro/errorReport/new_repo/ErrorRateTrend-GH-CDN-'+one_cdn_list[4:5]+'.xls')
    mySheet = myBook.sheet_by_index(0)

    #get datas
    date = mySheet.col(0)
    date = [x.value for x in date]
    plt.ylabel('ErrorRate(%)')
    plt.xlabel('date')
    plt.title('Error Rate Trend Diagram')

    # print date
    # for i in range(1,9):
    tps1 = mySheet.col_values(1)
    tps2 = mySheet.col_values(2)
    tps3 = mySheet.col_values(3)
    tps4 = mySheet.col_values(4)
    tps5 = mySheet.col_values(5)
    tps6 = mySheet.col_values(6)
    tps7 = mySheet.col_values(7)
    # print tps7
    # a=('01-000','01-300','02-200','02-300','5203','5206','5225')
    # m=i-1
    # n=i
    # b=a[m:n]
    #drop the 1st line of the data, which is the name of the data.
    date.pop(0)
    tps1.pop(0)
    tps2.pop(0)
    tps3.pop(0)
    tps4.pop(0)
    tps5.pop(0)
    tps6.pop(0)
    tps7.pop(0)
    #declare a figure object to plot
    fig = plt.figure(1)

    #plot tps
    l1=plt.plot(tps1,label='01-000',color="red",linewidth=1)
    l2=plt.plot(tps2,label='01-300',color="brown",linewidth=2)
    l3=plt.plot(tps3,label='02-200',color="green",linewidth=1)
    l4=plt.plot(tps4,label='02-300',color="black",linewidth=2)
    l5=plt.plot(tps5,label='5203',color="orange",linewidth=1)
    l6=plt.plot(tps6,label='5206',color="purple",linewidth=2)
    l7=plt.plot(tps7,label='5225',color="blue",linewidth=2)
    plt.legend()

    plt.xticks(range(len(date)),date)

    #advance settings

    figure = plt.gcf()
    figure.set_size_inches(20, 12)
    #show the figure
    # plt.show()
    # w.save(r'/tmp/ErrorReportPro/errorReport/report/CDN_'+one_cdn_list[4:5]+'/ErrorSum(Hour)-GH-CDN-'+one_cdn_list[4:5]+'.xls')
    plt.savefig(r'/tmp/ErrorReportPro/errorReport/report/CDN_'+one_cdn_list[4:5]+'/ErrorRateTrend-GH-CDN-'+one_cdn_list[4:5]+'.png',dpi=100)
    l1.pop(0).remove()
    l2.pop(0).remove()
    l3.pop(0).remove()
    l4.pop(0).remove()
    l5.pop(0).remove()
    l6.pop(0).remove()
    l7.pop(0).remove()

for one_cdn_list in cdn_list:
    print one_cdn_list[4:5]
    myBook = xlrd.open_workbook(r'/tmp/ErrorReportPro/errorReport/new_repo/ErrorRateTrend-GH-CDN-'+one_cdn_list[4:5]+'.xls')
    mySheet = myBook.sheet_by_index(0)

    #get datas
    date = mySheet.col(0)
    date = [x.value for x in date]
    plt.ylabel('vod capacity')
    plt.xlabel('date')
    plt.title('VOD Capacity Trend Diagram')

    # print date
    # for i in range(1,9):
    tps8 = mySheet.col_values(8)
    # print tps7
    # a=('01-000','01-300','02-200','02-300','5203','5206','5225')
    # m=i-1
    # n=i
    # b=a[m:n]
    #drop the 1st line of the data, which is the name of the data.
    date.pop(0)
    tps8.pop(0)
    #declare a figure object to plot
    fig = plt.figure(1)

    #plot tps
    l8=plt.plot(tps8,label='VODCapacity',color="red",linewidth=1)
    plt.legend()

    plt.xticks(range(len(date)),date)

    #advance settings

    figure = plt.gcf()
    figure.set_size_inches(20, 12)
    #show the figure
    # plt.show()
    plt.savefig(r'/tmp/ErrorReportPro/errorReport/report/CDN_'+one_cdn_list[4:5]+'/VODCapacity-GH-CDN-'+one_cdn_list[4:5]+'.png',dpi=50)
    l8.pop(0).remove()





f = open(r'/tmp/ErrorReportPro/errorReport/GraphIsReady.txt', 'w')
f.close()