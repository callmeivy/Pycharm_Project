#coding: UTF-8
'''
by Ivy
created on 20 Jan,2016
bless audience flow
CCTV1晚间新闻期间（22:00-22:30）
'''
import os
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import jieba
from collections import Counter
import csv
import datetime

now =  datetime.datetime.now()
print now

root_directory = 'C:\Users\Ivy\Desktop\\bless\\rating_20151231\data'
# root_directory = 'C:\Users\Ivy\Desktop\\bless\\1'
os.chdir(root_directory)
cmd = "dir /A-D /B"
# list_file指向该文件夹下的所有文件名称
list_file = os.popen(cmd).readlines()
print list_file


cctv1_audience = list()
cctv3_audience = list()
firstPrize = list()
cctv1_audience_total = list()
for item in list_file:
    single_file_path = root_directory+'\\'+str(item.strip())
    # print single_file_path
    with open(single_file_path) as rating_file:
        reading_file_line = rating_file.readlines()
        for line in reading_file_line:
            line = line.split(',')
            # print 'wwwwwwww',type(line),line
            cardNum = line[0].split(":")[1]
            date = line[2].split(":")[1][1:11]
            # date = datetime.datetime.strptime(date, "%Y-%m-%d")
            # 日期筛选
            if date == '2015-12-31' or date == '2016-01-01':
            # if date == '2015-12-30':
                start = line[4][5:13]

                # if start > datetime.datetime.strptime('18:50:00', "%H:%M:%S"):
                #     print 'yyyyyy'
                start_hour = start[0:2]
                start_min = start[3:5]
                start_sec = start[6:8]
                start = datetime.datetime.strptime(start, "%H:%M:%S")
                end = line[3][5:13]
                end_hour = end[0:2]
                end_min = end[3:5]
                end_sec = end[6:8]
                end = datetime.datetime.strptime(end, "%H:%M:%S")
                channel = line[5].split(":")[1][1:4]
                program = line[8].split(":")[1]
                length = int(end_hour)*3600+int(end_min)*60+int(end_sec)-int(start_hour)*3600-int(start_min)*60-int(start_sec)
                # print "cardNum",cardNum,"start",start,"end",end,"channel",channel

            # *************************************************************************************
                # 22:00-22:30为CCTV1晚间新闻，21:55还在CCTV1,22:00-22:30之间转到其他台如CCTV3的人数,cctv1_audience存储endtime在21:55且停留在cctv1的观众,并且停留时间在10秒以上
                if (channel == '121' or channel == '601'):
                        if program[1:23] == '%E5%90%AF%E8%88%AA2016':
                            if (datetime.datetime.strptime('21:55:00', "%H:%M:%S")>= start) and (datetime.datetime.strptime('21:55:00', "%H:%M:%S")<= end):
                                if int(length)>10:
                        # print 'sssss',line
                        # print "ffff",length
                                    if cardNum not in cctv1_audience:
                                        cctv1_audience.append(cardNum)

                if (channel == '121' or channel == '601'):
                        if program[1:23] == '%E5%90%AF%E8%88%AA2016':
                            if int(length)>10:
                                if cardNum not in cctv1_audience_total:
                                    cctv1_audience_total.append(cardNum)


# 1641
print 'cctv1_audience人数',len(cctv1_audience)
print '看过跨年的总人数',len(cctv1_audience_total)
# overlapping = list(set(cctv1_audience).intersection(set(cctv3_audience)))
# print len(overlapping)
# for o in firstPrize:
#     print 'test',o

# *************************************************************************************
# 看看晚间新闻开始后，那些原本在看CCTV1的人都干嘛了
evening_news = list()
evening_news_caid = list()
close = list()
for item in list_file:
    single_file_path = root_directory+'\\'+str(item.strip())
    # print single_file_path
    with open(single_file_path) as rating_file:
        reading_file_line = rating_file.readlines()
        for line in reading_file_line:
            line = line.split(',')
            # print 'wwwwwwww',type(line),line
            cardNum = line[0].split(":")[1]
            date = line[2].split(":")[1][1:11]
            # [1:4]
            channel = line[5].split(":")[1][1:4]
            # 日期筛选
            if date == '2015-12-31' or date == '2016-01-01':
                start = line[4][5:13]
                start_hour = start[0:2]
                start_min = start[3:5]
                start_sec = start[6:8]
                start = datetime.datetime.strptime(start, "%H:%M:%S")
                end = line[3][5:13]
                end_hour = end[0:2]
                end_min = end[3:5]
                end_sec = end[6:8]
                end = datetime.datetime.strptime(end, "%H:%M:%S")
                length = int(end_hour)*3600+int(end_min)*60+int(end_sec)-int(start_hour)*3600-int(start_min)*60-int(start_sec)
                if cardNum in cctv1_audience:
                    # print '0000',cardNum
                    if (datetime.datetime.strptime('22:10:00', "%H:%M:%S")>= start) and (datetime.datetime.strptime('22:10:00', "%H:%M:%S")<= end) and (length >10) :
                        # print line,length
                        evening_news.append(channel)
                        evening_news_caid.append(cardNum)

cctv1_audience_listcount = dict(Counter(evening_news))
cctv1_audience_listcount = sorted(cctv1_audience_listcount.iteritems(), key=lambda e:e[1], reverse=True)
for item in cctv1_audience_listcount:
    change_channel = item[0]
    how_many = item[1]
    print change_channel,how_many


close = list(set(cctv1_audience).difference(set(evening_news_caid)))
print "可能已经关机的人数：",len(close)