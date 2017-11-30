#coding: UTF-8
'''
by Ivy
created on 20 Jan,2016
bless audience flow
四次摇奖后的观众流向分析
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

            #
            # *************************************************************************************
                # 在第一次摇奖瞬间看CCTV1的人存到firstPrize，第一次摇奖是20:40:29
                if program[1:23] == '%E5%90%AF%E8%88%AA2016':
                    if (channel == '121' or channel == '601'):
                        if (datetime.datetime.strptime('20:40:29', "%H:%M:%S")>= start) and (datetime.datetime.strptime('20:40:29', "%H:%M:%S")<= end):
                            if int(length)>10:
                                # print 'wwwwwww',line
                                if cardNum not in firstPrize:
                                    firstPrize.append(cardNum)
            # *************************************************************************************
print 'firstPrize人数',len(firstPrize)

# *************************************************************************************
# 看看第一次摇奖过后，那些原本在看CCTV1的人，在5分钟之后都干嘛了
firstPrize5MinAfter = list()
firstPrize5MinAfter_caid = list()
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
                if cardNum in firstPrize:
                    # print '0000',cardNum
                    # 记录区间包含20:45:29时间节点
                    if (datetime.datetime.strptime('20:50:29', "%H:%M:%S")>= start) and (datetime.datetime.strptime('20:50:29', "%H:%M:%S")<= end) and (length >10) :
                        # print line,length
                        firstPrize5MinAfter.append(channel)
                        # firstPrize5MinAfter_caid这是第一次摇奖后还再活动的caid
                        firstPrize5MinAfter_caid.append(cardNum)

firstPrize5MinAfter_listcount = dict(Counter(firstPrize5MinAfter))
firstPrize5MinAfter_listcount = sorted(firstPrize5MinAfter_listcount.iteritems(), key=lambda e:e[1], reverse=True)
for item in firstPrize5MinAfter_listcount:
    change_channel = item[0]
    how_many = item[1]
    print change_channel,how_many


close = list(set(firstPrize).difference(set(firstPrize5MinAfter_caid)))
print "可能已经关机的人数：",len(close)