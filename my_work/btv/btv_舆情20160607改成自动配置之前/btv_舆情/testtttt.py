#coding:UTF-8
import time
now = int(time.time())-86400*0
timeArray = time.localtime(now)
otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
print otherStyleTime


transform_date_time = '2016-05-28'

if otherStyleTime > transform_date_time:
    print 1111