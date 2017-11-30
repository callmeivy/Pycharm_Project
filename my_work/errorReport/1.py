import time
now = int(time.time())-86400
print now
timeArray = time.localtime(now)
print timeArray
otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
print otherStyleTime