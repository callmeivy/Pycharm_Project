# coding=UTF-8
#earlier version
import pymongo
conn=pymongo.Connection('172.16.168.45',27017)
iae_hitlog_new=conn.gehua.iae_hitlog_new


caid1=iae_hitlog_new.distinct('previous.parameter.CAID')
caid3=iae_hitlog_new.distinct('resource.parameter.CAID')

caid2=iae_hitlog_new.distinct('previous.parameter.smid')
#print caid2
for onecaid2 in caid2:
    if onecaid2 not in caid1:
        caid1.append(onecaid2)

for onecaid3 in caid3:
    if onecaid3 not in caid1:
        caid1.append(onecaid3)

print len(caid1)

conn.close()