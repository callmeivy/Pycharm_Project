# coding=UTF-8
import pymongo
conn=pymongo.Connection('172.16.168.45',27017)
print "connected"
iae_hitlog_new=conn.gehua.iae_hitlog_new
print 0
caid1=iae_hitlog_new.distinct('previous.parameter.CAID')
print 1
caid2=iae_hitlog_new.distinct('previous.parameter.smid')
print 2
caid3=iae_hitlog_new.distinct('resource.parameter.CAID')
print 3
#print caid2
c=list(set(caid1).union(set(caid2)))
print 4
print len(list(set(c).union(set(caid3))))
print "done"

# a=set(caid1)|set(caid2)
# print 4
# b=set(caid3)|a
# print 5
# print len(b)

conn.close()