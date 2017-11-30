# coding=UTF-8
import pymongo
conn=pymongo.Connection('172.16.168.45',27017)
print  "connected"
iae_hitlog_new=conn.gehua.iae_hitlog_new
print 0
index = iae_hitlog_new.create_index("insertTime")
# iae_hitlog_new.ensureIndex({"date":1})
# date=iae_hitlog_new.distinct('date')
lines=iae_hitlog_new.find({"insertTime":{"$gt":1411920000000}})
print 1
caid1=list()
caid2=list()
caid3=list()

for line in lines:

    # print line['date']
    # exit(0)
    # caid1=iae_hitlog_new.distinct('previous.parameter.CAID')
    # if line['previous']['parameter']['CAID'] is not None:
    if 'CAID' in line['previous']['parameter']:
        caid1.append(line['previous']['parameter']['CAID'])
        print "11"
    # if line['previous']['parameter']['smid'] is not None:
    if 'smid' in line['previous']['parameter']:
        caid2.append(line['previous']['parameter']['smid'])
    # if line['resource']['parameter']['CAID'] is not None:
    if 'CAID' in line['resource']['parameter']:
        caid3.append(line['resource']['parameter']['CAID'])
#print caid2
print 1
c=list(set(caid1).union(set(caid2)))
print 3
print len(list(set(c).union(set(caid3))))
print "done"


conn.close()