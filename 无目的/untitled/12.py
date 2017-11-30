import pymongo
conn=pymongo.Connection('10.3.3.220',27017)
iae_hitlog_g=conn.gehua.iae_hitlog_g

caid1=iae_hitlog_g.distinct('resource.parameter.CAID')

caid2=iae_hitlog_g.distinct('previous.parameter.smid')
#print caid2
for onecaid2 in caid2:
    if onecaid2 not in caid1:
        caid1.append(onecaid2)

print len(caid1)

conn.close