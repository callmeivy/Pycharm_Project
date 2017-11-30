# coding=UTF-8
import pymongo
w=open('E:\\action.txt','w')
# conn=pymongo.Connection('10.3.3.220',27017)
# 172.16.168.45
conn=pymongo.Connection('172.16.168.45',27017)
iae_vod_record=conn.gehua.iae_vod_record
# index = iae_hitlog_g.create_index("ip")
# ip_as=iae_vod_record.find({'date': {'$gt':1416672000000,'$lt':1416758399999}}).distinct('caid')
print 1
caids=iae_vod_record.find({'playRequestTime':'2014-11-27 20:22:13.000'}).distinct('caid')
ind=0
# mysqlconn=MySQLdb.connect(host="10.3.3.182",user="root",passwd="",db="demo_vsp_g")
#for result in res:
for onecaid in caids:
    print ind
    if ind>5:
         break
    ind=ind+1
    lines=iae_vod_record.find({'caid':onecaid}).sort('playRequestTime')
    # print ip_g,lines.count()
    asset=[]
    # caid=[]
    # store CAID
    # store program type
    ##change into next line
    #print "rowNumber2",rowNumber
    # time buffer
    for line in lines:
        # if line.get('assetName')=="toPlayBundle.do":
        #     tempLocalID = line.get('previous').get('parameter').get('rp2')
        #     if tempLocalID not in action:
        #             action.append(tempLocalID)
        # if line.get('previous').get('action').get('rp2')=="toPlayPackAsset.do":
        #     tempSPLocalID = line.get('previous').get('parameter').get('spLocalID')
        #     if tempSPLocalID not in action:
        #             action.append(tempSPLocalID)
        # store caid
        if line.get('assetName') is not None:
            action=(line.get('assetName')).encode('utf-8')
        asset.append(action)
        # if line.get('previous').get('parameter').get('smid') is not None:
        #     caid.append(line.get('previous').get('parameter').get('smid'))
    # row=ip_g
    if len(asset)>0:
        allaction=','.join(asset)
        print onecaid,allaction
        w.write(allaction+'\r\n' )
    # w.write(asset)
w.close()
conn.close()
