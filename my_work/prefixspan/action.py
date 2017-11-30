# coding=UTF-8
import pymongo
w=open('E:\\action.txt','w')
# conn=pymongo.Connection('10.3.3.220',27017)
# 172.16.168.45
conn=pymongo.Connection('172.16.168.45',27017)
iae_vod_record=conn.gehua.iae_vod_record
print 1
caids=iae_vod_record.find({'playRequestTime':'2014-11-27 20:22:13.000'}).distinct('caid')
ind=0
for onecaid in caids:
    print ind
    if ind>5:
         break
    ind=ind+1
    lines=iae_vod_record.find({'caid':onecaid}).sort('playRequestTime')
    asset=[]
    for line in lines:
        if line.get('assetName') is not None:
            action=(line.get('assetName')).encode('utf-8')

            print action
        asset.append(action)
        print "asset1",asset[-1]
        print "lenn",len(asset)
        print "ac",action
        m=len(asset)-1
        # if len(asset)>1:
        if len(asset)>1 and line.get('poname')[0:3]=='电视剧' and action[-1]=='0' or '1' or '2' or '3' or '4' or '5' or '6' or '7' or '8' or '9' and asset[m][0:6]==action[0:6]:
            print 'asset',asset
            print "m",asset[m]
            del asset[-1]
    # one character indicates 3 blabla...

        # asset.pop()

    if len(asset)>0:
        allaction=','.join(asset)
        print onecaid,allaction
        w.write(allaction+'\r\n' )
w.close()
conn.close()
