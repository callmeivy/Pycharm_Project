# coding=UTF-8
# import sys
# reload(sys)
# sys.setdefaultencoding('utf8')
import pymongo
import codecs
import sys
reload(sys)
# sys.setdefaultencoding('utf8′)
sys.setdefaultencoding('utf8')

# print myname.decode('utf-8').encode(type)
# codecs.open(filepath, 'r', 'gbk')
w=codecs.open('aaaction.txt','w', 'utf-8')
# conn=pymongo.Connection('10.3.3.220',27017)
# 172.16.168.45
conn=pymongo.Connection('172.16.168.45',27017)
iae_vod_record=conn.gehua.iae_vod_record
iae_hitlog_record=conn.gehua.iae_hitlog_record
print "connected"
# index_1=iae_vod_record.ensure_index("assetName")
# index_2=iae_vod_record.ensure_index("caid")
# index_3=iae_hitlog_record.ensure_index("assetName")
# index_4=iae_hitlog_record.ensure_index("caid")
caids=iae_hitlog_record.find({'assetName':'奔跑吧兄弟'}).distinct('caid')
# print len(caids)
print "caid extraction done!"
ind=0
for onecaid in caids:
    allaction=''
    # print ind
    if ind>100:
         break
    ind=ind+1
    lines=iae_hitlog_record.find({'caid':onecaid}).sort('playRequestTime')
    asset=[]
    for line in lines:
        if line.get('assetName') is not None:
            action=(line.get('assetName')).encode('utf-8')
            print 'action',action
            if action[-4]=='(':
                n=len(action)-4
                action=action[0:n]
            if action[-3]=='(':
                n=len(action)-3
                action=action[0:n]
                # action=action.decode('utf-8').encode(type)
                # w.write(str(action))
            asset.append(action)
        m=len(asset)-2
        if len(asset)>1:
            if asset[m][0:6]==action[0:6]:
                del asset[-1]
    # # one character indicates 3 blabla...
    #
    #     # asset.pop()
    if len(asset)>0:
        # for element in asset:
        #     print 'element',element,type(element)
        allaction=','.join(asset)
            # allaction+=str(element)+','
        print "caid",onecaid,allaction
        print "allaction",allaction
        w.write(unicode(allaction)+'\r\n' )


# vod
# caids2=iae_vod_record.find({'assetName':'奔跑吧兄弟'}).distinct('caid')
# for onecaid in caids2:
#     print ind
#     if ind>2:
#          break
#     ind=ind+1
#     lines=iae_vod_record.find({'caid':onecaid}).sort('playRequestTime')
#     asset=[]
#     for line in lines:
#         if line.get('assetName') is not None:
#             action=(line.get('assetName')).encode('utf-8')
#             if (line.get('poname')[0:3]).encode('utf-8')=='电视剧':
#                 n=len(action)-2
#                 action=action[0:n]
#         asset.append(action)
#         m=len(asset)-2
#         if len(asset)>1:
#             if (line.get('poname')[0:3]).encode('utf-8')=='电视剧' and asset[m][0:6]==action[0:6]:
#                 del asset[-1]
#     # one character indicates 3 blabla...
#
#         # asset.pop()
#
#     if len(asset)>0:
#         allaction=','.join(asset)
#         print "caid",onecaid,allaction
#         w.write(allaction+'\r\n' )
w.close()
conn.close()
