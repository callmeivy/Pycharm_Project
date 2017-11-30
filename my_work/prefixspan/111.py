# coding=UTF-8
import pymongo

w=open('E:\\action.txt','w')
# conn=pymongo.Connection('10.3.3.220',27017)
conn=pymongo.Connection('172.16.168.45',27017)
iae_hitlog_c=conn.gehua.iae_hitlog_c
# index = iae_hitlog_c.create_index("ip")
print 111
ip_cs=iae_hitlog_c.find({'date': {'$gt':1403020800000,'$lt':1403107199999}}).distinct('ip')
print 1111
ind=0
#for result in res:
# for ip_c in ip_cs[:500]:
for ip_c in ip_cs:
    print ind
    if ind>1000:
         break
    ind=ind+1
    #ipstr=result['ip']
    #if ipstr.startswith("172.16"):
    if ip_c.startswith("172.16"):
        continue
    lines=iae_hitlog_c.find({'ip':ip_c}).sort('date')
    print ip_c

    action=[]
    # store CAID
    caid=[]

    list1=[]
    # cursive same ip adress hitlog
    for line in lines:
        # parp2=line.get('previous').get('action').get('rp2')
        # if line.get('previous').get('action').get('rp2')=="shiyi.do":
        #     parp2+=line.get('previous').get('parameter').get('name')
        #     # print parp2
        #     action.append(parp2)
        # if line.get('previous').get('action').get('rp1')=="dshk":
        #     if line.get('previous').get('parameter').get('title') is not None:
        #         parp2+=(line.get('previous').get('parameter').get('title')).encode('utf-8')
        #         action.append(parp2)
                # print parp2
        if line.get('resource').get('action').get('rp1') is not None:
            rarp2=line.get('resource').get('action').get('rp1')
            print rarp2
        if line.get('resource').get('action').get('rp2') is not None:
            rarp2+=line.get('resource').get('action').get('rp2')
        if line.get('resource').get('action').get('rp3') is not None:
            rarp2+=line.get('resource').get('action').get('rp3')
        if line.get('resource').get('action').get('rp4') is not None:
            rarp2+=line.get('resource').get('action').get('rp4')
        if line.get('resource').get('parameter').get('name') is not None:
            rarp2+=(line.get('resource').get('parameter').get('name')).encode('utf-8')
                    # if line.get('resource').get('parameter').get('programId') is not None:
                    #     rarp2+=line.get('resource').get('parameter').get('programId')
        action.append(rarp2)
        # print rp2
        # if rp2 is not None and rp2!="shiyi.do" and rp2!="index-final.htm" and rp2!="dshk.htm":


        # store caid
        # if line.get('resource').get('parameter').get('CAID') is not None:
        #     caid.append(line.get('resource').get('parameter').get('CAID'))
        # if line.get('previous').get('parameter').get('smid') is not None:
        #     caid.append(line.get('previous').get('parameter').get('smid'))
    if len(action)>0:
        # print "action",action
        #duplicated elements should be handled
        #Notice:set is unordered.
        # unique_actions = list(set(action))
        # lineaction=(','.join(unique_actions))
        # w.write(lineaction+'\r\n')
        # for i in range (0,len(action)-1):
        #     if action[i]!=action[i-1]:
        #         list1.append(action[i])
        #         allaction=','.join(list1)
        # print "allaction",allaction
        # allaction=set(action)
        allaction=','.join(action)
        print allaction
        w.write(allaction+'\r\n' )



w.close()

conn.close()