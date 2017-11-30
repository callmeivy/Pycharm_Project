# coding=UTF-8
import datetime
import pymongo
#import pyExcelerator.Workbook as Workbook
from xlsxwriter.workbook import Workbook
import time
import MySQLdb

def timepattern(localtime):
    cate=0
    if  localtime.tm_hour>0 and localtime.tm_hour<1:
        cate=0
    elif localtime.tm_hour>=1 and localtime.tm_hour<2:
        cate=1
    elif localtime.tm_hour>=2 and localtime.tm_hour<3:
        cate=2
    elif localtime.tm_hour>=3 and localtime.tm_hour<4:
        cate=3
    elif localtime.tm_hour>=4 and localtime.tm_hour<5:
        cate=4
    elif localtime.tm_hour>=5 and localtime.tm_hour<6:
        cate=5
    elif localtime.tm_hour>=6 and localtime.tm_hour<7:
        cate=6
    elif localtime.tm_hour>=7 and localtime.tm_hour<8:
        cate=7
    elif localtime.tm_hour>=8 and localtime.tm_hour<9:
        cate=8
    elif localtime.tm_hour>=9 and localtime.tm_hour<10:
        cate=9
    elif localtime.tm_hour>=10 and localtime.tm_hour<11:
        cate=10
    elif localtime.tm_hour>=11 and localtime.tm_hour<12:
        cate=11
    elif localtime.tm_hour>=12 and localtime.tm_hour<13:
        cate=12
    elif localtime.tm_hour>=13 and localtime.tm_hour<14:
        cate=13
    elif localtime.tm_hour>=14 and localtime.tm_hour<15:
        cate=14
    elif localtime.tm_hour>=15 and localtime.tm_hour<16:
        cate=15
    elif localtime.tm_hour>=16 and localtime.tm_hour<17:
        cate=16
    elif localtime.tm_hour>=17 and localtime.tm_hour<18:
        cate=17
    elif localtime.tm_hour>=18 and localtime.tm_hour<19:
        cate=18
    elif localtime.tm_hour>=19 and localtime.tm_hour<20:
        cate=19
    elif localtime.tm_hour>=20 and localtime.tm_hour<21:
        cate=20
    elif localtime.tm_hour>=21 and localtime.tm_hour<22:
        cate=21
    elif localtime.tm_hour>=22 and localtime.tm_hour<23:
        cate=22
    elif localtime.tm_hour>=23 and localtime.tm_hour<24:
        cate=23
    return cate

w=Workbook('iptime.xlsx')
#w=Workbook()
#ws=w.add_sheet("iptime")
ws=w.add_worksheet("iptime")
conn=pymongo.Connection('10.3.3.220',27017)
iae_hitlog_a=conn.gehua.iae_hitlog_a
inter_userinfo=conn.gehua.inter_userinfo
inter_userinfo.create_index("caid")
iae_hitlog_a.create_index("ip")
ip_as=iae_hitlog_a.distinct("ip")
#ip=conn.gehua.ip
#res=ip.find()
ind=0
rowNumber=0
sum=0
#mysqlconn=MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="demo_vsp_a")
#mysqlconn1=MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="ire")
mysqlconn=MySQLdb.connect(host="10.3.3.182",user="root",passwd="",db="demo_vsp_a")
#for result in res:
for ip_a in ip_as:

    print ind
    if ind>500:
        break
    ind=ind+1
    #ipstr=result['ip']
    #if ipstr.startswith("172.16"):
    if ip_a.startswith("172.16"):
        continue
    lines=iae_hitlog_a.find({'ip':ip_a}).sort('date')
    # linenumber=lines.count()
    print ip_a,lines.count()
    starttime=lines[0]['date']
    # if linenumber>1:
    #     endtime=lines[linenumber-1]['date']
    localtime=time.localtime(int(starttime/1000))
    # timeinterval=datetime.timedelta(0,0)
    # logesttime=0
    print "rowNumber1",rowNumber
    ws.write(rowNumber,1,ip_a)
    ws.write(rowNumber,2,str(starttime))
    print "rowNumber2",rowNumber
    #ws.write(rowNumber,3,endtime-starttime)
# cstarttime is the turn on time
    cstarttime=starttime
    ws.write(rowNumber,4,str(timepattern(time.localtime(cstarttime/1000))))
    print "rowNumber3",rowNumber
    #if not caid==None:
    #    print "caid:",caid
    # store resource ids
    resource=[]
    # store CAID
    caid=[]
    # store program type
    type=[]
    ##change into next line

    rowNumber+=1
    #print "rowNumber2",rowNumber
    ipTab=1
    # time buffer
    timelist=[]
    # cursive same ip adress hitlog
    for line in lines:
        timelist.append(line['date'])
        #caid=line.get('previous').get('parameter').get('CAID')
        #if not caid==None:
        #    print "caid:",caid
        if line.get('previous').get('action').get('rp2')=="toPlayBundle.do":
            #resource.append(line.get('previous').get('parameter').get('localID'))
            tempLocalID = line.get('previous').get('parameter').get('localID')
            if tempLocalID is not None:
                if tempLocalID not in resource:
                    resource.append(tempLocalID)

        if line.get('previous').get('action').get('rp2')=="toPlayPackAsset.do":
            tempSPLocalID = line.get('previous').get('parameter').get('spLocalID')
            if tempSPLocalID is not None:
                if tempSPLocalID not in resource:
                    resource.append(tempSPLocalID)

        # store caid
        if line.get('previous').get('parameter').get('CAID') is not None:
            caid.append(line.get('previous').get('parameter').get('CAID'))
        if line.get('previous').get('parameter').get('smid') is not None:
            caid.append(line.get('previous').get('parameter').get('smid'))
#         endtime=line['date']
        #timeinterval=endtime-starttime
#         starttime=endtime
#         if timeinterval>logesttime:
#             logesttime=timeinterval
#get parent_id and caid info, write into the file
        #if timeinterval>2*60*60*1000:
        # if cut
        if len(timelist)>2 and line.get('previous').get('action').get('rp1')=="portal" and timelist[-1]-timelist[-2]>2*60*60*1000:
            print timelist[-2],timelist[-1]
            lastasfirst=timelist[-1]
            print "rowNumber4",rowNumber
            ws.write(rowNumber-1,3,timelist[-2]-timelist[0])
            print "rowNumber5",rowNumber
            timelist=[]
            timelist.append(lastasfirst)
            # has watched resource
            if(len(resource)>0):
                cur = mysqlconn.cursor()
                #cur1=mysqlconn.cursor()
                #print "resource[0]:",resource[0],"len(resource)",len(resource)
                # search every resource
                for oneresource in resource:
                    cur.execute("""select parent_id from element_info where extra_1=%s""",(oneresource))
                    print oneresource
                    parent_id=cur.fetchone()
                    if parent_id!=None:
                        parent_id = parent_id[0]
                        if int(parent_id)>=10002433:
                            cur.execute("""select parent_id from catalog_info where id=%s""",(parent_id))
                            parent_id=cur.fetchone()
                            if parent_id is not None:
                                parent_id = parent_id[0]
                            else:
                                parent_id = -1
                        if parent_id not in type and parent_id!=-1:
                            type.append(parent_id)
                print "type",type
#change the type list into the 14-dimension vector
                typecode=0
                sum=0
                for onetype in type:
                    #watched=0
                    print "onetype",int(onetype)
                    if int(onetype)==10002340:
                        typecode+=100000000000000
                    if int(onetype)==10002341:
                        typecode+=10000000000000
                    if int(onetype)==10002342:
                        typecode+=1000000000000
                    if int(onetype)==10002344:
                        typecode+=100000000000
                    if int(onetype)==10002345:
                        typecode+=10000000000
                    if int(onetype)==10002346:
                        typecode+=1000000000
                    if int(onetype)==10002348:
                        typecode+=100000000
                    if int(onetype)==10002349:
                        typecode+=10000000
                    if int(onetype)==10002350:
                        typecode+=1000000
                    if int(onetype)==10002351:
                        typecode+=100000
                    if int(onetype)==10002352:
                        typecode+=10000
                    if int(onetype)==10002353:
                        typecode+=1000
                    if int(onetype)==10002354:
                        typecode+=100
                    if int(onetype)==10002357:
                        typecode+=10
                    if int(onetype)==10002358:
                        typecode+=10
                    if int(onetype)==10002359:
                        typecode+=10
                    if int(onetype)==10002363:
                        typecode+=1
                    if int(onetype)==10002364:
                        typecode+=1
                    if int(onetype)==10002365:
                        typecode+=1
                    if int(onetype)==10002366:
                        typecode+=1
                    if int(onetype)==10002367:
                        typecode+=1
                    sum+=typecode
                print "typecode",sum

                # cur.close()
                try:
                    print "rowNumber6",rowNumber
                    #ws.write(rowNumber-1,5,parent_id[0])
                    ws.write(rowNumber-1,5,str(sum))
                    #ws.write(rowNumber-1,5,','.join(type))
                    # rowNumber+=1
                except:
                    ws.write(rowNumber-1,5,str(0))

                cur.close()
            else:
                ws.write(rowNumber-1,5,str(0))

            print "rowNumber7",rowNumber
            # typecode=[]
            # sum=[]
#len(resource>0) circle ends.

#len(caid>0) circle starts.
            if len(caid)>0:
                ws.write(rowNumber-1,6,caid[0])
            else:
                ws.write(rowNumber-1,6,str(0))
#len(caid>0) circle ends.
            ws.write(rowNumber,1,ip_a+'_'+str(ipTab))
            cstarttime=timelist[0]
            ws.write(rowNumber,4,str(timepattern(time.localtime(cstarttime/1000))))
            ws.write(rowNumber,2,str(cstarttime))
            #resource=[]

#get the address info
            if(len(caid)>0):
                res1=inter_userinfo.find({'caid':caid[0]})
                for ress1 in res1:
                    addressdistrict=ress1['addressdistrict']
                try:
                    ws.write(rowNumber-1,7,addressdistrict[0])
                except:
                    ws.write(rowNumber-1,7,str(0))
            else:
                ws.write(rowNumber-1,7,str(0))
            # caid=[]
            #here!
            #caid=[]
            print "rowNumber8",rowNumber
            ipTab+=1
            rowNumber+=1
            print "rowNumber9",rowNumber
#get the address info
            # if(len(caid)>0):
            #     #SET NAMES utf8
            #     mysqlconn1=MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="ire")
            #     #cur=mysqlconn.cursor()
            #     cur1=mysqlconn1.cursor()
            #     print "caid[0]:",caid[0]
            #     cur1.execute("""select addressdistrictid from ire_user_district where caid=%s""",(caid[0]))
            #     #cur1.execute("""select parent_id from element_info where extra_1=%s""",(resource[0]))
            #     #cur1.execute("""select addressdistrict from ire_user_district where caid=%s""",(caid[0]))
            #     #contains:http://blog.csdn.net/abaowu/article/details/4325210
            #     #cur1.execute("""select address from ire_user_info where caid contains %s""",(caid[0]))
            #     #cur1.execute("""select address from ire_user_info Where instr(caid,caid[0])""")
            #     #cur1.execute("""select address from ire_user_info Where instr(caid,"1370962948")""")
            #     #cur1.execute("""select address from ire_user_info Where instr(caid,"%s")""",(caid[0]))
            #     #cur.execute('select * from element_info')
            #     addressdistrictid=cur1.fetchone()
            #     #if not parent_id==None and int(parent_id[0])>=10002433:
            #         #cur.execute("""select parent_id from catalog_info where id=%s""",(parent_id))
            #         #address=cur1.fetchone()
            #         #parent_id=cur.fetchone()
            #     print addressdistrictid
            #     cur1.close()
            #     try:
            #         ws.write(rowNumber-1,7,addressdistrictid[0])
            #     except:
            #         print '\nSome error/exception occurred.'
                #except:
                    #ws.write(rowNumber-1,5,str(0))

#             rowNumber+=1

    #ws.write(rowNumber-1,3,endtime-cstarttime)
    print "rowNumber10",rowNumber
    #ws.write(rowNumber-1,3,timelist[len(timelist)-1]-timelist[0])
    ws.write(rowNumber-1,3,timelist[len(timelist)-1]-timelist[0])
    #ws.write(rowNumber-1,3,endtime-starttime)
    if(len(resource)>0):
        cur=mysqlconn.cursor()
                #cur1=mysqlconn.cursor()
                #print "resource[0]:",resource[0],"len(resource)",len(resource)
                # search every resource
        for oneresource in resource:
            cur.execute("""select parent_id from element_info where extra_1=%s""",(oneresource))
            print oneresource
            parent_id=cur.fetchone()
            if parent_id!=None:
                parent_id = parent_id[0]
                if int(parent_id)>=10002433:
                    cur.execute("""select parent_id from catalog_info where id=%s""",(parent_id))
                    parent_id=cur.fetchone()
                    if parent_id is not None:
                        parent_id = parent_id[0]
                    else:
                        parent_id = -1
                if parent_id not in type and parent_id!=-1:
                    type.append(parent_id)

        print "type",type
        # cur.close()
        #int type has the maximun limit
        typecode=0
        sum=0
        for onetype in type:
            #watched=0
            print "onetype",int(onetype)
            if int(onetype)==10002340:
                typecode+=100000000000000
            if int(onetype)==10002341:
                typecode+=10000000000000
            if int(onetype)==10002342:
                typecode+=1000000000000
            if int(onetype)==10002344:
                typecode+=100000000000
            if int(onetype)==10002345:
                typecode+=10000000000
            if int(onetype)==10002346:
                typecode+=1000000000
            if int(onetype)==10002348:
                typecode+=100000000
            if int(onetype)==10002349:
                typecode+=10000000
            if int(onetype)==10002350:
                typecode+=1000000
            if int(onetype)==10002351:
                typecode+=100000
            if int(onetype)==10002352:
                typecode+=10000
            if int(onetype)==10002353:
                typecode+=1000
            if int(onetype)==10002354:
                typecode+=100
            if int(onetype)==10002357:
                typecode+=10
            if int(onetype)==10002358:
                typecode+=10
            if int(onetype)==10002359:
                typecode+=10
            if int(onetype)==10002363:
                typecode+=1
            if int(onetype)==10002364:
                typecode+=1
            if int(onetype)==10002365:
                typecode+=1
            if int(onetype)==10002366:
                typecode+=1
            if int(onetype)==10002367:
                typecode+=1
            sum+=typecode
        print "typecode",sum
        # typecode=[]
        # sum=[]
        try:
            #ws.write(rowNumber-1,5,parent_id[0])
            #ws.write(rowNumber-1,5,','.join(type))
            ws.write(rowNumber-1,5,str(sum))
            # rowNumber+=1
        except:
            ws.write(rowNumber-1,5,str(0))

        cur.close()
    else:
        ws.write(rowNumber-1,5,str(0))
    # typecode=[]
    # sum=[]
    if len(caid)>0:
         ws.write(rowNumber-1,6,caid[0])
    else:
         ws.write(rowNumber-1,6,str(0))
                #cur1=mysqlconn.cursor()
                #print "resource[0]:",resource[0],"len(resource)",len(resource)
    #     #cur=mysqlconn.cursor()
    #     cur1=mysqlconn1.cursor()
    #     print "caid[0]:",caid[0]
    #     cur1.execute("""select addressdistrictid from ire_user_district where caid=%s""",(caid[0]))
    #     #cur1.execute("""select parent_id from element_info where extra_1=%s""",(resource[0]))
    #     #cur1.execute("""select addressdistrict from ire_user_district where caid=%s""",(caid[0]))
    #     #contains:http://blog.csdn.net/abaowu/article/details/4325210
    #     #cur1.execute("""select address from ire_user_info where caid contains %s""",(caid[0]))
    #     #cur1.execute("""select address from ire_user_info Where instr(caid,caid[0])""")
    #     #cur1.execute("""select address from ire_user_info Where instr(caid,"1370962948")""")
    #     #cur1.execute("""select address from ire_user_info Where instr(caid,"%s")""",(caid[0]))
    #     #cur.execute('select * from element_info')
    #     addressdistrictid=cur1.fetchone()
    #     #if not parent_id==None and int(parent_id[0])>=10002433:
    #         #cur.execute("""select parent_id from catalog_info where id=%s""",(parent_id))
    #         #address=cur1.fetchone()
    #         #parent_id=cur.fetchone()
    #     print addressdistrictid
    #     cur1.close()
    #     try:
    #         ws.write(rowNumber-1,7,addressdistrictid[0])
    #     except:
    #         print '\nSome error/exception occurred.'
    if(len(caid)>0):
        res1=inter_userinfo.find({'caid':caid[0]})
        for ress1 in res1:
            addressdistrict=ress1['addressdistrict']
            # print "caid[0]:",caid[0]
            # print addressdistrict
        try:
            ws.write(rowNumber-1,7,addressdistrict[0])
        except:
            ws.write(rowNumber-1,7,str(0))
    else:
        ws.write(rowNumber-1,7,str(0))
w.close()

#iae_hitlog_a.close()
#ip.close()
conn.close()
mysqlconn.close()

