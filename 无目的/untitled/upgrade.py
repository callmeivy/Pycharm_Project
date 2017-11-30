# coding=UTF-8
import datetime
import pymongo
#import pyExcelerator.Workbook as Workbook
#from xlsxwriter.workbook import Workbook
import csv
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

# w=Workbook('iptime.xlsx')
# ws=w.add_worksheet("iptime")
fp=file('iptime.csv','wb')
w=csv.writer(fp)
w.writerow(['ip','starttime','length',"starthour","programtype","caid","district"])
conn=pymongo.Connection('10.3.3.220',27017)
iae_hitlog_a=conn.gehua.iae_hitlog_a
inter_userinfo=conn.gehua.inter_userinfo
inter_userinfo.create_index("caid")
iae_hitlog_a.create_index("ip")
ip_as=iae_hitlog_a.distinct("ip")
ind=0
#rowNumber=0
sum=0
mysqlconn=MySQLdb.connect(host="10.3.3.182",user="root",passwd="",db="demo_vsp_a")
for ip_a in ip_as:

    print ind
    if ind>50:
        break
    ind=ind+1
    if ip_a.startswith("172.16"):
        continue
    lines=iae_hitlog_a.find({'ip':ip_a}).sort('date')

    print ip_a,lines.count()
    starttime=lines[0]['date']
    localtime=time.localtime(int(starttime/1000))
    # timeinterval=datetime.timedelta(0,0)
    # logesttime=0
    ws.write(rowNumber,1,ip_a)
    ws.write(rowNumber,2,str(starttime))

    # store resource ids
    resource=[]
    # store CAID
    caid=[]
    # store program type
    type=[]

    # cstarttime is the turn on time
    cstarttime=starttime
    ws.write(rowNumber,4,str(timepattern(time.localtime(cstarttime/1000))))
    rowNumber+=1
    ipTab=1
    # time buffer
    timelist=[]
    # cursive same ip adress hitlog
    for line in lines:
        timelist.append(line['date'])
        if line.get('previous').get('action').get('rp2')=="toPlayBundle.do":
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
            lastasfirst=timelist[-1]
            ws.write(rowNumber-1,3,timelist[-2]-timelist[0])

            timelist=[]
            timelist.append(lastasfirst)

            # has watched resource
            if(len(resource)>0):
                cur = mysqlconn.cursor()
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
                            #print ','.join(type)
                print "type",','.join(type)
                try:
                    #ws.write(rowNumber-1,5,parent_id[0])
                    ws.write(rowNumber-1,5,','.join(type))
                    # rowNumber+=1
                except:
                    ws.write(rowNumber-1,5,str(0))
                cur.close()
            else:
                ws.write(rowNumber-1,5,str(0))

            if len(caid)>0:
                ws.write(rowNumber-1,6,caid[0])
            else:
                ws.write(rowNumber-1,6,str(0))

            ws.write(rowNumber,1,ip_a+'_'+str(ipTab))
            cstarttime=timelist[0]
            ws.write(rowNumber,4,str(timepattern(time.localtime(cstarttime/1000))))
            ws.write(rowNumber,2,str(cstarttime))
            #resource=[]
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
            ipTab+=1
            rowNumber+=1
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
    ws.write(rowNumber-1,3,timelist[len(timelist)-1]-timelist[0])
    if(len(resource)>0):
        cur=mysqlconn.cursor()
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
#w.save('./iptime.xls')
w.close()

#iae_hitlog_a.close()
#ip.close()
conn.close()
mysqlconn.close()


