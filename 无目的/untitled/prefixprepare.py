# coding=UTF-8
import datetime
import pymongo
import pyExcelerator.Workbook as Workbook
import time
import MySQLdb

w=Workbook()
ws=w.add_sheet("iptime")
conn=pymongo.Connection('10.3.3.220',27017)
iae_hitlog_e=conn.gehua.iae_hitlog_e
inter_userinfo=conn.gehua.inter_userinfo
iae_hitlog_e.create_index("ip")
inter_userinfo.create_index("caid")
ip_e=iae_hitlog_e.distinct("ip")
ind=0
rowNumber=0
#mysqlconn=MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="demo_vsp_a")
#循环所有ip
for result in ip_e:
    print ind
    if ind>50:
         break
    ind=ind+1
    if result.startswith("172.16"):
        continue
    lines=iae_hitlog_e.find({'ip':result}).sort('date')
    print result,lines.count()

    starttime=lines[0]['date']
    #localtime=time.localtime(int(starttime/1000))
    #timeinterval=datetime.timedelta(0,0)
    #logesttime=0
    print starttime

    #ws.write(rowNumber,2,str(starttime))
    resource=[];
    caid=[];
    # if not  lines[0].get('previous').get('parameter').get('CAID')==None:
    #     caid.append(lines[0].get('previous').get('parameter').get('CAID'))
    #ws.write(rowNumber,2,(caid[0]))
    #if下面定义不准确，如果不符合条件便没有初值，头部加上cstarttime=0
    #rowNumber+=1
    # ipTab=1
    datebak=[]
    #每个ip内部的循环
    #for lineindex,line in enumerate(lines):
    for line in lines:
        ipstr=line['ip']
        start=str(line['date'])
        print ipstr
        #ws.write(rowNumber-1,1,ipstr)
        #ws.write(rowNumber-1,2,start)
        #rowNumber+=1
        datebak.append(line['date'])
        if not  line.get('previous').get('parameter').get('CAID')==None:
            #不重复的caid
            if not line.get('previous').get('parameter').get('CAID') in caid:
                caid.append(line.get('previous').get('parameter').get('CAID'))
                print "caid",caid[0],"caidnumber",len(caid)
                ###同一个ip有不重复的caid时才写入excel
                ws.write(rowNumber,1,ipstr)
                ws.write(rowNumber,2,start)
                ws.write(rowNumber,3,caid[0])
                ws.write(rowNumber,4,len(caid))
                rowNumber+=1
        # endtime=line['date']
        # timeinterval=endtime-starttime
        # starttime=endtime
        # if timeinterval>logesttime:
        #     logesttime=timeinterval

w.save('./iptime.xls')



#iae_hitlog_b.close()
#ip.close()
conn.close()
#mysqlconn.close()



