# coding=UTF-8
import datetime
import pymongo
import pyExcelerator.Workbook as Workbook
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

w=Workbook()
ws=w.add_sheet("iptime")
conn=pymongo.Connection('10.3.3.220',27017)
iae_hitlog_a=conn.gehua.iae_hitlog_a
iae_hitlog_a.create_index("ip")
ip=conn.gehua.ip
res=ip.find()
ind=0
rowNumber=0
mysqlconn=MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="demo_vsp_a")
#mysqlconn1=MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="ire")
for result in res:
  #Qestion
    print ind
    if ind>10000:
        break
    ind=ind+1
    ipstr=result['ip']
    if ipstr.startswith("172.16"):
        continue
    lines=iae_hitlog_a.find({'ip':ipstr}).sort('date')
  #Quesion
    print ipstr,lines.count()
    starttime=lines[0]['date']
    localtime=time.localtime(int(starttime/1000))
    timeinterval=datetime.timedelta(0,0)
    logesttime=0
    #保存每个IP和对应的开始时间
    ws.write(rowNumber,1,ipstr)
    ws.write(rowNumber,2,str(starttime))

    #if not caid==None:
    #    print "caid:",caid

    resource=[];
    caid=[];
    if lines[0].get('previous').get('action').get('rp2')=="toPlayBundle.do":
        if not (lines[0].get('previous').get('parameter').get('localID'))==None:
            resource.append(lines[0].get('previous').get('parameter').get('localID'))
    if lines[0].get('previous').get('action').get('rp2')=="toPlayPackAsset.do":
        if not  lines[0].get('previous').get('parameter').get('spLocalID')==None:
             resource.append(lines[0].get('previous').get('parameter').get('spLocalID'))
    if not  lines[0].get('previous').get('parameter').get('CAID')==None:
        caid.append(lines[0].get('previous').get('parameter').get('CAID'))
    #如何实现循环
    cstarttime=starttime;
  #Question
    ws.write(rowNumber,4,str(timepattern(time.localtime(cstarttime/1000))))
    rowNumber+=1
    ipTab=1
    datebak=[]
    for lineindex,line in enumerate(lines):
    #Question:枚举
        datebak.append(line['date'])
        #caid=line.get('previous').get('parameter').get('CAID')
        #if not caid==None:
        #    print "caid:",caid
        if line.get('previous').get('action').get('rp2')=="toPlayBundle.do":
            #resource.append(line.get('previous').get('parameter').get('localID'))
            if not  line.get('previous').get('parameter').get('localID')==None:
                resource.append(line.get('previous').get('parameter').get('localID'))

        if line.get('previous').get('action').get('rp2')=="toPlayPackAsset.do":
            #resource.append(line.get('previous').get('parameter').get('spLocalID'))
            if not  line.get('previous').get('parameter').get('spLocalID')==None:
                resource.append(line.get('previous').get('parameter').get('spLocalID'))

        if not  line.get('previous').get('parameter').get('CAID')==None:
            caid.append(line.get('previous').get('parameter').get('CAID'))
        endtime=line['date']
        timeinterval=endtime-starttime
        starttime=endtime
        if timeinterval>logesttime:
            logesttime=timeinterval
#get parent_id and caid info, write into the file
        if timeinterval>2*60*60*1000:
            #print 'should break'
            #给一个IP标签ipTab,保存IP相邻操作时间大于2小时的截止时间
            ws.write(rowNumber-1,3,datebak[lineindex-1]-cstarttime)
            if(len(resource)>0):
                cur=mysqlconn.cursor()
                #cur1=mysqlconn.cursor()
                print "resouce[0]:",resource[0]
                cur.execute("""select parent_id from element_info where extra_1=%s""",(resource[0]))
                #cur.execute('select * from element_info')
                parent_id=cur.fetchone()
                if not parent_id==None and int(parent_id[0])>=10002433:
                    cur.execute("""select parent_id from catalog_info where id=%s""",(parent_id))
                    parent_id=cur.fetchone()
                cur.close()
                try:
                    ws.write(rowNumber-1,5,parent_id[0])
                except:
                    ws.write(rowNumber-1,5,str(0))
            else:
                ws.write(rowNumber-1,5,str(0))

            if len(caid)>0:
                ws.write(rowNumber-1,6,caid[0])
            else:
                ws.write(rowNumber-1,6,str(0))

            ws.write(rowNumber,1,ipstr+'_'+str(ipTab))
            cstarttime=endtime;
            ws.write(rowNumber,4,str(timepattern(time.localtime(cstarttime/1000))))
            ws.write(rowNumber,2,str(endtime))
            resource=[]
            #here!
            #caid=[]
            ipTab+=1
            rowNumber+=1
#get the address info
            if(len(caid)>0):
                #SET NAMES utf8
                mysqlconn1=MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="ire")
                #cur=mysqlconn.cursor()
                cur1=mysqlconn1.cursor()
                print "caid[0]:",caid[0]
                cur1.execute("""select addressdistrictid from ire_user_district where caid=%s""",(caid[0]))
                #cur1.execute("""select parent_id from element_info where extra_1=%s""",(resource[0]))
                #cur1.execute("""select addressdistrict from ire_user_district where caid=%s""",(caid[0]))
                #contains:http://blog.csdn.net/abaowu/article/details/4325210
                #cur1.execute("""select address from ire_user_info where caid contains %s""",(caid[0]))
                #cur1.execute("""select address from ire_user_info Where instr(caid,caid[0])""")
                #cur1.execute("""select address from ire_user_info Where instr(caid,"1370962948")""")
                #cur1.execute("""select address from ire_user_info Where instr(caid,"%s")""",(caid[0]))
                #cur.execute('select * from element_info')
                addressdistrictid=cur1.fetchone()
                #if not parent_id==None and int(parent_id[0])>=10002433:
                    #cur.execute("""select parent_id from catalog_info where id=%s""",(parent_id))
                    #address=cur1.fetchone()
                    #parent_id=cur.fetchone()
                print addressdistrictid
                cur1.close()
                try:
                    ws.write(rowNumber-1,7,addressdistrictid[0])
                except:
                    print '\nSome error/exception occurred.'
                #except:
                    #ws.write(rowNumber-1,5,str(0))
            caid=[]
            rowNumber+=1


    #给一个IP标签longestTime,保存IP相邻操作时间间隔最大值
    ws.write(rowNumber-1,3,endtime-cstarttime)
    if(len(resource)>0):
        try:
            cur=mysqlconn.cursor()
            #cur1=mysqlconn.cursor()
            print "resource[0]:",resource[0]
            print"caid[0]:",caid[0]
            cur.execute("""select parent_id from element_info where extra_1=%s""",(resource[0]))
            #cur.execute('select * from element_info')
            parent_id=cur.fetchone()
            if not parent_id==None and int(parent_id[0])>=10002433:
                cur.execute("""select parent_id from catalog_info where id=%s""",(parent_id))
                parent_id=cur.fetchone()
            #cur1.execute("""select address from ire_user_info where caid=%s""",(caid[0]))
            #address=cur1.fetchone()
            cur.close()

            ws.write(rowNumber-1,5,parent_id[0])
            #ws.write(rowNumber-1,7,address[0])
        except:
            ws.write(rowNumber-1,5,str(0))
    else:
         ws.write(rowNumber-1,5,str(0))

    if len(caid)>0:
         ws.write(rowNumber-1,6,caid[0])
    else:
         ws.write(rowNumber-1,6,str(0))
    #ws.write(rowNumber,1,ipstr+'_longestTime')
    #ws.write(rowNumber,2,str(logesttime))
    #rowNumber+=1
    if(len(caid)>0):
        #SET NAMES utf8
        mysqlconn1=MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="ire")
        #cur=mysqlconn.cursor()
        cur1=mysqlconn1.cursor()
        print "caid[0]:",caid[0]
        cur1.execute("""select addressdistrictid from ire_user_district where caid=%s""",(caid[0]))
        #cur1.execute("""select parent_id from element_info where extra_1=%s""",(resource[0]))
        #cur1.execute("""select addressdistrict from ire_user_district where caid=%s""",(caid[0]))
        #contains:http://blog.csdn.net/abaowu/article/details/4325210
        #cur1.execute("""select address from ire_user_info where caid contains %s""",(caid[0]))
        #cur1.execute("""select address from ire_user_info Where instr(caid,caid[0])""")
        #cur1.execute("""select address from ire_user_info Where instr(caid,"1370962948")""")
        #cur1.execute("""select address from ire_user_info Where instr(caid,"%s")""",(caid[0]))
        #cur.execute('select * from element_info')
        addressdistrictid=cur1.fetchone()
        #if not parent_id==None and int(parent_id[0])>=10002433:
            #cur.execute("""select parent_id from catalog_info where id=%s""",(parent_id))
            #address=cur1.fetchone()
            #parent_id=cur.fetchone()
        print addressdistrictid
        cur1.close()
        try:
            ws.write(rowNumber-1,7,addressdistrictid[0])
        except:
            print '\nSome error/exception occurred.'
w.save('./iptime.xls')


#iae_hitlog_a.close()
#ip.close()
conn.close()
mysqlconn.close()

