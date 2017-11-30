# coding=UTF-8
import datetime
import pymongo
# from xlsxwriter.workbook import Workbook
import time
# import MySQLdb

# w=Workbook('iptime.xlsx')
#w=Workbook()
#ws=w.add_sheet("iptime")
w=open('E:\\action.txt','w')
conn=pymongo.Connection('10.3.3.220',27017)
iae_hitlog_g=conn.gehua.iae_hitlog_g
index = iae_hitlog_g.create_index("ip")
ip_gs=iae_hitlog_g.distinct("ip")
#ip=conn.gehua.ip
#res=ip.find()
ind=0
# mysqlconn=MySQLdb.connect(host="10.3.3.182",user="root",passwd="",db="demo_vsp_g")
#for result in res:
for ip_g in ip_gs:
    print ind
    if ind>50:
         break
    ind=ind+1
    if ip_g.startswith("172.16"):
        continue
    lines=iae_hitlog_g.find({'ip':ip_g}).sort('date')
    print ip_g,lines.count()
    action=[]
    caid=[]
    # store CAID
    # store program type
    ##change into next line
    #print "rowNumber2",rowNumber
    # time buffer
    for line in lines:
        if line.get('previous').get('action').get('rp2')=="toPlayBundle.do":
            tempLocalID = line.get('previous').get('parameter').get('rp2')
            if tempLocalID not in action:
                    action.append(tempLocalID)
        if line.get('previous').get('action').get('rp2')=="toPlayPackAsset.do":
            tempSPLocalID = line.get('previous').get('parameter').get('spLocalID')
            if tempSPLocalID not in resource:
                    action.append(tempSPLocalID)
        # store caid
        if line.get('resource').get('parameter').get('CAID') is not None:
            caid.append(line.get('resource').get('parameter').get('CAID'))
        if line.get('previous').get('parameter').get('smid') is not None:
            caid.append(line.get('previous').get('parameter').get('smid'))
    row=ip_g
    w.write(row)
w.close()
conn.close()