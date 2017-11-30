# coding=UTF-8
import datetime
import pymongo
import time
import MySQLdb
import math
def timepattern(localtime):
    cate=0
    if  localtime.tm_hour>0 and localtime.tm_hour<1:
        cate=1
    elif localtime.tm_hour>=1 and localtime.tm_hour<2:
        cate=math.cos(math.pi/12)
    elif localtime.tm_hour>=2 and localtime.tm_hour<3:
        cate=math.cos(math.pi/6)
    elif localtime.tm_hour>=3 and localtime.tm_hour<4:
        cate=math.cos(math.pi/4)
    elif localtime.tm_hour>=4 and localtime.tm_hour<5:
        cate=math.cos(math.pi/3)
    elif localtime.tm_hour>=5 and localtime.tm_hour<6:
        cate=math.cos((5/12)*math.pi)
    elif localtime.tm_hour>=6 and localtime.tm_hour<7:
        cate=0
    elif localtime.tm_hour>=7 and localtime.tm_hour<8:
        cate=math.cos((7/12)*math.pi)
    elif localtime.tm_hour>=8 and localtime.tm_hour<9:
        cate=math.cos((2/3)*math.pi)
    elif localtime.tm_hour>=9 and localtime.tm_hour<10:
        cate=math.cos((3/4)*math.pi)
    elif localtime.tm_hour>=10 and localtime.tm_hour<11:
        cate=math.cos((5/6)*math.pi)
    elif localtime.tm_hour>=11 and localtime.tm_hour<12:
        cate=math.cos((11/12)*math.pi)
    elif localtime.tm_hour>=12 and localtime.tm_hour<13:
        cate=-1
    elif localtime.tm_hour>=13 and localtime.tm_hour<14:
        cate=math.cos(1.08*math.pi)
    elif localtime.tm_hour>=14 and localtime.tm_hour<15:
        cate=math.cos(1.17*math.pi)
    elif localtime.tm_hour>=15 and localtime.tm_hour<16:
        cate=math.cos(1.25*math.pi)
    elif localtime.tm_hour>=16 and localtime.tm_hour<17:
        cate=math.cos(1.33*math.pi)
    elif localtime.tm_hour>=17 and localtime.tm_hour<18:
        cate=math.cos(1.42*math.pi)
    elif localtime.tm_hour>=18 and localtime.tm_hour<19:
        cate=0
    elif localtime.tm_hour>=19 and localtime.tm_hour<20:
        cate=math.cos(1.58*math.pi)
    elif localtime.tm_hour>=20 and localtime.tm_hour<21:
        cate=math.cos(1.67*math.pi)
    elif localtime.tm_hour>=21 and localtime.tm_hour<22:
        cate=math.cos(1.75*math.pi)
    elif localtime.tm_hour>=22 and localtime.tm_hour<23:
        cate=math.cos(1.83*math.pi)
    elif localtime.tm_hour>=23 and localtime.tm_hour<24:
        cate=math.cos(1.92*math.pi)
    return cate

def timepattern2(localtime):
    cate=0
    if  localtime.tm_hour>0 and localtime.tm_hour<1:
        cate=1
    elif localtime.tm_hour>=1 and localtime.tm_hour<2:
        cate=math.sin(math.pi/12)
    elif localtime.tm_hour>=2 and localtime.tm_hour<3:
        cate=math.sin(math.pi/6)
    elif localtime.tm_hour>=3 and localtime.tm_hour<4:
        cate=math.sin(math.pi/4)
    elif localtime.tm_hour>=4 and localtime.tm_hour<5:
        cate=math.sin(math.pi/3)
    elif localtime.tm_hour>=5 and localtime.tm_hour<6:
        cate=math.sin((5/12)*math.pi)
    elif localtime.tm_hour>=6 and localtime.tm_hour<7:
        cate=0
    elif localtime.tm_hour>=7 and localtime.tm_hour<8:
        cate=math.sin((7/12)*math.pi)
    elif localtime.tm_hour>=8 and localtime.tm_hour<9:
        cate=math.sin((2/3)*math.pi)
    elif localtime.tm_hour>=9 and localtime.tm_hour<10:
        cate=math.sin((3/4)*math.pi)
    elif localtime.tm_hour>=10 and localtime.tm_hour<11:
        cate=math.sin((5/6)*math.pi)
    elif localtime.tm_hour>=11 and localtime.tm_hour<12:
        cate=math.sin((11/12)*math.pi)
    elif localtime.tm_hour>=12 and localtime.tm_hour<13:
        cate=-1
    elif localtime.tm_hour>=13 and localtime.tm_hour<14:
        cate=math.sin(1.08*math.pi)
    elif localtime.tm_hour>=14 and localtime.tm_hour<15:
        cate=math.sin(1.17*math.pi)
    elif localtime.tm_hour>=15 and localtime.tm_hour<16:
        cate=math.sin(1.25*math.pi)
    elif localtime.tm_hour>=16 and localtime.tm_hour<17:
        cate=math.sin(1.33*math.pi)
    elif localtime.tm_hour>=17 and localtime.tm_hour<18:
        cate=math.sin(1.42*math.pi)
    elif localtime.tm_hour>=18 and localtime.tm_hour<19:
        cate=0
    elif localtime.tm_hour>=19 and localtime.tm_hour<20:
        cate=math.sin(1.58*math.pi)
    elif localtime.tm_hour>=20 and localtime.tm_hour<21:
        cate=math.sin(1.67*math.pi)
    elif localtime.tm_hour>=21 and localtime.tm_hour<22:
        cate=math.sin(1.75*math.pi)
    elif localtime.tm_hour>=22 and localtime.tm_hour<23:
        cate=math.sin(1.83*math.pi)
    elif localtime.tm_hour>=23 and localtime.tm_hour<24:
        cate=math.sin(1.92*math.pi)
    return cate

# w=Workbook('iptime.xlsx')
# ws=w.add_worksheet("iptime")
conn=pymongo.Connection('172.16.168.45',27017)
iae_hitlog_new=conn.gehua.iae_hitlog_new
inter_userinfo=conn.gehua.inter_userinfo
inter_userinfo.create_index("caid")
print 12
index = iae_hitlog_new.create_index("ip")
print 123
ip_as=iae_hitlog_new.find({'date': {'$gt':1416672000000}}).distinct('ip')
# find({"category": "movie"}).distinct("tags");

print 1234
ind=0
rowNumber=0
sum=0
mysqlconn1=MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire")
# mysqlconn1=MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="ire")
# mysqlconn=MySQLdb.connect(host="10.3.3.182",user="root",passwd="",db="demo_vsp_a")

# mysqlconn = MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
mysqlcursor = mysqlconn1.cursor()
mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS dap_persona_ori(
    pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, ip VARCHAR(50), starttime VARCHAR(50), length VARCHAR(50), costime VARCHAR(50),
    sintime VARCHAR(50), program_type VARCHAR(50), caid VARCHAR(50), district VARCHAR(50), cutornot VARCHAR(50),status VARCHAR(50)) charset=utf8
    ''' )


resource=list()
caid=list()


mysqlcursor.execute('update dap_persona_ori set status=1 ;')
print 1
for ip_a in ip_as:
    cutornot=False
    timelist=list()
    caid=list()
    resource=list()
    length=0
    print ind
    if ind>1000:
         break
    ind=ind+1
    if ip_a.startswith("172.16"):
        continue
    lines=iae_hitlog_new.find({'ip':ip_a}).sort('date',pymongo.ASCENDING)
    starttime=lines[0]['date']

    localtime=time.localtime(int(starttime/1000))

 #get caid and resource
    for line in lines:

        timelist.append(line['date'])
        #interval is larger than 2 hours
        if len(timelist)>2 and line.get('previous').get('action').get('rp1')=="portal" and timelist[-1]-timelist[-2]>2*60*60*1000:
                #cutornot states if there is cutting existing
            cutornot=True
            lastasfirst=timelist[-1]
            length=timelist[-2]-timelist[0]
            timelist=[]
            timelist.append(lastasfirst)

        else:
            # cutornot=False is unnecessary, coz it would cover the historical record
            length1=timelist[-1]-timelist[0]
            # print "ip_a",ip_a,"length1",length1,"line['date']",line['date']


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
        if line.get('resource').get('parameter').get('CAID') is not None:
            caid.append(line.get('resource').get('parameter').get('CAID'))
        if line.get('previous').get('parameter').get('smid') is not None:
            caid.append(line.get('previous').get('parameter').get('smid'))
    try:
        onecaid=caid[0]
    except:
        onecaid=''




#get district


        # if(len(caid)>0):
    res1=inter_userinfo.find({'caid':onecaid})
    # districtcode=0
    for ress1 in res1:
        addressdistrict=ress1['addressdistrict']
        districtcode=0
        if cmp(addressdistrict,'东城区'.decode("utf8"))==0:
            districtcode+=19
        if cmp(addressdistrict,'石景山区'.decode("utf8"))==0:
            districtcode+=18
        if cmp(addressdistrict,'朝阳区'.decode("utf8"))==0:
            districtcode+=17
        if cmp(addressdistrict,'海淀区'.decode("utf8"))==0:
            districtcode+=16
        if cmp(addressdistrict,'崇文区'.decode("utf8"))==0:
            districtcode+=15
        if cmp(addressdistrict,'宣武区'.decode("utf8"))==0:
            districtcode+=14
        if cmp(addressdistrict,'西城区'.decode("utf8"))==0:
            districtcode+=13
        if cmp(addressdistrict,'丰台区'.decode("utf8"))==0:
            districtcode+=12
        if cmp(addressdistrict,'门头沟区'.decode("utf8"))==0:
            districtcode+=11
        if cmp(addressdistrict,'房山区'.decode("utf8"))==0:
            districtcode+=10
        if cmp(addressdistrict,'大兴区'.decode("utf8"))==0:
            districtcode+=9
        if cmp(addressdistrict,'通州区'.decode("utf8"))==0:
            districtcode+=8
        if cmp(addressdistrict,'顺义区'.decode("utf8"))==0:
            districtcode+=7
        if cmp(addressdistrict,'平顶山区'.decode("utf8"))==0:
            districtcode+=6
        if cmp(addressdistrict,'昌平区'.decode("utf8"))==0:
            districtcode+=5
        if cmp(addressdistrict,'延庆区'.decode("utf8"))==0:
            districtcode+=4
        if cmp(addressdistrict,'怀柔区'.decode("utf8"))==0:
            districtcode+=3
        if cmp(addressdistrict,'密云区'.decode("utf8"))==0:
            districtcode+=2
        if cmp(addressdistrict,'V区'.decode("utf8"))==0:
            districtcode+=1


#get program type
    programtype=list()
    if(len(resource)>0):
        for oneresource in resource:
            mysqlconn2=MySQLdb.connect(host="10.3.3.182",user="root",passwd="",db="demo_vsp_a")
            mysqlcursor2 = mysqlconn2.cursor()
            mysqlcursor2.execute("""select parent_id from element_info where extra_1=%s""",(oneresource))
            parent_id=mysqlcursor2.fetchone()
            if parent_id!=None:
                parent_id = parent_id[0]
                if int(parent_id)>=10002433:
                    mysqlcursor2.execute("""select parent_id from catalog_info where id=%s""",(parent_id))
                    parent_id=mysqlcursor2.fetchone()
                    if parent_id is not None:
                        parent_id = parent_id[0]
                    else:
                        parent_id = -1
                if parent_id not in programtype and parent_id!=-1:
                    programtype.append(parent_id)

        mysqlconn2.close()
        typecode=0
        sum=0
        for onetype in programtype:
            #非剧集情感
            if int(onetype)==10002340:
                typecode+=100000000000000000000
            if int(onetype)==10002194:
                typecode+=100000000000000000000
            if int(onetype)==10002231:
                typecode+=100000000000000000000
            #非剧集动作
            if int(onetype)==10002341:
                typecode+=10000000000000000000
            if int(onetype)==10002195:
                typecode+=10000000000000000000
            if int(onetype)==10002232:
                typecode+=10000000000000000000
            #非剧集喜剧
            if int(onetype)==10002342:
                typecode+=1000000000000000000
            if int(onetype)==10002196:
                typecode+=1000000000000000000
            if int(onetype)==10002233:
                typecode+=1000000000000000000
            #非剧集科幻
            if int(onetype)==10002344:
                typecode+=100000000000000000
            if int(onetype)==10002198:
                typecode+=100000000000000000
            if int(onetype)==10002234:
                typecode+=100000000000000000
            #非剧集惊悚
            if int(onetype)==10002345:
                typecode+=10000000000000000
            if int(onetype)==10002199:
                typecode+=10000000000000000
            if int(onetype)==10002235:
                typecode+=10000000000000000
            #非剧集战争
            if int(onetype)==10002346:
                typecode+=1000000000000000
            if int(onetype)==10002120   :
                typecode+=1000000000000000
            if int(onetype)==10002236:
                typecode+=1000000000000000
            # 剧集情感
            if int(onetype)==10002348:
                typecode+=100000000000000
            if int(onetype)==10002202:
                typecode+=100000000000000
            if int(onetype)==10002238:
                typecode+=100000000000000
            # 剧集日韩
            if int(onetype)==10002349:
                typecode+=10000000000000
            if int(onetype)==10002203:
                typecode+=10000000000000
            if int(onetype)==10002239:
                typecode+=10000000000000
            # 剧集港台
            if int(onetype)==10002350:
                typecode+=1000000000000
            if int(onetype)==10002204:
                typecode+=1000000000000
            if int(onetype)==10002240:
                typecode+=1000000000000
            # 剧集军旅
            if int(onetype)==10002351:
                typecode+=100000000000
            if int(onetype)==10002205:
                typecode+=100000000000
            if int(onetype)==10002241:
                typecode+=100000000000
            # 剧集悬疑
            if int(onetype)==10002352:
                typecode+=10000000000
            if int(onetype)==10002206:
                typecode+=10000000000
            if int(onetype)==10002242:
                typecode+=10000000000
            # 剧集历史
            if int(onetype)==10002353:
                typecode+=1000000000
            if int(onetype)==10002207:
                typecode+=1000000000
            if int(onetype)==10002243:
                typecode+=1000000000
            # 剧集古装
            if int(onetype)==10002354:
                typecode+=100000000
            if int(onetype)==10002208:
                typecode+=100000000
            if int(onetype)==10002244:
                typecode+=100000000
            # 非剧集少儿-中
            if int(onetype)==10002357:
                typecode+=10000000
            if int(onetype)==10002211:
                typecode+=10000000
            if int(onetype)==10002247:
                typecode+=10000000
            # 非剧集少儿-日韩
            if int(onetype)==10002358:
                typecode+=1000000
            if int(onetype)==10002212:
                typecode+=1000000
            if int(onetype)==10002248:
                typecode+=1000000
            # 非剧集少儿-欧美
            if int(onetype)==10002359:
                typecode+=100000
            if int(onetype)==10002213:
                typecode+=100000
            if int(onetype)==10002249:
                typecode+=100000
            # 非剧集纪录-旅游
            if int(onetype)==10002363:
                typecode+=10000
            if int(onetype)==10002217:
                typecode+=10000
            if int(onetype)==10002253:
                typecode+=10000
            # 非剧集纪录-探秘
            if int(onetype)==10002364:
                typecode+=1000
            if int(onetype)==10002218:
                typecode+=1000
            if int(onetype)==10002254:
                typecode+=1000
            # 非剧集记录-自然
            if int(onetype)==10002365:
                typecode+=100
            if int(onetype)==10002219:
                typecode+=100
            if int(onetype)==10002255:
                typecode+=100
            # 非剧集纪录-人文
            if int(onetype)==10002366:
                typecode+=10
            if int(onetype)==10002320:
                typecode+=10
            if int(onetype)==10002256:
                typecode+=10
            # 非剧集纪录-科技
            if int(onetype)==10002367:
                typecode+=1
            if int(onetype)==10002219:
                typecode+=1
            if int(onetype)==10002257:
                typecode+=1
        sum+=typecode


    #insert into mysql
    tempInsert = list()
    #pk
    tempInsert.append('')
    #ip
    tempInsert.append(str(ip_a))
    #starttime
    tempInsert.append(str(starttime))

    #length
    if cutornot == True:
        tempInsert.append(str(length))
    else:
        tempInsert.append(str(length1))
    #costime
    tempInsert.append(str(timepattern(time.localtime(timelist[0]/1000))))
    #sintime
    tempInsert.append(str(timepattern2(time.localtime(timelist[0]/1000))))
    #program type
    tempInsert.append(str(sum))
    #caid
    tempInsert.append(str(onecaid))
    #district
    tempInsert.append(str(districtcode))
    #cutornot
    if cutornot == True:
        tempInsert.append('1')
    else:
        tempInsert.append('0')
    #status
    tempInsert.append('0')

    mysqlcursor.execute("insert into dap_persona_ori(pk, ip, starttime, length, costime, sintime, program_type, caid, district, cutornot, status) values (%s, %s,%s, %s, %s,%s, %s, %s, %s, %s, %s)" , tuple(tempInsert))
    mysqlconn1.commit()
    if cutornot==True:

        a=list(tempInsert)


        a[3]=str(length1)
        a[2]=str(lastasfirst)
        a[4]=str(timepattern(time.localtime(lastasfirst/1000)))

        a[5]=str(timepattern2(time.localtime(lastasfirst/1000)))
        b=tuple(a)
        mysqlcursor.execute("insert into dap_persona_ori(pk, ip, starttime, length, costime, sintime, program_type, caid, district, cutornot, status) values (%s, %s,%s, %s, %s,%s, %s, %s, %s, %s, %s)" , b)
        mysqlconn1.commit()








conn.close()
mysqlconn1.close()