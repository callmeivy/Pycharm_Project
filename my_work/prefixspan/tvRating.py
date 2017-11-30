# coding=UTF-8
'''
Created on 23 Dec 2014
日到达率
@author: Jin
'''
import pymongo
import time
import datetime
import xlwt
# w=open('E:\\action.txt','w')
# 172.16.168.45
conn=pymongo.Connection('172.16.168.45',27017)
iae_audiencerate_new=conn.gehua.iae_audiencerate_new
iae_hitlog_record=conn.gehua.iae_hitlog_record
w=xlwt.Workbook(encoding = 'utf-8',style_compression=2)
ws=w.add_sheet('date',cell_overwrite_ok=True)
borders = xlwt.Borders()
borders.left = 1
borders.right = 1
borders.top = 1
borders.bottom = 1
borders.bottom_colour=0x3A
style = xlwt.XFStyle()
style.borders = borders
alignment = xlwt.Alignment()
alignment.horz = xlwt.Alignment.HORZ_CENTER
alignment.vert = xlwt.Alignment.VERT_CENTER
style = xlwt.XFStyle()
style.borders = borders
style.alignment = alignment
ws.write(0,0,'date',style)
ws.write(0,1,'date2',style)
ws.write(0,2,'pro_hit',style)
ws.write(0,3,'pro_total',style)
ws.write(0,4,'total_Sample',style)
ws.write(0,5,'tvRating(%)',style)

# ws.write(0,6,'dshk_hit',style)
# ws.write(0,7,'dshk_total',style)
# ws.write(0,8,'total',style)
# ws.write(0,9,'dshkRating(%)',style)

# now = int(time.time())
# timeArray = time.localtime(now)
# otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
# print otherStyleTime

today = datetime.date.today()
rowNumber=1
for i in range (3,4):
    interday = datetime.timedelta(days=i)
    yesterday = today - interday
    print yesterday
    ws.write(rowNumber,0,str(yesterday),style)
    caids=iae_audiencerate_new.find({'WIC.A.p':'%E5%A5%94%E8%B7%91%E5%90%A7%E5%85%84%E5%BC%9F','WIC.date':str(yesterday),'WIC.A.sn':'浙江卫视'}).distinct('WIC.cardNum')

    # caids=iae_audiencerate_new.find({'WIC.A.p':'%E5%A5%94%E8%B7%91%E5%90%A7%E5%85%84%E5%BC%9F','WIC.date':str(yesterday),'WIC.A.t':{'$gt':480}}).distinct('WIC.cardNum')
    pro_hit=len(caids)
    print pro_hit
    ws.write(rowNumber,2,str(pro_hit),style)
    # 5304

    caids2=iae_audiencerate_new.find({'WIC.date':str(yesterday)}).distinct('WIC.cardNum')
    pro_total=len(caids2)
    ws.write(rowNumber,3,str(pro_total),style)
    ws.write(rowNumber,4,str('50000'),style)
    print len(caids2)
    # 33445

    if pro_total!=0:
        TVRating=round(float(pro_hit)/float(50000.00)*100,2)
        print TVRating
        ws.write(rowNumber,5,str(TVRating),style)
    else:
        ws.write(rowNumber,5,str('0'),style)

    # tv rating=0.16


    # ########################hitlog!!!!!##########################
    # caids3=iae_hitlog_record.find({'assetName':'奔跑吧兄弟','date':str(yesterday)}).distinct('caid')
    # # caids3=iae_hitlog_record.find({'assetName':'奔跑吧兄弟','date':str(yesterday),'duration':{'$gt':480.00}}).distinct('caid')
    # dshk_hit=len(caids3)
    # print '11111',dshk_hit
    # ws.write(rowNumber,6,str(dshk_hit),style)
    # # 5052
    #
    #
    #
    # caids4=iae_hitlog_record.find({'date':str(yesterday)}).distinct('caid')
    # dshk_total=len(caids4)
    # ws.write(rowNumber,7,str(dshk_total),style)
    # ws.write(rowNumber,8,str('4000000'),style)
    # # print len(caids4)
    # # 393530
    # if dshk_total!=0:
    #     dshkRating=round(float(dshk_hit)/float(4000000)*100,2)
    #     print dshkRating
    #     ws.write(rowNumber,9,str(dshkRating),style)
    # else:
    #     ws.write(rowNumber,9,str('0'),style)
    # # dshk rating=0.01
    # rowNumber+=1




w.save(r'E:\Rating.xls')
conn.close()