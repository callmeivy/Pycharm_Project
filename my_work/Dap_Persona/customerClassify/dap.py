#coding UTF-8
import math
import MySQLdb
# mysqlconn = MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
mysqlcursor = mysqlconn.cursor()

# data standardization##############################################################################################

'''
# update dap_vod_hshk set length=length/1000
'''
inter = 1
import numpy
mysqlcursor.execute("update dap_vod_dshk_ad set v_costime=round(COS(PI()*vodstarttime/12),2) where beha_type='3' and date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY));" %inter)
mysqlcursor.execute("update dap_vod_dshk_ad set v_sintime=round(SIN(PI()*vodstarttime/12),2) where beha_type='3' and date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY));" %inter)
mysqlcursor.execute("update dap_vod_dshk_ad set d_costime=round(COS(PI()*hitstarttime/12),2) where beha_type='3' and date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY));" %inter)
mysqlcursor.execute("update dap_vod_dshk_ad set d_sintime=round(SIN(PI()*hitstarttime/12),2) where beha_type='3' and date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY));" %inter)
mysqlcursor.execute('''SELECT avg(vodlength), avg(freevodcount), avg(chargevodcount), avg(hitlength), avg(hitcount), avg(AdsVodCount), avg(AdsHitCount), avg(IndexCount) from dap_vod_dshk_ad where beha_type="3" and date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY));''' %inter)
allresult=mysqlcursor.fetchone()
# print allresult
mysqlcursor.execute('''SELECT STD(vodlength), STD(freevodcount), STD(chargevodcount), STD(hitlength), STD(hitcount), STD(AdsVodCount), STD(AdsHitCount), STD(IndexCount) from dap_vod_dshk_ad where beha_type="3" and date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY));''' %inter)
allresult1=mysqlcursor.fetchone()
# print allresult1
#
#
# # (Decimal('2977.6034'), Decimal('0.6872'), Decimal('0.5181'), Decimal('2286.5794'), Decimal('1.1326'), Decimal('3.4155'), Decimal('0.4221'), Decimal('4.7590'))
# # (2471.1469, 0.6882, 0.8476, 2339.4706, 0.4712, 4.3962, 0.4939, 4.4086)
avg_vodlength = round(allresult[0],2)
print 'avg_vodlength',avg_vodlength
avg_freevodcount = round(allresult[1],2)
avg_chargevodcount=round(allresult[2],2)
avg_hitlength=round(allresult[3],2)
avg_hitcount=round(allresult[4],2)
avg_AdsVodCount=round(allresult[5],2)
print 'avg_AdsVodCount',avg_AdsVodCount
avg_AdsHitCount=round(allresult[6],2)
avg_IndexCount=round(allresult[7],2)
std_vodlength=round(allresult1[0],2)
print 'std_vodlength',std_vodlength
std_freevodcount=round(allresult1[1],2)
std_chargevodcount=round(allresult1[2],2)
std_hitlength=round(allresult1[3],2)
std_hitcount=round(allresult1[4],2)
std_AdsVodCount=round(allresult1[5],2)
print 'std_AdsVodCount',std_AdsVodCount
std_AdsHitCount=round(allresult1[6],2)
std_IndexCount=round(allresult1[7],2)
# print "update dap_vod_dshk_ad set s_vodlength=round((convert(vodlength,DECIMAL)-%f)/%f,2) where beha_type='3'" %(float(avg_vodlength),float(std_vodlength))
# print "update dap_vod_dshk_ad set s_freevodcount=round((convert(freevodcount,DECIMAL)-%f)/%f,2) where beha_type='3'" %(float(avg_freevodcount),float(std_freevodcount))
# print "update dap_vod_dshk_ad set s_chargevodcount=round((convert(chargevodcount,DECIMAL)-%f)/%f,2) where beha_type='3'" %(float(avg_chargevodcount),float(std_chargevodcount))
# print "update dap_vod_dshk_ad set s_hitlength=round((convert(hitlength,DECIMAL)-%f)/%f,2) where beha_type='3'" %(float(avg_hitlength),float(std_hitlength))
# print "update dap_vod_dshk_ad set s_hitcount=round((convert(hitcount,DECIMAL)-%f)/%f,2) where beha_type='3'" %(float(avg_hitcount),float(std_hitcount))
# print "update dap_vod_dshk_ad set s_AdsHitCount=round((convert(AdsHitCount,DECIMAL)-%f)/%f,2) where beha_type='3'" %(float(avg_AdsHitCount),float(std_AdsHitCount))
# print "update dap_vod_dshk_ad set s_IndexCount=round((convert(IndexCount,DECIMAL)-%f)/%f,2) where beha_type='3'" %(float(avg_IndexCount),float(std_IndexCount))
#
# update dap_vod_dshk_ad set s_vodlength=round((convert(vodlength,DECIMAL)-2977.600000)/2471.150000,2) where beha_type='3'
# update dap_vod_dshk_ad set s_freevodcount=round((convert(freevodcount,DECIMAL)-0.690000)/0.690000,2) where beha_type='3'
# update dap_vod_dshk_ad set s_chargevodcount=round((convert(chargevodcount,DECIMAL)-0.520000)/0.850000,2) where beha_type='3'
# update dap_vod_dshk_ad set s_hitlength=round((convert(hitlength,DECIMAL)-2286.580000)/2339.470000,2) where beha_type='3'
# update dap_vod_dshk_ad set s_hitcount=round((convert(hitcount,DECIMAL)-1.130000)/0.470000,2) where beha_type='3'
# update dap_vod_dshk_ad set s_AdsHitCount=round((convert(AdsHitCount,DECIMAL)-0.420000)/0.490000,2) where beha_type='3'
# update dap_vod_dshk_ad set s_IndexCount=round((convert(IndexCount,DECIMAL)-4.760000)/4.410000,2) where beha_type='3'

#where can be the mistake???????
mysqlcursor.execute("update dap_vod_dshk_ad set s_vodlength=round((convert(vodlength,DECIMAL)-%f)/%f,2) where beha_type='3' and date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY));" %(float(avg_vodlength),float(std_vodlength),inter))
mysqlcursor.execute("update dap_vod_dshk_ad set s_freevodcount=round((convert(freevodcount,DECIMAL)-%f)/%f,2) where beha_type='3' and date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY));" %(float(avg_freevodcount),float(std_freevodcount),inter))
mysqlcursor.execute("update dap_vod_dshk_ad set s_chargevodcount=round((convert(chargevodcount,DECIMAL)-%f)/%f,2) where beha_type='3' and date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY));" %(float(avg_chargevodcount),float(std_chargevodcount),inter))
mysqlcursor.execute("update dap_vod_dshk_ad set s_hitlength=round((convert(hitlength,DECIMAL)-%f)/%f,2) where beha_type='3' and date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY));" %(float(avg_hitlength),float(std_hitlength),inter))
mysqlcursor.execute("update dap_vod_dshk_ad set s_hitcount=round((convert(hitcount,DECIMAL)-%f)/%f,2) where beha_type='3' and date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY));" %(float(avg_hitcount),float(std_hitcount),inter))
# update dap_vod_dshk_ad set s_AdsVodCount=round((convert(AdsVodCount,DECIMAL)-3.420000)/4.400000,2) where beha_type='3'
mysqlcursor.execute("update dap_vod_dshk_ad set s_AdsVodCount=round((convert(AdsVodCount,DECIMAL)-%f)/%f,2) where beha_type='3' and date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY));" %(float(avg_AdsVodCount),float(std_AdsVodCount),inter))
mysqlcursor.execute("update dap_vod_dshk_ad set s_AdsHitCount=round((convert(AdsHitCount,DECIMAL)-%f)/%f,2) where beha_type='3' and date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY));" %(float(avg_AdsHitCount),float(std_AdsHitCount),inter))
mysqlcursor.execute("update dap_vod_dshk_ad set s_IndexCount=round((convert(IndexCount,DECIMAL)-%f)/%f,2) where beha_type='3' and date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY));" %(float(avg_IndexCount),float(std_IndexCount),inter))


# classify##############################################################################################

mysqlcursor.execute("SELECT value from weightvalue where name='vodlength'")
vodlength=mysqlcursor.fetchone()
grade_ratio1=vodlength[0]
mysqlcursor.execute("SELECT value from weightvalue where name='freevodcount'")
freevodcount=mysqlcursor.fetchone()
grade_ratio2=freevodcount[0]
mysqlcursor.execute("SELECT value from weightvalue where name='chargevodcount'")
chargevodcount=mysqlcursor.fetchone()
grade_ratio3=chargevodcount[0]
mysqlcursor.execute("SELECT value from weightvalue where name='vod_cos_time'")
vod_cos_time=mysqlcursor.fetchone()
grade_ratio4=vod_cos_time[0]
mysqlcursor.execute("SELECT value from weightvalue where name='vod_sin_time'")
vod_sin_time=mysqlcursor.fetchone()
grade_ratio5=vod_sin_time[0]
mysqlcursor.execute("SELECT value from weightvalue where name='hitlength'")
hitlength=mysqlcursor.fetchone()
grade_ratio6=hitlength[0]
mysqlcursor.execute("SELECT value from weightvalue where name='hitcount2'")
hitcount2=mysqlcursor.fetchone()
grade_ratio7=hitcount2[0]
mysqlcursor.execute("SELECT value from weightvalue where name='hit_cos_time'")
hit_cos_time=mysqlcursor.fetchone()
grade_ratio8=hit_cos_time[0]
mysqlcursor.execute("SELECT value from weightvalue where name='hit_sin_time'")
hit_sin_time=mysqlcursor.fetchone()
grade_ratio9=hit_sin_time[0]
mysqlcursor.execute("SELECT value from weightvalue where name='AdsVodCount'")
AdsVodCount=mysqlcursor.fetchone()
grade_ratio10=AdsVodCount[0]
mysqlcursor.execute("SELECT value from weightvalue where name='AdsHitCount'")
AdsHitCount=mysqlcursor.fetchone()
grade_ratio11=AdsHitCount[0]
mysqlcursor.execute("SELECT value from weightvalue where name='IndexCount'")
IndexCount=mysqlcursor.fetchone()
grade_ratio12=IndexCount[0]

mysqlcursor.execute("update dap_vod_dshk_ad set total_grade=round(s_vodlength*%f+s_freevodcount*%f+s_chargevodcount*%f+v_costime*%f+v_sintime*%f+s_hitlength*%f+s_hitcount*%f+d_costime*%f+d_sintime*%f+s_AdsVodCount*%f+s_AdsHitCount*%f+s_IndexCount*%f,2) where beha_type='3' and date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY));" %(float(grade_ratio1),float(grade_ratio2),float(grade_ratio3),float(grade_ratio4),float(grade_ratio5),float(grade_ratio6),float(grade_ratio7),float(grade_ratio8),float(grade_ratio9),float(grade_ratio10),float(grade_ratio11),float(grade_ratio12),inter))
mysqlconn.commit()
# SELECT * from dap_vod_dshk WHERE length is not null and starttime is not null and vodlength is not null and vodcount is not null and hitcount is not null limit 10
#
#
# ##############################################################################################
# # insert into the mysql column of CLASSIFY2
rank_range=0
quality_ratio1=0.05
quality_ratio2=0.1
quality_ratio3=0.35
quality_ratio4=0.5
mysqlcursor.execute("SELECT count(*) from dap_vod_dshk_ad where beha_type='3' and date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY));" %inter)
count1=mysqlcursor.fetchone()
# 349165
total=count1[0]
print total
num1=int(round(total*quality_ratio1,0))
num2=int(round(total*quality_ratio2,0))
num3=int(round(total*quality_ratio3,0))
num4=int(round(total*quality_ratio4,0))
# 287 574 2008 2869
print "quality_ratio1","quality_ratio2","quality_ratio3","quality_ratio4",num1,num2,num3,num4

# print "update dap_vod_dshk_ad set quality_class= %s where beha_type='3' and total_grade in (SELECT t.total_grade from (select total_grade from dap_vod_dshk_ad order by total_grade DESC limit %s) as t)" %(1,num1)
# print "update dap_vod_dshk_ad set quality_class= %s where beha_type='3' and total_grade in (SELECT t.total_grade from (select total_grade from dap_vod_dshk_ad order by total_grade DESC limit %s,%s) as t)" %(2,num1,num2)
# print "update dap_vod_dshk_ad set quality_class= %s where beha_type='3' and total_grade in (SELECT t.total_grade from (select total_grade from dap_vod_dshk_ad order by total_grade DESC limit %s,%s) as t)" %(3,num1+num2,num3)
# print "update dap_vod_dshk_ad set quality_class= %s where beha_type='3' and total_grade in (SELECT t.total_grade from (select total_grade from dap_vod_dshk_ad order by total_grade DESC limit %s,%s) as t)" %(4,num1+num2+num3,num4)
#
# update dap_vod_dshk_ad set quality_class= '1' where beha_type='3' and total_grade in (SELECT t.total_grade from (select total_grade from dap_vod_dshk_ad order by total_grade DESC limit 17458) as t)
# update dap_vod_dshk_ad set quality_class= '2' where beha_type='3' and total_grade in (SELECT t.total_grade from (select total_grade from dap_vod_dshk_ad order by total_grade DESC limit 17458,34917) as t)
# update dap_vod_dshk_ad set quality_class= '3' where beha_type='3' and total_grade in (SELECT t.total_grade from (select total_grade from dap_vod_dshk_ad order by total_grade DESC limit 52375,122208) as t)
# update dap_vod_dshk_ad set quality_class= '4' where beha_type='3' and total_grade in (SELECT t.total_grade from (select total_grade from dap_vod_dshk_ad order by total_grade DESC limit 174583,174583) as t)


# where can be the mistake???????
# classify="high quality user"
# 5min
mysqlcursor.execute("update dap_vod_dshk_ad set quality_class= %s where beha_type='3' and date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY)) and total_grade in (SELECT t.total_grade from (select total_grade from dap_vod_dshk_ad order by total_grade DESC limit %s) as t)",(1,inter,num1))
mysqlconn.commit()
# classify="potential user"
# 600s
mysqlcursor.execute("update dap_vod_dshk_ad set quality_class= %s where beha_type='3' and date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY)) and total_grade in (SELECT t.total_grade from (select total_grade from dap_vod_dshk_ad order by total_grade DESC limit %s,%s) as t)",(2,inter,num1,num2))
mysqlconn.commit()
# classify="normal user"
# print "update dap_vod_dshk set quality_class= %s where total_grade in (SELECT t.total_grade from (select total_grade from dap_vod_dshk order by total_grade DESC limit %s,%s) as t)" %(3,num1+num2,num3)
mysqlcursor.execute("update dap_vod_dshk_ad set quality_class= %s where beha_type='3' and date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY)) and total_grade in (SELECT t.total_grade from (select total_grade from dap_vod_dshk_ad order by total_grade DESC limit %s,%s) as t)",(3,inter,num1+num2,num3))
mysqlconn.commit()
# classify="low quality user"
mysqlcursor.execute("update dap_vod_dshk_ad set quality_class= %s where beha_type='3' and date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY)) and total_grade in (SELECT t.total_grade from (select total_grade from dap_vod_dshk_ad order by total_grade DESC limit %s,%s) as t)",(4,inter,num1+num2+num3,num4))
#
mysqlconn.commit()






############################################################################################
# insert into the mysql column of CLASSIFY3
rank_range=0
quality_ratio1=0.05
quality_ratio2=0.2
quality_ratio3=0.5
quality_ratio4=0.25

num1=int(round(total*quality_ratio1,0))
num2=int(round(total*quality_ratio2,0))
num3=int(round(total*quality_ratio3,0))
num4=int(round(total*quality_ratio4,0))


classify="super-long online user"
print "update dap_vod_dshk set length_class= %s where beha_type='3' and length in (SELECT t.length from (select length from dap_vod_dshk order by length DESC limit %s) as t)" %(1,num1)
mysqlcursor.execute("update dap_vod_dshk_ad set length_class= %s where beha_type='3' and length in (SELECT t.length from (select length from dap_vod_dshk_ad where beha_type='3' order by length DESC limit %s) as t)",(1,num1))
# classify="potential online user"
mysqlcursor.execute("update dap_vod_dshk_ad set length_class= %s where beha_type='3' and length in (SELECT t.length from (select length from dap_vod_dshk_ad where beha_type='3' order by length DESC limit %s,%s) as t)",(2,num1,num2))
# classify="normal online user"
mysqlcursor.execute("update dap_vod_dshk_ad set length_class= %s where beha_type='3' and length in (SELECT t.length from (select length from dap_vod_dshk_ad where beha_type='3' order by length DESC limit %s,%s) as t)",(3,num1+num2,num3))
# classify="short term online user"
mysqlcursor.execute("update dap_vod_dshk_ad set length_class= %s where beha_type='3' and length in (SELECT t.length from (select length from dap_vod_dshk_ad where beha_type='3' order by length DESC limit %s,%s) as t)",(4,num1+num2+num3,num4))

mysqlconn.commit()
mysqlconn.close()