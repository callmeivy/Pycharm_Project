#coding UTF-8
import math
import MySQLdb
# mysqlconn = MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
mysqlcursor = mysqlconn.cursor()


#
#
# # apply model#########################
# # beha_type of universal set
#
#

mysqlcursor.execute("update userdailylength set beha_type='0' where vodlength is NULL and hitlength is Not NULL and AdsHitCount is not NULL and IndexCount is not NULL")
mysqlcursor.execute("update userdailylength set beha_type='1' where vodlength is not NULL and hitlength is NULL and AdsHitCount is not NULL and IndexCount is not NULL")
mysqlcursor.execute("update userdailylength set beha_type='2' where vodlength is NULL and hitlength is NULL and AdsHitCount is not NULL and IndexCount is not NULL")
mysqlcursor.execute("update userdailylength set beha_type='3' where vodlength is not NULL and hitlength is not NULL and AdsHitCount is not NULL and IndexCount is not NULL")


mysqlcursor.execute("update userdailylength set beha_type='4' where vodlength is NULL and hitlength is Not NULL and AdsHitCount is NULL and IndexCount is NULL")
mysqlcursor.execute("update userdailylength set beha_type='5' where vodlength is not NULL and hitlength is NULL and AdsHitCount is NULL and IndexCount is NULL")
mysqlcursor.execute("update userdailylength set beha_type='6' where vodlength is not NULL and hitlength is not NULL and AdsHitCount is NULL and IndexCount is NULL")
mysqlcursor.execute("update userdailylength set beha_type='7' where vodlength is NULL and hitlength is NULL and AdsHitCount is NULL and IndexCount is NULL")

mysqlcursor.execute("update userdailylength set beha_type='8' where vodlength is NULL and hitlength is Not NULL and AdsHitCount is NULL and IndexCount is not NULL")
mysqlcursor.execute("update userdailylength set beha_type='9' where vodlength is not NULL and hitlength is NULL and AdsHitCount is NULL and IndexCount is not NULL")
mysqlcursor.execute("update userdailylength set beha_type='10' where vodlength is not NULL and hitlength is not NULL and AdsHitCount is NULL and IndexCount is not NULL")
mysqlcursor.execute("update userdailylength set beha_type='11' where vodlength is NULL and hitlength is NULL and AdsHitCount is NULL and IndexCount is not NULL")

mysqlcursor.execute("update userdailylength set beha_type='12' where vodlength is NULL and hitlength is Not NULL and AdsHitCount is not NULL and IndexCount is NULL")
mysqlcursor.execute("update userdailylength set beha_type='13' where vodlength is not NULL and hitlength is NULL and AdsHitCount is not NULL and IndexCount is NULL")
mysqlcursor.execute("update userdailylength set beha_type='14' where vodlength is not NULL and hitlength is not NULL and AdsHitCount is not NULL and IndexCount is NULL")
mysqlcursor.execute("update userdailylength set beha_type='15' where vodlength is NULL and hitlength is NULL and AdsHitCount is not NULL and IndexCount is NULL")
mysqlconn.commit()
#
#
#
#
# test phrase below
mysqlcursor.execute("Update userdailylength set quality_class = (select classification from user_profile where abs(user_profile.length-round(userdailylength.length/60000,2)) = (select min(abs(round(userdailylength.length/60000,2)-user_profile1.length)) from user_profile user_profile1 ))");
# beha_type='0'
mysqlcursor.execute("Update userdailylength set quality_class = (select classification from user_profile where (pow(user_profile.hitlength-round(userdailylength.hitlength/60,2),2)+pow(user_profile.hitcount-userdailylength.hitcount,2)+pow(user_profile.hitstarttime-userdailylength.hitstarttime,2)+pow(user_profile.AdsVodCount-userdailylength.AdsVodCount,2)+pow(user_profile.AdsHitCount-userdailylength.AdsHitCount,2)+pow(user_profile.IndexCount-userdailylength.IndexCount,2))= (select min(pow(user_profile1.hitlength-round(userdailylength.hitlength/60,2),2)+pow(user_profile1.hitcount-userdailylength.hitcount,2)+pow(user_profile1.hitstarttime-userdailylength.hitstarttime,2)+pow(user_profile1.AdsVodCount-userdailylength.AdsVodCount,2)+pow(user_profile1.AdsHitCount-userdailylength.AdsHitCount,2)+pow(user_profile1.IndexCount-userdailylength.IndexCount,2)) from user_profile user_profile1)) where beha_type='0'");


# beha_type='1'
mysqlcursor.execute("Update userdailylength set quality_class = (select classification from user_profile where (pow(user_profile.vodlength-round(userdailylength.vodlength/60,2),2)+pow(user_profile.freevodcount-userdailylength.freevodcount,2)+pow(user_profile.chargevodcount-userdailylength.chargevodcount,2)+pow(user_profile.vodstarttime-userdailylength.vodstarttime,2)+pow(user_profile.AdsVodCount-userdailylength.AdsVodCount,2)+pow(user_profile.AdsHitCount-userdailylength.AdsHitCount,2)+pow(user_profile.IndexCount-userdailylength.IndexCount,2))= (select min(pow(user_profile1.vodlength-round(userdailylength.vodlength/60,2),2)+pow(user_profile1.freevodcount-userdailylength.freevodcount,2)+pow(user_profile1.chargevodcount-userdailylength.chargevodcount,2)+pow(user_profile1.vodstarttime-userdailylength.vodstarttime,2)+pow(user_profile1.AdsVodCount-userdailylength.AdsVodCount,2)+pow(user_profile1.AdsHitCount-userdailylength.AdsHitCount,2)+pow(user_profile1.IndexCount-userdailylength.IndexCount,2)) from user_profile user_profile1)) where beha_type='1'");


# beha_type='2'
mysqlcursor.execute("Update userdailylength set quality_class = (select classification from user_profile where (pow(user_profile.AdsVodCount-userdailylength.AdsVodCount,2)+pow(user_profile.AdsHitCount-userdailylength.AdsHitCount,2)+pow(user_profile.IndexCount-userdailylength.IndexCount,2))= (select min(pow(user_profile1.AdsVodCount-userdailylength.AdsVodCount,2)+pow(user_profile1.AdsHitCount-userdailylength.AdsHitCount,2)+pow(user_profile1.IndexCount-userdailylength.IndexCount,2)) from user_profile user_profile1)) where beha_type='2'");


# beha_type='3'
mysqlcursor.execute("Update userdailylength set quality_class = (select classification from user_profile where (pow(user_profile.vodlength-round(userdailylength.vodlength/60,2),2)+pow(user_profile.freevodcount-userdailylength.freevodcount,2)+pow(user_profile.chargevodcount-userdailylength.chargevodcount,2)+pow(user_profile.vodstarttime-userdailylength.vodstarttime,2)+pow(user_profile.hitlength-round(userdailylength.hitlength/60,2),2)+pow(user_profile.hitcount-userdailylength.hitcount,2)+pow(user_profile.hitstarttime-userdailylength.hitstarttime,2)+pow(user_profile.AdsVodCount-userdailylength.AdsVodCount,2)+pow(user_profile.AdsHitCount-userdailylength.AdsHitCount,2)+pow(user_profile.IndexCount-userdailylength.IndexCount,2))= (select min(pow(user_profile1.vodlength-round(userdailylength.vodlength/60,2),2)+pow(user_profile1.freevodcount-userdailylength.freevodcount,2)+pow(user_profile1.chargevodcount-userdailylength.chargevodcount,2)+pow(user_profile1.vodstarttime-userdailylength.vodstarttime,2)+pow(user_profile1.hitlength-round(userdailylength.hitlength/60,2),2)+pow(user_profile1.hitcount-userdailylength.hitcount,2)+pow(user_profile1.hitstarttime-userdailylength.hitstarttime,2)+pow(user_profile1.AdsVodCount-userdailylength.AdsVodCount,2)+pow(user_profile1.AdsHitCount-userdailylength.AdsHitCount,2)+pow(user_profile1.IndexCount-userdailylength.IndexCount,2)) from user_profile user_profile1)) where beha_type='3'");


# beha_type='4'
mysqlcursor.execute("Update userdailylength set quality_class = (select classification from user_profile where (pow(user_profile.hitlength-round(userdailylength.hitlength/60,2),2)+pow(user_profile.hitcount-userdailylength.hitcount,2)+pow(user_profile.hitstarttime-userdailylength.hitstarttime,2))= (select min(pow(user_profile1.hitlength-round(userdailylength.hitlength/60,2),2)+pow(user_profile1.hitcount-userdailylength.hitcount,2)+pow(user_profile1.hitstarttime-userdailylength.hitstarttime,2)) from user_profile user_profile1)) where beha_type='4'");


# beha_type='5'
mysqlcursor.execute("Update userdailylength set quality_class = (select classification from user_profile where (pow(user_profile.vodlength-round(userdailylength.vodlength/60,2),2)+pow(user_profile.freevodcount-userdailylength.freevodcount,2)+pow(user_profile.chargevodcount-userdailylength.chargevodcount,2)+pow(user_profile.vodstarttime-userdailylength.vodstarttime,2))= (select min(pow(user_profile1.vodlength-round(userdailylength.vodlength/60,2),2)+pow(user_profile1.freevodcount-userdailylength.freevodcount,2)+pow(user_profile1.chargevodcount-userdailylength.chargevodcount,2)+pow(user_profile1.vodstarttime-userdailylength.vodstarttime,2)) from user_profile user_profile1)) where beha_type='5'");


# beha_type='6'
mysqlcursor.execute("Update userdailylength set quality_class = (select classification from user_profile where (pow(user_profile.vodlength-round(userdailylength.vodlength/60,2),2)+pow(user_profile.freevodcount-userdailylength.freevodcount,2)+pow(user_profile.chargevodcount-userdailylength.chargevodcount,2)+pow(user_profile.vodstarttime-userdailylength.vodstarttime,2)+pow(user_profile.hitlength-round(userdailylength.hitlength/60,2),2)+pow(user_profile.hitcount-userdailylength.hitcount,2)+pow(user_profile.hitstarttime-userdailylength.hitstarttime,2))= (select min(pow(user_profile1.vodlength-round(userdailylength.vodlength/60,2),2)+pow(user_profile1.freevodcount-userdailylength.freevodcount,2)+pow(user_profile1.chargevodcount-userdailylength.chargevodcount,2)+pow(user_profile1.vodstarttime-userdailylength.vodstarttime,2)+pow(user_profile1.hitlength-round(userdailylength.hitlength/60,2),2)+pow(user_profile1.hitcount-userdailylength.hitcount,2)+pow(user_profile1.hitstarttime-userdailylength.hitstarttime,2)) from user_profile user_profile1)) where beha_type='6'");
# 7 all null
mysqlcursor.execute("Update userdailylength set quality_class = (select classification from user_profile where (pow(user_profile.vodlength-round(userdailylength.vodlength/60,2),2)+pow(user_profile.freevodcount-userdailylength.freevodcount,2)+pow(user_profile.chargevodcount-userdailylength.chargevodcount,2)+pow(user_profile.vodstarttime-userdailylength.vodstarttime,2)+pow(user_profile.hitlength-round(userdailylength.hitlength/60,2),2)+pow(user_profile.hitcount-userdailylength.hitcount,2)+pow(user_profile.hitstarttime-userdailylength.hitstarttime,2)+pow(user_profile.AdsVodCount-userdailylength.AdsVodCount,2))= (select min(pow(user_profile1.vodlength-round(userdailylength.vodlength/60,2),2)+pow(user_profile1.freevodcount-userdailylength.freevodcount,2)+pow(user_profile1.chargevodcount-userdailylength.chargevodcount,2)+pow(user_profile1.vodstarttime-userdailylength.vodstarttime,2)+pow(user_profile1.hitlength-round(userdailylength.hitlength/60,2),2)+pow(user_profile1.hitcount-userdailylength.hitcount,2)+pow(user_profile1.hitstarttime-userdailylength.hitstarttime,2)+pow(user_profile1.AdsVodCount-userdailylength.AdsVodCount,2)) from user_profile user_profile1)) where beha_type='7'");
# 8
mysqlcursor.execute("Update userdailylength set quality_class = (select classification from user_profile where (pow(user_profile.hitlength-round(userdailylength.hitlength/60,2),2)+pow(user_profile.hitcount-userdailylength.hitcount,2)+pow(user_profile.hitstarttime-userdailylength.hitstarttime,2)+pow(user_profile.IndexCount-userdailylength.IndexCount,2))= (select min(pow(user_profile1.hitlength-round(userdailylength.hitlength/60,2),2)+pow(user_profile1.hitcount-userdailylength.hitcount,2)+pow(user_profile1.hitstarttime-userdailylength.hitstarttime,2)+pow(user_profile1.IndexCount-userdailylength.IndexCount,2)) from user_profile user_profile1)) where beha_type='8'");
# 9
mysqlcursor.execute("Update userdailylength set quality_class = (select classification from user_profile where (pow(user_profile.vodlength-round(userdailylength.vodlength/60,2),2)+pow(user_profile.freevodcount-userdailylength.freevodcount,2)+pow(user_profile.chargevodcount-userdailylength.chargevodcount,2)+pow(user_profile.vodstarttime-userdailylength.vodstarttime,2)+pow(user_profile.IndexCount-userdailylength.IndexCount,2))= (select min(pow(user_profile1.vodlength-round(userdailylength.vodlength/60,2),2)+pow(user_profile1.freevodcount-userdailylength.freevodcount,2)+pow(user_profile1.chargevodcount-userdailylength.chargevodcount,2)+pow(user_profile1.vodstarttime-userdailylength.vodstarttime,2)+pow(user_profile1.IndexCount-userdailylength.IndexCount,2)) from user_profile user_profile1)) where beha_type='9'");
# # 10
mysqlcursor.execute("Update userdailylength set quality_class = (select classification from user_profile where (pow(user_profile.vodlength-round(userdailylength.vodlength/60,2),2)+pow(user_profile.freevodcount-userdailylength.freevodcount,2)+pow(user_profile.chargevodcount-userdailylength.chargevodcount,2)+pow(user_profile.vodstarttime-userdailylength.vodstarttime,2)+pow(user_profile.hitlength-round(userdailylength.hitlength/60,2),2)+pow(user_profile.hitcount-userdailylength.hitcount,2)+pow(user_profile.hitstarttime-userdailylength.hitstarttime,2)+pow(user_profile.IndexCount-userdailylength.IndexCount,2))= (select min(pow(user_profile1.vodlength-round(userdailylength.vodlength/60,2),2)+pow(user_profile1.freevodcount-userdailylength.freevodcount,2)+pow(user_profile1.chargevodcount-userdailylength.chargevodcount,2)+pow(user_profile1.vodstarttime-userdailylength.vodstarttime,2)+pow(user_profile1.hitlength-round(userdailylength.hitlength/60,2),2)+pow(user_profile1.hitcount-userdailylength.hitcount,2)+pow(user_profile1.hitstarttime-userdailylength.hitstarttime,2)+pow(user_profile1.IndexCount-userdailylength.IndexCount,2)) from user_profile user_profile1)) where beha_type='10'");
# # 11
mysqlcursor.execute("Update userdailylength set quality_class = (select classification from user_profile where (pow(user_profile.IndexCount-userdailylength.IndexCount,2))= (select min(pow(user_profile1.IndexCount-userdailylength.IndexCount,2)) from user_profile user_profile1)) where beha_type='11'");
# # 12
mysqlcursor.execute("Update userdailylength set quality_class = (select classification from user_profile where (pow(user_profile.hitlength-round(userdailylength.hitlength/60,2),2)+pow(user_profile.hitcount-userdailylength.hitcount,2)+pow(user_profile.hitstarttime-userdailylength.hitstarttime,2)+pow(user_profile.AdsVodCount-userdailylength.AdsVodCount,2)+pow(user_profile.AdsHitCount-userdailylength.AdsHitCount,2))= (select min(pow(user_profile1.hitlength-round(userdailylength.hitlength/60,2),2)+pow(user_profile1.hitcount-userdailylength.hitcount,2)+pow(user_profile1.hitstarttime-userdailylength.hitstarttime,2)+pow(user_profile1.AdsVodCount-userdailylength.AdsVodCount,2)+pow(user_profile1.AdsHitCount-userdailylength.AdsHitCount,2)) from user_profile user_profile1)) where beha_type='12'");
# 13
mysqlcursor.execute("Update userdailylength set quality_class = (select classification from user_profile where (pow(user_profile.vodlength-round(userdailylength.vodlength/60,2),2)+pow(user_profile.freevodcount-userdailylength.freevodcount,2)+pow(user_profile.chargevodcount-userdailylength.chargevodcount,2)+pow(user_profile.vodstarttime-userdailylength.vodstarttime,2)+pow(user_profile.AdsVodCount-userdailylength.AdsVodCount,2)+pow(user_profile.AdsHitCount-userdailylength.AdsHitCount,2))= (select min(pow(user_profile1.vodlength-round(userdailylength.vodlength/60,2),2)+pow(user_profile1.freevodcount-userdailylength.freevodcount,2)+pow(user_profile1.chargevodcount-userdailylength.chargevodcount,2)+pow(user_profile1.vodstarttime-userdailylength.vodstarttime,2)+pow(user_profile1.AdsVodCount-userdailylength.AdsVodCount,2)+pow(user_profile1.AdsHitCount-userdailylength.AdsHitCount,2)) from user_profile user_profile1)) where beha_type='13'");
# 14
mysqlcursor.execute("Update userdailylength set quality_class = (select classification from user_profile where (pow(user_profile.vodlength-round(userdailylength.vodlength/60,2),2)+pow(user_profile.freevodcount-userdailylength.freevodcount,2)+pow(user_profile.chargevodcount-userdailylength.chargevodcount,2)+pow(user_profile.vodstarttime-userdailylength.vodstarttime,2)+pow(user_profile.hitlength-round(userdailylength.hitlength/60,2),2)+pow(user_profile.hitcount-userdailylength.hitcount,2)+pow(user_profile.hitstarttime-userdailylength.hitstarttime,2)+pow(user_profile.AdsVodCount-userdailylength.AdsVodCount,2)+pow(user_profile.AdsHitCount-userdailylength.AdsHitCount,2))= (select min(pow(user_profile1.vodlength-round(userdailylength.vodlength/60,2),2)+pow(user_profile1.freevodcount-userdailylength.freevodcount,2)+pow(user_profile1.chargevodcount-userdailylength.chargevodcount,2)+pow(user_profile1.vodstarttime-userdailylength.vodstarttime,2)+pow(user_profile1.hitlength-round(userdailylength.hitlength/60,2),2)+pow(user_profile1.hitcount-userdailylength.hitcount,2)+pow(user_profile1.hitstarttime-userdailylength.hitstarttime,2)+pow(user_profile1.AdsVodCount-userdailylength.AdsVodCount,2)+pow(user_profile1.AdsHitCount-userdailylength.AdsHitCount,2)) from user_profile user_profile1)) where beha_type='14'");
# 15
mysqlcursor.execute("Update userdailylength set quality_class = (select classification from user_profile where (pow(user_profile.AdsVodCount-userdailylength.AdsVodCount,2)+pow(user_profile.AdsHitCount-userdailylength.AdsHitCount,2))= (select min(pow(user_profile1.AdsVodCount-userdailylength.AdsVodCount,2)+pow(user_profile1.AdsHitCount-userdailylength.AdsHitCount,2)) from user_profile user_profile1)) where beha_type='15'");



mysqlconn.commit()
mysqlconn.close()