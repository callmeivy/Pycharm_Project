#coding:UTF-8
import MySQLdb
# caculate the freevodcount
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
mysqlcursor = mysqlconn.cursor()
# # apply model#########################
# beha_type of universal set
inter = 1
mysqlcursor.execute("update userdailylength set beha_type='0' where date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY)) and length is null and vodlength is NULL and hitlength is Not NULL;" %inter)
mysqlcursor.execute("update userdailylength set beha_type='1' where date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY)) and length is null and vodlength is not NULL and hitlength is NULL;" %inter)
mysqlcursor.execute("update userdailylength set beha_type='2' where date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY)) and length is null and vodlength is not NULL and hitlength is not NULL;" %inter)
# # please downsize the number of beha_type='3'!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
mysqlcursor.execute("update userdailylength set beha_type='3' where date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY)) and length is not null and vodlength is not NULL and hitlength is not NULL and AdsPicCount is not NULL limit 6000;" %inter)
mysqlcursor.execute("update userdailylength set beha_type='4' where date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY)) and length is not null and vodlength is NULL and hitlength is not NULL;" %inter)
mysqlcursor.execute("update userdailylength set beha_type='5' where date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY)) and length is not null and vodlength is NULL and hitlength is NULL;" %inter)
mysqlcursor.execute("update userdailylength set beha_type='6' where date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY)) and length is not null and vodlength is not NULL and hitlength is NULL;" %inter)
mysqlcursor.execute('''update userdailylength set freevodcount = (length(replace(vodCoulmn, '免费', '免费-')) - length(vodCoulmn)) where date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY)) and vodCoulmn is not null;''' %inter)
mysqlcursor.execute('''update userdailylength set chargevodcount = vodcount - freevodcount where date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY)) and vodcount is not null;''' %inter)


# dap_vod_dshk_ad复制userdailylength一天的数据出来
mysqlcursor.execute('''DROP TABLE IF EXISTS dap_vod_dshk_ad;''' )
mysqlcursor.execute('''create table dap_vod_dshk_ad select * from userdailylength where beha_type='3' and date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY));''' %inter)
mysqlcursor.execute('''alter table dap_vod_dshk_ad add column s_vodlength VARCHAR (20) AFTER vodlength;''' )
mysqlcursor.execute('''alter table dap_vod_dshk_ad add column s_vodcount VARCHAR (20) AFTER vodcount;''' )
mysqlcursor.execute('''alter table dap_vod_dshk_ad add column s_chargevodcount VARCHAR (20) AFTER chargevodcount;''' )
mysqlcursor.execute('''alter table dap_vod_dshk_ad add column v_costime VARCHAR (20) AFTER vodstarttime;''' )
mysqlcursor.execute('''alter table dap_vod_dshk_ad add column s_freevodcount VARCHAR (20) AFTER freevodcount;''' )
mysqlcursor.execute('''alter table dap_vod_dshk_ad add column v_sintime VARCHAR (20) AFTER v_costime;''' )
mysqlcursor.execute('''alter table dap_vod_dshk_ad add column s_hitlength VARCHAR (20) AFTER hitlength;''' )
mysqlcursor.execute('''alter table dap_vod_dshk_ad add column s_hitcount VARCHAR (20) AFTER hitcount;''' )
mysqlcursor.execute('''alter table dap_vod_dshk_ad add column d_costime VARCHAR (20) AFTER hitstarttime;''' )
mysqlcursor.execute('''alter table dap_vod_dshk_ad add column d_sintime VARCHAR (20) AFTER d_costime;''' )
mysqlcursor.execute('''alter table dap_vod_dshk_ad add column s_AdsVodCount VARCHAR (20) AFTER AdsVodCount;''' )
mysqlcursor.execute('''alter table dap_vod_dshk_ad add column s_AdsHitCount VARCHAR (20) AFTER AdsHitCount;''' )
mysqlcursor.execute('''alter table dap_vod_dshk_ad add column s_IndexCount VARCHAR (20) AFTER IndexCount;''' )
mysqlcursor.execute('''alter table dap_vod_dshk_ad add column total_grade VARCHAR (20) AFTER s_IndexCount;''' )
# #


# user_profile
# classification, vodlength, freevodcount, chargevodcount, vodstarttime, hitlength, hitcount, hitstarttime, AdsVodCount, HitVodCount, IndexCount
# 以下只有在新生成表的时候才用，平时可以不用！！！！！！——————————————————————————————
# mysqlcursor.execute('''alter table user_profile add column freevodcount VARCHAR (20) AFTER vodlength;''' )
# mysqlcursor.execute('''alter table user_profile add column chargevodcount VARCHAR (20) AFTER freevodcount;''' )
# mysqlcursor.execute('''alter table user_profile add column VodCount VARCHAR (20) AFTER hitstarttime;''' )
# mysqlcursor.execute('''alter table user_profile add column AdsHitCount VARCHAR (20) AFTER AdsVodCount;''' )
# mysqlcursor.execute('''alter table user_profile add column IndexCount VARCHAR (20) AFTER HitVodCount;''' )



mysqlconn.commit()
mysqlconn.close()