#coding UTF-8
import math
import MySQLdb
# mysqlconn = MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
mysqlcursor = mysqlconn.cursor()


# # feature extraction#########################
# # "high quality user"
mysqlcursor.execute("select quality_class, round(avg(vodlength)/60,2), round(avg(freevodcount),2), round(avg(chargevodcount),2), round(avg(vodstarttime),2), round(avg(hitlength)/60,2), round(avg(hitcount),2), round(avg(hitstarttime),2), round(avg(AdsVodCount),2), round(avg(AdsHitCount),2), round(avg(IndexCount),2)from dap_vod_dshk_ad where beha_type='3' and quality_class='1'")
allresult2=mysqlcursor.fetchone()
#                                 (Decimal('427.12'), Decimal('5.99'),            Decimal('61.59'),          Decimal('2.37'),              Decimal('14.28'),       Decimal('66.30'),           Decimal('1.75'),          Decimal('10.97'))
# print allresult2
mysqlcursor.execute("Insert into user_profile (classification, vodlength, freevodcount, chargevodcount, vodstarttime, hitlength, hitcount, hitstarttime, AdsVodCount, AdsHitCount, IndexCount) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", allresult2)


# # "potential user"
mysqlcursor.execute("select quality_class, round(avg(vodlength)/60,2), round(avg(freevodcount),2), round(avg(chargevodcount),2), round(avg(vodstarttime),2), round(avg(hitlength)/60,2), round(avg(hitcount),2), round(avg(hitstarttime),2), round(avg(AdsVodCount),2), round(avg(AdsHitCount),2), round(avg(IndexCount),2)from dap_vod_dshk_ad where beha_type='3' and quality_class='2'")
allresult3=mysqlcursor.fetchone()
#                                            (Decimal('278.40'),   Decimal('7.53'),            Decimal('53.41'),          Decimal('1.57'),          Decimal('15.59'),           Decimal('51.35'),         Decimal('1.33'),        Decimal('13.50'))
# print allresult3
mysqlcursor.execute("Insert into user_profile (classification, vodlength, freevodcount, chargevodcount, vodstarttime, hitlength, hitcount, hitstarttime, AdsVodCount, AdsHitCount, IndexCount) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", allresult3)
#
#
#
# # "normal user"
mysqlcursor.execute("select quality_class, round(avg(vodlength)/60,2), round(avg(freevodcount),2), round(avg(chargevodcount),2), round(avg(vodstarttime),2), round(avg(hitlength)/60,2), round(avg(hitcount),2), round(avg(hitstarttime),2), round(avg(AdsVodCount),2), round(avg(AdsHitCount),2), round(avg(IndexCount),2)from dap_vod_dshk_ad where beha_type='3' and quality_class='3'")
allresult4=mysqlcursor.fetchone()
#                                 (Decimal('107.20'),     Decimal('10.68'),         Decimal('40.69'),         Decimal('1.23'),          Decimal('17.01'),       Decimal('31.53'),             Decimal('1.13'),           Decimal('16.05'))
# print allresult4
mysqlcursor.execute("Insert into user_profile (classification, vodlength, freevodcount, chargevodcount, vodstarttime, hitlength, hitcount, hitstarttime, AdsVodCount, AdsHitCount, IndexCount) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", allresult4)



# "low quality user"
mysqlcursor.execute("select quality_class, round(avg(vodlength)/60,2), round(avg(freevodcount),2), round(avg(chargevodcount),2), round(avg(vodstarttime),2), round(avg(hitlength)/60,2), round(avg(hitcount),2), round(avg(hitstarttime),2), round(avg(AdsVodCount),2), round(avg(AdsHitCount),2), round(avg(IndexCount),2)from dap_vod_dshk_ad where beha_type='3' and quality_class='4'")
allresult5=mysqlcursor.fetchone()
#                                 (Decimal('51.92'),    Decimal('11.55'),           Decimal('35.95'),          Decimal('1.14'),         Decimal('17.42'),          Decimal('25.22'),         Decimal('1.09'),            Decimal('17.50'))
# print allresult5
mysqlcursor.execute("Insert into user_profile (classification, vodlength, freevodcount, chargevodcount, vodstarttime, hitlength, hitcount, hitstarttime, AdsVodCount, AdsHitCount, IndexCount) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", allresult5)


mysqlconn.commit()

mysqlconn.close()