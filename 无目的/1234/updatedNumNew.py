# coding=UTF-8
import pymongo
import MySQLdb
mysqlconn = MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="gehua_mysql", charset='utf8')
mysqlcursor = mysqlconn.cursor()
mysqlcursor.execute('''select parent_id from element_info where type=%s and (str(create_time)[0:10])=%s''',('2','2014-10-17'))
dis_date=mysqlcursor.fetchone()
print dis_date