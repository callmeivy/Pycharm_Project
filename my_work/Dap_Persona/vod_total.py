#coding UTF-8
import MySQLdb
import datetime

inter = 1

mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
mysqlcursor = mysqlconn.cursor()

mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS vod_total(
    pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, vod_total VARCHAR(255), date VARCHAR(10)) charset=utf8
    ''')

# for inter in range(1,13):
mysqlcursor.execute("insert into vod_total(vod_total,date) select sum(vodcount),date from dailyxordata where date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY));" %inter)
mysqlconn.commit()


# *************************************************






print "table vod_total success!"









mysqlconn.close()