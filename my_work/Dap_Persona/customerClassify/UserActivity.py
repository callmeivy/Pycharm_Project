#coding UTF-8
import MySQLdb


inter=1
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')

mysqlcursor = mysqlconn.cursor()

mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS user_activity(
    pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, vodcount int, hitcount int, dailylength int, s_vodcount double,
    s_hitcount double, s_dailylength double, total double, activity_rate VARCHAR(20), date VARCHAR(20), create_date DATETIME) charset=utf8
    ''' )

mysqlcursor.execute('''Delete from user_activity''')


# mysqlcursor.execute('''select sum(vodcount),sum(hitcount),dailyratelength from dailyxordata t1,dailycountdata t2 where t1.date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY) and t2.date = DATE(DATE_SUB(NOW(), INTERVAL %s DAY)''',(inter, inter))
mysqlcursor.execute('''Insert into user_activity(vodcount, hitcount, dailylength, date) select sum(vodcount),sum(hitcount),dailyratelength,t1.date from dailyxordata t1,dailycountdata t2 where vodcount is not null and hitcount is not null and dailyratelength is not null and t1.date = t2.date  group by t1.date;''')
mysqlconn.commit()



mysqlconn.close()