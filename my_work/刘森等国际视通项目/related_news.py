#coding:utf-8
'''
Created on 2017-05-10
@author: Ivy(jincan@ctvit.com.cn)
相关新闻获取：首先同时满足前两位的关键词，如果返回值为空，“同时满足”改为“两者满足其一"
'''
import MySQLdb
def related_news(mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'weibo'):
    sqlConn = MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db=dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()
    sqlcursor.execute("select MID, Storyline,key_words from cctv_news_content;")
    alldata = sqlcursor.fetchall()
    for element in alldata:
        if element[2] is None:
            continue
        key_words_list = element[2].split(",")[0:3]
        pk_main_news = element[0]
        # 如果正文内容为空
        if len(key_words_list) == 1:
            continue
        sqlcursor.execute("select MID from cctv_news_content where (Storyline like '%%%s%%' and Storyline like '%%%s%%') and pk <> %s limit 10" %(key_words_list[0],key_words_list[1],pk_main_news))
        related = str(list(sqlcursor.fetchall()))
        if len(related) == 2:
            sqlcursor.execute(
                "select MID from cctv_news_content where (Storyline like '%%%s%%' or Storyline like '%%%s%%') and pk <> %s limit 5" % (
                key_words_list[0], key_words_list[1], pk_main_news))
            related = str(list(sqlcursor.fetchall()))
        related = related.replace("(","")
        related = related.replace(",)","")
        related = related.replace("[","")
        related = related.replace("]","")
        related = related.replace("'","")
        related = related.replace("u","")
        sqlcursor.execute("update cctv_news_content set related_news = %s where MID = %s", (related,pk_main_news))
        sqlConn.commit()
    sqlConn.close()


if __name__ == '__main__':
    key_words = related_news(mysqlhostIP='192.168.168.105', dbname='weibo')
