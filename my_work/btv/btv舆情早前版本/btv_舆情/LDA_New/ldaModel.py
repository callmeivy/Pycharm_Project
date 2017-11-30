#coding=UTF-8

import numpy as np
import lda
import datetime
import lda.datasets
from collections import Counter
import MySQLdb
import sys,os
reload(sys)
sys.setdefaultencoding('utf8')
import json
import base64
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import requests

def issuccessful(request):
    if 200 <= request.status_code and request.status_code <= 299:
        return True
    else:
        return False


def excuteldamodel(baseurl,mysqlhostIP, how_many_topics, how_many_iteration, how_many_topic_words, mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv_v2'):
    X = lda.datasets.load_reuters()
    vocab = lda.datasets.load_reuters_vocab()
    titles = lda.datasets.load_reuters_titles()
    title_no = dict()
    # 将文章标题与序号存成字典
    for i in range(len(titles)):
        title_no[i] = titles[i]
    createTime = datetime.datetime.now()
    model = lda.LDA(n_topics = how_many_topics, n_iter = how_many_iteration, random_state = 1)
    model.fit(X)  # model.fit_transform(X) is also available
    topic_word = model.topic_word_  # model.components_ also works
    print 'here',type(topic_word),topic_word
    mysqlconn = MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    mysqlcursor = mysqlconn.cursor()
    mysqltopic = 'topic_attri'
    mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS top_topic_trend(
            pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, topic varchar(50), key_words VARCHAR(50), date DATE,popularity bigint(20), created_date datetime,
            topic_id VARCHAR(200)) charset=utf8''')

    mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS topic_populaity(
            pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, topic varchar(50), key_words VARCHAR(50), date DATE, popularity bigint(20), rank bigint(20), pic VARCHAR(250),\
             created_date datetime, topic_id VARCHAR(250), program_id varchar(50)) charset=utf8''')

    mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS related_news(
            pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, news_title varchar(200), source VARCHAR(200), date datetime, news_content varchar(200), popularity bigint(20), created_time datetime, topic_id VARCHAR(200),\
             region VARCHAR(200), url VARCHAR(200)) charset=utf8''')
    # mysqlcursor.execute('''delete from topic_populaity;''')
    # mysqlcursor.execute('''delete from top_topic_trend;''')
    # mysqlcursor.execute('''delete from related_news;''')




# 以下是各个主题下新闻的列表
    title_topic = dict()
    doc_topic = model.doc_topic_
    def isValid(position, rate):
        topicRate = doc_topic[position]
        valid = False
        for i in range(len(topicRate)):
            if topicRate[i] > rate:
                valid = True
                break
        return valid
    for i in range(len(titles)):
    #    print("{} (top topic: {})".format(titles[i], doc_topic[i].argmax()))
        if isValid(i, 0.5): # zuchuanlong --- add
            title_topic[titles[i]] = doc_topic[i].argmax()
        # print doc_topic[i]
        print("{} (top topic: {})".format(doc_topic[i].argmax(),titles[i]))

    tempInsert_topic_title = list()
    # for j in range(how_many_topics):
    title_box = list()
    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    # 表名
    tablename = "DATA:NEWS_Content"
    r = requests.get(baseurl + "/" + tablename + "/*",  auth=kerberos_auth, headers = {"Accept" : "application/json"})
    if issuccessful(r) == False:
        print "Could not get messages from HBase. Text was:\n" + r.text
    # quit()
    bleats = json.loads(r.text)
    already_title = list()

    for i, m in title_topic.iteritems():
            # bleats is json file
        for row in bleats['Row']:
            flag = True
            for cell in row['Cell']:
                columnname = base64.b64decode(cell['column'])
                value = cell['$']
                if value == None:
                    print 'none'
                    continue
                if columnname == "base_info:title":
                    title = base64.b64decode(value)
                    if title != str(i):
                        flag = False
                        break
                # 因为有标题做判断了，没必要再判断日期了
                if columnname == "base_info:news_from":
                    source = base64.b64decode(value)
                if columnname == "base_info:datetime":
                    date_time = base64.b64decode(value)
                    if " 2016" in date_time:
                        date_time = date_time[3:]
                    if ('年' in date_time):
                        date_time = date_time.replace('年','-')
                    if ('月' in date_time):
                        date_time =date_time.replace('月','-')
                    if ('日' in date_time):
                        date_time =date_time.replace('日',' ')
                    if len(date_time) == 16:
                        date_time = date_time + ':00'
                    if len(date_time) == 18:
                        date_time = date_time[:10] + ' ' + date_time[10:19]

                    if len(date_time) == 10:
                        date_time = date_time + ' 08:00:00'
                    if len(date_time) == 8:
                        continue
                    if len(date_time) == 22:
                        date_time = str(date_time)[0:10] + ' ' + str(date_time)[11:19]
                if columnname == "body:content":
                    content = base64.b64decode(value)
                if columnname == "base_info:website":
                    url = base64.b64decode(value)
                if columnname == "base_info:comment_count":
                    comment_count = base64.b64decode(value)
                # click_number = data['base_info:website']
            if flag:
                # 新闻标题
                if title not in already_title:
                    already_title.append(title)
                    tempInsert_topic_title.append(str(i))
                    # source???
                    tempInsert_topic_title.append(source)
                    # 新闻的date
                    tempInsert_topic_title.append(date_time)
                    # 新闻正文
                    tempInsert_topic_title.append(content)
                    # 单篇新闻的热度？？？
                    # tempInsert_topic_title.append(click_number)
                    tempInsert_topic_title.append(comment_count)
                    # 新闻插入表格的时间
                    tempInsert_topic_title.append(createTime)
                    # topicId,即topic序号
                    tempInsert_topic_title.append(str(m))
                    # region应该是没有用的
                    tempInsert_topic_title.append('')
                    # url
                    tempInsert_topic_title.append(url)
                    mysqlcursor.execute("insert into related_news(news_title, source, date, news_content, popularity, created_time, topic_id, region, url) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)", tempInsert_topic_title)
                    tempInsert_topic_title = []
                    mysqlconn.commit()



    # 以下是主题列表，主要计算主题热度等
    doc_topic_probability = model.fit_transform(X)
    # 获取矩阵每列最大元素的Index,每个主题最符合的文章
    max_title_index = doc_topic_probability.argmax(axis=0)
    doc_topic_again = list(model.fit_transform(X))
    # 各篇文章属于各topic的概率
    thefile = open('values.txt', 'w+')
    for ii in doc_topic_again:
        thefile.write("%s\n" % str(ii).encode('utf-8'))
    thefile.close()
    tempInsert = list()
    topic_dict = dict()
    for i, topic_dist in enumerate(topic_word):
        topic_words = np.array(vocab)[np.argsort(topic_dist)][:-how_many_topic_words:-1]
        # print('Topic {}: {}'.format(i, ' '.join(topic_words)))
        bag_of_words = ' '.join(topic_words)
        topic_dict[i] = (str(bag_of_words))
        # topic,其实为最能代表该topic的文章代码
        number = max_title_index[i]
        title_utmost = title_no[number]
        tempInsert.append(str(title_utmost))
        # key_words,其实是bag_of_words
        tempInsert.append(str(bag_of_words))
        # 文章的date
        mysqlcursor.execute("select date from related_news where topic_id = %s limit 1", str(i))
        bufferTemp = mysqlcursor.fetchone()
        news_date = str(bufferTemp[0]).split(' ')[0]
        print 'hah',news_date
        tempInsert.append(news_date)
        # 热度，基于click_number以及评论number
        mysqlcursor.execute("select sum(popularity) from related_news where topic_id = %s", str(i))
        bufferTemp = mysqlcursor.fetchone()
        topic_popularity = bufferTemp[0]
        tempInsert.append(topic_popularity)
        # 数据插入时间
        tempInsert.append(createTime)
        # topic number
        tempInsert.append(i)
        mysqlcursor.execute("insert into top_topic_trend(topic, key_words, date, popularity, created_date, topic_id) values (%s, %s, %s, %s, %s, %s)", tempInsert)
        tempInsert = list()
        mysqlconn.commit()

    mysqlcursor.execute ("insert into topic_populaity (topic, key_words, date, popularity, created_date, topic_id) select topic, key_words, date, popularity, created_date, topic_id from top_topic_trend")
    mysqlcursor.execute ("update topic_populaity set program_id = '-3413556768156676966';")
    mysqlconn.close()

if __name__=='__main__':
    # how_many_topics,how_many_iteration是数值型
    excuteldamodel(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', how_many_topics = 10, how_many_iteration = 100, how_many_topic_words = 8, dbname = 'btv_v2')