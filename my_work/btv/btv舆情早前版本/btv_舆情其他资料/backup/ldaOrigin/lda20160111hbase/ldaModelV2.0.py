#coding=UTF-8

import numpy as np
import lda
import datetime
import lda.datasets
from collections import Counter
import MySQLdb

X = lda.datasets.load_reuters()
vocab = lda.datasets.load_reuters_vocab()
titles = lda.datasets.load_reuters_titles()
title_no = dict()
# 将文章标题与序号存成字典
for i in range(len(titles)):
    title_no[i] = titles[i]
createTime = datetime.datetime.now()
# print len(titles),'hahahh'
# print X.shape
# print X.sum()
# n_iter:迭代次数
# 手动指定话题个数
topic_number = 5
model = lda.LDA(n_topics = topic_number, n_iter = 100, random_state = 1)
model.fit(X)  # model.fit_transform(X) is also available
topic_word = model.topic_word_  # model.components_ also works
print 'here',type(topic_word),topic_word
n_top_words = 8

mysqlconn = MySQLdb.connect(host = '10.3.3.182', user = 'root', passwd ='', db = 'btv', charset = 'utf8')
mysqlcursor = mysqlconn.cursor()
mysqltopic = 'topic_attri'
mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS top_topic_trend(
        pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, topic varchar(50), key_words VARCHAR(50), date DATE,popularity bigint(20), created_date datetime,
        topic_id VARCHAR(200)) charset=utf8''')

mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS topic_populaity(
        pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, topic varchar(50), key_words VARCHAR(50), date DATE, popularity bigint(20), rank bigint(20), pic VARCHAR(250),\
         created_date datetime, topic_id VARCHAR(250), program_id varchar(50)) charset=utf8''')

mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS related_news(
        pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, news_title varchar(200), source VARCHAR(200), date datetime, news_content varchar(200), created_time datetime, topic_id VARCHAR(200),\
         region VARCHAR(200), url VARCHAR(200)) charset=utf8''')

print 'new',model.components_

print "hahah",model.loglikelihood()
doc_topic_probability = model.fit_transform(X)
print 'biubiu',type(doc_topic_probability), doc_topic_probability
# 获取矩阵每列最大元素的Index,每个主题最符合的文章
max_title_index = doc_topic_probability.argmax(axis=0)
print 'again', max_title_index
print len(list(model.fit_transform(X)))

doc_topic_again = list(model.fit_transform(X))

# 各篇文章属于各topic的概率
thefile = open('values.txt', 'w+')
for ii in doc_topic_again:
    print 'iiii',ii
    thefile.write("%s\n" % str(ii).encode('utf-8'))
thefile.close()

tempInsert = list()

topic_dict = dict()
for i, topic_dist in enumerate(topic_word):
    print 'hohohoho',i,topic_dist
    topic_words = np.array(vocab)[np.argsort(topic_dist)][:-n_top_words:-1]
    print('Topic {}: {}'.format(i, ' '.join(topic_words)))
    bag_of_words = ' '.join(topic_words)
    topic_dict[i] = (str(bag_of_words))
    # topic,其实为最能代表该topic的文章代码
    number = max_title_index[i]
    title_utmost = title_no[number]
    tempInsert.append(str(title_utmost))
    # key_words,其实是bag_of_words
    tempInsert.append(str(bag_of_words))
    # 文章的date
    tempInsert.append('')
    # 热度，基于click_number以及评论number
    tempInsert.append('')
    # 数据插入时间
    tempInsert.append(createTime)
    # topic number
    tempInsert.append(i)
    mysqlcursor.execute("insert into top_topic_trend(topic, key_words, date, popularity, created_date, topic_id) values (%s, %s, %s, %s, %s, %s)", tempInsert)
    tempInsert = list()
    mysqlconn.commit()

# zuchuanlong --- start
def isValid(position, rate):
    topicRate = doc_topic[position]
    valid = False
    for i in range(len(topicRate)):
        if topicRate[i] > rate:
            valid = True
            break
    return valid
# zuchuanlong --- end

title_topic = dict()
doc_topic = model.doc_topic_
for i in range(len(titles)):
#    print("{} (top topic: {})".format(titles[i], doc_topic[i].argmax()))
    if isValid(i, 0.7): # zuchuanlong --- add
        title_topic[titles[i]] = doc_topic[i].argmax()
    # print doc_topic[i]
    print("{} (top topic: {})".format(doc_topic[i].argmax(),titles[i]))



# 各个topic所含的文章数print出来了
# title_box = list()
# j_box = list()
# for i, j in title_topic.iteritems():
#     print i,j,'blabla'
#     j_box.append(j)
# j_box_dic = dict(Counter(j_box))
# seg_listcount = sorted(j_box_dic.iteritems(), key=lambda e:e[1], reverse=True)
# for element in seg_listcount:
#     topic_no = element[0]
#     how_many  = element[1]
#     # print topic_no,how_many
#     mysqlcursor.execute("update topic_attri set number_of_docs = '%s' where topic_number = '%s';" %(how_many,topic_no))
#     mysqlconn.commit()

# for i, j in title_topic.iteritems():

tempInsert_topic_title = list()
for j in range(topic_number):
    title_box = list()
    for i, m in title_topic.iteritems():
        print 'llllll',i,m
        # 新闻标题
        tempInsert_topic_title.append(str(i))
        # source???
        tempInsert_topic_title.append('')
        # 新闻的date
        tempInsert_topic_title.append('')
        # 新闻正文
        tempInsert_topic_title.append('')
        # 新闻插入表格的时间
        tempInsert_topic_title.append(createTime)
        # topicId,即topic序号
        tempInsert_topic_title.append(str(m))
        # region应该是没有用的
        tempInsert_topic_title.append('')
        # url
        tempInsert_topic_title.append('')
        mysqlcursor.execute("insert into related_news(news_title, source, date, news_content, created_time, topic_id, region, url) values (%s, %s, %s, %s, %s, %s, %s, %s)", tempInsert_topic_title)
        tempInsert_topic_title = list()
        mysqlconn.commit()


mysqlconn.close()