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
createTime = datetime.datetime.now()
# print len(titles),'hahahh'
# print X.shape
# print X.sum()
# n_iter:迭代次数
model = lda.LDA(n_topics=10, n_iter=100, random_state=1)
model.fit(X)  # model.fit_transform(X) is also available
topic_word = model.topic_word_  # model.components_ also works
n_top_words = 8
mysqlconn = MySQLdb.connect(host = '10.3.3.182', user = 'root', passwd ='', db = 'btv', charset = 'utf8')
mysqlcursor = mysqlconn.cursor()
mysqltopic = 'topic_attri'
mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS topic_attri_f(
        pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, topic_number bigint(20), bag_of_words VARCHAR(50), title_involved VARCHAR(20000),topic VARCHAR(255), topic_backup1 VARCHAR(255),
        topic_backup2 VARCHAR(255), number_of_docs bigint(100), popularity bigint(200), date_of_topic DATE, create_date DATETIME) charset=utf8''')
print 'new',model.components_

print "hahah",model.loglikelihood()
print 'biubiu',type(model.fit_transform(X)),model.fit_transform(X)
print len(list(model.fit_transform(X)))
doc_topic_again = list(model.fit_transform(X))
# 各篇文章属于各topic的概率
thefile = open('E:\lda00\\values.txt', 'w+')
for ii in doc_topic_again:
    thefile.write("%s\n" % str(ii).encode('utf-8'))
thefile.close()
tempInsert = list()
for i, topic_dist in enumerate(topic_word):
    topic_words = np.array(vocab)[np.argsort(topic_dist)][:-n_top_words:-1]
    print('Topic {}: {}'.format(i, ' '.join(topic_words)))
    bag_of_words = ' '.join(topic_words)
    tempInsert.append(i)
    tempInsert.append(str(bag_of_words))
    tempInsert.append('')
    tempInsert.append('')
    tempInsert.append('')
    tempInsert.append('')
    tempInsert.append('')
    tempInsert.append('')
    tempInsert.append('')
    tempInsert.append(createTime)
    mysqlcursor.execute("insert into topic_attri_f(topic_number, bag_of_words, title_involved, topic, topic_backup1, topic_backup2, number_of_docs, popularity, date_of_topic, create_date) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", tempInsert)
    tempInsert = list()
    mysqlconn.commit()




title_topic = dict()
doc_topic = model.doc_topic_
# doc_topic[i]是topic号码，titles[i]是每篇文章的标题
print len(titles)
for i in range(len(titles)):
    try:
        print("{} (top topic: {})".format(titles[i], doc_topic[i].argmax())),i
        title_topic[titles[i]] = doc_topic[i].argmax()
        # print doc_topic[i]
        # print("{} (top topic: {})".format(doc_topic[i].argmax(),titles[i]))
    except:
        print "error"

# print title_topic.items()

# 各个topic所含的文章数print出来了
title_box = list()
j_box = list()
for i, j in title_topic.iteritems():
    # print i,j,'blabla'
    j_box.append(j)
j_box_dic = dict(Counter(j_box))
seg_listcount = sorted(j_box_dic.iteritems(), key=lambda e:e[1], reverse=True)
for element in seg_listcount:
    topic_no = element[0]
    how_many  = element[1]
    # print topic_no,how_many
    mysqlcursor.execute("update topic_attri_f set number_of_docs = '%s' where topic_number = '%s';" %(how_many,topic_no))
    mysqlconn.commit()

# for i, j in title_topic.iteritems():
topic_number = 10
for j in range(topic_number):
    title_box = list()
    for i, m in title_topic.iteritems():
        # print i,m,'llllll'
        if j == m:
            # print i,j,m,'here'
            title_box.append(str(i))
    title_box = ','.join(title_box)
    # print 'heyhey!',title_box,j
    mysqlcursor.execute("update topic_attri_f set title_involved = '%s' where topic_number = '%s';" %(title_box,j))
    mysqlconn.commit()
    # title_box = list()
# print title_box,11111111


mysqlconn.close()