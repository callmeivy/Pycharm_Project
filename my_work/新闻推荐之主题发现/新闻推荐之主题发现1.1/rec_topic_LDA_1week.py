#coding=UTF-8
'''
Updated on 6 June,2017
北京台新闻推荐部分
@author: Ivy
为LDA准备reuters.tokens、tokens.txt、list_of_lists.txt、reuters.titles：
tokens文档在root_directory_lda目录下
reuters.titles在root_directory_lda目录下
list_of_lists在/usr/jincan目录下
'''
import os
import os.path
import jieba
import MySQLdb
from sys import path
path.append('tools/')
path.append('/usr/local/lib/python2.7/site-packages/lda/tests/')
path.append(path[0]+'/tools')
from collections import Counter
from json import JSONDecoder
import sys
import shutil
import time
import math
import string
import nltk
from nltk.corpus import stopwords
default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)
root_directory = 'D:\\tmp'
root_directory_lda = 'C:\Python27\Lib\site-packages\lda\\tests'
title_box = list()
import re
import json
import cPickle
import lda
import datetime
from collections import defaultdict
import numpy as np
from nltk.corpus import stopwords
import textmining

def corpus_to_list():

    # # category = ["weather", "conflicts, war and peace", "sport", "society", "science and technology", "religion and belief", "politics", "lifestyle and leisure", "labour", "human interest", "health", "environment", "education", "economy, business and finance", "disaster and accident", "crime, law and justice", "arts, culture and entertainment"]
    category = ["economy, business and finance"]
    # sqlConn = MySQLdb.connect(host='192.168.168.105', user='root', passwd='', db='weibo', charset='utf8')
    sqlConn = MySQLdb.connect(host='192.168.168.105', user='root', passwd='', db='cctv', charset='utf8')
    sqlcursor = sqlConn.cursor()
    tdm = textmining.TermDocumentMatrix()
    delset = string.punctuation
    for one_type in category:
        print one_type
        word_box = list()
        # sqlcursor.execute("SELECT PubTitle,Storyline from cctv_news_content where tname like '%%%s%%' and (ScriptIssTime between '2017-01-16 00:00:00' and '2017-01-22 23:59:59')" %(one_type,))
        # sqlcursor.execute("SELECT PubTitle,Storyline from cctv_news_content where tname like '%%%s%%' and (ScriptIssTime between '2017-01-01 00:00:00' and '2017-01-07 23:59:59')" %(one_type,))
        sqlcursor.execute("SELECT PubTitle,Storyline from cctv_news_content where tname like '%%%s%%' and (ScriptIssTime between '2017-01-15 00:00:00' and '2017-01-17 23:59:59')" %(one_type,))
        # sqlcursor.execute("SELECT PubTitle,Storyline from cctv_news_content where MID = '491051';")
        traindata = list(sqlcursor.fetchall())
        ind = 0
        print len(traindata)
        all_docs_to_lists = list()
        allDoc_coma_join_lists = list()
        title_box =list()
        tdms = list()
        for PubTitle,Storyline in traindata:
            word_box_single = list()
            if PubTitle not in title_box:
                ind += 1
                title_box.append(PubTitle)
                full_text = str(Storyline) + str(PubTitle)
                full_text = full_text.replace("\n", "")
                full_text = full_text.translate(None, delset)
                # tdm.add_doc(full_text)
                full_text = full_text.split(" ")
                # print PubTitle
                # full_text = nltk.word_tokenize(str(Storyline) + str(PubTitle))
                # print Storyline
                # full_text = nltk.word_tokenize(str(Storyline).decode('utf8'))
                # full_text = nltk.word_tokenize(str(Storyline))
                # full_text = nltk.pos_tag(full_text)
                for i in full_text:
                    if len(i) > 1:
                        if (i != "keywords") and (i != "title") and (i != "content") and (i != "description") and (i != "time") and (len(i) != 8):
                            if i not in stopwords.words('english'):
                                # if chara in valid:
                                word_box.append(i)
                                word_box_single.append(i)
                word_box_str = ','.join(word_box_single)
                all_docs_to_lists.append(word_box_single)
                allDoc_coma_join_lists.append(word_box_str)

        print '总共多少篇', ind, len(title_box)
        # 以下准备reuters.titles
        title_file = open(root_directory_lda+'/reuters.titles', 'w+')
        mark3 = 0
        # docs_total = ind

        if len(title_box) ==0:
            # print("{} (top topic: {})".format(doc_topic[i].argmax(), titles[i]))
            print ("{} (category has NO news.)".format(one_type))
            continue
        for one_title in title_box:
            mark3 += 1
            if mark3 != len(title_box):
                title_file.write("%s\n" % str(one_title).encode('utf-8'))
            else:
                title_file.write("%s" % str(one_title).encode('utf-8'))
        title_file.close()
        print 'reuters.titles is ready-----------------------------------------------------------'

        # 以下准备tokens
        # ffile = open(root_directory_lda+'/reuters.tokens', 'w+')
        # b = list()
        # all_doc_into_list = all_docs_to_lists
        # word_total = len(word_box)
        # listcount = dict(Counter(word_box))
        # listcount = sorted(listcount.iteritems(), key=lambda e:e[1], reverse=True)
        # word_tfidf = dict()
        # for i in listcount:
        #     word = i[0]
        #     timesss = i[1]
        #     ind_2 = 0
        #     for j in all_doc_into_list:
        #         if i[0] in j:
        #             ind_2 += 1
        #     word_idf = round(math.log(round(float(ind)/float(ind_2),4)),4)
        #     word_df = round(float(i[1])/float(word_total),4)
        #     word_tf_idf = word_idf * word_df
        #     word_tfidf[i[0]] = word_tf_idf
        # word_tfidf = sorted(word_tfidf.iteritems(), key=lambda e:e[1], reverse=True)
        # count = 0
        # how_many = 150
        # for iii in word_tfidf:
        #     if len(iii[0]) >0:
        #         if count != how_many-1:
        #             ffile.write("%s\n" % iii[0].encode('utf-8'))
        #         else:
        #             ffile.write("%s" % iii[0].encode('utf-8'))
        #         count += 1
        #     if count > how_many-1:
        #         break
        # print "reuters.tokens is ready----------------------------------------------------------"
        # ffile.close()
        # exit(0)


        list_of_lists_file = open(root_directory+'/list_of_lists.txt', 'w+')
        mark = 0
        print 'len(allDoc_coma_join_lists)',len(allDoc_coma_join_lists)
        for one in allDoc_coma_join_lists:
            mark += 1
            if mark != ind:
                list_of_lists_file.write("%s\n" % one.encode('utf-8'))
            else:
                list_of_lists_file.write("%s" % one.encode('utf-8'))
        list_of_lists_file.close()
        print "mark",mark
        print "list_of_lists.txt is ready"



        execfile('docToMatrix.py')
        execfile('formal_matrix_title.py')
        excuteldamodel(mysqlhostIP='192.168.168.105', how_many_topics = 3, how_many_iteration=100, how_many_topic_words = 15,catcat = one_type,
                       dbname='cctv', )

def excuteldamodel(mysqlhostIP, how_many_topics, how_many_iteration, how_many_topic_words,catcat,mysqlUserName = 'root', mysqlPassword = '', dbname = 'cctv'):
    valid = ['NN', 'NNS', 'NNP', 'NNPS']
    X = lda.datasets.load_reuters()
    vocab = lda.datasets.load_reuters_vocab()
    titles = lda.datasets.load_reuters_titles()
    title_no = dict()
    vv=0
    # 将文章标题与序号存成字典
    for i in range(len(titles)):
        title_no[i] = titles[i]
    createTime = datetime.datetime.now()
    model = lda.LDA(n_topics = how_many_topics, n_iter = how_many_iteration, random_state = 1)
    model.fit(X)  # model.fit_transform(X) is also available
    topic_word = model.topic_word_  # model.components_ also works
    # model.save('E:\lda.model')
    # with open('E:/lda.model', 'wb') as f:
    #     cPickle.dump(model, f)
    mysqlconn = MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    mysqlcursor = mysqlconn.cursor()
    mysqltopic = 'topic_attri'
    mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS top_topic_trend_week(
            pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, category VARCHAR(50), topic_id VARCHAR(200), core_vector varchar(250), key_words VARCHAR(500), news_under_topic text, period varchar(20), created_date datetime) charset=utf8''')
    # mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS topic_populaity_v2(
    #         pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, topic varchar(50), key_words VARCHAR(50), date DATE, popularity bigint(20), rank bigint(20), pic VARCHAR(250),\
    #          created_date datetime, topic_id VARCHAR(250), program_id varchar(50)) charset=utf8''')
    # 添加索引
    # mysqlcursor.execute('''ALTER TABLE top_topic_trend_week ADD INDEX index_name_many(`key_words`, `pk`, `topic_id`, `similar_to_topic_in_summary`, `similarity_degree`)''')
# 以下是各个主题下新闻的列表
    title_topic = defaultdict(list)
    doc_topic = model.doc_topic_
    def isValid(position, rate):
        topicRate = doc_topic[position]
        valid = False
        for i in range(len(topicRate)):
            if topicRate[i] > rate:
                valid = True
                break
        return valid

    count_c = 0
    for i in range(len(titles)):
        # 如果阈值过高，有可能发生某个topic下新闻条数为0的情况
        # 0.7以上，根据最高概率来；0.7以下（未完成)，用户手动判断
        if isValid(i, 0.7):
            print("{} (top topic: {})".format(titles[i], doc_topic[i].argmax()))
            count_c += 1
            title_topic[doc_topic[i].argmax()].append(titles[i])
        # print("{} (top topic: {})".format(doc_topic[i].argmax(),titles[i]))
    tempInsert_topic_title = list()
    title_box = list()
    already_title = list()
    print "heyhey", count_c
    doc_topic_probability = model.fit_transform(X)
    max_title_index = doc_topic_probability.argmax(axis=0)
    doc_topic_again = list(model.fit_transform(X))
    thefile = open('values.txt', 'w+')
    for ii in doc_topic_again:
        thefile.write("%s\n" % str(ii).encode('utf-8'))
    thefile.close()
    tempInsert = list()
    topic_dict = dict()

    for i, topic_dist in enumerate(topic_word):
        # print "hhah", i, topic_dist
        topic_words = np.array(vocab)[np.argsort(topic_dist)][:-how_many_topic_words:-1]
        # print('Topic {}: {}'.format(i, ' '.join(topic_words)))
        bag_of_words = ' '.join(topic_words)
        topic_dict[i] = (str(bag_of_words))
        # topic,最能代表该topic的文章代码
        number = max_title_index[i]
        title_utmost = title_no[number]
        # category
        tempInsert.append(catcat)
        # topic number
        tempInsert.append(i)
        tempInsert.append(str(title_utmost))
        word_list = list()
        tokens = nltk.word_tokenize(bag_of_words)
        tags = nltk.pos_tag(tokens)
        # 词袋只留更有意义的词，像动词形容词啥的都删去了
        for word, flag in tags:
            if flag in valid:
                word_list.append(word)
        bag_of_words = ' '.join(word_list)
        # print "new", bag_of_words
        tempInsert.append(str(bag_of_words))
        # 文章的date
        # 该topic所含的新闻MID
        for k, v in title_topic.items():
            if k ==i:
                news_under_topic = ";".join(v)
                print(" (news under topic{}: {})".format(i, news_under_topic))
                tempInsert.append(news_under_topic)
        # 数据插入时间段
        tempInsert.append('20170115-20170117')
        # 数据插入时间
        tempInsert.append(createTime)
        mysqlcursor.execute("insert into top_topic_trend_week(category,topic_id, core_vector, key_words, news_under_topic, period, created_date) values (%s, %s, %s, %s, %s, %s, %s)", tempInsert)
        tempInsert = list()
        mysqlconn.commit()
    mysqlconn.close()

if __name__=='__main__':
    result = corpus_to_list()