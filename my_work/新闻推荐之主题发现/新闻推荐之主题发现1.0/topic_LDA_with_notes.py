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
import lda
import datetime
from collections import defaultdict
import numpy as np
inter = 5
now = int(time.time())-86400*inter
timeArray = time.localtime(now)
otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
print otherStyleTime

# def getRidOfStopwords():
#     if os.path.exists(r'/tokens.txt'):
#         os.path.remove(r'/tokens.txt')
#     if os.path.exists(r'/list_of_lists.txt'):
#         os.path.remove(r'/list_of_lists.txt')
#     if os.path.exists(r'/part-00000.txt'):
#         os.path.remove(r'/part-00000.txt')
#     path = os.path.abspath(os.path.dirname(sys.argv[0]))
#     dicFile = open(path+'/tools/NTUSD_simplified/stopwords.txt','r')
#     stopwords = dicFile.readlines()
#     stopwordList = []
#     stopwordList.append(' ')
#     for stopword in stopwords:
#         temp = stopword.strip().replace('\r\n','').decode('utf8')
#         stopwordList.append(temp)
#     dicFile.close()
#     return stopwordList


# doc should be string
def remove_stopwords(doc):
    stop = set(stopwords.words('english'))
    doc_without_stop=(" ").join([i for i in doc.lower().split() if i not in stop])
    return doc_without_stop
# 以下对每篇新闻分词，去停用词后，不去重地存入list，最终以‘，’连接所有词,并且返回多少行，也就是多少篇新闻,
# word_box是所有新闻所有词的大杂烩，all_docs_to_lists是list里面包含list,每篇新闻是一个嵌套的list
def corpus_to_list():
    category = ["politics","weather","sport"]
    sqlConn = MySQLdb.connect(host='192.168.168.105', user='root', passwd='', db='weibo', charset='utf8')
    sqlcursor = sqlConn.cursor()
    for one_type in category:
        print one_type
        word_box = list()
        sqlcursor.execute("SELECT PubTitle,Storyline from cctv_news_content where tname like '%%%s%%' and ScriptCreateTime > '2017-05-05 12:01:26' limit 1000" %(one_type,))
        traindata = list(sqlcursor.fetchall())
        ind = 0
        all_docs_to_lists = list()
        allDoc_coma_join_lists = list()
        title_box =list()
        for PubTitle,Storyline in traindata:
            word_box_single = list()
            print 'ttt',PubTitle,Storyline
            if PubTitle not in title_box:
                ind += 1
                title_box.append(PubTitle)
                full_text = str(Storyline) + str(PubTitle)
                # full_text = jieba.cut(full_text,cut_all = False)
                full_text = remove_stopwords(full_text)
                print "full_text", full_text
                full_text = full_text.split(" ")
                print "full_text2",full_text
                for i in full_text:
                    if len(i) > 1:
                        if (i != "keywords") and (i != "title") and (i != "content") and (i != "description") and (i != "time") and (len(i) != 8):
                            word_box.append(i)
                            word_box_single.append(i)
                # word_box_str将每篇新闻的词袋用“,”做连接，是个str
                word_box_str = ','.join(word_box_single)
                print "word_box_str",word_box_str
                # all_docs_to_lists，每篇文章是一个list,这个list存入到一个大的list当中去
                all_docs_to_lists.append(word_box_single)
            # allDoc_coma_join_lists存储新闻，一篇新闻是一个elemnet,这个element是str,是这篇新闻词用逗号做连接
                allDoc_coma_join_lists.append(word_box_str)


        # 以下准备reuters.titles
        title_file = open(root_directory_lda+'/reuters.titles', 'w+')
        mark = 0
        docs_total = ind
        print '总共多少篇', docs_total,len(title_box)
        for one_title in title_box:
            mark += 1
            if mark != len(title_box):
                title_file.write("%s\n" % str(one_title).encode('utf-8'))
            else:
                title_file.write("%s" % str(one_title).encode('utf-8'))
        title_file.close()
        print 'reuters.titles is ready'
    #以下得到所有文档的固定关键词（其实是一个将多个函数串起来的函数），按照词频排序，可以自定义个数，放到tokens文件中,
    #另外结果存成list，作为传给“变矩阵”代码的list（由list构成的list）的首位元素
    # 以下根据tf-idf获取关键词
    # def get_tokens(hbaseIP):
        ffile = open(root_directory_lda+'/reuters.tokens', 'w+')
        b = list()
        # all_doc_into_list，每篇文章是一个list,所有文章又存成list
        all_doc_into_list = all_docs_to_lists
        # a是所有文档所有词的list
        a = word_box
        word_total = len(a)
        listcount = dict(Counter(a))
        listcount = sorted(listcount.iteritems(), key=lambda e:e[1], reverse=True)
        word_tfidf = dict()
        for i in listcount:
            word = i[0]
            timesss = i[1]
            ind_2 = 0
            for j in all_doc_into_list:
                # print j,word
                if i[0] in j:
                    ind_2 += 1
            # print ind_2
            word_idf = round(math.log(round(float(docs_total)/float(ind_2),4)),4)
            word_df = round(float(i[1])/float(word_total),4)
            word_tf_idf = word_idf * word_df
            word_tfidf[i[0]] = word_tf_idf
        word_tfidf = sorted(word_tfidf.iteritems(), key=lambda e:e[1], reverse=True)
        count = 0
        how_many = 150
        for iii in word_tfidf:
            if count != how_many-1:
                ffile.write("%s\n" % iii[0].encode('utf-8'))
            else:
                ffile.write("%s" % iii[0].encode('utf-8'))
            count += 1
            if count > how_many-1:
                break
        print "reuters.tokens is ready"
        ffile.close()
        # 另外，因为上面改成tokens.txt了，需要再复制一份到reuters.tokens
        # shutil.copy(root_directory_lda+'/reuters.tokens', root_directory_lda+'/tokens.txt')


        # docs_to_lists(hbaseIP):
        list_of_lists_file = open(root_directory+'/list_of_lists.txt', 'w+')
        mark = 0
        # element是一个list,存储多篇新闻，一篇新闻是一个elemnet,这个element是str,是这篇新闻词用逗号做连接
        element = allDoc_coma_join_lists
        docs_total = ind
        print "docs_total",docs_total
        for one in element:
            mark += 1
            if mark != docs_total:
                list_of_lists_file.write("%s\n" % one.encode('utf-8'))
            else:
                # 末行就没有换行符了
                list_of_lists_file.write("%s" % one.encode('utf-8'))
        list_of_lists_file.close()
        print "list_of_lists.txt is ready"

        execfile('docToMatrix.py')
        execfile('formal_matrix_title.py')
        excuteldamodel(mysqlhostIP='192.168.168.105', how_many_topics=10, how_many_iteration=100, how_many_topic_words=8,catcat = one_type,
                       dbname='weibo', )

def excuteldamodel(mysqlhostIP, how_many_topics, how_many_iteration, how_many_topic_words,catcat,mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv_v2'):
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
    mysqlconn = MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    mysqlcursor = mysqlconn.cursor()
    mysqltopic = 'topic_attri'
    mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS top_topic_trend(
            pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, category VARCHAR(50), topic_id VARCHAR(200), core_vector varchar(250), key_words VARCHAR(50), news_under_topic text, created_date datetime) charset=utf8''')

    mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS topic_populaity(
            pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, topic varchar(50), key_words VARCHAR(50), date DATE, popularity bigint(20), rank bigint(20), pic VARCHAR(250),\
             created_date datetime, topic_id VARCHAR(250), program_id varchar(50)) charset=utf8''')


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
    for i in range(len(titles)):
        if isValid(i, 0.5):
            # print "select MID from cctv_news_content where PubTitle like '%%%s%%' limit 1" % (titles[i],)
            if "'" in titles[i]:
                titles_h = (titles[i]).replace("'","\\'")
            else:
                titles_h = titles[i]
            mysqlcursor.execute(
                "select MID from cctv_news_content where PubTitle like '%%%s%%' limit 1" % (titles_h,))
            mid_data = list(mysqlcursor.fetchall())[0][0]
            title_topic[doc_topic[i].argmax()].append(mid_data)
        print("{} (top topic: {})".format(doc_topic[i].argmax(),titles[i]))
    tempInsert_topic_title = list()
    title_box = list()

    sqlConn = MySQLdb.connect(host='192.168.168.105', user='root', passwd='', db='weibo', charset='utf8')
    sqlcursor = sqlConn.cursor()
    already_title = list()

    # 以下是主题列表，主要计算主题热度等title_topic
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
        # category
        tempInsert.append(catcat)
        # topic number
        tempInsert.append(i)
        tempInsert.append(str(title_utmost))
        # key_words,其实是bag_of_words
        tempInsert.append(str(bag_of_words))
        # 文章的date
        # 该topic所含的新闻MID
        for k, v in title_topic.items():
            if k ==i:
                news_under_topic = ",".join(v)
                tempInsert.append(news_under_topic)
        # 数据插入时间
        tempInsert.append(createTime)

        mysqlcursor.execute("insert into top_topic_trend(category,topic_id, core_vector, key_words, news_under_topic, created_date) values (%s, %s, %s, %s, %s, %s)", tempInsert)
        tempInsert = list()
        mysqlconn.commit()

    mysqlconn.close()

if __name__=='__main__':
    result = corpus_to_list()










