#coding=UTF-8
'''
Updated on 11 Jan,2016
Run with Hbase
Created on 12 Aug, 2015 and 13 Aug, 2015
@author: Ivy
为LDA准备reuters.tokens、tokens.txt、list_of_lists.txt、reuters.titles
3月26日补充
tokens文档在root_directory_lda目录下
reuters.titles在root_directory_lda目录下
list_of_lists在/usr/jincan目录下
'''
import os
import os.path
import jieba
import MySQLdb
import numpy as np
import lda
import datetime
import lda.datasets
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
default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)
# root_directory = '/home/ctvit'
# root_directory = '/usr/local/lib/python2.7/site-packages/lda/tests'
root_directory = '/tmp'
root_directory_lda = '/usr/local/lib/python2.7/site-packages/lda/tests'
title_box = list()
import re
import json
import base64
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import requests



def issuccessful(request):
    if 200 <= request.status_code and request.status_code <= 299:
        return True
    else:
        return False

inter = 5
now = int(time.time())-86400*inter
timeArray = time.localtime(now)
otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
print "5 days ago:", otherStyleTime

inter = 0
today = int(time.time())-86400*inter
timeArray_0 = time.localtime(today)
today_date = time.strftime("%Y-%m-%d", timeArray_0)
print "today:",today_date

def getRidOfStopwords():
    if os.path.exists(r'/tokens.txt'):
        os.path.remove(r'/tokens.txt')
    if os.path.exists(r'/list_of_lists.txt'):
        os.path.remove(r'/list_of_lists.txt')
    if os.path.exists(r'/part-00000.txt'):
        os.path.remove(r'/part-00000.txt')
    path = os.path.abspath(os.path.dirname(sys.argv[0]))
    dicFile = open(path+'/tools/NTUSD_simplified/stopwords.txt','r')
    stopwords = dicFile.readlines()
    stopwordList = []
    stopwordList.append(' ')
    for stopword in stopwords:
        temp = stopword.strip().replace('\r\n','').decode('utf8')
        stopwordList.append(temp)
    dicFile.close()
    return stopwordList

# 以下对每篇新闻分词，去停用词后，不去重地存入list，最终以‘，’连接所有词,并且返回多少行，也就是多少篇新闻,
# word_box是所有新闻所有词的大杂烩，all_docs_to_lists是list里面包含list,每篇新闻是一个嵌套的list
def corpus_to_list(baseurl,mysqlhostIP, topic_number, mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv_v2'):
    sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()
    stop_word = getRidOfStopwords()
    # 连接hbase数据库
    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    h_m_t = topic_number
    # sqlcursor.execute("SELECT DISTINCT(program_name) from ini_app_program_source_rel")
    sqlcursor.execute("SELECT program_name from ini_app_program_source_rel where program_name = '军情解码' limit 1;")
    bufferTemp = sqlcursor.fetchall()
    for one_program in bufferTemp:
        one_program = one_program[0]
        word_box = list()
        # 表名
        tablename = "DATA:NEWS_Content"
        r = requests.get(baseurl + "/" + tablename + "/*",  auth=kerberos_auth, headers = {"Accept" : "application/json"})
        if issuccessful(r) == False:
            print "Could not get messages from HBase. Text was:\n" + r.text
        bleats = json.loads(r.text)
        sqlcursor.execute("SELECT program_type_name from ini_app_program_source_rel where program_name = %s limit 1;", (one_program,))
        bufferTemp = sqlcursor.fetchone()
        program_type_name = bufferTemp[0]
        print 'kk', one_program, program_type_name
        ind = 0
        all_docs_to_lists = list()
        allDoc_coma_join_lists = list()
        title_box =list()
        stop_word = getRidOfStopwords()
        # for key,data in table.scan(limit = 500, batch_size = 10):
            # bleats is json file
        for row in bleats['Row']:
            word_box_single = list()
            flag_type = False
            flag_date = False
            full_text = ''
            content = ''
            title = ''
            type_0 = ''
            for cell in row['Cell']:
                columnname = base64.b64decode(cell['column'])
                value = cell['$']
                if value == None:
                    print 'none'
                    continue
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
                    # print "date_time", date_time
                    transform_date_time = date_time.split(' ')[0]
                    if transform_date_time > otherStyleTime:
                        flag_date = True
                    else:
                        break
                if columnname == "base_info:type":
                    type_0 = base64.b64decode(value)
                    # 这里一定要用==,不能用in
                    if program_type_name in type_0:
                    # if (type_0 == '军事') or (type_0 == '新浪军事') or (type_0 == '搜狐军事') or (type_0 == '滚动军事') or (type_0 == '凤凰军事') or (type_0 == '军事要闻'):
                        flag_type = True
                    else:
                        break
                if columnname == "body:content":
                    content = base64.b64decode(value)
                if columnname == "base_info:news_from":
                    source = base64.b64decode(value)

                if columnname == "base_info:title":
                    title = base64.b64decode(value)
                if columnname == "base_info:website":
                    url = base64.b64decode(value)
            if flag_type and flag_date:
                # print 'transform_date_time',transform_date_time
                if title not in title_box:
                    ind += 1
                    title_box.append(title)
                    full_text = str(content) + str(title)
                    full_text = jieba.cut(full_text,cut_all = False)
                    for i in full_text:
                        if i not in stop_word:
                            # if len(i) == 0:
                            #     print 'kkk',i
                            if len(i) > 1:
                                if (i != "keywords") and (i != "title") and (i != "content") and (i != "description") and (i != "time") and (len(i) != 8):
                                    word_box.append(i)
                                    word_box_single.append(i)
                    # word_box_str将每篇新闻的词袋用“,”做连接，是个str
                    word_box_str = ','.join(word_box_single)
                    # all_docs_to_lists，每篇文章是一个list,这个list存入到一个大的list当中去
                    all_docs_to_lists.append(word_box_single)
                # allDoc_coma_join_lists存储新闻，一篇新闻是一个elemnet,这个element是str,是这篇新闻词用逗号做连接
                    allDoc_coma_join_lists.append(word_box_str)

        global root_directory_lda
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
                if i[0] in j:
                    ind_2 += 1
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

        global root_directory
        # docs_to_lists(hbaseIP):
        list_of_lists_file = open(root_directory+'/list_of_lists.txt', 'w+')
        mark = 0
        # element是一个list,存储多篇新闻，一篇新闻是一个elemnet,这个element是str,是这篇新闻词用逗号做连接
        element = allDoc_coma_join_lists
        docs_total = ind
        print "docs_total",docs_total
        for one in element:
            # print 'hh',one
            mark += 1
            if mark != docs_total:
                list_of_lists_file.write("%s\n" % one.encode('utf-8'))
            else:
                # 末行就没有换行符了
                list_of_lists_file.write("%s" % one.encode('utf-8'))
        list_of_lists_file.close()
        print "list_of_lists.txt is ready"

    # 以下为原来的docToMatrix.py***********************
        root_directory_lda = '/usr/local/lib/python2.7/site-packages/lda/tests'
        # docFile_reuters_tokens = open(root_directory_lda+'/tokens.txt','r')
        docFile_reuters_tokens = open(root_directory_lda+'/reuters.tokens','r')
        # docFile_reuters_tokens = open('C:\Users\Ivy\Desktop\myself\\tokens.txt','r')
        tokenslist = docFile_reuters_tokens.readlines()
        token_list = list()
        for word in tokenslist:
            if len(word) > 0:
                token_list.append(word)
        # token_list存储关键词，这是一个list,测试用的tokens一共350个，以为这矩阵的列数固定为350

        # root_directory = '/home/ctvit'
        # root_directory = '/usr/local/lib/python2.7/site-packages/lda/tests'
        root_directory = '/tmp'
        docFile_list_of_lists = open(root_directory+'/list_of_lists.txt','r')
        docIntolist = docFile_list_of_lists.readlines()

        # 'E:\\hqtest'
        if os.path.exists(r'/part-00000.txt'):
                os.path.remove(r'/part-00000.txt')
        ori_matrix_file = open(root_directory+'/part-00000.txt', 'w+')
        for single in docIntolist:
            # single_list代表着一篇文章所有的有意义词，这是一个list
            single_list = single.split(',')
            tokens_doc_times = list()
            for one_word in token_list:
                # 注意这里需要strip,否则count结果不正确
                one_word = one_word.strip()
                times = single_list.count(one_word)
                # print one_word,len(one_word),times
                tokens_doc_times.append(str(times))
            tokens_doc_times = ','.join(tokens_doc_times)
            ori_matrix_file.write("%s\n" % str(tokens_doc_times).encode('utf-8'))

        ori_matrix_file.close()
        docFile_reuters_tokens.close()
        docFile_list_of_lists.close()

    # 以下为原来的formal_matrix_title.py***********************
        thefile = open(root_directory_lda+'/reuters.ldac', 'w+')
        term_matrix = open(root_directory+'/part-00000.txt', 'r')
        reading_file_line = term_matrix.readlines()
        matrix_row_input = list()
        for line in reading_file_line:
            line = line.replace('[','')
            line = line.replace(']','')
            line = line.split(',')
            matrix_row_input.append(line)
        mark = 0
        print 'ppppppp',len(matrix_row_input)
        for row in matrix_row_input:
            mark += 1
            # print row,'haha'
            # break
            count = 0
            count_index = 0
            matrix_row = list()
            ele = ''
            for row_element in row:
                row_element = row_element.strip()
                count_index += 1
                if row_element != str('0'):
                    ele = str(count_index-1)+":"+str(row_element)
                    count += 1
                    matrix_row.append(ele)
            matrix_row.insert(0,str(count))
            matrix_row = ','.join(matrix_row)
            matrix_row = matrix_row.replace(","," ")
            if str(count) == '0':
                matrix_row = '1 1:1'
            if mark != len(matrix_row_input):
                thefile.write("%s\n" % str(matrix_row).encode('utf-8'))
            else:
                thefile.write("%s" % str(matrix_row).encode('utf-8'))
        term_matrix.close()
        thefile.close()
        print 'reuters.ldac is ready'
        # return h_m_t
        excuteldamodel(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', how_many_topics = h_m_t, how_many_iteration = 100, how_many_topic_words = 8, dbname = 'btv_v2')
        # excuteldamodel(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', how_many_iteration = 100, how_many_topic_words = 8, dbname = 'btv_v2')

# def excuteldamodel(baseurl,mysqlhostIP, how_many_topics, how_many_iteration, how_many_topic_words, mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv_v2'):
def excuteldamodel(baseurl,mysqlhostIP, how_many_topics, how_many_iteration, how_many_topic_words, mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv_v2'):
    # a = corpus_to_list(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', dbname = 'btv_v2')
    X = lda.datasets.load_reuters()
    vocab = lda.datasets.load_reuters_vocab()
    titles = lda.datasets.load_reuters_titles()
    title_no = dict()
    # 将文章标题与序号存成字典
    for i in range(len(titles)):
        title_no[i] = titles[i]
    createTime = datetime.datetime.now()
    # how_many_topics
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
        # print 'hah',news_date
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

    mysqlcursor.execute ("insert into topic_populaity (topic, key_words, date, popularity, created_date, topic_id) select topic, key_words, date, popularity, created_date, topic_id from top_topic_trend where left(created_date,10) = %s",(today_date,))
    mysqlcursor.execute ("update topic_populaity set program_id = '-3413556768156676966';")
    mysqlconn.close()
if __name__=='__main__':
    result = corpus_to_list(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', topic_number = 10, dbname = 'btv_v2')
    # def corpus_to_list(baseurl,mysqlhostIP, topic_number, mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv_v2')
    # excuteldamodel(baseurl = "http://172.28.12.34:8080", mysqlhostIP = '172.28.34.16', how_many_topics = 10, how_many_iteration = 100, how_many_topic_words = 8, dbname = 'btv_v2')









