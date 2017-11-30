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
from sys import path
path.append('tools/')
path.append(path[0]+'/tools')
from collections import Counter
from json import JSONDecoder
import sys
import shutil
import math
default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)
root_directory = '/usr/jincan'
root_directory_lda = '/usr/local/lib/python2.7/site-packages/lda/tests'
title_box = list()
import re
import json
import base64
from requests_kerberos import HTTPKerberosAuth, OPTIONAL

def issuccessful(request):
    if 200 <= request.status_code and request.status_code <= 299:
        return True
    else:
        return False


def getRidOfStopwords():
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
def corpus_to_list(baseurl):
    word_box = list()
    stop_word = getRidOfStopwords()
    # 连接hbase数据库
    os.popen('kinit -k -t ctvit.keytab ctvit')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    # 表名
    # table=conn.table('NEWS_Content')
    tablename = "DATA:NEWS_Content"
    r = requests.get(baseurl + "/" + tablename + "/*",  auth=kerberos_auth, headers = {"Accept" : "application/json"})
    if issuccessful(r) == False:
        print "Could not get messages from HBase. Text was:\n" + r.text
    quit()
    bleats = json.loads(r.text)

    ind = 0
    all_docs_to_lists = list()
    allDoc_coma_join_lists = list()
    title_box =list()
    # for key,data in table.scan(limit = 500, batch_size = 10):
        # bleats is json file
    for row in bleats['Row']:
        word_box_single = list()
        ind += 1
        for cell in row['Cell']:
            columnname = base64.b64decode(cell['column'])
            value = cell['$']
            if value == None:
                print 'none'
                continue
            if columnname == "'base_info:cdate'":
                cdate = base64.b64decode(value)
            if columnname == "'base_info:datetime'":
                date_time = base64.b64decode(value)
            if columnname == "'base_info:content'":
                content = base64.b64decode(value)
            if columnname == "'base_info:from'":
                source = base64.b64decode(value)
            if columnname == "'base_info:title'":
                title = base64.b64decode(value)
                title_box.append(title)
            if columnname == "'base_info:website'":
                url = base64.b64decode(value)
            full_text = str(content) + str(title)
            full_text = jieba.cut(full_text,cut_all = False)
            stop_word = getRidOfStopwords()
        for i in full_text:
            if i not in stop_word:
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


    # 以下准备reuters.titles
    title_file = open(root_directory_lda+'/reuters.titles', 'w+')
    mark = 0
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
    ffile = open(root_directory_lda+'/tokens.txt', 'w+')
    b = list()
    # all_doc_into_list，每篇文章是一个list,所有文章又存成list
    all_doc_into_list = all_docs_to_lists
    docs_total = ind
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
    shutil.copy(root_directory_lda+'/tokens.txt', root_directory_lda+'/reuters.tokens')


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

if __name__=='__main__':
    result = corpus_to_list(baseurl = '192.168.168.41')









