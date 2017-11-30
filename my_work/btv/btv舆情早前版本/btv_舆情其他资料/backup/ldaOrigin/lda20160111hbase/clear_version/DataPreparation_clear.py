#coding=UTF-8
'''
Updated on 11 Jan,2016
Run in Hbase

Created on 12 Aug, 2015 and 13 Aug, 2015
@author: Ivy
为LDA准备reuters.ldac、reuters.tokens、reuters.title
'''
import os
import os.path
import jieba
from collections import Counter
from json import JSONDecoder
import sys
import shutil
default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)
root_directory = 'E:\\hqtest'
root_directory2 = 'E:\PeopleMilitaryNews'
root_directory_lda = 'C:\Python27\Lib\site-packages\lda\\tests'
title_box = list()
import re
import json
def getRidOfStopwords():
        dicFile = open('E:\ctvit\lab\Code\Pycharm Project\\nlp\stopwords.txt','r')
        stopwords = dicFile.readlines()
        stopwordList = []
        stopwordList.append(' ')
        for stopword in stopwords:
            temp = stopword.strip().replace('\r\n','').decode('utf8')
            stopwordList.append(temp)
        dicFile.close()
        return stopwordList

def get_corpus():

    os.chdir(root_directory2)
    cmd = "dir /A-D /B"
    list_file = os.popen(cmd).readlines()
    return list_file


def corpus_to_list(single_file):
    word_box = list()
    stop_word = getRidOfStopwords()

    single_file_path = root_directory2+'\\'+str(single_file.strip())
    with open(single_file_path) as mili_file:
        reading_file_line = mili_file.readlines()
        for line in reading_file_line:
            line = jieba.cut(line,cut_all = False)
            for i in line:
                if i not in stop_word:
                    if len(i) > 1:
                        if (i != "keywords") and (i != "title") and (i != "content") and (i != "description") and (i != "time") and (len(i) != 8):
                            word_box.append(i)
    word_box_str = ','.join(word_box)
    return word_box,word_box_str

def key_words(list1):
    ind = 0
    token_box = list()
    seg_listcount = dict(Counter(list1))
    seg_listcount = sorted(seg_listcount.iteritems(), key=lambda e:e[1], reverse=True)
    for item in seg_listcount:
        word = item[0]
        how_many = item[1]
        if (word != "keywords") and (word != "title") and (word != "content") and (word != "description") and (word != "time") and (len(word) != 8):
            ind += 1
            token_box.append(word)
    return token_box




def get_tokens():
    ffile = open(root_directory_lda+'\\tokens.txt', 'w+')
    list_file_1 = get_corpus()
    a = list()
    for item in list_file_1:
        a = a + corpus_to_list(item)[0]
    tokenss = key_words(a)
    ind = 0
    how_many_word = 350
    mark = 0
    for j in tokenss:
        ind += 1
        if ind > how_many_word:
            break
        mark += 1
        if mark != len(tokenss):
            ffile.write("%s\n" % j.encode('utf-8'))
        else:
            ffile.write("%s" % j.encode('utf-8'))
    print "reuters.tokens is ready"
    ffile.close()
    # 另外，因为上面改成tokens.txt了，需要再复制一份到reuters.tokens
    shutil.copy(root_directory_lda+'\\tokens.txt', root_directory_lda+'\\reuters.tokens')
    return tokenss


def docs_to_lists():
    list_of_lists_file = open(root_directory+'\\list_of_lists.txt', 'w+')
    one_doc_list = list()
    list_file_2 = get_corpus()
    list_contain_list = list()
    all_doc_list = list()
    mark = 0
    for item in list_file_2:
        mark += 1
        element = corpus_to_list(item)[1]
        if mark != len(list_file_2):
            list_of_lists_file.write("%s\n" % element.encode('utf-8'))
        else:
            list_of_lists_file.write("%s" % element.encode('utf-8'))

    list_of_lists_file.close()


# 生成term_document_matrix
def term_document_matrix_roy_1():
    thefile = open(root_directory_lda+'\\reuters.ldac', 'w+')
    term_matrix = open(root_directory+'\part-00000', 'r')
    reading_file_line = term_matrix.readlines()
    matrix_row_input = list()
    for line in reading_file_line:
        line = line.replace('[','')
        line = line.replace(']','')
        line = line.split(',')
        matrix_row_input.append(line)
    mark = 0
    for row in matrix_row_input:
        mark += 1
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
        if mark != len(matrix_row_input):
            thefile.write("%s\n" % str(matrix_row).encode('utf-8'))
        else:
            thefile.write("%s" % str(matrix_row).encode('utf-8'))
    term_matrix.close()
    thefile.close()
    print 'reuters.ldac is ready'

def corpus_to_list_without_cut(single_file):
    word_box = list()
    stop_word = getRidOfStopwords()

    single_file_path = root_directory2+'\\'+str(single_file.strip())
    with open(single_file_path) as mili_file:
        reading_file_line = mili_file.readlines()
        ind = 0

        for line in reading_file_line:

            if ind > 1:
                break
            ind += 1
            title_get = line.split(',')
            title_get = title_get[0].replace('{"title":','')

    return title_get


def get_title():
    title_file = open(root_directory_lda+'\\reuters.titles', 'w+')
    list_file_1 = get_corpus()
    a = list()
    count = 0
    mark = 0
    for item in list_file_1:
        mark +=1
        count += 1
        element = corpus_to_list_without_cut(item)
        if mark != len(list_file_1):
            title_file.write("%s\n" % str(element).encode('utf-8'))
        else:
            title_file.write("%s" % str(element).encode('utf-8'))

    title_file.close()
    print 'reuters.titles is ready'


if __name__=='__main__':
    # 首先运行以下两个：get_tokens()和docs_to_lists()，产生的结果有两个：list_of_lists.txt和tokens.txt,作为下一步的输入，docToMatrix.py
    # get_tokens()
    #
    # docs_to_lists()


    term_document_matrix_roy_1()
    get_title()








