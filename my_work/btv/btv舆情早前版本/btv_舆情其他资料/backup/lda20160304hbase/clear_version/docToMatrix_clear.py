#coding=UTF-8
'''
Created on Feb 16, 2016

get docs into matrix

@author: Ivy
'''
import os
import os.path
root_directory_lda = 'C:\Python27\Lib\site-packages\lda\\tests'
docFile_reuters_tokens = open(root_directory_lda+'\\tokens.txt','r')
tokenslist = docFile_reuters_tokens.readlines()
token_list = list()
for word in tokenslist:
    if len(word) > 0:
        token_list.append(word)

root_directory = 'E:\\hqtest'
docFile_list_of_lists = open(root_directory+'\\list_of_lists.txt','r')
docIntolist = docFile_list_of_lists.readlines()

ori_matrix_file = open(root_directory+'\\part-00000.txt', 'w+')
for single in docIntolist:
    single_list = single.split(',')
    tokens_doc_times = list()
    for one_word in token_list:
        one_word = one_word.strip()
        times = single_list.count(one_word)
        tokens_doc_times.append(str(times))
    tokens_doc_times = ','.join(tokens_doc_times)
    ori_matrix_file.write("%s\n" % str(tokens_doc_times).encode('utf-8'))

ori_matrix_file.close()
docFile_reuters_tokens.close()
docFile_list_of_lists.close()