#coding:UTF-8
'''
Created on 2016年4月11日

@author: Ivy

对话栏目，选书的几个指标分析
'''
import sys,os
from sys import path
path.append('tools/')
path.append(path[0]+'/tools')
import MySQLdb
import re
# 工具类
import jieba
import datetime
import time
import urllib2
reload(sys)
sys.setdefaultencoding('utf8')

def big_shot(mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'cctv'):
    url_get_base = "http://api.ltp-cloud.com/analysis/?"
    api_key = 'd5L8f838PDpfYH0DKvkcdFEMKE9FVU1kbY1QHZoq'
    # text = '“既然年轻人喜欢网络，敌对势力又用网络丑化我们的领袖，攻击毛泽东思想，抹黑我们的英雄，我们就要善于用网络回击。”毛新宇谈到，这其中一个重要的举措就是，在网上阵地支持鼓励宣传马克思主义哲学、宣传毛泽东思想、宣传中国特色社会主义理论。'
    format = 'json'
    # pos是词性标注，ner是命名实体识别
    # pattern = 'pos'
    pattern = 'ner'
    # 连接数据库
    sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()
    # print '新建库成功'
    sqlcursor.execute('''SELECT book_id, book_editorRecommend from dangdangbook_detial limit 300''')
    bufferTemp = sqlcursor.fetchall()
    book_name_dict = dict()
    ind = 0
    for i in bufferTemp:
        ind += 1
        name_list = list()
        book_id = i[0]
        book_editorRecommend = i[1].decode("utf8")
        book_editorRecommend = re.sub("[\s+\.\!\/_,$%^*(+\"\']+|[+——！？、~@#￥%……&*（）]+".decode("utf8"), "".decode("utf8"),book_editorRecommend)
        # book_mediaRecommend = i[2].encode('utf8')
        result = urllib2.urlopen("%sapi_key=%s&text=%s&format=%s&pattern=%s" % (url_get_base,api_key,book_editorRecommend,format,pattern))
        print ind
        content = result.read().strip()
        content = eval(content)
        content = content[0]
        # print len(content)
        for sentence in content:
            for word in sentence:
                if word['ne'] == 'S-Nh':
                    person = word['cont']
                    if person not in name_list:
                        if len(person) > 1:
                            name_list.append(person)
        name_list_join = ','.join(name_list)
        if len(name_list_join) > 0:
            book_name_dict[book_id] = name_list_join
    for k,s in book_name_dict.iteritems():
        # print k,s
        sqlcursor.execute('''UPDATE dangdangbook_detial set person = %s where book_id = %s''',(s,k))
        sqlConn.commit()



    sqlConn.close()

if __name__=='__main__':
    weiboTest = big_shot(mysqlhostIP = '10.3.3.182', dbname = 'cctv')


