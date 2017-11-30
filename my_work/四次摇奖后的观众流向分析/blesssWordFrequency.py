#coding: UTF-8
'''
by Ivy
created on 18 Jan,2016
bless high-freqency words
'''
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import jieba
from collections import Counter
import csv
import datetime

now =  datetime.datetime.now()
print now
# 停用词读到stopwordList
dicFile = open('E:\ctvit\lab\Code\Pycharm Project\\nlp\NamedEntity\stopwords.txt','r')
stopwords = dicFile.readlines()
stopwordList = []
stopwordList.append(' ')
for stopword in stopwords:
    temp = stopword.strip().replace('\r\n','').decode('utf8')
    stopwordList.append(temp)
dicFile.close()

# 读取待分析文件
path = 'E:\\bless\comment1.txt'
Corpus_cut = list()
candidate_list = list()
print "retrieving the file"
with open(path, "r") as dataFile:
    try:
        print 'start',now
        bless = dataFile.readlines()
        for line in bless:
            Corpus_cut = Corpus_cut + list(jieba.cut(line,cut_all = False))
    except Exception:
    # 不符合JSON格式规范
        pass
print "close the file"

    # if json.has_key("title"):
    #     content = json["title"]
    #     print content
    # else:
    #     # JSON中没有content
    #     pass

# print 1,len(candidate_list)
bless_listcount = dict(Counter(Corpus_cut))
bless_listcount = sorted(bless_listcount.iteritems(), key=lambda e:e[1], reverse=True)
csvfile = file('E:\\bless_frequency_words.csv', 'wb')
writer = csv.writer(csvfile)
writer.writerow(['word', 'frequency'])
ind = 0
print 1111111111
for item in bless_listcount:
    # ind += 1
    word = item[0].encode('utf-8')
    # print 'word',word
    how_many = item[1]
    # print how_many

    # if ind > 100:
    #     break
    if word not in stopwordList:
        write_line = [word.encode('utf-8'), how_many]
        writer.writerow(write_line)


print now

csvfile.close()