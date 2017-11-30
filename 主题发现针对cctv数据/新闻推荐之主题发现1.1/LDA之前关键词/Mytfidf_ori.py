#coding=UTF-8
#Importing necessary libraries
import math
import re
from textblob import TextBlob as tb
import jieba
import jieba.posseg as pseg
from nltk.stem.wordnet import WordNetLemmatizer
path = "customize_dict.txt"
jieba.load_userdict(path)
# stopwords
dicFile = open('stopwords.txt', 'r')
stopwords = dicFile.readlines()
stopwordList = []
stopwordList.append(' ')
for stopword in stopwords:
    temp = stopword.strip().replace('\r\n', '').decode('utf8')
    stopwordList.append(temp)
dicFile.close()

#Function to compute Term Frequency(TF = no. of times word present in passage/no. of words)
def tf(word, passage, passagelen):
    return (float)(passage.words.count(word)) / passagelen

#Function to compute the number of passages a word is present in
def n_containing(word, commentList):
    return sum(1 for passage in commentList if word in passage)

#Function to compute inverse document frequency (IDF = log_e(Total number of documents / Number of documents with term t in it)
def idf(word, commentList):
    return math.log(len(commentList) / (float)(1 + n_containing(word, commentList)))

#Function to compute tf idf
def tfidf(word, passage, commentList):
    passagelen = (float)(len(passage.words))
    return round(tf(word, passage, passagelen) * idf(word, commentList),5)

def preprocessing(commentFull):
    tokens = list()

    # tokens = filter(lambda word: not word in stopwordList,jieba.cut(commentFull))
    # tokens = filter(lambda word: not word in stopwordList,pseg.cut(commentFull))
    token = pseg.cut(commentFull)
    good_tags = set(['n', 'ns', 'nz', 'nr', 'nrt', 'nt', 'x'])
    for word, flag in token:
        if flag in good_tags:
            if word not in stopwordList:
                tokens.append(word)
    processedComment = ' '
    return processedComment.join(tokens)

#Function to run TF IDF algorithm
# completeComment is dict, topNumber is the number of key words
def runmytfidf(completeComment, topNumber):
    commentList = []

    # textblob为了使用text.word.count
    for i in range(0,len(completeComment)):
        commentList.append(tb(preprocessing(completeComment[i])))

    returnList=[]
    print 'len(commentList)',len(commentList)
    print commentList[0]
    print commentList[1]
    #Obtaing the Top Key words for all the passages
    for i, passage in enumerate(commentList):
        scores = {word: tfidf(word, passage, commentList) for word in passage.words}
        sorted_words = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        topWords=[]
        for word, score in sorted_words[:topNumber]:
            if len(word) >1:
                # print word,len(word)
                topWords.append(word)
        returnList.append(topWords)
    print 'returnList of TF IDF:第一篇前3个关键字：',returnList[0][0],returnList[0][1],returnList[0][2]
    return returnList