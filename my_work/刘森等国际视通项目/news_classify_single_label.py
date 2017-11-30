#coding:utf-8
'''
Created on 2017-05-12
@author: Ivy(jincan@ctvit.com.cn)
新闻分类，每篇新闻只能属于一类
'''
import MySQLdb
from time import time
from sklearn import preprocessing
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
import scipy.sparse as sp
import numpy as np
import matplotlib.pyplot as plt
from nltk.corpus import stopwords
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.multiclass import OneVsRestClassifier
from sklearn.metrics import classification_report
mlb = MultiLabelBinarizer()
sqlConn = MySQLdb.connect(host='192.168.168.105', user= 'root', passwd= '', db='weibo', charset='utf8')
sqlcursor = sqlConn.cursor()
total = 800
sqlcursor.execute("SELECT concat(PubTitle,Storyline) as doc,tname from cctv_news_content where tname <> 'Others' order by rand() limit %s", (total,))
traindata = sqlcursor.fetchall()
# label为others的删除
total_valid_data = len(list(traindata))
train_data = list()
train_target = list()
test_target = list()
test_data = list()
train_portion = 0.8
i = 0
# doc should be string
def remove_stopwords(doc):
    # print 222,type(doc)
    stop = set(stopwords.words('english'))
    doc_without_stop=(" ").join([i for i in doc.lower().split() if i not in stop])
    return doc_without_stop

for row in traindata:
    # print 111,row[0].encode('utf-8')
    # print 111,type(row[0].encode('utf-8'))
    content = str(remove_stopwords(row[0].encode('utf-8')))
    # print 2222,content
    try:
        labels = ((row[1].split(';'))[0]).encode('utf-8')
        # print 'ppp', labels
    except:
        print 'error'
    i += 1
    if i <= total_valid_data*train_portion:
        # print 3333,content
        train_data.append(content)
        train_target.append(labels)
    else:
        test_data.append(content)
        test_target.append(labels)
print "训练样本量",len(train_data),len(train_target)
print "测试样本量",len(test_data),len(test_target)

print("Loading newsgroups training set... ")
print("Extracting features from the dataset using a sparse vectorizer")
t0 = time()
vectorizer = TfidfVectorizer(encoding='latin1')
X_train = vectorizer.fit_transform(train_data)
print("done in %fs" % (time() - t0))
print("n_samples: %d, n_features: %d" % X_train.shape)
assert sp.issparse(X_train)
# print "train_target",train_target
train_le = preprocessing.LabelEncoder()
y_train = train_le.fit_transform(train_target)
# y_train = mlb.fit_transform(train_target)
# print "y_train",y_train
print("Loading newsgroups test set... ")

print("Extracting features from the dataset using the same vectorizer")
t0 = time()
X_test = vectorizer.transform(test_data)
test_le = preprocessing.LabelEncoder()
y_test = test_le.fit_transform(test_target)
# y_test = mlb.fit_transform(test_target)
print("done in %fs" % (time() - t0))
print("n_samples: %d, n_features: %d" % X_test.shape)
feature_names = np.asarray(vectorizer.get_feature_names())
print(feature_names)

ovr = OneVsRestClassifier(MultinomialNB())
ovr.fit(X_train, y_train)
pred = ovr.predict(X_test)

print("Classification report on test set for classifier:")
# print(clf)
# print()
print(classification_report(y_test, pred))

sqlConn.close()

