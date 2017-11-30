#coding:utf-8
'''
Created on 2017-05-17
@author: Ivy(jincan@ctvit.com.cn)
多标签新闻分类，每篇新闻可能同时属于多个类别，准确率指每个类别均判断正确，不能缺也不能多
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
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.metrics import classification_report
from sklearn.svm import LinearSVC
from sklearn.feature_extraction.text import CountVectorizer
mlb = MultiLabelBinarizer()
from sklearn.externals import joblib
sqlConn = MySQLdb.connect(host='192.168.168.105', user= 'root', passwd= '', db='weibo', charset='utf8')
sqlcursor = sqlConn.cursor()
total = 6000
sqlcursor.execute("SELECT concat(PubTitle,Storyline) as doc,tname from cctv_news_content where tname <> 'Others' order by rand() limit %s", (total,))
traindata = sqlcursor.fetchall()
total_valid_data = len(list(traindata))
train_data = list()
train_target = list()
test_target = list()
test_data = list()
from sklearn.pipeline import Pipeline
# train_portion = 0.8
i = 0
# doc should be string
def remove_stopwords(doc):
    stop = set(stopwords.words('english'))
    doc_without_stop=(" ").join([i for i in doc.lower().split() if i not in stop])
    return doc_without_stop

for row in traindata:
    content = str(remove_stopwords(row[0].encode('utf-8')))
    try:
        labels = set(row[1].split(';'))
    except:
        print 'error'
    test_data.append(content)
    test_target.append(labels)
print "测试样本量",len(test_data),len(test_target)
loaded_model = joblib.load('E://model.pkl')
loaded_vocabulary = joblib.load('E://vocabulary.pkl')

X_test = np.array((test_data))
y_test = mlb.fit_transform(test_target)
print ("An ordering for the test class labels:")
print list(mlb.classes_)
predicted = loaded_model.predict(X_test)

all_labels = mlb.inverse_transform(predicted)
for item, labels in zip(X_test, all_labels):
    print('{0} => {1}'.format(item, ', '.join(labels)))

print("Classification report on test set for classifier:")
print(classification_report(y_test, predicted))