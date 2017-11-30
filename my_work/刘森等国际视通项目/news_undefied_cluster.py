#coding:utf-8
'''
Created on 2017-05-16
@author: Ivy(jincan@ctvit.com.cn)
未归类新闻的聚类
'''
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import adjusted_rand_score
import MySQLdb
sqlConn = MySQLdb.connect(host='192.168.168.105', user= 'root', passwd= '', db='weibo', charset='utf8')
sqlcursor = sqlConn.cursor()
total = 7000
sqlcursor.execute("SELECT concat(PubTitle,Storyline) as doc from cctv_news_content where tname = 'Others' order by rand() limit %s", (total,))
undefied = map("".join,list(sqlcursor.fetchall()))


vectorizer = TfidfVectorizer(stop_words='english')
# X = vectorizer.fit_transform(documents)
X = vectorizer.fit_transform(undefied)
true_k = 6
model = KMeans(n_clusters=true_k, init='k-means++', max_iter=100, n_init=1)
model.fit(X)
print("Top terms per cluster:")
order_centroids = model.cluster_centers_.argsort()[:, ::-1]
terms = vectorizer.get_feature_names()
for i in range(true_k):
    print "Cluster %d:" % i,
    for ind in order_centroids[i, :10]:
        print ' %s' % terms[ind],

sqlConn.close()