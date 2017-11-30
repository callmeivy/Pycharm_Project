#coding:utf-8
'''
Created on 2017-05-12
@author: Ivy(jincan@ctvit.com.cn)
新闻关键词提取,原理为if-idf，排名越靠前，关键词的权重越大
'''
from sklearn.feature_extraction.text import TfidfVectorizer
import MySQLdb
from nltk.corpus import stopwords
# plays_corpus contains all documents in your corpus
def get_key_words(mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'weibo'):
    sqlConn = MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db=dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()
    # sqlcursor.execute("select Storyline as doc from cctv_news_content where pk = '5432';")
    sqlcursor.execute("select Storyline as doc from cctv_news_content;")
# list of tuple convert into list of string
    plays_corpus = map("".join,list(sqlcursor.fetchall()))
    plays_corpus_without_stop = list()
    content_pk = dict()
    for single_doc in plays_corpus:
        # 模糊匹配，精简为storyline的前100个字符，可调
        single_doc_sym = str(single_doc.encode('utf-8'))[:100]
        if "'" in single_doc_sym:
            # print "eee", single_doc_sym
            single_doc_sym = (single_doc_sym).replace("'","\\'")
        # print single_doc_sym
        sqlcursor.execute("select pk from cctv_news_content where Storyline like '%%%s%%' limit 1" %(single_doc_sym,))
        try:
            pk = sqlcursor.fetchone()[0]
            single_doc = str(remove_stopwords(single_doc))
            content_pk[single_doc] = pk
            plays_corpus_without_stop.append(single_doc)
        except:
            print "error with",single_doc_sym

# Initialise your TFIDF Vectorizer object
    tfidf_vectorizer = TfidfVectorizer()
    romeo = [plays_corpus_without_stop[0]]
    # Now create a model by fitting the vectorizer to your main plays corpus, this creates an array of TFIDF scores
    model = tfidf_vectorizer.fit_transform(plays_corpus_without_stop)
    terms = tfidf_vectorizer.get_feature_names()
    count = 0
    insert_count = 0
    for one_doc in plays_corpus_without_stop:
        key_word_box = list()
        one_doc_list = [(one_doc)]
        romeo_scored = tfidf_vectorizer.transform(one_doc_list) # note - .fit() not .fit_transform
        scores = romeo_scored.toarray().flatten().tolist()
        data = list(zip(terms,scores))
        sorted_data = sorted(data,key=lambda x: x[1],reverse=True)
        pk_no = int(content_pk[one_doc])
        # print 'lll', pk_no
        for value in sorted_data[:5]:
            # 阈值可调整
            if value[1]> 0.0001:
                key_word_box.append(value[0])
        if len(key_word_box) ==0:
            count += 1
            print "no key word"
            # print pk_no,one_doc

        key_word = (",").join(key_word_box)
        sqlcursor.execute("update cctv_news_content set key_words = %s where pk = %s",(key_word,pk_no))
        insert_count += 1
        sqlConn.commit()
    print count
    print insert_count
    sqlConn.close()

# doc should be string,return to a string
def remove_stopwords(doc):
    stop = set(stopwords.words('english'))
    doc_without_stop=(" ").join([i for i in doc.lower().split() if i not in stop]).encode('utf-8')
    return doc_without_stop




if __name__ == '__main__':
    key_words = get_key_words(mysqlhostIP='192.168.168.105', dbname='weibo')
