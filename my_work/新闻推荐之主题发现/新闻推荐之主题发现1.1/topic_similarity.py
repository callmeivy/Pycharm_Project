#coding=UTF-8
'''
Created on 20 July,2017

@author: Ivy

主题相似度,如果相似，将新闻合并都总表相应的主题，如果不相似，则在总表产生新的主题
'''
import MySQLdb
import difflib
import operator
def topic_similarity():
    sqlConn = MySQLdb.connect(host='192.168.168.105', user='root', passwd='', db='cctv', charset='utf8')
    sqlcursor = sqlConn.cursor()
    sqlcursor.execute("select news_under_topic from top_topic_trend_summary")
    summary_data = list(sqlcursor.fetchall())
    topics_content = list()
    topic_dgree = dict()
    for topics in summary_data:
        topic_content = list()
        titles = topics[0].split(';')
        for doc in titles:
            if "'" in doc:
                doc = doc.replace("'","\\'")
            sqlcursor.execute("select Storyline from cctv_news_content where PubTitle = '%s' limit 1" %(doc,))
            # print "select Storyline from cctv_news_content where PubTitle = '%s' limit 1" %(doc,)
            Storyline = list(sqlcursor.fetchall())
            topic_content.append(Storyline[0][0])
        topics_content.append(topic_content)
    # Attention!
    # 注意把period限定条件，否则week表所有内容都会再算一次相似度再合并到总表
    sqlcursor.execute("select news_under_topic from top_topic_trend_week where period = '20170115-20170117';")
    week_data = list(sqlcursor.fetchall())
    topics_content_add = list()
    for topics in week_data:
        topic_content = list()
        titles = topics[0].split(';')
        for doc in titles:
            if "'" in doc:
                doc = doc.replace("'", "\\'")
            sqlcursor.execute("select Storyline from cctv_news_content where PubTitle = '%s' limit 1" % (doc,))
            Storyline = list(sqlcursor.fetchall())
            topic_content.append(Storyline[0][0])
        topics_content_add.append(topic_content)
    # print topics_content_add
    # print len(topics_content_add), len(topics_content_add[0])

    for one in topics_content_add:
        ii = list()
        search_again = one[0][:105]
        if "'" in search_again:
            search_again = search_again.replace("'", "\\'")
        sqlcursor.execute("select PubTitle from cctv_news_content where Storyline like '%s%%' limit 1" % (search_again,))
        topic_id_data = list(sqlcursor.fetchall())[0][0]
        if "'" in topic_id_data:
            topic_id_data = topic_id_data.replace("'", "\\'")
        sqlcursor.execute("select pk from top_topic_trend_week where news_under_topic like '%%%s%%' limit 1" % (topic_id_data,))
        topic_id_week = list(sqlcursor.fetchall())
        for one_topic_content in topics_content:
            topic_simi =  doc_similary_list(one_topic_content,one)
            if topic_simi > 0.7:
                ii.append(topic_simi)
                search_one =  one_topic_content[0][:105]
                if "'" in search_one:
                    search_one = search_one.replace("'", "\\'")
                sqlcursor.execute("select PubTitle from cctv_news_content where Storyline like '%s%%' limit 1" %(search_one,))
                topic_id_data = list(sqlcursor.fetchall())[0][0]
                sqlcursor.execute("select pk from top_topic_trend_summary where news_under_topic like '%%%s%%' limit 1" % (topic_id_data,))
                topic_id = list(sqlcursor.fetchall())
                topic_dgree[str(topic_id[0][0])+"&"+str(topic_id_week[0][0])] = topic_simi

        if len(ii)>0:
            # print ii
            index, value = max(enumerate(ii), key=operator.itemgetter(1))
            # print index, value
            for k, v in topic_dgree.iteritems():
                if v == value:
                    k = k.split("&")
                    sqlcursor.execute("update top_topic_trend_week set similar_to_topic_in_summary = '%s' where pk = '%s';" %(k[0],k[1]))
                    sqlcursor.execute("update top_topic_trend_week set similarity_degree = '%s' where pk = '%s';" %(v,k[1]))
                    sqlConn.commit()

    # 相似的主题进行合并
    sqlcursor.execute("select core_vector, key_words, news_under_topic, period, similar_to_topic_in_summary from top_topic_trend_week where length(similar_to_topic_in_summary) > 0")
    results = list(sqlcursor.fetchall())
    period_box = list()
    for core_vector,key_words,news_under_topic,period,similar_to_topic_in_summary in results:
        if "'" in news_under_topic:
            news_under_topic = news_under_topic.replace("'","\\'")
        if period not in period_box:
            period_box.append(period)
            sqlcursor.execute("update top_topic_trend_summary set core_vector = concat(core_vector,',','%s'),key_words = concat(key_words,',','%s'),\
        news_under_topic = concat(news_under_topic,';','%s'),period = concat(period,';','%s') where pk = '%s';" %(core_vector,key_words,news_under_topic,period,similar_to_topic_in_summary))

        sqlConn.commit()

    # 相似度低的成为新的主题更新到总表
    # Attention!
    # 注意把period限定条件，否则week表所有内容都会再算一次相似度再合并到总表
    sqlcursor.execute("INSERT INTO top_topic_trend_summary(category, core_vector, period, key_words, news_under_topic, created_date) SELECT category,\
     core_vector, period, key_words, news_under_topic, created_date FROM top_topic_trend_week where length(top_topic_trend_week.similar_to_topic_in_summary)\
      = 0 and period = '20170115-20170117'")
    sqlConn.commit()
    sqlcursor.execute("select pk from top_topic_trend_summary where length(topic_id)=0")
    sqlcursor.execute("select pk from top_topic_trend_summary where topic_id is null")
    pks = list(sqlcursor.fetchall())
    for pk in pks:
        sqlcursor.execute("select MAX(topic_id) FROM top_topic_trend_summary")
        max_number_add = int(list(sqlcursor.fetchone())[0])+1
        sqlcursor.execute("update top_topic_trend_summary set topic_id = '%d' where pk = '%s';" %(max_number_add,pk[0]))
        sqlConn.commit()

    sqlConn.close()


def doc_similary(doc1,doc2):
    similary = (difflib.SequenceMatcher(None, doc1, doc2).quick_ratio())
    return similary

def doc_similary_list(list1,list2):
    sum = 0
    for a in list1:
        for b in list2:
            sum += doc_similary(a,b)
    simi_avg = float(sum)/float(len(list1)*len(list2))
    return simi_avg



if __name__ == '__main__':
    result = topic_similarity()