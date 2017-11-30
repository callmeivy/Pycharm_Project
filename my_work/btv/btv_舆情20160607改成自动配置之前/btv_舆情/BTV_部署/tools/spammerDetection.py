#coding:UTF-8
'''
Created on 2014年3月5日

@author: hao
'''
import simplejson
temp={"title": "\u6587\u7ae0\u6807\u9898\n\u6362\u884c"}
import pymongo
class spammerdetect():
    def __init__(self):
        self.score = 0
    
    def detectSpammer(self, userInfo):
        try:
            simplejson.dumps(temp)
        except:
            print "wrong input"
            return -1
        title = []
        self.score = 0
        for doc in userInfo:
            title.append(doc)
        if ('statuses_count' in title) and ('followers_count' in title) and ('friends_count' in title) and ('bi_followers_count' in title):
            if userInfo['statuses_count']<=20:
                self.score -= 200
            if userInfo['friends_count']<=20:
                self.score -= 300
            elif userInfo['followers_count']<=20:
                self.score -= 500
            else:
                ratio = float(userInfo['followers_count'])/float(userInfo['friends_count'])
                if (ratio>1):
                    self.score = self.score + 100 + ratio*100
                elif (ratio<0.5):
                    self.score = self.score+ 400*ratio - 400
                else:
                    self.score = self.score+ ratio*800 - 400
                    
            if userInfo['bi_followers_count']<=10:
                self.score -= 100
            else:
                ratio = float(userInfo['followers_count'])/float(userInfo['bi_followers_count'])
                if (ratio>0.5):
                    self.score += 100
                elif (ratio<0.1):
                    self.score -= 200
                else:
                    self.score += 50
            
        else:
            print "信息不全"
            return -1
        
        if ('domain' in title) and ('url' in title) and ('description' in title) and ('location' in title):
            if len(userInfo['domain'])>0:
                self.score += 100
            if len(userInfo['url'])>0:
                self.score += 100
            if len(userInfo['description'])<=0:
                self.score -= 100
            if userInfo['location']=='其他':
                self.score -= 100
                
        else:
            print "信息不全"
            return -1      
        
        if ('verified' in title) and ('verified_type' in title):
            if userInfo['verified']:
                self.score += 1000
            if userInfo['verified_type'] != -1:
                self.score += 500
        else:
            print "信息不全"
            return -1  
          
        return self.score

if __name__=='__main__':
    mongoConn = pymongo.Connection(host='10.3.3.220', port = 27017)
    # 查询某条微博的回复
    mongoCollection = mongoConn.cctv.comment
    mongoCursor = mongoCollection.find().limit(1).skip(12151)
    test = spammerdetect()
    for doc in mongoCursor:
        score = test.detectSpammer(doc['user'])
        print doc['user']['name']
        print score

    mongoConn.close()