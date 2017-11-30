#coding:UTF-8
'''
Created on 2014年3月5日

@author: hao
'''

#去除表情
class removeIrrelevant():

    def removeEmotions(self,sentence):
        while True:
            index1 = sentence.find('[')
            index2 = sentence.find(']')
            
            if (index1<0 and index2<0):
                break
            if (index1>=0 and index2>=0):
                tempSentence1 = sentence[index1+1:]
                index11 = tempSentence1.find('[')
                if index11<0:
                    tempSentence = tempSentence1
                    index12 = tempSentence.find(']')
                    if index12>=0:
                        sentence = sentence[:index1]+tempSentence[index12+1:]
                    else:
                        sentence = sentence[:index1]+sentence[index1+1:]
                        index2 = sentence.find(']')
                        sentence = sentence[:index2]+sentence[index2+1:]
                    
                else:            
                    tempSentence2 = tempSentence1[:index11]
                    index12 = tempSentence2.find(']')
                    if index12>=0:
                        sentence = sentence[:index1]+tempSentence2[index12+1:]+tempSentence1[index11:]
                    else:
                        sentence = sentence[:index1]+sentence[index1+1:]
                continue
                
            elif(index1>=0):
                sentence = sentence[:index1]+sentence[index1+1:]
            elif(index2>=0):
                sentence = sentence[:index2]+sentence[index2+1:]
            else:
                print "unexpect situation"
                break   
        return sentence
                
    #去除短链
    def removeShortURL(self,sentence):
        while True:
            if 'http://t.cn/' not in sentence:
                break
            else:
                index = sentence.find('http://t.cn/')
                index1 = sentence[index:].find(' ')
                if index1<0:
                    sentence = sentence[:index]
                else:
                    sentence = sentence[:index]+sentence[index:][index1+1:]
        return sentence
        
    # 去除转发
    def removeRepostWord(self,sentence):
        while True:
            if ('转发微博' not in sentence) and ('Repost' not in sentence):
                break
            else:
                index = sentence.find('转发微博')
                if index>=0:
                    sentence = sentence[:index]+sentence[index+12:]
                index = sentence.find('Repost')
                if index>=0:
                    sentence = sentence[:index]+sentence[index+7:]
        return sentence
    # 去除回复@
    def removeReplyAt(self,sentence):
        while True:
            if '回复@' not in sentence:
                break
            else:
                index1 = sentence.find('回复@')
                subSen = sentence[index1+1:]
                index2 = subSen.find(':')
                if index2<=0:
                    sentence = sentence[:index1] + sentence[index1+6:]
                else:
                    sentence = sentence[:index1]+subSen[index2+1:]
        return sentence
    
    # 去除@
    def removeAt(self,sentence):
        while True:
            if '@' not in sentence:
                break
            else:
                index = sentence.find('@')
                index1 = sentence[index:].find(' ')
                if index1<0:
                    sentence = sentence[:index]
                else:
                    sentence = sentence[:index]+sentence[index:][index1+1:]
        return sentence
    
    # 去除//
    def removeslashslash(self,sentence):
        while True:
            if '//' not in sentence:
                break
            else:
                index = sentence.find('//')
                sentence = sentence[:index]
        return sentence
    def removeTopic(self,sentence):
        while True:
            if '#' not in sentence:
                break
            else:
                signCount = sentence.count('#')
                if signCount<=1:
                    index = sentence.find('#')
                    sentence = sentence[:index]+sentence[index+1:]
                else:
                    index1 = sentence.find('#')
                    index2 = sentence[index1+1:].find('#')
                    sentence = sentence[:index1]+sentence[index1+1:][index2+1:]
        return sentence
    
    def removeEverything(self, sentence):        
        sentence = self.removeShortURL(sentence)
        sentence = self.removeslashslash(sentence)
        sentence = self.removeRepostWord(sentence)
        sentence = self.removeEmotions(sentence)
        sentence = self.removeReplyAt(sentence)        
        sentence = self.removeTopic(sentence)
        sentence = self.removeAt(sentence)
        return sentence
    
    def removeEverythingButEmotion(self, sentence):   
        sentence = self.removeslashslash(sentence)
        sentence = self.removeShortURL(sentence)
        sentence = self.removeRepostWord(sentence)
#         sentence = self.removeEmotions(sentence)
        sentence = self.removeReplyAt(sentence)        
        sentence = self.removeTopic(sentence)     
        sentence = self.removeAt(sentence)
        return sentence

if __name__=='__main__':
    ret = removeIrrelevant().removeEverything('//回复@荆歆1991  //@我要成为科学家 转发微博')    
    print ret