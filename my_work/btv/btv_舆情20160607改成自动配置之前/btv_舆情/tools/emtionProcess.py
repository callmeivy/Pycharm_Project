#coding:UTF-8
'''
Created on 2014年3月5日

@author: hao
'''
import jieba
from readInTaiwanUniversityWord import readWord
from readInEmotions import readEmotion

class emotionProcess():
    def __init__(self):
        self.good = []
        self.bad = []
        self.positiveEmotions = []
        self.negativeEmotions = []
        (self.good, self.bad) = readWord().readWordList()
        (self.positiveEmotions, self.negativeEmotions) = readEmotion().readEmotions()

    def processSentence(self, sentence):
        out = jieba.cut(sentence)
        sentiCount = 0
        emotionBuffer = ''
        emotionBufferStart = 0
        positiveWord = []
        negativeWord = []
        for word in out:
            if word =='[':
                emotionBufferStart = 1
            elif word == ']':
                emotionBuffer += word
                emotionBufferStart = 2
                
            if emotionBufferStart == 1:
                emotionBuffer+=word
            elif emotionBufferStart == 2:
                if emotionBuffer in self.positiveEmotions:
                    sentiCount+=2
#                     print ("好表情: "+ emotionBuffer)
                elif emotionBuffer in self.negativeEmotions:
                    sentiCount-=2
#                     print ("坏表情: "+ emotionBuffer)
                else:
#                     print ("中立表情: "+ emotionBuffer)
                    pass
                emotionBufferStart = 0
                emotionBuffer = ''
                
            else:        
                if word in self.good:
                    sentiCount+=1
#                     print ("褒义词: "+ word)
                    positiveWord.append(word)
                elif word in self.bad:
                    sentiCount-=1
#                     print ("贬义词: "+ word)
                    negativeWord.append(word)
#         print sentiCount
        if sentiCount>0:
            return (positiveWord, sentiCount)
        elif sentiCount<0:
            return (negativeWord, sentiCount)
        else:
            return ([],0)

if __name__=='__main__':
    test = emotionProcess()
    (ret1,ret2) = test.processSentence('哦，完蛋[浮云]那傻了，老美超市[浮云]')
    print ret1
    x = ','.join(ret1)
    print x
    out = tuple(ret1)
    print out
    print ret2