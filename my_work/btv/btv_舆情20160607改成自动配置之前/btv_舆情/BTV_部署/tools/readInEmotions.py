#coding:UTF-8
'''
Created on 2014年2月26日

@author: hao
'''
import os,sys

class readEmotion():
    def __init__(self):
        self.positiveEmotion = []
        self.negativeEmotion = []
        self.path = os.path.abspath(os.path.dirname(sys.argv[0]))  

    def readEmotions(self):
        dicFile = open(self.path+'/tools/NTUSD_simplified/negativeEmotions.txt','r')
        emotions = dicFile.readlines()
        for emotion in emotions:
            temp = emotion.strip().replace('\r\n','').decode('utf8')
            self.negativeEmotion.append(temp)
        dicFile.close()
        
        dicFile = open(self.path+'/tools/NTUSD_simplified/positiveEmotions.txt','r')
        emotions = dicFile.readlines()
        for emotion in emotions:
            temp = emotion.strip().replace('\r\n','').decode('utf8')
            self.positiveEmotion.append(temp)
        dicFile.close()
        
        return (self.positiveEmotion, self.negativeEmotion)
    
if __name__=='__main__':
    (good, bad) = readEmotion().readEmotions()
    for i in good:
        print i
    for i in bad:
        print i