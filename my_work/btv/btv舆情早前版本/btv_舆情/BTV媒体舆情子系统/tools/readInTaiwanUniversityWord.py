#coding:UTF-8
'''
Created on 2014年2月21日

@author: hao
'''
import os,sys
class readWord():
    def __init__(self):
        self.complimentWord = []
        self.criticalWord = []
        self.path = os.path.abspath(os.path.dirname(sys.argv[0]))  
         

    def readWordList(self):
#         dicFile = open(self.path+'/NTUSD_simplified/NTUSD_negative_simplified.txt','r')
        dicFile = open(self.path+'/tools/NTUSD_simplified/negative.txt','r')
        words = dicFile.readlines()
        for word in words:
            ss = word.strip().replace('\r\n','')
            self.criticalWord.append(ss)
        dicFile.close()
        
        dicFile = open(self.path+'/tools/NTUSD_simplified/positive.txt','r')
#         dicFile = open(self.path+'/NTUSD_simplified/NTUSD_positive_simplified.txt','r')
        words = dicFile.readlines()
        for word in words:
            ss = word.strip().replace('\r\n','')
            self.complimentWord.append(ss)
        dicFile.close()
        

        return (self.complimentWord, self.criticalWord)
        
if __name__=='__main__':
    (good, bad) = readWord().readWordList()
    for i in good:
        print i
    for i in bad:
        print i