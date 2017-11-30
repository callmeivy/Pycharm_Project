#coding:UTF-8
'''
Created on 2014年3月6日

@author: hao
'''
import sys,os
import cProfile as profile
import pstats
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
import sklearn
import networkx as nx
import nltk
import jieba

class textrank():
    def __init__(self):
        self.sign = []
        self.path = os.path.abspath(os.path.dirname(sys.argv[0]))  
        dicFile = open(self.path+'/tools/NTUSD_simplified/signal.txt','r')
        signs = dicFile.readlines()
        for word in signs:
            ss = word.strip().replace('\r\n','')
            self.sign.append(ss)
        dicFile.close()
        
    def textrank(self,content):
        if len(content)<=0:
            return ''
        sents = list(jieba.cut(content))
        vect = TfidfVectorizer(min_df=1,tokenizer=jieba.tokenize)
        tfidf = vect.fit_transform(sents)
        tfidf_graph = tfidf*tfidf.T
        nx_graph = nx.from_scipy_sparse_matrix(tfidf_graph)
        scores = nx.pagerank(nx_graph)
        res = sorted(((scores[i],i) for i,s in enumerate(sents)), reverse=True)
        summary = [sents[i] for _,i in sorted(res[:5])]
        for i in summary:
            if i in self.sign:
                summary.remove(i)
        if len(summary) >3:            
            return ','.join(summary[:3])
        else:
            return ','.join(summary)

'''
prof = profile.Profile()
prof.run('test4()')
stats = pstats.Stats(prof)
stats.strip_dirs().print_stats()
'''
if __name__=='__main__':
    ret = textrank().textrank('是的。是大家阿塑。料..\....袋到')
    print ret

