#coding: UTF-8
'''
__author__ = 'Ivy'
'''

# -*- coding:utf8 -*-
import jieba
import urllib2
import jieba.posseg as pseg
if __name__ == '__main__':
    url_get_base = "http://api.ltp-cloud.com/analysis/?"
    api_key = 'd5L8f838PDpfYH0DKvkcdFEMKE9FVU1kbY1QHZoq'
    text = '“既然年轻人喜欢网络，敌对势力又用网络丑化我们的领袖，攻击毛泽东思想，抹黑我们的英雄，我们就要善于用网络回击。”毛新宇谈到，这其中一个重要的举措就是，在网上阵地支持鼓励宣传马克思主义哲学、宣传毛泽东思想、宣传中国特色社会主义理论。'
    format = 'json'
    pattern = 'pos'
    result = urllib2.urlopen("%sapi_key=%s&text=%s&format=%s&pattern=%s" % (url_get_base,api_key,text,format,pattern))
    content = result.read().strip()
    print content

    text = pseg.cut(text)
    for i in text:
        print i.word, i.flag

