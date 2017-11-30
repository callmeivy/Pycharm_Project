#coding:UTF-8
'''
Created on 2016年4月11日

@author: Ivy

将json转为csv
'''
import os
import json
folder = 'E:\data'
files = []
for root, dirs, f in os.walk(folder):
    files = files + f
    print root, dirs, f
    print files

def js(fn):
    try:
        with open(fn, 'r') as f:
            contents = f.read()
            print contents
            f.close()
            return json.loads(contents)
    except:
        return ''
text = [js(f) for f in files]
for i in text:
    print text

