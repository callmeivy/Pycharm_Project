#coding:UTF-8
# import re
# f = open('E:\\ars10768@20150121144000.txt', 'r')
# for line in open('E:\\ars10768@20150121144000.txt'):
#     line = f.readline()
#     m = re.findall('''\=["]?(.*?)["]+''',str(line))
#     print 'm',m
#
# f.close()

# from bs4 import BeautifulSoup
#
# # soup = BeautifulSoup(html_doc)
# soup = BeautifulSoup(open("E:\\ars10768@20150121144000.html"))
# print soup
# print soup.wic['date']
# print soup.wic['cardnum']




# 将txt文件名后缀改了之后，在文档最开始加上‘<任意单词>’，在文档结尾加上‘</任意单词>’
# i=0
#
# import xml.etree.ElementTree as ET
# f=open('E:\\ars10768@20150121144000.xml')
#
# tree = ET.ElementTree(file='E:\\ars10768@20150121144000.xml')
# root = tree.getroot()
# A_Paramter = root.findall(".//A")
# print 'eles_Paramter',A_Paramter
# if i<10:
#     for A in A_Paramter:
#         print A.attrib
#         e = A.get('e')
#         print e

import xml.etree.ElementTree as ET
for line in open('E:\\test.xml').readlines():
    print line
    # root = ET.fromstring(line)
    # print "root",root
    # for child in root.iter('A'):
    #     print child.tag,child.attrib
    tree = ET.fromstring(line)
    # root = tree.find("GHApp")
    # root = tree.find("Restrictions")
    root = tree.getroot()
    for child in root:
        print child

