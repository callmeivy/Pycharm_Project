#-*- encoding:utf-8 -*-
from  textrank4zh import TextRank4Keyword,TextRank4Sentence
import codecs

# file = r"C:\Users\Administrator\Desktop\02.txt"
# text = codecs.open(file, 'r', 'utf-8').read()
text = ["中共中央总书记、国家主席、中央军委主席习近平当日给内蒙古自治区苏尼特右旗乌兰牧骑的队员们回信","曾是腐败重灾区的辽宁再添一虎，党的十八大以来，除了涉贿选案被处分的部分省部级领导之外，辽宁已有省委原书记王珉、省委原常委苏宏章、省人大常委会原副主任王阳、郑玉焯、李文科，原副省长刘强、省政协原副主席陈铁新等七人落马。"]
word = TextRank4Keyword()

word.analyze(text, window=2, lower=True)
w_list = word.get_keywords(num=20, word_min_len=1)

print '关键词:'
print
for w in w_list:
    print w.word, w.weight
print
phrase = word.get_keyphrases(keywords_num=5, min_occur_num=2)

# print '关键词组:'
# print
# for p in phrase:
#     print p
# print
# sentence = TextRank4Sentence()
#
# sentence.analyze(text, lower=True)
# s_list = sentence.get_key_sentences(num=3, sentence_min_len=5)
#
# print '关键句:'
# print
# for s in s_list:
#     print s.sentence, s.weight
# print