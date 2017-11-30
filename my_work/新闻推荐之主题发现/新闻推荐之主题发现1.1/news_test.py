#coding=UTF-8
import nltk
text=nltk.word_tokenize("And now for something completely different")
word_chara = nltk.pos_tag(text)
print word_chara
print type(word_chara)
valid = ['NN','NNS','NNP','NNPS']
word_box = [word for word,chara in word_chara if chara in valid]
print word_box