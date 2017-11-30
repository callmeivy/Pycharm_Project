#coding=UTF-8
import numpy as np
import pyximport
pyximport.install()
import sys
# sys.path.append('E:\ctvit\lab\Code\Pycharm Project\\nlp')
from nlp.lda0811.lda.datasets import load_reuters
from nlp.lda0811.lda.datasets import load_reuters_vocab
from nlp.lda0811.lda.datasets import load_reuters_titles
from nlp.lda0811.lda.lda import LDA
X = load_reuters()
vocab = load_reuters_vocab()
titles = load_reuters_titles()
print X.shape
print X.sum()

model = LDA(n_topics=20, n_iter=1500, random_state=1)
model.fit(X)  # model.fit_transform(X) is also available
topic_word = model.topic_word_  # model.components_ also works
n_top_words = 8
for i, topic_dist in enumerate(topic_word):
    topic_words = np.array(vocab)[np.argsort(topic_dist)][:-n_top_words:-1]
    print('Topic {}: {}'.format(i, ' '.join(topic_words)))