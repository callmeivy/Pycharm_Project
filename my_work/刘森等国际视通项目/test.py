#coding:utf-8
from sklearn.feature_extraction.text import TfidfVectorizer,CountVectorizer

# Two sets of documents
# plays_corpus contains all documents in your corpus *including Romeo and Juliet*
plays_corpus = ['This is Romeo and Juliet','this is another play','and another','and one more']

#romeo is a list that contains *just* the text for Romeo and Juliet
romeo = [plays_corpus[0]] # must be in a list even if only one object

# Initialise your TFIDF Vectorizer object
tfidf_vectorizer = TfidfVectorizer()

# Now create a model by fitting the vectorizer to your main plays corpus, this creates an array of TFIDF scores
model = tfidf_vectorizer.fit_transform(plays_corpus)

romeo_scored = tfidf_vectorizer.transform(romeo) # note - .fit() not .fit_transform

terms = tfidf_vectorizer.get_feature_names()

scores = romeo_scored.toarray().flatten().tolist()

data = list(zip(terms,scores))

sorted_data = sorted(data,key=lambda x: x[1],reverse=True)

print sorted_data[:5]