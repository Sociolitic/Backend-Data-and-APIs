import numpy as np
import pickle
MultinomialNB_model =  pickle.load(open("MultinomialNB.dump","rb"))
SVC_model =  pickle.load(open("SVC.dump","rb"))
features = pickle.load(open("feature.pkl","rb"))
import re
import unidecode
import html
import contractions
import nltk
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize as wt

def preprocess(tweet):
    tweet = re.sub(r'http[^\s]+', '', tweet)
    tweet = re.sub('@[^\s]+','',tweet)
    tweet = re.sub('#[^\s]+','',tweet)
    tweet = re.sub('[^A-Za-z]', ' ', tweet)
    tweet = html.unescape(tweet)
    tweet = tweet.lower()
    tokenized_tweet = wt(tweet)
    tweet = contractions.fix(tweet)
    tweet = unidecode.unidecode(tweet)
    sent = (word for word in tokenized_tweet if word.isalpha())
    sent = [word for word in sent if word not in set(stopwords.words('english'))]
    lemmatizer = WordNetLemmatizer()
    lem_sent = [lemmatizer.lemmatize(words_sent) for words_sent in sent]
    tweet = " ".join(lem_sent)
    return tweet

def is_spam(text):
    text = preprocess(text)
    x = features.transform([text])
    return bool(MultinomialNB_model.predict(x) and SVC_model.predict(x))
