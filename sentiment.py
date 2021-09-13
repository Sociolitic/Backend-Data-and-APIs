import html,re ,contractions,emoji,unidecode
from nltk.tokenize import word_tokenize
from google_trans_new import google_translator
from nltk.stem.wordnet import WordNetLemmatizer
from textblob import TextBlob
import nltk
nltk.download("punkt")
nltk.download('wordnet')


def removeEmoji(sentence):
    sentence=emoji.demojize(sentence, delimiters=(" ","."))
    l=list(sentence)
    for pos,i in enumerate(l):
        if(not i.isalpha()):
            l[pos]=' '

    return ''.join(l)
def normalization(sentence,tokenize=True):
    sentence = html.unescape(sentence)
    regex = re.compile(r'[\n\r\t]')
    sentence = regex.sub(" ", sentence)
    sentence = re.sub(r'http\S+', '', sentence)
    sentence = re.sub('@[^\s]+','',sentence)
    try:
        lang = detect(sentence)
        if lang != 'en':
            translator = google_translator()
            sentence = translator.translate (sentence,lang_tgt='en')
    except :
        try:
            translator = google_translator()
            sentence = translator.translate (sentence,lang_tgt='en')
        except:
            pass
    sentence = sentence.lower()
    sentence = contractions.fix(sentence)
    sentence=removeEmoji(sentence)
    sentence = unidecode.unidecode(sentence)
    sent = word_tokenize(sentence)
    sent = (word for word in sent if word.isalpha())
    lemmatizer = WordNetLemmatizer()
    try:
        lem_sent = [lemmatizer.lemmatize(words_sent) for words_sent in sent]
        sentence = [' '.join(lem_sent)]
    except:
        sentence = [' '.join(sent)]
    return sentence[0]
def sentiment_analysis(tweet):
    tweet = normalization(tweet)
    def getSubjectivity(text):
        return TextBlob(text).sentiment.subjectivity
    def getPolarity(text):
        return TextBlob(text).sentiment.polarity
    TextBlob_Subjectivity =getSubjectivity(tweet)
    TextBlob_Polarity = getPolarity(tweet)
    def getAnalysis(score):
        if score < 0:
            return 'Negative'
        elif score == 0:
            return 'Neutral'
        else:
            return 'Positive'
    TextBlob_Analysis = getAnalysis(TextBlob_Polarity)
    return TextBlob_Analysis
