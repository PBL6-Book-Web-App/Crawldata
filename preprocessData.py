import pandas as pd
import string
import re
from nltk.corpus import stopwords
from textblob import Word
from autocorrect import Speller
import pandas as pd
import datetime


lookup_dict = {
    'nlp': 'natural language processing',
    'ur': 'your',
    'wbu': 'what about you',
    'imo': 'in my opinion',
    'lol': 'laugh out loud',
    'btw': 'by the way',
    'idk': "I don't know",
    'omg': 'oh my god',
    'thx': 'thanks',
    'tldr': 'too long; didn\'t read',
    'brb': 'be right back',
    'afk': 'away from keyboard',
    'fyi': 'for your information',
    'imho': 'in my humble opinion',
    'btw': 'by the way',
    'gtg': 'got to go',
    'ttyl': 'talk to you later',
    'np': 'no problem',
    'yw': 'you’re welcome',
    'aint': 'is not',
    'arent': 'are not',
    'couldnt': 'could not',
    'hadnt': 'had not',
    'hasnt': 'has not',
    'isnt': 'is not',
    'mightnt': 'might not',
    'mustnt': 'must not',
    'neednt': 'need not',
    'shant': 'shall not',
    'shouldnt': 'should not',
    'wasnt': 'was not',
    'werent': 'were not',
    'wont': 'will not',
    'wouldnt': 'would not',
    'didnt': 'did not',
    'doesnt': 'does not',
    'dont': 'do not',
    'havent': 'have not',
    'mr.': 'mr',
    'dr.': 'dr',
    'mrs.': 'mrs',
    'ms.': 'ms',
    'jr.': 'jr',
    'sr.': 'sr',
    'prof.': 'prof',
    'etc.': 'etc',
    'e.g.': 'eg',
    'i.e.': 'ie',
    'u.s.': 'us',
    'u.k.': 'uk',
    'n.a.': 'na',
    'etc.': 'etc'
}

def standardize_text(input_text):
    words = input_text.split()
    new_words = []
    for word in words:
        word = re.sub(r'[^\w\s.]', '', word)
        word = re.sub(r'\.\.\.', ' ', word)

        if word in lookup_dict:
            word = lookup_dict[word.lower()]
        new_words.append(word)

    new_text = " ".join(new_words)

    return new_text

stop_words = stopwords.words('english')

i = 0
def preprocessing(text):
    global i
    text = str(text)
    # Lowercase
    text = text.lower()

    # Punctuation Removal
    punctuations = set(string.punctuation)
    preprocessed_text = ''
    for ch in text:
        if ch == '.' or ch not in punctuations:
            preprocessed_text += ch
        else:
            preprocessed_text += ' '
    text = preprocessed_text

    # Stop words removal
    text = ' '.join(w for w in text.split() if w not in stop_words)

    # Text Standardization
    text = standardize_text(text)

    i = i + 1

    # Spelling Correction
    spell = Speller()
    text = spell(text)

    # Lemmatization
    result = " ".join([Word(word).lemmatize() for word in text.split()])
    return result

def getDateTime():
    current_date = datetime.date.today().strftime("%Y-%m-%d")
    return current_date

def executeByAttribute(rawFilePath, attribute = 'description'):
    current_date = getDateTime()
    columns = ['id', 'title', 'description', 'book_cover', 'image_url', 'release_date', 'publisher', 'number_of_pages', 'price', 'authors', 'rating', 'number_of_ratings', 'number_of_reviews']
    df = pd.read_csv(rawFilePath, names = columns)
    print(df)
    df[attribute] = df[attribute].apply(preprocessing)
    df.to_csv(f'dataset/thrift-books/preprocessed/thrift-books-{current_date}.csv', mode = 'a', header=False, index=False)
    print('Done preprocessing')
    return df

def executeByListAttributes(rawFilePath, attributes):
    current_date = getDateTime()
    columns = ['id', 'title', 'description', 'book_cover', 'image_url', 'release_date', 'publisher', 'number_of_pages', 'price', 'authors', 'rating', 'number_of_ratings', 'number_of_reviews']
    df = pd.read_csv(rawFilePath, names = columns)
    df[attributes] = df[attributes].applymap(preprocessing)
    df.to_csv(f'dataset/thrift-books/preprocessed/thrift-books-{current_date}.csv', mode = 'a', header=False, index=False)
    print('Done preprocessing')
    return df
