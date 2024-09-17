import pandas as pd
import string
import re
from nltk.corpus import stopwords
from textblob import Word
from autocorrect import Speller
import pandas as pd
import datetime

import nltk

nltk.download("stopwords")
nltk.download("wordnet")

lookup_dict = {
    "ur": "your",
    "wbu": "what about you",
    "imo": "in my opinion",
    "lol": "laugh out loud",
    "btw": "by the way",
    "idk": "I don't know",
    "omg": "oh my god",
    "thx": "thanks",
    "tldr": "too long; didn't read",
    "brb": "be right back",
    "afk": "away from keyboard",
    "fyi": "for your information",
    "imho": "in my humble opinion",
    "btw": "by the way",
    "gtg": "got to go",
    "ttyl": "talk to you later",
    "np": "no problem",
    "yw": "you are welcome",
    "aint": "is not",
    "arent": "are not",
    "couldnt": "could not",
    "hadnt": "had not",
    "hasnt": "has not",
    "isnt": "is not",
    "mightnt": "might not",
    "mustnt": "must not",
    "neednt": "need not",
    "shant": "shall not",
    "shouldnt": "should not",
    "wasnt": "was not",
    "werent": "were not",
    "wont": "will not",
    "wouldnt": "would not",
    "didnt": "did not",
    "doesnt": "does not",
    "dont": "do not",
    "havent": "have not",
}


def standardize_text(input_text):
    words = input_text.split()
    new_words = []
    for word in words:
        if re.match(r"^https?:\/\/.*[\r\n]*", word):
            continue
        # word = re.sub(r'\.\.\.', ' ', word)

        if word in lookup_dict:
            word = lookup_dict[word.lower()]

        new_words.append(word)

    new_text = " ".join(new_words)

    return new_text


stop_words = stopwords.words("english")
punctuations = set(string.punctuation)


def preprocessing(text):
    text = str(text)
    # Lowercase
    text = text.lower()

    # Punctuation Removal
    preprocessed_text = ""
    for ch in text:
        if ch not in punctuations:
            preprocessed_text += ch
        else:
            preprocessed_text += " "
    text = preprocessed_text

    # Stopwords Removal
    new_words = []
    for word in text.split():
        if (
            re.match(r"^[a-zA-Z\s.]+$", word)
            and len(word) > 2
            and word not in stop_words
        ):
            new_words.append(word)
    text = " ".join(new_words)

    # Text Standardization
    text = standardize_text(text)

    # Spelling Correction
    # spell = Speller()
    # text = spell(text)

    # Lemmatization
    result = " ".join([Word(word).lemmatize() for word in text.split()])
    return result


def getDateTime():
    current_date = datetime.date.today().strftime("%Y-%m-%d")
    return current_date


def executeByAttribute(rawFilePath, attribute="description", sourceId=1):
    current_date = getDateTime()
    columns = [
        "id",
        "title",
        "description",
        "book_cover",
        "image_url",
        "release_date",
        "publisher",
        "number_of_pages",
        "price",
        "authors",
        "rating",
        "number_of_ratings",
        "number_of_reviews",
        "preprocessed_description",
    ]

    if sourceId == 1:  # thrift-books
        df = pd.read_csv(rawFilePath, names=columns)
    else:
        df = pd.read_csv(rawFilePath)

    new_attribute = f"preprocessed_{attribute}"
    df[new_attribute] = df[attribute].apply(preprocessing)

    if sourceId == 1:  # thrift-books
        df.to_csv(
            f"dataset/thrift-books/preprocessed/thrift-books-{current_date}.csv",
            header=False,
            index=False,
        )
    if sourceId == 3:  # goodreads
        df.to_csv(
            f"dataset/goodreads/preprocessed/goodreads-{current_date}.csv", index=False
        )
    if sourceId == 4:  # bx-books
        df.to_csv(
            f"dataset/book-crossing/preprocessed/book-crossing-{current_date}.csv",
            index=False,
        )

    print("Done preprocessing")
    return df
