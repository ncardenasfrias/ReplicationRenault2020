# -*- coding: utf-8 -*-
"""
Machine Learning - Other Algorithms

@author: ncardenafria
"""

import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk import pos_tag
import emoji
import re
from pymongo import MongoClient
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_validate
import string

# Connect to MongoDB
client = MongoClient("mongodb+srv://nataliacardenas:stocktwits@twits.mgv4dfh.mongodb.net/?retryWrites=true&w=majority&appName=Twits")
db = client["StockTwits"]
df = pd.DataFrame(list(db["twits"].find({"sentiment": {"$ne": ""}}, {"content": 1, "sentiment": 1})))

nltk.download('averaged_perceptron_tagger')
nltk.download('punkt')
nltk.download('stopwords')

def remove_punctuation(text):
    translator = str.maketrans('', '', string.punctuation)
    return text.translate(translator)

def stem_text(text):
    ps = PorterStemmer()
    return ' '.join([ps.stem(word) for word in word_tokenize(text)])

def handle_emojis(text):
    text = emoji.demojize(text)
    text = text.replace(':', ' ')
    return text

def remove_stopwords(text):
    stop_words = set(stopwords.words('english'))
    return ' '.join([word for word in word_tokenize(text) if word.lower() not in stop_words])

def apply_pos_tags(text):
    words = word_tokenize(text)
    pos_tags = pos_tag(words)
    return ' '.join([f"{word}_{tag}" for word, tag in pos_tags])

def emojis_and_punctuation(text):
    text = emoji.demojize(text)
    text = re.sub(r'[^\w\s]', '', text)
    return text

# Function to preprocess, train, and evaluate
def preprocess_train_evaluate(preprocess_fn, df, vectorizer, model):
    df['processed_content'] = df['content'].apply(preprocess_fn)
    X = vectorizer.fit_transform(df['processed_content'])
    y = df['sentiment']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    scores = cross_validate(model, X_train, y_train, scoring=['accuracy', 'matthews_corrcoef'], cv=10)
    accuracy = scores['test_accuracy'].mean()
    mcc = scores['test_matthews_corrcoef'].mean()
    return accuracy, mcc

vect = CountVectorizer(stop_words="english")
bayes = MultinomialNB()

results = {}
results['Benchmark'] = preprocess_train_evaluate(lambda x: x, df, vect, bayes)
results['Punctuation'] = preprocess_train_evaluate(remove_punctuation, df, vect, bayes)
results['Stem'] = preprocess_train_evaluate(stem_text, df, vect, bayes)
results['Emoticons'] = preprocess_train_evaluate(handle_emojis, df, vect, bayes)
results['StopWords'] = preprocess_train_evaluate(remove_stopwords, df, vect, bayes)
results['PosTagging'] = preprocess_train_evaluate(apply_pos_tags, df, vect, bayes)
results['Emojis + Punctuation'] = preprocess_train_evaluate(emojis_and_punctuation, df, vect, bayes)

results_df = pd.DataFrame(results, index=['Accuracy', 'MCC']).T
print(results_df)
