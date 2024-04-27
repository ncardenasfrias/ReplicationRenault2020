# -*- coding: utf-8 -*-
"""
Machine Learning - Other Algorithms

@author: ncardenafria
"""

#Basic packages 
import pandas as pd
import os
import time 
from datetime import datetime
#Mongo DB 
import pymongo 
from pymongo.mongo_client import MongoClient
#sklearn
from sklearn.model_selection import train_test_split, cross_validate
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import accuracy_score, matthews_corrcoef
from sklearn.feature_extraction.text import CountVectorizer
#balanced sample 
from imblearn.under_sampling import RandomUnderSampler
#Viz 
import seaborn as sns
sns.set(style="whitegrid")
import matplotlib.pyplot as plt

os.chdir("C:/Users/ncardenafria/Documents/GitHub/ReplicationRenault2020/")
export_on=False

#%% Connect to MongoDB and call the DB
#mongodb password stocktwits
uri = "mongodb+srv://nataliacardenas:stocktwits@twits.mgv4dfh.mongodb.net/?retryWrites=true&w=majority&appName=Twits"

# Create a new client and connect to the server
client = MongoClient(uri)

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
    
#%% call the database 
db = client["StockTwits"]
df = pd.DataFrame(list(db['twits_v2'].find({"sentiment": {"$ne": ""}, "n_three": 1, "is_bot": 0}, {"c_content": 1, "sentiment": 1})))


#%% Get balanced data set, sample =250k
sampler = RandomUnderSampler(sampling_strategy={-1: 125000, 1: 125000})
X_bal, y_bal = sampler.fit_resample(df['c_content'].values.reshape(-1, 1),df["sentiment"])
X_bal = X_bal.flatten()

#%% Initialize naive bayes
bayes = MultinomialNB(alpha=1.0, fit_prior=True, class_prior=None)

#%% CountVectorizer
n_grams = [(1,1), (1,2), (1,3), (1,4)]
names = ['Unigrams', 'Unigrams + Bigrams', 'Unigrams + Bigrams + Trigrams', 'Unigrams + Bigrams + Trigrams + 4-grams']

results = []
for n in range(len(n_grams)):
    #print(n_grams[n])
    start_t = time.time()                                                     
    vect = CountVectorizer(ngram_range=n_grams[n])
    X = vect.fit_transform(X_bal) #we get a huge sparse matrix 

    
    X_train, X_test, y_train, y_test = train_test_split(X, y_bal,
                                                     random_state=3456,
                                                     test_size=0.2)

    #get 10-fold CV scores
    scoring = {"acc":"accuracy", 'mcc': 'matthews_corrcoef'}
    scores = cross_validate(bayes, X_train, y_train, scoring=scoring, cv=10, return_train_score=True)
    acc_mean = round(scores['test_acc'].mean(), 3)
    mcc_mean = round(scores['test_mcc'].mean(),3)
    
    # get time for benchmarking
    end_t = time.time()
    delta_t = end_t-start_t 
    s = round(delta_t, 1)

    results.append({'Ngrams': names[n], 'Accuracy': acc_mean, 'MCC': mcc_mean, "Time":f"{s} sec"})

#%% Results
results = pd.DataFrame(results)
results.to_csv("REPORT/TABLES/bal_ngram.csv")

with open('REPORT/TABLES/bal_ngrams.tex', 'w') as tf:
     tf.write(results.to_latex(index=False, 
                 caption="Number of Ngrams and classification accuracy", 
                 label="tab:ngrams")) 


# Plot results
melted_results = results.melt(id_vars='Ngrams', var_name='Metric', value_name='Score')
melted_results = melted_results[melted_results["Metric"].isin(["Accuracy", "MCC"])]

if export_on:
    plt.figure(figsize=(10, 6))
    sns.barplot(data=melted_results, x="Ngrams", y="Score", hue="Metric", palette="crest")
    
    plt.xlabel("Ngrams")
    plt.ylabel("Score")
    plt.title("Number of Ngrams and classification accuracy")
    custom_labels = ['Unigrams', '+ Bigrams', '+ Trigrams', '+ 4-grams']
    plt.gca().set_xticklabels(custom_labels)
    plt.xticks(rotation=0)

    plt.legend()
    #plt.show()
    plt.savefig('REPORT/IMAGES/n_grams.png', bbox_inches='tight')