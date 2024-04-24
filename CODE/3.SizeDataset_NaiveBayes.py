# -*- coding: utf-8 -*-
"""
Machine Learning - Benchmark with Naive Bayes, 10-fold CV
Replication of Table 1 of the paper. 
Check on balanced and unbalanced data set AC and MCC of naive bayes with 10-fold CV for different sample sizes for the training dataset. 

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

os.chdir("C:/Users/ncardenafria/Documents/GitHub/ReplicationRenault2020/")

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
    

#%% Call data base : only entries with declared sentiment
db = client["StockTwits"]
df = pd.DataFrame(list(db['twits_v2'].find({"sentiment":{"$ne":""}})))

#call only what is needed, else it is going to be super heavy to load 
#df = pd.DataFrame(db["twits"].find({}))

#%% Count Vectorizer 
vect = CountVectorizer()
X = vect.fit_transform(df["content"])


#%% Unbalanced
bayes = MultinomialNB(alpha=1.0, fit_prior=True, class_prior=None)

results = []
for i in [500,1000]: 
     start_t = time.time()
     X_train, X_test, y_train, y_test = train_test_split(X, df['sentiment'],
                                                         random_state=1234,
                                                         train_size=i, test_size=0.2)

     scoring = {"acc":"accuracy", 'mcc': 'matthews_corrcoef'}
     scores = cross_validate(bayes, X_train, y_train, scoring=scoring, cv=10, return_train_score=True)
     acc_mean = round(scores['test_acc'].mean(), 3)
     mcc_mean = round(scores['test_mcc'].mean(),3)
     
     end_t = time.time()
     delta_t = end_t-start_t
     m = delta_t // 60
     s = round(delta_t % 60, 1)

     results.append({'Sample Size': i, 'Accuracy': acc_mean, 'MCC': mcc_mean, "Time":f"{m} min, {s} sec"})
    

#%% Balanced 

min(db["twits_v2"].count_documents({"sentiment":1}), db["twits_v2"].count_documents({"sentiment":-1}))
sampler = RandomUnderSampler(sampling_strategy='majority')
X_bal, y_bal = sampler.fit_resample(X,df["sentiment"])

len(y_bal) == 2*min(db["twits_v2"].count_documents({"sentiment":1}), db["twits_v2"].count_documents({"sentiment":-1}))

results_bal = []
for i in [500,1000]: 
     start_t = time.time()
     X_train, X_test, y_train, y_test = train_test_split(X_bal, y_bal,
                                                         random_state=1234,
                                                         train_size=i, test_size=0.2)

     scoring = {"acc":"accuracy", 'mcc': 'matthews_corrcoef'}
     scores = cross_validate(bayes, X_train, y_train, scoring=scoring, cv=10, return_train_score=True)
     acc_mean = round(scores['test_acc'].mean(), 3)
     mcc_mean = round(scores['test_mcc'].mean(),3)
     
     end_t = time.time()
     delta_t = end_t-start_t
     m = delta_t // 60
     s = round(delta_t % 60, 1)

     results_bal.append({'Sample Size': i, 'Accuracy': acc_mean, 'MCC': mcc_mean, "Time":f"{m} min, {s} sec"})
    
    
    


