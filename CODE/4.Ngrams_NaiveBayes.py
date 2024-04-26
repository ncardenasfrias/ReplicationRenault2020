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
    
#%% call the database 
db = client["StockTwits"]
df = pd.DataFrame(list(db['twits_v2'].find({"sentiment": {"$ne": ""}, "n_three": 1, "is_bot": 0}, {"c_content": 1, "sentiment": 1})))


#%% Get balanced data set, sample =250k
sampler = RandomUnderSampler(sampling_strategy={-1: 125000, 1: 125000})
X_bal, y_bal = sampler.fit_resample(df['c_content'].values.reshape(-1, 1),df["sentiment"])

#%% use different types of 
