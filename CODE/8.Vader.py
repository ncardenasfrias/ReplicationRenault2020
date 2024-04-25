# -*- coding: utf-8 -*-
"""
Machine Learning - Other Algorithms

@author: ncardenafria
"""

#Basic packages 
import pandas as pd
import os
import datetime
import time 
#Mongo DB 
import pymongo 
from pymongo.mongo_client import MongoClient
#Vader
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
#sklearn
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
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

df.head(1)
df.columns
df["sentiment"].isna().sum()         # 0, only entries with declared sentiment 

full_sample = len(df)                       # 923638 entries
full_positive = len(df[df["sentiment"]==1]) # 580658, 62.87%
full_negative = len(df[df["sentiment"]==-1]) # 342980, 37.13%


#%% Get balanced data set, sample =250k
sampler = RandomUnderSampler(sampling_strategy={-1: 125000, 1: 125000})
X_bal, y_bal = sampler.fit_resample(df['c_content'].values.reshape(-1, 1),df["sentiment"])

#%% Vader Sentiment
vader = SentimentIntensityAnalyzer()

def vader_sentiment(content_field):
    score = vader.polarity_scores(content_field)["compound"]
    if score > 0: 
        sent = 1
    else: 
        sent = -1
    return sent

#%% Get Accuracy
x_train, x_test, y_train, y_test = train_test_split(X_bal, y_bal, random_state=3456, test_size=0.2)

y_fit = [vader_sentiment(i) for i in x_test]

start = time.time()
vader_ac = accuracy_score(y_test, y_fit) 
end = time.time()

time_delta = end-start 

vader_tab = {"Model":"Vader", "Accuracy":round(vader_ac*100,3), "Time":f"{round(time_delta,2)} sec"}
vader_tab= pd.DataFrame([vader_tab])
with open('REPORT/TABLES/vader.tex', 'w') as tf:
    tf.write(vader_tab.to_latex(index=False, 
                     caption="Vader - Performance",
                     label="tab:vader"))

#%% update database since 2023

sample = pd.DataFrame(list(db["twits_v2"].find({"date": {"$gte": datetime.datetime(2023,1,1)}}, {"c_content": 1,"_id": 1})))

def update_vader(sample):
    i = 0
    for document in sample:
        content = document["c_content"]
        sent = vader_sentiment(content)
        
        db["twits_v2"].update_one({"_id": document["_id"]}, {"$set": {"vader": sent}})
        
        print(i)
        i += 1

update_vader(sample)


