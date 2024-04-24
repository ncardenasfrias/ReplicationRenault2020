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
#Vader
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
#sklearn
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

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
    
db = client["StockTwits"]
#call only what is needed, else it is going to be super heavy to load 
#df = pd.DataFrame(db["test"].find({}))

df = pd.DataFrame(list(db['twits'].find({"sentiment":{"$ne":""}})))

db["twits_v2"].count_documents({"symbol":"AAPL"}) #367588
db["twits_v2"].count_documents({"symbol":"META"}) #127111
db["twits_v2"].count_documents({"symbol":"AMZN"}) #150429
db["twits_v2"].count_documents({"symbol":"MSFT"}) #59031
db["twits_v2"].count_documents({"symbol":"GOOG"}) #72912
db["twits_v2"].count_documents({"symbol":"NVDA"}) #39577
db["twits_v2"].count_documents({"symbol":"TSLA"}) #66369



vader = SentimentIntensityAnalyzer()

def vader_sentiment(content_field):
    score = vader.polarity_scores(content_field)["compound"]
    if score > 0: 
        sent = 1
    else: 
        sent = -1
    return sent

df = pd.DataFrame(list(db["twits_v2"].find({"sentiment":{"$ne":""}})))
x_train, x_test, y_train, y_test = train_test_split(df['content'], df["sentiment"], random_state=1234, test_size=0.2)

i=0
for document in db["twits_v2"].find({}):
    content = document["content"]
    vader = vader_sentiment(content)
    
    db["twits_v2"].update_one({"_id": document["_id"]}, {"$set": {"vader": vader}})
    #to follow 
    print(i)
    i+=1