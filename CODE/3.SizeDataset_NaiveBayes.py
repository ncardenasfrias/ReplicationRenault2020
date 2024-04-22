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
    

#%% Call data base
db = client["StockTwits"]
df = pd.DataFrame(list(db['twits'].find({"sentiment":{"$ne":""}})))

#call only what is needed, else it is going to be super heavy to load 
#df = pd.DataFrame(db["twits"].find({}))

#%% Get something balanced 

#%% 500
#%% 1000
#%% 2500
#%% 5000
#%% 10000
#%% 25000
#%% 50000
#%% 100000
#%% 250000
#%% 500000
#%% 1000000



