# -*- coding: utf-8 -*-
"""
Pre-processing

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
    
db = client["StockTwits"]
#call only what is needed, else it is going to be super heavy to load 
df = pd.DataFrame(db["twits"].find({}))


#%% Find bots and remove them from database




#%% get only messages with at meast 3 words 




#%% Deal with emojis 




#%% cashtag, linktag, usertag 



#%% Get balanced database 
