# -*- coding: utf-8 -*-
"""
Scrapping StockTwits script

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
#scrapping from StockTwits 
import json 
import requests 

os.chdir("C:/Users/ncardenafria/Documents/GitHub/ReplicationRenault2020/")

#%% Connect to MongoDB and create the DB
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
    
    
#create db 
db = client['StockTwits']
db['twits'].create_index([("id",  pymongo.ASCENDING)], unique=True)


#%% Scrapping from StockTwits 

headers = {'User-Agent': 'Mozilla/5.0 Chrome/39.0.2171.95 Safari/537.36'}

initial_docs = db["twits"].count_documents({})
initial_time = time.time()

#crash BRK 
maxid= 251813842 # need change range i to start with 1 to restart
maxid= 49003918 # need change range i to start with 1 to restart
maxid= 13365320
maxid= 45403059
maxid= 3942038
maxid= 2978814
maxid= 1177072
maxid= 49003918



for tick in ["BRK.B", "JPM", "BAC", "MS", "WFC", "GS"]:
    
    for i in range(1, 10000000):
        
        if i == 0:  #get last batch of twits about Apple (latest id)
            url = f"https://api.stocktwits.com/api/2/streams/symbol/{tick}.json"
        else:       #get following batches of size 30
            url = f"https://api.stocktwits.com/api/2/streams/symbol/{tick}.json?max=" + str(maxid)
    
        print(url)
    
        r = requests.get(url, headers=headers)  #scrapping with requests 
        data = json.loads(r.content)
    
        maxid = data["cursor"]["max"] 
        
        #time sleep, avoid being blocked by StockTwits
        time.sleep(0.3)
    
        #iterate over messages and fill the information in dictionary to build db on mongo 
        for m in data["messages"]:
            date = m["created_at"]
            date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
            
            content = m["body"]
            twitid = m["id"]
            #symbol = m["symbols"]["symbol"]
            user = m["user"]["username"]
            
            symbols = []
            for symbol in m["symbols"]:
                symbols.append(symbol["symbol"])
            
            sentiment = ""      #get declared sentiment if any 
            if "Bearish" in str(m["entities"]["sentiment"]):
                sentiment = -1
            elif "Bullish" in str(m["entities"]["sentiment"]):
                sentiment = 1
           
            try: #push into mongo db 
                db["twits"].insert_one({"id":twitid, "date":date, "content":content , "sentiment":sentiment, "user":user, 'symbol':symbols})
            except: 
                pass
        
        if date < datetime(2009,12,31): 
            break 
        
final_docs = db["twits"].count_documents({})
final_time = time.time()

delta_docs = final_docs-initial_docs
delta_time = final_time - initial_time

#del data, date, headers, i, tick, m, maxid, r, sentiment, uri, url, client, content, twitid
 

#%% Call the db from MongoDB 
#df= pd.DataFrame(list(db['twits'].find({"sentiment": {"$ne": ""}})))

