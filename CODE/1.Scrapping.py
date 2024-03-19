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

for tick in ["BRK.B", "JPM", "BAC", "MS", "WFC", "GS"]:
    
    for i in range(0, 10000000):
        if i == 0:  #get last batch of twits about Apple (latest id)
            url = f"https://api.stocktwits.com/api/2/streams/symbol/{tick}.json"
        else:       #get following batches of size 30
            url = f"https://api.stocktwits.com/api/2/streams/symbol/{tick}.json?max=" + str(maxid)
    
        print(url)
    
        r = requests.get(url, headers=headers)  #scrapping with requests 
        data = json.loads(r.content)
    
        maxid = data["cursor"]["max"] 
        
        #time sleep, avoid being blocked by StockTwits
        time.sleep(1)
    
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

del data, date, headers, i, tick, m, maxid, r, sentiment, uri, url, client, content, twitid
 

#%% Call the db from MongoDB 
df= pd.DataFrame(list(db['twits'].find({"sentiment": {"$ne": ""}})))

