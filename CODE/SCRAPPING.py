# -*- coding: utf-8 -*-
"""
Scrappingd Stock Twits 

@author: ncardenafria
"""

#Basic packages 
import pandas as pd
import os
import time 
#Mongo DB 
import pymongo 
from pymongo.mongo_client import MongoClient
#scrapping from StockTwits 
import json 
import requests 

#os.chdir("/Users/nataliacardenasf/Documents/GitHub/FTD_NCF/13. Applied Big Data")
os.chdir("C:/Users/ncardenafria/Documents/GitHub/FTD_NCF/13. Applied Big Data/")

#%% Connect to MongoDB
#mongodb password stocktwits
uri = "mongodb+srv://nataliacardenas:stocktwits@cluster0.selymub.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
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

j=0
for i in range(0, 10000000):
    if i == 0:  #get last batch of twits about Apple (latest id)
        url = "https://api.stocktwits.com/api/2/streams/symbol/BRK.B.json"
    else:       #get following batches of size 30
        url = "https://api.stocktwits.com/api/2/streams/symbol/BRK.B.json?max=" + str(maxid)

    print(url)

    r = requests.get(url, headers=headers)  #scrapping with requests 
    data = json.loads(r.content)

    maxid = data["cursor"]["max"] 
    #time sleep 
    time.sleep(3)

    #iterate over messages and fill the information in dictionary to build db on mongo 
    for m in data["messages"]:
        date = m["created_at"]
        content = m["body"]
        twitid = m["id"]
        
        sentiment = ""      #get declared sentiment if any 
        if "Bearish" in str(m["entities"]["sentiment"]):
            sentiment = -1
        if "Bullish" in str(m["entities"]["sentiment"]):
            sentiment = 1
       
        try: #push into mongo db 
            db["twits"].insert_one({"id":twitid, "date":date, "content":content , "sentiment":sentiment})
            j += 1
        except: 
            pass
    
    if j > 1000000: #get overall 3000 message
        break 

del data, date, headers, i, j, m, maxid, r, sentiment, uri, url, client, content, twitid
 

#%% Call the db from mongo db 

#df= pd.DataFrame(list(db['twits'].find({}))) #import all db and then get declared sentiment 
#df = df[df["sentiment"]!=""] #1207 twits with declared sentiment 

#cleaner way, just call the entries with declared sentiment directly 
df= pd.DataFrame(list(db['twits'].find({"sentiment": {"$ne": ""}})))

