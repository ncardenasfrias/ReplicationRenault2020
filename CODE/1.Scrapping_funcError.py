# -*- coding: utf-8 -*-
"""
Scrapping StockTwits script

This script defines a function to scrap the messages about given stocks from StockTwits before a certain date. 
The messages are laoded in a MongoDB collection to allow for a correct data management. 
For benchmarking purposes (if any), the function returns the number of new documents uploaded to the database and the time it took to do so.

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


#%% Define scrapping function for StockTwits

def scrapping(collection, tickers_list, last_day, t, error_t):
    """
    Scrapping function. It allows to get the messages from StockTwits and store them in a MongoDB collection. 
    
    To avoid being blocked by the StockTwits platform in case of scrapping large volumes of messages, the function allows to include sleeping times. 
    A first one, t, is executed after each json file scrapped. To avoid a "large" t that makes the function slow, another sleeping time, error_t, is executed in case of having an error in the scrapping before resuming the scrapping where it stopped.
    
    The end of the execution comes if all twits after a certain date last_day have been collected, or if all previous messages are.  
    
    Parameters
    ----------
    collection : pymongo.collection.Collection
        Receipient collection from MongoDB. The conncetion to the database and the creation of the collection needs to be done beforehand.
        This fuction will fill it with an id,date, content, sentiment, user and symbol features.
    tickers_list : list of str
        List of the tickers that need to be scrapped 
    last_day : datetime.datetime
        Date until which messages should be scrapped by the code. It will be common to all the stocks
    t : int or float
        Number of seconds to sleep between each request to avoid being blocked. Can be set to 0 if number of requests is small and this is not to worry
    error_t : int or float
        Number of seconds to wait if there is an error in the scrapping request.
    
    Returns
    -------
    tuple
        The output tuple contains the number of new elements added to the collection and the time it took to scrap and charge them in MongoDB. This allows to get information for benchmarking if needed.
        The database is update is made inside the loop. 
        A message indicating the propper execution of the scrapping loops is printed before returning these values. 
    """

    #get initial lenght db and current time: get benchmark
    initial_docs = collection.count_documents({})
    initial_time = time.time()
    
    headers = {'User-Agent': 'Mozilla/5.0 Chrome/39.0.2171.95 Safari/537.36'}
    
    for tick in tickers_list:
        maxid = None
        
        while True:

            if maxid is None:                                                  #get latest batch of twits
                url = f"https://api.stocktwits.com/api/2/streams/symbol/{tick}.json"
                prev_id = 999999999999999999999999999                          #initiate prev_id with very large number            
            else:                                                              #get following batches of size 30
                url = f"https://api.stocktwits.com/api/2/streams/symbol/{tick}.json?max=" + str(maxid)
                prev_id = maxid 
                
            print(url)                                                         #allows to see in the terminal the advancement of the code
            
            try:                                                               #using this try except allows me to avoid being blocked by the platfom while keeping t small
                r = requests.get(url, headers=headers)                         #scrapping with requests 
                data = json.loads(r.content)
            
                maxid = data["cursor"]["max"]                                  #update maxid, allows to change json to get following batch of twits
                 
                time.sleep(t)                                                  #time sleep, avoid being blocked by StockTwits
            
            except:                                                            #if error in scrapping, wait for a longer time and reenter loop from last batch (latest maxid)
                time.sleep(error_t)                 
                continue
        
            #iterate over messages and fill the information in dictionary to build db
            for m in data["messages"]:
                date = m["created_at"]
                date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")           #format is important to be able to compare to last_day
                
                content = m["body"]
                twitid = m["id"]
                user = m["user"]["username"]
                
                symbols = []                                                   #get all tickers on twit, list comprehension likely more efficient, oops
                for symbol in m["symbols"]:
                    symbols.append(symbol["symbol"])
                
                sentiment = ""                                                 #get declared sentiment in {-1,1}  if any, else empty
                if "Bearish" in str(m["entities"]["sentiment"]):
                    sentiment = -1
                elif "Bullish" in str(m["entities"]["sentiment"]):
                    sentiment = 1
               
                try:                                                           #push into mongo db, will only work if id is unique/not already present in the db
                    collection.insert_one({"id":twitid, "date":date, "content":content , "sentiment":sentiment, "user":user, 'symbol':symbols})
                except: 
                    pass
            
            if date < last_day:                                                #break if beyond desired cutoff date
                break 
            
            if prev_id < maxid:                                                #BRK error: 1st twit after cutoff date, so loop restarts scrapping from the top.
                break                                                          #if maxid not decreasing, that is the problem, break the loop after
                

    #get final db lenght and time + deltas for benchmarking
    final_docs = collection.count_documents({})
    final_time = time.time()
    
    delta_docs = final_docs-initial_docs
    delta_time = final_time-initial_time
    
    print(f"{collection} has been successfully updated with {delta_docs} new documents in {delta_time} seconds:)")
    return delta_docs, delta_time
        


#%% Call the function function
stock_list = ["BRK.B", "JPM", "BAC", "MS", "WFC", "GS"]
end_day = datetime(2009,12,31)
t=0.02
error_t = 15
collect_mongo = db["twits"]

new_docs, time_ellapsed = scrapping(collect_mongo, stock_list, end_day, t, error_t)
# db with these 6 stocks is only about 800k, to get to the million messages we rerun the function with 2 new tickers 

stock_list = ["SCHW", "C"]
new_docs, time_ellapsed = scrapping(collect_mongo, stock_list, end_day, t, error_t)
# we're at 925k, add another one 

stock_list = ["UBS"]
new_docs, time_ellapsed = scrapping(collect_mongo, stock_list, end_day, t, error_t)
#937k, another is needed but store the benchmarking output 

benchmarks = {'UBS':{'time':time_ellapsed, "docs":new_docs}}

stock_list = ["HSBC"]
new_docs, time_ellapsed = scrapping(collect_mongo, stock_list, end_day, t, error_t) #945k
benchmarks["HSBC"] = {'time':time_ellapsed, "docs":new_docs}

stock_list = ["ING", "BCS", "DB"]
new_docs, time_ellapsed = scrapping(collect_mongo, stock_list, end_day, t, error_t) #945k
benchmarks["ING, BCS, DB"] = {'time':time_ellapsed, "docs":new_docs}

stock_list = ["BBVA", "BNPQY"]
new_docs, time_ellapsed = scrapping(collect_mongo, stock_list, end_day, t, error_t) #945k
benchmarks["BBVA, BNPQY"]] = {'time':time_ellapsed, "docs":new_docs}