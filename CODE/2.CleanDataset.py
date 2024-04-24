# -*- coding: utf-8 -*-
"""
Pre-processing of the data
We process the data to: 
    (i) replace all stock tickers, usernames, and urls by common words (cashtag, usertag and linktag respectivelly)
    (ii) have messages in lower cases only 
This cleaned version of the messages is stored in a new field in the data base "c_content".

We also made sure to inspect the frequency of posts by user in our sample. In particualr we flagged as bots users whose messages represent over 0.5% of the overall sample. 
They represent 16 users and shy of 250k messages. They are flagged by a field in the data base "is_bot" = 1

Another field n_three is created to be able to retrieve messages with more of 3 words. It takes value 1 if it is the case.

@author: ncardenafria
"""

#Basic packages 
import pandas as pd
import os
import re
from collections import Counter
#Viz
import seaborn as sns
import matplotlib.pyplot as plt
#Mongo DB 
import pymongo 
from pymongo.mongo_client import MongoClient

os.chdir("C:/Users/ncardenafria/Documents/GitHub/ReplicationRenault2020/")
export_on = False

#%% Connect to MongoDB 
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
 
#%% Call the database
db = client["StockTwits"]

# Call only the first 100 documents -> test the function 
#dfx = pd.DataFrame(list(db["twits_v2"].find({}).limit(100)))

#call only what is needed, else it is going to be super heavy to load 
#df = pd.DataFrame(db["twits"].find({"sentiment": {"$ne": ""}}, {"content": 1, 'user':1, "_id": 1}))


#%% cashtag, linktag, usertag 

def clean_content(text):
    """
    Get cleaned content using regular expressions, i.e. 
    (i) get all text in lowercases, and 
    (ii) replace user names, stock tickers and links by common words usertag, cashtag and link tag

    Parameters
    ----------
    text : str
        Twit content to be cleaned

    Returns
    -------
    str

    """
    cleaned_text = re.sub(r'http\S+', 'linktag', text)                  #replace urls with http/https
    cleaned_text = re.sub(r'https\S+', 'linktag', cleaned_text)
    cleaned_text = re.sub(r'\$\w+(\.\w+)?', 'cashtag', cleaned_text)    #replace tickers, made sure tickers like $BRK.B get propperly trated ie don't get cashtag.b
    cleaned_text = re.sub(r'@\w+', 'usertag', cleaned_text)             #repalce usernames ie those starting with @
    cleaned_text = cleaned_text.lower()                                 #lowercase everything and we're good to go
    
    return cleaned_text

# # test on small subsection of the db, make sure function works as intended
# for i in dfx.index:
#     dfx.loc[i,"c_content"] = clean_content(dfx.loc[i,"content"])
# dfxx = dfx[["content", "c_content"]]


#%% Find bots and remove them from database

tot = db["twits_v2"].count_documents({}) #2 236 664 docs

# Get all the users from the collection
user_data = list(db["twits_v2"].find({}, {"user": 1}))                            #list of dictionnaries
user_ids = [data["user"] for data in user_data]     #get only the ids
len(list(set(user_ids))) #91555 unique users 

# Get 1% of users with highest frequencies
top_users = Counter(user_ids).most_common(915) # 1% top users
top_users = pd.DataFrame(top_users[:100])

# Plot 100 top users
if export_on:
    sns.set(style="whitegrid", palette="crest")
    plt.figure(figsize=(10, 6))
    sns.barplot(x=0, y=1, data=top_users)
    plt.xlabel('Users')
    plt.ylabel('# twits in the sample')
    plt.title('Top 100 users with the most twits in the database')
    plt.xticks(rotation=90)  # Rotate x-axis labels for better readability
    plt.gca().tick_params(axis='x', labelsize=6)  # Set font size for x-axis labels
    plt.tight_layout()
    plt.savefig('REPORT/IMAGES/bots_top100users.png')
    plt.show()


# select users with more than 5k messages ~ 0.25% of the sample 
top_users = top_users[top_users[1] > 5000]
len(top_users)                      #end up with 12 users to be removed
top_users[1].sum()                  #134234 messages to be deleted
top_users = list(top_users[0])                 

#%% Update database 
# get dummy bot, cleaned version of the data + dummy if the lenght text >= 3 as in the paper

# i=0
# for document in db["test"].find({}):
#     user = document["user"]
#     text = document["content"]
#     n = len(list(text.split(" ")))
    
#     is_bot = 1 if user in top_users else 0
#     n_three = 1 if n>=3 else 0
#     cleaned = clean_content(text)
    
#     db["test"].update_one({"_id": document["_id"]}, {"$set": {"is_bot": is_bot}})
#     db["test"].update_one({"_id": document["_id"]}, {"$set": {"c_content": cleaned}})
#     db["test"].update_one({"_id": document["_id"]}, {"$set": {"n_three": n_three}})
    
#     #to follow 
#     print(i)
#     i+=1


# Define a function for processing a subset of documents
def process_subset(subset):
    for document in subset:
        user = document["user"]
        text = document["content"]
        n = len(list(text.split(" ")))
        
        is_bot = 1 if user in top_users else 0
        cleaned = clean_content(text)
        n_three = 1 if n>=3 else 0
        
        db["twits_v2"].update_one({"_id": document["_id"]}, {"$set": {"is_bot": is_bot}})
        db["twits_v2"].update_one({"_id": document["_id"]}, {"$set": {"c_content": cleaned}})
        db["twits_v2"].update_one({"_id": document["_id"]}, {"$set": {"n_three": n_three}})
        print(".")

# Define the number of processes (computers) for parallel processing
num_processes = 4  # Adjust according to available resources

# Divide the dataset into smaller subsets
subset_size = tot // num_processes
# subsets = []
# for i in range(num_processes):
#     subset = list(db["twits_v2"].find().skip(i * subset_size).limit(subset_size))
#     subsets.append(subset)

nb_process = int(input("What process are we in? "))
subset = list(db["twits_v2"].find({"sentiment": {"$ne": ""}}, {"content": 1, 'user':1, "_id": 1}).skip(nb_process * subset_size).limit(subset_size))

process_subset(subset)
