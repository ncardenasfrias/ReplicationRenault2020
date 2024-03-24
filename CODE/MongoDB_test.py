# -*- coding: utf-8 -*-
"""
MongoDB
Get connected to the database
Create a collection
Connect to the collection

@author: ncardenafria
"""

import pandas as pd

#Mongo DB 
import pymongo 
from pymongo.mongo_client import MongoClient


#%% Connection - Directly from Mongo DB

def connection_mongodb(password):
    '''
    Connection to *our* MongoDB Project, the code is taken from MongoDB

    Parameters
    ----------
    password : str
        Password to the DB. Use "stocktwits"

    Returns
    -------
    client : pymongo.mongo_client.MongoClient
        Connection to teh project.
    '''
    #mongodb password stocktwits
    uri = f"mongodb+srv://nataliacardenas:{password}@twits.mgv4dfh.mongodb.net/?retryWrites=true&w=majority&appName=Twits"
    # Create a new client and connect to the server
    client = MongoClient(uri)
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
        
    return client

#connection_mongodb("stocktwits") #works


def create_collection(password, database, collection, unique_index):
    """
    Creates a new collection for given project and database. 
    Requires a column to be an unique identifier.

    Parameters
    ----------
    password : str
        Password to access the MongoDB project. Use "stocktwits" for this project.
    database : str
        Name of the database. Use "StockTwits".
    collection : str
        Name of the collection. Use "twits".
    unique_index : str
        Name column to be used as unique identifier. Use 'id'

    Returns
    -------
    None

    """
    client = connection_mongodb(password)
    #create db 
    db = client[database]
    db[collection].create_index([(unique_index,  pymongo.ASCENDING)], unique=True)
    
    f"Propper creation of collection {collection} in database {database}"
    return db[collection]

#create_collection("stocktwits", "StockTwits", "test", "id") #works perfectly :)


def db_to_pd(password, database, collection, criteria):
    """
    Access a MongoDB collection and call it or part of it into a pandas dataframe

    Parameters
    ----------
    password : str
        Password to access the MongoDB project. Use "stocktwits" for this project.
    database : str
        Name of the database. Use "StockTwits".
    collection : str
        Name of the collection. Use "twits".
    criteria : dict
        Filter to be used to import in the dataframe. Use {} to import whole collection. 

    Returns
    -------
    df : pandas.core.frame.DataFrame
        Pandas dataframe with filtered collection.

    """
    client = connection_mongodb(password)
    #get db 
    db = client[database]
    df = pd.DataFrame(list(db[collection].find(criteria)))
    return df 
    
#df = db_to_pd ("stocktwits", "StockTwits", "twits", {"sentiment":-1}) #works too
