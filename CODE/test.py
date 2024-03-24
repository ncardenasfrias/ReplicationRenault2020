# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 23:38:01 2024

@author: ncardenafria
"""
#Set working directory
import os 
os.chdir("C:/Users/ncardenafria/Documents/GitHub/ReplicationRenault2020/CODE/")

#needed packages 
from datetime import datetime

#Import files with functios used 
from MongoDB_test import connection_mongodb, create_collection, db_to_pd
from Scrapping_funcError_test import scrapping

#%% 1. Scrapping 

#% Create a collection
db = create_collection("stocktwits", "StockTwits", "test", "id")

#% Run scrapping function 
stock_list = ["JPM"]
end_day = datetime(2019,12,31)
t = 0.2
error_t = 15
collect_mongo = db

new_docs, time_ellapsed = scrapping(collect_mongo, stock_list, end_day, t, error_t)







df = db_to_pd ("stocktwits", "StockTwits", "twits", {"sentiment":-1}) #works too
