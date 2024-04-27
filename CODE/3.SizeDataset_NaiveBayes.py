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
#sklearn 
from sklearn.model_selection import train_test_split, cross_validate
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import accuracy_score, matthews_corrcoef
from sklearn.feature_extraction.text import CountVectorizer
#balanced sample 
from imblearn.under_sampling import RandomUnderSampler
#Viz 
import seaborn as sns
sns.set(style="whitegrid")
import matplotlib.pyplot as plt

os.chdir("C:/Users/ncardenafria/Documents/GitHub/ReplicationRenault2020/")
export_on=False

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
    

#%% Call data base : only entries with declared sentiment
db = client["StockTwits"]
df = pd.DataFrame(list(db['twits_v2'].find({"sentiment": {"$ne": ""}, "n_three": 1, "is_bot": 0}, {"c_content": 1, "sentiment": 1})))

df.head(1)
df.columns
df["sentiment"].isna().sum()         # 0, only entries with declared sentiment 

full_sample = len(df)                       # 923638 entries
full_positive = len(df[df["sentiment"]==1]) # 580658, 62.87%
full_negative = len(df[df["sentiment"]==-1]) # 342980, 37.13%


#%% Count Vectorizer : need to transform text into numerical input
#paper removes stop words with default in CVect + ignore punctuation (default does so)
vect = CountVectorizer(stop_words="english")
X = vect.fit_transform(df["c_content"]) #we get a huge sparse matrix 


#%% Naives Bayes: default parameters
bayes = MultinomialNB(alpha=1.0, fit_prior=True, class_prior=None)

#%% Unbalanced

results = []
for i in [500,1000,2500,5000,10000,25000,50000,100000,250000,500000,full_sample]: 
     start_t = time.time()                                                     
     
     if i != full_sample:
         X_train, X_test, y_train, y_test = train_test_split(X, df['sentiment'],
                                                             random_state=3456,
                                                             train_size=i, test_size=0.2)
     else:
         X_train, X_test, y_train, y_test = train_test_split(X, df['sentiment'],
                                                             random_state=3456,
                                                             test_size=0.2)

    #get 10-fold CV scores
     scoring = {"acc":"accuracy", 'mcc': 'matthews_corrcoef'}
     scores = cross_validate(bayes, X_train, y_train, scoring=scoring, cv=10, return_train_score=True)
     acc_mean = round(scores['test_acc'].mean(), 3)
     mcc_mean = round(scores['test_mcc'].mean(),3)
     
     # get time for benchmarking
     end_t = time.time()
     delta_t = end_t-start_t 
     s = round(delta_t, 1)

     results.append({'Sample Size': i, 'Accuracy': acc_mean, 'MCC': mcc_mean, "Time":f"{s} sec"})
    

#%% Balanced 

sampler = RandomUnderSampler(sampling_strategy='majority')
X_bal, y_bal = sampler.fit_resample(X,df["sentiment"])

len(y_bal) == 2*min(full_positive, full_negative) #685960

results_bal = []
for i in [500,1000,2500,5000,10000,25000,50000,100000,250000,500000,len(y_bal)]: 
     start_t = time.time()
     if i != len(y_bal):
         X_train, X_test, y_train, y_test = train_test_split(X_bal, y_bal,
                                                             random_state=3456,
                                                             train_size=i, test_size=0.2)
     else:
         X_train, X_test, y_train, y_test = train_test_split(X_bal, y_bal,
                                                             random_state=3456,
                                                             test_size=0.2)

     scoring = {"acc":"accuracy", 'mcc': 'matthews_corrcoef'}
     scores = cross_validate(bayes, X_train, y_train, scoring=scoring, cv=10, return_train_score=True)
     acc_mean = round(scores['test_acc'].mean(), 3)
     mcc_mean = round(scores['test_mcc'].mean(),3)
     
     end_t = time.time()
     delta_t = end_t-start_t
     s = round(delta_t, 1)

     results_bal.append({'Sample Size': i, 'Accuracy': acc_mean, 'MCC': mcc_mean, "Time":f"{s} sec"})
    
    
#%% Results
results = pd.DataFrame(results)
results_bal = pd.DataFrame(results_bal)
results.to_csv("REPORT/TABLES/unbal_size.csv")
results_bal.to_csv("REPORT/TABLES/bal_size.csv")

if export_on:
    with open('REPORT/TABLES/unabal_size.tex', 'w') as tf:
         tf.write(results.to_latex(index=False, 
                     caption="Size of the dataset and classification accuracy - Unbalanced Dataset", 
                     label="tab:unbal_size")) 
    
    with open('REPORT/TABLES/bal_size.tex', 'w') as tf:
        tf.write(results_bal.to_latex(index=False, 
                         caption="Size of the dataset and classification accuracy - Balanced Dataset",
                         label="tab:bal_size"))
                     
# Plot results
melted_results = results.melt(id_vars='Sample Size', var_name='Metric', value_name='Score')
melted_results = melted_results[melted_results["Metric"].isin(["Accuracy", "MCC"])]

melted_results_bal = results.melt(id_vars='Sample Size', var_name='Metric', value_name='Score')
melted_results_bal = melted_results[melted_results["Metric"].isin(["Accuracy", "MCC"])]

if export_on:
##unbalanced
    plt.figure(figsize=(10, 6))
    sns.barplot(data=melted_results, x="Sample Size", y="Score", hue="Metric", palette="crest")
    
    plt.xlabel("Sample size")
    plt.ylabel("Score")
    plt.title("Size of the unbalanced dataset and classification accuracy")
    custom_labels = [500,1000,2500,5000,10000,25000,50000,100000,250000,500000,full_sample]
    plt.gca().set_xticklabels(custom_labels)
    plt.xticks(rotation=0)

    plt.legend()
   # plt.show()
    plt.savefig('REPORT/IMAGES/unbal_size.png', bbox_inches='tight')

##balanced
    plt.figure(figsize=(10, 6))
    sns.barplot(data=melted_results_bal, x="Sample Size", y="Score", hue="Metric", palette="crest")
    
    plt.xlabel("Sample size")
    plt.ylabel("Score")
    plt.title("Size of the balanced dataset and classification accuracy")
    custom_labels = [500,1000,2500,5000,10000,25000,50000,100000,250000,500000,len(y_bal)]
    plt.gca().set_xticklabels(custom_labels)
    plt.xticks(rotation=0)

    plt.legend()
   # plt.show()
    plt.savefig('REPORT/IMAGES/bal_size.png', bbox_inches='tight')