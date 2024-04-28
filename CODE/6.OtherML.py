# -*- coding: utf-8 -*-
"""
Machine Learning - Other Algorithms

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


#%% Multinomial Naive Bayes 
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import accuracy_score, classification_report
from pymongo import MongoClient
import time  # Import the time module

uri = "mongodb+srv://nataliacardenas:stocktwits@twits.mgv4dfh.mongodb.net/?retryWrites=true&w=majority&appName=Twits"

# Initialize the timer for data fetching
start_time_data_fetch = time.time()

try:
    client = MongoClient(uri)
    db = client["StockTwits"]
    collection = db["twits"]
    # Fetching data
    cursor = collection.find({}, {'content': 1, 'sentiment': 1})
    df = pd.DataFrame(list(cursor))
finally:
    client.close()

# Stop the timer for data fetching and print the elapsed time
elapsed_time_data_fetch = time.time() - start_time_data_fetch
print(f"Data fetching time: {elapsed_time_data_fetch:.2f} seconds")

# Preprocessing
start_time_preprocessing = time.time()  # Start timer for preprocessing

vectorizer = TfidfVectorizer(max_features=1000)
X = vectorizer.fit_transform(df['content'])
y = pd.to_numeric(df['sentiment'], errors='coerce').fillna(0).astype(int)

# Stop the timer for preprocessing and print the elapsed time
elapsed_time_preprocessing = time.time() - start_time_preprocessing
print(f"Preprocessing time: {elapsed_time_preprocessing:.2f} seconds")

# Data splitting
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# Model definition and training
start_time_training = time.time()  # Start timer for training

model = MultinomialNB()
model.fit(X_train, y_train)

# Stop the timer for training and print the elapsed time
elapsed_time_training = time.time() - start_time_training
print(f"Training time: {elapsed_time_training:.2f} seconds")

# Model evaluation
start_time_evaluation = time.time()  # Start timer for evaluation

predictions = model.predict(X_test)
accuracy = accuracy_score(y_test, predictions)
report = classification_report(y_test, predictions)

# Stop the timer for evaluation and print the elapsed time
elapsed_time_evaluation = time.time() - start_time_evaluation
print(f"Evaluation time: {elapsed_time_evaluation:.2f} seconds")

# Display results
print(f"Multinomial Naive Bayes - Accuracy: {accuracy}\nReport:\n{report}")




#%% Maximum entropy
from sklearn.linear_model import LogisticRegression
import time

# Start timing the Logistic Regression setup and training
start_time_lr = time.time()

# Model definition
max_ent_model = LogisticRegression(max_iter=1000)  # Increase max_iter if convergence issues occur

# Training the model
max_ent_model.fit(X_train, y_train)

# Timing the training process
elapsed_time_lr_train = time.time() - start_time_lr
print(f"Training Maximum Entropy model took {elapsed_time_lr_train:.2f} seconds")

# Model evaluation
start_time_lr_eval = time.time()
predictions_lr = max_ent_model.predict(X_test)
accuracy_lr = accuracy_score(y_test, predictions_lr)
report_lr = classification_report(y_test, predictions_lr)

# Timing the evaluation process
elapsed_time_lr_eval = time.time() - start_time_lr_eval
print(f"Evaluating Maximum Entropy model took {elapsed_time_lr_eval:.2f} seconds")

# Display results
print(f"Maximum Entropy (Logistic Regression) - Accuracy: {accuracy_lr}\nReport:\n{report_lr}")



#%% SVM 
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC
from sklearn.metrics import accuracy_score, classification_report
from pymongo import MongoClient
import time

# Connect to MongoDB
uri = "mongodb+srv://nataliacardenas:stocktwits@twits.mgv4dfh.mongodb.net/?retryWrites=true&w=majority&appName=Twits"

# Start timing the data fetching
start_time_data_fetch = time.time()

# Connect to MongoDB
client = MongoClient(uri)
try:
    db = client["StockTwits"]  # Adjust the database name
    collection = db["twits"]    # Adjust the collection name
    # Fetching data
    cursor = collection.find({}, {'content': 1, 'sentiment': 1}).limit(100000)  # Limiting to prevent excessive loading time
    df = pd.DataFrame(list(cursor))
finally:
    client.close()

# Timing the data fetching
elapsed_time_data_fetch = time.time() - start_time_data_fetch
print(f"Data fetching time: {elapsed_time_data_fetch:.2f} seconds")

# Check if the DataFrame is populated
if df.empty:
    raise ValueError("No data fetched from MongoDB. Check database and collection names, and ensure they contain data.")

# Text vectorization
vectorizer = TfidfVectorizer(max_features=1000)
X = vectorizer.fit_transform(df['content'])
y = pd.to_numeric(df['sentiment'], errors='coerce').fillna(0).astype(int)

# Splitting the data into training and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# Define and train the SVM model
start_time_svm = time.time()
svm_model = SVC(kernel='linear', random_state=42)  # Using a linear kernel
svm_model.fit(X_train, y_train)
elapsed_time_svm_train = time.time() - start_time_svm
print(f"Training SVM model took {elapsed_time_svm_train:.2f} seconds")

# Evaluating the SVM model
start_time_svm_eval = time.time()
predictions_svm = svm_model.predict(X_test)
accuracy_svm = accuracy_score(y_test, predictions_svm)
report_svm = classification_report(y_test, predictions_svm)
elapsed_time_svm_eval = time.time() - start_time_svm_eval
print(f"Evaluating SVM model took {elapsed_time_svm_eval:.2f} seconds")

# Displaying results
print(f"SVM (Support Vector Machine) - Accuracy: {accuracy_svm}\nReport:\n{report_svm}")




#%% Random Forest



#%% Multilayer Perceptron 



#%% Neural Networks
