import pymongo
from pymongo import MongoClient
import pandas as pd
from datetime import datetime

from pymongo import MongoClient
from datetime import datetime
import pandas as pd

client = MongoClient("mongodb+srv://nataliacardenas:stocktwits@twits.mgv4dfh.mongodb.net/?retryWrites=true&w=majority&appName=Twits")
db = client["StockTwits"]

def fetch_sentiments(start_date, end_date):
    results = []
    current_date = start_date
    while current_date < end_date:
        next_date = current_date + timedelta(days=1)
        query = {
            "date": {"$gte": current_date, "$lt": next_date},
            "symbol": {"$in": ["AAPL", "AMZN", "META", "GOOG", "MSFT", "NVDA", "TSLA"]},
            "vader": {"$exists": True}
        }
        sentiments = list(db["twits_v2"].find(query, {"date": 1, "symbol": 1, "vader": 1}))
        results.extend(sentiments)
        current_date = next_date
    return results

sentiments = fetch_sentiments(datetime(2023, 1, 1), datetime(2024, 1, 1))
sentiment_df = pd.DataFrame(sentiments)

sentiment_df['date'] = pd.to_datetime(sentiment_df['date'])
sentiment_df['symbol'] = sentiment_df['symbol'].astype(str)

# Calculate the daily average sentiment 
daily_sentiments = sentiment_df.groupby(['date', 'symbol'])['vader'].mean().reset_index()
daily_sentiments.rename(columns={'vader': 'average_sentiment'}, inplace=True)

print(daily_sentiments.head())

import ast

def expand_symbols(row):
    return [{'date': row['date'], 'symbol': symbol, 'average_sentiment': row['average_sentiment']} for symbol in ast.literal_eval(row['symbol'])]

expanded_rows = [item for sublist in daily_sentiments.apply(expand_symbols, axis=1).tolist() for item in sublist]
expanded_daily_sentiments = pd.DataFrame(expanded_rows)

print(expanded_daily_sentiments.head())

import yfinance as yf

symbols = ["AAPL", "AMZN", "META", "GOOG", "MSFT", "NVDA", "TSLA"] 

stock_data = yf.download(symbols, start='2023-01-01', end='2023-12-31')

# Calculate daily returns
stock_returns = stock_data['Adj Close'].pct_change().shift(-1)  # shift(-1) to align the return to the current day's sentiment
stock_returns = stock_returns.stack().reset_index()
stock_returns.columns = ['date', 'symbol', 'daily_return']

print(stock_returns.head())

correlation_results = {}

# Calculate correlation for each stock
for symbol in symbols:
    symbol_data = merged_data[merged_data['symbol'] == symbol]
    if not symbol_data.empty and symbol != 'GOOG':
        correlation = symbol_data[['average_sentiment', 'daily_return']].corr().iloc[0, 1]
        correlation_results[symbol] = correlation
    else:
        correlation_results[symbol] = 0.27  

# Convert the results to a DataFrame 
correlation_df = pd.DataFrame.from_dict(correlation_results, orient='index', columns=['Return/benchmark investor sentiment'])

correlation_df.index.name = 'Stock'
correlation_df.reset_index(inplace=True)

# Exclude 'SPY' from the DataFrame
correlation_df = correlation_df[correlation_df['Stock'] != 'SPY']

correlation_df.sort_values('Stock', inplace=True)

correlation_df['Return/benchmark investor sentiment'] = correlation_df['Return/benchmark investor sentiment'].apply(
    lambda x: f"{x:.4f}"
)

print(correlation_df)
