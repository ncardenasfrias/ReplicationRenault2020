### Forecasting investor sentimentForecasting investor sentiment

import statsmodels.api as sm
from statsmodels.regression.linear_model import OLS

# Add lagged average sentiment and return columns
merged_data['lagged_average_sentiment'] = merged_data.groupby('symbol')['average_sentiment'].shift(1)
merged_data['lagged_daily_return'] = merged_data.groupby('symbol')['daily_return'].shift(1)

# Drop the rows with NaN values due to lagging
merged_data.dropna(inplace=True)

# Store the regression results
regression_results = {}

# Perform the regression for each stock
for symbol in merged_data['symbol'].unique():
    stock_data = merged_data[merged_data['symbol'] == symbol]

    # Define the independent variables (add a constant to the model for the intercept)
    X = stock_data[['lagged_average_sentiment', 'lagged_daily_return']]
    X = sm.add_constant(X)

    # Define the dependent variable
    Y = stock_data['average_sentiment']

    # Perform the regression
    model = OLS(Y, X).fit(cov_type='HC3')  # HC3 for White’s heteroskedasticity robust standard errors

    # Store the results
    regression_results[symbol] = {
        'beta_1': model.params['lagged_average_sentiment'],
        'beta_2': model.params['lagged_daily_return'],
        'R_squared': model.rsquared
    }

# Convert the regression results to a DataFrame for display
regression_df = pd.DataFrame.from_dict(regression_results, orient='index')

# Format the DataFrame to match the desired output
regression_df.reset_index(inplace=True)
regression_df.rename(columns={'index': 'Stock'}, inplace=True)
print(regression_df)



#Table 7Forecasting stock

import statsmodels.api as sm
from statsmodels.regression.linear_model import OLS

# Ensure all necessary packages are installed
!pip install pymongo
!pip install emoji
!pip install vaderSentiment
!pip install yfinance


# Establish connection to MongoDB
client = MongoClient("mongodb+srv://nataliacardenas:stocktwits@twits.mgv4dfh.mongodb.net/?retryWrites=true&w=majority&appName=Twits")
db = client["StockTwits"]

# Calculate lagged values for the regression model
merged_data['lagged_average_sentiment'] = merged_data.groupby('symbol')['average_sentiment'].shift(1)
merged_data['lagged_daily_return'] = merged_data.groupby('symbol')['daily_return'].shift(1)

# Drop rows with NaN values which might be a result of shifting
merged_data.dropna(inplace=True)

# Store regression results
return_forecast_results = {}

# Perform regression to forecast stock returns
for symbol in merged_data['symbol'].unique():
    stock_data = merged_data[merged_data['symbol'] == symbol]
    
    # Define the independent variables for the return forecast model
    X_returns = stock_data[['lagged_average_sentiment', 'lagged_daily_return']]
    X_returns = sm.add_constant(X_returns)

    # Define the dependent variable as the current day's return
    Y_returns = stock_data['daily_return']

    # Perform the regression
    model_returns = OLS(Y_returns, X_returns).fit(cov_type='HC3')  # Using HC3 to be consistent

    # Store the results
    return_forecast_results[symbol] = {
        'return_beta_1': model_returns.params['lagged_average_sentiment'],
        'return_beta_2': model_returns.params['lagged_daily_return'],
        'return_R_squared': model_returns.rsquared
    }

# Convert the regression results to a DataFrame for display
return_forecast_df = pd.DataFrame.from_dict(return_forecast_results, orient='index')
return_forecast_df.reset_index(inplace=True)
return_forecast_df.rename(columns={'index': 'Stock'}, inplace=True)
print("Forecasting Stock Returns Based on Previous Day's Sentiment and Returns:")
print(return_forecast_df)
