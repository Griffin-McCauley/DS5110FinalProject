# DS5110FinalProject
Serverless Computing for Big Data Time-Series Forecasting

## Lambda Functions
processing.py
  * Cleans and processes our raw data into a form that can be fed into a random forest regression model.
  * event: {"bucket": bucket_name, "file": file_name}

modeling.py
  * Trains a random forest regression model on processed training and validation data and stores the resulting model.
  * event: {"bucket": bucket_name, "stock": stock_name}

predict.py
  * Generates predictions for a trained model on testing data and computes and stores the mean squared error (MSE).
  * event: {"bucket": bucket_name, "stock": stock_name}

metrics.py
  * Analyzes the runtimes for each process in our pipeline and calculates the average MSE for our models across all stocks.
  * event: {}
