# UVA MSDS Big Data Systems (DS5110) Spring 2023 Final Project: Serverless Computing for Big Data Time-Series Forecasting

## Data

The dataset for this project can be found on Kaggle at this [link](https://www.kaggle.com/datasets/debashis74017/stock-market-data-nifty-50-stocks-1-min-data).

It consists of 101 CSV files tracking various price-related metrics and technical indicators on a minute-by-minute level for the Nifty 100 stocks and indices currently traded on the Indian Stock Market.

The total size of this dataset is 66GB.

## Lambda Functions

#### Runtime Settings
Runtime: Python 3.9

Architecture: x86_64

### Final

parent.py
  * Invokes asynchronous child functions to perform the data processing, hyperparameter optimization, and final model training and testing in a fully parallelized manner.
  * event: {"type": function_type}
 
processing.py
  * Cleans and processes the raw data into a form that can be fed into a random forest regression model.
  * event: {"inbucket": "ds5110s3", "outbucket": f'{file_name.split("_")[0].lower()}-ds5110s3bucket', "file": file_name}
  
parampreds.py
  * Trains a random forest model with a specific set of hyperparameters and stores the predictions generated for the validation set.
  * event: {"bucket": f'{stock_name.lower()}-ds5110s3bucket', "stock": stock_name, "feat": max_features, "samp": min_features_split, "n_estimators": n_estimators, "pred_num": prediction_number}
  
predmses.py
  * Aggregates the predictions produced by the previously trained set of distributed groves with a shared set of hyperparameters and computes an overall mean squared error (MSE).
  * event: {"bucket": f'{stock_name.lower()}-ds5110s3bucket', "stock": stock_name}
  
minmse.py
  * Identifies and saves the set of hyperparameters that produced the lowest MSE.
  * event: {"bucket": f'{stock_name.lower()}-ds5110s3bucket', "stock": stock_name}
  
trainfull.py
  * Using the optimally determined hyperparameters, trains and stores a larger random forest regression model and its validation predictions.
  * event: {"bucket": f'{stock_name.lower()}-ds5110s3bucket', "stock": stock_name, "n_estimators": n_estimators, "pred_num": prediction_number}
  
testfull.py
  * Combines the predictions of the previously trained set of optimized distributed groves and computes and saves the final MSE value.
  * event: {"bucket": f'{stock_name.lower()}-ds5110s3bucket', "stock": stock_name}
  
timingmetrics.py
  * Computes the relevant, internally-tracked runtime metrics for each function's invocations, computations, and I/Os.
  * event: {}
  
clearbuckets.py
  * Utility function for emptying and resetting buckets.
  * event: {}

### Checkpoint 2

parent.py
 * Invokes one child function for each stock in order to fully parallelize our data analysis process.
 * event: {"bucket": bucket_name, "type": child_type}

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

## S3 Bucket File Structure

### Final

 * ds5110s3/
   * raw/
     * {stock}_minute_data_with_indicators.csv for each stock
   * runtimes/
     * invoke_{type}.txt for each Lambda function
   * timingmetrics/
     * actualtimingmetrics.csv
     * fullparalleltimingmetrics.csv
     * instancecomputetimingmetrics.csv
     * instanceiotimingmetrics.csv
     * sequentialtimingmetrics.csv

 * {stock}-ds5110s3bucket/ for each stock
   * models/
     * optimized/
       * best_params.csv
       * models/
         * models/
           * model_{pred_num}.sav for each pred_num
         * mse.csv
         * preds/
           * preds_{pred_num}.csv
         * runtimes/
           * compute_{pred_num}.txt
           * compute.txt
           * reads3_{pred_num}.txt
           * reads3.txt
           * startwrites3_{pred_num}.txt
           * startwrites3.txt
       * runtimes/
         * compute.txt
         * reads3.txt
         * startwrites3.txt
     * params/
       * feat{feat}_samp{samp}/ for each feat, samp
         * mse.csv
         * preds/
           * preds_{pred_num}.csv
         * runtimes/
           * compute_{pred_num}.txt
           * compute.txt
           * reads3_{pred_num}.txt
           * reads3.txt
           * startwrites3_{pred_num}.txt
           * startwrites3.txt
   * processed/
     * runtimes/
       * compute.txt
       * reads3.txt
       * startwrites3.txt
     * testing/
       * {stock}.csv
     * training/
       * {stock}.csv
     * validation/
       * {stock}.csv

### Checkpoint 2

 * models/
   * models/
     * {stock}.sav for each stock
   * mses/
     * {stock}.txt for each stock
   * runtimes/
     * {stock}.txt for each stock
 * processed
   * runtimes/
     * {stock}.txt for each stock
   * testing/
     * {stock}.csv for each stock
   * training/
     * {stock}.csv for each stock
   * validation/
     * {stock}.csv for each stock
 * raw/
   * {stock}_minute_data_with_indicators.csv for each stock
