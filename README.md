# DS5110FinalProject
Serverless Computing for Big Data Time-Series Forecasting

## Lambda Functions

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
  
### Final

parent.py
  * Invokes asynchronous child functions to perform the data processing, hyperparameter optimization, and final model training and testing in a fully parallelized manner.
  * event: {"type": function_type}
 
processing.py
  * Cleans and processes our raw data into a form that can be fed into a random forest regression model.
  * {"inbucket": "ds5110s3", "outbucket": f'{file_name.split("_")[0].lower()}-ds5110s3bucket', "file": file_name}
  
parampreds.py
  * Trains a random forest model with a specific set of hyperparameters and stores the predictions generated for the validation set.
  * 
  
predmses.py
  * Aggregates the predictions produced by the previously trained set of distributed groves with a shared set of hyperparameters and computes an overall mean squared error (MSE).
  * 
  
minmse.py
  * Identifies and saves the set of hyperparameters that produced the lowest MSE.
  * 
  
trainfull.py
  * Using the optimally determined hyperparameters, trains and stores a larger random forest regression model and its validation predictions.
  * 
  
testfull.py
  * Combines the predictions of the previously trained set of distributed groves and saves a final MSE value.
  * 
  
timingmetrics.py
  * Computes the relevant, internally tracked runtime metrics for each function and for invocations, computations, and I/O in particular.
  * event: {}
  
clearbuckets.py
  * Utility function for emptying and resetting buckets.
  * event: {}

## S3 Bucket File Structure

### Final

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
