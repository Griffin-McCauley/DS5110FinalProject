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
  * 
  * 
 
processing.py
  * 
  * 
  
parampreds.py
  * 
  * 
  
predmses.py
  * 
  * 
  
minmse.py
  * 
  * 
  
trainfull.py
  * 
  * 
  
testfull.py
  * 
  * 
  
timingmetrics.py
  * 
  * 
  
clearbuckets.py
  * 
  * 

## S3 Bucket File Structure

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
