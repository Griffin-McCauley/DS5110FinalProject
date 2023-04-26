import json
import boto3
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import joblib
import tempfile
from datetime import datetime
import pytz

s3_resource = boto3.resource('s3')

# function for actually training the RandomForest
def lambda_handler(event, context):
#def train_rf(stock, target_col, feat, samp, other_params, pred_num):
    # define parameters
    target_col = 'TYPPRICE_TARGET'
    feat = event["feat"]
    samp = event["samp"]
    pred_num = event["pred_num"]
    other_params = {'n_estimators': event["n_estimators"], 'n_jobs': -1}
    
    # Read in train and validate datasets
    bucket_name = event["bucket"]
    stock = event["stock"]
    trainfile_name = f'processed/training/{stock}.csv'
    validatefile_name = f'processed/validation/{stock}.csv'
    
    startread = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    trainresp = s3_resource.Object(bucket_name, trainfile_name).get()
    train = pd.read_csv(trainresp['Body'], sep=',', index_col = 0)
    valresp = s3_resource.Object(bucket_name, validatefile_name).get()
    validate = pd.read_csv(valresp['Body'], sep=',', index_col = 0)
    
    stopread = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    
    startcomp = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    
    # Drop NA columns
    train.dropna(inplace = True)
    validate.dropna(inplace = True)
    
    # Get target column for training data
    y_train = train[target_col]
    
    # Drop target columns
    train.drop(columns = [target_col], inplace = True)
    validate.drop(columns = [target_col], inplace = True)
    
    # Train RF
    rf = RandomForestRegressor(max_features = feat, 
                               min_samples_split = samp, 
                               **other_params)
    rf.fit(train, y_train)
    
    # Predict for test data
    rf_preds = rf.predict(validate)
    
    # Clean output
    preds_df = pd.DataFrame.from_records(rf_preds.reshape(-1,1), columns=[target_col])
    preds_df.index = validate.index
    
    stopcomp = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    
    startwrite = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    
    # Save preds
    outfile_name = f'models/params/feat{feat}_samp{samp}/preds/preds_{str(pred_num)}.csv'
    s3_resource.Object(bucket_name, outfile_name).put(Body=preds_df.to_csv())
    
    string = f'{(stopread-startread)*1000}'
    encoded_string = string.encode("utf-8")
    
    timefile_name = f'models/params/feat{feat}_samp{samp}/runtimes/reads3_{str(pred_num)}.txt'
    s3_resource.Object(bucket_name, timefile_name).put(Body=encoded_string)
    
    string = f'{(stopcomp-startcomp)*1000}'
    encoded_string = string.encode("utf-8")
    
    timefile_name = f'models/params/feat{feat}_samp{samp}/runtimes/compute_{str(pred_num)}.txt'
    s3_resource.Object(bucket_name, timefile_name).put(Body=encoded_string)
    
    string = f'{(startwrite)*1000}'
    encoded_string = string.encode("utf-8")
    
    timefile_name = f'models/params/feat{feat}_samp{samp}/runtimes/startwrites3_{str(pred_num)}.txt'
    s3_resource.Object(bucket_name, timefile_name).put(Body=encoded_string)
        
    return
