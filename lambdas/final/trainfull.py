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
    pred_num = event["pred_num"]
    other_params = {'n_estimators': event["n_estimators"], 'n_jobs': -1}
    
    startread = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    # Read in train and validate datasets
    bucket_name = event["bucket"]
    stock = event["stock"]
    trainfile_name = f'processed/training/{stock}.csv'
    trainresp = s3_resource.Object(bucket_name, trainfile_name).get()
    train = pd.read_csv(trainresp['Body'], sep=',', index_col = 0)
    validatefile_name = f'processed/validation/{stock}.csv'
    valresp = s3_resource.Object(bucket_name, validatefile_name).get()
    validate = pd.read_csv(valresp['Body'], sep=',', index_col = 0)
    testfile_name = f'processed/testing/{stock}.csv'
    testresp = s3_resource.Object(bucket_name, testfile_name).get()
    test = pd.read_csv(testresp['Body'], sep=',', index_col = 0)
    
    # get best parameters
    paramsfile_name = f'models/optimized/best_params.csv'
    paramsresp = s3_resource.Object(bucket_name, paramsfile_name).get()
    params = pd.read_csv(paramsresp['Body'], sep=',')
    stopread = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    
    startcomp = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    feat = params.feat.item()
    samp = params.samp.item()
    
    # Drop NA columns
    train.dropna(inplace = True)
    validate.dropna(inplace = True)
    
    full = pd.concat([train, validate])
    
    # Get target column for training data
    y_full = full[target_col]
    
    # Drop target columns
    full.drop(columns = [target_col], inplace = True)
    test.drop(columns = [target_col], inplace = True)
    
    # Train RF
    rf = RandomForestRegressor(max_features = feat, 
                               min_samples_split = samp, 
                               **other_params)
    rf.fit(full, y_full)
    
    # Predict for test data
    rf_preds = rf.predict(test)
    
    # Clean output
    preds_df = pd.DataFrame.from_records(rf_preds.reshape(-1,1), columns=[target_col])
    preds_df.index = test.index
    
    stopcomp = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    
    startwrite = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    
    # Save preds
    predsfile_name = f'models/optimized/models/preds/preds_{str(pred_num)}.csv'
    s3_resource.Object(bucket_name, predsfile_name).put(Body=preds_df.to_csv())
    
    # Save RandomForest model
    modelfile_name = f'models/optimized/models/models/model_{pred_num}.sav'
    with tempfile.TemporaryFile() as fp:
        joblib.dump(rf, fp)
        fp.seek(0)
        s3_resource.Object(bucket_name, modelfile_name).put(Body=fp.read())
        
    string = f'{(stopread-startread)*1000}'
    encoded_string = string.encode("utf-8")
    
    timefile_name = f'models/optimized/models/runtimes/reads3_{str(pred_num)}.txt'
    s3_resource.Object(bucket_name, timefile_name).put(Body=encoded_string)
    
    string = f'{(stopcomp-startcomp)*1000}'
    encoded_string = string.encode("utf-8")
    
    timefile_name = f'models/optimized/models/runtimes/compute_{str(pred_num)}.txt'
    s3_resource.Object(bucket_name, timefile_name).put(Body=encoded_string)
    
    string = f'{(startwrite)*1000}'
    encoded_string = string.encode("utf-8")
    
    timefile_name = f'models/optimized/models/runtimes/startwrites3_{str(pred_num)}.txt'
    s3_resource.Object(bucket_name, timefile_name).put(Body=encoded_string)
        
    return
