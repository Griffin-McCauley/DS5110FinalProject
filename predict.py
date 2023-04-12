import json
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import joblib
import pandas as pd
import numpy as np
import boto3
import tempfile

# function to prepare data for final training. 
# It takes in a train df, copies , gets target variable and returns x_train, y_train (vector)
def prep_rf_data_final(train_df):
    train = train_df.copy()
    train.dropna(inplace = True)
    y_train = train['TYPPRICE_TARGET'].copy()
    train.drop(columns = ['TYPPRICE_TARGET'], inplace = True)
    return train, y_train

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

def lambda_handler(event, context):
    bucket_name = event["bucket"]
    stock = event["stock"]
    key = f'models/models/{stock}.sav'
    
    # Load RandomForest model
    with tempfile.TemporaryFile() as fp:
        s3_client.download_fileobj(Fileobj=fp, Bucket=bucket_name, Key=key)
        fp.seek(0)
        loaded_model = joblib.load(fp)
        
    s3_resource = boto3.resource('s3')
    
    testfile_name = f'processed/testing/{stock}.csv'
    resp = s3_resource.Object(bucket_name, testfile_name).get()
    test = pd.read_csv(resp['Body'], sep=',')
    
    # prep final test data
    test_final, y_test_final = prep_rf_data_final(test)

    # predict for test data
    rf_preds = loaded_model.predict(test_final)
    # calculate mse 
    mse = mean_squared_error(y_test_final, rf_preds)
    
    string = f'{mse}'
    encoded_string = string.encode("utf-8")
    print(string)
    
    msefile_name = f'models/mses/{stock}.txt'
    s3_resource.Object(bucket_name, msefile_name).put(Body=encoded_string)
    
    # store predictions for future analysis
    test["predictions"] = rf_preds
