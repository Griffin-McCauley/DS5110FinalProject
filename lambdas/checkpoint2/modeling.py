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

# function to prepare data for training. 
# It takes in a train and test df, copies them, gets target variable and returns x_train, y_train (vector), x_test, y_test (vector), 
def prep_rf_data(train_df, test_df):
    train = train_df.copy()
    train.dropna(inplace = True)
    y_train = train['TYPPRICE_TARGET'].copy()
    train.drop(columns = ['TYPPRICE_TARGET'], inplace = True)
    
    test = test_df.copy()
    test.dropna(inplace = True)
    y_test = test['TYPPRICE_TARGET'].copy()
    test.drop(columns = ['TYPPRICE_TARGET'], inplace = True)
    return train, y_train, test, y_test


# function for actually training the RandomForest
def train_rf(train_df, y_train_df, test_df, y_test_df, feat, samp, params):
    rf = RandomForestRegressor(max_features = feat, 
                               min_samples_split = samp, 
                               **params)
    rf.fit(train_df, y_train_df)
    rf_preds = rf.predict(test_df)
    mse = mean_squared_error(y_test_df, rf_preds)
    return rf, mse, rf_preds


# function that iterates over a parameter list and returns MSE for each set of parameters
def search_params(train_df, y_train_df, test_df, y_test_df, params, feat_list, samp_list):
    rf_res_df = pd.DataFrame()
    for samp in samp_list:
        for feat in feat_list:
            rf_, mse, preds = train_rf(train_df, 
                                y_train_df, 
                                test_df, 
                                y_test_df, 
                                feat, 
                                samp, 
                                params)
            out_row = pd.DataFrame({'max features':[feat], 'sample split' : [samp], 'mse':mse})
            rf_res_df = pd.concat([rf_res_df, out_row])
    return rf_res_df

# wrapper function that builds necessary df for training and calls the grid search for Random Forest
def rf_param_search(train_data, val_data, params, feat_list, samp_list):   
    train_df, y_train_df, test_df, y_test_df = prep_rf_data(train_data, val_data)
    rf_res_df = search_params(train_df,
                              y_train_df, 
                              test_df, 
                              y_test_df, 
                              params, 
                              feat_list, 
                              samp_list)
    
    return rf_res_df

# function that looks for best set of hyperparameters based on a train/validate split
def optimize_rf(params_train, feat_list, samp_list, train_data, val_data): 
    rf_grid_res = rf_param_search(train_data, 
                                         val_data, 
                                         params_train, 
                                         feat_list, 
                                         samp_list)
    
    min_res = rf_grid_res[rf_grid_res['mse'] == rf_grid_res['mse'].min()]
    print('max feat: ' , min_res['max features'][0])
    print('min samp: ' , min_res['sample split'][0])
    return min_res


# function that builds final RandomForest with all data
def final_rf(params_final, optimize_results, final_train_data, test_data):
    features = optimize_results['max features'][0]
    samples = optimize_results['sample split'][0]
    train_df, y_train_df, test_df, y_test_df = prep_rf_data(final_train_data, 
                            test_data)
    rf_tuned, mse, preds = train_rf(train_df, y_train_df, test_df, y_test_df, features, samples, params_final)
    print("Test MSE: ", mse)
    print(len(test_df))
    print(len(preds))
    return rf_tuned, preds

# function to prepare data for final training. 
# It takes in a train df, copies , gets target variable and returns x_train, y_train (vector)
def prep_rf_data_final(train_df):
    train = train_df.copy()
    train.dropna(inplace = True)
    y_train = train['TYPPRICE_TARGET'].copy()
    train.drop(columns = ['TYPPRICE_TARGET'], inplace = True)
    return train, y_train
 
# function to save RF model 
def save_rf(rf_model, name):
    joblib.dump(rf_model, f"./{name}.joblib")
    return

s3_resource = boto3.resource('s3')

def lambda_handler(event, context):
    start = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    
    bucket_name = event["bucket"]
    stock = event["stock"]
    trainfile_name = f'processed/training/{stock}.csv'
    resp = s3_resource.Object(bucket_name, trainfile_name).get()
    train = pd.read_csv(resp['Body'], sep=',')
    validatefile_name = f'processed/training/{stock}.csv'
    resp = s3_resource.Object(bucket_name, validatefile_name).get()
    validate = pd.read_csv(resp['Body'], sep=',')
    
    # initial parameters 
    fea = [5,6,7,8]
    min_samp = [5,10,15]
    fea = [7]
    min_samp = [10]
    par1 = {'n_estimators': 20, 
                  'random_state': 0, 
                  'n_jobs': -1}
    # run optimization for best parameters
    optimize_results = optimize_rf(par1, fea, min_samp, train, validate)
    
    # build full training data set
    train_data_all = pd.concat([train, validate])
    
    # get best hyperparameters
    features = optimize_results['max features'][0]
    samples = optimize_results['sample split'][0]
    
    # build final training data for RF
    train_final, y_train_final = prep_rf_data_final(train_data_all)

    # final parameter list
    final_par = {'n_estimators': 20, 
                  'random_state': 0, 
                  'n_jobs': -1}
    # instantiate RF Regressor
    rf = RandomForestRegressor(max_features = features, 
                                   min_samples_split = samples, 
                                   **final_par)
    
    # fit model
    rf.fit(train_final, y_train_final)
    
    pre = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    
    # Save RandomForest model
    outfile_name = f'models/models/{stock}.sav'
    with tempfile.TemporaryFile() as fp:
        joblib.dump(rf, fp)
        fp.seek(0)
        s3_resource.Object(bucket_name, outfile_name).put(Body=fp.read())
        
    post = datetime.now(pytz.timezone('US/Eastern')).timestamp()
        
    string = f'{(pre-start)*1000}'
    encoded_string = string.encode("utf-8")
    print(string)
    
    timefile_name = f'models/runtimes/pres3/{stock}.txt'
    s3_resource.Object(bucket_name, timefile_name).put(Body=encoded_string)
    
    string = f'{(post-start)*1000}'
    encoded_string = string.encode("utf-8")
    print(string)
    
    timefile_name = f'models/runtimes/posts3/{stock}.txt'
    s3_resource.Object(bucket_name, timefile_name).put(Body=encoded_string)
    
    return f'{stock} modeled successfully.'
