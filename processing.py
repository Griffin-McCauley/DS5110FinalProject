import boto3
import pandas as pd
from datetime import datetime
import pytz

s3_resource = boto3.resource('s3')

def lambda_handler(event, context):
    try:
        start = datetime.now(pytz.timezone('US/Eastern')).timestamp()
        
        bucket_name = event["bucket"]
        file = event["file"]
        infile_name = f'raw/{file}'
        resp = s3_resource.Object(bucket_name, infile_name).get()

        df = pd.read_csv(resp['Body'], sep=',')
       
        # add date-based variables
        df['month'] = pd.to_datetime(df['date']).dt.month
        df['day'] = pd.to_datetime(df['date']).dt.day
        df['hour'] = pd.to_datetime(df['date']).dt.hour
        df['minute'] = pd.to_datetime(df['date']).dt.minute
        df['dow'] = pd.to_datetime(df['date']).dt.dayofweek
    
        # set and sort date index
        df.set_index("date", inplace=True)
        df.sort_index(inplace=True)
    
        # extract relevant columns
        df = pd.concat([df.iloc[:,:10], df.iloc[:,-5:], df["TYPPRICE"]], axis=1)
    
        # add lags
        for i in range(1,11):
            df[f'TYPPRICE_LAG_{i}'] = df["TYPPRICE"].shift(i)
    
        # add target variable
        df['TYPPRICE_TARGET'] = df['TYPPRICE'].shift(-1)
        
        # drop NA rows and extract simple lag columns
        df.dropna(inplace=True)
        df = df.iloc[:,-12:]
       
        # create splits
        splits = create_training_splits(df)
        
        pre = datetime.now(pytz.timezone('US/Eastern')).timestamp()
        
        # store files to s3
        for i, type in enumerate(["training","validation","testing"]):
            outfile_name = f'processed/{type}/{file.split("_")[0]}.csv'
            s3_resource.Object(bucket_name, outfile_name).put(Body=splits[i].to_csv(index=False))
            
        post = datetime.now(pytz.timezone('US/Eastern')).timestamp()
        
        string = f'{(pre-start)*1000}'
        encoded_string = string.encode("utf-8")
        print(string)
        
        timefile_name = f'processed/runtimes/pres3/{file.split("_")[0]}.txt'
        s3_resource.Object(bucket_name, timefile_name).put(Body=encoded_string)
        
        string = f'{(post-start)*1000}'
        encoded_string = string.encode("utf-8")
        print(string)
        
        timefile_name = f'processed/runtimes/posts3/{file.split("_")[0]}.txt'
        s3_resource.Object(bucket_name, timefile_name).put(Body=encoded_string)
        
        return f'{file} processed successfully.'
 
    except Exception as err:
        return err
      
# function to perform test-train split
def create_training_splits(data, test_frac = .2, validate_frac = .2, random_state = 2019):

    train_data_all = data.sample(frac = 1-test_frac, random_state = random_state)
    test_data = data.drop(train_data_all.index)

    train_data = train_data_all.sample(frac = 1-validate_frac, random_state = random_state)
    validate_data = train_data_all.drop(train_data.index)

    train_data = train_data.sort_index()
    test_data = test_data.sort_index()

    train_data = train_data.sort_index()
    validate_data = validate_data.sort_index()

    return train_data, validate_data, test_data
