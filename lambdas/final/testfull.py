import json
import numpy as np
import pandas as pd
import boto3
from datetime import datetime
import pytz

s3_resource = boto3.resource('s3')

def lambda_handler(event, context):
            
    bucket_name = event['bucket']
    stock = event['stock']
            
    startread = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    testfile_name = f'processed/testing/{stock}.csv'
    testresp = s3_resource.Object(bucket_name, testfile_name).get()
    test = pd.read_csv(testresp['Body'], sep=',', index_col = 0)
    stopread = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    test_y = pd.DataFrame(test["TYPPRICE_TARGET"])
    
    read_time = (stopread-startread)
    comp_time = 0
            
    c = 0
    for i in range(30):
        try:
            startread = datetime.now(pytz.timezone('US/Eastern')).timestamp()
            file_name = f'models/optimized/models/preds/preds_{i}.csv'
            resp = s3_resource.Object(bucket_name, file_name).get()
            preds = pd.read_csv(resp['Body'], sep=',', index_col = 0)
            stopread = datetime.now(pytz.timezone('US/Eastern')).timestamp()
            startcomp = datetime.now(pytz.timezone('US/Eastern')).timestamp()
            if (i == 0):
                total_pred = preds
            else:
                total_pred = total_pred + preds
            c += 1
            stopcomp = datetime.now(pytz.timezone('US/Eastern')).timestamp()
            
            read_time += (stopread-startread)
            comp_time += (stopcomp-startcomp)
        except Exception as err:
            print(err)
    startcomp = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    total_pred = total_pred/(c)
    mse = ((total_pred - test_y)**2).mean().item()
    mse_df = pd.DataFrame({'mse': [mse]})
    msefile_name = f'models/optimized/models/mse.csv'
    stopcomp = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    comp_time += (stopcomp-startcomp)
    startwrite = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    s3_resource.Object(bucket_name, msefile_name).put(Body=mse_df.to_csv(index=False))
    stopwrite = datetime.now(pytz.timezone('US/Eastern')).timestamp()

    string = f'{read_time*1000}'
    encoded_string = string.encode("utf-8")
    
    timefile_name = f'models/optimized/models/runtimes/reads3.txt'
    s3_resource.Object(bucket_name, timefile_name).put(Body=encoded_string)
    
    string = f'{comp_time*1000}'
    encoded_string = string.encode("utf-8")
    
    timefile_name = f'models/optimized/models/runtimes/compute.txt'
    s3_resource.Object(bucket_name, timefile_name).put(Body=encoded_string)
    
    string = f'{(startwrite)*1000}'
    encoded_string = string.encode("utf-8")
    
    timefile_name = f'models/optimized/models/runtimes/startwrites3.txt'
    s3_resource.Object(bucket_name, timefile_name).put(Body=encoded_string)
