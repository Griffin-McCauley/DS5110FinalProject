import json
import numpy as np
import pandas as pd
import boto3
from datetime import datetime
import pytz

s3_resource = boto3.resource('s3')

def lambda_handler(event, context):
            
    startcomp = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    bucket_name = event['bucket']
    stock = event['stock']
    
    feats = [5,7,9]
    samps = [5,15,25]
    mses = pd.DataFrame({})
    stopcomp = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    
    comp_time = (stopcomp-startcomp)
    read_time = 0
    for feat in feats:
        for samp in samps:
            try:
                file_name = f'models/params/feat{feat}_samp{samp}/mse.csv'
                startread = datetime.now(pytz.timezone('US/Eastern')).timestamp()
                resp = s3_resource.Object(bucket_name, file_name).get()
                mse = pd.read_csv(resp['Body'], sep=',')
                stopread = datetime.now(pytz.timezone('US/Eastern')).timestamp()
                
                startcomp = datetime.now(pytz.timezone('US/Eastern')).timestamp()
                mses = pd.concat([mses, mse])
                stopcomp = datetime.now(pytz.timezone('US/Eastern')).timestamp()
                
                read_time += (stopread-startread)
                comp_time += (stopcomp-startcomp)
            except Exception as err:
                print(err)
    startcomp = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    minmse = mses[mses.mse == mses.mse.min()]
    stopcomp = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    comp_time += (stopcomp-startcomp)
    msefile_name = f'models/optimized/best_params.csv'
    startwrite = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    s3_resource.Object(bucket_name, msefile_name).put(Body=minmse.to_csv(index=False))
    
    string = f'{read_time*1000}'
    encoded_string = string.encode("utf-8")
    
    timefile_name = f'models/optimized/runtimes/reads3.txt'
    s3_resource.Object(bucket_name, timefile_name).put(Body=encoded_string)
    
    string = f'{comp_time*1000}'
    encoded_string = string.encode("utf-8")
    
    timefile_name = f'models/optimized/runtimes/compute.txt'
    s3_resource.Object(bucket_name, timefile_name).put(Body=encoded_string)
    
    string = f'{(startwrite)*1000}'
    encoded_string = string.encode("utf-8")
    
    timefile_name = f'models/optimized/runtimes/startwrites3.txt'
    s3_resource.Object(bucket_name, timefile_name).put(Body=encoded_string)
