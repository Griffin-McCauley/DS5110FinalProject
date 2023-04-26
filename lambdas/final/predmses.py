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
            
    valfile_name = f'processed/validation/{stock}.csv'
    startread = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    valresp = s3_resource.Object(bucket_name, valfile_name).get()
    val = pd.read_csv(valresp['Body'], sep=',', index_col = 0)
    stopread = datetime.now(pytz.timezone('US/Eastern')).timestamp()
    
    val_y = pd.DataFrame(val["TYPPRICE_TARGET"])
            
    comp_time = 0
    read_time = (stopread-startread)
    write_time = 0
    
    feats = [5,7,9]
    samps = [5,15,25]
    for feat in feats:
        for samp in samps:
            c = 0
            for i in range(3):
                try:
                    file_name = f'models/params/feat{feat}_samp{samp}/preds/preds_{i}.csv'
                    startread = datetime.now(pytz.timezone('US/Eastern')).timestamp()
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
            mse = ((total_pred - val_y)**2).mean().item()
            mse_df = pd.DataFrame({'feat': [feat], 'samp': [samp], 'mse': [mse]})
            stopcomp = datetime.now(pytz.timezone('US/Eastern')).timestamp()
            comp_time += (stopcomp-startcomp)
            
            msefile_name = f'models/params/feat{feat}_samp{samp}/mse.csv'
            startwrite = datetime.now(pytz.timezone('US/Eastern')).timestamp()
            s3_resource.Object(bucket_name, msefile_name).put(Body=mse_df.to_csv(index=False))
            stopwrite = datetime.now(pytz.timezone('US/Eastern')).timestamp()
            write_time += (stopwrite-startwrite)

            string = f'{read_time*1000}'
            encoded_string = string.encode("utf-8")
            
            timefile_name = f'models/params/feat{feat}_samp{samp}/runtimes/reads3.txt'
            s3_resource.Object(bucket_name, timefile_name).put(Body=encoded_string)
            
            string = f'{write_time*1000}'
            encoded_string = string.encode("utf-8")
            
            timefile_name = f'models/params/feat{feat}_samp{samp}/runtimes/writes3.txt'
            s3_resource.Object(bucket_name, timefile_name).put(Body=encoded_string)
            
            string = f'{comp_time*1000}'
            encoded_string = string.encode("utf-8")
            
            timefile_name = f'models/params/feat{feat}_samp{samp}/runtimes/compute.txt'
            s3_resource.Object(bucket_name, timefile_name).put(Body=encoded_string)
