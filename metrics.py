import boto3
import pandas as pd
import numpy as np
import json

s3_resource = boto3.resource('s3')

def lambda_handler(event, context):
    try:
        bucket_name = 'ds5110s3'
        
        stocks = ['LICI','YESBANK']
        
        all_stocks = ['KOTAKBANK',
                      'SBILIFE',
                      'DRREDDY',
                      'LUPIN',
                      'TORNTPHARM',
                      'SBIN',
                      'DLF',
                      'BIOCON',
                      'ICICIPRULI',
                      'CHOLAFIN',
                      'MARICO',
                      'COALINDIA',
                      'BANDHANBNK',
                      'NTPC',
                      'BERGEPAINT',
                      'BRITANNIA',
                      'MARUTI',
                      'HINDUNILVR',
                      'ICICIBANK',
                      'ADANIGREEN',
                      'BAJAJ-AUTO',
                      'LICI',
                      'RELIANCE',
                      'NAUKRI',
                      'HDFCAMC',
                      'ASIANPAINT',
                      'POWERGRID',
                      'BPCL',
                      'INDUSTOWER',
                      'TCS',
                      'JINDALSTEL',
                      'NESTLEIND',
                      'NIFTY 50',
                      'GAIL',
                      'BAJAJFINSV',
                      'ITC',
                      'YESBANK',
                      'MUTHOOTFIN',
                      'LTI',
                      'MM',
                      'LT',
                      'HCLTECH',
                      'JSWSTEEL',
                      'IOC',
                      'VEDL',
                      'ADANIPORTS',
                      'ADANIENT',
                      'TATACONSUM',
                      'SIEMENS',
                      'CIPLA',
                      'INFY',
                      'NMDC',
                      'PGHH',
                      'HDFCLIFE',
                      'HDFCBANK',
                      'AUROPHARMA',
                      'EICHERMOT',
                      'SBICARD',
                      'PIDILITIND',
                      'NIFTY BANK',
                      'TATAMOTORS',
                      'SUNPHARMA',
                      'UPL',
                      'BHARTIARTL',
                      'GODREJCP',
                      'APOLLOHOSP',
                      'HAVELLS',
                      'DABUR',
                      'SHREECEM',
                      'HDFC',
                      'COLPAL',
                      'DMART',
                      'AMBUJACEM',
                      'PIIND',
                      'BOSCHLTD',
                      'BANKBARODA',
                      'GRASIM',
                      'TITAN',
                      'GLAND',
                      'TECHM',
                      'SAIL',
                      'BAJFINANCE',
                      'IGL',
                      'ACC',
                      'INDIGO',
                      'ICICIGI',
                      'MCDOWELL-N',
                      'JUBLFOOD',
                      'HINDALCO',
                      'ONGC',
                      'WIPRO',
                      'TATASTEEL',
                      'HEROMOTOCO',
                      'DIVISLAB',
                      'PNB',
                      'INDUSINDBK',
                      'BAJAJHLDNG',
                      'PEL',
                      'AXISBANK',
                      'ULTRACEMCO',
                      'HINDPETRO']
        
        print('pres3 processing runtimes')
        preruntimes = []
        
        for stock in all_stocks:
            infile_name = f'processed/runtimes/pres3/{stock}.txt'
            resp = s3_resource.Object(bucket_name, infile_name).get()
    
            runtime = float(resp['Body'].read().decode("utf-8"))
            preruntimes.append(runtime)
        print(f'Sum of processing runtimes: {sum(preruntimes)}')
        print(f'Max of processing runtimes: {max(preruntimes)}')
        print(c)
        
        print('posts3 processing runtimes')
        postruntimes = []
        
        for stock in all_stocks:
            infile_name = f'processed/runtimes/posts3/{stock}.txt'
            resp = s3_resource.Object(bucket_name, infile_name).get()
    
            runtime = float(resp['Body'].read().decode("utf-8"))
            postruntimes.append(runtime)
        print(f'Sum of processing runtimes: {sum(postruntimes)}')
        print(f'Max of processing runtimes: {max(postruntimes)}')
        
        print(f'Sum difference: {sum(postruntimes) - sum(preruntimes)}')
        print(f'Max difference: {max(postruntimes) - max(preruntimes)}')
        
        print('pres3 modeling runtimes')
        preruntimes = []
        
        for stock in all_stocks:
            infile_name = f'models/runtimes/pres3/{stock}.txt'
            resp = s3_resource.Object(bucket_name, infile_name).get()
    
            runtime = float(resp['Body'].read().decode("utf-8"))
            preruntimes.append(runtime)
        print(f'Sum of modeling runtimes: {sum(preruntimes)}')
        print(f'Max of modeling runtimes: {max(preruntimes)}')
        
        print('posts3 modeling runtimes')
        postruntimes = []
        
        for stock in all_stocks:
            infile_name = f'models/runtimes/posts3/{stock}.txt'
            resp = s3_resource.Object(bucket_name, infile_name).get()
    
            runtime = float(resp['Body'].read().decode("utf-8"))
            postruntimes.append(runtime)
        print(f'Sum of modeling runtimes: {sum(postruntimes)}')
        print(f'Max of modeling runtimes: {max(postruntimes)}')
        
        print(f'Sum difference: {sum(postruntimes) - sum(preruntimes)}')
        print(f'Max difference: {max(postruntimes) - max(preruntimes)}')
        
        print('MSE')
        mses = []
        
        for stock in all_stocks:
            infile_name = f'models/mses/{stock}.txt'
            resp = s3_resource.Object(bucket_name, infile_name).get()
    
            mse = float(resp['Body'].read().decode("utf-8"))
            print(mse)
            mses.append(mse)
        print(f'Average MSE: {np.mean(mses)}')
        
    except Exception as err:
        return err
