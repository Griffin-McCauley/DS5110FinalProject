import json
import numpy as np
import pandas as pd
import boto3
from datetime import datetime
import pytz

all_files = ['KOTAKBANK_minute_data_with_indicators.csv',
                     'SBILIFE_minute_data_with_indicators.csv',
                     'DRREDDY_minute_data_with_indicators.csv',
                     'LUPIN_minute_data_with_indicators.csv',
                     'TORNTPHARM_minute_data_with_indicators.csv',
                     'SBIN_minute_data_with_indicators.csv',
                     'DLF_minute_data_with_indicators.csv',
                     'BIOCON_minute_data_with_indicators.csv',
                     'ICICIPRULI_minute_data_with_indicators.csv',
                     'CHOLAFIN_minute_data_with_indicators.csv',
                     'MARICO_minute_data_with_indicators.csv',
                     'COALINDIA_minute_data_with_indicators.csv',
                     'BANDHANBNK_minute_data_with_indicators.csv',
                     'NTPC_minute_data_with_indicators.csv',
                     'BERGEPAINT_minute_data_with_indicators.csv',
                     'BRITANNIA_minute_data_with_indicators.csv',
                     'MARUTI_minute_data_with_indicators.csv',
                     'HINDUNILVR_minute_data_with_indicators.csv',
                     'ICICIBANK_minute_data_with_indicators.csv',
                     'ADANIGREEN_minute_data_with_indicators.csv',
                     'BAJAJ-AUTO_minute_data_with_indicators.csv',
                     'LICI_minute_data_with_indicators.csv',
                     'RELIANCE_minute_data_with_indicators.csv',
                     'NAUKRI_minute_data_with_indicators.csv',
                     'HDFCAMC_minute_data_with_indicators.csv',
                     'ASIANPAINT_minute_data_with_indicators.csv',
                     'POWERGRID_minute_data_with_indicators.csv',
                     'BPCL_minute_data_with_indicators.csv',
                     'INDUSTOWER_minute_data_with_indicators.csv',
                     'TCS_minute_data_with_indicators.csv',
                     'JINDALSTEL_minute_data_with_indicators.csv',
                     'NESTLEIND_minute_data_with_indicators.csv',
                     #'NIFTY 50_minute_data_with_indicators.csv',
                     'GAIL_minute_data_with_indicators.csv',
                     'BAJAJFINSV_minute_data_with_indicators.csv',
                     'ITC_minute_data_with_indicators.csv',
                     'YESBANK_minute_data_with_indicators.csv',
                     'MUTHOOTFIN_minute_data_with_indicators.csv',
                     'LTI_minute_data_with_indicators.csv',
                     'MM_minute_data_with_indicators.csv',
                     'LT_minute_data_with_indicators.csv',
                     'HCLTECH_minute_data_with_indicators.csv',
                     'JSWSTEEL_minute_data_with_indicators.csv',
                     'IOC_minute_data_with_indicators.csv',
                     'VEDL_minute_data_with_indicators.csv',
                     'ADANIPORTS_minute_data_with_indicators.csv',
                     'ADANIENT_minute_data_with_indicators.csv',
                     'TATACONSUM_minute_data_with_indicators.csv',
                     'SIEMENS_minute_data_with_indicators.csv',
                     'CIPLA_minute_data_with_indicators.csv',
                     'INFY_minute_data_with_indicators.csv',
                     'NMDC_minute_data_with_indicators.csv',
                     'PGHH_minute_data_with_indicators.csv',
                     'HDFCLIFE_minute_data_with_indicators.csv',
                     'HDFCBANK_minute_data_with_indicators.csv',
                     'AUROPHARMA_minute_data_with_indicators.csv',
                     'EICHERMOT_minute_data_with_indicators.csv',
                     'SBICARD_minute_data_with_indicators.csv',
                     'PIDILITIND_minute_data_with_indicators.csv',
                     #'NIFTY BANK_minute_data_with_indicators.csv',
                     'TATAMOTORS_minute_data_with_indicators.csv',
                     'SUNPHARMA_minute_data_with_indicators.csv',
                     'UPL_minute_data_with_indicators.csv',
                     'BHARTIARTL_minute_data_with_indicators.csv',
                     'GODREJCP_minute_data_with_indicators.csv',
                     'APOLLOHOSP_minute_data_with_indicators.csv',
                     'HAVELLS_minute_data_with_indicators.csv',
                     'DABUR_minute_data_with_indicators.csv',
                     'SHREECEM_minute_data_with_indicators.csv',
                     'HDFC_minute_data_with_indicators.csv',
                     'COLPAL_minute_data_with_indicators.csv',
                     'DMART_minute_data_with_indicators.csv',
                     'AMBUJACEM_minute_data_with_indicators.csv',
                     'PIIND_minute_data_with_indicators.csv',
                     'BOSCHLTD_minute_data_with_indicators.csv',
                     'BANKBARODA_minute_data_with_indicators.csv',
                     'GRASIM_minute_data_with_indicators.csv',
                     'TITAN_minute_data_with_indicators.csv',
                     'GLAND_minute_data_with_indicators.csv',
                     'TECHM_minute_data_with_indicators.csv',
                     'SAIL_minute_data_with_indicators.csv',
                     'BAJFINANCE_minute_data_with_indicators.csv',
                     'IGL_minute_data_with_indicators.csv',
                     'ACC_minute_data_with_indicators.csv',
                     'INDIGO_minute_data_with_indicators.csv',
                     'ICICIGI_minute_data_with_indicators.csv',
                     'MCDOWELL-N_minute_data_with_indicators.csv',
                     'JUBLFOOD_minute_data_with_indicators.csv',
                     'HINDALCO_minute_data_with_indicators.csv',
                     'ONGC_minute_data_with_indicators.csv',
                     'WIPRO_minute_data_with_indicators.csv',
                     'TATASTEEL_minute_data_with_indicators.csv',
                     'HEROMOTOCO_minute_data_with_indicators.csv',
                     'DIVISLAB_minute_data_with_indicators.csv',
                     'PNB_minute_data_with_indicators.csv',
                     'INDUSINDBK_minute_data_with_indicators.csv',
                     'BAJAJHLDNG_minute_data_with_indicators.csv',
                     'PEL_minute_data_with_indicators.csv',
                     'AXISBANK_minute_data_with_indicators.csv',
                     'ULTRACEMCO_minute_data_with_indicators.csv',
                     'HINDPETRO_minute_data_with_indicators.csv']

s3_resource = boto3.resource('s3')

def lambda_handler(event, context):
    all_stocks = [file.split("_")[0] for file in all_files]
    fullparalleltimingmetrics = np.zeros([7,3])
    actualtimingmetrics = np.zeros([7,3])
    sequentialtimingmetrics = np.zeros([7,3])
    
    # Invocation Times
    print("-----")
    print('Invocation Times')
    bucket_name = 'ds5110s3'
    invoke_times = []
    for i, type in enumerate(['processing', 'parampreds', 'predmses', 'minmse', 'trainfull', 'testfull']):
        infile_name = f'runtimes/invoke_{type}.txt'
        resp = s3_resource.Object(bucket_name, infile_name).get()

        runtime = float(resp['Body'].read().decode("utf-8"))
        invoke_times.append(runtime)
        fullparalleltimingmetrics[i,0] = runtime
        actualtimingmetrics[i,0] = runtime
        sequentialtimingmetrics[i,0] = runtime
        
    print(f'Sum of invocation runtimes: {sum(invoke_times)}')
    
    fullparalleltimingmetrics[6,0] = sum(invoke_times)
    actualtimingmetrics[6,0] = sum(invoke_times)
    sequentialtimingmetrics[6,0] = sum(invoke_times)
    
    # Computation Times
    print("-----")
    print('Computation Times')
    feats = [5,7,9]
    samps = [5,15,25]
    processing_times = []
    parampreds_times = []
    predmses_times = []
    minmse_times = []
    trainfull_times = []
    testfull_times = []
    for stock in all_stocks:
        bucket_name = f'{stock.lower()}-ds5110s3bucket'
        
        # processing
        timefile_name = f'processed/runtimes/compute.txt'
        resp = s3_resource.Object(bucket_name, timefile_name).get()
        runtime = float(resp['Body'].read().decode("utf-8"))
        processing_times.append(runtime)
        fullparalleltimingmetrics[0,1] = max(processing_times)
        actualtimingmetrics[0,1] = max(processing_times)
        sequentialtimingmetrics[0,1] = sum(processing_times)
        
        # parampreds
        for feat in feats:
            for samp in samps:
                for pred_num in range(3):
                    timefile_name = f'models/params/feat{feat}_samp{samp}/runtimes/compute_{str(pred_num)}.txt'
                    resp = s3_resource.Object(bucket_name, timefile_name).get()
                    runtime = float(resp['Body'].read().decode("utf-8"))
                    parampreds_times.append(runtime)
        fullparalleltimingmetrics[1,1] = max(parampreds_times)
        actualtimingmetrics[1,1] = sum(sorted(parampreds_times, reverse=True)[:3])
        sequentialtimingmetrics[1,1] = sum(parampreds_times)
        
        # predmses
        for feat in feats:
            for samp in samps:
                timefile_name = f'models/params/feat{feat}_samp{samp}/runtimes/compute.txt'
                resp = s3_resource.Object(bucket_name, timefile_name).get()
                runtime = float(resp['Body'].read().decode("utf-8"))
                predmses_times.append(runtime)
        fullparalleltimingmetrics[2,1] = max(predmses_times)
        actualtimingmetrics[2,1] = max(predmses_times)
        sequentialtimingmetrics[2,1] = sum(predmses_times)
        
        # minmse
        timefile_name = f'models/optimized/runtimes/compute.txt'
        resp = s3_resource.Object(bucket_name, timefile_name).get()
        runtime = float(resp['Body'].read().decode("utf-8"))
        minmse_times.append(runtime)
        fullparalleltimingmetrics[3,1] = max(minmse_times)
        actualtimingmetrics[3,1] = max(minmse_times)
        sequentialtimingmetrics[3,1] = sum(minmse_times)
        
        # trainfull
        for pred_num in range(30):
            timefile_name = f'models/optimized/models/runtimes/compute_{str(pred_num)}.txt'
            resp = s3_resource.Object(bucket_name, timefile_name).get()
            runtime = float(resp['Body'].read().decode("utf-8"))
            trainfull_times.append(runtime)
        fullparalleltimingmetrics[4,1] = max(trainfull_times)
        actualtimingmetrics[4,1] = sum(sorted(trainfull_times, reverse=True)[:3])
        sequentialtimingmetrics[4,1] = sum(trainfull_times)
        
        # testfull
        timefile_name = f'models/optimized/models/runtimes/compute.txt'
        resp = s3_resource.Object(bucket_name, timefile_name).get()
        runtime = float(resp['Body'].read().decode("utf-8"))
        testfull_times.append(runtime)
        fullparalleltimingmetrics[5,1] = max(testfull_times)
        actualtimingmetrics[5,1] = max(testfull_times)
        sequentialtimingmetrics[5,1] = sum(testfull_times)
        
    print('Processing')
    print(f'Sum of processing runtimes: {sum(processing_times)}')
    print(f'Max of processing runtimes: {max(processing_times)}')
    
    print('.')
    print('Parampreds')
    print(f'Sum of parampreds runtimes: {sum(parampreds_times)}')
    print(f'Max of parampreds runtimes: {max(parampreds_times)}')
    
    print('.')
    print('Predmses')
    print(f'Sum of predmses runtimes: {sum(predmses_times)}')
    print(f'Max of predmses runtimes: {max(predmses_times)}')
    
    print('.')
    print('Minmse')
    print(f'Sum of minmse runtimes: {sum(minmse_times)}')
    print(f'Max of minmse runtimes: {max(minmse_times)}')
    
    print('.')
    print('Trainfull')
    print(f'Sum of trainfull runtimes: {sum(trainfull_times)}')
    print(f'Max of trainfull runtimes: {max(trainfull_times)}')
    
    print('.')
    print('Testfull')
    print(f'Sum of testfull runtimes: {sum(testfull_times)}')
    print(f'Max of testfull runtimes: {max(testfull_times)}')
    
    print('.')
    print(f'Total of sequential computation runtimes: {sum(processing_times)+sum(parampreds_times)+sum(predmses_times)+sum(minmse_times)+sum(trainfull_times)+sum(testfull_times)}')
    print(f'Total of parallel computation runtimes: {max(processing_times)+max(parampreds_times)+max(predmses_times)+max(minmse_times)+max(trainfull_times)+max(testfull_times)}')
    
    fullparalleltimingmetrics[6,1] = fullparalleltimingmetrics[0,1] + fullparalleltimingmetrics[1,1] + fullparalleltimingmetrics[2,1] + \
                                     fullparalleltimingmetrics[3,1] + fullparalleltimingmetrics[4,1] + fullparalleltimingmetrics[5,1]
    actualtimingmetrics[6,1] = actualtimingmetrics[0,1] + actualtimingmetrics[1,1] + actualtimingmetrics[2,1] + \
                                     actualtimingmetrics[3,1] + actualtimingmetrics[4,1] + actualtimingmetrics[5,1]
    sequentialtimingmetrics[6,1] = sequentialtimingmetrics[0,1] + sequentialtimingmetrics[1,1] + sequentialtimingmetrics[2,1] + \
                                     sequentialtimingmetrics[3,1] + sequentialtimingmetrics[4,1] + sequentialtimingmetrics[5,1]
    
    # I/O and Throttling Time
    print("-----")
    print('I/O and Throttling Time')
    feats = [5,7,9]
    samps = [5,15,25]
    processing_iotimes = []
    parampreds_iotimes = []
    predmses_iotimes = []
    minmse_iotimes = []
    trainfull_iotimes = []
    testfull_iotimes = []
    for stock in all_stocks:
        bucket_name = f'{stock.lower()}-ds5110s3bucket'
        
        # processing
        timefile_name = f'processed/runtimes/reads3.txt'
        resp = s3_resource.Object(bucket_name, timefile_name).get()
        read = float(resp['Body'].read().decode("utf-8"))
        timefile_name = f'processed/runtimes/startwrites3.txt'
        resp = s3_resource.Object(bucket_name, timefile_name).get()
        writestart = float(resp['Body'].read().decode("utf-8"))
        s3_object = s3_resource.Object(bucket_name, f'processed/training/{stock}.csv')
        writeend = s3_object.last_modified.astimezone(pytz.timezone('US/Eastern')).timestamp()*1000
        processing_iotimes.append(read + (writeend-writestart))
        fullparalleltimingmetrics[0,2] = max(processing_iotimes)
        actualtimingmetrics[0,2] = max(processing_iotimes)
        sequentialtimingmetrics[0,2] = sum(processing_iotimes)
    
        # parampreds
        for feat in feats:
            for samp in samps:
                for pred_num in range(3):
                    timefile_name = f'models/params/feat{feat}_samp{samp}/runtimes/reads3_{str(pred_num)}.txt'
                    resp = s3_resource.Object(bucket_name, timefile_name).get()
                    read = float(resp['Body'].read().decode("utf-8"))
                    timefile_name = f'models/params/feat{feat}_samp{samp}/runtimes/startwrites3_{str(pred_num)}.txt'
                    resp = s3_resource.Object(bucket_name, timefile_name).get()
                    writestart = float(resp['Body'].read().decode("utf-8"))
                    s3_object = s3_resource.Object(bucket_name, f'models/params/feat{feat}_samp{samp}/preds/preds_{str(pred_num)}.csv')
                    writeend = s3_object.last_modified.astimezone(pytz.timezone('US/Eastern')).timestamp()*1000
                    parampreds_iotimes.append(read + (writeend-writestart))
        fullparalleltimingmetrics[1,2] = max(parampreds_iotimes)
        actualtimingmetrics[1,2] = sum(sorted(parampreds_iotimes, reverse=True)[:3])
        sequentialtimingmetrics[1,2] = sum(parampreds_iotimes)

        # predmses
        for feat in feats:
            for samp in samps:
                timefile_name = f'models/params/feat{feat}_samp{samp}/runtimes/reads3.txt'
                resp = s3_resource.Object(bucket_name, timefile_name).get()
                read = float(resp['Body'].read().decode("utf-8"))
                timefile_name = f'models/params/feat{feat}_samp{samp}/runtimes/writes3.txt'
                resp = s3_resource.Object(bucket_name, timefile_name).get()
                write = float(resp['Body'].read().decode("utf-8"))
                predmses_iotimes.append(read + write)
        fullparalleltimingmetrics[2,2] = max(predmses_iotimes)
        actualtimingmetrics[2,2] = max(predmses_iotimes)
        sequentialtimingmetrics[2,2] = sum(predmses_iotimes)
        
        # minmse
        timefile_name = f'models/optimized/runtimes/reads3.txt'
        resp = s3_resource.Object(bucket_name, timefile_name).get()
        read = float(resp['Body'].read().decode("utf-8"))
        timefile_name = f'models/optimized/runtimes/startwrites3.txt'
        resp = s3_resource.Object(bucket_name, timefile_name).get()
        writestart = float(resp['Body'].read().decode("utf-8"))
        s3_object = s3_resource.Object(bucket_name, f'models/optimized/best_params.csv')
        writeend = s3_object.last_modified.astimezone(pytz.timezone('US/Eastern')).timestamp()*1000
        minmse_iotimes.append(read + (writeend-writestart))
        fullparalleltimingmetrics[3,2] = max(minmse_iotimes)
        actualtimingmetrics[3,2] = max(minmse_iotimes)
        sequentialtimingmetrics[3,2] = sum(minmse_iotimes)
        
        # trainfull
        for pred_num in range(30):
            timefile_name = f'models/optimized/models/runtimes/reads3_{str(pred_num)}.txt'
            resp = s3_resource.Object(bucket_name, timefile_name).get()
            read = float(resp['Body'].read().decode("utf-8"))
            timefile_name = f'models/optimized/models/runtimes/startwrites3_{str(pred_num)}.txt'
            resp = s3_resource.Object(bucket_name, timefile_name).get()
            writestart = float(resp['Body'].read().decode("utf-8"))
            s3_object = s3_resource.Object(bucket_name, f'models/optimized/models/models/model_{str(pred_num)}.sav')
            writeend = s3_object.last_modified.astimezone(pytz.timezone('US/Eastern')).timestamp()*1000
            trainfull_iotimes.append(read + (writeend-writestart))
        fullparalleltimingmetrics[4,2] = max(trainfull_iotimes)
        actualtimingmetrics[4,2] = sum(sorted(trainfull_iotimes, reverse=True)[:3])
        sequentialtimingmetrics[4,2] = sum(trainfull_iotimes)
        
        # testfull
        timefile_name = f'models/optimized/models/runtimes/reads3.txt'
        resp = s3_resource.Object(bucket_name, timefile_name).get()
        read = float(resp['Body'].read().decode("utf-8"))
        timefile_name = f'models/optimized/models/runtimes/startwrites3.txt'
        resp = s3_resource.Object(bucket_name, timefile_name).get()
        writestart = float(resp['Body'].read().decode("utf-8"))
        s3_object = s3_resource.Object(bucket_name, f'models/optimized/models/mse.csv')
        writeend = s3_object.last_modified.astimezone(pytz.timezone('US/Eastern')).timestamp()*1000
        testfull_iotimes.append(read + (writeend-writestart))
        fullparalleltimingmetrics[5,2] = max(testfull_iotimes)
        actualtimingmetrics[5,2] = max(testfull_iotimes)
        sequentialtimingmetrics[5,2] = sum(testfull_iotimes)
        
    print('Processing')
    print(f'Sum of processing I/O runtimes: {sum(processing_iotimes)}')
    print(f'Max of processing I/O runtimes: {max(processing_iotimes)}')
    
    print('.')
    print('Parampreds')
    print(f'Sum of parampreds I/O runtimes: {sum(parampreds_iotimes)}')
    print(f'Max of parampreds I/O runtimes: {max(parampreds_iotimes)}')
    
    print('.')
    print('Predmses')
    print(f'Sum of predmses I/O runtimes: {sum(predmses_iotimes)}')
    print(f'Max of predmses I/O runtimes: {max(predmses_iotimes)}')
    
    print('.')
    print('Minmse')
    print(f'Sum of minmse I/O runtimes: {sum(minmse_iotimes)}')
    print(f'Max of minmse I/O runtimes: {max(minmse_iotimes)}')
    
    print('.')
    print('Trainfull')
    print(f'Sum of trainfull I/O runtimes: {sum(trainfull_iotimes)}')
    print(f'Max of trainfull I/O runtimes: {max(trainfull_iotimes)}')
    
    print('.')
    print('Testfull')
    print(f'Sum of testfull I/O runtimes: {sum(testfull_iotimes)}')
    print(f'Max of testfull I/O runtimes: {max(testfull_iotimes)}')
    
    print('.')
    print(f'Total of sequential I/O runtimes: {sum(processing_iotimes)+sum(parampreds_iotimes)+sum(predmses_iotimes)+sum(minmse_iotimes)+sum(trainfull_iotimes)+sum(testfull_iotimes)}')
    print(f'Total of parallel I/O runtimes: {max(processing_iotimes)+max(parampreds_iotimes)+max(predmses_iotimes)+max(minmse_iotimes)+max(trainfull_iotimes)+max(testfull_iotimes)}')
    
    fullparalleltimingmetrics[6,2] = fullparalleltimingmetrics[0,2] + fullparalleltimingmetrics[1,2] + fullparalleltimingmetrics[2,2] + \
                                     fullparalleltimingmetrics[3,2] + fullparalleltimingmetrics[4,2] + fullparalleltimingmetrics[5,2]
    actualtimingmetrics[6,2] = actualtimingmetrics[0,2] + actualtimingmetrics[1,2] + actualtimingmetrics[2,2] + \
                                     actualtimingmetrics[3,2] + actualtimingmetrics[4,2] + actualtimingmetrics[5,2]
    sequentialtimingmetrics[6,2] = sequentialtimingmetrics[0,2] + sequentialtimingmetrics[1,2] + sequentialtimingmetrics[2,2] + \
                                     sequentialtimingmetrics[3,2] + sequentialtimingmetrics[4,2] + sequentialtimingmetrics[5,2]
    
    df = pd.DataFrame(fullparalleltimingmetrics)
    df.columns = ['Invocation','Computation','I/O']
    df.index = ['processing','parampreds','predmses','minmse','trainfull','testfull','total']
    file_name = 'timingmetrics/fullparalleltimingmetrics.csv'
    s3_resource.Object('ds5110s3', file_name).put(Body=df.to_csv())
    
    df = pd.DataFrame(actualtimingmetrics)
    df.columns = ['Invocation','Computation','I/O']
    df.index = ['processing','parampreds','predmses','minmse','trainfull','testfull','total']
    file_name = 'timingmetrics/actualtimingmetrics.csv'
    s3_resource.Object('ds5110s3', file_name).put(Body=df.to_csv())
    
    df = pd.DataFrame(sequentialtimingmetrics)
    df.columns = ['Invocation','Computation','I/O']
    df.index = ['processing','parampreds','predmses','minmse','trainfull','testfull','total']
    file_name = 'timingmetrics/sequentialtimingmetrics.csv'
    s3_resource.Object('ds5110s3', file_name).put(Body=df.to_csv())
    
    df = pd.DataFrame([processing_times,parampreds_times,predmses_times,minmse_times,trainfull_times,testfull_times])
    df.index = ['processing','parampreds','predmses','minmse','trainfull','testfull']
    file_name = 'timingmetrics/instancecomputetimingmetrics.csv'
    s3_resource.Object('ds5110s3', file_name).put(Body=df.to_csv(header=False))
    
    df = pd.DataFrame([processing_iotimes,parampreds_iotimes,predmses_iotimes,minmse_iotimes,trainfull_iotimes,testfull_iotimes])
    df.index = ['processing','parampreds','predmses','minmse','trainfull','testfull']
    file_name = 'timingmetrics/instanceiotimingmetrics.csv'
    s3_resource.Object('ds5110s3', file_name).put(Body=df.to_csv(header=False))
