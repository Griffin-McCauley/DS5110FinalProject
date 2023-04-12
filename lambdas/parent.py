import json
import boto3
 
# Define the client to interact with AWS Lambda
client = boto3.client('lambda')
 
def lambda_handler(event,context):
    try:
        
        files = ["LICI_minute_data_with_indicators.csv",'YESBANK_minute_data_with_indicators.csv']
        
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
                     'NIFTY 50_minute_data_with_indicators.csv',
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
                     'NIFTY BANK_minute_data_with_indicators.csv',
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
                     
        stocks = ["LICI","YESBANK"]
        
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
                      
        bucket_name = event["bucket"]
        type = event["type"]
        
        if (type == "processing"):
            for file in all_files:
                
                # Define the input parameters that will be passed
                # on to the child function
                inputParams = {
                    "bucket"   : bucket_name,
                    "file"      : file
                }
                
                client.invoke(
                    FunctionName = 'arn:aws:lambda:us-east-1:875764137927:function:ds5110processing',
                    InvocationType = 'Event',
                    Payload = json.dumps(inputParams)
                )
                
        elif (type == "modeling"):
            for stock in all_stocks:
                
                # Define the input parameters that will be passed
                # on to the child function
                inputParams = {
                    "bucket"   : bucket_name,
                    "stock"      : stock
                }
                
                client.invoke(
                    FunctionName = 'arn:aws:lambda:us-east-1:875764137927:function:ds5110modeling',
                    InvocationType = 'Event',
                    Payload = json.dumps(inputParams)
                )
                
        elif (type == "predict"):
            for stock in all_stocks:
                
                # Define the input parameters that will be passed
                # on to the child function
                inputParams = {
                    "bucket"   : bucket_name,
                    "stock"      : stock
                }
                
                client.invoke(
                    FunctionName = 'arn:aws:lambda:us-east-1:875764137927:function:ds5110predict',
                    InvocationType = 'Event',
                    Payload = json.dumps(inputParams)
                )
        
    except Exception as err:
        print(err)
