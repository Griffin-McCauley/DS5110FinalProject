import json
import boto3

s3_resource = boto3.resource('s3')

stocks = ['KOTAKBANK','SBILIFE']

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
                      #'NIFTY 50',
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
                      #'NIFTY BANK',
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

def lambda_handler(event, context):
    for stock in all_stocks:
        bucket = s3_resource.Bucket(f'{stock.lower()}-ds5110s3bucket')
        bucket.objects.all().delete()
