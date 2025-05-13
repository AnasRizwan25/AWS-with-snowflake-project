import yfinance as yf
import pandas as pd
from datetime import datetime,timedelta
import boto3
import csv

def get_sp500_Symbols():
    sp_500_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies' 
    tables = pd.read_html(sp_500_url) 
    sp_500 = tables[0]
    return sp_500['Symbol'].tolist()

def get_finance_data (symbols,start_date,end_date, interval):
    result = {}
    for symbol in symbols:
        data = yf.download(tickers = symbol,start=start_date,end = end_date, interval=interval )
        if not data.empty:
            result[symbol] = data
    return result 

def transform_data(data, symbol):
    data.columns = [col[0] for col in data.columns]
    data = data.reset_index()
    
    data["symbol"] = symbol
    data['close_change'] = data['Close'].diff().fillna(0)
    data['close_pct_change'] = data['Close'].pct_change().fillna(0) * 100
    # print(data)
    return data[['Date', 'Close', 'High', 'Low', 'Open', 'Volume', 'symbol', 'close_change', 'close_pct_change']]

def ingest_yfinance_data(symbol_data, final_table, interval, filename):
    values = []
    for symbol in symbol_data:
        try:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            end_date = datetime.now().strftime('%Y-%m-%d')
            data_dict = get_finance_data([symbol], start_date, end_date, interval)
            if symbol in data_dict:
                data = transform_data(data_dict[symbol], symbol)
   
                if not data.empty:
                    values.extend([tuple(x) for x in data.to_numpy()])
        except Exception as e:
            # error_logger.error(f"{symbol} transformation error: {str(e)}")
            if values:
                text_to_csv(values, filename)

def text_to_csv(text, filename ):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        for line in text.strip().split('\n'):
            writer.writerow(line.split(','))

def main(interval):
    filename= "stock_data.csv"
    # interval = '1d'
    symbols = get_sp500_Symbols()
    print(len(symbols))
    ingest_yfinance_data(symbols[:10],interval,filename)

if __name__ == "__main__":
    main('1d')


# To connect python to aws
# First we configure in terminal using access key
# Then connect python to aws(s3) using boto3 library

# Initialize S3 client
s3 = boto3.client('s3')

bucket_name = 'us-stock-price-of-four-companies'

s3.create_bucket(Bucket=bucket_name)
print(f"Bucket '{bucket_name}' created!")

# File to upload
file_name = 'stock_data.csv'

# Upload the file
s3.upload_file(file_name, bucket_name, file_name)
print(f"File '{file_name}' uploaded to bucket '{bucket_name}' successfully!")