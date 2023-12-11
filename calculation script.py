import boto3
import pandas as pd
from io import StringIO
from statsmodels.tsa.stattools import ccf

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
bucket_name = 'test1314520'
sp500_key = 'csv/stock index/S&P 500 Historical Data.csv'
indictors = ['GDP','CORESTICKM159SFRBATL','WM2NS','UNRATE']
all_correlations = pd.DataFrame()
# get data from s3
sp500_data = pd.read_csv(StringIO(s3.Object(bucket_name, sp500_key).get()['Body'].read().decode('utf-8')))

#Data cleansing
sp500_data = sp500_data[['Date','Price']].dropna()

# Convert date columns to datetime
sp500_data['Date'] = pd.to_datetime(sp500_data['Date'])
sp500_earliest_date = sp500_data['Date'].min()

# Convert the S&P 500 Price column to float
sp500_data['Price'] = sp500_data['Price'].apply(lambda x: float(x.replace(',', '')))

data_dict = {}
for key in indictors:
    #read data
    temp_data = pd.read_csv(StringIO(s3.Object(bucket_name, 'csv/'+key+'.csv').get()['Body'].read().decode('utf-8')))
    #Data cleansing
    temp_data['DATE'] =  pd.to_datetime(temp_data['DATE'])
    filter_data = temp_data[temp_data['DATE'] >= sp500_earliest_date].dropna()
    data_dict[key] = filter_data
    


# Resample S&P 500 data to quarterly
sp500_quarterly = sp500_data.resample('Q', on='Date').mean()


def cal_correlations(sp500_raw,targetData):
    #align the date
    sp500_aligned = sp500_raw['Price'].reindex(targetData.index, method='nearest')
    
    # Calculate the cross-correlation
    correlations_reverse = ccf(sp500_aligned, targetData, adjusted=False)
    correlations = ccf(targetData,sp500_aligned,adjusted=False)
    corr_dict = {lag: correlations[-lag] for lag in range(-5, 0)}
    for lag in range(0,6):
        corr_dict[lag] = correlations_reverse[lag] 
    return corr_dict

def save2s3(cross_corr_dict,name):
    cross_corr_df = pd.DataFrame(list(cross_corr_dict.items()), columns=['Lag', 'Correlation'])
    
    # df2csv
    csv_buffer = StringIO()
    cross_corr_df.to_csv(csv_buffer, index=False)
    
    # upload csv to s3
    csv_key = 'csv/correlation/'+name+'_corr.csv'
    s3_client.put_object(Bucket=bucket_name, Key=csv_key, Body=csv_buffer.getvalue())
    print('success saved:',name)


for name in indictors:
    data_quarterly = data_dict[name].resample('Q', on='DATE').mean()
    cross_corr_dict = cal_correlations(sp500_quarterly,data_quarterly)
    # combine in one df to visulize on QuickSight
    cross_corr_df = pd.DataFrame(list(cross_corr_dict.items()), columns=['Lag', 'Correlation'])
    cross_corr_df['Indicator'] = name
    all_correlations = pd.concat([all_correlations, cross_corr_df], ignore_index=True)
    save2s3(cross_corr_dict,name)

csv_buffer = StringIO()
all_correlations.to_csv(csv_buffer, index=False)
csv_key = 'csv/correlation/all_indicators_corr.csv'
s3_client.put_object(Bucket=bucket_name, Key=csv_key, Body=csv_buffer.getvalue())