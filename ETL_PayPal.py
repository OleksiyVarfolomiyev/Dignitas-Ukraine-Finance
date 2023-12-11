#import boto3
#import json
import pandas as pd

# def etl():
#     #df = read_PayPal_txs()
#     df = read_data()
#     #df['Fee'].fillna(0, inplace=True)

#     df['Date'] = pd.to_datetime(df['Date'])
#     #df['Gross'] = df['Gross'].astype(float)
#     #df['Gross'] = df['Gross'].abs()
#     #df['Fee'] = df['Fee'].astype(float)

#     #df['Amount'] = df['Gross'] + df['Fee']
#     #df = df.drop(['Gross', 'Fee'], axis=1)

#     mask = df['Category'].str.contains('General', case=False, na=False)
#     df.loc[mask, 'Category'] = 'General'
#     mask = df['Category'].str.contains('1000', case=False, na=False)
#     df.loc[mask, 'Category'] = '1000 Drones for Ukraine'
#     mask = df['Category'].str.contains('support ukraine', case=False, na=False)
#     df.loc[mask, 'Category'] = 'General'
#     mask = df['Category'].str.contains('victory', case=False, na=False)
#     df.loc[mask, 'Category'] = 'Victory Drones'
#     mask = df['Category'].str.contains('flight', case=False, na=False)
#     df.loc[mask, 'Category'] = 'Flight to Recovery'
#     mask = df['Category'].str.contains('custom', case=False, na=False)
#     df.loc[mask, 'Category'] = 'General'
#     mask = df['Category'].str.contains('units', case=False, na=False)
#     df.loc[mask, 'Category'] = 'Mobile Shower Laundry Units'
#     mask = df['Category'].str.contains('shower', case=False, na=False)
#     df.loc[mask, 'Category'] = 'Mobile Shower Laundry Units'
#     df['Category'].unique()

#     df.to_csv('data/PayPal.csv', index=False)

def read_data():
    dtypes = {
        'Currency': 'str',
        'Category': 'str',
        'Country': 'str',
        'Amount': 'float'
    }
    df = pd.read_csv('data/PayPal.csv', dtype=dtypes, parse_dates=['Date'])
    df['Date'] = pd.to_datetime(df['Date'])
    df['Category'].fillna('', inplace=True)
    df['Country'].fillna('', inplace=True)

    return df
