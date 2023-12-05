import pandas as pd
import data_aggregation_tools as da

def format_money(value):
    if abs(value) >= 1e6:
        return '${:.2f}M'.format(value / 1e6)
    elif abs(value) >= 1e3:
        return '${:.2f}K'.format(value / 1e3)
    else:
        return '${:.2f}'.format(value)

def read_txs():
    dtypes = { 'UAH': 'float64', 'Category': 'str' }
    df = pd.read_csv('data/donations.csv', dtype=dtypes, parse_dates=['Date'])
    ds = pd.read_csv('data/spending.csv', dtype=dtypes, parse_dates=['Date'])
    return df, ds

def read_data(nrows = None):
    if nrows == None:
            df = pd.read_csv('./data/DIGNITAS UKRAINE INC_Transactions for Dashboard.csv',
                         index_col=None,
                         usecols = ['Account name', 'Date', 'Transaction type', 'Name', 'Amount line'],
                         dtype={ 'Transaction type': 'category', 'Name': 'string', 'Account name': 'string'}, parse_dates=['Date'])
    else:
            return None

    df['Amount line'] = df['Amount line'].replace({'\$': '', ',': ''}, regex=True).astype(float)
    df = df[df['Transaction type'] != 'Transfer']
    df = df[df['Account name'] != 'FIVE % FOR ADMIN EXPENSES']

    # in-kind donations
    df = df[df['Account name'] != 'IN-KIND DONATION DISTRIBUTIONS']
    df_inkind = df[df['Account name'] == 'In-kind donations']
    df = df[df['Account name'] != 'In-kind donations']

    # investments
    df_inv = df[df['Account name'] == 'DIVIDENT RECEIVED FIDELITY']

    df = df[df['Account name'] != 'DIVIDENT RECEIVED FIDELITY']
    df = df[df['Account name'] != 'FIDELITY INVESTMENTS']
    df = df[df['Account name'] != 'In-kind donations']

    # spending
    ds = df[df['Transaction type'] != 'Deposit']
    ds = ds[ds['Transaction type'] != 'Journal Entry']
    ds = ds[ds['Account name'] != 'Donations directed by individuals']
    ds = ds[ds['Account name'] != 'CHASE ENDING IN 5315']
    ds = ds[ds['Account name'] != 'PayPal Bank 3']
    ds = ds[ds['Account name'] != 'DIVIDENT RECEIVED FIDELITY']

    # donations
    df = df[df['Account name'] == 'Donations directed by individuals']
    df = df[df['Transaction type'] == 'Deposit']

    # Define the mapping of old column names to new column names
    column_mapping = {'Name': 'Category', 'Amount line': 'UAH'}
    # Use the mapping to rename the columns
    df.rename(columns=column_mapping, inplace=True)
    df_inkind.rename(columns=column_mapping, inplace=True)

    column_mapping = {'Account name': 'Category', 'Amount line': 'UAH'}
    ds.rename(columns=column_mapping, inplace=True)
    ds['Category'] = ds['Category'].str.lower()
    return df, ds

def extract_relevant_txs(df, ds, start_date, end_date):
    """Main category mapping module"""
    if (start_date != None) | (end_date != None):
        df = df[df['Date'] >= start_date]
        ds = ds[ds['Date'] >= start_date]
        df = df[df['Date'] <= end_date]
        ds = ds[ds['Date'] <= end_date]

    value_mapping = {'02 GENERAL BANK URESTR': 'General donations', 'PAY PALL UNRESTRICTED': 'General donations',
                '03 FUNDRASING EVENTS UNRESTR': 'General donations', '04 PAYPAL GENERAL UNRESTR': 'General donations',
                '05 VENMO UNRESTR': 'General donations', '06 SQUARE UNRESTR': 'General donations',
                '07 1000 DRONES RESTR': '1000 Drones for Ukraine', '09 MSU RESTR': 'Mobile Shower Units',
                '11 VETERANIUS RESTR' : 'Veteranius', '08 VICTORY DRONES RESTR': 'Victory Drones', '10 Flight to Recovery RESTR': 'Flight to Recovery'}
    df['Category'] = df['Category'].replace(value_mapping)
    ds['Category'] = ds['Category'].str.replace('software & apps', 'Admin')
    ds['UAH'] = ds['UAH'].abs()

    column_mapping = {'Name': 'Category', 'Amount line': 'UAH'}
    df.rename(columns=column_mapping, inplace=True)
    df = df[['Date', 'Category', 'UAH']]
    column_mapping = {'Account name': 'Category', 'Amount line': 'UAH'}
    ds.rename(columns=column_mapping, inplace=True)
    ds = ds[['Date', 'Category', 'UAH']]
    return df, ds