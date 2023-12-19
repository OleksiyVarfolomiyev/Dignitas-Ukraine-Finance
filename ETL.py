import pandas as pd
import data_aggregation_tools as da

def format_money(value):
    if abs(value) >= 1e6:
        return '{:.2f}M'.format(value / 1e6)
    elif abs(value) >= 1e3:
        return '{:.2f}K'.format(value / 1e3)
    else:
        return '{:.2f}'.format(value)

def format_money_USD(value):
    if abs(value) >= 1e6:
        return '${:.2f}M'.format(value / 1e6)
    elif abs(value) >= 1e3:
        return '${:.2f}K'.format(value / 1e3)
    else:
        return '${:.2f}'.format(value)

def read_clean_data():
    """read clean data from csv files"""
    dtypes = { 'Category': 'str', 'UAH': 'float', 'Payment method': 'str' }
    df = pd.read_csv('data/donations.csv', dtype=dtypes, parse_dates=['Date'])
    ds = pd.read_csv('data/spending.csv', dtype=dtypes, parse_dates=['Date'])

    df_inkind = pd.read_csv('./data/inkind.csv',  index_col=None, dtype={'UAH': 'float'}, parse_dates=['Date'])
    df_inv = pd.read_csv('./data/investments.csv',  index_col=None, dtype={'UAH': 'float'}, parse_dates=['Date'])

    return df, ds, df_inkind, df_inv

def ETL_raw_data(nrows = None):
    """ETL data from QuickBooks csv file"""
    if nrows == None:
            df = pd.read_csv('./data/DIGNITAS UKRAINE INC_Transactions for Dashboard.csv',
                        index_col=None,
                        usecols = ['Account', 'Date', 'Transaction type', 'Name', 'Amount', 'Description'],
                        dtype={ 'Transaction type': 'category', 'Name': 'string',
                                'Account': 'string', 'Description': 'string'},
                        parse_dates=['Date'])
            print(df.columns)
    else:
        return None

    column_mapping = {'Name': 'Category', 'Amount': 'UAH', 'Description': 'Payment method'}
    df.rename(columns=column_mapping, inplace=True)

    df['UAH'] = df['UAH'].replace({'\$': '', ',': '', '\(': '', '\)': ''}, regex=True).astype(float)
    df = df[df['Transaction type'] != 'Transfer']
    df = df[df['Account'] != 'FIVE % FOR ADMIN EXPENSES']

    # in-kind donations
    df = df[df['Account'] != 'IN-KIND DONATION DISTRIBUTIONS']
    df_inkind = df[df['Account'] == 'In-kind donations']
    df = df[df['Account'] != 'In-kind donations']

    # investments
    df_inv = df[df['Account'] == 'DIVIDENT RECEIVED FIDELITY']

    df = df[df['Account'] != 'DIVIDENT RECEIVED FIDELITY']
    df = df[df['Account'] != 'FIDELITY INVESTMENTS']
    df = df[df['Account'] != 'In-kind donations']

    value_mapping = {'02 GENERAL BANK URESTR': 'General donations', 'PAY PALL UNRESTRICTED': 'General donations',
                '03 FUNDRASING EVENTS UNRESTR': 'General donations', '04 PAYPAL GENERAL UNRESTR': 'General donations',
                '05 VENMO UNRESTR': 'General donations', '06 SQUARE UNRESTR': 'General donations',
                '07 1000 DRONES RESTR': '1000 Drones for Ukraine', '09 MSU RESTR': 'Mobile Shower Units',
                '11 VETERANIUS RESTR' : 'Veteranius', '08 VICTORY DRONES RESTR': 'Victory Drones', '10 Flight to Recovery RESTR': 'Flight to Recovery'}

    df['Category'] = df['Category'].replace(value_mapping)
    df['Category'] = df['Category'].str.title()

    # spending
    ds = df[df['Transaction type'] != 'Deposit']
    ds = ds[ds['Transaction type'] != 'Journal Entry']
    ds = ds[ds['Account'] != 'Donations directed by individuals']
    ds = ds[ds['Account'] != 'CHASE ENDING IN 5315']
    ds = ds[ds['Account'] != 'PayPal Bank 3']
    ds = ds[ds['Account'] != 'DIVIDENT RECEIVED FIDELITY']


    # donations
    df = df[df['Account'] == 'Donations directed by individuals']
    df = df[df['Transaction type'] == 'Deposit']

    df_inkind = df_inkind[['Date', 'UAH']]
    df_inv = df_inv[['Date', 'UAH']]

    payment_methods = [
        'PayPal',
        'Venmo',
        'Zelle',
        'International wire',
        'Domestic wire',
        'Square',
        'Deposit',
        'Book Transfer',
        'Fedwire Credit']

    for method in payment_methods:
        mask = df['Payment method'].str.contains(method, case=False, na=False)
        df.loc[mask, 'Payment method'] = method

    # Set the remaining 'Payment method' values to 'PayPal'
    mask = ~df['Payment method'].isin(payment_methods)
    df.loc[mask, 'Payment method'] = 'PayPal'
    df = df[['Date', 'Category', 'UAH', 'Payment method']]

    #column_mapping = {'Account': 'Category', 'Amount': 'UAH'}
    #ds.rename(columns=column_mapping, inplace=True)
    ds['Category'] = ds['Category'].str.title()
    value_mapping = {'CAR PURCHASES': 'Admin', 'LODGING': 'Admin',
                'TRANSPORTATION AND PARKING': 'Admin', 'EVENTS PARTICIPATION EXPENSES': 'Admin',
                'Legal fees': 'Admin', 'Software & apps': 'Admin',
                'MALING AND DELIVERY': 'Admin', 'Bank fees & service charges': 'Admin', 'Chase Bank': 'Admin',
                '01 Administrative Account': 'Admin', 'Quickbooks Payments': 'Admin',
                'Ngoptics' : 'Drone purchases', 'Collegiate Productions' : 'Supplies and materials'}

    ds['Category'] = ds['Category'].replace(value_mapping)
    ds['Category'] = ds['Category'].str.title()
    ds['UAH'] = ds['UAH'].abs()
    ds = ds[['Date', 'Category', 'UAH']]

    df.to_csv('data/donations.csv', index=False)
    ds.to_csv('data/spending.csv', index=False)
    df_inkind.to_csv('data/inkind.csv', index=False)
    df_inv.to_csv('data/investments.csv', index=False)

    return df, ds, df_inkind, df_inv
