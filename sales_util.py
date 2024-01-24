import numpy as np
import pandas as pd
import pandas_categorical as pdc
import os

# optimal column types
LIQUOR_DTYPES = {
    'Invoice/Item Number': "string",
    'Date': 'datetime64[ns]',
    'Store Number': "uint16",
    'Store Name': "string",
    'Address': 'string',
    'City': "string",
    'Zip Code': 'string',
    'Store Location': 'string',
    'County Number': "Int16",
    'County': "string",
    'Category': "Int32",
    'Category Name': 'string',
    'Vendor Number': "Int16",
    'Vendor Name': 'string',
    'Item Number': "string",
    'Item Description': "string",
    'Pack': "uint16",
    'Bottle Volume (ml)': "uint32",
    'State Bottle Cost': float,
    'State Bottle Retail': float,
    'Bottles Sold': int,
    'Sale (Dollars)': float,
    'Volume Sold (Liters)': float,
    'Volume Sold (Gallons)': float
}

LIQUOR_CAT_COLS = [
    'Date',
    'Store Number',
    'Store Name',
    'Address',
    'City',
    'Zip Code',
    'Store Location',
    'County Number',
    'County',
    'Category',
    'Category Name',
    'Vendor Number',
    'Vendor Name',
    'Item Number',
    'Item Description',
    'Pack',
    'Bottle Volume (ml)'
]

LIQUOR_ORDERED_COLS = [
    'Date',
    'Bottle Volume (ml)'
]

RAW_DATA_FOLDER = "data/raw"

def read_liquor_csv(file_name='Liquor_Sales'):
    """Read csv file and cast to optimal types"""
    
    df = pd.read_csv(
        os.path.join(RAW_DATA_FOLDER, file_name + ".csv"),
        engine='pyarrow',
        dtype=LIQUOR_DTYPES)

    pdc.cat_astype(
        df,
        cat_cols=LIQUOR_CAT_COLS,
        sub_dtypes=LIQUOR_DTYPES,
        ordered_cols=LIQUOR_ORDERED_COLS)

    return df


def infer_relation_cardinalities(df, vars=None):
    
    if (vars == None):
        vars = df.select_dtypes(include=['object', 'category']).columns

    cardinalities = {}

    for i in range(len(vars)):
        cards = []
        for j in range(len(vars)):
            if (i == j):
               cards.append(0)
            else: 
                cards.append(df.groupby(vars[i])[vars[j]].nunique().max())
        cardinalities[vars[i]] = cards

    return pd.DataFrame(cardinalities, columns=vars, index=vars).transpose()