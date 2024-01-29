import numpy as np
import pandas as pd
import pandas_categorical as pdc
import os


# optimal column types
LIQUOR_PYTHON_NAMES = {
    'Invoice/Item Number': "Invoice_Item_Number",
    'Date': 'Date',
    'Store Number': "Store_Number",
    'Store Name': "Store_Name",
    'Address': 'Address',
    'City': "City",
    'Zip Code': 'Zip_Code',
    'Store Location': 'Store_Location',
    'County Number': "County_Number",
    'County': "County",
    'Category': "Category",
    'Category Name': 'Category_Name',
    'Vendor Number': "Vendor_Number",
    'Vendor Name': 'Vendor_Name',
    'Item Number': "Item_Number",
    'Item Description': "Item_Description",
    'Pack': "Pack",
    'Bottle Volume (ml)': "Bottle_Volume_ml",
    'State Bottle Cost': 'State_Bottle_Cost',
    'State Bottle Retail': 'State_Bottle_Retail',
    'Bottles Sold': 'Bottles_Sold',
    'Sale (Dollars)': 'Sale_Dollars',
    'Volume Sold (Liters)': 'Volume_Sold_Liters',
    'Volume Sold (Gallons)': 'Volume_Sold_Gallons'
}
    


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
    """Read csv file, cast to optimal types, and rename fields"""
    
    if not os.path.isfile(file_name):
        os.path.join(RAW_DATA_FOLDER, file_name + ".csv")

    df = pd.read_csv(
        file_name,
        engine='pyarrow',
        dtype=LIQUOR_DTYPES)

    pdc.cat_astype(
        df,
        cat_cols=LIQUOR_CAT_COLS,
        sub_dtypes=LIQUOR_DTYPES,
        ordered_cols=LIQUOR_ORDERED_COLS)
    
    return df.rename(columns=LIQUOR_PYTHON_NAMES, copy=False)


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