
import sqlite3

# https://docs.python.org/3/library/hashlib.html
import hashlib # hashlib.algorithms_available

# md5 is available on every platform and is 128-bit short
# see https://en.wikipedia.org/wiki/List_of_hash_functions


import numpy as np
import pandas as pd
from sales_util import read_liquor_csv

df = read_liquor_csv('day_1')

date = df['Date'][0]

CALS = ['Invoice_Item_Number', 'Date', 'Store_Number', 'Store_Name', 'Address',
       'City', 'Zip_Code', 'Store_Location', 'County_Number', 'County',
       'Category', 'Category_Name', 'Vendor_Number', 'Vendor_Name',
       'Item_Number', 'Item_Description', 'Pack', 'Bottle_Volume_ml',
       'State_Bottle_Cost', 'State_Bottle_Retail', 'Bottles_Sold',
       'Sale_Dollars', 'Volume_Sold_Liters', 'Volume_Sold_Gallons']


DB_PATH = "data/db/liquor.sqlite"

SQL_INSERT_ITEM = """INSERT INTO Item(
    Category, Category_Name, Item_Number,
    Vendor_Number, Vendor_Name, Description,
    Pack, Bottle_Volume_ml, Id)
    VALUES(?,?,?,?,?,?,?,?,?)"""

import pickle

def append_item_id_hash(data):
    hash = hashlib.md5()
    for d in data:
        hash.update(pickle.dumps(d))
    hash.digest()

    data.append(hash.hexdigest())

    return data

    

def process_invoice(row):

    data = [row.Category, row.Category_Name, row.Item_Number,
            row.Vendor_Number, row.Vendor_Name, row.Item_Description,
            row.Pack, row.Bottle_Volume_ml]

    cur.execute(SQL_INSERT_ITEM, append_item_id_hash(data))


with sqlite3.connect(DB_PATH) as con:

    cur = con.cursor()

    for row in df.itertuples():
        process_invoice(row)
        break



with sqlite3.connect(DB_PATH) as con:

    cur = con.cursor()
    