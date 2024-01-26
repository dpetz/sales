
import sqlite3
from datetime import datetime
import pickle
# https://docs.python.org/3/library/hashlib.html
import hashlib # hashlib.algorithms_available

# md5 is available on every platform and is 128-bit short
# see https://en.wikipedia.org/wiki/List_of_hash_functions

import pandas as pd
from sales_util import read_liquor_csv

df = read_liquor_csv('day_1')




DB_PATH = "data/db/liquor.sqlite"

SQL_INSERT_ITEM = """INSERT INTO Item(
    Category, Category_Name, Item_Number,
    Vendor_Number, Vendor_Name, Description,
    Pack, Bottle_Volume_ml, Id, Created)
    VALUES(?,?,?,?,?,?,?,?,?,?)"""

SQL_INSERT_STORE = """INSERT INTO Store(
    Number, Name, Address,
    City, Zip_Code, County,
    County_Number, Created)
    VALUES(?,?,?,?,?,?,?,?,?,?)"""



import ctypes

def toSigned32(n):
    # n = n & 0xffffffff
    # return n | (-(n & 0x80000000))
    return ctypes.c_long(n).value


def item_id_hash(data):
    hash = hashlib.md5()
    for d in data:
        hash.update(pickle.dumps(d))
    hash.digest()

    return int(hash.hexdigest(), 16) % 2 ** 32

def synthetic_timestamp(assumed_day):
    return int(datetime.combine(assumed_day, datetime.now().time()).timestamp())

ITEM_ID_QUERY = "SELECT Id FROM Item WHERE Id = ?"
STORE_BY_NUMBER = "SELECT Id FROM Item WHERE Id = ? LIMIT 1"

def item_id_exists(id, con):
    return (con.execute(ITEM_ID_QUERY,(id,)).fetchone() is not None)

def process_invoice(row, cur):

    # Item TABLE

    data = [row.Category, row.Category_Name, row.Item_Number,
            row.Vendor_Number, row.Vendor_Name, row.Item_Description,
            row.Pack, row.Bottle_Volume_ml]
    
    hash = item_id_hash(data) 

    if not item_id_exists(hash, con):
        data.append(hash)
        data.append(synthetic_timestamp(row.Date))
        con.execute(SQL_INSERT_ITEM, data)

    # Store TABLE

    data = [row.Store_Number, row.Store_Name, row.Address,
            row.City, row.Zip_Code, row.Store_Location,
            row.County_Number, row.County]



def clear_table(table, con):
    print(con.execute("DELETE FROM %s" % table).rowcount, "row(s) deleted from", table)

with sqlite3.connect(DB_PATH) as con:

    clear_table('Item', con)

    i = 0

    for row in df.itertuples():
        process_invoice(row, con)
        i += 1
        if (i > 99):
            break




COLS = ['Invoice_Item_Number', 'Date', 'Store_Number', 'Store_Name', 'Address',
       'City', 'Zip_Code', 'Store_Location', 'County_Number', 'County',
       'Category', 'Category_Name', 'Vendor_Number', 'Vendor_Name',
       'Item_Number', 'Item_Description', 'Pack', 'Bottle_Volume_ml',
       'State_Bottle_Cost', 'State_Bottle_Retail', 'Bottles_Sold',
       'Sale_Dollars', 'Volume_Sold_Liters', 'Volume_Sold_Gallons']
