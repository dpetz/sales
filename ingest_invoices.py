
import sqlite3
from datetime import datetime
import pickle
# https://docs.python.org/3/library/hashlib.html
import hashlib # hashlib.algorithms_available

# md5 is available on every platform and is 128-bit short
# see https://en.wikipedia.org/wiki/List_of_hash_functions

import pandas as pd
from sales_util import read_liquor_csv
import ctypes
import math
import sys

DB_PATH = "data/db/liquor.sqlite"

STORE_SPECIFICS = "Name, Address, City, Zip_Code, Location, County_Number, County"

SQL_LATEST_STORE_BY_NUMBER = \
    "SELECT %s FROM Store WHERE Number = ? ORDER BY Created DESC LIMIT 1" % STORE_SPECIFICS

ITEM_ID_QUERY = "SELECT Id FROM Item WHERE Id = ? LIMIT 1"

STORE_BY_NUMBER = "SELECT Id FROM Item WHERE Id = ? LIMIT 1"

SQL_INSERT_STORE = """INSERT INTO Store(
    Number, %s, Created)
    VALUES(?,?,?,?,?,?,?,?,?)""" % STORE_SPECIFICS

SQL_INSERT_ITEM = """INSERT INTO Item(
    Category, Category_Name, Item_Number,
    Vendor_Number, Vendor_Name, Description,
    Pack, Bottle_Volume_ml, Id, Created)
    VALUES(?,?,?,?,?,?,?,?,?,?)"""

SQL_INSERT_INVOICE = """INSERT INTO Invoice(
    Id, Date, Shop, Item, Bottle_Cost_USC,
    Bottle_Retail_USC, Bottles_Sold, Sale_USD,
    Volume_Sold_Liters, Volume_Sold_Gallons, Created)
    VALUES(?,?,?,?,?,?,?,?,?,?,?)"""


def to_nano(dt):
    return int(dt.timestamp() * 10**9)


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
    return to_nano(datetime.combine(assumed_day, datetime.now().time()))


def item_id_exists(id, con):
    cur = con.execute(ITEM_ID_QUERY,(id,)).fetchone()
    return (cur != None)


def store_specifics_match(data, con):
    latest_data = con.execute(SQL_LATEST_STORE_BY_NUMBER, (data[0],)).fetchone()
    #print("DB:", latest_data)
    #print("MEM", tuple(data[1:-1]))
    return latest_data == tuple(data[1:-1])


def int_or_None(obj):
   if not obj:
        return None
   if  obj == math.nan:
        return None
   else:
        try:
            return int(obj)
        except:
            return None


def process_invoice(row, con):

    # Item TABLE

    data = [row.Category, row.Category_Name, row.Item_Number,
            row.Vendor_Number, row.Vendor_Name, row.Item_Description,
            row.Pack, row.Bottle_Volume_ml]
    
    item_hash = item_id_hash(data) 

    if not item_id_exists(item_hash, con):
        data.append(item_hash)
        data.append(synthetic_timestamp(row.Date))
        con.execute(SQL_INSERT_ITEM, data)

    # Store TABLE

    data = [int(row.Store_Number), row.Store_Name, row.Address,
            row.City, int_or_None(row.Zip_Code), row.Store_Location,
            int_or_None(row.County_Number), row.County, synthetic_timestamp(row.Date)]

    if not store_specifics_match(data, con):
        con.execute(SQL_INSERT_STORE, data)

    # Invoice TABLE

    data = [row.Invoice_Item_Number, int(row.Date.timestamp()), row.Store_Number, item_hash,
            int_or_None(row.State_Bottle_Cost * 100), int_or_None(row.State_Bottle_Retail * 100),
            row.Bottles_Sold, row.Sale_Dollars, row.Volume_Sold_Liters,
            row.Volume_Sold_Gallons, synthetic_timestamp(row.Date)]
    
    con.execute(SQL_INSERT_INVOICE, data)


def clear_table(table, con):
    print(con.execute("DELETE FROM %s" % table).rowcount, "row(s) deleted from:", table)

def ingest_batch(batch_id,limit=sys.maxsize,clear_tables=None):

    df = read_liquor_csv(batch_id)
    with sqlite3.connect(DB_PATH) as con:

        if(clear_tables):
            for t in clear_tables:
                clear_table(t, con)

        for row in df.itertuples():
            if limit <= 0:
                break
            process_invoice(row, con)
            limit -= 1

ingest_batch( 'Liquor_Sales', clear_tables=('Invoice', 'Store', 'Item'))