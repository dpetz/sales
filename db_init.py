import sqlite3
import sys

# Out of scope: UPDATE (all tables are append-only)


DB_DEFAULT_PATH = "data/db/%s.sqlite"


def init_store_table(con):
    """
    Stores  change name or address occasionally (max 5)
    County Number is unique per County
    City and Zip Code closely relate (~10 overlap both ways)
    Zip Code has occasional string typos which will lead to NaNs
    Store Location has some NAs
    """

    con.execute("DROP TABLE IF EXISTS Store")

    con.execute("""
        CREATE TABLE IF NOT EXISTS Store (
            Id INTEGER NOT NULL,
            Name TEXT NOT NULL,
            Address TEXT NOT NULL,
            City TEXT NOT NULL,
            Zip_Code INTEGER,
            Location TEXT,
            County_Number INTEGER,
            County TEXT NOT NULL,
            Created INTEGER UNIQUE NOT NULL
        )"""
    )

def init_item_table(con):
    """
    hash is an md5 of the other fields (except 'Created')
    all fields mostly stable (max. 6 changes per Item_Number)
    Category has ~0.1% missing (~17K of ~20M)
    """

    con.execute("DROP TABLE IF EXISTS Item")

    con.execute("""
        CREATE TABLE IF NOT EXISTS Item (
            Hash INTEGER PRIMARY KEY NOT NULL,
            Category INTEGER,
            Category_Name TEXT NOT NULL,
            Item_Number INTEGER NOT NULL,
            Vendor_Number INTEGER NOT NULL,
            Vendor_Name TEXT NOT NULL,
            Description TEXT NOT NULL,
            Pack INTEGER NOT NULL,
            Bottle_Volume_ml INTEGER NOT NULL,
            Created INTEGER UNIQUE NOT NULL
        )"""
    )

def init_invoice_table(con):
    """
    Vendors resp. Category change name infrequenly (max. 4 resp. 2)
    Volume_Sold_Gallons = Volume_Sold_Liters 3.8
    Profit almost always positive with mean ~$5 (51 exceptions in ~20M)
    Sales_USD = Bottles_Sold * Bottle_Retail_USC * 100
    Liters sold can be calculated by looking up bottle volume in Item table
    Missing values extremly rare (10 in ~20M)
    Dates are Days in UNIX timestamp (seconds since 2970-01-01) --> datetime(Date,'unixepoch') in SQLite
    """

    con.execute("DROP TABLE IF EXISTS Invoice")

  
    con.execute("""
        CREATE TABLE IF NOT EXISTS Invoice (
            Id TEXT PRIMARY KEY NOT NULL,
            Date INTEGER NOT NULL,
            Shop INTEGER NOT NULL,
            Item INTEGER NOT NULL,
            Bottle_Cost_USC INTEGER,
            Bottle_Retail_USC INTEGER,
            Bottles_Sold INTEGER NOT NULL,
            Sale_USD REAL,
            Volume_Sold_Liters REAL NOT NULL,
            Volume_Sold_Gallons REAL NOT NULL,
            Created INTEGER UNIQUE NOT NULL,
            FOREIGN KEY(Shop) REFERENCES Store(Number),
            FOREIGN KEY(Item) REFERENCES Item(Id)
        )"""
    )


def init_all_tables(db_path=None, db_name=None):

    assert db_path or db_name

    if not db_path:
        db_path = DB_DEFAULT_PATH % db_name
             
    with sqlite3.connect(db_path) as con:        
        init_store_table(con)
        init_item_table(con)
        init_invoice_table(con)

    print("initialized 3 tables:", db_path)

if (__name__ == '__main__'):
    assert sys.argv[1]
    init_all_tables(sys.argv[1])