import sqlite3

db_path = "data/db/liquor.sqlite"
con = sqlite3.connect(db_path) #":memory:"

con.execute("DROP TABLE IF EXISTS Store")

# Stores  change name or address occasionally (max 5)
# County Number is unique per County
# City and Zip Code closely relate (~10 overlap both ways)
con.execute("""
    CREATE TABLE IF NOT EXISTS Store (
        Number INTEGER,
        Name TEXT,
        Address TEXT,
        City TEXT,
        Zip_Code INTEGER,
        County TEXT,
        County_Number INTEGER,
        Valid_From INTEGER,
        PRIMARY KEY (Number, Valid_From)
    )"""
)

con.execute("DROP TABLE IF EXISTS Item")

# hash is an md5 of the 4 other fields

con.execute("""
    CREATE TABLE IF NOT EXISTS Item (
        Hash_Id INTEGER PRIMARY KEY,
        Number INTEGER,
        Description TEXT,
        Pack INTEGER,
        Bottle_Volume_ml INTEGER
    )"""
)

con.execute("DROP TABLE IF EXISTS Invoice")

# Vendors change name infrequenly (max. 4)
# A gallon has approx. 2.8 liters
# Profit almost always positive with mean ~$5 (51 exceptions in ~20M records)
# Sales_USD = Bottles_Sold * Bottle_Retail_USC * 100
# Liters sold can be calculated by looking up bottle volume in Item table

con.execute("""
    CREATE TABLE IF NOT EXISTS Invoice (
        Id TEXT PRIMARY KEY,
        Date INTEGER,
        Store_Number INTEGER,
        Vendor_Number INTEGER,
        Vendor_Name TEXT,
        Item_Hash INTEGER,
        Bottle_Cost_USC INTEGER,
        Bottle_Retail_USC INTEGER,
        Bottles_Sold INTEGER,
        Sale_USD REAL,
        Volume_Sold_Liters REAL,
        Volume_Sold_Gallons REAL,
        FOREIGN KEY(Store_Number) REFERENCES Store(Number)
    )"""
)