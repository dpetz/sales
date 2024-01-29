import sqlite3

# Out of scope: UPDATE (all tables are append-only)

DB_PATH = "data/db/liquor.sqlite"

with sqlite3.connect(DB_PATH) as con:

    con.execute("DROP TABLE IF EXISTS Store")

    # Stores  change name or address occasionally (max 5)
    # County Number is unique per County
    # City and Zip Code closely relate (~10 overlap both ways)
    # Zip Code has occasional string typos which will lead to NaNs
    con.execute("""
        CREATE TABLE IF NOT EXISTS Store (
            Number INTEGER NOT NULL,
            Name TEXT NOT NULL,
            Address TEXT NOT NULL,
            City TEXT NOT NULL,
            Zip_Code INTEGER,
            Location TEXT NOT NULL,
            County_Number INTEGER,
            County TEXT NOT NULL,
            Created UNIQUE INTEGER NOT NULL
        )"""
    )

    con.execute("DROP TABLE IF EXISTS Item")

    # hash is an md5 of the other fields (except 'Created')
    # all fields mostly stable (max. 6 changes per Item_Number)
    # Category has ~0.1% missing (~17K of ~20M)
    con.execute("""
        CREATE TABLE IF NOT EXISTS Item (
            Id INTEGER PRIMARY KEY NOT NULL,
            Category INTEGER,
            Category_Name TEXT NOT NULL,
            Item_Number INTEGER NOT NULL,
            Vendor_Number INTEGER NOT NULL,
            Vendor_Name TEXT NOT NULL,
            Description TEXT NOT NULL,
            Pack INTEGER NOT NULL,
            Bottle_Volume_ml INTEGER NOT NULL,
            Created UNIQUE INTEGER NOT NULL
        )"""
    )

    con.execute("DROP TABLE IF EXISTS Invoice")

    # Vendors resp. Category change name infrequenly (max. 4 resp. 2)
    # Volume_Sold_Gallons = Volume_Sold_Liters 3.8
    # Profit almost always positive with mean ~$5 (51 exceptions in ~20M)
    # Sales_USD = Bottles_Sold * Bottle_Retail_USC * 100
    # Liters sold can be calculated by looking up bottle volume in Item table
    # Missing values extremly rare (10 in ~20M)
    # Dates are Days in UNIX timestamp (seconds since 2970-01-01) --> datetime(Date,'unixepoch') in SQLite
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
            Created UNIQUE INTEGER NOT NULL,
            FOREIGN KEY(Shop) REFERENCES Store(Number),
            FOREIGN KEY(Item) REFERENCES Item(Id)
        )"""
    )