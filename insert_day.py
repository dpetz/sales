
import sqlite3

# https://docs.python.org/3/library/hashlib.html
import hashlib # hashlib.algorithms_available

# md5 is available on every platform and is 128-bit short
# see https://en.wikipedia.org/wiki/List_of_hash_functions

db_path = "data/db/liquor.sqlite"
con = sqlite3.connect(db_path) #":memory:"
cur = con.cursor()
