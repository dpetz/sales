import sqlite3

DB_PATH = "data/db/liquor.sqlite"


def create_indices():
    with sqlite3.connect(DB_PATH) as con:
        con.execute("CREATE UNIQUE INDEX Store_Create on Store(Create)")
        con.execute("CREATE UNIQUE INDEX Item_Create on Item(Create)")
        #con.execute("CREATE UNIQUE INDEX Invoice_Create on Invoice(Create)")


def log_row (table, row, idxs):
    print(row[-1], "{:<7}".format(table) ,
          ' '.join([ '@'+i+' '+str(f) for (i,f) in enumerate(row[:-1])]))


def iterate_by_created(con, table):
    yield con.execute("SELECT * from %s ORDERED BY Created" % table)


def log_tables(*tables):

    with sqlite3.connect(DB_PATH) as con:
        
        generators = [iterate_by_created(t) for t in tables]
        rows = [next(generators[t]) for t in tables]
        created = [rows[t][-1] for t in tables] # assumes Created is last column
        indices_to_log = [range(len(r)-1) for r in rows]

        while True:
            idx_min = created.index(min(created))
            log_row(tables[idx_min], rows[idx_min], indices_to_log[idx_min])
            try:
                rows[idx_min] = next(generators[idx_min])
                created[idx_min] = rows[idx_min][-1]
            except:
                break


log_tables('Store', 'Item')

