import sqlite3
import random



def log_row (table, row, idxs):
    #print (3*'\n', row, 3*'\n')
    return "%i %s %s" % ( \
        row[-1],
        "{:<7}".format(table) ,
         ' '.join([ "@%i %s" % (i+1,f) for (i,f) in enumerate(row[:-1])])
    )


def iterate_by_created(con, table):
    return con.execute("SELECT * from %s ORDER BY Created" % table)
    

def log_tables(*tables, db_path="data/db/days.sqlite", sample=1., seed=None,log_file="data/log/test.log"):

    random.seed(seed)

    with sqlite3.connect(db_path) as con:

        
        generators = [iterate_by_created(con,t) for t in tables]
        next_rows = [next(g) for g in generators]
        created = [r[-1] for r in next_rows] # assumes Created is last column
        indices_to_log = [range(len(r)-1) for r in next_rows]

        logged = 0

        with open(log_file, 'wt') as lfile:

            while True:
                idx_min = created.index(min(created))
                if (random.random() < sample):
                    lfile.write(log_row(tables[idx_min], next_rows[idx_min], indices_to_log[idx_min]) + '\n')
                    logged += 1
                try:
                    next_rows[idx_min] = next(generators[idx_min])
                    created[idx_min] = next_rows[idx_min][-1]
                except StopIteration:
                    break

        print("%i row logged to %s." % (logged, log_file))
        
def test():
    with sqlite3.connect("data/db/days.sqlite") as con:
        gen = iterate_by_created(con,'Store')
        print(next(gen))
        print(next(gen))

log_tables('Store', 'Item', 'Invoice', sample=1.)

