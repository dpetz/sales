import regex as re
import datetime
from tqdm import tqdm
from pathlib import Path
import mmap

sample = """
Invoice/Item Number,Date,Store Number,Store Name,Address,City,Zip Code,Store Location,County Number,County,Category,Category Name,Vendor Number,Vendor Name,Item Number,Item Description,Pack,Bottle Volume (ml),State Bottle Cost,State Bottle Retail,Bottles Sold,Sale (Dollars),Volume Sold (Liters),Volume Sold (Gallons)
S24127700024,02/19/2015,3678,"Smoke Shop, The",1918 SE 14TH ST,DES MOINES,50320,POINT (-93.597011 41.570844),77,Polk,1031200,VODKA FLAVORED,380,Phillips Beverage Company,41783,Uv Blue Raspberry Vodka Mini,6,500,4.89,7.34,2,14.68,1.00,0.26
S15066200002,10/10/2013,2633,Hy-Vee #3 / BDI / Des Moines,3221 SE 14TH ST,DES MOINES,50320,POINT (-93.596754 41.554101),77,Polk,1082900,MISC. IMPORTED CORDIALS & LIQUEURS,305,MHW Ltd,904969,Sabe Premiom Sake Double Barrel,6,750,14.99,22.49,6,134.94,4.50,1.19
"""


def old():
    with open(src_file) as file:
        for line in file:
            if (row > 0):
                m = p.match(line)
                if m:
                    day = datetime.datetime(int(m.group(3)), int(m.group(1)), int(m.group(2)))
                else:
                    raise Exception("NO DATE in row %i: %s" % (row,line))
                if (row % 500 == 0):
                    print("Row: %i ( %s )" % (row,day))
            row += 1



            

def split_csv_by_day(src_file, trg_folder):

    fsize = Path(src_file).stat().st_size
    tot = 0


    # example line:
    # S24127700024,02/19/2015,3678,"Smoke Shop, The" ..

    p = re.compile(".*(\d{2})/(\d{2})/(\d{4})")

    # https://gist.github.com/zapalote/30aa2d7b432a08e6a7d95e536e672494

    # use a progress bar
    with tqdm(total=fsize, desc=src_file) as pbar:

        with open(src_file, "r+b") as fp:

            # map the entire file into memory, normally much faster than buffered i/o
            mm = mmap.mmap(fp.fileno(), 0)

            for line in iter(mm.readline, b""):
                
                # convert the bytes to a utf-8 string and split the fields
                m = p.match(line.decode("utf-8"))
                if m:
                    day = datetime.datetime(int(m.group(3)), int(m.group(1)), int(m.group(2)))
                else:
                     print("No date in", line)
                tot += len(line)
                pbar.update(tot - pbar.n)
        mm.close()


# https://docs.python.org/3/library/subprocess.html#subprocess.run
import subprocess
import sys

def partition_lines(fname, line_count, header_lines = 1, prefix="split_"):
subprocess.run(["tail -n +2 file.txt | split -l %i - split_+", "-l"]) 
  
 head -n 1 file.txt > final_..
 cat split_.. >> final_..






if __name__ == '__main__':
    split_csv_by_day(sys.argv[1], "data/liquor_days")
else:
    split_csv_by_day("data/raw/Liquor_Sales.csv", "data/liquor_days")

