import regex as re
import datetime
from tqdm import tqdm
from pathlib import Path
import mmap
import os

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


from collections import defaultdict
            

def split_csv_by_day(csv_file, trg_folder, header_lines=1, encoding="utf-8",
                     day_regex=".*(?P<month>\d{2})/(?P<day>\d{2})/(?P<year>\d{4})"):
    
    """ Appends rows that match the given regex day pattern to day files in given folder.
    Create any missing files and copy header lines (default=1) in this case.
    
    The default regex pattern matche the following dates:
    ```
    # S24127700024,02/19/2015,3678,"Smoke Shop, The" ..
    ```
    Custom regexes must contain capturing groups named 'year', 'month', and `day`.

    Default encoding is utf-8.

    Full file is read into memory to speed up vs. buffered i/o.

    A byte-based progress bar is shown in stdout.

    Script is adopted from
    https://gist.github.com/zapalote/30aa2d7b432a08e6a7d95e536e672494

    """

    bytes_file = Path(csv_file).stat().st_size
    bytes_done = 0
    day_pattern = re.compile(day_regex)
    header = []

    Path(trg_folder).mkdir(parents=True, exist_ok=True)

    day_dict = defaultdict(list)

    tqdm()

    # use a progress bar
    with tqdm(total=bytes_file, desc=csv_file) as pbar:

        with open(csv_file, "r+b") as fp:

            # map the entire file into memory, normally much faster than buffered i/o
            mm = mmap.mmap(fp.fileno(), 0)

            for i, line_bytes in enumerate(iter(mm.readline, b"")):
                
                line = line_bytes.decode(encoding)

                if( i < header_lines):
                    header.append(line)
                    continue

                # convert the bytes to a utf-8 string and split the fields
                m = day_pattern.match(line)
                if m:
                    day = datetime.datetime(
                        int(m.group('year')),
                        int(m.group('month')),
                        int(m.group('day')))
                
                    day_dict[day].append(line)
                else:
                     print("No date in", line[0:20]) 
                
                bytes_done += len(line)
                pbar.update(bytes_done - pbar.n)
        mm.close()

        files_created = 0
        line_count = 0

        for (day, lines) in day_dict.items():
            date_file = os.path.join(trg_folder, day.strftime('%Y-%m-%d') + ".csv")
            exists = os.path.isfile(date_file)

            with open(date_file, 'a') as day_file:

                if not exists:
                    day_file.writelines(header)
                    files_created += 1

                day_file.writelines(lines)
                line_count += len(lines)

        print("%i lines written to %i new and %i existing files in %s" %
              (line_count, files_created, len(day_dict) - files_created, trg_folder))
        

# https://docs.python.org/3/library/subprocess.html#subprocess.run
import subprocess
import sys

def partition_lines(fname, line_count, header_lines = 1, prefix="split_"):
    subprocess.run(["tail -n +%i %s | split -l %i - %s", "-l" %
                    (header_lines, fname, line_count, prefix)],) 

    raise NotImplementedError()

    # head -n 1 file.txt > final_..
    # cat split_.. >> final_..


if __name__ == '__main__':
    split_csv_by_day(sys.argv[1], "data/liquor_days")
else:
    split_csv_by_day("data/raw/Liquor_Sales_10K.csv", "data/liquor_days")

