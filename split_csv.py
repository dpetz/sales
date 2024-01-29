# https://docs.python.org/3/library/subprocess.html#subprocess.run
import subprocess
import regex as re
import datetime
from tqdm import tqdm
from pathlib import Path
import mmap
import os
import sys
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

    for (day, lines) in tqdm(day_dict.items(), desc=trg_folder):
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
        

def partition_lines(fname, line_count, header_lines = 1, prefix="split_"):
    subprocess.run(["tail -n +%i %s | split -l %i - %s", "-l" %
                    (header_lines, fname, line_count, prefix)],) 

    raise NotImplementedError()

    # head -n 1 file.txt > final_..
    # cat split_.. >> final_..


if __name__ == '__main__':
    split_csv_by_day(sys.argv[1], "data/liquor_days")
else:
    split_csv_by_day("data/raw/Liquor_Sales_100K.csv", "data/liquor_days")

# wc -l data/liquor_days/*