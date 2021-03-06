# Import Modules
import glob
import psycopg2
import gzip
import sys
import json
import time
import numpy as np

# Input
thesis_path = r"C:\Users\cvaka\OneDrive\Master\Thesis"
max_entries = 100000
table = 'astra'

# Connect to db
conn = psycopg2.connect( \
    database="thesis",
    user = "postgres",
    password = "jonp8UMs8qDV4jEcwOC0",
    host = "localhost"
    )
cur = conn.cursor()

# Name of run using the time
run = str(int(time.time()))

# Read astra file paths, read list of completed file paths, keep not completed
paths = glob.glob(thesis_path + r"\Data\\" + "astra\\" + "*.ast.gz")
complete = []
for path in glob.glob("astra_insert_completed/*.json"):
    with open(path, 'r') as f:
        complete += json.load(f)
paths = [x for x in paths if x not in complete]

# If paralel grab relevant subset of paths to insert, add paralel name to run
if len(sys.argv) >=3:
    paths = np.array_split(paths, int(sys.argv[1]))[int(sys.argv[2])].tolist()
    run += "_" + str(int(sys.argv[1])) + "_" + str(int(sys.argv[2]))

# For every path
for i, path in enumerate([x for x in paths if x not in complete]): # Find all ast.gz files
    print(path)

    # Read file
    with gzip.GzipFile(path, "r") as f: # Open decode and split into lines
        lines = f.read().decode('ascii').strip('\n').split('\n')
    
    # Split file and transform datetime for PostgreSQL insertion
    for j, line in enumerate(lines): # Split lines into elments, timestamp column 0, sting column 8, make csv line
        line = line.split(';')
        line[0], line[8] = 'to_timestamp(' + line[0] + ')', "'" + line[8] + "'"
        lines[j] = ','.join(line)

    # Insert file in batches
    j = 0
    while j < len(lines): # Upload entries
        out = lines[j:min(j+max_entries, len(lines))]
        query = """
        INSERT INTO {} (t, trk, ssr, x, y, fl, gs, heading, f_id, "mode-s") VALUES
        ({})
        ON CONFLICT ON CONSTRAINT {}_pkey DO NOTHING
        ;""".format(table, "),\n(".join(out), table)
        cur.execute(query)
        j += len(out)

        pct = int(j/len(lines)*100)
        sys.stdout.write('\r')
        sys.stdout.write("\t[%-20s] %d%%" % ('='*int(pct/5), pct))
        sys.stdout.flush()    

    # Add completed path to completed paths
    complete.append(path)
    with open("SQL/FLT/flt_insert_completed/" + run + '.json', 'w') as f:
        json.dump(complete, f)
    conn.commit()
    print()
