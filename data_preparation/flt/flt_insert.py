# Import Modules
import glob
import psycopg2
import gzip
import sys
import pandas as pd
import numpy as np
import json
import re
import gzip
import io
import time
import os

# Input
thesis_path = r"C:\Users\cvaka\OneDrive\Master\Thesis"

chunk_size = 50000
table = 'flt'
flt_path = thesis_path + r"\\Data\\" + r"flt\\"

# Connect to PostgreSQL
conn = psycopg2.connect( \
    database="thesis",
    user = "postgres", 
    password = "jonp8UMs8qDV4jEcwOC0",
    host = "localhost"
    )
cur = conn.cursor()

# Read docs file
with open(flt_path + "docs\\" + "docs.json", 'r') as f:
    flt_docs = json.load(f)

# Set name of run 
run = str(int(time.time()))

# Identify paths of flt files
paths = glob.glob(flt_path + "\\*\\*.flt.gz")
complete = []
# for path in glob.glob("flt_insert_completed/*.json"):
# # for path in glob.glob("SQL/FLT/flt_insert_completed/*.json"):
#     with open(path, 'r') as f:
#         complete += json.load(f)
# paths = [x for x in paths if x not in complete]
# paths = paths[12:16]

# Read icao file and select LandPlanes
icao = pd.read_csv(thesis_path + r"\\Data\\" + r"icao_doc_8643.csv", index_col=False)
icao_land_plane = icao[icao['Description']=='LandPlane']['Type Designator']

# If parallel select relevant paths and adjust run name
if len(sys.argv) >=3:
    paths = np.array_split(paths, int(sys.argv[1]))[int(sys.argv[2])].tolist()
    run += "_{}".format(sys.argv[2])

# for all paths to be completed
for path in paths:

    print("{} start reading {}".format(int(time.time()), path))

    # Read
    with io.TextIOWrapper(gzip.open(path, "r")) as f:
        flt = f.readlines()
    
    # Clean a little
    n_removed = 0
    for j in range(len(flt)):
        line = flt[j-n_removed]
        if line.count(";") < len(flt_docs["names"])-1:
        # if line.count(";") <= 30:
            flt.pop(j-n_removed)
            n_removed += 1
        else:
            flt[j-n_removed] = line.replace(" ", "")
    
    # Turn into csv
    flt = io.StringIO('\n'.join(flt))
    
    # Read csv
    flt = pd.read_csv(
        flt, 
        sep=';', 
        names = flt_docs["names"],
        usecols = flt_docs["usecols"],
        dtype = flt_docs["dtype"],
        # compression = "gzip",
        index_col= False
        )
    
    print("{} read, transforming".format(int(time.time())))

    # Keep last entry of a thread of instantanous changes 
    # (one change leads to many changes and corresponding entries)
    # Only keep the last entry containing all the changes
    flt.sort_values('t', inplace=True)
    flt["thread"] = np.where(flt.groupby(["sfplid", "acid"])["t"].diff().fillna(0)>2, 1, 0)
    flt["thread"] = flt.groupby(["sfplid", "acid"])["thread"].cumsum()
    flt.drop_duplicates(subset=["sfplid", "acid", "thread"], keep="last", inplace=True)
    flt.drop("thread", axis=1, inplace=True)

    # Convert timestamps to timestamps
    flt[flt_docs['timestamp']] = flt[flt_docs['timestamp']].astype('Int64')
    for timestamp_col in flt_docs['timestamp']: 
        flt[timestamp_col] = flt[timestamp_col].where(flt[timestamp_col]>=365*24*60*60)
    # flt[flt_docs['timestamp']] =  flt[flt_docs['timestamp']].where(flt[flt_docs['timestamp']] >= 365*24*60*60) # Does not work ¯\_(*.*)_/¯

    print("{} transformed, calculating".format(int(time.time())))

    # Add attributes: landplane, civil registration or gatenumber, local
    flt['landplane'] = flt['actype'].isin(icao_land_plane)
    flt['civ_reg_gnr'] = (flt['acid']==flt['reg']) | ("N"+flt['acid']==flt['reg']) | \
        (flt['arrGnr'].str.contains("A[1-2]\d|GD|GL|HG.*").fillna(False)) | \
        (flt['depGnr'].str.contains("A[1-2]\d|GD|GL|HG.*").fillna(False))
    flt['local'] = ~np.logical_xor(flt['dest']=="EHAM", flt['adep']=="EHAM")

    # Add attribute: best available off block time
    flt['obt_predict'] = \
        (flt['tobt']).fillna(0) + \
            ((flt['tobt'].isna())).astype(int) * (flt['eobt']).fillna(0) + \
                ((flt['tobt'].isna()) & (flt['eobt'].isna())).astype(int) * (flt['sobt'])

    # Add attribute: best available arrival time
    flt['at_predict'] = \
        (flt['slot']).fillna(0) + \
            (flt['slot'].isna()).astype(int) * (flt['eta'])

    flt_columns = list(flt.columns)

    print("{} calculated, start inserting".format(int(time.time())))
    t0 = time.time()

    # Insert using multiple chuncks
    j = 0
    while j < len(flt):

        # Prepare Chunk
        flt_chunk = flt.iloc[j:min(j+chunk_size, len(flt))].copy()
        flt_chunk[flt_docs['timestamp'] + ['obt_predict', 'at_predict']] = \
            'to_timestamp(' + flt_chunk[flt_docs['timestamp'] + ['obt_predict', 'at_predict']].astype(str) + ')'
        flt_chunk = [tuple(x) for x in flt_chunk.to_numpy()]
        
        # Insert Chunk using query
        query = re.sub(
            r"'(to_timestamp\(\d+\))'", # remove ' 
            r"\1", 
            "INSERT INTO {}({}) VALUES {}"
                .format(table, ','.join(flt_columns), str(flt_chunk)[1:-1])
                .replace("'to_timestamp(<NA>)'", 'Null')
                .replace("nan", "Null")
                .replace("<NA>", "Null"))
        
        cur.execute(query)

        j += chunk_size

        pct = int(min(j, len(flt))/len(flt)*100)
        sys.stdout.write('\r')
        sys.stdout.write("\t[%-20s] %d%%" % ('='*int(pct/5), pct))
        sys.stdout.flush()
    print()

    # Add file to completed
    complete.append(path)
    # with open("SQL/FLT/flt_insert_completed/" + run + '.json', 'w') as f:
    with open("flt_insert_completed/" + run + '.TMP', 'w') as f:
        json.dump(complete, f)

    conn.commit()


# paths_tmp_result = glob.glob("flt_insert_completed/*.TMP")
# if paths_tmp_result == n_results:
#     results = {}
#     for path in paths_tmp_result:
#         with open(path, 'r') as f:
#             results.update(json.load(f))
#             os.remove(path)
#     with open("flt_insert_completed/" + run + '.json', 'w') as f:
#         json.dump(results, f)
