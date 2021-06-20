# Import Modules
import psycopg2
import time
import sys
import glob
import re

timer_start = time.time()

# connect to postgresql
conn = psycopg2.connect( \
    database="thesis",
    user = "postgres",
    password = "jonp8UMs8qDV4jEcwOC0",
    host = "localhost"
    )
cur = conn.cursor()

# read generic query
with open("generic_astra_reduce_query.pgsql", "r") as f:
# with open("sql/astra/astra_process/astra_process_query.pgsql") as f:
    generic_query = f.read()

# If paralel, normal, or manual
if len(sys.argv) >= 6:
    settings = sys.argv[1:6]
elif len(sys.argv) >=4:
    settings = sys.argv[1:4] + [1,0]
else:
    print('Manual Settings')
    settings = [1514764800, 1577836800, 345600, 8, 6]

t_start, t_stop, t_step, n_step_skip, n_step_offset = int(settings[0]), int(settings[1]), int(settings[2]), int(settings[3]), int(settings[4])

# Generate list of completed astra_reduced
completed = re.compile("C:\\\\tmp\\\\(\d+)_\d+\.csv").findall(",".join(glob.glob("C:\\tmp\\*.csv")))

# Until the end
t = t_start + t_step * n_step_offset 
while t < t_stop:
    t_iter_start = time.time()

    # If it hasn't been reduced yet
    if str(t) not in completed:
        print("t: {}, t_start: {}, t_stop: {}, t_step: {}, n_step_skip: {}, n_step_offset: {}".format(t, t_start, t_stop, t_step, n_step_skip, n_step_offset))

        # Execute the generic query with the run specific settings to generate a temporary table
        run = {
            "name": "{}_{}".format(t, t + t_step),
            "start": t,
            "start_buffer": 2*24*60*60,
            "end": t + t_step,
            "trk_split_interval": 30*60, # 30 minutes!
            "queue_polygon_pattern": '%_threshold%',
            "queue_name_pattern": '([\w\/]*)_threshold',
            "gs_queue_threshold": 15,
            "gs_air_threshold": int(100 * 0.514444 * 4), # 100 knt * (0,514444 (m/s) / 1 knt) * (4 increments / 1 (m/s)) = 205.7776
            "distance_threshold": 150**2,
            "folder": "C:\\tmp\\",
            "empty_loc_array": "{NULL}", # Laat avond fix, hopelijk niet te verschrikekkelijk
            "air_loc_array": "{air}"
            }
        query = generic_query.format(**run)
        # print(query)
        cur.execute(query)

        # Download the results of the query
        query = """
        COPY (SELECT * FROM "{name}") TO '{folder}{name}.csv' DELIMITER ',' CSV HEADER;
        """.format(**run)
        cur.execute(query)

        # Rollback the temporary table (it has been downloaded)
        conn.rollback()

        print("Completed in {}s".format(int(time.time() - t_iter_start)))
    
    # Calculate next step
    t += n_step_skip * t_step

print(time.time()-timer_start)

