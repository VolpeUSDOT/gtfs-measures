
import sqlite3
import datetime
import csv

start_time = datetime.datetime.now()
print 'Start at ',str(start_time)
print '======================================================================='

#----------
# CONSTANTS FOR ANALYSIS
REAL_FILE = 'C:/GTFS/NAIPTA/ridership_handfixed.csv'
conn = sqlite3.connect('GTFS-NAIPTA.sqlite')

#----------
# Step 1 - Load in the real data from MBTA and push it to the database
print '======================================================================='
print 'Step 1: Load NAIPTA data from file and add to database'
print str(datetime.datetime.now())

with open(REAL_FILE,'rb') as csvfile:
     infile = csv.reader(csvfile,delimiter=',')
     real_data = []
     header = 0
     for row in infile:
          if header == 0:
               header = 1
          else:
               # Strip rows for dummy end of line on rt 71
               # Columns:
               # 0 - route_id - Route code
               # 1 - from_stop - From stop id
               # 2 - to_stop - To stop id
               # 3 - ridership
               
               # Route_type is uniformly 3
               real_data.append([row[0],row[1],row[2],3,float(row[3])])

print '... dropping table if it exists'
naipta_cursor = conn.cursor()
naipta_cursor.execute('drop table if exists real_redundant')
conn.commit()

print '... creating table for new data'
naipta_cursor.execute('create table real_redundant (route_num text, from_stop text, to_stop text, route_type int, ridership float)')
conn.commit()

print '... adding data to table'
naipta_cursor.executemany('insert into real_redundant values (?,?,?,?,?)',real_data)
conn.commit()

# END Step 1
#----------


#----------
# Step 2 - Consolidate data to unique segments
print '======================================================================='
print 'Step 2: Consolidate to unique segment total ridership'
print str(datetime.datetime.now())

print '... drop real_ridership_segments if exists'
naipta_cursor.execute('drop table if exists real_ridership_segments')
conn.commit()

# Here you could filter by time

print '... condense into new table real_ridership_segments'
naipta_cursor.execute('create table real_ridership_segments as select from_stop, to_stop, route_type, sum(ridership) as tot_ridership from real_redundant group by from_stop, to_stop, route_type order by from_stop, to_stop, route_type')
conn.commit()

# END Step 2
#----------


#----------
# Clean Up
naipta_cursor.execute('vacuum')
conn.commit()
conn.close

end_time = datetime.datetime.now()
total_time = end_time - start_time
print '======================================================================='
print '======================================================================='
print 'End at {}.  Total run time {}'.format(end_time, total_time)