
import sqlite3
import datetime
import csv

start_time = datetime.datetime.now()
print 'Start at ',str(start_time)
print '======================================================================='

#----------
# CONSTANTS FOR ANALYSIS
REAL_FILE = 'C:/GTFS/SJRTD/ridershipdata.csv'
conn = sqlite3.connect('GTFS-SJRTD.sqlite')

#----------
# Step 1 - Load in the real data from MBTA and push it to the database
print '======================================================================='
print 'Step 1: Load Stockton data from file and add to database'
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
               # 0 - Scheduled Start Time - hours, minutes
               # 1 - Route - route_id
               # 2 - Route Name - SKIP
               # 3 - Direction - direction
               # 4 - stop sequence - stop_sequence
               # 5 - Stop Number - stop_id
               # 6 - Stop Name - SKIP
               # 7 - On - boardings
               # 8 - Off - alightings
               # 9 - Departure Load - ridership
                    
               time_pieces = row[0].split(' ')
               hhmm = time_pieces[0].split(':')
               hours = int(hhmm[0])
               minutes = int(hhmm[1])
               # Fix hours if necessary
               if time_pieces[1] == 'PM' and hours != 12:
                    hours += 12
               
               real_data.append([row[1],hours,minutes,row[3],row[5],int(row[4]),float(row[7]),float(row[8]),float(row[9])])

print '... dropping temporary tables if they exist'
sjrtd_cursor = conn.cursor()
sjrtd_cursor.execute('drop table if exists real_distinct')
conn.commit()

print '... creating temporary table for new data'
sjrtd_cursor.execute('create table real_distinct (route_id text, dep_hour int, dep_minute int, direction text, stop_id text, stop_sequence int, boarding float, alighting float, ridership float)')
conn.commit()

print '... adding data to temporary table'
sjrtd_cursor.executemany('insert into real_distinct values (?,?,?,?,?,?,?,?,?)',real_data)
conn.commit()

# END Step 1
#----------


#----------
# Step 2 - Create segment-level ridership
print '======================================================================='
print 'Step 2: Create segment-level ridership totals'
print str(datetime.datetime.now())

#
# NOTE
# This would be the place to look at limiting the data in time
# However, a more complex method would be needed since the origin departure time is given instead of the stop-by-stop level data
#

# Retrieve sorted data
sjrtd_cursor.execute('select route_id, dep_hour, dep_minute, direction, stop_id, ridership from real_distinct order by route_id, dep_hour, dep_minute, direction, stop_sequence')
sorted_data = sjrtd_cursor.fetchall()

real_segment_redundant = []
prev_route = []
prev_hour = []
prev_minute = []
prev_direction = []
prev_stop = []
prev_riders = 0

for row in sorted_data:
     this_route = row[0]
     this_hour = int(row[1])
     this_minute = int(row[2])
     this_direction = row[3]
     this_stop = row[4]
     this_riders = float(row[5])
     
     # Do something
     if this_route == prev_route and this_hour == prev_hour and this_minute == prev_minute and this_direction == prev_direction:
          # Matching route and departure time, make a segment
          # Route type is uniformly 3
          real_segment_redundant.append(tuple([prev_stop, this_stop, 3, this_hour, this_minute, prev_riders]))
          
     # Update previous information
     prev_route = row[0]
     prev_hour = int(row[1])
     prev_minute = int(row[2])
     prev_direction = row[3]
     prev_stop = row[4]
     prev_riders = float(row[5])

# Push to server
print '... dropping real_segments_redundant table if it exists'
sjrtd_cursor.execute('drop table if exists real_segments_redundant')
conn.commit()

print '... creating real_segments_redundant for new data'
sjrtd_cursor.execute('create table real_segments_redundant (from_stop text, to_stop text, route_type int, depart_hour int, depart_minute int, ridership float)')
conn.commit()

print '... adding data to real_segments_redundant'
sjrtd_cursor.executemany('insert into real_segments_redundant values (?,?,?,?,?,?)',real_segment_redundant)
conn.commit()

# END Step 2
#----------


#----------
# Step 3 - Consolidate data to unique segments
print '======================================================================='
print 'Step 3: Consolidate to unique segment total ridership'
print str(datetime.datetime.now())

print '... drop real_ridership_segments if exists'
sjrtd_cursor.execute('drop table if exists real_ridership_segments')
conn.commit()

# Here you could filter by time

print '... condense into new table real_ridership_segments'
sjrtd_cursor.execute('create table real_ridership_segments as select from_stop, to_stop, route_type, sum(ridership) as tot_ridership from real_segments_redundant group by from_stop, to_stop, route_type order by from_stop, to_stop, route_type')
conn.commit()

# END Step 3
#----------


#----------
# Clean Up
sjrtd_cursor.execute('vacuum')
conn.commit()
conn.close

end_time = datetime.datetime.now()
total_time = end_time - start_time
print '======================================================================='
print '======================================================================='
print 'End at {}.  Total run time {}'.format(end_time, total_time)