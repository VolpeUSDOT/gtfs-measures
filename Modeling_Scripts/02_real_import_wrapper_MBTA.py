
import sqlite3
import datetime
import csv

start_time = datetime.datetime.now()
print 'Start at ',str(start_time)
print '======================================================================='

#----------
# CONSTANTS FOR ANALYSIS
REAL_FILE = 'C:/GTFS/MBTA/ridership_estimate.csv'
conn = sqlite3.connect('GTFS-MBTA.sqlite')
db_cursor = conn.cursor()

#----------
# Step 1 - Load in the real data from MBTA and push it to the database
print '======================================================================='
print 'Step 1: Load MBTA data from file and add to database'
print str(datetime.datetime.now())

with open(REAL_FILE,'rb') as csvfile:
     infile = csv.reader(csvfile,delimiter=',')
     mbta_data = []
     header = 0
     for row in infile:
          if header == 0:
               header = 1
          else:
               if row[0] != '99999':
                    # Strip rows for dummy end of line on rt 71
                    # Columns:
                         # 0 - stop_id                 text
                         # 1 - stop_description        text
                         # 2 - route_id                text
                         # 3 - boarding                float
                         # 4 - alighting               float
                         # 5 - ridership (load out)    float
                         # 6 - direction               text
                         # 7 - origin_time             datetime
                             #--> time converted to ints of hh, mm, ss
                         # 8 - day_of_week             text
                         # 9 - stop_sequence           int
                    time_pieces = row[7].split(':')
                    hours = int(time_pieces[0])
                    minutes = int(time_pieces[1])
                    seconds = 0
                    # Fix hours if necessary
                    if hours >= 24:
                         hours -= 24
                    
                    if len(time_pieces) > 2:
                         seconds = int(time_pieces[2])
                    
                    mbta_data.append(tuple([row[0],row[1],row[2],float(row[3]),float(row[4]),float(row[5]),row[6],hours,minutes,seconds,row[8],int(row[9])]))

print '... dropping tables'
db_cursor.execute('drop table if exists mbta_real')
db_cursor.execute('drop table if exists mbta_real_temp')
conn.commit()

print '... creating mbta_real_temp for new data'
db_cursor.execute('create table mbta_real_temp (stop_id text, stop_desc text, route_id text, boarding float, alighting float, ridership float, direction text, depart_hour int, depart_minute int, depart_second int, day_of_week text, stop_sequence int)')
conn.commit()

print '... adding data to mbta_real_temp'
db_cursor.executemany('insert into mbta_real_temp values (?,?,?,?,?,?,?,?,?,?,?,?)',mbta_data)
conn.commit()

print '... condensing out redundant rows in real data'
db_cursor.execute('create table mbta_real as select stop_id, stop_desc, route_id, sum(boarding) as boarding, sum(alighting) as alighting, sum(ridership) as ridership, direction, depart_hour, depart_minute, depart_second, day_of_week, stop_sequence from mbta_real_temp group by stop_id, stop_desc, route_id, direction, depart_hour, depart_minute, depart_second, day_of_week, stop_sequence')

print '... dropping temporary table'
db_cursor.execute('drop table if exists mbta_real_temp')
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

# Retrieve sorted data - NOTE: all data is currently WKDY so day of week is dropped
db_cursor.execute('select stop_id, route_id, boarding, alighting, ridership, direction, depart_hour, depart_minute, depart_second, stop_sequence from mbta_real order by route_id, direction, depart_hour, depart_minute, depart_second, stop_sequence')
sorted_mbta = db_cursor.fetchall()

redundant_segment_ridership = []
prev_stop_id = []
prev_route_id = []
prev_direction = []
prev_hour = []
prev_minute = []
prev_second = []
prev_boarding = 0
prev_alighting = 0
prev_ridership = 0

for row in sorted_mbta:
     this_stop_id = row[0]
     this_route_id = row[1]
     this_direction = row[5]
     this_hour = row[6]
     this_minute = row[7]
     this_second = row[8]
     
     # Do something
     if this_route_id == prev_route_id and this_direction == prev_direction and this_hour == prev_hour and this_minute == prev_minute and this_second == prev_second:
          # Matching route, direction, and departure time, make a segment
          redundant_segment_ridership.append(tuple([prev_stop_id,this_stop_id,this_route_id,prev_hour,prev_minute,prev_second,prev_ridership]))
          
     # Update previous information
     prev_stop_id = row[0]
     prev_route_id = row[1]
     prev_boarding = row[2]
     prev_alighting = row[3]
     prev_ridership = row[4]
     prev_direction = row[5]
     prev_hour = row[6]
     prev_minute = row[7]
     prev_second = row[8]

# Push to server
print '... dropping mbta_real_segments_temp table if it exists'
db_cursor.execute('drop table if exists mbta_real_segments_temp')
conn.commit()

print '... creating mbta_real_segments_temp for new data'
db_cursor.execute('create table mbta_real_segments_temp (from_stop text, to_stop text, route_id text, depart_hour int, depart_minute int, depart_second int, ridership float)')
conn.commit()

print '... adding data to mbta_real_segments_temp'
db_cursor.executemany('insert into mbta_real_segments_temp values (?,?,?,?,?,?,?)',redundant_segment_ridership)
conn.commit()

# Tie in route type
print '... tie in route type information'
db_cursor.execute('drop table if exists mbta_real_segments')
db_cursor.execute('create table mbta_real_segments as select mbta_real_segments_temp.*, routes.route_type from mbta_real_segments_temp inner join routes on routes.route_id = mbta_real_segments_temp.route_id')
conn.commit()

# Clean up
db_cursor.execute('drop table if exists mbta_real_segments_temp')
conn.commit()

# END Step 2
#----------


#----------
# Step 3 - Consolidate data to unique segments
print '======================================================================='
print 'Step 3: Consolidate to unique segment total ridership'
print str(datetime.datetime.now())

print '... drop real_ridership_segments if exists'
cursor_drop_mbta_segments_unique = conn.cursor()
cursor_drop_mbta_segments_unique.execute('drop table if exists real_ridership_segments')
conn.commit()

print '... condense into new table real_ridership_segments'
cursor_condense_mbta_segments = conn.cursor()
cursor_condense_mbta_segments.execute('create table real_ridership_segments as select from_stop, to_stop, route_type, sum(ridership) as tot_ridership from mbta_real_segments group by from_stop, to_stop, route_type order by from_stop, to_stop, route_type')
conn.commit()

# END Step 3
#----------


#----------
# Clean Up
db_cursor.execute('vacuum')
conn.commit()
conn.close

end_time = datetime.datetime.now()
total_time = end_time - start_time
print '======================================================================='
print '======================================================================='
print 'End at {}.  Total run time {}'.format(end_time, total_time)