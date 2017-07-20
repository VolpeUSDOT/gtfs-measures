#------------------------------------------------------------------------------
# Name:        Real Import Wrapper for ValleyMetro
#
# Purpose:     This function interfaces with a csv containing ridership data
#              provided by Phoenix's Valley Metro Regional Public 
#              Transportation Authority (Valley Metro).  Data was provided at 
#              the route-stop level.
#
# Author:      Stephen Zitzow-Childs
#
# Created:     Winter 2016
# Updated:     7/19/2017
#
# Volpe National Transportation Systems Center
# United States Department of Transportation
#------------------------------------------------------------------------------

import sqlite3
import datetime
import csv

start_time = datetime.datetime.now()
print 'Start at ',str(start_time)
print '======================================================================='

#----------
# CONSTANTS FOR ANALYSIS
REAL_FILE = 'C:/GTFS/ValleyMetro/ridershipdata.csv'
conn = sqlite3.connect('GTFS-ValleyMetro.sqlite')
db_cursor = conn.cursor()

#----------
# Step 1 - Load in the real data from ValleyMetro and push it to the database
print '======================================================================='
print 'Step 1: Load ValleyMetro data from file and add to database'
print str(datetime.datetime.now())
 
with open(REAL_FILE,'rb') as csvfile:
     infile = csv.reader(csvfile,delimiter=',')
     valleymetro_data = []
     header = 0
     for row in infile:
          if header == 0:
               header = 1
          else:
               if row[0] != '99999':
                    # Strip rows for dummy end of line on rt 71
                    # Columns:
                         # 0 - day_of_week             text
                         # 1 - route_id                text
                         # 2 - direction               text
                         # 3 - stop_sequence           int
                         # 4 - stop_id                 text
                         # 5 - stop_name               text
                         # 6 - boarding                float
                         # 7 - alighting               float
                         # 8 - ridership               float
                    
                    valleymetro_data.append(tuple([row[0],row[1],row[2],int(row[3]),row[4],row[5],float(row[6]),float(row[7]),float(row[8])]))

print '... dropping temporary table if it exists'
db_cursor.execute('drop table if exists real_distinct')
conn.commit()

print '... creating real_distinct for new data'
db_cursor.execute('create table real_distinct (day_of_week text, route_id text, direction text, stop_sequence int, stop_id text, stop_name text, boarding float, alighting float, ridership float)')
conn.commit()

print '... adding data to real_distinct'
db_cursor.executemany('insert into real_distinct values (?,?,?,?,?,?,?,?,?)',valleymetro_data)
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
db_cursor.execute('select route_id, direction, stop_sequence, stop_id, boarding, alighting, ridership from real_distinct order by route_id, direction, stop_sequence')
sorted_valleymetro = db_cursor.fetchall()

redundant_segment_ridership = []
prev_stop_id = []
prev_route_id = []
prev_direction = []
prev_ridership = 0

for row in sorted_valleymetro:
     this_route_id = row[0]
     this_direction = row[1]
     this_stop_id = row[3]
     this_boarding = row[4]
     this_alighting = row[5]
     this_ridership = row[6]
     
     # Do something
     if this_route_id == prev_route_id and this_direction == prev_direction:
          # Matching route and direction, make a segment
          # Route type is uniformly 3
            redundant_segment_ridership.append(tuple([prev_stop_id,this_stop_id,3,this_route_id,prev_ridership]))
          
     # Update previous information
     prev_stop_id = row[3]
     prev_route_id = row[0]
     prev_direction = row[1]
     prev_ridership = row[6]

# Push to server
print '... dropping real_segments_redundant table if it exists'
db_cursor.execute('drop table if exists real_segments_redundant')
conn.commit()

print '... creating real_segments_redundant for new data'
db_cursor.execute('create table real_segments_redundant (from_stop text, to_stop text, route_type int, route_id text, ridership float)')
conn.commit()

print '... adding data to real_segments_redundant'
db_cursor.executemany('insert into real_segments_redundant values (?,?,?,?,?)',redundant_segment_ridership)
conn.commit()

# END Step 2
#----------


#----------
# Step 3 - Consolidate data to unique segments
print '======================================================================='
print 'Step 3: Consolidate to unique segment total ridership'
print str(datetime.datetime.now())

print '... drop real_ridership_segments if exists'
db_cursor.execute('drop table if exists real_ridership_segments')
conn.commit()

print '... condense into new table real_ridership_segments'
db_cursor.execute('create table real_ridership_segments as select from_stop, to_stop, route_type, sum(ridership) as tot_ridership from real_segments_redundant group by from_stop, to_stop, route_type order by from_stop, to_stop, route_type')
conn.commit()

# END Step 3
#----------


#----------
# Clean Up
conn.close

end_time = datetime.datetime.now()
total_time = end_time - start_time
print '======================================================================='
print '======================================================================='
print 'End at {}.  Total run time {}'.format(end_time, total_time)