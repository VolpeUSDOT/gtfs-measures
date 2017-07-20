#------------------------------------------------------------------------------
# Name:        Real Import Wrapper for MBTA Rail
#
# Purpose:     This function interfaces with a csv containing ridership data
#              provided by Boston's Massachusetts Bay Transportation Authority.
#              This function only processes rail service only, in the form of
#              trip-station level data.
#
# Author:      Stephen Zitzow-Childs
#
# Created:     Sprint 2017
# Updated:     7/19/2017
#
# Volpe National Transportation Systems Center
# United States Department of Transportation
#------------------------------------------------------------------------------

import sqlite3
import datetime
import csv
import sys

start_time = datetime.datetime.now()
print 'Start at ',str(start_time)
print '======================================================================='

#----------
# CONSTANTS FOR ANALYSIS
REAL_FILE = 'C:/GTFS/MBTA/Rail flow database.csv'
MAP_FILE = 'C:/GTFS/MBTA/rail_stop_map.csv'
conn = sqlite3.connect('GTFS-MBTA.sqlite')
db_cursor = conn.cursor()

#----------
# Step 1 - Load in the real data from MBTA and push it to the database
print '======================================================================='
print 'Step 1: Load MBTA rail data from file and add to database'
print str(datetime.datetime.now())

# Preload the stop map
with open(MAP_FILE,'rb') as csvfile:
     infile = csv.reader(csvfile,delimiter=',')
     map_data = []
     header = 0
     for row in infile:
          if header == 0:
               header = 1
          else:
               # Read in map (route, orig, dest, seq, parent, stop_id)
               map_data.append([row[0],row[1],row[2],int(row[3]),row[4],row[5]])
     
print '... open and read in real rail data'
db_cursor.execute('drop table if exists rail_map')
db_cursor.execute('create table rail_map (route_id text, origin text, destination text, sequence int, parent_station text, stop_id text)')
db_cursor.executemany('insert into rail_map values (?,?,?,?,?,?)',map_data)
conn.commit()

with open(REAL_FILE,'rb') as csvfile:
     infile = csv.reader(csvfile,delimiter=';')
     mbta_data = []
     header = 0
     for row in infile:
          if header == 0:
               header = 1
          else:
               # add data to db
               season = row[0]
               dayofweek = row[1]
               route_id = row[2]
               direction = row[3]
               orig_stop = row[4]
               dest_stop = row[5]
               departure = row[6]
               stop_seq = int(row[7])
               parent_sta = row[8]
               parent_name = row[9]
               boarding = float(row[10])
               #skip
               #skip
               #skip
               #skip
               alighting = float(row[15])
               #skip
               #skip
               #skip
               #skip
               riders = float(row[20])
               #skip
               #skip
               #skip
               #skip
               try:
                    capacity = row[25]
               except:
                    cepacity = 0.0
               
               if route_id == 'Green':
                    # Reassign Green line route_ids to specify which branch
                    if orig_stop == 'Boston College':
                         route_id = 'Green-B'
                    elif orig_stop == 'Cleveland Circle':
                         route_id = 'Green-C'
                    elif orig_stop == 'Riverside':
                         route_id = 'Green-D'
                    elif orig_stop == 'Heath Street':
                         route_id = 'Green-E'
                    elif orig_stop == 'Lechmere':
                         if dest_stop == 'Boston College':
                              route_id = 'Green-B'
                         elif dest_stop == 'Cleveland Circle':
                              route_id = 'Green-C'
                         elif dest_stop == 'Riverside':
                              route_id = 'Green-D'
                         elif dest_stop == 'Heath Street':
                              route_id = 'Green-E'
                              
               
               
               mbta_data.append([season, dayofweek, route_id, direction, orig_stop, dest_stop, departure, stop_seq, parent_sta, parent_name, boarding, alighting, riders, capacity])

print '... load rail into database'
db_cursor.execute('drop table if exists real_rail')
db_cursor.execute('create table real_rail (season text, dayofweek text, route_id text, direction text, origin text, destination text, departure text, stop_seq int, parent_station text, parent_name text, boarding float, alighting float, riders float, capacity float)')
db_cursor.executemany('insert into real_rail values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',mbta_data)
conn.commit()
# END Step 1
#----------


#----------
# Step 2 - Create segment-level ridership
print '======================================================================='
print 'Step 2: Create segment-level ridership totals for rail'
print str(datetime.datetime.now())

print '... select only the fall rail data for analysis'
db_cursor.execute('select real_rail.route_id, real_rail.origin, real_rail.destination, real_rail.departure, rail_map.stop_id, real_rail.stop_seq, real_rail.riders from real_rail inner join rail_map on rail_map.route_id = real_rail.route_id and rail_map.origin = real_rail.origin and rail_map.destination = real_rail.destination and real_rail.parent_station = rail_map.parent_station where real_rail.season = \"Fall 2015\" and real_rail.dayofweek = \"weekday\" order by real_rail.route_id, real_rail.origin, real_rail.destination, real_rail.departure, real_rail.stop_seq')
sorted_subset = db_cursor.fetchall()

segment_data = []

prev_route = []
prev_orig = []
prev_dest = []
prev_depart = []
prev_stop = []
prev_seq = []
prev_riders = 0.0

for row in sorted_subset:
     this_route = row[0]
     this_orig = row[1]
     this_dest = row[2]
     this_depart = row[3]
     this_stop = row[4]
     this_seq = row[5]
     if this_seq == prev_seq:
          print 'something went wrong'
          print row
          conn.close()
          sys.exit(0)
     
     if prev_route == this_route and prev_orig == this_orig and prev_dest == this_dest and prev_depart == this_depart:
          # still on the same route, add a segment
          segment_data.append([prev_stop, this_stop, this_route, this_depart, prev_riders])
     
     prev_route = row[0]
     prev_orig = row[1]
     prev_dest = row[2]
     prev_depart = row[3]
     prev_stop = row[4]
     prev_seq = row[5]
     prev_riders = float(row[6])
     

print '... align rail data into segments'
db_cursor.execute('drop table if exists rail_redundant_segments_temp')
db_cursor.execute('create table rail_redundant_segments_temp (from_stop text, to_stop text, route_id text, departure text, ridership float)')
db_cursor.executemany('insert into rail_redundant_segments_temp values (?,?,?,?,?)',segment_data)
conn.commit()

print '... tie in route type'
db_cursor.execute('drop table if exists rail_redundant_segments')
db_cursor.execute('create table rail_redundant_segments as select rail_redundant_segments_temp.from_stop, rail_redundant_segments_temp.to_stop, routes.route_type, rail_redundant_segments_temp.ridership from rail_redundant_segments_temp inner join routes on routes.route_id = rail_redundant_segments_temp.route_id')

# Clean up
db_cursor.execute('drop table if exists rail_redundant_segments_temp')

# END Step 2
#----------


#----------
# Step 3 - Consolidate data to unique segments
print '======================================================================='
print 'Step 3: Consolidate to unique segment total ridership'
print str(datetime.datetime.now())

db_cursor.execute('drop table if exists rail_segments_unique')
db_cursor.execute('create table rail_segments_unique as select from_stop, to_stop, route_type, sum(ridership) as tot_ridership from rail_redundant_segments group by from_stop, to_stop, route_type order by from_stop, to_stop, route_type')
conn.commit()

# END Step 3
#----------


#----------
# Step 4 - Consolidate all real data
print '======================================================================='
print 'Step 4: Consolidate all real data to one table'
print str(datetime.datetime.now())

print '... unify rail and bus data'
db_cursor.execute('drop table if exists real_ridership_segments_temp')
db_cursor.execute('create table real_ridership_segments_temp as select * from real_ridership_segments union select * from rail_segments_unique')
conn.commit()

print '... uniquify'
db_cursor.execute('drop table if exists real_ridership_segments')
db_cursor.execute('create table real_ridership_segments as select from_stop, to_stop, route_type, sum(tot_ridership) as tot_ridership from real_ridership_segments_temp group by from_stop, to_stop, route_type order by from_stop, to_stop, route_type')
conn.commit()

# Clean up
db_cursor.execute('drop table if exists real_ridership_segments_temp')
conn.commit()

# END Step 4
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