#------------------------------------------------------------------------------
# Name:        Fix GTFS MBTA
# Purpose:     Certain data fields which are imported using the pygtfs module 
#              use the wrong type and inadvertenly strip important information.  
#              To remedy this, the affected tables are reimported with a 
#              replacement script.  The affected tables are:
#              -- Shapes
#              -- Trips
#              -- Stops
#              Only data in the MBTA feed was affected for this project, so 
#              only the one feed was corrected.
#
# Author:      Stephen Zitzow-Childs
#
# Created:     Spring 2017
# Updated:     4/27/2017
#
# Volpe National Transportation Systems Center
# United States Department of Transportation
#
#------------------------------------------------------------------------------

import csv
import sqlite3
import datetime

start_time = datetime.datetime.now()
print 'Start at ',str(start_time)
print '======================================================================='

#----------
# CONSTANTS FOR ANALYSIS
REAL_SHAPES = 'C:/GTFS/MBTA/shapes.txt'
REAL_TRIPS = 'C:/GTFS/MBTA/trips.txt'
REAL_STOPS = 'C:/GTFS/MBTA/stops.txt'
conn = sqlite3.connect('GTFS-MBTA.sqlite')

#----------
# Step 1
print '======================================================================='
print 'Step 1: Load data for MBTA feed from shapes.txt, trips.txt, and stops.txt'
print str(datetime.datetime.now())

# Read and load shapes.txt
shapes_data = []
with open(REAL_SHAPES,'rb') as csvfile:
     infile = csv.reader(csvfile,delimiter=',')
     header = 0
     for row in infile:
          if header == 0:
               header = 1
          else:
               try:
                    lat = float(row[1])
               except:
                    lat = -1.0
               
               try:
                    lon = float(row[2])
               except:
                    lon = -1.0
                    
               try:
                    seq = int(row[3])
               except:
                    seq = -1
                    
               try:
                    dist = float(row[4])
               except:
                    dist = -1.0
               shapes_data.append([row[0],lat,lon,seq,dist])

# Read and load trips.txt
trips_data = []
with open(REAL_TRIPS,'rb') as csvfile:
     infile = csv.reader(csvfile,delimiter=',')
     header = 0
     for row in infile:
          if header == 0:
               header = 1
          else:
               trips_data.append([row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]])
         
# Read and load stops.txt
stops_data = []
with open(REAL_STOPS,'rb') as csvfile:
     infile = csv.reader(csvfile,delimiter=',')
     header = 0
     for row in infile:
          if header == 0:
               header = 1
          else:
               try:
                    lat = float(row[4])
               except:
                    lat = -1
                    
               try:
                    lon = float(row[5])
               except:
                    lon = -1
                    
               try:
                    loc_type = int(row[8])
               except:
                    loc_type = -1
                    
               stops_data.append([row[0],row[1],row[2],row[3],lat,lon,row[6],row[7],loc_type,row[9],row[10]])

# END Step 1
#----------


#----------
# Step 2
print '======================================================================='
print 'Step 2: Replace the tables in GTFS-MBTA.sqlite'
print str(datetime.datetime.now())

db_cursor = conn.cursor()

print '... drop the tables created by pygtfs'
db_cursor.execute('drop table if exists shapes')
db_cursor.execute('drop table if exists trips')
db_cursor.execute('drop table if exists stops')
conn.commit()

print '... add back the replacement tables'
db_cursor.execute('create table shapes (shape_id text, shape_pt_lat float, shape_pt_lon float, shape_pt_sequence int, shape_dist_traveled float)')
db_cursor.execute('create table trips (route_id text, service_id text, trip_id text, trip_headsign text, trip_short_name text, direction_id text, block_id text, shape_id text, wheelchair_accessible int)')
db_cursor.execute('create table stops (stop_id text, stop_code text, stop_name text, stop_desc text, stop_lat float, stop_lon float, zone_id text, stop_url text, location_type int, parent_station text, wheelchair_boarding text)')
db_cursor.executemany('insert into shapes values (?,?,?,?,?)',shapes_data)
db_cursor.executemany('insert into trips values (?,?,?,?,?,?,?,?,?)',trips_data)
db_cursor.executemany('insert into stops values (?,?,?,?,?,?,?,?,?,?,?)',stops_data)
conn.commit()

# END Step 2
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