#------------------------------------------------------------------------------
# Name:        Add Intersection Density
#
# Purpose:     This function takes stop/station 1/4-mile-shed intersection 
#              count around each station/stop within a system's transit 
#              network.  The counts were developed using the ARNOLD network 
#              within an ArcGIS framework.
#
# Author:      Stephen Zitzow-Childs
#
# Created:     Sprint 2017
# Updated:     7/19/2017
#
# Volpe National Transportation Systems Center
# United States Department of Transportation
#------------------------------------------------------------------------------

import csv
import sqlite3
import datetime

start_time = datetime.datetime.now()
print 'Start at ',str(start_time)
print '======================================================================='

REAL_FILE = 'C:/GTFS/MBTA/buffered_stops_with_nodes.csv'
conn = sqlite3.connect('GTFS-MBTA.sqlite')
db_cursor = conn.cursor()

#----------
# Step 1 - Load in the census data from file and load to DB
print '======================================================================='
print 'Step 1: Load intersection density data from file and add to database'
print str(datetime.datetime.now())

with open(REAL_FILE,'rb') as csvfile:
     infile = csv.reader(csvfile,delimiter=',')
     int_dens_data = []
     header = 0
     for row in infile:
          if header == 0:
               header = 1
          else:
               # Columns:
               # All Data based on 1/4 mile buffers as of 4/10/2017
               # 0 - intersection_density
               # 1 - stop_id
               try:
                    int_dens = float(row[0])
               except:
                    int_dens = 0.0
                    
               stop_id = row[1]
               
               int_dens_data.append([stop_id, int_dens])
               
print '... dropping table if it exists'
db_cursor.execute('drop table if exists intersection_density')
conn.commit()

print '... creating intersection_density table for new data'
db_cursor.execute('create table intersection_density (stop_id text, intersections float)')
conn.commit()

print '... adding data to census_data'
db_cursor.executemany('insert into intersection_density values (?,?)',int_dens_data)
conn.commit()
    
# END Step 1
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