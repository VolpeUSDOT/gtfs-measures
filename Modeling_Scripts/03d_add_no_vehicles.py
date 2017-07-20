#------------------------------------------------------------------------------
# Name:        Add No Vehicles
#
# Purpose:     This function takes stop/station 1/4-mile-shed demographic 
#              information taken from the US Census Bureau's American Community 
#              Survey data and imports it to a system's database file for later 
#              analysis.  The features include number of households which do
#              and do not have vehicle access.  Data were compiled within an 
#              ArcGIS framework, and provided to this script in a 
#              preformatted csv.
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

REAL_FILE = 'C:/GTFS/MBTA/buffered_stops_with_addtl_demographics.csv'
conn = sqlite3.connect('GTFS-MBTA.sqlite')
db_cursor = conn.cursor()

#----------
# Step 1 - Load in the census data from file and load to DB
print '======================================================================='
print 'Step 1: Load additional demographic data from file and add to database'
print str(datetime.datetime.now())

with open(REAL_FILE,'rb') as csvfile:
     infile = csv.reader(csvfile,delimiter=',')
     add_acs = []
     header = 0
     for row in infile:
          if header == 0:
               header = 1
          else:
               # Columns:
               # All Data based on 1/4 mile buffers as of 4/10/2017
               # 0 - stop_id
               # 1 - occupied_housing
               # 2 - occupied_housing_no_vehicle
                    
               stop_id = row[0]
               
               try:
                    occupied_housing = float(row[1])
               except:
                    occupied_housing = 0.0
                    
               try:
                    occupied_housing_no_vehicle = float(row[2])
               except:
                    occupied_housing_no_vehicle = 0.0
               
               add_acs.append([stop_id, occupied_housing, occupied_housing_no_vehicle])
               
print '... dropping table if it exists'
db_cursor.execute('drop table if exists census_data_2')
conn.commit()

print '... creating census_data_2 table for new data'
db_cursor.execute('create table census_data_2 (stop_id text, occupied_housing float, occupied_housing_noveh float)')
conn.commit()

print '... adding data to census_data_2'
db_cursor.executemany('insert into census_data_2 values (?,?,?)',add_acs)
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