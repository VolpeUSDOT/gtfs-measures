
import sqlite3
import datetime
import csv

start_time = datetime.datetime.now()
print 'Start at ',str(start_time)
print '======================================================================='

#----------
# CONSTANTS FOR ANALYSIS
REAL_FILE = 'C:/GTFS/MPLS/ridershipdata.csv'
conn = sqlite3.connect('GTFS-MPLS.sqlite')
db_cursor = conn.cursor()


# *************************************************
#                                                       //
# BIG NOTE!!!                                          //==============
#                                                     {{==============
# MPLS doesn't use time, everything is day totals     \\===============
#                                                      \\
# *************************************************



#----------
# Step 1 - Load in the real data from MBTA and push it to the database
print '======================================================================='
print 'Step 1: Load MPLS data from file and add to database'
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
               # 0 - Route - Route number         USE
               # 1 - Dir - Direction of route     USE
               # 2 - site_id - Stop Id            USE
               # 3 - StopName - Stop Name         USE
               # 4 - Ons - Boardings              USE
               # 5 - Offs - Alightings            USE
               # 6 - Seq - Stop order             USE
               temp1 = 0
               temp2 = 0
               if row[4] != '':
                    temp1 = row[4].replace(',','')
               if row[5] != '':
                    temp2 = row[5].replace(',','')
                        
                    
               # Fix Route Type based on simple rules for special cases:
               if row[0] in ['Blue Line','Green Line']:
                    route_type = 0
               elif row[0] == 'North Star':
                    route_type = 2
               else:
                    route_type = 3
                    
               real_data.append([row[0],route_type,row[1],row[2],row[3],float(temp1),float(temp2),float(row[6])])

print '... dropping temporary tables if they exist'
db_cursor.execute('drop table if exists real_temp')
db_cursor.execute('drop table if exists real_distinct')
conn.commit()

print '... creating temporary table for new data'
db_cursor.execute('create table real_temp (route text, route_type int, direction text, stop_id text, stop_name text, boarding float, alighting float, stop_order int)')
conn.commit()

print '... adding data to temporary table'
db_cursor.executemany('insert into real_temp values (?,?,?,?,?,?,?,?)',real_data)
conn.commit()

print '... condensing out redundant rows in real data'
# Added flag to only grab buses
db_cursor.execute('create table real_distinct as select route, route_type, direction, stop_id, stop_order, sum(boarding) as boarding, sum(alighting) as alighting from real_temp group by route, route_type, direction, stop_id order by route, direction, stop_order')
conn.commit()

print '... dropping temp table'
db_cursor.execute('drop table if exists real_temp')
conn.commit()

# END Step 1
#----------


#----------
# Step 2 - Create segment-level ridership
print '======================================================================='
print 'Step 2: Create segment-level ridership totals'
print str(datetime.datetime.now())


# Retrieve sorted data
db_cursor.execute('select route, direction, stop_id, boarding, alighting, route_type from real_distinct order by route, direction, stop_order')
sorted_mpls = db_cursor.fetchall()

real_segment_redundant = []
prev_route = []
prev_direction = []
prev_stop_id = []
running_riders = 0

for row in sorted_mpls:
     this_route = row[0]
     this_direction = row[1]
     this_stop_id = row[2]
     this_boarding = float(row[3])
     this_alighting = float(row[4])
     this_route_type = row[5]
     
     # Do something
     if this_route == prev_route and this_direction == prev_direction:
          # Matching route and departure time, make a segment
          real_segment_redundant.append(tuple([prev_stop_id, this_stop_id, this_route_type, running_riders]))
     else:
          # Not the same grouping, reset the ridership running total
          running_riders = 0
          
     # Update previous information
     prev_route = row[0]
     prev_direction = row[1]
     prev_stop_id = row[2]
     running_riders = max(0,running_riders + this_boarding - this_alighting)

# Push to server
print '... dropping real_segments_redundant table if it exists'
db_cursor.execute('drop table if exists real_segments_redundant')
conn.commit()

print '... creating real_segments_redundant for new data'
db_cursor.execute('create table real_segments_redundant (from_stop text, to_stop text, route_type int, ridership float)')
conn.commit()

print '... adding data to real_segments_redundant'
db_cursor.executemany('insert into real_segments_redundant values (?,?,?,?)',real_segment_redundant)
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

# Here you could filter by time

print '... condense into new table real_ridership_segments'
db_cursor.execute('create table real_ridership_segments as select from_stop, to_stop, route_type, sum(ridership) as tot_ridership from real_segments_redundant group by from_stop, to_stop, route_type order by from_stop, to_stop, route_type')
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