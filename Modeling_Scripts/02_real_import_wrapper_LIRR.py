
import sqlite3
import datetime
import csv

start_time = datetime.datetime.now()
print 'Start at ',str(start_time)
print '======================================================================='

#----------
# CONSTANTS FOR ANALYSIS
REAL_FILE = 'C:/GTFS/LIRR/ridershipdata_lirr.csv'
conn = sqlite3.connect('GTFS-LIRR.sqlite')
db_cursor = conn.cursor()

#----------
# Step 1 - Load in the real data from MBTA and push it to the database
print '======================================================================='
print 'Step 1: Load LIRR data from file and add to database'
print str(datetime.datetime.now())

with open(REAL_FILE,'rb') as csvfile:
     infile = csv.reader(csvfile,delimiter=',')
     lirr_data = []
     header = 0
     for row in infile:
          if header == 0:
               header = 1
          else:
               # Strip rows for dummy end of line on rt 71
               # Columns:
               # 0 - TRAIN - Train number                             USE
               # 1 - DATE - Date of data                              SKIP
               # 2 - DAY - Day                                        USE
               # 3 - DIRECTION - Direction of trip                    SKIP
               # 4 - OSTATION - Trip Origin Station                   USE
               # 5 - OTIME - Origin Time                              SPLIT
               # 6 - DSTATION - Trip Destination Station              USE
               # 7 - DTIME - Destination Time                         SKIP
               # 8 - CARS - Number of carriages in train              SKIP
               # 9 - WEATHER - Weather                                SKIP
               # 10 - BRANCH - Route branch                           SKIP
               # 11 - STAT_ORDER - Order of stations on this trip     USE
               # 12 - stop_id - Stop id of origin station             USE
               # 13 - STATION - Current station                       USE
               # 14 - ONS - Boardings                                 USE
               # 15 - OFFS - Alightings                               USE
               # 16 - ONBOARD - Ridership heading out                 USE
                    
               time_pieces = row[5].split(' ')
               hhmm = time_pieces[0].split(':')
               hours = int(hhmm[0])
               minutes = int(hhmm[1])
               # Fix hours if necessary
               if time_pieces[1] == 'PM' and hours != 12:
                    hours += 12
               
               lirr_data.append([int(row[0]),unicode(row[2]),unicode(row[4]),hours,minutes,unicode(row[6]),int(row[11]),unicode(row[12]),unicode(row[13]),float(row[14]),float(row[15]),float(row[16])])

print '... dropping temporary tables if they exist'
db_cursor.execute('drop table if exists real_temp')
db_cursor.execute('drop table if exists real_distinct')
conn.commit()

print '... creating temporary table for new data'
db_cursor.execute('create table real_temp (train int, dayofweek text, train_orig text, hour int, minute int, train_dest text, stat_order int, stop_id text, station text, boarding int, alighting int, ridership int)')
conn.commit()

print '... adding data to temporary table'
db_cursor.executemany('insert into real_temp values (?,?,?,?,?,?,?,?,?,?,?,?)',lirr_data)
conn.commit()

print '... condensing out redundant rows in real data'
db_cursor.execute('create table real_distinct as select train_orig, train_dest, hour, minute, stop_id, stat_order, sum(boarding) as boarding, sum(alighting) as alighting, sum(ridership) as ridership from real_temp where dayofweek not in (\'Sunday\',\'Saturday\') group by train_orig, train_dest, hour, minute, stop_id order by train, train_orig, train_dest, dayofweek, hour, minute, stat_order')
conn.commit()

print '... dropping temp table'
db_cursor.execute('drop table if exists real_temp')

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
db_cursor.execute('select train_orig, train_dest, hour, minute, stop_id, boarding, alighting, ridership from real_distinct order by train_orig, train_dest, hour, minute, stat_order')
sorted_lirr = db_cursor.fetchall()

real_segment_redundant = []
prev_orig = []
prev_dest = []
prev_hour = []
prev_minute = []
prev_stop_id = []
prev_riders = 0

for row in sorted_lirr:
     this_orig = row[0]
     this_dest = row[1]
     this_hour = row[2]
     this_minute = row[3]
     this_stop_id = row[4]
     
     # Do something
     if this_orig == prev_orig and this_dest == prev_dest and this_hour == prev_hour and this_minute == prev_minute:
          # Matching route and departure time, make a segment
          # Add route type which is uniformly 2
          real_segment_redundant.append(tuple([prev_stop_id, this_stop_id, 2, this_hour, this_minute, prev_riders]))
          
     # Update previous information
     prev_orig = row[0]
     prev_dest = row[1]
     prev_hour = int(row[2])
     prev_minute = int(row[3])
     prev_stop_id = row[4]
     prev_riders = float(row[7])

# Push to server
print '... dropping real_segments_redundant table if it exists'
db_cursor.execute('drop table if exists real_segments_redundant')
conn.commit()

print '... creating real_segments_redundant for new data'
db_cursor.execute('create table real_segments_redundant (from_stop text, to_stop text, route_type int,depart_hour int, depart_minute int, ridership float)')
conn.commit()

print '... adding data to real_segments_redundant'
db_cursor.executemany('insert into real_segments_redundant values (?,?,?,?,?,?)',real_segment_redundant)
conn.commit()

# END Step 2
#----------


#----------
# Step 3 - Consolidate data to unique segments
print '======================================================================='
print 'Step 3: Consolidate to unique segment total ridership'
print str(datetime.datetime.now())

print '... drop real_segments_unique if exists'
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