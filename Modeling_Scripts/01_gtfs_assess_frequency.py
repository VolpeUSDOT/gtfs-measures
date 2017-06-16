#------------------------------------------------------------------------------
# Name:        Assess GTFS Frequency
# Purpose:     This GTFS analysis effort relies on compiling service 
#              characteristics from a GTFS feed. Given an input date, daily 
#              total frequency, routes, etc. are assembled for every stop-to-
#              stop segment in the transit network.
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

import sqlite3
import datetime
import calendar
import gtfsloader
import gtfsfreq
import myprint

start_time = datetime.datetime.now()
print 'Start at ',str(start_time)
print '======================================================================='

#----------
# CONSTANTS FOR ANALYSIS
TEST_DATE = '2016-10-05'
DAY_OF_WEEK = calendar.day_name[datetime.datetime.strptime(TEST_DATE,'%Y-%m-%d').weekday()].lower()
GTFS_FOLDER = 'C:/GTFS/ValleyMetro/'
conn = sqlite3.connect('GTFS-Test.sqlite')
db_cursor = conn.cursor()

#----------
# Step 1 - Get the list of service_ids which match with the given date
print '======================================================================='
print 'Step 1: Get the list of matching service ids for input date'
print str(datetime.datetime.now())


# Get the list of all service ids matching that date generically
service_id_list = []
for service_id_row in db_cursor.execute('select service_id from calendar where start_date <= \"%s\" and end_date >= \"%s\" and %s' % (TEST_DATE, TEST_DATE, DAY_OF_WEEK)):
     service_id_list.append([service_id_row[0]])
     
# Check for exceptions
service_id_exceptions_sql = ()

# Check whether it is an add-in exception, or a remove exception
add_exception = []
remove_exception = []
for exception_row in db_cursor.execute('select service_id, exception_type from calendar_dates where \"date\" = \"%s\"' % TEST_DATE):
     if exception_row[1] == 1:
          add_exception.append(exception_row[0])
     elif exception_row[1] == 2:
          remove_exception.append(exception_row[0])

# Check the list for removals
final_service_id_list = []
for this_serv in service_id_list:
     this_serv = this_serv[0]
     if this_serv not in remove_exception:
          final_service_id_list.append(tuple([this_serv]))

# Add all add-in service_ids
for addin in add_exception:
     final_service_id_list.append(tuple([addin]))

print '... dropping selected_service table if it exists'
db_cursor.execute('drop table if exists selected_service')
conn.commit()

print '... creating selected_service for new data'
db_cursor.execute('create table selected_service (service_id text)')
conn.commit()

print '... adding data to selected_service'
db_cursor.executemany('insert into selected_service values (?)',final_service_id_list)
conn.commit()

# END Step 1
#----------


#----------
# Step 2 - For each service_id, get a full list of matching trip_ids, and join in the route type
print '======================================================================='
print 'Step 2: Get full list of trip ids for the given selected date'
print str(datetime.datetime.now())

print '... dropping selected_trips table if it exists'
db_cursor.execute('drop table if exists selected_trips')
conn.commit()

print '... creating selected_trips for new data'
db_cursor.execute('create table selected_trips as select trips.trip_id, trips.route_id, shape_id, route_type from trips inner join selected_service on selected_service.service_id = trips.service_id inner join routes on routes.route_id = trips.route_id')
conn.commit()

print '... new component to deal with frequency-based systems'
db_cursor.execute('select trip_id, start_time, end_time, headway_secs from frequencies')
freq_data = db_cursor.fetchall()

freq_adjustments = []
for row in freq_data:
     this_trip = row[0]
     cur_depart = datetime.datetime.strptime(row[1],'%Y-%m-%d %H:%M:%S.%f')
     last_depart = datetime.datetime.strptime(row[2],'%Y-%m-%d %H:%M:%S.%f')
     headway = int(row[3])
     tot_freq = 1
     
     while cur_depart <= last_depart:
          tot_freq += 1
          cur_depart = cur_depart + datetime.timedelta(seconds=headway)

     freq_adjustments.append([this_trip,tot_freq])

db_cursor.execute('drop table if exists frequency_adjustments')
db_cursor.execute('create table frequency_adjustments (trip_id text, freq_adj int)')
db_cursor.executemany('insert into frequency_adjustments values (?,?)',freq_adjustments)
conn.commit()

print '... create final adjustment list'
db_cursor.execute('drop table if exists freq_temp')
db_cursor.execute('drop table if exists freq_temp_union')
db_cursor.execute('create table freq_temp as select distinct trip_id, 1 as freq_adj from trips')
db_cursor.execute('create table freq_temp_union as select * from frequency_adjustments union select * from freq_temp')
db_cursor.execute('drop table if exists frequency_adjustments')
db_cursor.execute('create table frequency_adjustments as select trip_id, max(freq_adj) as freq_adj from freq_temp_union group by trip_id')
db_cursor.execute('drop table if exists freq_temp')
db_cursor.execute('drop table if exists freq_temp_union')

# END Step 2
#----------


#----------
# Step 3 - Get all segments in the entire trip list, and tie in several key columns
print '======================================================================='
print 'Step 3: Create list of all segments for all trips and tie in additional data'
print str(datetime.datetime.now())

redundant_segs = []
counter = 0
previous_trip = []
previous_stop = []
shape_id = []
for this_trip in db_cursor.execute('select stop_times.trip_id, stop_times.stop_id, trips.shape_id, stop_times.departure_time from stop_times inner join trips on trips.trip_id = stop_times.trip_id order by stop_times.trip_id, stop_times.stop_sequence'):
     if previous_trip == this_trip[0]:
          # Attached to the same trip, add a segment
          redundant_segs.append(tuple([previous_stop,this_trip[1],this_trip[0],shape_id,this_trip[3]]))
     # Update previous information
     previous_trip = this_trip[0]
     previous_stop = this_trip[1]
     shape_id = this_trip[2]
     
     counter+= 1
     if counter % 10000 == 0:
          myprint.update()
myprint.final()

print '... dropping redundant_segments_temp table if it exists'
db_cursor.execute('drop table if exists redundant_segments_temp')
conn.commit()

print '... dropping redundant_segments_timed table if it exists'
db_cursor.execute('drop table if exists redundant_segments_timed')
conn.commit()

print '... creating redundant_segments_temp for new data'
db_cursor.execute('create table redundant_segments_temp (from_stop text, to_stop text, trip_id text, shape_id text, departure_time datetime)')
conn.commit()
     
print '... adding data to redundant_segments_temp'
db_cursor.executemany('insert into redundant_segments_temp values (?,?,?,?,?)',redundant_segs)
conn.commit()

print '... join in route_id and route_type to each trip-segment'
db_cursor.execute('create table redundant_segments_timed as select redundant_segments_temp.from_stop, redundant_segments_temp.to_stop, redundant_segments_temp.trip_id, redundant_segments_temp.shape_id, redundant_segments_temp.departure_time, trips.route_id, routes.route_type from redundant_segments_temp inner join trips on trips.trip_id = redundant_segments_temp.trip_id inner join routes on routes.route_id = trips.route_id')

print '... dropping redundant_segments_temp'
db_cursor.execute('drop table if exists redundant_segments_temp')
conn.commit()
# END Step 3
#----------


#----------
# Step 4 - Parse through the list of segments and get all unique segments in the network
print '======================================================================='
print 'Step 4: Get list of all unique segments in the network'
print str(datetime.datetime.now())

print '... dropping distinct_segments_all table if it exists'
db_cursor.execute('drop table if exists distinct_segments_all')
conn.commit()

print '... creating distinct_segments_all from select'
db_cursor.execute('create table distinct_segments_all as select distinct from_stop, to_stop, shape_id, route_type from redundant_segments_timed')
conn.commit()
# END Step 4
#----------


#----------
# Step 5 - Get list of timed segments for selected date
print '======================================================================='
print 'Step 5: Select all segments/times for matching service_ids'
print str(datetime.datetime.now())

print '... dropping redundant_segments_selected table if it exists'
db_cursor.execute('drop table if exists redundant_segments_selected')
conn.commit()

print '... creating distinct_segments_selected from select'
db_cursor.execute('create table redundant_segments_selected as select from_stop, to_stop, redundant_segments_timed.trip_id, redundant_segments_timed.shape_id, departure_time, redundant_segments_timed.route_id, redundant_segments_timed.route_type, frequency_adjustments.freq_adj from redundant_segments_timed inner join selected_trips on selected_trips.trip_id = redundant_segments_timed.trip_id inner join frequency_adjustments on frequency_adjustments.trip_id = redundant_segments_timed.trip_id')
conn.commit()

# END Step 5
#----------


#----------
# Step 6 - For each distinct segment, shape, type produce frequency and # routes
print '======================================================================='
print 'Step 6: Produce segment-level frequency and # routes'
print str(datetime.datetime.now())

#
# NOTE
# Here is where time could be incorporated within the sql call to limit start time and end time
#

db_cursor.execute('select redundant_segments_selected.from_stop, redundant_segments_selected.to_stop, redundant_segments_selected.shape_id, redundant_segments_selected.route_type, redundant_segments_selected.route_id, redundant_segments_selected.freq_adj from redundant_segments_selected order by redundant_segments_selected.from_stop, redundant_segments_selected.to_stop, redundant_segments_selected.route_type, redundant_segments_selected.shape_id, redundant_segments_selected.route_id')
all_segments = db_cursor.fetchall()

[from_to_type_results, from_to_type_shape_results] = gtfsfreq.calculateSegmentFrequency(all_segments)

# Load the from_to_type frequency and route counts data
print '... dropping segment_frequencies_aggregated table if it exists'
db_cursor.execute('drop table if exists segment_frequencies_aggregated')
conn.commit()

print '... creating segment_frequencies_aggregated for new data'
db_cursor.execute('create table segment_frequencies_aggregated (from_stop text, to_stop text, route_type int, frequency int, num_routes int)')
conn.commit()
     
print '... adding data to segment_frequencies_aggregated'
db_cursor.executemany('insert into segment_frequencies_aggregated values (?,?,?,?,?)',from_to_type_results)
conn.commit()

# Load the from_to_type_shape frequency and route counts data
print '... dropping segment_frequencies table if it exists'
db_cursor.execute('drop table if exists segment_frequencies')
conn.commit()

print '... creating segment_frequencies for new data'
db_cursor.execute('create table segment_frequencies (from_stop text, to_stop text, route_type text, shape_id text, frequency int, num_routes int)')
conn.commit()
     
print '... adding data to segment_frequencies'
db_cursor.executemany('insert into segment_frequencies values (?,?,?,?,?,?)',from_to_type_shape_results)
conn.commit()

# END Step 6
#----------


#----------
# Step 7 - Retrieve the parent stops from the original GTFS in order to aggregate multimodal stations data
print '======================================================================='
print 'Step 7: Get parent stations to aggregate multimodal stations'
print str(datetime.datetime.now())

print '... Loading original stops data'
stopsdata = gtfsloader.stops_gtfs(GTFS_FOLDER)

print '... Listing parent stops'
parent_list = []

for item in stopsdata:
     if item[-1]:
          parent_list.append([item[0], item[-1]])

# Load the parent stops to the database
print '... dropping parent_stops table if it exists'
db_cursor.execute('drop table if exists parent_stops')
conn.commit()

print '... creating parent_stops for new data'
db_cursor.execute('create table parent_stops (stop_id text, parent_sta text)')
conn.commit()
     
print '... adding data to parent_stops'
db_cursor.executemany('insert into parent_stops values (?,?)',parent_list)
conn.commit()

# END Step 7
#----------


#----------
# Step 8 - Retrieve redundant_segments_selected and replace parent stations
print '======================================================================='
print 'Step 8: Replace parent stations in redundant segments listing'
print str(datetime.datetime.now())

db_cursor.execute('select from_stop, to_stop, trip_id, shape_id, departure_time, route_id, route_type, freq_adj from redundant_segments_selected')
all_segments = db_cursor.fetchall()

# Replace any stop_id with the appropriate parent_id
updated_segments = []
for row in all_segments:
     this_seg = [row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7]]
     for possible_parent in parent_list:
          if this_seg[0] == possible_parent[0]:
               this_seg[0] = possible_parent[1]
          if this_seg[1] == possible_parent[0]:
               this_seg[1] = possible_parent[1]
     updated_segments.append(tuple(this_seg))

     if len(updated_segments) % 1000 == 0:
          myprint.update()
myprint.final()

# Load the parent stops to the database
print '... dropping redundant_segments_selected_parents table if it exists'
db_cursor.execute('drop table if exists redundant_segments_selected_parents')
conn.commit()

print '... creating redundant_segments_selected_parents for new data'
db_cursor.execute('create table redundant_segments_selected_parents (from_stop text, to_stop text, trip_id text, shape_id text, departure_time datetime, route_id text, route_type int, freq_adj int)')
conn.commit()
     
print '... adding data to redundant_segments_selected_parents'
db_cursor.executemany('insert into redundant_segments_selected_parents values (?,?,?,?,?,?,?,?)',updated_segments)
conn.commit()

# END Step 8
#----------


#----------
# Step 9 - Redo step 6 with the data where parent stations were replaced
print '======================================================================='
print 'Step 9: Produce segment-level frequency and # routes with parent stations replaced'
print str(datetime.datetime.now())

#
# NOTE
# Here is where time could be incorporated within the sql call to limit start time and end time
#

db_cursor.execute('select from_stop, to_stop, shape_id, route_type, route_id, freq_adj from redundant_segments_selected_parents order by from_stop, to_stop, route_type, shape_id, route_id')
all_segments = db_cursor.fetchall()

[from_to_type_results_parents, from_to_type_shape_results_parents] = gtfsfreq.calculateSegmentFrequency(all_segments)

# Load the from_to_type frequency and route counts data
print '... dropping segment_frequencies_aggregated_parents table if it exists'
db_cursor.execute('drop table if exists segment_frequencies_aggregated_parents')
conn.commit()

print '... creating segment_frequencies_aggregated_parents for new data'
db_cursor.execute('create table segment_frequencies_aggregated_parents (from_stop text, to_stop text, route_type int, frequency int, num_routes int)')
conn.commit()
     
print '... adding data to segment_frequencies_aggregated_parents'
db_cursor.executemany('insert into segment_frequencies_aggregated_parents values (?,?,?,?,?)',from_to_type_results_parents)
conn.commit()

# Load the from_to_type_shape frequency and route counts data
print '... dropping segment_frequencies_parents table if it exists'
db_cursor.execute('drop table if exists segment_frequencies_parents')
conn.commit()

print '... creating segment_frequencies_parents for new data'
db_cursor.execute('create table segment_frequencies_parents (from_stop text, to_stop text, route_type text, shape_id text, frequency int, num_routes int)')
     
print '... adding data to segment_frequencies_parents'
db_cursor.executemany('insert into segment_frequencies_parents values (?,?,?,?,?,?)',from_to_type_shape_results_parents)
conn.commit()

# END Step 9
#----------


#----------
# Step 10 - Produce stop-frequency for both the non-parent and parent versions of redundant_segments
print '======================================================================='
print 'Step 10: Produce stop-level frequency and # routes with and without parents'
print str(datetime.datetime.now())

#
# NOTE
# Here is where time could be incorporated within the sql call to limit start time and end time
#
#--- Process the non-parent version
db_cursor.execute('select from_stop, route_type, route_id, freq_adj from redundant_segments_selected order by from_stop, route_type, route_id')
all_stops_no_parents = db_cursor.fetchall()

stop_freq_no_parent = gtfsfreq.calculateStopFrequency(all_stops_no_parents)

#
# NOTE
# Here is where time could be incorporated within the sql call to limit start time and end time
#
#--- Process the parent version
db_cursor.execute('select from_stop, route_type, route_id, freq_adj from redundant_segments_selected_parents order by from_stop, route_type, route_id')
all_stops_parents = db_cursor.fetchall()

stop_freq_parent = gtfsfreq.calculateStopFrequency(all_stops_parents)

# Load the from_to_type frequency and route counts data
print '... dropping stop_frequencies table if it exists'
db_cursor.execute('drop table if exists stop_frequencies')
conn.commit()

print '... creating stop_frequencies for new data'
db_cursor.execute('create table stop_frequencies (stop_id text, serv_freq int, num_routes int, num_rt_types int, intermodal int)')
conn.commit()

print '... adding data to stop_frequencies'
db_cursor.executemany('insert into stop_frequencies values (?,?,?,?,?)',stop_freq_no_parent)
conn.commit()

# Load the from_to_type_shape frequency and route counts data
print '... dropping stop_frequencies_parents table if it exists'
db_cursor.execute('drop table if exists stop_frequencies_parents')
conn.commit()

print '... creating stop_frequencies_parents for new data'
db_cursor.execute('create table stop_frequencies_parents (stop_id text, serv_freq int, num_routes int, num_rt_types int, intermodal int)')
conn.commit()

print '... adding data to stop_frequencies_parents'
db_cursor.executemany('insert into stop_frequencies_parents values (?,?,?,?,?)',stop_freq_parent)
conn.commit()

# END Step 10
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