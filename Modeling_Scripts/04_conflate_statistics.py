# -*- coding: utf-8 -*-

import sqlite3
import datetime

start_time = datetime.datetime.now()
print 'Start at ',str(start_time)
print '======================================================================='

conn = sqlite3.connect('GTFS-MBTA.sqlite')
db_cursor = conn.cursor()

#----------
# Step 1 - 
print '======================================================================='
print 'Step 1: Determine what datasets are available for conflating'
print str(datetime.datetime.now())

# Census
try:
     db_cursor.execute('select count(*) as num_recs from census_data')
     count = db_cursor.fetchall()
     count = count[0][0]
     if count > 0:
          census = True
     else:
          census = False
except:
     census = False
     
# Census addendum
try:
     db_cursor.execute('select count(*) as num_recs from census_data_2')
     count = db_cursor.fetchall()
     count = count[0][0]
     if count > 0:
          census2 = True
     else:
          census2 = False
except:
     census2 = False

# LODES data
try:
     db_cursor.execute('select count(*) as num_recs from lodes_data')
     count = db_cursor.fetchall()
     count = count[0][0]
     if count > 0:
          lodes = True
     else:
          lodes = False
except:
     lodes = False     

# Intersection density
try:
     db_cursor.execute('select count(*) as num_recs from intersection_density')
     count = db_cursor.fetchall()
     count = count[0][0]
     if count > 0:
          intersections = True
     else:
          intersections = False
except:
     intersections = False

# END Step 1
#----------


#----------
# Step 2 - Add data to conflated
print '======================================================================='
print 'Step 2: Conflate data to segments'
print str(datetime.datetime.now())

print '... drop existing conflated statistics'
db_cursor.execute('drop table if exists conflated_statistics')
conn.commit()

print '... set aside the primary service characteristics and ridership'
db_cursor.execute('create table conflated_statistics as select segment_frequencies_aggregated.from_stop, segment_frequencies_aggregated.to_stop, segment_frequencies_aggregated.route_type, real_ridership_segments.tot_ridership, segment_frequencies_aggregated.frequency, segment_frequencies_aggregated.num_routes from segment_frequencies_aggregated inner join real_ridership_segments on real_ridership_segments.from_stop = segment_frequencies_aggregated.from_stop and real_ridership_segments.to_stop = segment_frequencies_aggregated.to_stop and real_ridership_segments.route_type = segment_frequencies_aggregated.route_type')
conn.commit()


if census:
     print '... add census data to conflated statistics'
     # Set aside current conflated
     db_cursor.execute('drop table if exists conflated_statistics_temp')
     db_cursor.execute('create table conflated_statistics_temp as select * from conflated_statistics')
     conn.commit()

     # Drop current conflated
     db_cursor.execute('drop table if exists conflated_statistics')
     conn.commit()

     # Create new conflated as select
     db_cursor.execute('create table conflated_statistics as select conflated_statistics_temp.*, census_data.pop_all, census_data.pop_minority, census_data.households, census_data.households_poverty, census_data.pop_edu_age25plus, census_data.pop_edu_hs_or_less, census_data.pop_edu_college_or_some, census_data.pop_edu_adv_degree, census_data.housing_structures, census_data.housing_structures_single, census_data.housing_structures_small_multi, census_data.housing_structures_medium_multi, census_data.housing_structures_large_multi, census_data.pop_work_16plus, census_data.pop_work_16plus_transit, census_data.housing_renting, census_data.median_age, census_data.median_hh_income, census_data.median_rent from conflated_statistics_temp inner join census_data on conflated_statistics_temp.from_stop = census_data.stop_id')
     conn.commit()
          
     # Clean up
     db_cursor.execute('drop table if exists conflated_statistics_temp')
     conn.commit()

if census2:
     print '... add census addendum'
     # Set aside current conflated
     db_cursor.execute('drop table if exists conflated_statistics_temp')
     db_cursor.execute('create table conflated_statistics_temp as select * from conflated_statistics')
     conn.commit()

     # Drop current conflated
     db_cursor.execute('drop table if exists conflated_statistics')
     conn.commit()

     # Create new conflated as select
     db_cursor.execute('create table conflated_statistics as select conflated_statistics_temp.*, census_data_2.occupied_housing, census_data_2.occupied_housing_noveh from conflated_statistics_temp inner join census_data_2 on conflated_statistics_temp.from_stop = census_data_2.stop_id')
     conn.commit()
          
     # Clean up
     db_cursor.execute('drop table if exists conflated_statistics_temp')
     conn.commit()

if lodes:
     print '... add LODES data to conflated statistics'
     # Set aside current conflated
     db_cursor.execute('drop table if exists conflated_statistics_temp')
     db_cursor.execute('create table conflated_statistics_temp as select * from conflated_statistics')
     conn.commit()

     # Drop current conflated
     db_cursor.execute('drop table if exists conflated_statistics')
     conn.commit()

     # Create new conflated as select
     db_cursor.execute('create table conflated_statistics as select conflated_statistics_temp.*, lodes_data.jobs_all, lodes_data.jobs_low, lodes_data.jobs_mid, lodes_data.jobs_high, lodes_data.jobs_info, lodes_data.jobs_finance, lodes_data.jobs_real_estate, lodes_data.jobs_prof, lodes_data.jobs_mgmt, lodes_data.jobs_admin, lodes_data.jobs_edu, lodes_data.jobs_health from conflated_statistics_temp inner join lodes_data on conflated_statistics_temp.from_stop = lodes_data.stop_id')
     conn.commit()
          
     # Clean up
     db_cursor.execute('drop table if exists conflated_statistics_temp')
     conn.commit()

if intersections:
     print '... add intersection density to conflated statistics'
     # Set aside current conflated
     db_cursor.execute('drop table if exists conflated_statistics_temp')
     db_cursor.execute('create table conflated_statistics_temp as select * from conflated_statistics')
     conn.commit()

     # Drop current conflated
     db_cursor.execute('drop table if exists conflated_statistics')
     conn.commit()

     # Create new conflated as select
     db_cursor.execute('create table conflated_statistics as select conflated_statistics_temp.*, intersection_density.intersections from conflated_statistics_temp inner join intersection_density on intersection_density.stop_id = conflated_statistics_temp.from_stop')
     conn.commit()
          
     # Clean up
     db_cursor.execute('drop table if exists conflated_statistics_temp')
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