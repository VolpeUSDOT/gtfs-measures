#------------------------------------------------------------------------------
# Name:        Add Census Features
#
# Purpose:     This function takes stop/station 1/4-mile-shed demographic 
#              information taken from the US Census Bureau's American Community 
#              Survey data and imports it to a system's database file for later 
#              analysis.  The features include housing, population, and 
#              education level data for the region immediately surrounding 
#              each stop or station within a system's transit network.  Data 
#              were compiled within an ArcGIS framework, and provided to this 
#              script in a preformatted csv.
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

REAL_FILE = 'C:/GTFS/ValleyMetro/buffered_stops_with_demographics.csv'
conn = sqlite3.connect('GTFS-ValleyMetro.sqlite')
db_cursor = conn.cursor()

#----------
# Step 1 - Load in the census data from file and load to DB
print '======================================================================='
print 'Step 1: Load Census data from file and add to database'
print str(datetime.datetime.now())

with open(REAL_FILE,'rb') as csvfile:
     infile = csv.reader(csvfile,delimiter=',')
     census_data = []
     header = 0
     for row in infile:
          if header == 0:
               header = 1
          else:
               # Columns:
               # All Data based on 1/4 mile buffers as of 3/2/2017
               # 0 - Stop_ID
               # 1 - SUM_CLIP_POP
               # 2 - SUM_CLIP_MINORITY	
               # 3 - SUM_CLIP_HH	
               # 4 - SUM_CLIP_HH_POV	
               # 5 - SUM_CLIP_POP_EDUCATION	
               # 6 - SUM_CLIP_POP_HS_LESS	
               # 7 - SUM_CLIP_POP_COLLEGE	
               # 8 - SUM_CLIP_POP_ADVANCED_DEGREE	
               # 9 - SUM_CLIP_HU_STRUCT	
               # 10 - SUM_CLIP_HU_STRUCT_1	
               # 11 - SUM_CLIP_HU_STRUCT_2_4	
               # 12 - SUM_CLIP_HU_STRUCT_5_19	
               # 13 - SUM_CLIP_HU_STRUCT_20_PLUS	
               # 14 - SUM_CLIP_WORKERS	
               # 15 - SUM_CLIP_WORKERS_TRANSIT	
               # 16 - SUM_CLIP_HU_CASH_RENT	
               # 17 - NEW_MED_AGE	
               # 18 - NEW_HH_INC	
               # 19 - NEW_MED_RENT
               
               stop_id = row[0]
               try:
                    pop_all = float(row[1])
               except:
                    pop_all = 0.0
               
               try:
                    pop_minority = float(row[2])
               except:
                    pop_minority = 0.0
               
               try:
                    households = float(row[3])
               except:
                    households = 0.0
               
               try:
                    households_poverty = float(row[4])
               except:
                    households_poverty = 0.0
               
               try:
                    pop_edu_age25plus = float(row[5])
               except:
                    pop_edu_age25plus = 0.0
               
               try:
                    pop_edu_hs_or_less = float(row[6])
               except:
                    pop_edu_hs_or_less = 0.0
               
               try:
                    pop_edu_college_or_some = float(row[7])
               except:
                    pop_edu_college_or_some = 0.0
               
               try:
                    pop_edu_adv_degree = float(row[8])
               except:
                    pop_edu_adv_degree = 0.0
               
               try:
                    housing_structures = float(row[9])
               except:
                    housing_structures = 0.0
               
               try:
                    housing_structures_single = float(row[10])
               except:
                    housing_structures_single = 0.0
               
               try:
                    housing_structures_small_multi = float(row[11])
               except:
                    housing_structures_small_multi = 0.0
               
               try:
                    housing_structures_medium_multi = float(row[12])
               except:
                    housing_structures_medium_multi = 0.0
               
               try:
                    housing_structures_large_multi = float(row[13])
               except:
                    housing_structures_large_multi = 0.0
               
               try:
                    pop_work_16plus = float(row[14])
               except:
                    pop_work_16plus = 0.0
               
               try:
                    pop_work_16plus_transit = float(row[15])
               except:
                    pop_work_16plus_transit = 0.0
               
               try:
                    housing_renting = float(row[16])
               except:
                    housing_renting = 0.0
               
               try:
                    median_age = float(row[17])
               except:
                    median_age = 0.0
               
               try:
                    median_hh_income = float(row[18])
               except:
                    median_hh_income = 0.0
               
               try:
                    median_rent = float(row[19])
               except:
                    median_rent = 0.0
               
               census_data.append([stop_id,pop_all,pop_minority,households,households_poverty,pop_edu_age25plus,pop_edu_hs_or_less,pop_edu_college_or_some,pop_edu_adv_degree,housing_structures,housing_structures_single,housing_structures_small_multi,housing_structures_medium_multi,housing_structures_large_multi,pop_work_16plus,pop_work_16plus_transit,housing_renting,median_age,median_hh_income,median_rent])

print '... dropping table if it exists'
db_cursor.execute('drop table if exists census_data')
conn.commit()

print '... creating census_data table for new data'
db_cursor.execute('create table census_data (stop_id text, pop_all float, pop_minority float, households float, households_poverty float, pop_edu_age25plus float, pop_edu_hs_or_less float, pop_edu_college_or_some float, pop_edu_adv_degree float, housing_structures float, housing_structures_single float, housing_structures_small_multi float, housing_structures_medium_multi float, housing_structures_large_multi float, pop_work_16plus float, pop_work_16plus_transit float, housing_renting float ,median_age float, median_hh_income float, median_rent float)')
conn.commit()

print '... adding data to census_data'
db_cursor.executemany('insert into census_data values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',census_data)
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