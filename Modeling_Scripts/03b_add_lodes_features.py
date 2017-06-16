# -*- coding: utf-8 -*-



import csv
import sqlite3
import datetime

start_time = datetime.datetime.now()
print 'Start at ',str(start_time)
print '======================================================================='

REAL_FILE = 'C:/GTFS/MBTA/buffered_stops_with_lodes.csv'
conn = sqlite3.connect('GTFS-MBTA.sqlite')
db_cursor = conn.cursor()

#----------
# Step 1 - Load in the census data from file and load to DB
print '======================================================================='
print 'Step 1: Load LODES data from file and add to database'
print str(datetime.datetime.now())

with open(REAL_FILE,'rb') as csvfile:
     infile = csv.reader(csvfile,delimiter=',')
     lodes_data = []
     header = 0
     for row in infile:
          if header == 0:
               header = 1
          else:
               # Columns:
               # All Data based on 1/4 mile buffers as of 4/10/2017
               # 0 - Stop_ID
               # 1 - SUM_CLIP_JOBS
               # 2 - SUM_CLIP_LOW_EARNINGS
               # 3 - SUM_CLIP_MID_EARNINGS
               # 4 - SUM_CLIP_HIGH_EARNINGS
               # 5 - SUM_CLIP_INFORMATION
               # 6 - SUM_CLIP_FINANCE_INS
               # 7 - SUM_CLIP_REAL_ESTATE
               # 8 - SUM_CLIP_PROFESSIONAL
               # 9 - SUM_CLIP_MANAGEMENT
               # 10 - SUM_CLIP_ADMIN
               # 11 - SUM_CLIP_EDUCATION
               # 12 - SUM_CLIP_HEALTH_CARE
               
               stop_id = row[0]
               try:
                    jobs_all = float(row[1])
               except:
                    jobs_all = 0.0
               
               try:
                    jobs_low = float(row[2])
               except:
                    jobs_low = 0.0
               
               try:
                    jobs_mid = float(row[3])
               except:
                    jobs_mid = 0.0
               
               try:
                    jobs_high = float(row[4])
               except:
                    jobs_high = 0.0
               
               try:
                    jobs_info = float(row[5])
               except:
                    jobs_info = 0.0
               
               try:
                    jobs_finance = float(row[6])
               except:
                    jobs_finance = 0.0
               
               try:
                    jobs_real_estate = float(row[7])
               except:
                    jobs_real_estate = 0.0
               
               try:
                    jobs_prof = float(row[8])
               except:
                    jobs_prof = 0.0
               
               try:
                    jobs_mgmt = float(row[9])
               except:
                    jobs_mgmt = 0.0
               
               try:
                    jobs_admin = float(row[10])
               except:
                    jobs_admin = 0.0
               
               try:
                    jobs_edu = float(row[11])
               except:
                    jobs_edu = 0.0
               
               try:
                    jobs_health = float(row[12])
               except:
                    jobs_health = 0.0
                              
               lodes_data.append([stop_id,jobs_all,jobs_low,jobs_mid,jobs_high,jobs_info,jobs_finance,jobs_real_estate,jobs_prof,jobs_mgmt,jobs_admin,jobs_edu,jobs_health])

print '... dropping table if it exists'
db_cursor.execute('drop table if exists lodes_data')
conn.commit()

print '... creating lodes_data table for new data'
db_cursor.execute('create table lodes_data (stop_id text, jobs_all float, jobs_low float, jobs_mid float, jobs_high float, jobs_info float, jobs_finance float, jobs_real_estate float, jobs_prof float, jobs_mgmt float, jobs_admin float, jobs_edu float, jobs_health float)')
conn.commit()

print '... adding data to census_data'
db_cursor.executemany('insert into lodes_data values (?,?,?,?,?,?,?,?,?,?,?,?,?)',lodes_data)
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