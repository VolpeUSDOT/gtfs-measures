#------------------------------------------------------------------------------
# Name:        Dump Estimated to CSV
#
# Purpose:     This function exports estimated ridership data to a csv for 
#              integration with mapping functions in ArcGIS.
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

# Retrieve fields from correct sources
conn = sqlite3.connect('GTFS-ALL.sqlite')
conn2 = sqlite3.connect('GTFS-ValleyMetro.sqlite')
db_cursor = conn.cursor()
db2_cursor = conn2.cursor()

runname = 'calib_04d_busonly_excludeValleyMetro_significant3_for_ValleyMetro'

tablename = 'weighted_estimated_ridership'
outfilename = 'weighted_estimated_ridership--%s--ValleyMetro.csv' % runname

# Format weighted outputs for mapping
db_cursor.execute('select from_stop, to_stop, est_riders from %s' % runname)
temp = db_cursor.fetchall()

db2_cursor.execute('drop table if exists temp')
db2_cursor.execute('create table temp (from_stop text, to_stop text, est_riders float)')
db2_cursor.executemany('insert into temp values (?,?,?)',temp)

db2_cursor.execute('drop table if exists weighted_estimated_ridership')
db2_cursor.execute('create table weighted_estimated_ridership as select temp.from_stop, temp.to_stop, (temp.est_riders * segment_frequencies.frequency / conflated_statistics.frequency) as weighted_ridership, segment_frequencies.shape_id from temp inner join segment_frequencies on segment_frequencies.from_stop = temp.from_stop and segment_frequencies.to_stop = temp.to_stop inner join conflated_statistics on conflated_statistics.to_stop = temp.to_stop and conflated_statistics.from_stop = temp.from_stop')
conn2.commit()

with open(outfilename,'wb') as csvfile:
     outfile = csv.writer(csvfile,delimiter=',')
     outfile.writerow(['from_stop','to_stop','weighted_ridership','shape_id'])
     for row in db2_cursor.execute('select from_stop, to_stop, weighted_ridership, shape_id from weighted_estimated_ridership'):
          outfile.writerow(row)

conn.close()
conn2.close()