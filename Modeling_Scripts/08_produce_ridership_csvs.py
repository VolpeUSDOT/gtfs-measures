import csv
import sqlite3

# Retrieve fields from correct sources
agency = ['BART','LIRR','MBTA','MPLS','NAIPTA','SJRTD','ValleyMetro']
for this_agency in agency:
     conn = sqlite3.connect('GTFS-%s.sqlite' % this_agency)
     db_cursor = conn.cursor()
     
     tablename = 'weighted_real_ridership_by_shape'
     outfilename = 'weighted_real_ridership--%s.csv' % this_agency
     
     # Format weighted outputs for mapping
     db_cursor.execute('drop table if exists weighted_real_ridership_by_shape')
     db_cursor.execute('create table weighted_real_ridership_by_shape as select conflated_statistics.from_stop, conflated_statistics.to_stop, (conflated_statistics.tot_ridership * segment_frequencies.frequency / conflated_statistics.frequency) as weighted_ridership, segment_frequencies.shape_id from conflated_statistics inner join segment_frequencies on segment_frequencies.from_stop = conflated_statistics.from_stop and segment_frequencies.to_stop = conflated_statistics.to_stop')
     #db_cursor.execute('drop table if exists weighted_modeled_ridership_by_shape')
     #db_cursor.execute('create table weighted_modeled_ridership_by_shape as select model_results.from_stop, model_results.to_stop, (model_results.modeled_ridership * segment_frequencies.frequency / segment_frequencies_aggregated.frequency) as weighted_ridership, segment_frequencies.shape_id from model_results inner join segment_frequencies on segment_frequencies.from_stop = model_results.from_stop and segment_frequencies.to_stop = model_results.to_stop inner join segment_frequencies_aggregated on segment_frequencies_aggregated.from_stop = model_results.from_stop and segment_frequencies_aggregated.to_stop = model_results.to_stop')
     conn.commit()
     
     with open(outfilename,'wb') as csvfile:
          outfile = csv.writer(csvfile,delimiter=',')
          outfile.writerow(['from_stop','to_stop','weighted_ridership','shape_id'])
          for row in db_cursor.execute('select * from %s' % tablename):
               outfile.writerow(row)
               
     conn.close()