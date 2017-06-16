# -*- coding: utf-8 -*-

import gtfs_model
import datetime

start_time = datetime.datetime.now()
print 'Start at ',str(start_time)
print '======================================================================='

database = 'GTFS-ALL.sqlite'

agency_list = ['BART','LIRR','MBTA','MPLS','NAIPTA','SJRTD','ValleyMetro']

#runlist = ['calib_01_allmodes_kitchensink','calib_02a_busonly_kitchensink','calib_02b_busonly_significant','calib_02c_busonly_significant2','calib_02d_busonly_significant3','calib_03_railonly_kitchensink','calib_04a_busonly_excludeValleyMetro_kitchensink','calib_04b_busonly_excludeValleyMetro_significant','calib_04c_busonly_excludeValleyMetro_significant2','calib_04d_busonly_excludeValleyMetro_significant3']
runlist = ['calib_02a_busonly_kitchensink']

for runname in runlist:
     inputs = gtfs_model.load_calibration(runname)
     
     coefficients = inputs['coeff']
     formula_string = inputs['fs']
     pvals = inputs['pvals']
     route_type = inputs['rt']
     route_type_string = inputs['rts']
     selected_features = inputs['selected']
     
     for agency in agency_list:
          print agency
          this_name = '%s_for_%s' % (runname, agency)
          # Get Data
          df = gtfs_model.get_all_from_db(database, route_type,'None',[agency])
          if len(df['from_stop']) == 0:
               continue
          # Estimate Riderhsip
          results = gtfs_model.estimate_ridership(df, selected_features, coefficients)
          estimated = results['est']
          maxval = results['max']
          
          # Calculate Adjusted R-Squared
          n = len(df['y'])
          p = len(selected_features)
          adj_rsquared = gtfs_model.calculate_adj_rsquared(df['y'], estimated['est'], n, p)
          
          # Create Figure
          gtfs_model.plot_comparison(list(df['y']),list(estimated['est']),maxval,route_type_string,adj_rsquared, this_name)
          
          # Write out results
          gtfs_model.write_csv_summary(this_name, route_type_string, formula_string, adj_rsquared, selected_features, coefficients, pvals)
          
          # Write estimation to 
          sqlresults = gtfs_model.write_sql_summary('GTFS-ALL.sqlite',this_name,estimated)

end_time = datetime.datetime.now()
total_time = end_time - start_time
print '======================================================================='
print '======================================================================='
print 'End at {}.  Total run time {}'.format(end_time, total_time)