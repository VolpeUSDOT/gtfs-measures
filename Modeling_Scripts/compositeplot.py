
# -*- coding: utf-8 -*-

import gtfs_model
import datetime
import pandas as pd

start_time = datetime.datetime.now()
print 'Start at ',str(start_time)
print '======================================================================='

database = 'GTFS-ALL.sqlite'

agency_list = ['BART','LIRR','MBTA','MPLS','NAIPTA','SJRTD','ValleyMetro']

#runname = 'calib_01_allmodes_kitchensink'
#adj_rsquared = 0.327
runname = 'calib_02a_busonly_kitchensink'
adj_rsquared = 0.695

inputs = gtfs_model.load_calibration(runname)

coefficients = inputs['coeff']
formula_string = inputs['fs']
pvals = inputs['pvals']
route_type = inputs['rt']
route_type_string = inputs['rts']
selected_features = inputs['selected']

df = gtfs_model.get_all_from_db(database, route_type,'None',['BART'])
if len(df['from_stop']) > 0:
     results = gtfs_model.estimate_ridership(df, selected_features, coefficients)
     maxval = results['max']
     temp = results['est']
     bart = pd.DataFrame()
     bart['real'] = df['y']
     bart['est'] = temp['est']
else:
     maxval = 0
     bart = []

df = gtfs_model.get_all_from_db(database, route_type,'None',['LIRR'])
if len(df['from_stop']) > 0:
     results = gtfs_model.estimate_ridership(df, selected_features, coefficients)
     maxval = max([maxval,results['max']])
     temp = results['est']
     lirr = pd.DataFrame()
     lirr['real'] = df['y']
     lirr['est'] = temp['est']
else:
     lirr = []

df = gtfs_model.get_all_from_db(database, route_type,'None',['MBTA'])
if len(df['from_stop']) > 0:
     results = gtfs_model.estimate_ridership(df, selected_features, coefficients)
     maxval = max([maxval,results['max']])
     temp = results['est']
     mbta = pd.DataFrame()
     mbta['real'] = df['y']
     mbta['est'] = temp['est']
else:
     mbta = []

df = gtfs_model.get_all_from_db(database, route_type,'None',['MPLS'])
if len(df['from_stop']) > 0:
     results = gtfs_model.estimate_ridership(df, selected_features, coefficients)
     maxval = max([maxval,results['max']])
     temp = results['est']
     mpls = pd.DataFrame()
     mpls['real'] = df['y']
     mpls['est'] = temp['est']
else:
     mpls = []

df = gtfs_model.get_all_from_db(database, route_type,'None',['NAIPTA'])
if len(df['from_stop']) > 0:
     results = gtfs_model.estimate_ridership(df, selected_features, coefficients)
     maxval = max([maxval,results['max']])
     temp = results['est']
     naipta = pd.DataFrame()
     naipta['real'] = df['y']
     naipta['est'] = temp['est']
else:
     naipta = []

df = gtfs_model.get_all_from_db(database, route_type,'None',['SJRTD'])
if len(df['from_stop']) > 0:
     results = gtfs_model.estimate_ridership(df, selected_features, coefficients)
     maxval = max([maxval,results['max']])
     temp = results['est']
     sjrtd = pd.DataFrame()
     sjrtd['real'] = df['y']
     sjrtd['est'] = temp['est']
else:
     sjrtd = []

df = gtfs_model.get_all_from_db(database, route_type,'None',['ValleyMetro'])
if len(df['from_stop']) > 0:
     results = gtfs_model.estimate_ridership(df, selected_features, coefficients)
     maxval = max([maxval,results['max']])
     temp = results['est']
     valley = pd.DataFrame()
     valley['real'] = df['y']
     valley['est'] = temp['est']
else:
     valley = []


gtfs_model.plot_comparison_colored(bart, lirr, mbta, mpls, naipta, sjrtd, valley, maxval, route_type_string, adj_rsquared, runname)

end_time = datetime.datetime.now()
total_time = end_time - start_time
print '======================================================================='
print '======================================================================='
print 'End at {}.  Total run time {}'.format(end_time, total_time)