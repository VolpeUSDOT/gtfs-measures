#------------------------------------------------------------------------------
# Name:        CompositePlot
#
# Purpose:     This file includes functions related to model analysis and 
#              figure generation to support functions number 06_ and later.  
#              Individual function short descriptions are provided below.
#
# Author:      Stephen Zitzow-Childs
#
# Created:     Sprint 2017
# Updated:     7/19/2017
#
# Volpe National Transportation Systems Center
# United States Department of Transportation
#------------------------------------------------------------------------------

import statsmodels.formula.api as sm
import sqlite3
import pandas
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import csv

# Plot real versus estimated ridership with a 45 degree line for perfect match
def plot_comparison(real, estimated, ax_max, route_type_string, adj_rsquared, runname):
     plt.figure()
     plt.subplot(111)
     plt.scatter(real,estimated,s=10,c='b')
     plt.plot([0,ax_max],[0,ax_max],c='r')
     plt.ylim([0,ax_max])
     plt.xlim([0,ax_max])
     plt.xlabel('Real Ridership')
     plt.ylabel('Estimated Ridership')
     plt.text(ax_max*0.1,ax_max*0.9,'Route Types: %s' % route_type_string)
     plt.text(ax_max*0.1,ax_max*0.85,'Adj. R-Squared: %.3f' % adj_rsquared)
     plt.title(runname)
     plt.show()
     return

# Plot real versus estimated ridership with a 45 degree line for perfect match,
# but with each agency colorized individually
def plot_comparison_colored(bart, lirr, mbta, mpls, naipta, sjrtd, valley, ax_max, route_type_string, adj_rsquared, runname):
     plt.figure()
     plt.subplot(111)
     if len(mbta) > 0:
          plt.scatter(mbta['real'],mbta['est'],c='w',label='MBTA')
     if len(mpls) > 0:
          plt.scatter(mpls['real'],mpls['est'],c='g',label='MPLS')
     if len(bart) > 0:
          plt.scatter(bart['real'],bart['est'],c='b',label='BART')
     if len(lirr) > 0:
          plt.scatter(lirr['real'],lirr['est'],c='r',label='LIRR')
     if len(naipta) > 0:
          plt.scatter(naipta['real'],naipta['est'],c='m',label='NAIPTA')
     if len(sjrtd) > 0:
          plt.scatter(sjrtd['real'],sjrtd['est'],c='y',label='SJRTD')
     if len(valley) > 0:
          plt.scatter(valley['real'],valley['est'],c='c',label='ValleyMetro')
     plt.plot([0,ax_max],[0,ax_max],c='r')
     plt.ylim([0,ax_max])
     plt.xlim([0,ax_max])
     plt.xlabel('Real Ridership')
     plt.ylabel('Estimated Ridership')
     plt.text(ax_max*0.1,ax_max*0.9,'Route Types: %s' % route_type_string)
     plt.text(ax_max*0.1,ax_max*0.85,'Adj. R-Squared: %.3f' % adj_rsquared)
     plt.title(runname)
     plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
           ncol=7, mode="expand", borderaxespad=1.)
     plt.show()
     return

# Retrieve data from a database according to route type and, optionally, 
# according to an inclusive of exclusion list of agencies
def get_all_from_db(db_name, route_type, exclude = 'None', include = 'None'):

     # Retrieve data
     tot_ridership = []
     frequency = []
     num_routes = []
     pop_all = []
     minority_perc = []
     households = []
     house_pov_perc = []
     edu_25plus = []
     edu_hs_perc = []
     edu_col_perc = []
     edu_adv_perc = []
     housing_struct = []
     housing_single_perc = []
     housing_sm_perc = []
     housing_mm_perc = []
     housing_lm_perc = []
     work_pop = []
     work_pop_transit = []
     work_transit_perc = []
     renting = []
     median_age = []
     median_hh_income = []
     median_rent = []
     housing_noveh_perc = []
     jobs_all = []
     jobs_low = []
     jobs_mid = []
     jobs_high = []
     jobs_info = []
     jobs_finance = []
     jobs_real_estate = []
     jobs_prof = []
     jobs_mgmt = []
     jobs_admin = []
     jobs_edu = []
     jobs_health = []
     intersections = []
     ntd_riders = []
     from_stop = []
     to_stop = []
     
     if exclude == 'None' or exclude == []:
          exclude_str = ''
     else:
          exclude_str = '('
          for exc in exclude:
               exclude_str = exclude_str + '\"' + exc + '\", '
          exclude_str = exclude_str[0:len(exclude_str)-2]
          exclude_str = exclude_str + ')'
     
     if include == 'None' or include == []:
          include_str = ''
     else:
          include_str = '('
          for inc in include:
               include_str = include_str + '\"' + inc + '\", '
          include_str = include_str[0:len(include_str)-2]
          include_str = include_str + ')'
     
     
     for rt in route_type:
          # Retrieve fields from correct sources
          conn = sqlite3.connect(db_name)
          db_cursor = conn.cursor()
          
          if len(exclude_str) == 0 and len(include_str) == 0:
               db_cursor.execute('select tot_ridership, frequency, num_routes, pop_all, pop_minority, households, households_poverty, pop_edu_age25plus, pop_edu_hs_or_less, pop_edu_college_or_some, pop_edu_adv_degree, housing_structures, housing_structures_single, housing_structures_small_multi, housing_structures_medium_multi, housing_structures_large_multi, pop_work_16plus, pop_work_16plus_transit, housing_renting, median_age, median_hh_income, median_rent, occupied_housing, occupied_housing_noveh, jobs_all, jobs_low, jobs_mid, jobs_high, jobs_info, jobs_finance, jobs_real_estate, jobs_prof, jobs_mgmt, jobs_admin, jobs_edu, jobs_health, intersections, ntd_riders, from_stop, to_stop from conflated_statistics where route_type = \"%i\"' % rt)
          elif len(exclude_str)  > 0 and len(include_str) == 0:
               db_cursor.execute('select tot_ridership, frequency, num_routes, pop_all, pop_minority, households, households_poverty, pop_edu_age25plus, pop_edu_hs_or_less, pop_edu_college_or_some, pop_edu_adv_degree, housing_structures, housing_structures_single, housing_structures_small_multi, housing_structures_medium_multi, housing_structures_large_multi, pop_work_16plus, pop_work_16plus_transit, housing_renting, median_age, median_hh_income, median_rent, occupied_housing, occupied_housing_noveh, jobs_all, jobs_low, jobs_mid, jobs_high, jobs_info, jobs_finance, jobs_real_estate, jobs_prof, jobs_mgmt, jobs_admin, jobs_edu, jobs_health, intersections, ntd_riders, from_stop, to_stop from conflated_statistics where route_type = \"%i\" and agency not in %s' % (rt,exclude_str))
               
          elif len(exclude_str) == 0 and len(include_str) > 0:
               db_cursor.execute('select tot_ridership, frequency, num_routes, pop_all, pop_minority, households, households_poverty, pop_edu_age25plus, pop_edu_hs_or_less, pop_edu_college_or_some, pop_edu_adv_degree, housing_structures, housing_structures_single, housing_structures_small_multi, housing_structures_medium_multi, housing_structures_large_multi, pop_work_16plus, pop_work_16plus_transit, housing_renting, median_age, median_hh_income, median_rent, occupied_housing, occupied_housing_noveh, jobs_all, jobs_low, jobs_mid, jobs_high, jobs_info, jobs_finance, jobs_real_estate, jobs_prof, jobs_mgmt, jobs_admin, jobs_edu, jobs_health, intersections, ntd_riders, from_stop, to_stop from conflated_statistics where route_type = \"%i\" and agency in %s' % (rt,include_str))
               
          data = db_cursor.fetchall()
          for row in data:
               # 0       ridership
               tot_ridership.append(row[0])
               # 1       frequency
               frequency.append(row[1])
               # 2       num_routes
               num_routes.append(row[2])
               # 3       pop_all
               pop_all.append(row[3])
               # 4       pop_minority
               if row[3] != 0:
                    minority_perc.append(row[4]/row[3])
               else:
                    minority_perc.append(0)
               # 5       households
               households.append(row[5])
               # 6       households_poverty
               if row[5] != 0:
                    house_pov_perc.append(row[6]/row[5])
               else:
                    house_pov_perc.append(0)
               # 7       pop_edu_age25plus
               edu_25plus.append(row[7])
               # 8       pop_edu_hs_or_less
               # 9       pop_edu_college_or_some
               # 10      pop_edu_adv_degree
               if row[7] != 0:
                    edu_hs_perc.append(row[8]/row[7])
                    edu_col_perc.append(row[9]/row[7])
                    edu_adv_perc.append(row[10]/row[7])
               else:
                    edu_hs_perc.append(0)
                    edu_col_perc.append(0)
                    edu_adv_perc.append(0)
               # 11      housing_structures
               housing_struct.append(row[11])
               # 12      housing_structures_single
               # 13      housing_structures_small_multi
               # 14      housing_structures_medium_multi
               # 15      housing_structures_large_multi
               if row[11] != 0:
                    housing_single_perc.append(row[12]/row[11])
                    housing_sm_perc.append(row[13]/row[11])
                    housing_mm_perc.append(row[14]/row[11])
                    housing_lm_perc.append(row[15]/row[11])
               else:
                    housing_single_perc.append(0)
                    housing_sm_perc.append(0)
                    housing_mm_perc.append(0)
                    housing_lm_perc.append(0)
               # 16      pop_work_16plus
               work_pop.append(row[16])
               # 17      pop_work_16plus_transit
               work_pop_transit.append(row[17])
               if row[16] != 0:
                    work_transit_perc.append(row[17]/row[16])
               else:
                    work_transit_perc.append(0)
               # 18      housing_renting
               renting.append(row[18])
               # 19      median_age
               median_age.append(row[19])
               # 20      median_hh_income
               median_hh_income.append(row[20])
               # 21      median_rent
               median_rent.append(row[21])
               # 22      occupied_housing
               # 23      occupied_housing_noveh
               if row[22] != 0:
                    housing_noveh_perc.append(row[23]/row[22])
               else:
                    housing_noveh_perc.append(0)     
               # 24      jobs_all
               jobs_all.append(row[24])
               # 25      jobs_low
               jobs_low.append(row[25])
               # 26      jobs_mid
               jobs_mid.append(row[26])
               # 27      jobs_high
               jobs_high.append(row[27])
               # 28      jobs_info
               jobs_info.append(row[28])
               # 29      jobs_finance
               jobs_finance.append(row[29])
               # 30      jobs_real_estate
               jobs_real_estate.append(row[30])
               # 31      jobs_prof
               jobs_prof.append(row[31])
               # 32      jobs_mgmt
               jobs_mgmt.append(row[32])
               # 33      jobs_admin
               jobs_admin.append(row[33])
               # 34      jobs_edu
               jobs_edu.append(row[34])
               # 35      jobs_health
               jobs_health.append(row[35])
               # 36      intersections
               intersections.append(row[36])
               # 37      ntd_riders
               ntd_riders.append(row[37])
               # 38      from_stop
               from_stop.append(row[38])
               # 39      to_stop
               to_stop.append(row[39])
          
          # Arrange the data
          df = pandas.DataFrame({
          "y": tot_ridership,
          "x1": frequency,
           "x2": num_routes, 
           "x3": pop_all,
           "x4": minority_perc,
           "x5": households,
           "x6": house_pov_perc,
           "x7": edu_25plus,
           "x8": edu_hs_perc,
           "x9": edu_col_perc,
           "x10": edu_adv_perc,
           "x11": housing_struct,
           "x12": housing_single_perc,
           "x13": housing_sm_perc,
           "x14": housing_mm_perc,
           "x15": housing_lm_perc,
           "x16": work_pop,
           "x17": work_pop_transit,
           "x18": work_transit_perc,
           "x19": renting,
           "x20": median_age,
           "x21": median_hh_income,
           "x22": median_rent,
           "x23": housing_noveh_perc,
           "x24": jobs_all,
           "x25": jobs_low,
           "x26": jobs_mid,
           "x27": jobs_high,
           "x28": jobs_info,
           "x29": jobs_finance,
           "x30": jobs_real_estate,
           "x31": jobs_prof,
           "x32": jobs_mgmt,
           "x33": jobs_admin,
           "x34": jobs_edu,
           "x35": jobs_health,
           "x36": intersections,
           "x37": ntd_riders,
           "from_stop": from_stop,
           "to_stop": to_stop})
          
          conn.close()
     
     return(df)

# Produce a summary csv for a model calibration fit
def write_csv_summary(runname, route_type_string, formula_string, adj_rsquared, selected_features, coefficients, pvals):
     with open('%s.csv' % runname,'wb') as csvfile:
          outfile = csv.writer(csvfile,delimiter=',')
          outfile.writerow(['Run Name',runname])
          outfile.writerow(['Modes',route_type_string])
          outfile.writerow(['Formula',formula_string])
          outfile.writerow(['Adj. R-Squared',adj_rsquared])
          outfile.writerow([])
          outfile.writerow(['Element','Coefficient','P-Val'])
          for elem in selected_features:
               outfile.writerow([elem,coefficients[elem],pvals[elem]])
          outfile.writerow(['Intercept',coefficients['Intercept'],pvals['Intercept']])
               
     return

# Using a fitted model, estimate riderhsip by stop-pair
def estimate_ridership(df, selected_features, coefficients):
     estimated = df[['from_stop','to_stop']].copy()
     estimated['est'] = np.zeros((len(estimated),1))
     for elem in selected_features:
          estimated['est'] = estimated['est'] + coefficients[elem]*df[elem]
     estimated['est'] = estimated['est'] + coefficients['Intercept']
     
     estimated['est'] = np.maximum(estimated['est'],0)
     maxval = 1.2 * np.maximum(estimated['est'].max(),df['y'].max())
     
     return {'est':estimated,'max':maxval}

def prepare_model_strings(route_type, selected_features):
     route_type_string = ''
     for rt in route_type:
          route_type_string = route_type_string + ', ' + str(rt)
     route_type_string = route_type_string[2:]
     
     formula_string = 'y ~ '
     first = True
     for elem in selected_features:
          if first:
               formula_string = formula_string + elem
               first = False
          else:
               formula_string = formula_string + ' + ' + elem
     return {'rts':route_type_string,'fs':formula_string}

# Write ridership estimation to a database
def write_sql_summary(database, runname, data):
     conn = sqlite3.connect(database)
     db_cursor = conn.cursor()
     
     outdata = []
     for i in range(0,len(data)):
          outdata.append([data['from_stop'][i],data['to_stop'][i],data['est'][i]])
     
     db_cursor.execute('drop table if exists %s' % runname)
     db_cursor.execute('create table %s (from_stop text, to_stop text, est_riders float)' % runname)
     db_cursor.executemany('insert into %s values (?,?,?)' % runname, outdata)
     conn.commit()
     
     conn.close()
     
     return {'out':outdata,'in':data}

# Perform a model calibration
def calibrate_with_settings(route_type, selected_features, runname, exclude = 'None', include = 'None'):

     prep_strings = prepare_model_strings(route_type, selected_features)
     
     formula_string = prep_strings['fs']
     route_type_string = prep_strings['rts']
     
     df = get_all_from_db('GTFS-ALL.sqlite', route_type, exclude, include)
     
     # The actual model fitting
     result = sm.ols(formula=formula_string,data=df).fit()
     
     coefficients = result.params
     pvals = result.pvalues
     adj_rsquared = result.rsquared_adj
     print result.summary()
     
     # Model Results
     results = estimate_ridership(df, selected_features, coefficients)
     estimated = results['est']
     maxval = results['max']
     
     plot_comparison(list(df['y']),list(estimated['est']),maxval,route_type_string,adj_rsquared, runname)
     
     # Write out results
     write_csv_summary(runname, route_type_string, formula_string, adj_rsquared, selected_features, coefficients, pvals)
     
     # Write estimation to 
     sqlresults = write_sql_summary('GTFS-ALL.sqlite',runname,estimated)
     
     return (sqlresults)

# Find adjusted R-squared statistic
def calculate_adj_rsquared(real, estimated, n, p):
     mean_real = real.mean()
     intermediate = pd.DataFrame()
     intermediate['sstot_inter'] = (real - mean_real)
     intermediate['sstot'] = (intermediate['sstot_inter']**2)
     intermediate['ssres_inter'] = (real - estimated)
     intermediate['ssres'] = (intermediate['ssres_inter']**2)
     ssres = intermediate['ssres'].sum()
     sstot = intermediate['sstot'].sum()
     dfe = n-1
     dft = n-p-1
     adj_r_squared = 1 - ((ssres/dfe) / (sstot/dft))
     return (adj_r_squared)

# Close plots
def clear_plots():
     plt.cla()
     return

# Load a calibration csv
def load_calibration(runname):
     with open('%s.csv' % runname,'rb') as csvfile:
          infile = csv.reader(csvfile,delimiter=',')
          counter = 0
          selected_features = []
          coefficients = pd.Series()
          pvals = pd.Series()
          for row in infile:
               counter += 1
               if counter == 2:
                    route_type_str = row[1]
                    temp = route_type_str.split(',')
                    route_type = []
                    for elem in temp:
                         route_type.append(int(elem))
               elif counter == 3:
                    formula_str = row[1]
               elif counter > 6:
                    if row[0] != 'Intercept':
                         selected_features.append(row[0])
                    coefficients = coefficients.set_value(str(row[0]), float(row[1]))
                    pvals = pvals.set_value(str(row[0]), float(row[2]))
     return {'rts':route_type_str,'fs':formula_str,'selected':selected_features,'coeff':coefficients,'pvals':pvals,'rt':route_type}