# -*- coding: utf-8 -*-

import sqlite3
import pandas

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
     
     if exclude == 'None':
          exclude_str = ''
     else:
          exclude_str = '('
          for exc in exclude:
               exclude_str = exclude_str + '\"' + exc + '\", '
          exclude_str = exclude_str[0:len(exclude_str)-2]
          exclude_str = exclude_str + ')'
     print exclude_str
     
     if include == 'None':
          include_str = ''
     else:
          include_str = '('
          for inc in include:
               include_str = include_str + '\"' + inc + '\", '
          include_str = include_str[0:len(include_str)-2]
          include_str = include_str + ')'
     print include_str
     
     
     for rt in route_type:
          # Retrieve fields from correct sources
          conn = sqlite3.connect(db_name)
          db_cursor = conn.cursor()
          
          if exclude == 'None' and include == 'None':
               db_cursor.execute('select tot_ridership, frequency, num_routes, pop_all, pop_minority, households, households_poverty, pop_edu_age25plus, pop_edu_hs_or_less, pop_edu_college_or_some, pop_edu_adv_degree, housing_structures, housing_structures_single, housing_structures_small_multi, housing_structures_medium_multi, housing_structures_large_multi, pop_work_16plus, pop_work_16plus_transit, housing_renting, median_age, median_hh_income, median_rent, occupied_housing, occupied_housing_noveh, jobs_all, jobs_low, jobs_mid, jobs_high, jobs_info, jobs_finance, jobs_real_estate, jobs_prof, jobs_mgmt, jobs_admin, jobs_edu, jobs_health, intersections, ntd_riders, from_stop, to_stop from conflated_statistics where route_type = \"%i\"' % rt)
          elif exclude != 'None' and include == 'None':
               db_cursor.execute('select tot_ridership, frequency, num_routes, pop_all, pop_minority, households, households_poverty, pop_edu_age25plus, pop_edu_hs_or_less, pop_edu_college_or_some, pop_edu_adv_degree, housing_structures, housing_structures_single, housing_structures_small_multi, housing_structures_medium_multi, housing_structures_large_multi, pop_work_16plus, pop_work_16plus_transit, housing_renting, median_age, median_hh_income, median_rent, occupied_housing, occupied_housing_noveh, jobs_all, jobs_low, jobs_mid, jobs_high, jobs_info, jobs_finance, jobs_real_estate, jobs_prof, jobs_mgmt, jobs_admin, jobs_edu, jobs_health, intersections, ntd_riders, from_stop, to_stop from conflated_statistics where route_type = \"%i\" and agency not in %s' % (rt,exclude_str))
               
          elif exclude == 'None' and include != 'None':
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