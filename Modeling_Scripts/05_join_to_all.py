#------------------------------------------------------------------------------
# Name:        Join to All
#
# Purpose:     This function combines data from all systems into a single 
#              database file for analysis.  Hard-coded NTD total system-mode
#              level ridership are incorporated in this function as well.
#
# Author:      Stephen Zitzow-Childs
#
# Created:     Sprint 2017
# Updated:     7/19/2017
#
# Volpe National Transportation Systems Center
# United States Department of Transportation
#------------------------------------------------------------------------------

import sqlite3

dbs = [['GTFS-BART.sqlite', 'BART'],['GTFS-LIRR.sqlite', 'LIRR'],['GTFS-MBTA.sqlite', 'MBTA'],['GTFS-MPLS.sqlite', 'MPLS'],['GTFS-SJRTD.sqlite', 'SJRTD'],['GTFS-ValleyMetro.sqlite', 'ValleyMetro'],['GTFS-NAIPTA.sqlite', 'NAIPTA']]

conn = sqlite3.connect('GTFS-ALL.sqlite')
cursor_all = conn.cursor()

insert_data = []

for db in dbs:
     print db[0]
     print db[1]
     conn_sub = sqlite3.connect(db[0])
     cursor_sub = conn_sub.cursor()
     agency = db[1]
     
     cursor_sub.execute('select from_stop, to_stop, route_type, tot_ridership, frequency, num_routes, pop_all, pop_minority, households, households_poverty, pop_edu_age25plus, pop_edu_hs_or_less, pop_edu_college_or_some, pop_edu_adv_degree, housing_structures, housing_structures_single, housing_structures_small_multi, housing_structures_medium_multi, housing_structures_large_multi, pop_work_16plus, pop_work_16plus_transit, housing_renting, median_age, median_hh_income, median_rent, occupied_housing, occupied_housing_noveh, jobs_all, jobs_low, jobs_mid, jobs_high, jobs_info, jobs_finance, jobs_real_estate, jobs_prof, jobs_mgmt, jobs_admin, jobs_edu, jobs_health, intersections from conflated_statistics')
     this_data = cursor_sub.fetchall()
     
     for row in this_data:
          new_row = list(row)
          new_row.append(agency)
          insert_data.append(new_row)
          
     conn_sub.close()
     
cursor_all.execute('drop table if exists temp')
cursor_all.execute('create table temp (from_stop text, to_stop text, route_type int, tot_ridership float, frequency int, num_routes int, pop_all float, pop_minority float, households float, households_poverty float, pop_edu_age25plus float, pop_edu_hs_or_less float, pop_edu_college_or_some float, pop_edu_adv_degree float, housing_structures float, housing_structures_single float, housing_structures_small_multi float, housing_structures_medium_multi float, housing_structures_large_multi float, pop_work_16plus float, pop_work_16plus_transit float, housing_renting float, median_age float, median_hh_income float, median_rent float, occupied_housing float, occupied_housing_noveh float, jobs_all float, jobs_low float, jobs_mid float, jobs_high float, jobs_info float, jobs_finance float, jobs_real_estate float, jobs_prof float, jobs_mgmt float, jobs_admin float, jobs_edu float, jobs_health float, intersections float, agency text)')
cursor_all.executemany('insert into temp values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',insert_data)
conn.commit()

# Hard Coded data here from NTD broken by mode
ntd_data = [['MPLS', 0, 2186841],['MPLS', 2, 57384],['MPLS', 3, 5165379],['MBTA', 0, 6201573],['MBTA', 1, 14961417],['MBTA', 2, 2850318],['MBTA', 3, 9506362],['MBTA', 4, 122726],['BART', 1, 11589551],['NAIPTA', 3, 230363],['ValleyMetro', 3, 2921034],['SJRTD', 3, 321469],['LIRR', 2, 8784069]]

cursor_all.execute('drop table if exists ntd_data')
cursor_all.execute('create table ntd_data (agency text, route_type int, ntd_riders float)')
cursor_all.executemany('insert into ntd_data values (?,?,?)',ntd_data)
conn.commit()

# Join in NTD
cursor_all.execute('drop table if exists conflated_statistics')
cursor_all.execute('create table conflated_statistics as select temp.*, ntd_data.ntd_riders from temp inner join ntd_data on ntd_data.agency = temp.agency and ntd_data.route_type = temp.route_type')
conn.commit()

conn.close()