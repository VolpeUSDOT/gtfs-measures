#------------------------------------------------------------------------------
# Name:        GTFSLoader
#
# Purpose:     This function (possibly redundant) reloads stops.txt GTFS data
#              manually to fix issues with the pygtfs loading algorithm 
#              found in 00_gtfs_ingest_sqlite.  Specifically, parent stations 
#              are stripped in the ingester function, and are required for
#              certain analyses within this framework.
#
# Author:      Stephen Zitzow-Childs
#
# Created:     Winter 2016
# Updated:     7/19/2017
#
# Volpe National Transportation Systems Center
# United States Department of Transportation
#------------------------------------------------------------------------------

import csv

# Loader for the stops.txt data
def stops_gtfs(folder):
    print '... Loading Stops from GTFS'
    fullpath = folder + 'stops.txt'
    with open(fullpath,'rb') as csvfile:
        infile = csv.reader(csvfile,delimiter=',')
        counter = -1

        c_sid = -1
        c_code = -1
        c_name = -1
        c_desc = -1
        c_lat = -1
        c_lon = -1
        c_loc_type = -1
        c_parent = -1
        
        stops = []
        for row in infile:
            if counter == -1:
                # Identify the mapping, in case the columns are in different orders
                for c in range(len(row)):
                    if row[c] == 'stop_id':
                        c_sid = c
                    elif row[c] == 'stop_code':
                        c_code = c
                    elif row[c] == 'stop_name':
                        c_name = c
                    elif row[c] == 'stop_desc':
                        c_desc = c
                    elif row[c] == 'stop_lat':
                        c_lat = c
                    elif row[c] == 'stop_lon':
                        c_lon = c
                    elif row[c] == 'location_type':
                        c_loc_type = c
                    elif row[c] == 'parent_station':
                        c_parent = c
                    # Else, unused column, do nothing
                        
                if (c_sid == -1 or c_code == -1 or c_name == -1 or c_desc == -1 or
                    c_lat == -1 or c_lon == -1 or c_loc_type == -1 or c_parent == -1):
                    print 'Error in loading stops: could not find all necessary columns'
                    return ([])
            else:
                # With the mapping, create the features and add them to a list
                stops.append(tuple([row[c_sid],row[c_code],row[c_name],row[c_desc],
                                              row[c_lat],row[c_lon],row[c_loc_type],row[c_parent]]))
            counter += 1
    return (stops)