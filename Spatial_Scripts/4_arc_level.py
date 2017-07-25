#-------------------------------------------------------------------------------
# Name:    Arc_Level
#
# Purpose: Associate GTFS route data with the Arnold road network
#
# Author:  Alex Oberg and Gary Baker
#
# Created: 12/5/2016
#
# Last updated 6/15/2017
#-------------------------------------------------------------------------------

# CONFIG
#-------------------------------------------------------------------------------

#date: Must identify a date in 'yyyy-mm-dd' format that represents a service date of interest. User must examine GTFS feed calendar to choose an appropriate date that is covered by feed.
#day_of_week: Must identify a day of week that represents a service day of interest (e.g. Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, or Sunday)


#MN MODEL
sqlite_file      = r"C:\tasks\2016_09_12_GTFS_ingest\Model\MPLS\GTFS-MPLS.sqlite"
actual_ridership_csv        = r"C:\tasks\2016_09_12_GTFS_ingest\Model\MPLS\weighted_real_ridership--MPLS.csv"
modeled_ridership_csv       = r"C:\tasks\2016_09_12_GTFS_ingest\Model\MPLS\weighted_estimated_ridership--calib_02d_busonly_significant3_for_MPLS--MPLS.csv"
date             = '2016-10-05'
day_of_week      = 'Wednesday'
output_dir       = r"C:\tasks\2016_09_12_GTFS_ingest\Model\MPLS\Output"


# SETUP
#-------------------------------------------------------------------------------

import datetime
import arcpy
import os
import sqlite3
import csv

start_time = datetime.datetime.now()
print('\nStart at ' + str(start_time))
print "Started Step 4: ARNOLD Segment Frequency Calculations"
print "GTFS database being processed: " + sqlite_file


output_gdb = "gtfs_arnold_final.gdb"
full_path_to_output_gdb = os.path.join(output_dir, output_gdb)
arcpy.env.workspace = full_path_to_output_gdb

# NEAR STOPS TO NETWORK JUNCTIONS
# -----------------------------------------------------------------------
print "Identifying stops that are at an ARNOLD intersection..."

arcpy.Near_analysis(
        in_features=os.path.join(full_path_to_output_gdb, "stops_lrs"),
        near_features="network\\network_ND_Junctions",
        search_radius="10 Meters",
        location="NO_LOCATION",
        angle="NO_ANGLE",
        method="PLANAR"
        )

# READ IN STOPS (ALL_BUS_SHAPE_ID_PT) AND MAKE DICT MAPPING FROM OID TO SOURCEOID WHERE SOURCEID = 2 (NODE)
# -------------------------------------------------------------------------------------------------------

stop_oid_to_net_jct_id_dict = {}

with arcpy.da.SearchCursor("stops", ["OID@", "SourceID", "SourceOID"]) as scursor:
    for row in scursor:
        stops_oid, network_source_id, network_source_oid = row

        if network_source_id == 2:
            stop_oid_to_net_jct_id_dict[stops_oid] = network_source_oid


#for k, v in stop_oid_to_net_jct_id_dict.iteritems():
    #print '{}, {}'.format(k, v)

#sys.exit()

# use the above to make a single net jct lookup
# -------------------------------------------------------------------------------------------------------

net_jct_lookup = {}

with arcpy.da.SearchCursor("traversedJunctions", ["OID@", "SourceName", "SourceOID"]) as scursor:
    for row in scursor:

        junction_id, network_source_name, network_source_oid = row

        if network_source_name == 'Stops':
            if network_source_oid in stop_oid_to_net_jct_id_dict:
                net_jct_lookup[junction_id] = stop_oid_to_net_jct_id_dict[network_source_oid]

        elif network_source_name == 'network_ND_Junctions':
            net_jct_lookup[junction_id] = network_source_oid

del stop_oid_to_net_jct_id_dict

# -------------------------------------------------------------------------------------------------------

stops_dict = {}

in_features = os.path.join(full_path_to_output_gdb, "stops_lrs")

flds = ['NEAR_FID', 'STOP_ID']

with arcpy.da.SearchCursor(in_features, flds) as scursor:
    for row in scursor:

        near_fid, stop_id = row

        if near_fid not in stops_dict:
            stops_dict[near_fid] = [stop_id]
        elif stop_id not in stops_dict[near_fid]:
            stops_dict[near_fid].append(stop_id)


# -------------------------------------------------------------------------------------------------------

print "Creating Object ID to route shape name dictionary..."

# Create dictionary linking the object ID to the actual route shape name (needed to link traversed edges to the solved route, and solved routes to their component trips)
route_shape_dict = {}

in_features = os.path.join(full_path_to_output_gdb, "route_results")

flds = ['OBJECTID', 'NAME']

with arcpy.da.SearchCursor(in_features, flds) as scursor:
    for row in scursor:

        object_id, route_shape = row

        route_shape_dict[object_id] = route_shape


# -------------------------------------------------------------------------------------------------------

print "Creating list of traversed ARNOLD segments and stops for each route shape..."

route_shape_traversed_dict = {}

flds = ['SourceOID', 'RouteID', 'FromPosition', 'ToPosition', 'FromJunctionId', 'ToJunctionId']

for object_id, route_shape in route_shape_dict.items():
    with arcpy.da.SearchCursor("TraversedEdges", flds) as scursor:

        #print str(object_id) + "\nProcessing route shape ID: " + str(route_shape)

        result = []

        arclist = []
        last_arc = None
        current_from = None

        for row in scursor:

            arc_oid, route_id, fr_pos, to_pos, fr_jct_id, to_jct_id = row

            if route_id == object_id:

                fr_netjct = '' if not fr_jct_id in net_jct_lookup else net_jct_lookup[fr_jct_id]
                to_netjct = '' if not to_jct_id in net_jct_lookup else net_jct_lookup[to_jct_id]

                fr_stop_id = stops_dict.get(fr_netjct)
                if fr_stop_id == None:
                    fr_stop_id = ''

                to_stop_id = stops_dict.get(to_netjct)
                if to_stop_id == None:
                    to_stop_id = ''

                #print '{:<5}  {:<5}  {:<5}'.format(arc_oid, fr_stop_id, to_stop_id)

                if fr_stop_id and to_stop_id:
                    result.append([(fr_stop_id, to_stop_id), [arc_oid]])
                elif fr_stop_id:
                    arclist = [arc_oid]
                    current_from = fr_stop_id
                elif to_stop_id:
                    if arc_oid != last_arc:
                        arclist.append(arc_oid)
                    if not current_from == None:
                        result.append([(current_from, to_stop_id), arclist])
                    current_from = None
                else:
                    if arc_oid != last_arc:
                        arclist.append(arc_oid)

                last_arc = arc_oid
        #print "RESULT: " + str(result)
        route_shape_traversed_dict[route_shape] = result


#print route_shape_traversed_dict

print "Integrating ridership data..."

def sum_ridership(route_shape_traversed_dict, ridership_result_dict, route_shape_csv, from_stop_id_csv, to_stop_id_csv, ridership_csv):
    if route_shape_csv in route_shape_traversed_dict:
        data_for_route_shape = route_shape_traversed_dict[route_shape_csv]


        # LOOK FOR FROM STOP

        from_stop_index = None

        for i, from_to_info in enumerate(data_for_route_shape):
            if from_stop_id_csv in from_to_info[0][0]:
                from_stop_index = i


        # LOOK FOR TO STOP

        to_stop_index = None

        for i, from_to_info in enumerate(data_for_route_shape):
            if to_stop_id_csv in from_to_info[0][1]:
                to_stop_index = i


        if from_stop_index and to_stop_index:

            for i in range(from_stop_index, to_stop_index + 1):

                for arc_oid in data_for_route_shape[i][1]:
                    if not arc_oid in ridership_result_dict:
                        ridership_result_dict[arc_oid] = ridership_csv
                    else:
                        ridership_result_dict[arc_oid] += ridership_csv


        else:
            print ("Stop Pair " + from_stop_id_csv + ", " + to_stop_id_csv + " in shape id" + route_shape_csv + " could not be located.")
    else:
        print (route_shape_csv + " could not be located in dataset")

# ========================================================
# main ridership calculations (both actual ridership from agency and modeled ridership from model)

actual_ridership_result_dict = {}
modeled_ridership_result_dict = {}

print "Integrating actual ridership data..."

with open(actual_ridership_csv, 'rb') as f:
    next(f)
    mycsv = csv.reader(f)
    for line in mycsv:
        #print line
        sum_ridership(route_shape_traversed_dict, actual_ridership_result_dict, str(line[3]), line[0], line[1], float(line[2]))

print "Integrating modeled ridership data..."

with open(modeled_ridership_csv, 'rb') as f:
    next(f)
    mycsv = csv.reader(f)
    for line in mycsv:
        #print line
        sum_ridership(route_shape_traversed_dict, modeled_ridership_result_dict, str(line[3]), line[0], line[1], float(line[2]))

### USE RESULT (put in output file)
##
##out_ridership = os.path.join(output_dir, 'ridership.txt')
##with open(out_ridership, 'w') as wf:
##    wf.write("ARC_OID,Ridership\n")
##
##with open(out_ridership, 'a') as wf:
##
##        for k,v in ridership_result_dict.items():
##
##            wf.write('{},{}\n'.format(k, v))



## Connecting to the database file
print "Aggregating trips..."

trip_aggregator = {}

con = sqlite3.connect(sqlite_file)

# TO DO DOUBLE CHECK SQL
trips_fetch = '''
select a.trip_id, a.shape_id
from trips a
inner join calendar b on a.service_id = b.service_id
inner join stop_times c on a.trip_id = c.trip_id
inner join routes d on a.route_id = d.route_id
where start_date<='__date__' AND end_date>='__date__' AND __day_of_week__ = 1 AND d.route_type = 3
group by a.trip_id, a.shape_id
'''

trips_fetch = trips_fetch.replace('__date__', (date))
trips_fetch = trips_fetch.replace('__day_of_week__', (day_of_week))

cur_trips = con.cursor()

for trip_row in cur_trips.execute(trips_fetch):

    trip_id, shape_id = trip_row

    if shape_id not in trip_aggregator:
        trip_aggregator[shape_id] = 1
    else:
        trip_aggregator[shape_id] +=1

#print trip_aggregator

# prepare the output file
# -----------------------

out_trip_aggregator = os.path.join(output_dir, 'trip_aggregator.txt')
with open(out_trip_aggregator, 'w') as wf:
    wf.write("Route_Shape_ID,Frequency\n")

with open(out_trip_aggregator, 'a') as wf:

        for k,v in trip_aggregator.items():

            wf.write('{},{}\n'.format(k, v))


# SET UP ARC ENV AND CREATE THE FLOW RESULT LAYER
    # --------------------------------------------------------------------

print "Creating flow result..."


ALBERS_PRJ = arcpy.SpatialReference(102039)

if arcpy.Exists("flow_result"):
    arcpy.Delete_management("flow_result")
    print "Deleted existing flow result..."

arcpy.CreateFeatureclass_management(
    full_path_to_output_gdb, "flow_result", "polyline", "", "DISABLED", "DISABLED", ALBERS_PRJ)

arcpy.AddField_management("flow_result", 'ARCID', 'long')
arcpy.AddField_management("flow_result", 'FLOW', 'long')
arcpy.AddField_management("flow_result", 'AADT', 'long')
arcpy.AddField_management("flow_result", 'Actual_Ridership', 'Double')
arcpy.AddField_management("flow_result", 'Modeled_Ridership', 'Double')

# TRANSLATE list of links to dictionary with frequency
# ------------------------------

print "Aggregating at the link level ..."

link_aggregator = {}

for k1, v1 in trip_aggregator.items():

    for k2,v2 in route_shape_traversed_dict.items():

        if k1 == k2:
            for tuple in v2:
                for link in tuple[1]:
                    #print link
                    if link not in link_aggregator:
                        link_aggregator[link] = v1
                    else:
                        link_aggregator[link] += v1

#print link_aggregator

# WRITING FINAL LAYER
# -------------------

print "Writing results to flow result layer ..."

flds = ['OBJECTID', 'OID@', 'SHAPE@', 'AADT_VN']

with arcpy.da.SearchCursor('network/arnold_split_nw', flds) as scursor:

    for row in scursor:

        arcid, oid, shape, aadt = row

        if arcid in link_aggregator:
            #print arcid

            flow = link_aggregator[arcid]
            #print flow

            # Get actual_ridership data in the result
            if arcid in actual_ridership_result_dict:
                actual_ridership = actual_ridership_result_dict[arcid]
            else:
                actual_ridership = None

            # Get modeled_ridership data in the result
            if arcid in modeled_ridership_result_dict:
                modeled_ridership = modeled_ridership_result_dict[arcid]
            else:
                modeled_ridership = None

            with arcpy.da.InsertCursor("flow_result", ['SHAPE@', 'ARCID', 'FLOW', 'AADT', 'Actual_Ridership', 'Modeled_Ridership']) as icursor:

                icursor.insertRow([shape, arcid, flow, aadt, actual_ridership, modeled_ridership])


arcpy.AddField_management("flow_result", 'Multimodal_Users', 'Double')
arcpy.CalculateField_management("flow_result", "Multimodal_Users", "!AADT! * 1.6", "PYTHON_9.3")

arcpy.AddField_management("flow_result", 'Actual_Ridership_Multimodal_Users_Ratio', 'Double')
arcpy.CalculateField_management("flow_result", "Actual_Ridership_Multimodal_Users_Ratio", "!Actual_Ridership! / !Multimodal_Users!", "PYTHON_9.3")

arcpy.AddField_management("flow_result", 'Modeled_Actual_Ridership_Ratio', 'Double')
arcpy.CalculateField_management("flow_result", "Modeled_Actual_Ridership_Ratio", "!Modeled_Ridership! / !Actual_Ridership!", "PYTHON_9.3")

arcpy.AddField_management("flow_result", 'Miles', 'Double')
arcpy.CalculateField_management("flow_result", "Miles", "!SHAPE_LENGTH! / 1609.34", "PYTHON_9.3")

arcpy.AddField_management("flow_result", 'VMT', 'Double')
arcpy.CalculateField_management("flow_result", "VMT", "!FLOW! * !Miles!", "PYTHON_9.3")

arcpy.AddField_management("flow_result", 'Actual_PMT', 'Double')
arcpy.CalculateField_management("flow_result", "Actual_PMT", "!Actual_Ridership! * !Miles!", "PYTHON_9.3")

arcpy.AddField_management("flow_result", 'Modeled_PMT', 'Double')
arcpy.CalculateField_management("flow_result", "Modeled_PMT", "!Modeled_Ridership! * !Miles!", "PYTHON_9.3")

if arcpy.Exists(os.path.join(output_dir, 'vmt_pmt_system_summary.txt')):
    arcpy.Delete_management(os.path.join(output_dir, 'vmt_pmt_system_summary.txt'))
    print "Deleted existing summary statistics..."

arcpy.Statistics_analysis("flow_result", os.path.join(output_dir, 'vmt_pmt_system_summary.txt'), [["Miles", "SUM"], ["VMT", "SUM"], ["Actual_PMT", "SUM"], ["Modeled_PMT", "SUM"]])

end_time = datetime.datetime.now()
total_time = end_time - start_time
print ("\nEnd at {}.  Total run time {}".format(end_time, total_time))
