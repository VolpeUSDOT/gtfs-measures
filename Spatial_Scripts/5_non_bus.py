#-------------------------------------------------------------------------------
# Name:    GTFS_Non_Bus_Processing
#
# Purpose: Prepare GTFS non-bus data for stop to stop calculations and mapping
#
# Author:  Alex Oberg and Gary Baker
#
# Created: 2/15/2017
#
# Last updated 6/15/2017
#-------------------------------------------------------------------------------

# CONFIG
#-------------------------------------------------------------------------------

template_network = r"c:\tasks\2016_09_12_GTFS_ingest\network_template\network_template_non_bus.gdb\network"

#MBTA MODEL
sqlite_file      = r"C:\tasks\2016_09_12_GTFS_ingest\Model\MBTA\GTFS-MBTA.sqlite"
actual_ridership_csv        = r"C:\tasks\2016_09_12_GTFS_ingest\Model\MBTA\weighted_real_ridership--MBTA.csv"
modeled_ridership_csv       = r"C:\tasks\2016_09_12_GTFS_ingest\Model\MBTA\modeled_ridership_placeholder.csv"
date             = '2016-10-05'
day_of_week      = 'Wednesday'
output_dir       = r"c:\tasks\2016_09_12_GTFS_ingest\Model\MBTA\output"

# SETUP
#-------------------------------------------------------------------------------

import datetime
import sqlite3
import arcpy
import os
import csv

start_time = datetime.datetime.now()
print('\nStart at ' + str(start_time))
print "Started Step 5: Processing non-bus route shapes"
print "GTFS database being processed: " + sqlite_file

output_gdb = "gtfs_non_road.gdb"
full_path_to_output_gdb = os.path.join(output_dir, output_gdb)

# GTFS lat/lon are written in WGS1984 coordinates
WGS84 = arcpy.SpatialReference(4326)

# Script projects data into USA Contiguous Albers Equal Area Conic USGS coordinate system
ALBERS_PRJ = arcpy.SpatialReference(102039)

if arcpy.Exists(full_path_to_output_gdb):
    arcpy.Delete_management(full_path_to_output_gdb)
    print "Deleted existing non-bus geodatabase..."

arcpy.CreateFileGDB_management(output_dir, output_gdb)

arcpy.env.workspace = full_path_to_output_gdb

print "Preparing non-bus route shapes ..."

# GET ALL NON-BUS SHAPE ID POINTS AND ADD THEM TO A LAYER
#-------------------------------------------------------------------------------

arcpy.management.CreateFeatureclass(full_path_to_output_gdb, "all_non_bus_shape_id_pt_tmp", "POINT", spatial_reference=WGS84)
arcpy.AddField_management("all_non_bus_shape_id_pt_tmp", "ID", "TEXT", "40")
arcpy.AddField_management("all_non_bus_shape_id_pt_tmp", "route_id", "TEXT", "100")
arcpy.AddField_management("all_non_bus_shape_id_pt_tmp", "feed_id", "LONG")
arcpy.AddField_management("all_non_bus_shape_id_pt_tmp", "shape_id", "TEXT", "40")
arcpy.AddField_management("all_non_bus_shape_id_pt_tmp", "lat", "DOUBLE")
arcpy.AddField_management("all_non_bus_shape_id_pt_tmp", "long", "DOUBLE")
arcpy.AddField_management("all_non_bus_shape_id_pt_tmp", "VisitOrder", "LONG")
arcpy.AddField_management("all_non_bus_shape_id_pt_tmp", "shape_dist_traveled", "DOUBLE")
arcpy.AddField_management("all_non_bus_shape_id_pt_tmp", "route_type", "LONG")

print "Fetching all non-bus shape ID point data from GTFS feed ..."

# Connecting to the database file
conn = sqlite3.connect(sqlite_file)
cursor = conn.cursor()

# Fetch shape ID points, rounding lat/longs to six places. CURRENTLY ONLY BUSES
shapes_fetch = '''
    SELECT
        t.route_id,
        shapes.feed_id,
	shapes.shape_id,
    ROUND(shapes.shape_pt_lat, 6) as shape_pt_lat,
	ROUND(shapes.shape_pt_lon, 6) as shape_pt_lon,
	shapes.shape_pt_sequence,
	shapes.shape_dist_traveled,
    r.route_type
    FROM shapes
    INNER JOIN (
    	SELECT shape_id, route_id
	FROM trips
	GROUP BY shape_id, route_id
	) t ON t.shape_id = shapes.shape_id
    INNER JOIN (
	SELECT route_type, route_id
	FROM routes
	WHERE route_type <> 3	GROUP BY route_id
	) r ON r.route_id = t.route_id;
'''

cursor.execute(shapes_fetch)
shape_list = cursor.fetchall()  # TODO dangerous
conn.close

print "All non-bus shape ID list contains {} points".format(len(shape_list))

shape_seg_flds = ["shape@", "ID", "route_id", "feed_id", "shape_id", "lat", "long", "VisitOrder", "shape_dist_traveled", "route_type"]
shape_insert_cursor = arcpy.da.InsertCursor("all_non_bus_shape_id_pt_tmp", shape_seg_flds)

# Create point out of lat long/fields and insert geometry and fields into feature class.
# also create an ID field concatanating lat:lon.

print "Creating all non-bus shape ID point feature class ..."

shape_point = arcpy.Point()

for shape in shape_list:
    shape_point.X = float(shape[4])
    shape_point.Y = float(shape[3])
    shape_point_geom = arcpy.PointGeometry(shape_point, WGS84)
    shape_insert_cursor.insertRow([shape_point_geom, str(shape[3])+":"+str(shape[4]), shape[0], shape[1], shape[2], shape[3], shape[4], shape[5], shape[6], shape[7]])

del shape_insert_cursor


# PREPARE THE ALL BUS SHAPES LAYER
# ----------------------------

print "Projecting all bus GTFS shape ID points ..."
arcpy.Project_management("all_non_bus_shape_id_pt_tmp", "all_non_bus_shape_id_pt", ALBERS_PRJ)

arcpy.PointsToLine_management("all_non_bus_shape_id_pt",
                              "non_bus_route_shapes_tmp",
                              "shape_id", "VisitOrder")

arcpy.JoinField_management("non_bus_route_shapes_tmp", "shape_id", "all_non_bus_shape_id_pt", "shape_ID", ["Route_Type"])

arcpy.MakeFeatureLayer_management ("non_bus_route_shapes_tmp", "non_bus_route_shapes_lyr")

# Integrate every route type separately (e.g. you don't want commuter rail integrated with subway)

print "Integrating non-bus route shapes ..."

merge_list = []

arcpy.SelectLayerByAttribute_management ("non_bus_route_shapes_lyr", "NEW_SELECTION", "Route_Type = 0")
count = arcpy.GetCount_management("non_bus_route_shapes_lyr")
if count > 0:
    arcpy.Integrate_management("non_bus_route_shapes_lyr", "15 meters")
    # Split at junctions as well
    arcpy.FeatureToLine_management("non_bus_route_shapes_lyr", "non_bus_route_shapes_tram", "0.001 meters")
    merge_list.append("non_bus_route_shapes_tram")

arcpy.SelectLayerByAttribute_management ("non_bus_route_shapes_lyr", "NEW_SELECTION", "Route_Type = 1")
count = arcpy.GetCount_management("non_bus_route_shapes_lyr")
if count > 0:
    arcpy.Integrate_management("non_bus_route_shapes_lyr", "15 meters")
    # Split at junctions as well
    arcpy.FeatureToLine_management("non_bus_route_shapes_lyr", "non_bus_route_shapes_subway", "0.001 meters")
    merge_list.append("non_bus_route_shapes_subway")

arcpy.SelectLayerByAttribute_management ("non_bus_route_shapes_lyr", "NEW_SELECTION", "Route_Type = 2")
count = arcpy.GetCount_management("non_bus_route_shapes_lyr")
if count > 0:
    arcpy.Integrate_management("non_bus_route_shapes_lyr", "15 meters")
    # Split at junctions as well
    arcpy.FeatureToLine_management("non_bus_route_shapes_lyr", "non_bus_route_shapes_rail", "0.001 meters")
    merge_list.append("non_bus_route_shapes_rail")

arcpy.SelectLayerByAttribute_management ("non_bus_route_shapes_lyr", "NEW_SELECTION", "Route_Type = 4")
count = arcpy.GetCount_management("non_bus_route_shapes_lyr")
if count > 0:
    arcpy.Integrate_management("non_bus_route_shapes_lyr", "15 meters")
    # Split at junctions as well
    arcpy.FeatureToLine_management("non_bus_route_shapes_lyr", "non_bus_route_shapes_ferry", "0.001 meters")
    merge_list.append("non_bus_route_shapes_ferry")

arcpy.SelectLayerByAttribute_management ("non_bus_route_shapes_lyr", "NEW_SELECTION", "Route_Type = 5")
count = arcpy.GetCount_management("non_bus_route_shapes_lyr")
if count > 0:
    arcpy.Integrate_management("non_bus_route_shapes_lyr", "15 meters")
    # Split at junctions as well
    arcpy.FeatureToLine_management("non_bus_route_shapes_lyr", "non_bus_route_shapes_cable_car", "0.001 meters")
    merge_list.append("non_bus_route_shapes_cable_car")

arcpy.SelectLayerByAttribute_management ("non_bus_route_shapes_lyr", "NEW_SELECTION", "Route_Type = 6")
count = arcpy.GetCount_management("non_bus_route_shapes_lyr")
if count > 0:
    arcpy.Integrate_management("non_bus_route_shapes_lyr", "15 meters")
    # Split at junctions as well
    arcpy.FeatureToLine_management("non_bus_route_shapes_lyr", "non_bus_route_shapes_gondola", "0.001 meters")
    merge_list.append("non_bus_route_shapes_gondola")

arcpy.SelectLayerByAttribute_management ("non_bus_route_shapes_lyr", "NEW_SELECTION", "Route_Type = 7")
count = arcpy.GetCount_management("non_bus_route_shapes_lyr")
if count > 0:
    arcpy.Integrate_management("non_bus_route_shapes_lyr", "15 meters")
    # Split at junctions as well
    arcpy.FeatureToLine_management("non_bus_route_shapes_lyr", "non_bus_route_shapes_funicular", "0.001 meters")
    merge_list.append("non_bus_route_shapes_funicular")

arcpy.Merge_management(merge_list, "non_bus_route_shapes")

#Clean up layers
arcpy.Delete_management("non_bus_route_shapes_tmp")
if arcpy.Exists("non_bus_route_shapes_tram"):
    arcpy.Delete_management("non_bus_route_shapes_tram")
if arcpy.Exists("non_bus_route_shapes_subway"):
    arcpy.Delete_management("non_bus_route_shapes_subway")
if arcpy.Exists("non_bus_route_shapes_rail"):
    arcpy.Delete_management("non_bus_route_shapes_rail")
if arcpy.Exists("non_bus_route_shapes_cable_car"):
    arcpy.Delete_management("non_bus_route_shapes_cable_car")
if arcpy.Exists("non_bus_route_shapes_gondola"):
    arcpy.Delete_management("non_bus_route_shapes_gondola")
if arcpy.Exists("non_bus_route_shapes_funicular"):
    arcpy.Delete_management("non_bus_route_shapes_funicular")

arcpy.CreateRoutes_lr("non_bus_route_shapes", "shape_id", "non_bus_routes", "LENGTH", "#", "#", "LOWER_LEFT")

# Join route_type field back into routes feature class
arcpy.JoinField_management("non_bus_routes", "shape_id", "non_bus_route_shapes", "shape_id", ["route_type"])


# SPLIT AT STOPS

con = sqlite3.connect(sqlite_file)


# Prepare the output file
# -----------------------

out_lrs_file = os.path.join(output_dir, 'non_bus_rtshp_lr_stops.txt')
with open(out_lrs_file, 'w') as wf:
    wf.write("ROUTE_SHAPE,MP,STOP_ID\n")

#Add dummy values so ArcGIS doesn't mis-identify the field types
with open(out_lrs_file, 'a') as wf:
    wf.write("randomtext,0.00,randomtext2\nrandomtext,0.00,randomtext3\nrandomtext,0.00,randomtext4\nrandomtext,0.00,randomtext5\n")



# FOR EACH ROUTE SHAPE ID (AKA CONSTRUCTED ROUTE)
# -----------------------------------------
print "Retrieving stops for each route shape ID..."

sql_shape = '''
select distinct shape_id
from  trips t
join routes r on t.route_id = r.route_id
where r.route_type <> 3 AND shape_id <> ""
'''

cur_shape_id = con.cursor()

for shape_row in cur_shape_id.execute(sql_shape):
    #Cast as string otherwise non-numeric characters in shape_ID can cause many issues (e.g. some can come across as scientic notation).
    shape_id = str(shape_row[0])
    #print 'processing shape id {}'.format(shape_id)

    # GET THE THE CONSTRUCTED ROUTE GEOMETRY FOR THE current ROUTE SHAPE ID
    # --------------------------------------------------------
    arcpy.MakeFeatureLayer_management ("non_bus_routes", "non_bus_routes_lyr")
    route_results_query = 'shape_id = \'{}\''.format(shape_id)
    arcpy.SelectLayerByAttribute_management ("non_bus_routes_lyr", "NEW_SELECTION", route_results_query)

    if int(arcpy.GetCount_management("non_bus_routes_lyr").getOutput(0)) != 1:
        print 'Can''t process route shape {} because it doesn''t have a single geography'.format(shape_id)

    route_geometry = None
    with arcpy.da.SearchCursor("non_bus_routes_lyr", ["SHAPE@"]) as scursor:
        row = scursor.next()
        route_geometry = row[0]



    # All stops every seen on the current route shape
    # ------------------------------------------------
    #Note that tick marks have to be added to __SHAPE_ID__ to work with shape IDs that contain text.
    sql_stops = '''
        select stop_id, stop_lat, stop_lon
        from stops
        where stop_id in (
            select distinct stop_id
            from stop_times
            where trip_id in (
                select trip_id from trips where shape_id = '__SHAPE_ID__'
            )
        )
        '''

    sql_stops = sql_stops.replace('__SHAPE_ID__', (shape_id))

    #print sql_stops


    with open(out_lrs_file, 'a') as wf:

        point = arcpy.Point()

        cur_stops = con.cursor()
        for stop_row in cur_stops.execute(sql_stops):

            stop_id, stop_lat, stop_lon = stop_row

            #print '\n{}, {}, {}'.format(stop_id, stop_lat, stop_lon)

            point.X = stop_lon
            point.Y = stop_lat

            point_geom = arcpy.PointGeometry(point, WGS84).projectAs(ALBERS_PRJ)

            result = route_geometry.queryPointAndDistance(point_geom, False)

            #print result

            result_geom = result[0]   # TODO make layer from this for use in itegrate step below
            #Adding code to deal with milepost rounding issue
            if result[1] <> 0:
                milepost  = result[1]-.01
            else:
                milepost = result[1]

            wf.write('{},{:.2f},{}\n'.format(shape_id, milepost, stop_id))


# Linear reference the stops
print "Linear referencing the stops with the route results..."

arcpy.MakeRouteEventLayer_lr ("non_bus_routes", "shape_id", out_lrs_file, "ROUTE_SHAPE POINT MP", "stop_events", "", "ERROR_FIELD")

# Create a layer from them
arcpy.CopyFeatures_management("stop_events", "stops_lrs_temp")
arcpy.MakeFeatureLayer_management ("stops_lrs_temp", "stops_lrs_temp_lyr")
arcpy.SelectLayerByAttribute_management(in_layer_or_view="stops_lrs_temp_lyr", selection_type="NEW_SELECTION", where_clause="ROUTE_SHAPE <> 'randomtext'")
arcpy.CopyFeatures_management("stops_lrs_temp_lyr", "stops_lrs")
arcpy.Delete_management("stops_lrs_temp")

# Combine stops together that are within a certain distance of each other
print "Integrating stops that are near each other..."
arcpy.Integrate_management(in_features="stops_lrs #", cluster_tolerance="3 Meters")

# Split network by those integrated points (TODO segregate network that had routes from network that didn't and only split them?)
print "Splitting network at stops..."

arcpy.SplitLineAtPoint_management("non_bus_routes","stops_lrs","non_bus_routes_split","1 Meters")

# Dissolve to remove duplicate geometries

arcpy.AddGeometryAttributes_management("non_bus_routes_split", "LENGTH;LINE_START_MID_END")

arcpy.Dissolve_management("non_bus_routes_split", "non_bus_network",
                         ["MID_X", "MID_Y", "LENGTH", "route_type"], "", "SINGLE_PART",
                          "DISSOLVE_LINES")


# PREPARE THE NETWORK
#-------------------------------------------------------------------------------

print "Preparing network ..."

# Copy template network dataset into working file geodatabase
arcpy.Copy_management(template_network, os.path.join(full_path_to_output_gdb, "network"), "FeatureDataset")

edit = arcpy.da.Editor(full_path_to_output_gdb)
edit.startEditing(False, False)
edit.startOperation()

# Delete all existing network segments from template
with arcpy.da.UpdateCursor("network/non_bus_nw", "OBJECTID") as cursor:
    for row in cursor:
        cursor.deleteRow()

edit.stopOperation()
edit.stopEditing(True)


# LOAD DATA TO NETWORK DATSET, BUILD, AND CALCULATE NETWORK LOCATIONS FOR BUS GTFS SHAPE POINTS
#-------------------------------------------------------------------------------

print "Adding non-road data to network ..."
arcpy.Append_management("non_bus_network", "network/non_bus_nw", "NO_TEST")

print "Building network ..."
arcpy.CheckOutExtension("Network")
arcpy.BuildNetwork_na ("network/network_ND")


# SETUP AND SOLVE THE ROUTE
# --------------------------

arcpy.CheckOutExtension("Network")

print "Preparing route layer ..."
routeLayer = arcpy.na.MakeRouteLayer("network/network_ND", "Routes", "Length").getOutput(0)

arcpy.AddLocations_na( \
    in_network_analysis_layer=routeLayer, \
    sub_layer="Stops", \
    in_table="stops_lrs", \
    field_mappings="RouteName ROUTE_SHAPE #;Name Name #", \
    search_tolerance="5000 Meters", \
    sort_field="MP", \
    search_criteria="non_bus_nw SHAPE;network_ND_Junctions NONE", \
    match_type="MATCH_TO_CLOSEST", \
    append="CLEAR", \
    snap_to_position_along_network="SNAP", \
    snap_offset="5 Meters", \
    exclude_restricted_elements="INCLUDE", \
    search_query="non_bus_nw #;network_ND_Junctions #" \
    )

print "Solving route layer ..."
arcpy.na.Solve(routeLayer, "SKIP", "CONTINUE")

print "Saving route results to a feature class ..."
na_classes = arcpy.na.GetNAClassNames(routeLayer)
#print na_classes
routes_layer_name = na_classes["Routes"]
routes_sublayer = arcpy.mapping.ListLayers(routeLayer, routes_layer_name )[1]
#print routes_sublayer
arcpy.management.CopyFeatures(routes_sublayer, "route_results")
arcpy.na.CopyTraversedSourceFeatures(routeLayer, full_path_to_output_gdb, "TraversedEdges", "TraversedJunctions", "TraversedTurns")

stops_layer_name = na_classes["Stops"]
stops_sublayer = arcpy.mapping.ListLayers(routeLayer, stops_layer_name)[0]
arcpy.management.CopyFeatures(stops_sublayer, "stops")

# Join shape_ID name into the traversed edges feature class
arcpy.JoinField_management("TraversedEdges", "RouteID", "route_results", "OBJECTID", ["Name"])

#Clean up additional unneeded feature classes
arcpy.Delete_management("TraversedTurns")

arcpy.CheckInExtension("Network")


# AGGREGATE TRIPS (essentially the same as step 4 in bus code)

# NEAR STOPS TO NETWORK JUNCTIONS
# -----------------------------------------------------------------------
print "Identifying stops that are at an existing network intersection..."

arcpy.Near_analysis(
        in_features=os.path.join(full_path_to_output_gdb, "stops_lrs"),
        near_features="network\\network_ND_Junctions",
        search_radius="10 Meters",
        location="NO_LOCATION",
        angle="NO_ANGLE",
        method="PLANAR"
        )

# READ IN STOPS AND MAKE DICT MAPPING FROM OID TO SOURCEOID WHERE SOURCEID = 1 (NODE)
# -------------------------------------------------------------------------------------------------------

stop_oid_to_net_jct_id_dict = {}

with arcpy.da.SearchCursor("stops", ["OID@", "SourceID", "SourceOID"]) as scursor:
    for row in scursor:
        stops_oid, network_source_id, network_source_oid = row

        if network_source_id == 1:
            stop_oid_to_net_jct_id_dict[stops_oid] = network_source_oid


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

print "Creating list of traversed network segments and stops for each route shape..."

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



# Connecting to the database file
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
where start_date<='__date__' AND end_date>='__date__' AND __day_of_week__ = 1 AND d.route_type <> 3
group by a.trip_id, a.shape_id
'''

trips_fetch = trips_fetch.replace('__date__', (date))
trips_fetch = trips_fetch.replace('__day_of_week__', (day_of_week))

###TRIPS FETCH FOR LIRR IS DIFFERENT
##trips_fetch = '''
##select a.trip_id, a.shape_id
##from trips a
##inner join calendar_dates b on a.service_id = b.service_id
##inner join stop_times c on a.trip_id = c.trip_id
##inner join routes d on a.route_id = d.route_id
##where date='2016-05-24' AND d.route_type <> 3
##group by a.trip_id, a.shape_id
##'''

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
arcpy.AddField_management("flow_result", 'route_type', 'long')
arcpy.AddField_management("flow_result", 'FLOW', 'long')
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

flds = ['OBJECTID', 'OID@', 'SHAPE@', 'route_type']

with arcpy.da.SearchCursor('network/non_bus_nw', flds) as scursor:

    for row in scursor:

        arcid, oid, shape, route_type = row

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
            with arcpy.da.InsertCursor("flow_result", ['SHAPE@', 'ARCID', 'route_type', 'FLOW', 'Actual_Ridership', 'Modeled_Ridership']) as icursor:

                icursor.insertRow([shape, arcid, route_type, flow, actual_ridership, modeled_ridership])


arcpy.AddField_management("flow_result", 'modeled_actual_ridership_ratio', 'Double')
arcpy.CalculateField_management("flow_result", "modeled_actual_ridership_ratio", "!Modeled_Ridership! / !Actual_Ridership!", "PYTHON_9.3")

arcpy.AddField_management("flow_result", 'Miles', 'Double')
arcpy.CalculateField_management("flow_result", "Miles", "!SHAPE_LENGTH! / 1609.34", "PYTHON_9.3")

arcpy.AddField_management("flow_result", 'VMT', 'Double')
arcpy.CalculateField_management("flow_result", "VMT", "!FLOW! * !Miles!", "PYTHON_9.3")

arcpy.AddField_management("flow_result", 'Actual_PMT', 'Double')
arcpy.CalculateField_management("flow_result", "Actual_PMT", "!Actual_Ridership! * !Miles!", "PYTHON_9.3")

arcpy.AddField_management("flow_result", 'Modeled_PMT', 'Double')
arcpy.CalculateField_management("flow_result", "Modeled_PMT", "!Modeled_Ridership! * !Miles!", "PYTHON_9.3")

if arcpy.Exists(os.path.join(output_dir, 'vmt_pmt_system_summary.txt')):
    arcpy.Delete_management(os.path.join(output_dir, 'vmt_pmt_system_summary_non_bus.txt'))
    print "Deleted existing summary statistics..."

arcpy.Statistics_analysis("flow_result", os.path.join(output_dir, 'vmt_pmt_system_summary_non_bus.txt'), [["Miles", "SUM"], ["VMT", "SUM"], ["Actual_PMT", "SUM"], ["Modeled_PMT", "SUM"]])

end_time = datetime.datetime.now()
total_time = end_time - start_time
print ("\nEnd at {}.  Total run time {}".format(end_time, total_time))
