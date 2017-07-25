#-------------------------------------------------------------------------------
# Name:    GTFS_Arnold_Merge
#
# Purpose: Associate GTFS route data with the FHWA ARNOLD Road Network
#
# Author:  Alex Oberg and Gary Baker
#
# Created: 9/15/2016
#
# Last updated 6/15/2017
#-------------------------------------------------------------------------------

# CONFIG
#-------------------------------------------------------------------------------
# 2015 ARNOLD/HPMS Data retreived from FHWA
# sqlite_file must be built from raw gtfs text files using Step 0 script (pygtfs)
# flag is a point located along the transit area's ARNOLD network-- facilitates check for disconnected features that must be deleted before route solve. Must be manually created for each transit agency
# template_network is a blank network dataset needed to programatically build networks for multiple cities
# enhanced_arnold is a shapefile that contains road segments missing from ARNOLD. Must be created manually and maintained as a companion to each state's ARNOLD data
# output_dir is the directory where the output geodatabase is saved (must exist)

#MBTA MODEL
sqlite_file      = r"C:\tasks\2016_09_12_GTFS_ingest\Model\MBTA\GTFS-MBTA.sqlite"
arnold           = r"C:\tasks\2016_09_12_GTFS_ingest\hpms\MA_2015\ROUTE_SHAPES.shp"
hpms             = r"C:\tasks\2016_09_12_GTFS_ingest\hpms\MA_2015\National_2015_25_Intersections.csv"
flag             = r"C:\tasks\2016_09_12_GTFS_ingest\Model\MBTA\Input\MBTA_connected_flag.shp"
enhanced_arnold  = r"C:\tasks\2016_09_12_GTFS_ingest\hpms\MA_2015\enhanced_arnold.shp"
calc_location_tol_1 = "5 meters"
calc_location_tol_2 = "2.5 meters"
template_network = r"c:\tasks\2016_09_12_GTFS_ingest\network_template\network_template_bus.gdb\network"
output_dir       = r"c:\tasks\2016_09_12_GTFS_ingest\Model\MBTA\Output"


#Dangle tolerance to connect ARNOLD dangles with neighboring ARNOLD segments not currently customized for each city.
DANGLE_TOLERANCE = "15 Meters"

# SETUP
#-------------------------------------------------------------------------------

import datetime
import sqlite3
import arcpy
import os

start_time = datetime.datetime.now()
print('\nStart at ' + str(start_time))
print "Started Step 1: Route Snapping to ARNOLD"
print "GTFS database being processed: " + sqlite_file

output_gdb = "gtfs_arnold_prelim.gdb"
full_path_to_output_gdb = os.path.join(output_dir, output_gdb)

# GTFS lat/lon are written in WGS1984 coordinates
WGS84 = arcpy.SpatialReference(4326)

# Script projects data into USA Contiguous Albers Equal Area Conic USGS coordinate system
ALBERS_PRJ = arcpy.SpatialReference(102039)

# CREATE GEODATABASE AND BUS_SHAPE_ID_PT_TMP FEATURE CLASS
#-------------------------------------------------------------------------------
if arcpy.Exists(full_path_to_output_gdb):
    arcpy.Delete_management(full_path_to_output_gdb)
    print "Deleted existing geodatabase"

arcpy.CreateFileGDB_management(output_dir, output_gdb)

arcpy.env.workspace = full_path_to_output_gdb

arcpy.management.CreateFeatureclass(full_path_to_output_gdb, "bus_shape_id_pt_tmp", "POINT", spatial_reference=WGS84)
arcpy.AddField_management("bus_shape_id_pt_tmp", "ID", "TEXT", "40")
arcpy.AddField_management("bus_shape_id_pt_tmp", "lat", "DOUBLE")
arcpy.AddField_management("bus_shape_id_pt_tmp", "long", "DOUBLE")

# GET UNIQUE BUSH SHAPE POINTS AND ADD THEM TO THE LAYER
#-------------------------------------------------------------------------------

print "Fetching unique bus shape ID point data from GTFS feed ..."

# Connecting to the database file
conn = sqlite3.connect(sqlite_file)
cursor = conn.cursor()

# Fetch unique bus shape ID points, rounding lat/longs to six places. At this point, only need BUSES (ROUTE TYPE = 3)
bus_shapes_fetch = '''
    SELECT DISTINCT ROUND(shape_pt_lat, 6) as shape_pt_lat,  ROUND(shape_pt_lon, 6) as shape_pt_lon
    FROM shapes
    INNER JOIN (
    	SELECT shape_id, route_id
	FROM trips
	GROUP BY shape_id, route_id
	) t ON t.shape_id = shapes.shape_id
    INNER JOIN (
	SELECT route_type, route_id
	FROM routes
	WHERE route_type = 3
	GROUP BY route_id
	) r ON r.route_id = t.route_id;
'''

cursor.execute(bus_shapes_fetch)
bus_shape_list = cursor.fetchall()
conn.close

print "Unique bus shape ID list contains {} points".format(len(bus_shape_list))

bus_shape_flds = ["shape@", "ID", "lat", "long"]
bus_shape_insert_cursor = arcpy.da.InsertCursor("bus_shape_id_pt_tmp", bus_shape_flds)

# Create point out of lat long/fields and insert geometry and fields into feature class.
# also create a unique ID field concatanating lat:lon.

print "Creating bus shape ID point feature class ..."

bus_shape_point = arcpy.Point()

for shape in bus_shape_list:
    bus_shape_point.X = float(shape[1])
    bus_shape_point.Y = float(shape[0])
    bus_shape_point_geom = arcpy.PointGeometry(bus_shape_point, WGS84)
    bus_shape_insert_cursor.insertRow([bus_shape_point_geom, str(shape[0])+":"+str(shape[1]), shape[0], shape[1]])

del bus_shape_insert_cursor

arcpy.Project_management("bus_shape_id_pt_tmp", "bus_shape_id_pt", ALBERS_PRJ)

# SUBSET ARNOLD AND PROJECTING ARNOLD AND GTFS POINT TO LOCAL COORD SYS
#-------------------------------------------------------------------------------
print "Subsetting ARNOLD ..."
arcpy.MakeFeatureLayer_management (arnold, "arnold_lyr")
arcpy.SelectLayerByLocation_management('arnold_lyr', 'WITHIN_A_DISTANCE_GEODESIC', 'bus_shape_id_pt_tmp', '.5 miles')
arcpy.CopyFeatures_management("arnold_lyr", "arnold_clipped_tmp")

print "Adding HPMS AADT Data ..."
arcpy.MakeRouteEventLayer_lr ("arnold_clipped_tmp", "ROUTE_ID", hpms, "ROUTE_ID LINE Begin_Point End_Point", "route_event_lyr")
arcpy.CopyFeatures_management("route_event_lyr", "arnold_hpms_tmp")

print "Appending roads without HPMS AADT Data ..."
# This is needed because making a route event layer discards any portions of ARNOLD which do not have any linear-referenced data-- we still want these sections of the network for routing
arcpy.Erase_analysis(in_features="arnold_clipped_tmp", erase_features="arnold_hpms_tmp", out_feature_class="arnold_no_hpms", cluster_tolerance="")
arcpy.CopyFeatures_management("arnold_hpms_tmp", "arnold_enhanced")
arcpy.Append_management("arnold_no_hpms", "arnold_enhanced", "NO_TEST","","")

print "Appending enhanced ARNOLD data ..."
arcpy.Append_management(enhanced_arnold, "arnold_enhanced", "NO_TEST","","")

print "Converting ARNOLD Data into road segments and further subsetting..."
arcpy.FeatureToLine_management("arnold_enhanced", "arnold_split_tmp")

arcpy.MakeFeatureLayer_management ("arnold_split_tmp", "arnold_split_tmp_lyr")
arcpy.SelectLayerByLocation_management('arnold_split_tmp_lyr', 'WITHIN_A_DISTANCE_GEODESIC', 'bus_shape_id_pt_tmp', '.5 miles')
arcpy.CopyFeatures_management("arnold_split_tmp_lyr", "arnold_split_tmp2")

print "Projecting ARNOLD subset ..."
arcpy.Project_management("arnold_split_tmp2", "arnold_split_tmp3", ALBERS_PRJ)

print "Removing duplicate geometry ..."
arcpy.AddGeometryAttributes_management("arnold_split_tmp3", "LENGTH;LINE_START_MID_END")

arcpy.Dissolve_management("arnold_split_tmp3", "arnold_split",
                         ["MID_X", "MID_Y", "LENGTH"], [["AADT_VN", "MAX"]], "SINGLE_PART",
                          "DISSOLVE_LINES")

arcpy.AddField_management("arnold_split", "AADT_VN", "DOUBLE" )
arcpy.CalculateField_management("arnold_split", "AADT_VN", "!MAX_AADT_VN!", "PYTHON_9.3")

arcpy.DeleteField_management("arnold_split", ["MAX_AADT_VN"])

arcpy.Delete_management("arnold_enhanced")
arcpy.Delete_management("arnold_lyr")
arcpy.Delete_management("arnold_clipped_tmp")
arcpy.Delete_management("route_event_lyr")
arcpy.Delete_management("arnold_hpms_tmp")
arcpy.Delete_management("arnold_no_hpms")
arcpy.Delete_management("arnold_split_tmp")
arcpy.Delete_management("arnold_split_tmp_lyr")
arcpy.Delete_management("arnold_split_tmp2")
arcpy.Delete_management("arnold_split_tmp3")
arcpy.Delete_management("bus_shape_id_pt_tmp")


# CONNECT DANGLES
# ---------------------------------------------------------------------------------------

print "Identifying and connecting dangles ..."
arcpy.FeatureVerticesToPoints_management(\
    in_features="arnold_split", \
    out_feature_class="arnold_split_dangnodes", \
    point_location="DANGLE")

arcpy.GenerateNearTable_analysis(
    in_features="arnold_split_dangnodes",
    near_features="arnold_split",
    out_table="tmp_arnold_unsp_dangnodes_near_tab",
    search_radius = DANGLE_TOLERANCE,
    location="LOCATION",
    angle="NO_ANGLE",
    closest="ALL",
    closest_count="25",
    method="PLANAR")

arcpy.CopyFeatures_management("arnold_split", "tmp_arnold_unsp_dngfx")

arcpy.AddField_management("tmp_arnold_unsp_dngfx", "ARTIFICIAL", "LONG" )
arcpy.CalculateField_management("tmp_arnold_unsp_dngfx", "ARTIFICIAL", 0, "PYTHON_9.3")

poly_lines_to_add = []

with arcpy.da.SearchCursor("tmp_arnold_unsp_dangnodes_near_tab", ["from_x", "from_y", "near_x", "near_y"]) as scursor:
    for row in scursor:
        coordList = []
        coordList.append(arcpy.Point(row[0], row[1]))
        coordList.append(arcpy.Point(row[2], row[3]))
        poly_lines_to_add.append(arcpy.Polyline(arcpy.Array(coordList)))

edit = arcpy.da.Editor(full_path_to_output_gdb)
edit.startEditing(False, False)
edit.startOperation()

icursor = arcpy.da.InsertCursor("tmp_arnold_unsp_dngfx", ['SHAPE@', 'ARTIFICIAL'])

for line in poly_lines_to_add:
    icursor.insertRow([line, 1])

del icursor

edit.stopOperation()
edit.stopEditing(True)

print 'inserted {} dangle links'.format(len(poly_lines_to_add))

# Create points at each end of the newly added dangles for splitting
arcpy.MakeFeatureLayer_management ("tmp_arnold_unsp_dngfx", "dangle_layer")
arcpy.SelectLayerByAttribute_management ("dangle_layer", "NEW_SELECTION", "ARTIFICIAL = 1")
arcpy.CopyFeatures_management("dangle_layer", "temp_dangle")

arcpy.FeatureVerticesToPoints_management(\
    in_features="temp_dangle", \
    out_feature_class="temp_dangle_nodes", \
    point_location="BOTH_ENDS")

arcpy.SplitLineAtPoint_management( \
    in_features="tmp_arnold_unsp_dngfx", \
    point_features="temp_dangle_nodes", \
    out_feature_class=  "arnold_split_dngfx", \
    search_radius="1 Meters")

# Search radius above is essential-- if a minimal search radius is not used some spurious disconnected pieces of the network dataset may be created, regardless of tolerance setting"

print "Deleting disconnected ARNOLD features ..."
#Create new feature dataset for geometric network
arcpy.CreateFeatureDataset_management(full_path_to_output_gdb, "geom_nw_fd", "arnold_split_dngfx")

#Copy arnold_split_dngfx into the feature dataset
arcpy.CopyFeatures_management("arnold_split_dngfx", "geom_nw_fd/arnold_split_geom_nw")

#Create geometric network
arcpy.CreateGeometricNetwork_management("geom_nw_fd", "geom_nw", "arnold_split_geom_nw SIMPLE_EDGE NO", "")

#Find connectivity errors- requires a flag point known to be on network. Snapping ensures the flag point does occur along the network even after network is projected
arcpy.Snap_edit(flag, [["geom_nw_fd/arnold_split_geom_nw", "EDGE", "5 Meters"]])
arcpy.TraceGeometricNetwork_management("geom_nw_fd/geom_nw", "geom_con_lyr", flag, "FIND_CONNECTED")
arcpy.SelectLayerByAttribute_management("geom_con_lyr/arnold_split_geom_nw", "SWITCH_SELECTION")
result = arcpy.GetCount_management("geom_con_lyr/arnold_split_geom_nw")
count = int(result.getOutput(0))

#Delete disconnected features
arcpy.DeleteFeatures_management("geom_con_lyr/arnold_split_geom_nw")
print str(count) + " disconnected features deleted"


# PREPARE THE NETWORK
#-------------------------------------------------------------------------------

print "Preparing network ..."

# Copy template network dataset into working file geodatabase
arcpy.Copy_management(template_network, os.path.join(full_path_to_output_gdb, "network"), "FeatureDataset")

edit = arcpy.da.Editor(full_path_to_output_gdb)
edit.startEditing(False, False)
edit.startOperation()

# Delete all existing Arnold road segments from template
with arcpy.da.UpdateCursor("network/arnold_split_nw", "OBJECTID") as cursor:
    for row in cursor:
        cursor.deleteRow()

edit.stopOperation()
edit.stopEditing(True)


# LOAD DATA TO NETWORK DATSET, BUILD, AND CALCULATE NETWORK LOCATIONS FOR BUS GTFS SHAPE POINTS
#-------------------------------------------------------------------------------

print "Adding ARNOLD data to network ..."
arcpy.Append_management("geom_nw_fd/arnold_split_geom_nw", "network/arnold_split_nw", "NO_TEST")

arcpy.Delete_management("arnold_split")
arcpy.Delete_management("arnold_split_dangnodes")
arcpy.Delete_management("tmp_arnold_unsp_dangnodes_near_tab")
arcpy.Delete_management("tmp_arnold_unsp_dngfx")
arcpy.Delete_management("temp_dangle")
arcpy.Delete_management("temp_dangle_nodes")
arcpy.Delete_management("arnold_split_dngfx")
arcpy.Delete_management("geom_nw_fd")

print "Building network ..."
arcpy.CheckOutExtension("Network")
arcpy.BuildNetwork_na ("network/network_ND")

print "Calculating network locations for points ..."

arcpy.CalculateLocations_na(
        in_point_features="bus_shape_id_pt",
        in_network_dataset="NETWORK/network_ND",
        search_tolerance=calc_location_tol_1,
        search_criteria="network_ND_Junctions SHAPE;arnold_split_nw NONE"
        )

arcpy.MakeFeatureLayer_management ("bus_shape_id_pt", "bus_shape_id_pt_lyr")
arcpy.SelectLayerByAttribute_management ("bus_shape_id_pt_lyr", "NEW_SELECTION", "SourceID = -1")

arcpy.CalculateLocations_na(
        in_point_features="bus_shape_id_pt_lyr",
        in_network_dataset="NETWORK/network_ND",
        search_tolerance=calc_location_tol_2,
        search_criteria="arnold_split_nw SHAPE;network_ND_Junctions NONE"
        )


# GET ALL BUS SHAPE ID POINTS AND ADD THEM TO A LAYER
#-------------------------------------------------------------------------------

arcpy.management.CreateFeatureclass(full_path_to_output_gdb, "all_bus_shape_id_pt_tmp", "POINT", spatial_reference=WGS84)
arcpy.AddField_management("all_bus_shape_id_pt_tmp", "ID", "TEXT", "40")
arcpy.AddField_management("all_bus_shape_id_pt_tmp", "route_id", "TEXT", "100")
arcpy.AddField_management("all_bus_shape_id_pt_tmp", "feed_id", "LONG")
arcpy.AddField_management("all_bus_shape_id_pt_tmp", "shape_id", "TEXT", "40")
arcpy.AddField_management("all_bus_shape_id_pt_tmp", "lat", "DOUBLE")
arcpy.AddField_management("all_bus_shape_id_pt_tmp", "long", "DOUBLE")
arcpy.AddField_management("all_bus_shape_id_pt_tmp", "VisitOrder", "LONG")
arcpy.AddField_management("all_bus_shape_id_pt_tmp", "shape_dist_traveled", "DOUBLE")
arcpy.AddField_management("all_bus_shape_id_pt_tmp", "route_type", "LONG")

print "Fetching all bus shape ID point data from GTFS feed ..."

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
	WHERE route_type = 3	GROUP BY route_id
	) r ON r.route_id = t.route_id;
'''

cursor.execute(shapes_fetch)
shape_list = cursor.fetchall()  # TODO dangerous
conn.close

print "All bus shape ID list contains {} points".format(len(shape_list))

shape_seg_flds = ["shape@", "ID", "route_id", "feed_id", "shape_id", "lat", "long", "VisitOrder", "shape_dist_traveled", "route_type"]
shape_insert_cursor = arcpy.da.InsertCursor("all_bus_shape_id_pt_tmp", shape_seg_flds)

# Create point out of lat long/fields and insert geometry and fields into feature class.
# also create an ID field concatanating lat:lon.

print "Creating all bus shape ID point feature class ..."

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
arcpy.Project_management("all_bus_shape_id_pt_tmp", "all_bus_shape_id_pt", ALBERS_PRJ)


# Join bus shapes with network locations that were built previously
print "Joining GTFS bus shape ID points with network location data ..."
arcpy.JoinField_management("all_bus_shape_id_pt", "ID", "bus_shape_id_pt", "ID", ["SourceID", "SourceOID", "PosAlong", "SideOfEdge", "SnapX", "SnapY"])

#Delete extra feature classes
arcpy.Delete_management("all_bus_shape_id_pt_tmp")

# DELETE BUS SHAPE ID POINTS WITHIN THE SAME ROUTE THAT SNAPPED TO THE SAME NODE
# --------------------------------------------------------------------------

dup_junc_dict = {}

flds = ['shape_id', 'SourceID', 'SourceOID', 'SnapX', 'SnapY', 'OID@']

with arcpy.da.SearchCursor("all_bus_shape_id_pt", flds) as scursor:

    for row in scursor:

        shape_id, source_id, source_oid, snap_x, snap_y, oid = row

        #if shape_id != '710030':
            #continue

        # TODO Make sure we don't need to worry about points that snap to an identical edge-- doesn't seem to be causing problems so far
        if source_id == 1:
            pass

        elif source_id == 2:

            if not shape_id in dup_junc_dict:
                dup_junc_dict[shape_id] = {}

            if not source_oid in dup_junc_dict[shape_id]:
                dup_junc_dict[shape_id][source_oid] = [oid]
            else:
                dup_junc_dict[shape_id][source_oid].append(oid)

oids_to_del = []

for shape_id, oid_dict in dup_junc_dict.iteritems():

    #print 'shape id = {}'.format(shape_id)

    for source_oid, rec_oid_array in oid_dict.iteritems():

        if len(rec_oid_array) > 1:
            #print '\t{} -> {}'.format(source_oid, rec_oid_array[1:])
            oids_to_del.extend(rec_oid_array[1:])

#print oids_to_del

flds = ['oid@']
with arcpy.da.UpdateCursor("all_bus_shape_id_pt", flds) as cursor:

    for row in cursor:

        if row[0] in oids_to_del:

            cursor.deleteRow()

# SETUP AND SOLVE THE ROUTE
# --------------------------

arcpy.CheckOutExtension("Network")

print "Preparing route layer ..."
routeLayer = arcpy.na.MakeRouteLayer("network/network_ND", "Routes", "Length").getOutput(0)

arcpy.AddLocations_na( \
    in_network_analysis_layer=routeLayer, \
    sub_layer="Stops", \
    in_table="all_bus_shape_id_pt", \
    field_mappings="SourceID SourceID #;SourceOID SourceOID #;PosAlong PosAlong #;SideOfEdge SideOfEdge #;RouteName shape_id #;Name Name #", \
    search_tolerance="5000 Meters", \
    sort_field="VisitOrder", \
    search_criteria="arnold_split_nw SHAPE;network_ND_Junctions NONE", \
    match_type="MATCH_TO_CLOSEST", \
    append="CLEAR", \
    snap_to_position_along_network="SNAP", \
    snap_offset="5 Meters", \
    exclude_restricted_elements="INCLUDE", \
    search_query="arnold_split_nw #;network_ND_Junctions #" \
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

# Join shape_ID name into the traversed edges feature class
arcpy.JoinField_management("TraversedEdges", "RouteID", "route_results", "OBJECTID", ["Name"])

#Clean up additional unneeded feature classes
arcpy.Delete_management("TraversedJunctions")
arcpy.Delete_management("TraversedTurns")

arcpy.CheckInExtension("Network")

# REPORT FINAL ROUTES
# -------------------

#Connecting to the database file
conn = sqlite3.connect(sqlite_file)
cursor = conn.cursor()

# Fetch unique bus shape ID points, rounding lat/longs to six places. CURRENTLY ONLY BUSES
shapes_fetch = '''
SELECT COUNT(*) AS count FROM  (
    SELECT DISTINCT
        shapes.shape_id
    FROM shapes
    INNER JOIN (
        SELECT shape_id, route_id
        FROM trips
        GROUP BY shape_id, route_id
        ) t ON t.shape_id = shapes.shape_id
    INNER JOIN (
        SELECT route_type, route_id
        FROM routes
        WHERE route_type = 3
        GROUP BY route_id
        ) r ON r.route_id = t.route_id);
'''

cursor.execute(shapes_fetch)
count = cursor.fetchone()[0]
conn.close

print count

total_routes = int(count)

routes_solved = int(arcpy.GetCount_management(routes_sublayer).getOutput(0))

pct = (routes_solved * 1.0 / total_routes * 1.0 ) * 100

print '{} of {} routes created ({:.1f}%)'.format(routes_solved, total_routes, pct)

# Compare raw bus shape length to solved routes
print "Comparing route results to raw shape lines ..."

#CHECK # 1: RAW LENGTH VS SNAPPED TO ARNOLD LENGTH

#Create raw route shapes from the points without snapping to ARNOLD
arcpy.PointsToLine_management("all_bus_shape_id_pt",
                              "bus_route_shapes_raw_temp",
                              "shape_id", "VisitOrder")

# Project output from raw shapes
arcpy.Project_management("bus_route_shapes_raw_temp", "bus_route_shapes_raw", ALBERS_PRJ)
arcpy.Delete_management("bus_route_shapes_raw_temp")

#Add and calculate length field
arcpy.AddField_management("bus_route_shapes_raw", "raw_length", "DOUBLE")
arcpy.CalculateField_management("bus_route_shapes_raw", "raw_length", "!shape.length@meters!", "PYTHON_9.3")

# Join
arcpy.JoinField_management("route_results", "Name", "bus_route_shapes_raw", "shape_id", ["Raw_Length"])

# Add ratio and calculate field
arcpy.AddField_management("route_results", "ratio", "DOUBLE")
arcpy.CalculateField_management("route_results", "ratio", "!SHAPE_LENGTH!/!Raw_Length!", "PYTHON_9.3")

# CHECK # 2 identify bus shape ID points not near an ARNOLD road by shape ID

# Select by attribute
arcpy.MakeFeatureLayer_management ("all_bus_shape_id_pt", "all_bus_shape_id_pt_lyr")
arcpy.SelectLayerByLocation_management ("all_bus_shape_id_pt_lyr", "WITHIN_A_DISTANCE", "network/arnold_split_nw", "15 meters")
arcpy.SelectLayerByAttribute_management ("all_bus_shape_id_pt_lyr", "SWITCH_SELECTION")

# Export to new feature class-- fc shows bus shape points that are not located within 15 meters of an ARNOLD road--
# may help with identifying areas with missing ARNOLD data that could be added to the enhanced_arnold dataset
arcpy.CopyFeatures_management("all_bus_shape_id_pt_lyr", "problem_bus_shape_points")

# Summary statistics-- table shows bus shape points that did not occur within 15 meters of an ARNOLD road--
# may help with identifying areas with missing ARNOLD data that could be added to the enhanced_arnold dataset
arcpy.Statistics_analysis("problem_bus_shape_points", "problem_bus_shape_points_summary", "ID COUNT", "shape_id")


#-------------------------------------------------------------------------------
## TO DO deal with projections- original/projected data not matching up

## TO DO Make script compatible with multi-state transit systems

## TO DO deal with ability to automatically select ARNOLD state/states data based on transit system state/states

## TO DO Make script compatible with national BTS GTFS feed rather than just individual transit feeds

end_time = datetime.datetime.now()
total_time = end_time - start_time
print ("\nEnd at {}.  Total run time {}".format(end_time, total_time))

