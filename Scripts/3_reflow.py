#-------------------------------------------------------------------------------
# Name:    GTFS_Arnold_Merge_Reflow
#
# Purpose: Reflow GTFS route data along the Arnold Road Network now that stops have been integrated into the network
#
# Author:  Alex Oberg and Gary Baker
#
# Created: 12/5/2016
#
# Last updated 6/15/2017
#-------------------------------------------------------------------------------

# CONFIG
#-------------------------------------------------------------------------------

#MBTA MODEL
sqlite_file      = r"C:\tasks\2016_09_12_GTFS_ingest\Model\MBTA\GTFS-MBTA.sqlite"
calc_location_tol_1 = "5 meters"
calc_location_tol_2 = "2.5 meters"
template_network = r"c:\tasks\2016_09_12_GTFS_ingest\network_template\network_template_bus.gdb\network"
output_dir       = r"c:\tasks\2016_09_12_GTFS_ingest\Model\MBTA\Output"


#Dangle tolerance to connect ARNOLD dangles not currently customized for each city.
DANGLE_TOLERANCE = "15 Meters"

# SETUP
#-------------------------------------------------------------------------------

import datetime
import sqlite3
import arcpy
import os

start_time = datetime.datetime.now()
print('\nStart at ' + str(start_time))
print "Started Step 3: Rerunning Route Solve with Stops"
print "GTFS database being processed: " + sqlite_file

#Original geodatabase must be referenced as it will be copied into a new gdb for the additional round of routing
orig_gdb = os.path.join(output_dir, "gtfs_arnold_prelim.gdb")

output_gdb = "gtfs_arnold_final.gdb"
full_path_to_output_gdb = os.path.join(output_dir, output_gdb)

# GTFS lat/lon are written in WGS1984 coordinates
WGS84 = arcpy.SpatialReference(4326)

# Script projects data into USA Contiguous Albers Equal Area Conic USGS coordinate system
ALBERS_PRJ = arcpy.SpatialReference(102039)

# CREATE NEW GEODATABASE AND COPY FEATURE CLASSES FROM ORIGINAL GEODATABASE
#-------------------------------------------------------------------------------
if arcpy.Exists(full_path_to_output_gdb):
    arcpy.Delete_management(full_path_to_output_gdb)
    print "Deleted existing geodatabase"

arcpy.CreateFileGDB_management(output_dir, output_gdb)

arcpy.env.workspace = full_path_to_output_gdb

print "Adding bus shape ID points to new geodatabase ..."

#Copy existing bus_shape_id_pt layer over and delete all fields calculated in Step 1 so these can be recalculated later
arcpy.CopyFeatures_management(os.path.join(orig_gdb, "bus_shape_id_pt"), "bus_shape_id_pt")
arcpy.DeleteField_management("bus_shape_id_pt", ["SourceID", "SourceOID", "PosAlong", "SideofEdge", "SnapX", "SnapY", "Distance"])

print "Adding stops to new geodatabase ..."

arcpy.CopyFeatures_management(os.path.join(orig_gdb, "stops_lrs"), "stops_lrs")

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


# LOAD DATA TO NETWORK DATSET, BUILD, AND CALC NETWORK LOCATIONS FOR GTFS POINTS
#-------------------------------------------------------------------------------

print "Adding ARNOLD data to network ..."

new_arnold = os.path.join(orig_gdb, "network/arnold_split_stops_nw")

arcpy.Append_management(new_arnold, "network/arnold_split_nw", "NO_TEST")

print "Building network ..."
arcpy.CheckOutExtension("Network")
arcpy.BuildNetwork_na ("network/network_ND")

print "Calculating network locations for bus shape ID points ..."

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


# COPY OVER AND PREPARE THE ALL BUS SHAPES LAYER
# ----------------------------

#Copy existing all_bus_shape_id_pt layer over and delete all fields calculated in Step 1 so updated fields can be joined in
arcpy.CopyFeatures_management(os.path.join(orig_gdb, "all_bus_shape_id_pt"), "all_bus_shape_id_pt")
arcpy.DeleteField_management("all_bus_shape_id_pt", ["SourceID", "SourceOID", "PosAlong", "SideofEdge", "SnapX", "SnapY", "Distance"])

# Join bus shapes with network locations that were built previously
print "Joining GTFS bus shape ID points with network location data ..."
arcpy.JoinField_management("all_bus_shape_id_pt", "ID", "bus_shape_id_pt", "ID", ["SourceID", "SourceOID", "PosAlong", "SideOfEdge", "SnapX", "SnapY"])


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

end_time = datetime.datetime.now()
total_time = end_time - start_time
print ("\nEnd at {}.  Total run time {}".format(end_time, total_time))