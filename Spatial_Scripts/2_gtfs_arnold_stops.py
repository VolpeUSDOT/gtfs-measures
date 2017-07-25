#-------------------------------------------------------------------------------
# Name:    GTFS_Arnold_Stops
#
# Purpose: Associate stops with the route shapes that have already been snapped to ARNOLD
#
# Author:  Alex Oberg and Gary Baker
#
# Created: 10/17/2016
#
# Last updated 6/15/2017
#-------------------------------------------------------------------------------

# CONFIG
#-------------------------------------------------------------------------------

#MBTA MODEL
sqlite_file      = r"C:\tasks\2016_09_12_GTFS_ingest\Model\MBTA\GTFS-MBTA.sqlite"
output_dir       = r"c:\tasks\2016_09_12_GTFS_ingest\Model\MBTA\Output"

# SETUP
#-------------------------------------------------------------------------------

import datetime
import sqlite3
import arcpy
import os


#out_file = os.path.join(output_dir, 'test.txt')
#wf = open(out_file, 'w')
#wf.write("shape_id, trip_id, stop_lat, stop_lon, milepost\n")


start_time = datetime.datetime.now()
print('\nStart at ' + str(start_time))
print "Started Step 2: Snapping Stops to Routes"
print "GTFS database being processed: " + sqlite_file

output_gdb = "gtfs_arnold_prelim.gdb"
full_path_to_output_gdb = os.path.join(output_dir, output_gdb)
arcpy.env.workspace = full_path_to_output_gdb
arcpy.env.overwriteOutput = True

WGS84      = arcpy.SpatialReference(4326)
ALBERS_PRJ = arcpy.SpatialReference(102039)

traversed_oid_dict = {}

con = sqlite3.connect(sqlite_file)


# Prepare the output file
# -----------------------

out_lrs_file = os.path.join(output_dir, 'rtshp_lr_stops.txt')
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
where r.route_type = 3 AND shape_id <> ""
'''

cur_shape_id = con.cursor()

for shape_row in cur_shape_id.execute(sql_shape):
    #Cast as string otherwise non-numeric characters in shape_ID can cause many issues (e.g. some can come across as scientific notation).
    shape_id = str(shape_row[0])
    #print 'processing shape id {}'.format(shape_id)

    #Testing on individual route shapes
    #if not shape_id == '34E0040':
        #continue

    #if not shape_id == '850026':
        #continue


    # GET THE THE CONSTRUCTED ROUTE GEOMETRY FOR THE current ROUTE SHAPE ID
    # --------------------------------------------------------

    arcpy.MakeFeatureLayer_management ("route_results", "route_results_lyr")
    route_results_query = 'name = \'{}\''.format(shape_id)
    arcpy.SelectLayerByAttribute_management ("route_results_lyr", "NEW_SELECTION", route_results_query)

    if int(arcpy.GetCount_management("route_results_lyr").getOutput(0)) != 1:
        print 'Can''t process route shape {} because it doesn''t have a single geography'.format(shape_id)

    route_geometry = None
    with arcpy.da.SearchCursor("route_results_lyr", ["SHAPE@"]) as scursor:
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

arcpy.MakeRouteEventLayer_lr ("route_results", "Name" , out_lrs_file, "ROUTE_SHAPE POINT MP", "stop_events")

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
arcpy.SplitLineAtPoint_management("network/arnold_split_nw","stops_lrs","network/arnold_split_stops_nw","1 Meters")

end_time = datetime.datetime.now()
total_time = end_time - start_time
print ("\nEnd at {}.  Total run time {}".format(end_time, total_time))