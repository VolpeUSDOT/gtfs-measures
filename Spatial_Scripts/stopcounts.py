#-------------------------------------------------------------------------------
# Name:        Create Stops
# Purpose: Creates stop feature class based on a GTFS feed that has already been ingested into SQlite.
# Includes count of trips served by each stop. Currently divides by seven to get a average daily # of trips per stop
# Further improvements necessary to incorporate data from calendar.txt and weekday/weekend distinctions.
#
# Requires arcpy
#
# Author:      Alex Oberg
#
# Created:     9/15/2016

#-------------------------------------------------------------------------------
import datetime
import sqlite3
import arcpy
import os
import csv

start_time = datetime.datetime.now()
print('\nStart at ' + str(start_time))

# CONFIG
#-------------------------------------------------------------------------------
sqlite_file = r"C:\tasks\2016_09_12_GTFS_ingest\MBTA\mbta_gtfs.sqlite"
output_dir = r"C:\tasks\2016_09_12_GTFS_ingest\MBTA\output"
output_gdb = "stops.gdb"
full_path_to_output_gdb = os.path.join(output_dir, output_gdb)

#MAIN
#-------------------------------------------------------------------------------


# GTFS stop lat/lon are written in WGS1984 coordinates
WGS84 = arcpy.SpatialReference(4326)

# Create geodatabase if it doesn't already exist
if arcpy.Exists(full_path_to_output_gdb):
    arcpy.Delete_management(full_path_to_output_gdb)
    print "Deleted existing geodatabase"
arcpy.CreateFileGDB_management(output_dir, output_gdb)
    
arcpy.env.workspace = full_path_to_output_gdb

arcpy.management.CreateFeatureclass(full_path_to_output_gdb, "stops", "POINT", spatial_reference=WGS84)
arcpy.AddField_management("stops", "stop_id", "text", "100")
arcpy.AddField_management("stops", "stop_name", "text", "100")
arcpy.AddField_management("stops", "count", "long")
arcpy.AddField_management("stops", "count_day", "double")
    
# Connecting to the database file
conn = sqlite3.connect(sqlite_file)
c = conn.cursor()

# Query stops with counts of how many times each stop is served by any trip in the GTFS feed
stop_fetch = ('SELECT stops.stop_id, stops.stop_name, count(stop_times.trip_id) AS stop_count, stops.stop_lat, stops.stop_lon\
  FROM trips\
  INNER JOIN stop_times ON stop_times.trip_id = trips.trip_id\
  INNER JOIN stops ON stops.stop_id = stop_times.stop_id\
  INNER JOIN routes ON routes.route_id = trips.route_id\
  WHERE (routes.route_type = 3 AND stop_times.pickup_type <> 1)\
  GROUP BY stops.stop_id\
  ORDER BY stop_count DESC;')

c.execute(stop_fetch)
stop_list = c.fetchall()

# Print out list of top ten stops as a test
print stop_list[:10]

# Start editing session and create insert cursor
edit = arcpy.da.Editor(full_path_to_output_gdb)
edit.startEditing(False, False)
edit.startOperation()

stop_flds = ["shape@", "stop_id", "stop_name", "count", "count_day"]
stop_insert_cursor = arcpy.da.InsertCursor("stops", stop_flds)

# Create point out of lat long/fields and insert geometry and fields into feature class
stop_point = arcpy.Point()

for stop in stop_list:

    stop_point.X = float(stop[4])
    stop_point.Y = float(stop[3])

    stop_point_geom = arcpy.PointGeometry(stop_point, WGS84)

    stop_insert_cursor.insertRow([stop_point_geom, stop[0], stop[1], stop[2], None])

# Stop editing session and delete insert cursor
edit.stopOperation()
edit.stopEditing(True)

del stop_insert_cursor

# Calculate frequency per day
arcpy.CalculateField_management("stops", "count_day", "float(!count!) /float(7) ", "PYTHON_9.3")

# Closing the connection to the database file
conn.close

end_time = datetime.datetime.now()
total_time = end_time - start_time
print ("\nEnd at {}.  Total run time {}".format(end_time, total_time))
